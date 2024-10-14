use bytemuck::AnyBitPattern;
use thiserror::Error;

mod header;
use header::*;

mod keyval_array;
mod sized_array;
mod rev_sized_array;
pub use sized_array::SizedArray;
pub use rev_sized_array::RevSizedArray;
pub use keyval_array::KeyValArray;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PageError {
    #[error("No space left in page to insert")]
    OutofSpace,
    #[error("Data Corruption")]
    DataCorruption,
}

const VAR_BASE: usize = 0;
const PAGE_4K: usize = 4096;
const SPACE_4K: usize = PAGE_4K - std::mem::size_of::<TwoArrayTrailer>();

/// View over a region of memory with variable-length slots, that grows upwards.
struct VarPageIter {
    ptr: *mut u8,
    ptr_end: *mut u8,
    len: *mut u8,
    prev_ptr: *mut u8,
    prev_end: *mut u8,
}

impl VarPageIter {
    /// Construct a view over a page, given the appropriate length
    pub unsafe fn new(base: *mut u8, len: isize) -> Self {
        let end = base.offset(len);
        Self {
            ptr: base,
            ptr_end: end,
            len: end,
            prev_ptr: base,
            prev_end: end,
        }
    }

    /// Read the current data under the pointer
    pub fn read_unaligned<T: AnyBitPattern>(&self) -> T {
        assert!(
            std::mem::size_of::<T>() <= 8,
            "Type T must be 8 bytes or fewer"
        );
        unsafe { (self.ptr as *mut T).read_unaligned() }
    }

    /// Read the current data under the end pointer
    pub fn read_unaligned_back<T: AnyBitPattern>(&self) -> T {
        assert!(
            std::mem::size_of::<T>() <= 8,
            "Type T must be 8 bytes or fewer"
        );
        unsafe { (self.ptr_end as *mut T).read_unaligned() }
    }

    /// Get the data range we just moved past, as a slice
    pub fn data(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.ptr as *const u8,
                self.ptr.offset_from(self.prev_ptr) as usize,
            )
        }
    }

    /// Get the data range we just moved past, as a mutable slice
    pub fn data_mut(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self.ptr, self.ptr.offset_from(self.prev_ptr) as usize)
        }
    }

    /// Get the data range currently under the end, as a slice
    pub fn data_back(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.ptr_end as *const u8,
                self.ptr_end.offset_from(self.prev_end) as usize,
            )
        }
    }

    /// Get the data range currently under the end, as a mutable slice
    pub fn data_mut_back(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.ptr_end,
                self.ptr_end.offset_from(self.prev_end) as usize,
            )
        }
    }

    pub fn len(&self) -> usize {
        self.len as usize & 0xFFF
    }

    pub fn done(&self) -> bool {
        self.ptr == self.ptr_end
    }

    /// Increment the base, failing if the result pushes us past the end pointer.
    pub fn next(&mut self, by: isize) -> Result<(), PageError> {
        let new_ptr = self.ptr.wrapping_offset(by);
        if new_ptr > self.ptr_end {
            return Err(PageError::DataCorruption);
        }
        self.ptr = new_ptr;
        Ok(())
    }

    /// Decrement the end, failing if the result pushes us past the base pointer.
    pub fn next_back(&mut self, by: isize) -> Result<(), PageError> {
        let new_end = self.ptr.wrapping_offset(-by);
        if new_end < self.ptr {
            return Err(PageError::DataCorruption);
        }
        self.prev_end = self.ptr_end;
        self.ptr_end = new_end;
        Ok(())
    }

    /// Delete the object under the endpoint cursor, using it up
    pub fn delete_back(self) {
        // Safety: As long as the pointers were initialized correctly, this copy is only going to
        // cover the initial pointed-to slice.
        unsafe {
            core::ptr::copy(
                self.prev_end,
                self.ptr_end,
                self.len.offset_from(self.prev_end) as usize,
            );
        }
    }

    /// Resize the area under the endpoint cursor, returning a pointer to the resized area.
    ///
    /// # Safety
    ///
    /// If using a positive adjustment, it must not move the end past the end of the valid memory
    /// region these pointers were constructed from, nor overwrite any in-use data.
    ///
    /// Relatedly, it must not resize the area under the endpoint cursor to be larger than the valid
    /// memory region this struct was constructed from.
    pub unsafe fn resize_back(self, diff: isize) -> *mut u8 {
        unsafe {
            core::ptr::copy(
                self.prev_end,
                self.prev_end.offset(diff),
                self.len.offset_from(self.prev_end) as usize,
            );
            self.ptr_end
        }
    }

    /// Create a new slot under the endpoint cursor, returning a pointer to the new area.
    ///
    /// # Safety
    ///
    /// The new size must not move the end of this slice past the end of the valid memory region.
    pub unsafe fn insert_back(self, new_size: usize) -> *mut u8 {
        unsafe {
            core::ptr::copy(
                self.ptr_end,
                self.ptr_end.add(new_size),
                self.len.offset_from(self.ptr_end) as usize,
            );
            self.ptr_end
        }
    }
}

// View over a memory region of fixed-size slots that grows downwards from the header at the end of
// the page.
struct FixedPageIter<T: AnyBitPattern> {
    ptr: *mut T,
    ptr_top: *mut T,
    bottom: *mut T,
    prev_ptr: *mut T,
}

impl<T: AnyBitPattern> FixedPageIter<T> {
    /// Construct a view over a page, given the appropriate length
    ///
    /// # Safety
    ///
    /// The length must be less than the offset value [`PAGE_HEADER`].
    ///
    pub unsafe fn new(base: *mut u8, len: isize) -> Self {
        assert!(std::mem::size_of::<T>() <= 8);
        let base = base as *mut T;
        let bottom =
            base.byte_offset(SPACE_4K as isize - len * (std::mem::size_of::<T>() as isize));
        Self {
            ptr: bottom,
            prev_ptr: bottom,
            ptr_top: base.byte_offset(SPACE_4K as isize),
            bottom,
        }
    }

    pub fn len(&self) -> usize {
        (SPACE_4K - (self.bottom as usize & 0xFFF)) / std::mem::size_of::<T>()
    }

    /// Get the next element in the array
    pub fn next(&mut self) -> Option<T> {
        if self.ptr == self.ptr_top {
            return None;
        }
        unsafe {
            self.ptr_top = self.ptr_top.sub(1);
            Some(self.ptr_top.read())
        }
    }

    /// Get the next element in the array from the back
    pub fn next_back(&mut self) -> Option<T> {
        self.prev_ptr = self.ptr;
        if self.ptr == self.ptr_top {
            return None;
        }
        unsafe {
            let val = self.ptr.read();
            self.ptr = self.ptr.add(1);
            Some(val)
        }
    }

    /// Delete the element that was just read from next_back().
    /// 
    /// # Safety
    /// 
    /// next_back() needed to have been called and returned `Some(T)` or this
    /// will perform an invalid move.
    pub unsafe fn delete_back(self) {
        unsafe {
            core::ptr::copy(
                self.bottom,
                self.bottom.add(1),
                self.ptr.offset_from(self.bottom) as usize,
            )
        }
    }

    /// Replace the element taht was just read from next_back().
    /// 
    /// # Safety
    /// 
    /// next_back() needed to have been called and returned `Some(T)` or this
    /// will perform an invalid write.
    pub fn replace_back(self, val: T) {
        unsafe {
            self.prev_ptr.write(val);
        }
    }

    /// Insert a value into the array, pushing all later values up.
    /// This inserts *after* the value that was just read.
    ///
    /// # Safety
    ///
    /// The backing memory must have space for this array to grow down by one element.
    pub unsafe fn insert_back(self, val: T) {
        core::ptr::copy(
            self.bottom,
            self.bottom.sub(1),
            self.prev_ptr.offset_from(self.bottom) as usize,
        );
        self.prev_ptr.sub(1).write(val);
    }
}
