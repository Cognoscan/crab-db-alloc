use core::{marker::PhantomData, slice};

use crate::Error;

/// An array of variable-size key-value pairs that grows upward in memory.
#[derive(Clone, Debug)]
pub struct KeyValArray<'a> {
    // These pointers are ordered from lowest memory point to highest.
    front: *const u8,
    back: *const u8,
    data: PhantomData<&'a [u8]>,
}

impl<'a> KeyValArray<'a> {
    /// Create a new iterator over an array of variable-length key-value pairs.
    pub fn new(data: &[u8]) -> Self {
        let range = data.as_ptr_range();
        Self {
            front: range.start,
            back: range.end,
            data: PhantomData,
        }
    }

    /// Get how many bytes are still held inside this array
    pub fn remaining_bytes(&self) -> usize {
        unsafe { self.back.offset_from(self.front) as usize }
    }

    /// Try to increment the front to the next key-value pair, failing if the
    /// result pushes us past the end pointer. This returns the key and value as
    /// slices on success.
    pub fn next_pair(
        &mut self,
        key_size: usize,
        val_size: usize,
    ) -> Result<(&'a [u8], &'a [u8]), Error> {
        let val_ptr = self.front.wrapping_add(key_size);
        let new_front = val_ptr.wrapping_add(val_size);
        if new_front > self.back {
            return Err(Error::DataCorruption);
        }

        let ret = unsafe {
            (
                slice::from_raw_parts(self.front, key_size),
                slice::from_raw_parts(val_ptr, val_size),
            )
        };
        self.front = new_front;
        Ok(ret)
    }

    /// Try to decrement the end to the next key-value, failing if the result
    /// pushes us past the start pointer. This returns the key and value as
    /// slices on success.
    pub fn next_pair_back(
        &mut self,
        key_size: usize,
        val_size: usize,
    ) -> Result<(&'a [u8], &'a [u8]), Error> {
        let val_ptr = self.back.wrapping_sub(val_size);
        let new_back = val_ptr.wrapping_sub(key_size);
        if new_back < self.front {
            return Err(Error::DataCorruption);
        }

        let ret = unsafe {
            (
                slice::from_raw_parts(new_back, key_size),
                slice::from_raw_parts(val_ptr, val_size),
            )
        };
        self.back = new_back;
        Ok(ret)
    }

    /// Update the internal pointers to mimic the outcome of getting a `None`
    /// result from iterating. This returns an error if our iterator isn't
    /// actually exhausted.
    pub fn next_none(&mut self) -> Result<(), Error> {
        if self.back != self.front {
            return Err(Error::DataCorruption);
        }
        Ok(())
    }
}

/// Mutable value access to an array of variable-size key-value pairs that grows
/// upward in memory.
#[derive(Clone, Debug)]
pub struct KeyValArrayMut<'a> {
    front: *mut u8,
    back: *mut u8,
    data: PhantomData<&'a mut [u8]>
}

impl<'a> KeyValArrayMut<'a> {
    pub fn new(data: &mut [u8]) -> Self {
        let range = data.as_mut_ptr_range();
        Self {
            front: range.start,
            back: range.end,
            data: PhantomData
        }
    }

    /// Get how many bytes are still held inside this array
    pub fn remaining_bytes(&self) -> usize {
        unsafe { self.back.offset_from(self.front) as usize }
    }

    /// Try to increment the front to the next key-value pair, failing if the
    /// result pushes us past the end pointer. This returns the key and value as
    /// slices on success.
    pub fn next_pair(
        &mut self,
        key_size: usize,
        val_size: usize,
    ) -> Result<(&'a [u8], &'a mut [u8]), Error> {
        let val_ptr = self.front.wrapping_add(key_size);
        let new_front = val_ptr.wrapping_add(val_size);
        if new_front > self.back {
            return Err(Error::DataCorruption);
        }

        let ret = unsafe {
            (
                slice::from_raw_parts(self.front, key_size),
                slice::from_raw_parts_mut(val_ptr, val_size),
            )
        };
        self.front = new_front;
        Ok(ret)
    }

    /// Try to decrement the end to the next key-value, failing if the result
    /// pushes us past the start pointer. This returns the key and value as
    /// slices on success.
    pub fn next_pair_back(
        &mut self,
        key_size: usize,
        val_size: usize,
    ) -> Result<(&'a [u8], &'a mut [u8]), Error> {
        let val_ptr = self.back.wrapping_sub(val_size);
        let new_back = val_ptr.wrapping_sub(key_size);
        if new_back < self.front {
            return Err(Error::DataCorruption);
        }

        let ret = unsafe {
            (
                slice::from_raw_parts(new_back, key_size),
                slice::from_raw_parts_mut(val_ptr, val_size),
            )
        };
        self.back = new_back;
        Ok(ret)
    }

    /// Update the internal pointers to mimic the outcome of getting a `None`
    /// result from iterating. This returns an error if our iterator isn't
    /// actually exhausted.
    pub fn next_none(&mut self) -> Result<(), Error> {
        if self.back != self.front {
            return Err(Error::DataCorruption);
        }
        Ok(())
    }


}

/// A mutable, resizable array of variable-size key-value pairs that grows
/// upward in memory.
#[derive(Clone, Debug)]
pub struct KeyValArrayMutResize<'a> {
    // These pointers are ordered from lowest memory point to highest.
    front: *mut u8,
    back: *mut u8,
    back_val: *mut u8,
    prev_back_key: *mut u8,
    end: *mut u8,
    data: PhantomData<&'a mut [u8]>,
}

impl<'a> KeyValArrayMutResize<'a> {
    /// Create a new iterator over an array of variable-length key-value pairs.
    pub fn new(data: &mut [u8]) -> Self {
        let range = data.as_mut_ptr_range();
        Self {
            front: range.start,
            back: range.end,
            end: range.end,
            back_val: range.end,
            prev_back_key: range.end,
            data: PhantomData,
        }
    }

    /// Get how many bytes are still held inside this array
    pub fn remaining_bytes(&self) -> usize {
        unsafe { self.back.offset_from(self.front) as usize }
    }

    /// Try to decrement the end to the next key-value, failing if the result
    /// pushes us past the start pointer.
    pub fn next_pair_back<'s>(&'s mut self, key_size: usize, val_size: usize) -> Result<(), Error> {
        self.prev_back_key = self.back;
        let val_ptr = self.back.wrapping_sub(val_size);
        let new_back = val_ptr.wrapping_sub(key_size);
        if new_back < self.front {
            return Err(Error::DataCorruption);
        }

        self.back = new_back;
        self.back_val = val_ptr;
        Ok(())
    }

    /// Update the internal pointers to mimic the outcome of getting a `None`
    /// result from calling [`next_pair_back`](#method.next_pair_back) on
    /// iteration. This returns an error if our iterator isn't actually exhausted.
    pub fn next_pair_back_none(&mut self) -> Result<(), Error> {
        if self.back != self.front {
            return Err(Error::DataCorruption);
        }
        self.prev_back_key = self.back;
        self.back_val = self.back;
        Ok(())
    }

    /// Insert a pair right after the pair that was just read with the aid of
    /// [`next_pair_back`](#method.next_pair_back). If the pair was updated to
    /// be none with [`next_pair_back_none`](#method.next_pair_back_none), the
    /// pair will be inserted at the point in the array the iterator ended at
    /// (i.e. the front of the array).
    ///
    /// On completion, this struct points to the new pair.
    ///
    /// # Safety
    ///
    /// The backing memory must have space for the additional pair within its
    /// existing allocation.
    pub unsafe fn back_insert<'s>(&'s mut self, key_size: usize, val_size: usize) {
        unsafe {
            let pair_size = key_size + val_size;
            // Make room
            core::ptr::copy(
                self.prev_back_key,
                self.prev_back_key.add(pair_size),
                self.end.offset_from(self.prev_back_key) as usize,
            );
            // Update our pointers, moving ourself up to the newly inserted value.
            self.end = self.end.add(pair_size);
            self.back = self.prev_back_key;
            self.back_val = self.prev_back_key.add(key_size);
        }
    }

    /// Access the most recent key.
    pub fn key(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.back, self.back_val.offset_from(self.back) as usize) }
    }

    /// Mutably access the most recent key.
    pub fn key_mut(&mut self) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(self.back, self.back_val.offset_from(self.back) as usize)
        }
    }

    /// Access the most recent value.
    pub fn val(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(
                self.back_val,
                self.prev_back_key.offset_from(self.back_val) as usize,
            )
        }
    }

    /// Mutably access the most recent value.
    pub fn val_mut(&mut self) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(
                self.back_val,
                self.prev_back_key.offset_from(self.back_val) as usize,
            )
        }
    }

    /// Delete the most recent key-value pair, returning the number of bytes
    /// deleted. Afterwards, this struct will be pointing to a zero-sized key
    /// and zero-sized value.
    pub fn delete(&mut self) -> isize {
        unsafe {
            let len = self.prev_back_key.offset_from(self.back);
            core::ptr::copy(
                self.prev_back_key,
                self.back,
                self.end.offset_from(self.prev_back_key) as usize,
            );
            self.prev_back_key = self.back;
            self.back_val = self.back;
            len
        }
    }

    /// Resize the value.
    ///
    /// # Safety
    ///
    /// If increasing in size, the backing memory must have sufficient space.
    pub unsafe fn resize(&mut self, delta: isize) {
        unsafe {
            // Calculate the new location for the neighboring key
            let new_prev_back_key = self.prev_back_key.offset(delta);
            // Shift the trailing data up/down
            core::ptr::copy(
                self.prev_back_key,
                new_prev_back_key,
                self.end.offset_from(self.prev_back_key) as usize,
            );
            // Update ourself
            self.end = self.end.offset(delta);
            self.prev_back_key = new_prev_back_key;
        }
    }
}
