use std::marker::PhantomData;

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
    pub fn next_pair(&mut self, key_size: usize, val_size: usize) -> Result<(&'a [u8], &'a [u8]), Error> {
        let val_ptr = self.front.wrapping_add(key_size);
        let new_front = val_ptr.wrapping_add(val_size);
        if new_front > self.back {
            return Err(Error::DataCorruption);
        }

        let ret = unsafe {
            (
                core::slice::from_raw_parts(self.front, key_size),
                core::slice::from_raw_parts(val_ptr, val_size),
            )
        };
        self.front = new_front;
        Ok(ret)
    }

    /// Try to decrement the end to the next key-value, failing if the result
    /// pushes us past the start pointer. This returns the key and value as
    /// slices on success.
    pub fn next_pair_back(&mut self, key_size: usize, val_size: usize) -> Result<(&'a [u8], &'a [u8]), Error> {
        let val_ptr = self.back.wrapping_sub(val_size);
        let new_back = val_ptr.wrapping_sub(key_size);
        if new_back < self.front {
            return Err(Error::DataCorruption);
        }

        let ret = unsafe {
            (
                core::slice::from_raw_parts(new_back, key_size),
                core::slice::from_raw_parts(val_ptr, val_size)
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

/// An array of variable-size key-value pairs that grows upward in memory.
#[derive(Clone, Debug)]
pub struct KeyValArrayMut<'a> {
    // These pointers are ordered from lowest memory point to highest.
    front: *mut u8,
    back: *mut u8,
    back_val: *mut u8,
    prev_back_key: *mut u8,
    end: *mut u8,
    data: PhantomData<&'a mut [u8]>,
}

impl<'a> KeyValArrayMut<'a> {
    /// Create a new iterator over an array of variable-length key-value pairs.
    ///
    /// # Safety
    ///
    /// Behavior is defined only if the following conditions are met:
    /// - The `front` pointer must point to valid memory
    /// - The length must be a positive offset to the pointer that does not
    ///   result in a pointer going beyond the pointed-to memory.
    ///
    pub unsafe fn new(data: &mut [u8]) -> Self {
        let range = data.as_mut_ptr_range();
        Self {
            front: range.start,
            back: range.end,
            end: range.end,
            prev_back_key: range.end,
            back_val: range.end,
            data: PhantomData,
        }
    }

    /// Get how many bytes are still held inside this array
    pub fn remaining_bytes(&self) -> usize {
        unsafe { self.back.offset_from(self.front) as usize }
    }

    /// Try to decrement the end to the next key-value, failing if the result
    /// pushes us past the start pointer. This returns the key and value as
    /// slices on success.
    pub fn next_pair_back(&mut self, key_size: usize, val_size: usize) -> Result<(&'a [u8], &'a [u8]), Error> {
        self.prev_back_key = self.back;
        let val_ptr = self.back.wrapping_sub(val_size);
        let new_back = val_ptr.wrapping_sub(key_size);
        if new_back < self.front {
            return Err(Error::DataCorruption);
        }

        let ret = unsafe {
            (
                core::slice::from_raw_parts(new_back, key_size),
                core::slice::from_raw_parts(val_ptr, val_size)
            )
        };
        self.back_val = val_ptr;
        self.back = new_back;
        Ok(ret)
    }

    /// Update the internal pointers to mimic the outcome of getting a `None`
    /// result from calling [`next_pair_back`](#method.next_pair_back) on
    /// iteration. This returns an error if our iterator isn't actually exhausted.
    pub fn next_pair_back_none(&mut self) -> Result<(), Error> {
        if self.back != self.front {
            return Err(Error::DataCorruption);
        }
        self.prev_back_key = self.back;
        Ok(())
    }

    /// Delete the key-value pair that was last read with the aid of
    /// [`next_pair_back`](#method.next_pair_back), returning the number of
    /// bytes deleted.
    ///
    /// # Safety
    ///
    /// [`next_pair_back`](#method.next_pair_back) needs to have been called at
    /// least once, and it cannot have returned an error on any call.
    pub unsafe fn delete_back(self) -> usize {
        let len = self.prev_back_key.offset_from(self.back) as usize;
        core::ptr::copy(
            self.prev_back_key,
            self.back,
            self.end.offset_from(self.prev_back_key) as usize,
        );
        len
    }

    /// Resize the value that was just read using
    /// [`next_pair_back`](#method.next_pair_back), returning a pointer to it.
    ///
    /// # Safety
    ///
    /// [`next_pair_back`](#method.next_pair_back) needs to have been called and
    /// cannot have returned an error,
    /// [`next_pair_back_none`](#method.next_pair_back_none) cannot have been
    /// called, and the backing memory must have space if the new size is larger than the previous one.
    pub unsafe fn back_resize(&mut self, delta: isize) -> &mut [u8] {
        // Calculate the new location and size of the value
        let new_prev_back_key = self.prev_back_key.offset(delta);
        let len = new_prev_back_key.offset_from(self.back_val) as usize;
        // Shift the trailing data up/down
        core::ptr::copy(
            self.prev_back_key,
            new_prev_back_key,
            self.end.offset_from(self.prev_back_key) as usize,
        );
        // Update ourself
        self.end = self.end.offset(delta);
        self.prev_back_key = new_prev_back_key;
        core::slice::from_raw_parts_mut(self.back_val, len)
    }

    /// Insert a pair right after the pair that was just read with the aid of
    /// [`next_pair_back`](#method.next_pair_back). If the pair was updated to
    /// be none with [`next_pair_back_none`](#method.next_pair_back_none), the
    /// pair will be inserted at the point in the array the iterator ended at
    /// (i.e. the front of the array).
    ///
    /// This returns a slice of uninitialized memory for use by the pair.
    ///
    /// # Safety
    ///
    /// The backing memory must have space for the additional pair within its
    /// existing allocation.
    pub unsafe fn back_insert(&mut self, pair_size: usize) -> &mut [u8] {
        core::ptr::copy(
            self.prev_back_key,
            self.prev_back_key.add(pair_size),
            self.end.offset_from(self.prev_back_key) as usize,
        );
        self.end = self.end.add(pair_size);
        core::slice::from_raw_parts_mut(self.prev_back_key, pair_size)
    }
}
