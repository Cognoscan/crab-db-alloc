use crate::Error;

/// An array of variable-size key-value pairs that grows upward in memory.
#[derive(Clone, Debug)]
pub struct KeyValArray {
    // These pointers are ordered from lowest memory point to highest.
    prev_front: *mut u8,
    front_val: *mut u8,
    front: *mut u8,
    back: *mut u8,
    back_val: *mut u8,
    prev_back_key: *mut u8,
    end: *mut u8,
}

impl KeyValArray {
    /// Create a new iterator over an array of variable-length key-value pairs.
    ///
    /// # Safety
    ///
    /// Behavior is defined only if the following conditions are met:
    /// - The `front` pointer must point to valid memory
    /// - The length must be a positive offset to the pointer that does not
    ///   result in a pointer going beyond the pointed-to memory.
    ///
    pub unsafe fn new(front: *mut u8, len: isize) -> Self {
        let back = front.offset(len);
        Self {
            front,
            front_val: front,
            back,
            end: back,
            prev_front: front,
            prev_back_key: back,
            back_val: back,
        }
    }

    /// Get how many bytes are still held inside this array
    pub fn remaining_bytes(&self) -> usize {
        unsafe {
            self.back.offset_from(self.front) as usize
        }
    }

    /// Try to increment the front to the next key-value pair, failing if the
    /// result pushes us past the end pointer. This also returns a pointer to
    /// the value on success.
    pub fn next_pair(&mut self, key_size: usize, val_size: usize) -> Result<*mut u8, Error> {
        let val_ptr = self.front.wrapping_add(key_size);
        let new_front = val_ptr.wrapping_add(val_size);
        if new_front > self.back {
            return Err(Error::DataCorruption);
        }
        self.front_val = val_ptr;
        self.front = new_front;
        Ok(val_ptr)
    }

    /// Try to decrement the end to the next key-value, failing if the result
    /// pushes us past the start pointer. This also returns a pointer to the
    /// value on success.
    pub fn next_pair_back(
        &mut self,
        key_size: usize,
        val_size: usize,
    ) -> Result<*mut u8, Error> {
        self.prev_back_key = self.back;
        let val_ptr = self.back.wrapping_sub(val_size);
        let new_back = val_ptr.wrapping_sub(key_size);
        if new_back < self.front {
            return Err(Error::DataCorruption);
        }
        self.back_val = val_ptr;
        self.back = new_back;
        Ok(val_ptr)
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

    /// Pointer to the key at the front of the array
    pub fn front_ptr(&self) -> *mut u8 {
        self.front
    }

    /// Pointer to the key at the back of the array (invalid until the first
    /// [`next_pair_back`](#method.next_pair_back) call).
    pub fn back_ptr(&self) -> *mut u8 {
        self.back
    }

    /// Delete the key-value pair that was last read with the aid of
    /// [`next_pair_back`](#method.next_pair_back), returning the number of
    /// bytes deleted.
    ///
    /// # Safety
    ///
    /// [`next_pair_back`](#method.next_pair_back) needs to have been called at
    /// least once, and it cannot have returned an error on any call.
    pub unsafe fn delete_back(self) -> isize {
        let len = self.prev_back_key.offset_from(self.back);
        core::ptr::copy(
            self.prev_back_key,
            self.back,
            self.end.offset_from(self.prev_back_key) as usize,
        );
        len
    }

    /// Delete all pairs in the remaining iterator, including the ones just read
    /// with the aid of [`next_pair`](#method.next_pair) and
    /// [`next_pair_back`](#method.next_pair_back). This returns the number of
    /// bytes deleted, which will always be at least 0.
    ///
    /// # Safety
    ///
    /// [`next_pair`](#method.next_pair) needs to have been called until finding
    /// a cutpoint set to `Some(T)`, followed by calling
    /// [`next_pair_back`](#method.next_pair_back) until finding the cutpoint or
    /// exhausting the pairs stored in the array.
    pub unsafe fn delete_range(self) -> isize {
        let len = self.prev_back_key.offset_from(self.prev_front);
        core::ptr::copy(
            self.prev_back_key,
            self.prev_front,
            self.end.offset_from(self.prev_back_key) as usize,
        );
        len
    }

    /// Resize the key-value pair that was just read using
    /// [`next_pair_back`](#method.next_pair_back), returning a pointer to it.
    /// 
    /// # Safety
    /// 
    /// [`next_pair_back`](#method.next_pair_back) needs to have been called and
    /// cannot have returned an error,
    /// [`next_pair_back_none`](#method.next_pair_back_none) cannot have been
    /// called, and the backing memory must have space if the new size is larger than the previous one.
    pub unsafe fn back_resize(&mut self, delta: isize) -> *mut u8 {
        let new_prev_back_key = self.prev_back_key.offset(delta);
        core::ptr::copy(
            self.prev_back_key,
            new_prev_back_key,
            self.end.offset_from(self.prev_back_key) as usize,
        );
        self.end = self.end.offset(delta);
        self.prev_back_key = new_prev_back_key;
        self.back_val
    }

    /// Resize the value that was just read using
    /// [`next_pair`](#method.next_pair), returning a pointer to it.
    /// 
    /// # Safety
    /// 
    /// [`next_pair`](#method.next_pair) needs to have been called and cannot
    /// have returned an error, and the backing memory must have space if the
    /// new size is larger than the previous one.
    pub unsafe fn resize(&mut self, delta: isize) -> *mut u8 {
        let new_front = self.front.offset(delta);
        core::ptr::copy(
            self.front,
            new_front,
            self.end.offset_from(self.front) as usize,
        );
        // We need to shift *all* of these pointers when doing this resize.
        // If we're only doing forward iteration though, the other pointers
        // won't get used and will be dropped by the compiler.
        self.front = new_front;
        self.back = self.back.offset(delta);
        self.back_val = self.back_val.offset(delta);
        self.prev_back_key = self.prev_back_key.offset(delta);
        self.end = self.end.offset(delta);
        self.front_val
    }

    /// Insert a pair right after the pair that was just read with the aid of
    /// [`next_pair_back`](#method.next_pair_back). If the pair was updated to
    /// be none with [`next_pair_back_none`](#method.next_pair_back_none), the
    /// pair will be inserted at the point in the array the iterator ended at
    /// (i.e. the front of the array, if forward iteration never occured).
    /// 
    /// # Safety
    /// 
    /// The backing memory must have space for the additional pair within its
    /// existing allocation.
    pub unsafe fn back_insert(self, pair_size: usize) -> *mut u8 {
        core::ptr::copy(
            self.prev_back_key,
            self.prev_back_key.add(pair_size),
            self.end.offset_from(self.prev_back_key) as usize,
        );
        self.prev_back_key
    }
}
