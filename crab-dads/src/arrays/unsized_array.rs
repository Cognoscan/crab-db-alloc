use crate::Error;

/// An array of variable-size values that grows upward in memory.
pub struct UnsizedArray {
    prev_front: *mut u8,
    front: *mut u8,
    back: *mut u8,
    prev_back: *mut u8,
    end: *mut u8,
}

impl UnsizedArray {
    /// Create a new iterator over an array of variable-length values.
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
            back,
            end: back,
            prev_front: front,
            prev_back: back,
        }
    }

    pub fn next(&mut self, size: usize) -> Result<(), Error> {
        let new_front = self.front.wrapping_add(size);
        if new_front > self.back {
            return Err(Error::DataCorruption);
        }
        self.front = new_front;
        Ok(())
    }

    pub fn next_back(&mut self, size: usize) -> Result<(), Error> {
        let new_back = self.back.wrapping_sub(size);
        if new_back < self.front {
            return Err(Error::DataCorruption);
        }
        self.back = new_back;
        Ok(())
    }

    /// Update the internal pointers to mimic the outcome of getting a `None`
    /// result from calling [`next_back`](#method.next_back) on iteration.
    pub fn next_back_none(&mut self) {
        self.prev_back = self.back;
    }

    /// Pointer to the value at the front of the array
    pub fn front_ptr(&self) -> *mut u8 {
        self.front
    }

    /// Pointer to the value at the back of the array (invalid until the first
    /// [`next_back`](#method.next_back) call).
    pub fn back_ptr(&self) -> *mut u8 {
        self.back
    }

    /// Delete the value that was last read with the aid of
    /// [`next_back`](#method.next_back), returning the number of bytes deleted.
    ///
    /// # Safety
    ///
    /// [`next_back`](#method.next_back) needs to have been called at least
    /// once, and it cannot have returned an error on any call.
    pub unsafe fn delete_back(self) -> isize {
        let len = self.prev_back.offset_from(self.back);
        core::ptr::copy(
            self.prev_back,
            self.back,
            self.end.offset_from(self.prev_back) as usize,
        );
        len
    }

    /// Delete all values in the remaining iterator, including the ones just
    /// read with the aid of [`next`](#method.next) and
    /// [`next_back`](#method.next_back). This returns the number of bytes
    /// deleted, which will always be at least 0.
    ///
    /// # Safety
    ///
    /// [`next`](#method.next) needs to have been called until finding a
    /// cutpoint set to `Some(T)`, followed by calling
    /// [`next_back`](#method.next_back) until finding the cutpoint or
    /// exhausting the values stored in the array.
    pub unsafe fn delete_range(self) -> isize {
        let len = self.prev_back.offset_from(self.prev_front);
        core::ptr::copy(
            self.prev_back,
            self.prev_front,
            self.end.offset_from(self.prev_back) as usize,
        );
        len
    }

    /// Resize the value that was just read using
    /// [`next_back`](#method.next_back), returning a pointer to it.
    /// 
    /// # Safety
    /// 
    /// [`next_back`](#method.next_back) needs to have been called and cannot
    /// have returned an error, [`next_back_none`](#method.next_back_none)
    /// cannot have been called, and the backing memory must have space if the
    /// new size is larger than the previous one.
    pub unsafe fn back_resize(&mut self, delta: isize) {
        let new_prev_back = self.prev_back.offset(delta);
        core::ptr::copy(
            self.prev_back,
            new_prev_back,
            self.end.offset_from(self.prev_back) as usize,
        );
        self.end = self.end.offset(delta);
        self.prev_back = new_prev_back;
    }

    /// Resize the value that was just read using [`next`](#method.next),
    /// returning a pointer to it.
    /// 
    /// # Safety
    /// 
    /// [`next`](#method.next) needs to have been called and cannot have
    /// returned an error, and the backing memory must have space if the new
    /// size is larger than the previous one.
    pub unsafe fn resize(&mut self, delta: isize) {
        let new_front = self.front.offset(delta);
        core::ptr::copy(
            self.front,
            new_front,
            self.end.offset_from(self.front) as usize,
        );
        self.front = new_front;
        self.back = self.back.offset(delta);
        self.prev_back = self.prev_back.offset(delta);
        self.end = self.end.offset(delta);
    }

    /// Insert a value right after the value that was just read with the aid of
    /// [`next_back`](#method.next_back). If the value was updated to be none
    /// with [`next_back_none`](#method.next_back_none), the value will be
    /// inserted at the point in the array the iterator ended at (i.e. the front
    /// of the array, if forward iteration never occured).
    /// 
    /// # Safety
    /// 
    /// The backing memory must have space for the additional value within its
    /// existing allocation.
    pub unsafe fn back_insert(self, size: usize) -> *mut u8 {
        core::ptr::copy(
            self.prev_back,
            self.prev_back.add(size),
            self.end.offset_from(self.prev_back) as usize,
        );
        self.prev_back
    }





}