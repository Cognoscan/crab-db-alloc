use std::iter::FusedIterator;

use bytemuck::AnyBitPattern;

/// An array of fixed-size values that grows downward in memory.
pub struct RevSizedArray<T: AnyBitPattern> {
    front: *mut T,
    back: *mut T,
    end: *mut T,
    prev_back: *mut T,
}

impl<T: AnyBitPattern> RevSizedArray<T> {
    /// Create a new iterator over a sized array of values that grow downwards,
    /// given a pointer that is 1 byte past the top of the array.
    ///
    /// # Safety
    ///
    /// Behavior is defined only if the following conditions are met:
    ///
    /// - The `front` pointer must be within the accepted range of a memory
    ///   allocation.
    /// - The `front` pointer must point to 1 byte past the end of the array.
    /// - The `front` pointer must be aligned to the size of `T`.
    /// - When the length (which must be positive) is subtracted from `end`, the
    ///   resulting pointer should not go beyond the pointed-to memory.
    ///
    pub unsafe fn new(front: *mut T, len: isize) -> Self {
        let back = front.offset(-len);
        Self {
            front,
            back,
            end: back,
            prev_back: back,
        }
    }

    /// Delete the element that was last read from
    /// [`next_back`](#method.next_back).
    ///
    /// # Safety
    ///
    /// [`next_back`](#method.next_back) needs to have been called and returned
    /// `Some(T)`.
    pub unsafe fn back_delete(self) {
        unsafe {
            core::ptr::copy(
                self.end,
                self.end.add(1),
                self.prev_back.offset_from(self.end) as usize,
            );
        }
    }

    /// Delete all elements in the remaining iterator, including the ones just
    /// read with [`next`](#method.next) and [`next_back`](#method.next_back).
    /// This returns the number of elements deleted, which will always be at
    /// least one if the safety requirements are upheld.
    ///
    /// # Safety
    ///
    /// [`next`](#method.next) needs to have called until finding a cutpoint set
    /// to `Some(T)`, followed by calling [`next_back`](#method.next_back) until
    /// finding the cutpoint or hitting `None`.
    pub unsafe fn delete_range(self) -> isize {
        let len = self.front.offset_from(self.prev_back) + 1;
        core::ptr::copy(
            self.end,
            self.end.offset(len),
            self.prev_back.offset_from(self.end) as usize,
        );
        len
    }

    /// Replace the element that was just read from
    /// [`next_back`](#method.next_back).
    ///
    /// # Safety
    ///
    /// [`next_back`](#method.next_back) needs to have been called and returned
    /// `Some(T)`.
    pub unsafe fn back_replace(&mut self, val: T) {
        unsafe { self.prev_back.write(val) }
    }

    /// Replace the element that was just read from [`next`](#method.next).
    ///
    /// # Safety
    ///
    /// [`next`](#method.next) needs to have been called and returned `Some(T)`.
    pub unsafe fn replace(&mut self, val: T) {
        self.front.write(val);
    }

    /// Insert an element right after the element that was just read from
    /// [`next_back`](#method.next_back). If `None` was returned, the element
    /// will be inserted at the point in the array the iterator ended at (i.e.
    /// the front of the array, if [`next`](#method.next) was never called).
    ///
    /// # Safety
    ///
    /// The backing memory must have space for an additional element within its
    /// existing allocation, and [`next_back`](#method.next_back) must have been
    /// called at least once.
    pub unsafe fn back_insert(self, val: T) {
        core::ptr::copy(
            self.end,
            self.end.sub(1),
            self.prev_back.offset_from(self.end) as usize,
        );
        self.prev_back.sub(1).write(val);
    }
}

impl<T: AnyBitPattern> Iterator for RevSizedArray<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        if self.front == self.back {
            return None;
        }
        unsafe {
            self.front = self.front.sub(1);
            Some(self.front.read())
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = unsafe { self.front.offset_from(self.back) as usize };
        (size, Some(size))
    }
}

impl<T: AnyBitPattern> FusedIterator for RevSizedArray<T> {}

impl<T: AnyBitPattern> ExactSizeIterator for RevSizedArray<T> {}

impl<T: AnyBitPattern> DoubleEndedIterator for RevSizedArray<T> {
    fn next_back(&mut self) -> Option<T> {
        self.prev_back = self.back;
        if self.front == self.back {
            return None;
        }
        unsafe {
            let ret = self.back.read();
            self.back = self.back.add(1);
            Some(ret)
        }
    }
}
