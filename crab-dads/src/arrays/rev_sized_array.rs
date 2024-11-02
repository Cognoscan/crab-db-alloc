use core::{iter::FusedIterator, marker::PhantomData};

use bytemuck::AnyBitPattern;

/// An array of fixed-size values that grows downward in memory.
#[derive(Clone, Debug)]
pub struct RevSizedArray<'a, T: AnyBitPattern> {
    front: *const T,
    back: *const T,
    data: PhantomData<&'a [T]>
}

impl<'a, T: AnyBitPattern> RevSizedArray<'a, T> {
    pub fn new(data: &[T]) -> Self {
        let range = data.as_ptr_range();
        Self {
            front: range.end,
            back: range.start,
            data: PhantomData,
        }
    }

    /// Get how many remaining bytes are in the array.
    pub fn remaining_bytes(&self) -> usize {
        unsafe {
            self.front.byte_offset_from(self.back) as usize
        }
    }

}

impl<'a, T: AnyBitPattern> Iterator for RevSizedArray<'a, T> {
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

impl<'a, T: AnyBitPattern> FusedIterator for RevSizedArray<'a, T> {}

impl<'a, T: AnyBitPattern> ExactSizeIterator for RevSizedArray<'a, T> {}

impl<'a, T: AnyBitPattern> DoubleEndedIterator for RevSizedArray<'a, T> {
    fn next_back(&mut self) -> Option<T> {
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


/// An array of fixed-size values that grows downward in memory.
#[derive(Clone, Debug)]
pub struct RevSizedArrayMut<'a, T: AnyBitPattern> {
    front: *mut T,
    back: *mut T,
    end: *mut T,
    prev_back: *mut T,
    data: PhantomData<&'a mut [T] >
}

impl<'a, T: AnyBitPattern> RevSizedArrayMut<'a, T> {
    /// Create a new iterator over a sized array of values that grow downwards,
    pub fn new(data: &mut [T]) -> Self {
        let range = data.as_mut_ptr_range();
        Self {
            front: range.end,
            back: range.start,
            end: range.start,
            prev_back: range.start,
            data: PhantomData,
        }
    }

    /// Get how many remaining bytes are in the array.
    pub fn remaining_bytes(&self) -> usize {
        unsafe {
            self.front.byte_offset_from(self.back) as usize
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

    /// Get the next item in the array, from the back.
    pub fn next_back(&mut self) -> Option<T> {
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
