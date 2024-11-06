use core::{iter::FusedIterator, marker::PhantomData};

use bytemuck::CheckedBitPattern;

use crate::Error;

/// An array of fixed-size values that grows downward in memory.
#[derive(Clone, Debug)]
pub struct RevSizedArray<'a, T: CheckedBitPattern>
where
    T: 'a,
{
    front: *const T,
    back: *const T,
    data: PhantomData<&'a [T]>,
}

impl<'a, T: CheckedBitPattern> RevSizedArray<'a, T> {
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
        unsafe { self.front.byte_offset_from(self.back) as usize }
    }
}

impl<'a, T: CheckedBitPattern> Iterator for RevSizedArray<'a, T> {
    type Item = Result<&'a T, Error>;

    fn next(&mut self) -> Option<Result<&'a T, Error>> {
        if self.front == self.back {
            return None;
        }
        unsafe {
            self.front = self.front.sub(1);
            if !T::is_valid_bit_pattern(&*(self.front as *const T::Bits)) {
                return Some(Err(Error::DataCorruption));
            }
            Some(Ok(&*(self.front as *const T)))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = unsafe { self.front.offset_from(self.back) as usize };
        (size, Some(size))
    }
}

impl<'a, T: CheckedBitPattern> FusedIterator for RevSizedArray<'a, T> {}

impl<'a, T: CheckedBitPattern> ExactSizeIterator for RevSizedArray<'a, T> {}

impl<'a, T: CheckedBitPattern> DoubleEndedIterator for RevSizedArray<'a, T> {
    fn next_back(&mut self) -> Option<Result<&'a T, Error>> {
        if self.front == self.back {
            return None;
        }
        unsafe {
            if !T::is_valid_bit_pattern(&*(self.back as *const T::Bits)) {
                return Some(Err(Error::DataCorruption));
            }
            let ret = &*(self.back as *const T);
            self.back = self.back.add(1);
            Some(Ok(ret))
        }
    }
}

/// An array of fixed-size values that grows downward in memory.
#[derive(Clone, Debug)]
pub struct RevSizedArrayMut<'a, T: CheckedBitPattern> {
    front: *mut T,
    back: *mut T,
    end: *mut T,
    prev_back: *mut T,
    data: PhantomData<&'a mut [T]>,
}

impl<'a, T: CheckedBitPattern> RevSizedArrayMut<'a, T> {
    /// Create a new iterator over a sized array of values that grow downwards.
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
        unsafe { self.front.byte_offset_from(self.back) as usize }
    }

    /// Delete the element that was last read from
    /// [`next_back`](#method.next_back).
    ///
    /// # Safety
    ///
    /// [`next_back`](#method.next_back) needs to have been called and returned
    /// `Some(Ok(T))`.
    pub unsafe fn back_delete(self) {
        unsafe {
            core::ptr::copy(
                self.end,
                self.end.add(1),
                self.prev_back.offset_from(self.end) as usize,
            );
        }
    }

    /// Get the element that was last read from
    /// [`next_back`](#method.next_back).
    ///
    /// # Safety
    ///
    /// [`next_back`](#method.next_back) needs to have been called and returned
    /// `Some(Ok(T))`.
    pub unsafe fn get(&self) -> &T {
        unsafe { &*(self.prev_back) }
    }

    /// Mutably get the element that was last read from
    /// [`next_back`](#method.next_back).
    ///
    /// # Safety
    ///
    /// [`next_back`](#method.next_back) needs to have been called and returned
    /// `Some(Ok(T))`.
    pub unsafe fn get_mut(&mut self) -> &mut T {
        unsafe { &mut *(self.prev_back) }
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
    pub unsafe fn back_insert(&mut self, val: T) {
        unsafe {
            core::ptr::copy(
                self.end,
                self.end.sub(1),
                self.prev_back.offset_from(self.end) as usize,
            );
            self.end = self.end.sub(1);
            self.prev_back = self.prev_back.sub(1);
            self.back = self.back.sub(1);
            self.prev_back.write(val);
        }
    }

    /// Get the next item in the array, from the back.
    pub fn next_back(&mut self) -> Option<Result<&mut T, Error>> {
        self.prev_back = self.back;
        if self.front == self.back {
            return None;
        }
        unsafe {
            let ret = &mut *self.back;
            if !T::is_valid_bit_pattern(&*(self.back as *const T::Bits)) {
                return Some(Err(Error::DataCorruption));
            }
            self.back = self.back.add(1);
            Some(Ok(ret))
        }
    }
}
