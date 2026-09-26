use super::*;

#[derive(Clone)]
#[repr(C)]
pub struct TwoArrayTrailer {
    /// lower array length (grows up from start of the page)
    lower_len: u16,
    /// upper array length (grows down from end, minus this trailer)
    upper_len: u16,
    unused0: u16,
    unused1: u8,
    /// The page type identifier
    pub page_type: u8,
}

impl core::fmt::Debug for TwoArrayTrailer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TwoArrayTrailer")
            .field("page_type", &self.page_type)
            .field("lower_len", &self.lower_len)
            .field("upper_len", &self.upper_len)
            .finish()
    }
}

#[derive(Clone, Debug)]
/// The lengths of the two arrays within a page.
pub struct TwoArrayLengths {
    /// lower array length, in elements
    lower: usize,
    /// upper array length, in elements
    upper: usize,
}

impl TwoArrayLengths {
    /// Get the number of elements in the upper array.
    pub fn upper<U>(&self) -> usize {
        self.upper / core::mem::size_of::<U>()
    }

    /// Get the total number of bytes.
    pub fn total(&self) -> usize {
        self.lower + self.upper
    }

    /// Get the number of bytes in the lower array.
    pub fn lower_bytes(&self) -> usize {
        self.lower
    }

    /// Get the number of bytes in the upper array.
    pub fn upper_bytes(&self) -> usize {
        self.upper
    }
}

impl TwoArrayTrailer {
    /// Extract the lengths of the fixed and variable portions, erroring if they
    /// are out of range or are invalid. The check ensures that pointers can be
    /// constructed from these lengths in combination with a pointer to the base
    /// of the page.
    #[inline]
    pub fn lengths(&self, space: usize) -> Result<TwoArrayLengths, Error> {
        let ret = unsafe { self.lengths_unchecked() };
        if ret.total() > space {
            return Err(Error::DataCorruption(
                "lengths are too large to fit within a page",
            ));
        }
        Ok(ret)
    }

    /// Extract the lengths of the fixed and variable portions, erroring if they
    /// are out of range or are invalid. This also checks if the lengths are
    /// valid for a given type layout. The check ensures that pointers can be
    /// constructed from these lengths in combination with a pointer to the base
    /// of the page.
    #[inline]
    pub fn lengths_layout<U>(&self, space: usize) -> Result<TwoArrayLengths, Error> {
        let ret = self.lengths(space)?;
        if ret.upper % core::mem::size_of::<U>() != 0 {
            return Err(Error::DataCorruption(
                "upper length is not a multiple of the layout type",
            ));
        }
        Ok(ret)
    }

    /// Extract the lengths of the fixed and variable portions, assuming they're
    /// in range and valid. This check should generally be performed at least
    /// once by using the checked version of this function call,
    /// [`lengths`](#method.lengths).
    ///
    /// # Safety
    ///
    /// This is technically safe to call, but see the above advice before doing
    /// anything with the result of this function.
    pub unsafe fn lengths_unchecked(&self) -> TwoArrayLengths {
        TwoArrayLengths {
            upper: self.upper_len as usize,
            lower: self.lower_len as usize,
        }
    }

    /// Set the upper length
    #[inline]
    pub fn set_upper_len(&mut self, len: u16) {
        debug_assert!(len <= 4088);
        self.upper_len = len;
    }

    /// Add to the upper length.
    ///
    /// # Safety
    ///
    /// The delta must not cause the length to over/underflow a `u16` value.
    #[inline]
    pub unsafe fn add_to_upper_len(&mut self, delta: isize) {
        let len = self.upper_len as isize + delta;
        debug_assert!(len <= 4088);
        self.upper_len = len as u16;
    }

    /// Set the lower length
    #[inline]
    pub fn set_lower_len(&mut self, len: u16) {
        debug_assert!(len <= 4088);
        self.lower_len = len;
    }

    /// Add to the lower length.
    ///
    /// # Safety
    ///
    /// The delta must not cause the length to over/underflow a `u16` value.
    #[inline]
    pub unsafe fn add_to_lower_len(&mut self, delta: isize) {
        let len = self.lower_len as isize + delta;
        debug_assert!(len <= 4088);
        self.lower_len = len as u16;
    }
}
