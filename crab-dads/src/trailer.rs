use super::*;

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

/// The lengths of the two arrays within a page.
pub struct TwoArrayLengths {
    /// lower array length, in elements
    pub lower: isize,
    /// upper array length, in elements
    pub upper: isize,
}

impl TwoArrayLengths {

    /// Get the total number of bytes, given the element type of the lower array
    /// (`L`) and the upper array (`U`).
    pub fn total<L,U>(&self) -> isize {
        self.lower_bytes::<L>() + self.upper_bytes::<U>()
    }

    /// Get the number of bytes in the lower array, given its element type (`L`).
    pub fn lower_bytes<L>(&self) -> isize {
        self.lower * (std::mem::size_of::<L>() as isize)
    }

    /// Get the number of bytes in the upper array, given its element type (`U`).
    pub fn upper_bytes<U>(&self) -> isize {
        self.upper * (std::mem::size_of::<U>() as isize)
    }
}

impl TwoArrayTrailer {

    /// Extract the lengths of the fixed and variable portions, erroring if they
    /// are out of range or are invalid. The check ensures that pointers can be
    /// constructed from these lengths in combination with a pointer to the base
    /// of the page.
    #[inline]
    pub fn lengths<L,U,const PAGE_SPACE: usize>(&self) -> Result<TwoArrayLengths, PageError> {
        let ret = TwoArrayLengths {
            upper: self.upper_len.to_le() as isize,
            lower: self.lower_len.to_le() as isize,
        };
        if ret.total::<L,U>() > (PAGE_SPACE as isize) {
            return Err(PageError::DataCorruption);
        }
        Ok(ret)
    }

    /// Set the upper length
    #[inline]
    pub fn set_upper_len(&mut self, len: u16) {
        self.upper_len = len.to_le();
    }

    /// Set the lower length
    #[inline]
    pub fn set_lower_len(&mut self, len: u16) {
        self.lower_len = len.to_le();
    }
}
