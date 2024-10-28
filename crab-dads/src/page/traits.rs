
use bytemuck::AnyBitPattern;
use crate::Error;

/// Implement for any struct that needs to be endian-swapped when read or written.
pub trait EndianSwappable {
    fn endian_swap(self) -> Self;
}

impl EndianSwappable for u8 {
    fn endian_swap(self) -> Self {
        self
    }
}

impl EndianSwappable for u16 {
    fn endian_swap(self) -> Self {
        self.to_le()
    }
}

impl EndianSwappable for u32 {
    fn endian_swap(self) -> Self {
        self.to_le()
    }
}

impl EndianSwappable for u64 {
    fn endian_swap(self) -> Self {
        self.to_le()
    }
}

/// Layout of key-value pairs for a page of memory.
///
/// # Safety
///
/// - `Info` must have an alignment of 8 bytes or fewer
/// - The read operations cannot read more than 8 bytes beyond the end of the
///   provided source slices.
pub unsafe trait PageLayout<'a>: Sized {
    type Info: AnyBitPattern + EndianSwappable + Clone + Copy;
    type Key: Ord + 'a;
    type Value: 'a;

    fn from_info(info: Self::Info) -> Self;

    fn key_len(&self) -> usize;
    fn value_len(&self) -> usize;

    /// Read the key out of a source slice.
    /// 
    /// # Safety
    /// 
    /// The caller must ensure that `src` is exactly the length specified by
    /// calling [`key_len`](#method.key_len), and the 8 bytes after the end of
    /// the slice must also be valid to read from with the same pointer.
    unsafe fn read_key(&self, src: &'a [u8]) -> Result<Self::Key, Error>;

    /// Read the value out of a source slice.
    /// 
    /// # Safety
    /// 
    /// The caller must ensure that `src` is exactly the length specified by
    /// calling [`value_len`](#method.value_len), and the 8 bytes after the end
    /// of the slice must also be valid to read from with the same pointer.
    unsafe fn read_value(&self, src: &'a [u8]) -> Result<Self::Value, Error>;

    /// Update with a new value, returning the change in how many bytes are
    /// required to store the new value.
    fn update_value(&mut self, new: &Self::Value) -> Result<isize, Error>;

    /// Create a new layout meant to hold a provided key-value pair.
    fn from_data(key: &Self::Key, value: &Self::Value) -> Result<Self, Error>;

    /// Write an updated value to the provided destination slice.
    /// 
    /// # Safety
    /// 
    /// The caller must ensure that `dest` is exactly the length specified by
    /// calling [`value_len`](#method.value_len), and the value must have been
    /// previously part of the construction of this layout.
    /// 
    /// If `from_info` was used to construct this, then the value must
    /// match the one retrieved with `read_value`. If `update_value` has been
    /// called after that, that value should be used instead.
    /// 
    /// If `from_value` was used to construct this, then the value used
    /// for that must also be used here.
    unsafe fn write_value(&self, value: &Self::Value, dest: &mut [u8]);

    /// Write the key-value pair to the provided destination slice.
    /// 
    /// # Safety
    /// 
    /// The caller must ensure that `dest` is exactly the sum of the lengths
    /// specified by calling [`key_len`](#method.key_len) and
    /// [`value_len`](#method.value_len), and both key and value must have been
    /// previously part of the construction of this layout.
    /// 
    /// If `from_info` was used to construct this, then the key and value must
    /// match the ones retrieved with `read_key` and `read_value`. If
    /// `update_value` has been called after that, that value should be used
    /// instead.
    /// 
    /// If `from_value` was used to construct this, then the key and value used
    /// for that must also be used here.
    unsafe fn write_pair(&self, key: &Self::Key, value: &Self::Value, dest: &mut [u8]);

    /// Return the info data needed to recreate this layout.
    fn info(&self) -> Self::Info;
}
