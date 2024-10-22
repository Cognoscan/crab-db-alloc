
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
/// `Info` must have an alignment of 8 bytes or fewer, and the read operations
/// cannot read more than 8 bytes beyond the end of the provided source slices.
pub unsafe trait PageLayout<'a>: Sized {
    type Info: AnyBitPattern + EndianSwappable;
    type Key: Ord + 'a;
    type Value: 'a;

    fn from_info(info: Self::Info) -> Self;

    fn key_len(&self) -> usize;
    fn value_len(&self) -> usize;

    fn read_key(&self, src: &'a [u8]) -> Result<Self::Key, Error>;
    fn read_value(&self, src: &'a [u8]) -> Result<Self::Value, Error>;

    /// Update with a new value, returning the change in how many bytes are
    /// required to store the new value.
    fn update_value(&mut self, new: &Self::Value) -> Result<isize, Error>;

    /// Create a new layout meant to hold a provided key-value pair.
    fn from_data(key: &Self::Key, value: &Self::Value) -> Result<Self, Error>;

    /// Write an updated value to the provided destination slice, which is
    /// guaranteed to always be exactly large enough to hold the desired value.
    fn write_value(&self, value: &Self::Value, dest: &mut [u8]);

    /// Write the key-value pair to the provided destination slice, which is
    /// guaranteed to always be exactly large enough to hold the desired key &
    /// value.
    fn write_pair(&self, key: &Self::Key, value: &Self::Value, dest: &mut [u8]);

    /// Return the info data needed to recreate this layout.
    fn info(&self) -> Self::Info;
}
