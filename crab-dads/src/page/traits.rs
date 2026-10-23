
use bytemuck::{CheckedBitPattern, NoUninit};
use crate::Error;

/// Layout of key-value pairs for a page of memory.
/// 
/// Layouts are used to operate on a continguous byte array of key-value pairs.
/// They can also occupy space, but must not require an alignment larger than 8
/// bytes. A layout is used to indicate the space occupied by keys & values,
/// to indicate what space is required to store a new key or value, and to
/// actually write a key or value into memory. It must also provide mutable
/// access to the value, once it is stored.
///
/// # Safety
///
/// - The implementing struct must have an alignment of 8 bytes or fewer.
/// - The read operations cannot read more than 8 bytes beyond the end of the
///   provided source slices.
/// - `write_key` and `write_value` must work even if the current bit pattern is
///   incorrect.
/// 
/// The implementor can assume the key and value data are laid out sequentially
/// in memory, starting from a 4 kiB aligned pointer. Data will be packed, which
/// lets an implementor control alignment of the key & value data. For instance,
/// if stored key data is always 8 bytes and stored value data is always a
/// multiple of 8 bytes, then both values and keys will always be 8 byte
/// aligned.
pub unsafe trait PageLayout: NoUninit + CheckedBitPattern + Default {
    type Key: Ord + core::fmt::Debug + ?Sized;
    type Value: ?Sized;

    /// The size of the variable-length portion of the current key.
    fn key_len(&self) -> usize;

    /// The size of the variable-length portion of the current value.
    fn value_len(&self) -> usize;

    /// Read the key out of a source slice.
    /// 
    /// # Safety
    /// 
    /// The caller must ensure that `src` is exactly the length specified by
    /// calling [`key_len`](#method.key_len), and the 8 bytes after the end of
    /// the slice must also be valid to read from with the same pointer.
    unsafe fn read_key<'a>(&'a self, src: &'a [u8]) -> &'a Self::Key;

    /// Read the value out of a source slice.
    /// 
    /// # Safety
    /// 
    /// The caller must ensure that `src` is exactly the length specified by
    /// calling [`value_len`](#method.value_len), and the 8 bytes after the end
    /// of the slice must also be valid to read from with the same pointer.
    unsafe fn read_value<'a>(&'a self, src: &'a [u8]) -> &'a Self::Value;

    /// Get mutable access to the value.
    /// 
    /// # Safety
    /// 
    /// The caller must ensure that `src` is exactly the length specified by
    /// calling [`value_len`](#method.value_len), the 8 bytes after the end of
    /// the slice must also be valid to read from with the same pointer.
    unsafe fn update_value<'a>(&'a self, src: &'a mut [u8]) -> &'a mut Self::Value;

    /// Determine how many bytes are needed to store a given key.
    fn determine_key_len(key: &Self::Key) -> Result<usize, Error>;

    /// Determine how many bytes are needed to store a given value.
    fn determine_value_len(value: &Self::Value) -> Result<usize, Error>;

    /// Write out a new key.
    ///
    /// # Safety
    /// 
    /// This must be called with a key that was already checked by
    /// `determine_key_len`, and the destination must have a size that is
    /// exactly equal to what the function returned.
    unsafe fn write_key(&mut self, key: &Self::Key, dst: &mut [u8]);

    /// Write out a new value.
    ///
    /// # Safety
    /// 
    /// This must be called with a value that was already checked by
    /// `determine_value_len`, and the destination must have a size that is
    /// exactly equal to what the function returned.
    unsafe fn write_value(&mut self, val: &Self::Value, dst: &mut [u8]);

}

/// An extension trait to [`PageLayout`], supporting vectored writes.
/// 
/// Vectored writes are where a slice of values are written, with some way of
/// combining them such that a single value is stored. Usually the value is some
/// kind of slice itself, such that a slice of values becomes concatenated into
/// a single longer slice.
pub trait PageLayoutVectored: PageLayout {

    /// Determine how many bytes are needed to store a slice of values.
    fn determine_value_len_vectored(value: &[&Self::Value]) -> Result<usize, Error>;

    /// Write out a new single value from a slice of values.
    /// 
    /// # Safety
    /// 
    /// This must be called with a slice of values that was already checked by
    /// `determine_value_len_vectored`, and the destination must have a size
    /// that is exactly equal to what the function returned.
    unsafe fn write_value_vectored(&mut self, val: &[&Self::Value], dst: &mut [u8]);
}