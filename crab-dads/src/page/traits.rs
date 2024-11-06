
use bytemuck::{CheckedBitPattern, NoUninit};
use crate::Error;

/// Layout of key-value pairs for a page of memory.
///
/// # Safety
///
/// - The implementing struct must have an alignment of 8 bytes or fewer.
/// - The implementing struct must be endianess-agnostic: either by only having
///   single-byte content fields, or by performing endian conversions on read &
///   write of fields.
/// - The read operations cannot read more than 8 bytes beyond the end of the
///   provided source slices.
/// - `write_key` and `write_value` must work even if the current bit pattern is
///   incorrect.
pub unsafe trait PageLayout: NoUninit + CheckedBitPattern + Default {
    type Key: Ord + ?Sized;
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
    /// calling [`key_len`](#method.key_len), the 8 bytes after the end of
    /// the slice must also be valid to read from with the same pointer.
    unsafe fn update_value<'a>(&'a mut self, src: &'a mut [u8]) -> &'a mut Self::Value;

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
    unsafe fn write_key(&mut self, key: &Self::Key, dest: &mut [u8]);

    /// Write out a new value.
    ///
    /// # Safety
    /// 
    /// This must be called with a value that was already checked by
    /// `determine_value_len`, and the destination must have a size that is
    /// exactly equal to what the function returned.
    unsafe fn write_value(&mut self, val: &Self::Value, dest: &mut [u8]);

}