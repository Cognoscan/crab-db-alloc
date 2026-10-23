use bytemuck::{CheckedBitPattern, NoUninit, Zeroable};

use crate::Error;

use super::{PageLayout, MAX_VAR_SIZE};

/// A Layout storing variable-length keys and u64 values. Keys are always 8 byte
/// aligned.
/// 
/// The layout data is the length of the key. The stored keys are always 8 byte
/// aligned, which requires rounding the length up to a multiple of 8 when
/// reporting the key length. This is required in order for the `u64` values
/// to be properly aligned.
#[repr(C)]
#[derive(Zeroable, Clone, Copy, Default)]
pub struct LayoutVarU64 {
    len: u16,
}

unsafe impl NoUninit for LayoutVarU64 {}

unsafe impl CheckedBitPattern for LayoutVarU64 {
    type Bits = u16;
    fn is_valid_bit_pattern(bits: &Self::Bits) -> bool {
        *bits <= (MAX_VAR_SIZE as u16)
    }
}

unsafe impl PageLayout for LayoutVarU64 {
    type Key = [u8];
    type Value = u64;

    fn key_len(&self) -> usize {
        ((self.len + 7) & !7) as usize
    }

    fn value_len(&self) -> usize {
        8
    }

    unsafe fn read_key<'a>(&'a self, src: &'a [u8]) -> &'a Self::Key {
        unsafe { src.get_unchecked(0..(self.len as usize)) }
    }

    unsafe fn read_value<'a>(&'a self, src: &'a [u8]) -> &'a Self::Value {
        unsafe { &*(src.as_ptr() as *const u64) }
    }

    fn determine_key_len(key: &Self::Key) -> Result<usize, Error> {
        if key.len() > MAX_VAR_SIZE {
            return Err(Error::WriteTooLarge);
        }
        Ok((key.len() + 7) & !7)
    }

    fn determine_value_len(_: &Self::Value) -> Result<usize, Error> {
        Ok(8)
    }

    unsafe fn update_value<'a>(
        &'a self,
        src: &'a mut [u8],
    ) -> &'a mut Self::Value {
        unsafe { &mut *(src.as_mut_ptr() as *mut u64) }
    }

    unsafe fn write_key(&mut self, key: &Self::Key, dst: &mut [u8]) {
        unsafe {
            self.len = key.len() as u16;
            core::ptr::copy_nonoverlapping(key.as_ptr(), dst.as_mut_ptr(), key.len());
        }
    }

    unsafe fn write_value(&mut self, val: &Self::Value, dst: &mut [u8]) {
        unsafe {
            (dst.as_mut_ptr() as *mut u64).write(*val);
        }
    }

}
