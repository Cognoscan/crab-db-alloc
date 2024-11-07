use bytemuck::{CheckedBitPattern, NoUninit, Zeroable};

use crate::Error;

use super::{PageLayout, MAX_VAR_SIZE};

#[repr(C)]
#[derive(Zeroable, Clone, Copy, Default)]
pub struct LayoutU64Var {
    len: u16,
}

unsafe impl NoUninit for LayoutU64Var {}

unsafe impl CheckedBitPattern for LayoutU64Var {
    type Bits = u16;
    fn is_valid_bit_pattern(bits: &Self::Bits) -> bool {
        *bits <= (MAX_VAR_SIZE as u16)
    }
}

unsafe impl PageLayout for LayoutU64Var {
    type Key = u64;
    type Value = [u8];

    fn key_len(&self) -> usize {
        8
    }

    fn value_len(&self) -> usize {
        ((self.len + 7) / 8) as usize
    }

    unsafe fn read_key<'a>(&'a self, src: &'a [u8]) -> &'a Self::Key {
        unsafe { &*(src.as_ptr() as *const u64) }
    }

    unsafe fn read_value<'a>(&'a self, src: &'a [u8]) -> &'a Self::Value {
        unsafe { src.get_unchecked(0..(self.len as usize)) }
    }

    fn determine_key_len(_: &Self::Key) -> Result<usize, Error> {
        Ok(8)
    }

    fn determine_value_len(value: &Self::Value) -> Result<usize, Error> {
        if value.len() > MAX_VAR_SIZE {
            return Err(Error::WriteTooLarge);
        }
        Ok((value.len() + 7) / 8)
    }

    unsafe fn write_value(&mut self, val: &Self::Value, dest: &mut [u8]) {
        unsafe {
            self.len = val.len() as u16;
            core::ptr::copy_nonoverlapping(val.as_ptr(), dest.as_mut_ptr(), val.len());
        }
    }

    unsafe fn update_value<'a>(
        &'a self,
        src: &'a mut [u8],
    ) -> &'a mut Self::Value {
        unsafe { src.get_unchecked_mut(0..(self.len as usize)) }
    }

    unsafe fn write_key(&mut self, key: &Self::Key, dest: &mut [u8]) {
        unsafe {
            (dest.as_mut_ptr() as *mut u64).write(*key);
        }
    }
}
