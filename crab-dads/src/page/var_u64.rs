use bytemuck::{CheckedBitPattern, NoUninit, Zeroable};

use crate::Error;

use super::PageLayout;

#[derive(Zeroable, Clone, Copy)]
pub struct LayoutVarU64(u16);

unsafe impl NoUninit for LayoutVarU64 {}

unsafe impl CheckedBitPattern for LayoutVarU64 {
    type Bits = u16;
    fn is_valid_bit_pattern(bits: &Self::Bits) -> bool {
        *bits <= 1008
    }
}

unsafe impl PageLayout for LayoutVarU64 {
    type Key = [u8];
    type Value = u64;

    fn from_data(&mut self, key: &Self::Key, _: &Self::Value) -> Result<(), Error> {
        if key.len() > 1008 {
            return Err(Error::WriteTooLarge);
        }
        self.0 = key.len() as u16;
        Ok(())
    }

    fn key_len(&self) -> usize {
        ((self.0 + 7) & 0xFFF8) as usize
    }

    fn value_len(&self) -> usize {
        8
    }

    unsafe fn read_key<'a>(&'a self, src: &'a [u8]) -> Result<&'a Self::Key, Error> {
        unsafe { Ok(src.get_unchecked(..(self.0 as usize))) }
    }

    unsafe fn read_value<'a>(&'a self, src: &'a [u8]) -> Result<&'a Self::Value, Error> {
        unsafe { Ok(&*(src.as_ptr() as *const u64)) }
    }

    unsafe fn resize_value(&mut self, _: usize) -> Result<isize, Error> {
        Err(Error::IncorrectOperation)
    }

    unsafe fn update_value<'a>(
        &'a mut self,
        src: &'a mut [u8],
    ) -> Result<&'a mut Self::Value, Error> {
        unsafe { Ok(&mut *(src.as_mut_ptr() as *mut u64)) }
    }

    unsafe fn write_key(&mut self, key: &Self::Key, dest: &mut [u8]) {
        unsafe {
            core::ptr::copy_nonoverlapping(key.as_ptr(), dest.as_mut_ptr(), key.len());
        }
    }
}
