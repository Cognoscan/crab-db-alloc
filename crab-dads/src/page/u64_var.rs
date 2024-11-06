use bytemuck::{CheckedBitPattern, NoUninit, Zeroable};

use crate::Error;

use super::PageLayout;

#[repr(C)]
#[derive(Zeroable, Clone, Copy)]
pub struct LayoutU64Var {
    len: u8,
}

unsafe impl NoUninit for LayoutU64Var {}

unsafe impl CheckedBitPattern for LayoutU64Var {
    type Bits = u8;
    fn is_valid_bit_pattern(bits: &Self::Bits) -> bool {
        *bits <= 126
    }
}

unsafe impl PageLayout for LayoutU64Var {
    type Key = u64;
    type Value = [u8];

    fn from_data(&mut self, _: &Self::Key, value: &Self::Value) -> Result<(), Error> {
        let len = (value.len() + 7) / 8;
        if len > 126 { return Err(Error::WriteTooLarge); }
        self.len = len as u8;
        Ok(())
    }

    fn key_len(&self) -> usize {
        8
    }

    fn value_len(&self) -> usize {
        self.len as usize * 8
    }

    unsafe fn read_key<'a>(&'a self, src: &'a [u8]) -> Result<&'a Self::Key, Error> {
        unsafe {
            Ok(&*(src.as_ptr() as *const u64))
        }
    }

    unsafe fn read_value<'a>(&'a self, src: &'a [u8]) -> Result<&'a Self::Value, Error>
    {
        Ok(src)
    }

    unsafe fn resize_value(&self, new_size: usize) -> Result<isize, Error> {
        let len = (new_size + 7) / 8;
        if len > 126 { return Err(Error::WriteTooLarge); }
        let delta = (len as isize - self.len as isize) * 8;
        self.len = len as u8;
        Ok(delta)
    }

    unsafe fn complete_resize(&mut self, new_size: usize) -> Result<(), Error> {
        let len = (new_size + 7) / 8;
        if len > 126 { return Err(Error::WriteTooLarge); }
        let delta = (len as isize - self.len as isize) * 8;
        self.len = len as u8;
        Ok(())
    }

    unsafe fn update_value<'a>(&'a mut self, src: &'a mut [u8]) -> Result<&'a mut Self::Value, Error> {
        Ok(src)
    }

    unsafe fn write_key(&mut self, key: &Self::Key, dest: &mut [u8]) {
        unsafe {
            (dest.as_mut_ptr() as *mut u64).write(*key);
        }
    }

}
