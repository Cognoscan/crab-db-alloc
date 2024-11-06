use bytemuck::{AnyBitPattern, NoUninit, Zeroable};

use crate::Error;

use super::PageLayout;

#[derive(Zeroable, Clone, Copy, Default)]
pub struct LayoutU64U64;

unsafe impl NoUninit for LayoutU64U64 {}
unsafe impl AnyBitPattern for LayoutU64U64 {}

unsafe impl PageLayout for LayoutU64U64 {
    type Key = u64;
    type Value = u64;

    fn key_len(&self) -> usize {
        8
    }

    fn value_len(&self) -> usize {
        8
    }

    unsafe fn read_key<'a>(&'a self, src: &'a [u8]) -> Result<&'a Self::Key, Error> {
        unsafe { Ok(&*(src.as_ptr() as *const u64)) }
    }

    unsafe fn read_value<'a>(&'a self, src: &'a [u8]) -> Result<&'a Self::Value, Error> {
        unsafe { Ok(&*(src.as_ptr() as *const u64)) }
    }

    fn determine_key_len(_: &Self::Key) -> Result<usize, Error> {
        Ok(8)
    }

    fn determine_value_len(_: &Self::Value) -> Result<usize, Error> {
        Ok(8)
    }

    unsafe fn update_value<'a>(
        &'a mut self,
        src: &'a mut [u8],
    ) -> Result<&'a mut Self::Value, Error> {
        unsafe { Ok(&mut *(src.as_mut_ptr() as *mut u64)) }
    }

    unsafe fn write_key(&mut self, key: &Self::Key, dest: &mut [u8]) {
        unsafe {
            (dest.as_mut_ptr() as *mut u64).write(*key);
        }
    }

    unsafe fn write_value(&mut self, val: &Self::Value, dest: &mut [u8]) {
        unsafe {
            (dest.as_mut_ptr() as *mut u64).write(*val);
        }
    }
}
