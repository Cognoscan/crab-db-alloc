use bytemuck::{AnyBitPattern, NoUninit, Zeroable};

use crate::Error;

use super::PageLayout;

#[derive(Zeroable, Clone, Copy, Default)]
pub struct LayoutU64U64 {
    key: u64,
}

unsafe impl NoUninit for LayoutU64U64 {}
unsafe impl AnyBitPattern for LayoutU64U64 {}

unsafe impl PageLayout for LayoutU64U64 {
    type Key = u64;
    type Value = u64;

    fn key_len(&self) -> usize {
        0
    }

    fn value_len(&self) -> usize {
        8
    }

    unsafe fn read_key<'a>(&'a self, _: &'a [u8]) -> &'a Self::Key {
        &self.key
    }

    unsafe fn read_value<'a>(&'a self, src: &'a [u8]) -> &'a Self::Value {
        unsafe { &*(src.as_ptr() as *const u64) }
    }

    fn determine_key_len(_: &Self::Key) -> Result<usize, Error> {
        Ok(0)
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

    unsafe fn write_key(&mut self, key: &Self::Key, _: &mut [u8]) {
        self.key = *key;
    }

    unsafe fn write_value(&mut self, val: &Self::Value, dst: &mut [u8]) {
        unsafe {
            (dst.as_mut_ptr() as *mut u64).write(*val);
        }
    }
}
