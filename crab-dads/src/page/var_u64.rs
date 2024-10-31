use crate::Error;

use super::{PageLayout, MAX_VAR_SIZE};

/// Page map layout for variable-length keys and U64 values.
///
/// The fixed-size information is a `u16` where the lower 4 bits encode the
/// length of the value, and the upper 12 bits encode the length of the key.
///
/// The value is encoded as a little-endian byte sequence and truncating any
/// trailing zero bytes. The 4 length bits in the info word are equal to 8 minus
/// the number of bytes written - if the bits are over 8 then they are clamped
/// to be 8.
///
/// The key is directly encoded, with the 12 bits of length info clamped to the
/// [maximum variable length](MAX_VAR_SIZE).
///
pub struct LayoutVarU64 {
    info: u16,
    key_len: usize,
    val_len: usize,
}

unsafe impl<'a> PageLayout<'a> for LayoutVarU64 {
    type Info = u16;
    type Key = &'a [u8];
    type Value = u64;

    fn from_info(info: Self::Info) -> Self {
        let key_len = ((info as usize & 0xFFF0) >> 4).min(MAX_VAR_SIZE);
        Self {
            info,
            key_len,
            val_len: 8 - ((info as usize) & 0xF).min(0x8),
        }
    }

    fn key_len(&self) -> usize {
        self.key_len
    }

    fn value_len(&self) -> usize {
        self.val_len
    }

    fn from_data(key: &Self::Key, value: &Self::Value) -> Result<Self, Error> {
        let key_len = key.len();
        if key_len > MAX_VAR_SIZE {
            return Err(Error::WriteTooLarge);
        }
        let val_bits = value.leading_zeros() as usize >> 3;
        let info = ((key_len << 4) | val_bits) as u16;
        Ok(Self {
            val_len: 8 - val_bits,
            key_len,
            info,
        })
    }

    fn info(&self) -> Self::Info {
        self.info
    }

    unsafe fn read_key(&self, src: &'a [u8]) -> Result<Self::Key, Error> {
        Ok(src)
    }

    unsafe fn read_value(&self, src: &'a [u8]) -> Result<Self::Value, Error> {
        if self.val_len == 0 {
            return Ok(0);
        }
        let val_mask = u64::MAX >> ((self.info & 0x7) << 3);
        unsafe { Ok(val_mask & (src.as_ptr() as *const u64).read_unaligned().to_le()) }
    }

    fn update_value(&mut self, new: &Self::Value) -> Result<isize, Error> {
        let val_bits = new.leading_zeros() as usize >> 3;
        let val_len = 8 - val_bits;
        let delta = (val_len as isize) - (self.val_len as isize);
        self.val_len = val_len;
        self.info = (self.info & 0xFFF) | (val_bits as u16);
        Ok(delta)
    }

    unsafe fn write_pair(&self, key: &Self::Key, value: &Self::Value, dest: &mut [u8]) {
        unsafe {
            // Copy the key over
            let key_ptr = dest.as_mut_ptr();
            let key: &[u8] = key;
            key_ptr.copy_from_nonoverlapping(key.as_ptr(), key.len());

            // Copy the value over
            if self.val_len == 0 {
                return;
            }
            let val_ptr = dest.as_mut_ptr().add(key.len()) as *mut u64;
            let val_mask = u64::MAX >> ((self.info & 0x7) << 3);
            let mut new_val = val_ptr.read_unaligned().to_le();
            new_val &= !val_mask;
            new_val |= value;
            val_ptr.write_unaligned(new_val.to_le());
        }
    }

    unsafe fn write_value(&self, value: &Self::Value, dest: &mut [u8]) {
        unsafe {
            if self.val_len == 0 {
                return;
            }
            let val_ptr = dest.as_mut_ptr() as *mut u64;
            let val_mask = u64::MAX >> ((self.info & 0x7) << 3);
            let mut new_val = val_ptr.read_unaligned().to_le();
            new_val &= !val_mask;
            new_val |= value;
            val_ptr.write_unaligned(new_val.to_le());
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate std;
    use std::prelude::rust_2021::*;
    use std::{vec, println};
    use super::*;

    #[test]
    fn empty_key_val_len() {
        for i in 0u16..16u16 {
            let info = LayoutVarU64::from_info(i);
            assert_eq!(info.val_len, 8 - ((i as usize).min(8)));
        }
    }


    #[test]
    fn golden_decode() {
        let test_vector: Vec<(u16, usize, usize, &[u8], u64)> = vec![
            (0x0000, 0, 8, &[], u64::MAX),
            (0x0001, 0, 7, &[], u64::MAX >> 8),
            (0x0002, 0, 6, &[], u64::MAX >> (2*8)),
            (0x0003, 0, 5, &[], u64::MAX >> (3*8)),
            (0x0004, 0, 4, &[], u64::MAX >> (4*8)),
            (0x0005, 0, 3, &[], u64::MAX >> (5*8)),
            (0x0006, 0, 2, &[], u64::MAX >> (6*8)),
            (0x0007, 0, 1, &[], u64::MAX >> (7*8)),
            (0x0008, 0, 0, &[], 0),
            (0x0009, 0, 0, &[], 0),
        ];

        let page = [0xFFu8; 4096];
        for test in test_vector {
            println!("Test = {:?}", test);
            let info = LayoutVarU64::from_info(test.0);
            assert_eq!(info.key_len(), test.1);
            assert_eq!(info.value_len(), test.2);
            unsafe {
                assert_eq!(info.read_key(&page[0..info.key_len()]), Ok(test.3));
                assert_eq!(info.read_value(&page[0..info.value_len()]), Ok(test.4));
            }
        }
    }

    #[test]
    fn golden_encode() {
        let mut page = [0xFFu8; 4096];
        #[allow(clippy::type_complexity)]
        let test_vector: Vec<(&[u8], u64, usize, usize, u16, &[u8])> = vec![
            (&[0xde,0xad], 0xefbe, 2, 2, 0x0026, &[0xde, 0xad, 0xbe, 0xef])
        ];

        for test in test_vector {
            let (key, val, key_len, val_len, info, enc) = test;
            let uut = LayoutVarU64::from_data(&key, &val).unwrap();
            assert_eq!(uut.info(), info);
            assert_eq!(uut.key_len(), key_len);
            assert_eq!(uut.value_len(), val_len);
            let enc_test = &mut page[0..(val_len+key_len)];
            unsafe {
                uut.write_pair(&key, &val, enc_test);
            }
            assert_eq!(enc, enc_test);
        }
    }
}
