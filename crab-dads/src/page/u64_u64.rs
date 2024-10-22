use crate::Error;

use super::PageLayout;

/// Helper struct for working with the variable-length encoding
pub struct LayoutU64U64 {
    info: u8,
    key_len: usize,
    val_len: usize,
    key_mask: u64,
    val_mask: u64,
}

unsafe impl<'a> PageLayout<'a> for LayoutU64U64 {
    type Info = u8;
    type Key = u64;
    type Value = u64;

    fn from_info(info: u8) -> Self {
        let val_bits = info & 0x78;
        let (val_len, val_mask) = if val_bits >= 0x10 {
            (0, 0)
        } else {
            (8 - (val_bits as usize), u64::MAX >> val_bits)
        };
        Self {
            info,
            val_len,
            val_mask,
            key_len: 8 - (info & 0x7) as usize,
            key_mask: u64::MAX >> ((info & 0x7) << 3),
        }
    }

    fn from_data(key: &u64, val: &u64) -> Result<Self, Error> {
        let key_bits = (key.leading_zeros() as usize >> 3).min(0x7);
        let val_bits = val.leading_zeros() as usize >> 3;
        let key_len = 8 - key_bits;
        let val_len = 8 - val_bits;
        let val_bits_shifted = val_bits << 3;
        let info = (key_bits | val_bits_shifted) as u8;
        let key_mask = u64::MAX >> (key_bits << 3);
        let val_mask = if val_bits >= 0x10 {
            0
        } else {
            u64::MAX >> val_bits_shifted
        };
        Ok(Self {
            info,
            key_len,
            val_len,
            key_mask,
            val_mask,
        })
    }

    fn info(&self) -> Self::Info {
        self.info
    }

    fn key_len(&self) -> usize {
        self.key_len
    }

    fn value_len(&self) -> usize {
        self.val_len
    }

    fn read_key(&self, src: &'a [u8]) -> Result<Self::Key, Error> {
        unsafe { Ok(self.key_mask & (src.as_ptr() as *const u64).read_unaligned().to_le()) }
    }

    fn read_value(&self, src: &'a [u8]) -> Result<Self::Value, Error> {
        unsafe { Ok(self.val_mask & (src.as_ptr() as *const u64).read_unaligned().to_le()) }
    }

    fn update_value(&mut self, new: &Self::Value) -> Result<isize, Error> {
        let val_bits = new.leading_zeros() as usize >> 3;
        let val_bits_shifted = val_bits << 3;
        let val_len = 8 - val_bits;
        let delta = val_len as isize - (self.val_len as isize);
        self.info = val_bits_shifted as u8 | (self.info & 7);
        self.val_mask = if val_bits >= 0x10 {
            0
        } else {
            u64::MAX >> val_bits_shifted
        };
        Ok(delta)
    }

    fn write_pair(&self, key: &Self::Key, value: &Self::Value, dest: &mut [u8]) {
        // We first fetch the old data using unaligned reads, as we may be
        // modifying less than 8 bytes on the key/value writes. We can fetch
        // both at the same time, so long as we write the key back first and the
        // value back second.
        unsafe {
            let ptr = dest.as_mut_ptr() as *mut u64;

            // Fetch the old data
            let mut new_key = ptr.read_unaligned().to_le();
            let val_ptr = ptr.byte_add(self.key_len);
            let mut new_val = ptr.read_unaligned().to_le();

            // Write in the new key
            new_key &= !self.key_mask;
            new_key |= key;
            ptr.write_unaligned(new_key.to_le());

            // Write in the new value
            new_val &= !self.val_mask;
            new_val |= value;
            val_ptr.write_unaligned(new_val.to_le());
        }
    }

    fn write_value(&self, value: &Self::Value, dest: &mut [u8]) {
        unsafe {
            let ptr = (dest.as_mut_ptr() as *mut u64).byte_add(self.key_len);
            let mut new_val = ptr.read_unaligned().to_le();
            new_val &= !self.val_mask;
            new_val |= value;
            ptr.write_unaligned(new_val.to_le());
        }
    }
}
