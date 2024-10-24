use crate::Error;

use super::{PageLayout, MAX_VAR_SIZE};

/// Page map layout for U64 keys and variable-length values, including support
/// for sub-trees and values too large for a page.
pub struct LayoutU64Var {
    info: u16,
    key_len: usize,
    val_len: usize,
    id: VarTypeList,
}

enum VarTypeList {
    Value,
    Ref,
    Page,
}

pub enum VarTypes<'a> {
    /// A page-local byte slice.
    Value(&'a [u8]),
    /// A reference to a large byte slice stored elsewhere.
    Ref(ExternalPage),
    /// A reference to a sub-database.
    Page(u64),
}

pub struct ExternalPage {
    page: u64,
    capacity: usize,
    used: usize,
}

impl ExternalPage {
    pub const ENCODED_SIZE: usize = 11;

    /// Load external page info.
    ///
    /// # Safety
    ///
    /// The pointer must point to memory that's at least 12 bytes in size.
    pub unsafe fn load(ptr: *const u8) -> Result<Self, Error> {
        let page_raw = (ptr as *const u64).read_unaligned().to_le();
        let used_raw = (ptr.byte_add(8) as *const u32).read_unaligned().to_le();
        let page = page_raw & 0xFFFF_FFFF_FFFF_F000;
        let capacity = ((page_raw & 0xFF) as usize + 1) << 12;
        let used = (used_raw as usize & 0x0FFFFF) + 1;
        if used > capacity {
            return Err(Error::DataCorruption);
        }

        Ok(Self {
            page,
            capacity,
            used,
        })
    }

    /// Store external page info.
    ///
    /// # Safety
    ///
    /// The pointer must point to memory that's at least 12 bytes in size.
    pub unsafe fn store(&self, ptr: *mut u8) {
        let page_raw = self.page | ((self.capacity as u64 >> 12) - 1);
        let used = self.used as u32 - 1;
        (ptr as *mut u64).write_unaligned(page_raw.to_le());
        (ptr.byte_add(8) as *mut u32).write_unaligned(used.to_le());
    }

    pub fn new(page: u64, capacity: usize, used: usize) -> Result<Self, Error> {
        if ((page & 0xFFF) != 0)
            || ((capacity & 0xFFF) != 0)
            || (capacity == 0)
            || (capacity > (256 << 12))
            || (used > capacity)
            || (used == 0)
        {
            return Err(Error::DataCorruption);
        }
        Ok(Self {
            page,
            capacity,
            used,
        })
    }

    /// # Safety
    ///
    /// `page` must be a 4kiB-aligned page, `capacity` must be in increments of
    /// 4kiB and be between 4 kiB and 1 MiB, and `used` must be nonzero and no
    /// more than the capacity.
    pub unsafe fn new_unchecked(page: u64, capacity: usize, used: usize) -> Self {
        Self {
            page,
            capacity,
            used,
        }
    }

    pub fn page(&self) -> u64 {
        self.page
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn used(&self) -> usize {
        self.used
    }
}

impl<'a> LayoutU64Var {
    fn parse_val(value: &VarTypes<'a>) -> Result<(VarTypeList, usize, u16), Error> {
        match value {
            VarTypes::Ref(_) => Ok((VarTypeList::Ref, ExternalPage::ENCODED_SIZE, 0x800)),
            VarTypes::Page(p) => {
                let bits = (p.leading_zeros() as u16 >> 3).min(7);
                let len = 8 - (bits as usize);
                Ok((VarTypeList::Page, len, bits | 0xC00))
            }
            VarTypes::Value(v) if v.len() <= MAX_VAR_SIZE => {
                Ok((VarTypeList::Value, v.len(), (v.len() as u16)))
            }
            VarTypes::Value(_) => Err(Error::WriteTooLarge),
        }
    }
}

unsafe impl<'a> PageLayout<'a> for LayoutU64Var {
    type Info = u16;
    type Key = u64;
    type Value = VarTypes<'a>;

    fn from_info(info: Self::Info) -> Self {
        let val_len = (info as usize & 0xFFF0) >> 4;
        let (id, val_len) = if val_len > MAX_VAR_SIZE {
            if (val_len & 0x0C00) == 0x0C00 {
                // Sub-tree
                (VarTypeList::Page, 8 - (val_len & 0x7))
            } else {
                // External page data
                (VarTypeList::Ref, ExternalPage::ENCODED_SIZE)
            }
        } else {
            // local refrence
            (VarTypeList::Value, val_len)
        };
        Self {
            info,
            key_len: 8 - (info as usize & 0x7),
            val_len,
            id,
        }
    }

    fn key_len(&self) -> usize {
        self.key_len
    }

    fn value_len(&self) -> usize {
        self.val_len
    }

    unsafe fn read_key(&self, src: &[u8]) -> Result<Self::Key, Error> {
        let key_mask = u64::MAX >> ((self.info & 0x7) << 3);
        unsafe { Ok(key_mask & (src.as_ptr() as *const u64).read_unaligned().to_le()) }
    }

    unsafe fn read_value(&self, src: &'a [u8]) -> Result<Self::Value, Error> {
        unsafe {
            Ok(match self.id {
                VarTypeList::Page => {
                    let mask = u64::MAX >> ((self.info & 0x70) >> 1);
                    let page = mask & (src.as_ptr() as *const u64).read_unaligned().to_le();
                    VarTypes::Page(page)
                }
                VarTypeList::Ref => VarTypes::Ref(ExternalPage::load(src.as_ptr())?),
                VarTypeList::Value => VarTypes::Value(src),
            })
        }
    }

    fn from_data(key: &Self::Key, value: &Self::Value) -> Result<Self, Error> {
        let key_bits = (key.leading_zeros() as u16 >> 3).min(7);
        let key_len = 8 - (key_bits as usize);
        let (id, val_len, val_bits) = Self::parse_val(value)?;
        let info = (val_bits << 4) | key_bits;
        Ok(Self {
            info,
            key_len,
            val_len,
            id,
        })
    }

    fn update_value(&mut self, new: &Self::Value) -> Result<isize, Error> {
        let (id, val_len, val_bits) = Self::parse_val(new)?;
        let delta = val_len as isize - (self.val_len as isize);
        self.id = id;
        self.val_len = val_len;
        self.info = (self.info & 7) | (val_bits << 4);
        Ok(delta)
    }

    unsafe fn write_value(&self, value: &Self::Value, dest: &mut [u8]) {
        unsafe {
            match value {
                VarTypes::Page(p) => {
                    // Write out the variable-length value
                    let mask = u64::MAX >> ((self.info & 0x70) >> 1);
                    let ptr = dest.as_mut_ptr() as *mut u64;
                    let mut new_val = ptr.read_unaligned().to_le();
                    new_val &= !mask;
                    new_val |= p;
                    ptr.write_unaligned(new_val.to_le());
                }
                VarTypes::Ref(r) => r.store(dest.as_mut_ptr()),
                VarTypes::Value(v) => dest
                    .as_mut_ptr()
                    .copy_from_nonoverlapping(v.as_ptr(), v.len()),
            }
        }
    }

    unsafe fn write_pair(&self, key: &Self::Key, value: &Self::Value, dest: &mut [u8]) {
        unsafe {
            // Copy the key over
            let key_mask = u64::MAX >> ((self.info & 7) << 3);
            let key_ptr = dest.as_mut_ptr() as *mut u64;
            let mut new_key = key_ptr.read_unaligned().to_le();
            new_key &= !key_mask;
            new_key |= key;
            key_ptr.write_unaligned(new_key.to_le());

            // Copy the value over
            let val_ptr = dest.as_mut_ptr().add(self.key_len);
            match value {
                VarTypes::Page(p) => {
                    // Write out the variable-length value
                    let mask = u64::MAX >> ((self.info & 0x70) >> 1);
                    let ptr = val_ptr as *mut u64;
                    let mut new_val = ptr.read_unaligned().to_le();
                    new_val &= !mask;
                    new_val |= p;
                    ptr.write_unaligned(new_val.to_le());
                }
                VarTypes::Ref(r) => r.store(val_ptr),
                VarTypes::Value(v) => {
                    let v: &[u8] = v;
                    val_ptr.copy_from_nonoverlapping(v.as_ptr(), v.len())
                },
            }
        }
    }

    fn info(&self) -> Self::Info {
        self.info
    }
}
