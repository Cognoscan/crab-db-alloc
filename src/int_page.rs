use std::{cmp::Ordering, fmt, iter::FusedIterator};

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
#[error("No space left in page to insert")]
pub struct OutofSpace;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PageError {
    /// Page wasn't aligned to a 4 kiB page boundary
    #[error("Page wasn't aligned to a 4 kiB page boundary")]
    Misaligned,
    /// Page size wasn't 4 kiB
    #[error("Page size wasn't 4 kiB")]
    Not4kiB(usize),
    /// Page data corrupted
    #[error("Page data corrupted")]
    DataCorruption,
}

/// Put the header in a submodule to stop us accidentally not using the accessor functions
mod h {
    #[repr(C)]
    pub struct Header {
        len: u16,
        end: u16,
        unused: u8,
        pub page_type: u8,
    }

    impl Header {
        /// Get the length, forcing it to be valid
        #[inline]
        pub fn len(&self) -> u16 {
            self.len.to_le() & 0xFFF
        }

        /// Get the end point, forcing it to be valid
        #[inline]
        pub fn end(&self) -> u16 {
            self.end.to_le() & 0xFFF
        }

        /// Set the length
        #[inline]
        pub fn set_len(&mut self, len: u16) {
            self.len = len.to_le();
        }

        /// Set the end
        #[inline]
        pub fn set_end(&mut self, end: u16) {
            self.end = end.to_le();
        }
    }
}

use h::Header;

const HEADER_SIZE: usize = std::mem::size_of::<Header>();
const PAGE_SIZE: usize = 4096;
const HEADER_OFFSET: usize = PAGE_SIZE - HEADER_SIZE;

/// A single-page `BTreeMap<u64,u64>`.
///
/// # Layout
///
/// Data in the page is stored contiguously from the start and grows upwards. At the end of the page
/// is a header, and below the header is a byte sequence holding the length of each variable-length
/// data item. The length sequence grows downwards. The data consists of a pair of
/// variable-length-encoded `u64` values, with the first one being the key and the second being the
/// value. Keys are stored in-order.
///
/// The header is a 6-byte structure at the end of the page, consisting of:
/// - 4090:4091 - the number of items in the page. Only the lower 12 bits are used.
/// - 4092:4093 - the offset to the end of the data section. Only the lower 12 bits are used.
/// - 4094 - spare byte
/// - 4095 - page type
///
/// The length sequence consists of a series of bytes where the bits are:
/// - 7 - unused
/// - 6:3 - 8 minus the number of bytes in the value (0-8) - values above 8 are clamped.
/// - 2:0 - 8 minus the number of bytes in the key (1-8)
///
/// The values are just little-endian u64 numbers, but with any upper zero bytes discarded. The keys
/// are the same, but are always at least one byte long.
///
///
pub struct IntPage {
    mem: *mut u8,
}

impl fmt::Debug for IntPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let header = self.header();
        f.debug_struct("IntPage")
            .field("type", &header.page_type)
            .field("len", &header.len())
            .field("end", &header.end())
            .field("data", &self.iter())
            .finish()
    }
}

impl IntPage {
    /// Initialize a page without checking for correct alignment or 4 kiB size.
    ///
    /// # Safety
    ///
    /// The page must be aligned to a 4 kiB boundary and 4 kiB in size.
    pub unsafe fn new(mem: *mut u8, page_type: u8) -> Self {
        let mut ret = Self { mem };
        let header = ret.header_mut();
        header.page_type = page_type;
        header.set_end(0);
        header.set_len(0);
        ret
    }

    /// Load a page without checking for correct alignment or 4 kiB size. It does still
    /// perform a basic sanity check of the header. For complete validation before use, call
    /// [`Self::validate`] after construction.
    ///
    /// # Safety
    ///
    /// The page must be aligned to a 4 kiB boundary and 4 kiB in size.
    pub unsafe fn load(mem: *mut u8) -> Result<Self, PageError> {
        let ret = Self { mem };
        let header = ret.header();
        if (header.len() + header.end() + (HEADER_SIZE as u16)) > 4096 {
            return Err(PageError::DataCorruption);
        }
        Ok(ret)
    }

    #[inline]
    fn header(&self) -> &Header {
        unsafe { &*(self.mem.add(HEADER_OFFSET) as *const Header) }
    }

    #[inline]
    fn header_mut(&mut self) -> &mut Header {
        unsafe { &mut *(self.mem.add(HEADER_OFFSET) as *mut Header) }
    }

    /// Iterate over the key-value pairs.
    pub fn iter(&self) -> IntPageIter {
        let header = self.header();
        // Safety: on creation, we verified the end & len values are collectively within the page
        // boundaries, so these offsets should be safe.
        unsafe {
            IntPageIter {
                data_ptr: self.mem,
                data_end: self.mem.add(header.end() as usize),
                item_ptr: self.mem.add(HEADER_OFFSET - 1),
                item_end: self.mem.add(HEADER_OFFSET - 1 - (header.len() as usize)),
                data: std::marker::PhantomData,
            }
        }
    }

    /// Validate the data within the page.
    pub fn validate(&self) -> Result<(), PageError> {
        // Extract header info
        let header = self.header();
        let len = header.len();
        let end = header.end();

        // If we're 0, perform checks without iterating
        if len == 0 {
            if end != 0 {
                return Err(PageError::DataCorruption);
            }
            return Ok(());
        }

        // Verify we can completely iterate over the data and the keys are in order.
        let mut iter = self.iter();
        let Some((mut prev_key, _)) = iter.next() else {
            return Err(PageError::DataCorruption);
        };
        for (k, _) in &mut iter {
            if prev_key >= k {
                return Err(PageError::DataCorruption);
            }
            prev_key = k;
        }
        if iter.data_end != iter.data_ptr {
            return Err(PageError::DataCorruption);
        }
        if iter.item_end != iter.item_ptr {
            return Err(PageError::DataCorruption);
        }
        Ok(())
    }

    /// Get a key-value pair from the page
    pub fn get(&self, key: u64) -> Option<u64> {
        for (k, v) in self.iter() {
            match k.cmp(&key) {
                Ordering::Greater => return None,
                Ordering::Equal => return Some(v),
                Ordering::Less => (),
            }
        }
        None
    }

    /// Get the free space in this page
    pub fn available(&self) -> usize {
        let header = self.header();
        HEADER_OFFSET - (header.end() as usize) - (header.len() as usize)
    }

    /// Get an entry for a key-value pair
    pub fn entry(&mut self, key: u64) -> Entry {
        let mut iter = self.iter();
        let mut prev_data_end = iter.data_end;
        while let Some((k, v)) = iter.next_back() {
            match k.cmp(&key) {
                Ordering::Equal => {
                    return Entry::Occupied(OccupiedEntry {
                        key,
                        val: v,
                        insert_data: iter.data_end as *mut u8,
                        insert_item: iter.item_end as *mut u8,
                        next_data: prev_data_end as *mut u8,
                        page: self,
                    });
                }
                Ordering::Greater => (),
                Ordering::Less => {
                    return Entry::Vacant(VacantEntry {
                        key,
                        insert_data: prev_data_end as *mut u8,
                        insert_item: unsafe { iter.item_end.offset(-1) } as *mut u8,
                        page: self,
                    })
                }
            }
            prev_data_end = iter.data_end;
        }
        Entry::Vacant(VacantEntry {
            key,
            insert_data: iter.data_end as *mut u8,
            insert_item: iter.item_end as *mut u8,
            page: self,
        })
    }

    /// Insert a value into the page, returning any old one that was present
    pub fn insert(&mut self, key: u64, val: u64) -> Result<Option<u64>, OutofSpace> {
        self.entry(key).insert(val)
    }

    /// Remove a key from the page, returning any old value, if one was present.
    pub fn remove(&mut self, key: u64) -> Option<u64> {
        match self.entry(key) {
            Entry::Occupied(e) => Some(e.remove()),
            Entry::Vacant(_) => None,
        }
    }
}

/// Iterate over the page, returning key-value pairs.
#[derive(Clone)]
pub struct IntPageIter<'a> {
    data_ptr: *const u8,
    data_end: *const u8,
    item_ptr: *const u8,
    item_end: *const u8,
    data: std::marker::PhantomData<&'a u8>,
}

impl<'a> fmt::Debug for IntPageIter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.clone()).finish()
    }
}

impl<'a> Iterator for IntPageIter<'a> {
    type Item = (u64, u64);
    fn next(&mut self) -> Option<Self::Item> {
        // Safety:
        // Our pointers are guaranteed to be within the 4 kiB page when we start, and it's
        // impossible to advance the data pointer past (4 kiB - 8 bytes), so it'll always read in a
        // valid range.
        // Before we read from the pointers, we always make sure they haven't crossed over.
        //
        unsafe {
            if self.item_end == self.item_ptr {
                return None;
            }

            // Get the length
            let len: u8 = *self.item_ptr;
            self.item_ptr = self.item_ptr.offset(-1);

            // Get the key and move the pointer
            let key_len = len & 0x7;
            if self.data_ptr >= self.data_end {
                return None;
            }
            let key_mask = u64::MAX >> (key_len << 3);
            let key: u64 = (self.data_ptr as *const u64).read_unaligned() & key_mask;
            self.data_ptr = self.data_ptr.offset((0x8 - key_len) as isize);

            // Get the value and move the pointer
            let val_len = len & 0x78;
            let val = if val_len >= 0x40 {
                0
            } else {
                if self.data_ptr >= self.data_end {
                    return None;
                }
                let val_mask = u64::MAX >> val_len;
                let val = (self.data_ptr as *const u64).read_unaligned() & val_mask;
                self.data_ptr = self.data_ptr.offset(((0x40 - val_len) >> 3) as isize);
                val
            };

            Some((key, val))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some((self.item_ptr as usize) - (self.item_end as usize)))
    }
}

impl<'a> DoubleEndedIterator for IntPageIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        unsafe {
            if self.item_end == self.item_ptr {
                return None;
            }

            // get the length
            self.item_end = self.item_end.offset(1);
            let len: u8 = *self.item_end;

            // Move the pointer to the value and extract it
            let val_len = len & 0x78;
            let val = if val_len >= 0x40 {
                0
            } else {
                self.data_end = self.data_end.offset(((0x40 - val_len) >> 3) as isize);
                if self.data_end < self.data_ptr {
                    return None;
                }
                let val_mask = u64::MAX >> val_len;
                (self.data_end as *const u64).read_unaligned() & val_mask
            };

            // Move the pointer to the key and extract it
            let key_len = len & 0x7;
            self.data_end = self.data_end.offset((0x8 - key_len) as isize);
            if self.data_end < self.data_ptr {
                return None;
            }
            let key_mask = u64::MAX >> (key_len << 3);
            let key: u64 = (self.data_end as *const u64).read_unaligned() & key_mask;

            Some((key, val))
        }
    }
}

/// The iterator will always return None once it completes.
impl<'a> FusedIterator for IntPageIter<'a> {}

pub enum Entry<'a> {
    Occupied(OccupiedEntry<'a>),
    Vacant(VacantEntry<'a>),
}

impl<'a> Entry<'a> {
    /// Get the key for this entry
    pub fn key(&self) -> u64 {
        match self {
            Self::Occupied(e) => e.key(),
            Self::Vacant(e) => e.key(),
        }
    }

    /// Try to insert a value into the page, returning any old value that was present. No
    /// modification occurs on failure.
    pub fn insert(self, val: u64) -> Result<Option<u64>, OutofSpace> {
        match self {
            Self::Occupied(e) => Ok(Some(e.insert(val)?)),
            Self::Vacant(e) => {
                e.insert(val)?;
                Ok(None)
            }
        }
    }
}

pub struct OccupiedEntry<'a> {
    page: &'a mut IntPage,
    insert_data: *mut u8,
    insert_item: *mut u8,
    next_data: *mut u8,
    key: u64,
    val: u64,
}

impl<'a> OccupiedEntry<'a> {
    /// Get the key for this entry
    pub fn key(&self) -> u64 {
        self.key
    }

    /// Get the current value for this entry
    pub fn get(&self) -> u64 {
        self.val
    }

    /// Try to set the new value for this entry, returning the old one
    pub fn insert(mut self, val: u64) -> Result<u64, OutofSpace> {
        // Calculate the new size
        let key_len = (self.key.leading_zeros() as u8 >> 3).min(0x7);
        let val_len = val.leading_zeros() as u8 >> 3;
        let data_len = 16 - ((key_len + val_len) as usize);

        // Get the old size and check if we have space
        let old_data_len = unsafe { self.insert_item.offset_from(self.insert_data) };
        if data_len > (self.page.available() - (old_data_len as usize)) {
            return Err(OutofSpace);
        }

        unsafe {
            // Replace the length number
            *self.insert_item = (val_len << 3) | key_len;

            // Move the existing data as needed
            let copy_len = self.page.header().end() as usize - (self.next_data as usize & 0xFFF);
            self.insert_data
                .add(data_len)
                .copy_from(self.next_data, copy_len);

            // Copy in the key
            let mut new_key = (self.insert_data as *const u64).read_unaligned();
            new_key &= u64::MAX << ((8 - key_len) << 3);
            new_key |= self.key;
            (self.insert_data as *mut u64).write_unaligned(new_key);
            self.insert_data = self.insert_data.add((8 - key_len) as usize);

            // Copy in the value
            if val_len < 0x10 {
                let mut new_val = (self.insert_data as *const u64).read_unaligned();
                new_val &= u64::MAX << ((8 - val_len) << 3);
                new_val |= val;
                (self.insert_data as *mut u64).write_unaligned(new_val);
            }
        }

        // Update the end of the data region
        let end = self.page.header().end() + (data_len as u16) - (old_data_len as u16);
        self.page.header_mut().set_end(end);
        Ok(self.val)
    }

    /// Remove the entry from the page.
    pub fn delete(self) {
        unsafe {
            // Delete the length entry
            let last_item = self
                .page
                .mem
                .add(HEADER_OFFSET + 1 - (self.page.header().len() as usize));
            let copy_len = self.insert_item.offset_from(last_item);
            last_item.copy_to(last_item.offset(1), copy_len as usize);

            // Move down the remaining data
            let copy_len = self.page.header().end() as usize - (self.next_data as usize & 0xFFF);
            self.insert_data.copy_from(self.next_data, copy_len);
        }

        // Update the length and the end
        let old_data_len = unsafe { self.next_data.offset_from(self.insert_data) as u16 };
        let header = self.page.header_mut();
        header.set_end(header.end() - old_data_len);
        header.set_len(header.len() - 1);
    }

    /// Remove this entry from the page, returning the value
    pub fn remove(self) -> u64 {
        let val = self.val;
        self.delete();
        val
    }

    /// Remove this entry from the page, returning its key and value
    pub fn remove_entry(self) -> (u64, u64) {
        let ret = (self.key, self.val);
        self.delete();
        ret
    }
}

pub struct VacantEntry<'a> {
    page: &'a mut IntPage,
    insert_data: *mut u8,
    insert_item: *mut u8,
    key: u64,
}

impl<'a> VacantEntry<'a> {
    /// Get the key for this entry
    pub fn key(&self) -> u64 {
        self.key
    }

    /// Try inserting a value, failing if there's no more space.
    pub fn insert(mut self, val: u64) -> Result<(), OutofSpace> {
        // Calculate the size and figure out if we have space
        let key_len = (self.key.leading_zeros() as u8 >> 3).min(0x7);
        let val_len = val.leading_zeros() as u8 >> 3;
        let data_len = 16 - ((key_len + val_len) as usize);
        if data_len >= self.page.available() {
            return Err(OutofSpace);
        }

        // Insert the new length number, shifting everything past it down 1
        unsafe {
            let end = self
                .page
                .mem
                .add(HEADER_OFFSET - 1 - (self.page.header().len() as usize));
            let copy_len = self.insert_item.offset_from(end);
            end.copy_from(end.offset(1), copy_len as usize);
            *self.insert_item = (val_len << 3) | key_len;
        }

        // Insert the new key and value, shifting everything past them.
        unsafe {
            // Move the existing data
            let copy_len = self.page.header().end() as usize - (self.insert_data as usize & 0xFFF);
            let copy_dst = self.insert_data.add(data_len);
            copy_dst.copy_from(self.insert_data, copy_len);

            // Copy in the key
            let mut new_key = (self.insert_data as *const u64).read_unaligned();
            new_key &= u64::MAX << ((8 - key_len) << 3);
            new_key |= self.key;
            (self.insert_data as *mut u64).write_unaligned(new_key);
            self.insert_data = self.insert_data.add((8 - key_len) as usize);

            // Copy in the value
            if val_len < 0x10 {
                let mut new_val = (self.insert_data as *const u64).read_unaligned();
                new_val &= u64::MAX << ((8 - val_len) << 3);
                new_val |= val;
                (self.insert_data as *mut u64).write_unaligned(new_val);
            }
        }

        // Update the length and the end
        let header = self.page.header_mut();
        header.set_end(header.end() + 16 - (key_len + val_len) as u16);
        header.set_len(header.len() + 1);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let mut mem = [0u8; 8192];
        let ptr = mem
            .as_mut_ptr()
            .wrapping_add(mem.as_mut_ptr().align_offset(4096));

        let mut page = unsafe { IntPage::new(ptr, 0) };
        assert_eq!(page.available(), HEADER_OFFSET);
        assert_eq!(page.get(0), None);
        assert_eq!(page.insert(0, 1), Ok(None));
        assert_eq!(page.insert(1, 2), Ok(None));
        assert_eq!(page.insert(2, 3), Ok(None));
        println!("page = {:x?}", page);
        assert_eq!(page.get(0), Some(1));
        assert_eq!(page.get(1), Some(2));
        assert_eq!(page.get(2), Some(3));
        println!("remove data");
        assert_eq!(page.remove(1), Some(2));
        assert_eq!(page.remove(2), Some(3));
        assert_eq!(page.remove(0), Some(1));
        assert_eq!(page.remove(0), None);

        println!("{}", mem[4095]);
    }
}
