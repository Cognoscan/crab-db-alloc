mod traits;
mod u64_u64;
mod u64_var;
mod var_u64;
pub use traits::*;
pub use u64_u64::*;
pub use u64_var::*;
pub use var_u64::*;

use std::{cmp::Ordering, marker::PhantomData};

/// 4 kiB - (8 trailer bytes) - (4 objects) * (8 bytes of other + 2 bytes of layout)
pub const MAX_VAR_SIZE: usize = PAGE_4K - 8 - (4 * (8 + 2));

const CONTENT_SIZE: usize = PAGE_4K - core::mem::size_of::<TwoArrayTrailer>();

use crate::{
    arrays::{KeyValArrayMut, RevSizedArrayMut},
    Error, TwoArrayTrailer, PAGE_4K,
};

#[repr(transparent)]
pub struct PageMap<'a, T: PageLayout<'a>> {
    layout: PhantomData<&'a T>,
    page: [u8],
}

impl<'a, T: PageLayout<'a>> TryFrom<&[u8]> for &PageMap<'a, T> {
    type Error = Error;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != PAGE_4K {
            return Err(Error::IncorrectPageSize);
        }
        // We can transmute because the layout of this type is set to be
        // `repr(transparent)` for a `[u8]`.
        let ret: &PageMap<'a, T> = unsafe { core::mem::transmute(value) };
        let trailer = ret.page_trailer();
        trailer.lengths::<u8, T::Info>(CONTENT_SIZE)?;
        Ok(ret)
    }
}

impl<'a, T: PageLayout<'a>> TryFrom<&mut [u8]> for &mut PageMap<'a, T> {
    type Error = Error;
    fn try_from(value: &mut [u8]) -> Result<Self, Self::Error> {
        if value.len() != PAGE_4K {
            return Err(Error::IncorrectPageSize);
        }
        // We can transmute because the layout of this type is set to be
        // `repr(transparent)` for a `[u8]`.
        let ret: &mut PageMap<'a, T> = unsafe { core::mem::transmute(value) };
        let trailer = ret.page_trailer();
        trailer.lengths::<u8, T::Info>(CONTENT_SIZE)?;
        Ok(ret)
    }
}

/// Get a page's type byte.
pub fn page_type(page: &[u8]) -> Result<u8, Error> {
    if page.len() != PAGE_4K {
        return Err(Error::IncorrectPageSize);
    }

    // Safety: We just verified the provided slice is large enough for us to
    // perform this offset, and the cast is to a `repr(C)` type with no padding
    // bytes.
    let trailer = unsafe {
        &*(page
            .as_ptr()
            .byte_add(PAGE_4K - core::mem::size_of::<TwoArrayTrailer>())
            as *const TwoArrayTrailer)
    };
    Ok(trailer.page_type)
}

impl<'a, T: PageLayout<'a>> PageMap<'a, T> {

    /// Coerce a slice into a immutable borrow of a PageMap.
    ///
    /// # Safety
    ///
    /// The safety checks of the corresponding `try_from` must be upheld:
    ///
    /// - The slice must be 4 kiB in size.
    /// - The page trailer layout must make sense for the type.
    /// 
    /// Usually this is meant for when `try_from` has already been called on the
    /// slice, but it needs to be turned into a borrowed `PageMap` multiple times.
    pub unsafe fn from_slice_unchecked(page: &[u8]) -> &Self {
        unsafe { core::mem::transmute(page) }
    }

    /// Coerce a slice into a mutable borrow of a PageMap.
    ///
    /// # Safety
    ///
    /// The safety checks of the corresponding `try_from` must be upheld:
    ///
    /// - The slice must be 4 kiB in size.
    /// - The page trailer layout must make sense for the type.
    /// 
    /// Usually this is meant for when `try_from` has already been called on the
    /// slice, but it needs to be turned into a borrowed `PageMap` multiple times.
    pub unsafe fn from_slice_mut_unchecked(page: &mut [u8]) -> &mut Self {
        unsafe { core::mem::transmute(page) }
    }

    /// Get the underlying data slice for this page.
    pub fn slice(&self) -> &[u8] {
        &self.page
    }

    pub fn page_trailer(&self) -> &TwoArrayTrailer {
        unsafe {
            &*(self
                .page
                .as_ptr()
                .byte_add(PAGE_4K - core::mem::size_of::<TwoArrayTrailer>())
                as *const TwoArrayTrailer)
        }
    }

    pub fn iter(&self) -> PageIter<'a, T> {
        unsafe {
            let lengths = self.page_trailer().lengths_unchecked();
            let data = crate::arrays::KeyValArray::new(self.page.get_unchecked(0..lengths.lower));
            let info = core::slice::from_raw_parts(
                self.page
                    .as_ptr()
                    .add(CONTENT_SIZE - lengths.upper_bytes::<T::Info>())
                    as *const T::Info,
                lengths.upper,
            )
            .iter();
            PageIter { info, data }
        }
    }

    pub fn page_trailer_mut(&mut self) -> &mut TwoArrayTrailer {
        unsafe {
            &mut *(self
                .page
                .as_ptr()
                .byte_add(PAGE_4K - core::mem::size_of::<TwoArrayTrailer>())
                as *mut TwoArrayTrailer)
        }
    }

    /// Copy a page's content to a new page.
    pub fn copy_to(&self, dst: &'a mut [u8]) -> Result<&mut PageMap<'a, T>, Error> {
        if dst.len() != PAGE_4K {
            return Err(Error::IncorrectPageSize);
        }
        unsafe {
            let lengths = self.page_trailer().lengths_unchecked();
            core::ptr::copy_nonoverlapping(self.page.as_ptr(), dst.as_mut_ptr(), lengths.lower);
            let upper_bytes = lengths.upper_bytes::<T::Info>();
            let upper_offset = CONTENT_SIZE - upper_bytes;
            core::ptr::copy_nonoverlapping(
                self.page.as_ptr().add(upper_offset),
                dst.as_mut_ptr().add(upper_offset),
                upper_bytes + core::mem::size_of::<TwoArrayTrailer>(),
            );
            let new_page: &mut PageMap<'a, T> = core::mem::transmute(dst);
            Ok(new_page)
        }
    }

    /// Get how much free space is in the page
    pub fn free_space(&self) -> usize {
        let lengths = unsafe { self.page_trailer().lengths_unchecked() };
        CONTENT_SIZE - lengths.total::<u8, T::Info>()
    }

    /// Get an entry in the page
    pub fn entry(&mut self, key: T::Key) -> Result<Entry<'a, T>, Error> {
        unsafe {
            let trailer = &mut *(self
                .page
                .as_ptr()
                .byte_add(PAGE_4K - core::mem::size_of::<TwoArrayTrailer>())
                as *mut TwoArrayTrailer);
            let lengths = trailer.lengths_unchecked();
            let free_space = CONTENT_SIZE - lengths.total::<u8, T::Info>();
            let mut kv =
                crate::arrays::KeyValArrayMut::new(self.page.get_unchecked_mut(0..lengths.lower));
            let info = core::slice::from_raw_parts_mut(
                self.page
                    .as_mut_ptr()
                    .add(CONTENT_SIZE - lengths.upper_bytes::<T::Info>())
                    as *mut T::Info,
                lengths.upper,
            );
            let mut info = crate::arrays::RevSizedArrayMut::new(info);

            while let Some(pair_info) = info.next_back() {
                let pair_info = T::from_info(pair_info.endian_swap());
                let (local_key, val) =
                    kv.next_pair_back(pair_info.key_len(), pair_info.value_len())?;
                let local_key = pair_info.read_key(local_key)?;
                let val = pair_info.read_value(val)?;
                match local_key.cmp(&key) {
                    Ordering::Equal => {
                        return Ok(Entry::Occupied(OccupiedEntry {
                            key,
                            val,
                            trailer,
                            kv,
                            info,
                            pair_info,
                            free_space,
                        }))
                    }
                    Ordering::Greater => {
                        return Ok(Entry::Vacant(VacantEntry {
                            key,
                            info,
                            trailer,
                            kv,
                            free_space,
                        }))
                    }
                    Ordering::Less => (),
                }
            }
            kv.next_pair_back_none()?;
            Ok(Entry::Vacant(VacantEntry {
                key,
                info,
                trailer,
                kv,
                free_space,
            }))
        }
    }

    /// Insert a key-value pair into the page.
    pub fn insert(&mut self, key: T::Key, val: &T::Value) -> Result<(), Error> {
        match self.entry(key)? {
            Entry::Occupied(mut o) => o.update(val),
            Entry::Vacant(v) => v.insert(val),
        }
    }
}

#[derive(Debug)]
pub struct PageIter<'a, T: PageLayout<'a>> {
    info: core::slice::Iter<'a, T::Info>,
    data: crate::arrays::KeyValArray<'a>,
}

impl<'a, T: PageLayout<'a>> Clone for PageIter<'a, T> {
    fn clone(&self) -> Self {
        Self {
            info: self.info.clone(),
            data: self.data.clone(),
        }
    }
}

impl<'a, T: PageLayout<'a>> PageIter<'a, T> {
    #[allow(clippy::type_complexity)]
    fn next_internal(&mut self) -> Result<Option<(T::Key, T::Value)>, Error> {
        let Some(info) = self.info.next() else {
            self.data.next_none()?;
            return Ok(None);
        };
        let info = T::from_info(info.endian_swap());
        let (key, val) = self.data.next_pair(info.key_len(), info.value_len())?;
        // Safety: we constructed our slices using the provided length numbers.
        unsafe { Ok(Some((info.read_key(key)?, info.read_value(val)?))) }
    }

    #[allow(clippy::type_complexity)]
    fn next_back_internal(&mut self) -> Result<Option<(T::Key, T::Value)>, Error> {
        let Some(info) = self.info.next_back() else {
            self.data.next_none()?;
            return Ok(None);
        };
        let info = T::from_info(info.endian_swap());
        let (key, val) = self.data.next_pair_back(info.key_len(), info.value_len())?;
        // Safety: we constructed our slices using the provided length numbers.
        unsafe { Ok(Some((info.read_key(key)?, info.read_value(val)?))) }
    }
}

impl<'a, T: PageLayout<'a>> Iterator for PageIter<'a, T> {
    type Item = Result<(T::Key, T::Value), Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_internal().transpose()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.info.len() + 1))
    }
}

impl<'a, T: PageLayout<'a>> DoubleEndedIterator for PageIter<'a, T>
where
    T::Key: 'a,
    T::Value: 'a,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.next_back_internal().transpose()
    }
}

pub enum Entry<'a, T: PageLayout<'a>> {
    Occupied(OccupiedEntry<'a, T>),
    Vacant(VacantEntry<'a, T>),
}

/// An occupied entry in the map, ready to be inspected and modified.
pub struct OccupiedEntry<'a, T: PageLayout<'a>> {
    key: T::Key,
    val: T::Value,
    pair_info: T,
    info: RevSizedArrayMut<'a, T::Info>,
    trailer: &'a mut TwoArrayTrailer,
    kv: KeyValArrayMut<'a>,
    free_space: usize,
}

impl<'a, T: PageLayout<'a>> OccupiedEntry<'a, T> {
    /// Get the key for this entry.
    pub fn key(&self) -> &T::Key {
        &self.key
    }

    /// Get the value for this entry.
    pub fn value(&self) -> &T::Value {
        &self.val
    }

    /// Delete the entire entry.
    pub fn delete(self) {
        // Delete the values from both arrays, then update the trailer lengths.
        unsafe {
            self.info.back_delete();
            let delta = self.kv.delete_back();
            let len = self.trailer.lengths_unchecked();
            self.trailer.set_lower_len((len.lower - delta) as u16);
            self.trailer.set_upper_len((len.upper - 1) as u16);
        }
    }

    /// Update the value in-place.
    pub fn update(&mut self, new_value: &T::Value) -> Result<(), Error> {
        let delta = self.pair_info.update_value(new_value)?;
        unsafe {
            if (self.free_space as isize) < delta {
                return Err(Error::OutofSpace(delta as usize - self.free_space));
            }
            self.pair_info
                .write_value(new_value, self.kv.back_resize(delta));
            let len = self.trailer.lengths_unchecked();
            self.trailer
                .set_lower_len((len.lower as isize + delta) as u16);
            Ok(())
        }
    }
}


/// An empty entry in the map, ready to be filled.
pub struct VacantEntry<'a, T: PageLayout<'a>> {
    key: T::Key,
    info: RevSizedArrayMut<'a, T::Info>,
    trailer: &'a mut TwoArrayTrailer,
    kv: KeyValArrayMut<'a>,
    free_space: usize,
}

impl<'a, T: PageLayout<'a>> VacantEntry<'a, T> {
    /// Get the key for this vacant entry.
    pub fn key(&self) -> &T::Key {
        &self.key
    }

    /// Insert a value into this entry.
    pub fn insert(mut self, value: &T::Value) -> Result<(), Error> {
        // Calculate the new pair's info and sizes, then check we have enough space.
        let pair_info = T::from_data(&self.key, value)?;
        let key_len = pair_info.key_len();
        let val_len = pair_info.value_len();
        let total_len = key_len + val_len;
        if self.free_space < total_len {
            return Err(Error::OutofSpace(total_len));
        }

        // Update the arrays, then update the trailer.
        unsafe {
            let len = self.trailer.lengths_unchecked();
            self.info.back_insert(pair_info.info());
            pair_info.write_pair(&self.key, value, self.kv.back_insert(total_len));
            self.trailer.set_lower_len((len.lower + total_len) as u16);
            self.trailer.set_upper_len((len.upper + 1) as u16);
        }
        Ok(())
    }
}
