mod traits;
mod u64_var;
mod u64_u64;
mod var_u64;
pub use traits::*;
pub use u64_var::*;
pub use u64_u64::*;
pub use var_u64::*;

use std::cmp::Ordering;

/// 4 kiB - (8 trailer bytes) - (4 objects) * (8 bytes of other + 2 bytes of layout)
pub const MAX_VAR_SIZE: usize = 4096 - 8 - (4 * (8 + 2));

use crate::{
    arrays::{KeyValArrayMut, RevSizedArrayMut},
    Error, TwoArrayTrailer,
};

pub type U64VarPageIter<'a> = PageIter<'a, LayoutU64Var>;

pub struct PageIter<'a, T: PageLayout<'a>> {
    info: core::slice::Iter<'a, T::Info>,
    data: crate::arrays::KeyValArray<'a>,
}

impl<'a, T: PageLayout<'a>> PageIter<'a, T> {
    pub fn iter_page(page: &[u8]) -> Result<Self, Error> {
        let (content, trailer) = page.split_last_chunk::<8>().ok_or(Error::DataCorruption)?;
        let trailer = unsafe { &*(trailer.as_ptr() as *const TwoArrayTrailer) };
        let lengths = trailer.lengths::<u8, T::Info>(content.len())?;
        unsafe {
            let data = crate::arrays::KeyValArray::new(content.get_unchecked(0..lengths.lower));
            let info = core::slice::from_raw_parts(
                content
                    .as_ptr()
                    .add(content.len() - lengths.upper_bytes::<T::Info>())
                    as *const T::Info,
                lengths.upper,
            )
            .iter();
            Ok(Self { info, data })
        }
    }

    #[allow(clippy::type_complexity)]
    fn next_internal(&mut self) -> Result<Option<(T::Key, T::Value)>, Error> {
        let Some(info) = self.info.next() else {
            self.data.next_none()?;
            return Ok(None);
        };
        let info = T::from_info(info.endian_swap());
        let (key, val) = self.data.next_pair(info.key_len(), info.value_len())?;
        // Safety: we constructed our slices using the provided length numbers.
        unsafe {
            Ok(Some((info.read_key(key)?, info.read_value(val)?)))
        }
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
        unsafe {
            Ok(Some((info.read_key(key)?, info.read_value(val)?)))
        }
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

pub fn page_free_space<'a, T: PageLayout<'a>>(page: &[u8]) -> Result<usize, Error> {
    let (content, trailer) = page.split_last_chunk::<8>().ok_or(Error::DataCorruption)?;
    let trailer = unsafe { &*(trailer.as_ptr() as *const TwoArrayTrailer) };
    let lengths = trailer.lengths::<u8, T::Info>(content.len())?;
    Ok(content.len() - lengths.total::<u8, T::Info>())
}

pub fn page_entry<'a, T: PageLayout<'a>>(
    page: &mut [u8],
    key: T::Key,
) -> Result<Entry<'a, T>, Error> {
    let (content, trailer) = page
        .split_last_chunk_mut::<8>()
        .ok_or(Error::DataCorruption)?;
    let trailer = unsafe { &mut *(trailer.as_ptr() as *mut TwoArrayTrailer) };
    let lengths = trailer.lengths::<u8, T::Info>(content.len())?;
    let free_space = content.len() - lengths.total::<u8, T::Info>();
    unsafe {
        let mut kv =
            crate::arrays::KeyValArrayMut::new(content.get_unchecked_mut(0..lengths.lower));
        let info = core::slice::from_raw_parts_mut(
            content
                .as_mut_ptr()
                .add(content.len() - lengths.upper_bytes::<T::Info>()) as *mut T::Info,
            lengths.upper,
        );
        let mut info = crate::arrays::RevSizedArrayMut::new(info);

        while let Some(pair_info) = info.next_back() {
            let pair_info = T::from_info(pair_info.endian_swap());
            let (local_key, val) = kv.next_pair_back(pair_info.key_len(), pair_info.value_len())?;
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

pub enum Entry<'a, T: PageLayout<'a>> {
    Occupied(OccupiedEntry<'a, T>),
    Vacant(VacantEntry<'a, T>),
}

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
    pub fn update(mut self, new_value: &T::Value) -> Result<(), Error> {
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
