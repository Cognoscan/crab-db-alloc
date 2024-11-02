mod traits;
mod u64_u64;
mod u64_var;
mod var_u64;
pub use traits::*;
pub use u64_u64::*;
pub use u64_var::*;
pub use var_u64::*;

use core::{cmp::Ordering, marker::PhantomData, slice};

/// 4 kiB - (8 trailer bytes) - (4 objects) * (8 bytes of other + 2 bytes of layout)
pub const MAX_VAR_SIZE: usize = PAGE_4K - 8 - (4 * (8 + 2));

const CONTENT_SIZE: usize = PAGE_4K - core::mem::size_of::<TwoArrayTrailer>();

use crate::{
    arrays::{KeyValArrayMut, RevSizedArrayMut},
    Error, TwoArrayTrailer, PAGE_4K,
};

struct Cutpoint {
    lower_len: usize,
    upper_bytes: usize,
}

pub enum Balance<'a, T: PageLayout<'a>> {
    Merged(PageMapMut<'a, T>),
    Balanced {
        lower: PageMapMut<'a, T>,
        higher: PageMapMut<'a, T>,
        higher_key: T::Key,
    },
}

#[repr(transparent)]
pub struct PageMap<'a, T: PageLayout<'a>> {
    layout: PhantomData<&'a T>,
    page: *const u8,
}

impl<'a, T: PageLayout<'a>> Clone for PageMap<'a, T> {
    fn clone(&self) -> Self {
        Self {
            page: self.page,
            layout: PhantomData,
        }
    }
}

impl<'a, T: PageLayout<'a>> PageMap<'a, T> {
    /// Convert a pointer to a page into a PageMap.
    ///
    /// # Safety
    ///
    /// This must point to a 4 kiB page.
    pub unsafe fn from_ptr(page: *const u8) -> Result<Self, Error> {
        let ret = Self {
            page,
            layout: PhantomData,
        };
        let trailer = ret.page_trailer();
        trailer.lengths::<u8, T::Info>(CONTENT_SIZE)?;
        Ok(ret)
    }

    /// Convert a pointer to a page into a PageMap without even the most basic
    /// checks.
    ///
    /// # Safety
    ///
    /// This must point to a 4 kiB page with valid length information.
    pub unsafe fn from_ptr_unchecked(page: *const u8) -> Self {
        Self {
            page,
            layout: PhantomData,
        }
    }

    /// Get the trailer data for this map.
    pub fn page_trailer(&self) -> &TwoArrayTrailer {
        unsafe { &*(self.page.byte_add(CONTENT_SIZE) as *const TwoArrayTrailer) }
    }

    /// Iterate over the data within the map.
    pub fn iter(&self) -> PageIter<'a, T> {
        unsafe {
            let lengths = self.page_trailer().lengths_unchecked();
            let data =
                crate::arrays::KeyValArray::new(slice::from_raw_parts(self.page, lengths.lower));
            let info = slice::from_raw_parts(
                self.page
                    .add(CONTENT_SIZE - lengths.upper_bytes::<T::Info>())
                    as *const T::Info,
                lengths.upper,
            )
            .iter();
            PageIter { info, data }
        }
    }

    /// Copy a page's content to a new page.
    ///
    /// # Safety
    ///
    /// Destination must be valid 4 kiB page.
    pub unsafe fn copy_to(&self, dst: *mut u8) -> PageMapMut<'a, T> {
        unsafe {
            let lengths = self.page_trailer().lengths_unchecked();
            core::ptr::copy_nonoverlapping(self.page, dst, lengths.lower);
            let upper_bytes =
                lengths.upper_bytes::<T::Info>() + core::mem::size_of::<TwoArrayTrailer>();
            let upper_offset = CONTENT_SIZE - upper_bytes;
            core::ptr::copy_nonoverlapping(
                self.page.add(upper_offset),
                dst.add(upper_offset),
                upper_bytes,
            );
            PageMapMut {
                page: dst,
                layout: PhantomData,
            }
        }
    }
}

/// Get a page's type byte.
///
/// # Safety
///
/// Must point to a 4 kiB page.
pub unsafe fn page_type(page: *const u8) -> u8 {
    let trailer = unsafe { &*(page.byte_add(CONTENT_SIZE) as *const TwoArrayTrailer) };
    trailer.page_type
}

#[repr(transparent)]
pub struct PageMapMut<'a, T: PageLayout<'a>> {
    layout: PhantomData<&'a T>,
    page: *mut u8,
}

impl<'a, T: PageLayout<'a>> PageMapMut<'a, T> {
    /// Construct a new, empty map from a 4 kiB page.
    ///
    /// # Safety
    ///
    /// This must point to a 4 kiB page.
    pub unsafe fn new(page: *mut u8, page_type: u8) -> Self {
        let mut ret = Self {
            page,
            layout: PhantomData,
        };
        let trailer = ret.page_trailer_mut();
        trailer.page_type = page_type;
        trailer.set_lower_len(0);
        trailer.set_upper_len(0);
        ret
    }

    /// Get the pointer to this page.
    pub fn as_ptr(&self) -> *mut u8 {
        self.page
    }

    /// Convert a pointer to a page into a `PageMapMut`.
    ///
    /// # Safety
    ///
    /// This must point to a 4 kiB page.
    pub unsafe fn from_ptr(page: *mut u8) -> Result<Self, Error> {
        let ret = Self {
            page,
            layout: PhantomData,
        };
        let trailer = ret.as_const().page_trailer();
        trailer.lengths::<u8, T::Info>(CONTENT_SIZE)?;
        Ok(ret)
    }

    /// Convert a pointer to a page into a `PageMapMut`.
    ///
    /// # Safety
    ///
    /// This must point to a 4 kiB page with valid length information.
    pub unsafe fn from_ptr_unchecked(page: *mut u8) -> Self {
        Self {
            page,
            layout: PhantomData,
        }
    }

    /// Borrow for immutable use
    pub fn as_const(&self) -> &PageMap<'a, T> {
        // These types have the same layout and point to data with the same layout.
        unsafe { &*(self as *const PageMapMut<'a, T> as *const PageMap<'a, T>) }
    }

    fn find_cutpoint(&self, target: usize, max: usize) -> Result<Cutpoint, Error> {
        unsafe {
            let lengths = self.as_const().page_trailer().lengths_unchecked();
            let info = slice::from_raw_parts(
                self.page
                    .add(CONTENT_SIZE - lengths.upper_bytes::<T::Info>())
                    as *mut T::Info,
                lengths.upper,
            );
            let mut info = crate::arrays::RevSizedArray::new(info);

            let mut move_amount = 0;
            let mut taken_lower = 0;

            // Iterate until we're at the approximate split point.
            while let Some(pair_info) = info.next_back() {
                let pair_info = T::from_info(pair_info.endian_swap());

                // Check if we're on the final pair or if we have more to go.
                let pair_len = pair_info.key_len() + pair_info.value_len();
                let add_len = pair_len + core::mem::size_of::<T::Info>();
                if (add_len + move_amount) > target {
                    let mut new_upper_len_bytes = info.remaining_bytes();
                    // Determine if we actually take this final key-value pair or
                    // not. Choose whatever gets us closer to an even split.
                    if ((add_len + move_amount - target) > (target - move_amount))
                        || ((move_amount + add_len) > max)
                    {
                        // We don't want to take it.
                        new_upper_len_bytes += core::mem::size_of::<T::Info>();
                    } else {
                        // We do want to take it
                        taken_lower += pair_len;
                    }
                    if taken_lower > lengths.lower {
                        return Err(Error::DataCorruption);
                    }
                    return Ok(Cutpoint {
                        lower_len: taken_lower,
                        upper_bytes: new_upper_len_bytes,
                    });
                }
                move_amount += add_len;
                taken_lower += pair_len;
            }
            Err(Error::UnexpectedNoOp)
        }
    }

    /// Split this page approximately in half.
    ///
    /// This moves the upper half into a new page, returning both the page and
    /// the key for the first entry in it.
    ///
    /// # Safety
    ///
    /// `page` must be a 4 kiB page that isn't this page.
    pub unsafe fn split_to(&mut self, page: *mut u8) -> Result<(Self, T::Key), Error> {
        let trailer = self.page_trailer_mut();
        let page_type = trailer.page_type;
        let lengths = trailer.lengths_unchecked();

        let mut new_page = Self::new(page, page_type);

        // Find the point at which we'll split the page
        let total_len = lengths.total::<u8, T::Info>();
        let cutpoint = self.find_cutpoint(total_len / 2, total_len)?;

        unsafe {
            // Copy the data over
            let split_lower_len = lengths.lower_bytes::<u8>() - cutpoint.lower_len;
            let upper_len_bytes = lengths.upper_bytes::<T::Info>();
            let split_upper_len_bytes = upper_len_bytes - cutpoint.upper_bytes;
            core::ptr::copy_nonoverlapping(self.page, new_page.page, split_lower_len);
            core::ptr::copy_nonoverlapping(
                self.page.add(CONTENT_SIZE - upper_len_bytes),
                new_page.page.add(CONTENT_SIZE - split_upper_len_bytes),
                split_upper_len_bytes,
            );

            // Update both trailers
            let trailer = self.page_trailer_mut();
            trailer.set_lower_len(cutpoint.lower_len as u16);
            trailer.set_upper_len((cutpoint.upper_bytes / core::mem::size_of::<T::Info>()) as u16);
            let new_trailer = new_page.page_trailer_mut();
            new_trailer.set_lower_len(split_lower_len as u16);
            new_trailer
                .set_upper_len((split_upper_len_bytes / core::mem::size_of::<T::Info>()) as u16);

            // Grab the new key and return it.
            let Some(Ok((key, _))) = new_page.as_const().iter().next() else {
                return Err(Error::DataCorruption);
            };
            Ok((new_page, key))
        }
    }

    /// Rebalance two pages, potentially reducing down to one page.
    ///
    /// This will attempt to evenly distribute data between the two pages,
    /// but will reduce down to one page if possible. Reducing will always
    /// eliminate the upper page, keeping the lower one. After rebalancing, if
    /// both pages are still present, then the key of the upper page's first
    /// entry will also be returned.
    ///
    /// # Safety
    ///
    /// The upper page *must* have its first key be higher than any key in this
    /// page. If these pages are all constructed using `split_to`, then this
    /// should be easy to maintain by keeping them ordered by first key.
    pub unsafe fn balance(
        mut self,
        mut higher: Self,
    ) -> Result<Balance<'a, T>, Error> {
        unsafe {
            let self_len = self.data_len();
            let higher_len = higher.data_len();

            if (self_len + higher_len) < CONTENT_SIZE {
                // Merge

                // Copy the data
                let self_len = self.page_trailer_mut().lengths_unchecked();
                let higher_len = higher.page_trailer_mut().lengths_unchecked();
                core::ptr::copy_nonoverlapping(
                    higher.page,
                    self.page.add(self_len.lower_bytes::<u8>()),
                    higher_len.lower_bytes::<u8>(),
                );
                let upper_copy_len = higher_len.upper_bytes::<T::Info>();
                core::ptr::copy_nonoverlapping(
                    higher.page.add(CONTENT_SIZE - upper_copy_len),
                    self.page
                        .add(CONTENT_SIZE - upper_copy_len - self_len.upper_bytes::<T::Info>()),
                    upper_copy_len,
                );

                // Update the lengths
                let trailer = self.page_trailer_mut();
                trailer.add_to_lower_len(higher_len.lower as isize);
                trailer.add_to_upper_len(higher_len.upper as isize);

                Ok(Balance::Merged(self))
            } else if self_len > higher_len {
                // Move from self to the higher page

                let cutpoint =
                    self.find_cutpoint((self_len - higher_len) / 2, higher.free_space())?;
                let self_len = self.page_trailer_mut().lengths_unchecked();
                let higher_len = higher.page_trailer_mut().lengths_unchecked();

                // Make room and then copy the lower data
                core::ptr::copy(
                    higher.page,
                    higher.page.add(cutpoint.lower_len),
                    higher_len.lower,
                );
                core::ptr::copy_nonoverlapping(
                    self.page.add(cutpoint.lower_len),
                    higher.page,
                    cutpoint.lower_len,
                );

                // Make room and then copy the upper data
                let higher_upper_bytes = higher_len.upper_bytes::<T::Info>();
                core::ptr::copy(
                    higher.page.add(CONTENT_SIZE - higher_upper_bytes),
                    higher
                        .page
                        .add(CONTENT_SIZE - higher_upper_bytes - cutpoint.upper_bytes),
                    higher_upper_bytes,
                );
                core::ptr::copy_nonoverlapping(
                    self.page
                        .add(CONTENT_SIZE - self_len.upper_bytes::<T::Info>()),
                    higher
                        .page
                        .add(CONTENT_SIZE - higher_upper_bytes - cutpoint.upper_bytes),
                    cutpoint.upper_bytes,
                );

                // Update everyone's lengths
                let lower_delta = cutpoint.lower_len as isize;
                let upper_delta = (cutpoint.upper_bytes / core::mem::size_of::<T::Info>()) as isize;
                let trailer = self.page_trailer_mut();
                trailer.add_to_lower_len(-lower_delta);
                trailer.add_to_upper_len(-upper_delta);
                let trailer = higher.page_trailer_mut();
                trailer.add_to_lower_len(lower_delta);
                trailer.add_to_upper_len(upper_delta);

                // Get the new base key in the higher page
                let Some(Ok((higher_key, _))) = higher.as_const().iter().next() else {
                    return Err(Error::DataCorruption);
                };
                Ok(Balance::Balanced { lower: self, higher, higher_key })
            } else {
                // Move from the higher page to self

                let cutpoint =
                    higher.find_cutpoint((higher_len - self_len) / 2, self.free_space())?;
                let self_len = self.page_trailer_mut().lengths_unchecked();
                let higher_len = higher.page_trailer_mut().lengths_unchecked();

                // Copy the lower data, then delete it from the higher page
                core::ptr::copy_nonoverlapping(
                    higher.page,
                    self.page.add(self_len.lower),
                    cutpoint.lower_len,
                );
                core::ptr::copy(
                    higher.page.add(cutpoint.lower_len),
                    higher.page,
                    higher_len.lower - cutpoint.lower_len,
                );

                // Make room and then copy the upper data
                let higher_upper_bytes = higher_len.upper_bytes::<T::Info>();
                let self_upper_bytes = self_len.upper_bytes::<T::Info>();
                core::ptr::copy_nonoverlapping(
                    higher.page.add(CONTENT_SIZE - cutpoint.upper_bytes),
                    self.page
                        .add(CONTENT_SIZE - self_upper_bytes - cutpoint.upper_bytes),
                    cutpoint.upper_bytes,
                );
                core::ptr::copy(
                    higher.page.add(CONTENT_SIZE - higher_upper_bytes),
                    higher
                        .page
                        .add(CONTENT_SIZE - higher_upper_bytes + cutpoint.upper_bytes),
                    higher_upper_bytes,
                );

                // Update everyone's lengths
                let lower_delta = cutpoint.lower_len as isize;
                let upper_delta = (cutpoint.upper_bytes / core::mem::size_of::<T::Info>()) as isize;
                let trailer = self.page_trailer_mut();
                trailer.add_to_lower_len(lower_delta);
                trailer.add_to_upper_len(upper_delta);
                let trailer = higher.page_trailer_mut();
                trailer.add_to_lower_len(-lower_delta);
                trailer.add_to_upper_len(-upper_delta);

                // Get the new base key in the higher page
                let Some(Ok((higher_key, _))) = higher.as_const().iter().next() else {
                    return Err(Error::DataCorruption);
                };
                Ok(Balance::Balanced { lower: self, higher, higher_key })
            }
        }
    }

    /// Borrow the trailer for modification.
    pub fn page_trailer_mut(&mut self) -> &mut TwoArrayTrailer {
        unsafe {
            &mut *(self
                .page
                .byte_add(PAGE_4K - core::mem::size_of::<TwoArrayTrailer>())
                as *mut TwoArrayTrailer)
        }
    }

    /// Get how much free space is in the page.
    pub fn free_space(&self) -> usize {
        let lengths = unsafe { self.as_const().page_trailer().lengths_unchecked() };
        CONTENT_SIZE - lengths.total::<u8, T::Info>()
    }

    /// Get how many bytes of data are in the page.
    pub fn data_len(&self) -> usize {
        let lengths = unsafe { self.as_const().page_trailer().lengths_unchecked() };
        lengths.total::<u8, T::Info>()
    }

    /// Get an entry in the page.
    pub fn entry(&mut self, key: T::Key) -> Result<Entry<'a, T>, Error> {
        unsafe {
            let trailer = &mut *(self
                .page
                .byte_add(PAGE_4K - core::mem::size_of::<TwoArrayTrailer>())
                as *mut TwoArrayTrailer);
            let lengths = trailer.lengths_unchecked();
            let free_space = CONTENT_SIZE - lengths.total::<u8, T::Info>();
            let mut kv = crate::arrays::KeyValArrayMut::new(slice::from_raw_parts_mut(
                self.page,
                lengths.lower,
            ));
            let info = slice::from_raw_parts_mut(
                self.page
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
    info: slice::Iter<'a, T::Info>,
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
