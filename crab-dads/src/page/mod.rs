mod traits;
mod u64_u64;
mod u64_var;
mod var_u64;
pub use traits::*;
pub use u64_u64::*;
pub use u64_var::*;
pub use var_u64::*;

use core::{cmp::Ordering, marker::PhantomData, slice};

const CONTENT_SIZE: usize = PAGE_4K - core::mem::size_of::<TwoArrayTrailer>();

use crate::{
    arrays::{KeyValArray, KeyValArrayMut, KeyValEntry, RevSizedArray, RevSizedArrayMut},
    Error, TwoArrayTrailer, PAGE_4K,
};

struct Cutpoint {
    lower_len: usize,
    upper_bytes: usize,
}

pub enum Balance<T: PageLayout> {
    Merged(PageMapMut<T>),
    Balanced {
        lower: PageMapMut<T>,
        higher: PageMapMut<T>,
    },
}

#[repr(transparent)]
pub struct PageMap<T: PageLayout> {
    layout: PhantomData<T>,
    page: *const u8,
}

impl<T: PageLayout> Clone for PageMap<T> {
    fn clone(&self) -> Self {
        Self {
            page: self.page,
            layout: PhantomData,
        }
    }
}

impl<T: PageLayout> PageMap<T> {
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
        trailer.lengths::<u8, T>(CONTENT_SIZE)?;
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
    pub fn iter(&self) -> PageIter<T> {
        unsafe {
            let lengths = self.page_trailer().lengths_unchecked();
            let data = KeyValArray::new(slice::from_raw_parts(self.page, lengths.lower));
            let info = RevSizedArray::new(slice::from_raw_parts(
                self.page.add(CONTENT_SIZE - lengths.upper_bytes::<T>()) as *const T,
                lengths.upper,
            ));
            PageIter { info, data }
        }
    }

    /// Copy a page's content to a new page.
    ///
    /// # Safety
    ///
    /// Destination must be valid 4 kiB page.
    pub unsafe fn copy_to(&self, dst: *mut u8) -> PageMapMut<T> {
        unsafe {
            let lengths = self.page_trailer().lengths_unchecked();
            core::ptr::copy_nonoverlapping(self.page, dst, lengths.lower);
            let upper_bytes = lengths.upper_bytes::<T>() + core::mem::size_of::<TwoArrayTrailer>();
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
pub struct PageMapMut<T: PageLayout> {
    layout: PhantomData<T>,
    page: *mut u8,
}

impl<T: PageLayout> PageMapMut<T> {
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
        trailer.lengths::<u8, T>(CONTENT_SIZE)?;
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
    pub fn as_const(&self) -> &PageMap<T> {
        // These types have the same layout and point to data with the same layout.
        unsafe { &*(self as *const PageMapMut<T> as *const PageMap<T>) }
    }

    fn find_cutpoint(&self, target: usize, max: usize) -> Result<Cutpoint, Error> {
        unsafe {
            let lengths = self.as_const().page_trailer().lengths_unchecked();
            let info = slice::from_raw_parts(
                self.page.add(CONTENT_SIZE - lengths.upper_bytes::<T>()) as *mut T,
                lengths.upper,
            );
            let mut info = crate::arrays::RevSizedArray::new(info);

            let mut move_amount = 0;
            let mut taken_lower = 0;

            // Iterate until we're at the approximate split point.
            while let Some(pair_info) = info.next_back() {
                let pair_info = pair_info?;

                // Check if we're on the final pair or if we have more to go.
                let pair_len = pair_info.key_len() + pair_info.value_len();
                let add_len = pair_len + core::mem::size_of::<T>();
                if (add_len + move_amount) > target {
                    let mut new_upper_len_bytes = info.remaining_bytes();
                    // Determine if we actually take this final key-value pair or
                    // not. Choose whatever gets us closer to an even split.
                    if ((add_len + move_amount - target) > (target - move_amount))
                        || ((move_amount + add_len) > max)
                    {
                        // We don't want to take it.
                        new_upper_len_bytes += core::mem::size_of::<T>();
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
    /// This moves the upper half into a new page and returns that page.
    ///
    /// # Safety
    ///
    /// `page` must be a 4 kiB page that isn't this page.
    pub unsafe fn split_to(&mut self, page: *mut u8) -> Result<Self, Error> {
        let trailer = self.page_trailer_mut();
        let page_type = trailer.page_type;

        unsafe {
            let lengths = trailer.lengths_unchecked();

            let mut new_page = Self::new(page, page_type);

            // Find the point at which we'll split the page
            let total_len = lengths.total::<u8, T>();
            let cutpoint = self.find_cutpoint(total_len / 2, total_len)?;

            // Copy the data over
            let split_lower_len = lengths.lower_bytes::<u8>() - cutpoint.lower_len;
            let upper_len_bytes = lengths.upper_bytes::<T>();
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
            trailer.set_upper_len((cutpoint.upper_bytes / core::mem::size_of::<T>()) as u16);
            let new_trailer = new_page.page_trailer_mut();
            new_trailer.set_lower_len(split_lower_len as u16);
            new_trailer.set_upper_len((split_upper_len_bytes / core::mem::size_of::<T>()) as u16);

            Ok(new_page)
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
    pub unsafe fn balance(mut self, mut higher: Self) -> Result<Balance<T>, Error> {
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
                let upper_copy_len = higher_len.upper_bytes::<T>();
                core::ptr::copy_nonoverlapping(
                    higher.page.add(CONTENT_SIZE - upper_copy_len),
                    self.page
                        .add(CONTENT_SIZE - upper_copy_len - self_len.upper_bytes::<T>()),
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
                let higher_upper_bytes = higher_len.upper_bytes::<T>();
                core::ptr::copy(
                    higher.page.add(CONTENT_SIZE - higher_upper_bytes),
                    higher
                        .page
                        .add(CONTENT_SIZE - higher_upper_bytes - cutpoint.upper_bytes),
                    higher_upper_bytes,
                );
                core::ptr::copy_nonoverlapping(
                    self.page.add(CONTENT_SIZE - self_len.upper_bytes::<T>()),
                    higher
                        .page
                        .add(CONTENT_SIZE - higher_upper_bytes - cutpoint.upper_bytes),
                    cutpoint.upper_bytes,
                );

                // Update everyone's lengths
                let lower_delta = cutpoint.lower_len as isize;
                let upper_delta = (cutpoint.upper_bytes / core::mem::size_of::<T>()) as isize;
                let trailer = self.page_trailer_mut();
                trailer.add_to_lower_len(-lower_delta);
                trailer.add_to_upper_len(-upper_delta);
                let trailer = higher.page_trailer_mut();
                trailer.add_to_lower_len(lower_delta);
                trailer.add_to_upper_len(upper_delta);

                Ok(Balance::Balanced {
                    lower: self,
                    higher,
                })
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
                let higher_upper_bytes = higher_len.upper_bytes::<T>();
                let self_upper_bytes = self_len.upper_bytes::<T>();
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
                let upper_delta = (cutpoint.upper_bytes / core::mem::size_of::<T>()) as isize;
                let trailer = self.page_trailer_mut();
                trailer.add_to_lower_len(lower_delta);
                trailer.add_to_upper_len(upper_delta);
                let trailer = higher.page_trailer_mut();
                trailer.add_to_lower_len(-lower_delta);
                trailer.add_to_upper_len(-upper_delta);

                Ok(Balance::Balanced {
                    lower: self,
                    higher,
                })
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
        CONTENT_SIZE - lengths.total::<u8, T>()
    }

    /// Get how many bytes of data are in the page.
    pub fn data_len(&self) -> usize {
        let lengths = unsafe { self.as_const().page_trailer().lengths_unchecked() };
        lengths.total::<u8, T>()
    }

    /// Get an entry in the page.
    pub fn entry<'a, 'k>(&'a mut self, key: &'k T::Key) -> Result<Entry<'a, 'k, T>, Error> {
        unsafe {
            // Extract the trailer and info inside it
            let trailer = &mut *(self
                .page
                .byte_add(PAGE_4K - core::mem::size_of::<TwoArrayTrailer>())
                as *mut TwoArrayTrailer);
            let lengths = trailer.lengths_unchecked();

            // Construct the two array iterators
            let mut kv = crate::arrays::KeyValArrayMut::new(slice::from_raw_parts_mut(
                self.page,
                lengths.lower,
            ));
            let info = slice::from_raw_parts_mut(
                self.page.add(CONTENT_SIZE - lengths.upper_bytes::<T>()) as *mut T,
                lengths.upper,
            );
            let mut info = crate::arrays::RevSizedArrayMut::new(info);

            while let Some(i) = info.next_back() {
                let i = i?;
                kv.next_pair_back(i.key_len(), i.value_len())?;
                match i.read_key(kv.key())?.cmp(key) {
                    Ordering::Equal => {
                        return Ok(Entry::Occupied(OccupiedEntry { trailer, kv, info }))
                    }
                    Ordering::Greater => {
                        return Ok(Entry::Vacant(VacantEntry {
                            trailer,
                            kv,
                            info,
                            key,
                        }))
                    }
                    Ordering::Less => (),
                }
            }
            kv.next_pair_back_none()?;
            Ok(Entry::Vacant(VacantEntry {
                info,
                trailer,
                kv,
                key,
            }))
        }
    }

    // Insert a key-value pair into the page.
    //pub fn insert(&mut self, key: T::Key, val: &T::Value) -> Result<(), Error> {
    //    match self.entry(key)? {
    //        Entry::Occupied(mut o) => o.update(val),
    //        Entry::Vacant(v) => v.insert(val),
    //    }
    //}
}

#[derive(Debug)]
pub struct PageIter<'a, T: PageLayout> {
    info: RevSizedArray<'a, T>,
    data: KeyValArray<'a>,
}

impl<'a, T: PageLayout> Clone for PageIter<'a, T> {
    fn clone(&self) -> Self {
        Self {
            info: self.info.clone(),
            data: self.data.clone(),
        }
    }
}

impl<'a, T: PageLayout> PageIter<'a, T> {
    #[allow(clippy::type_complexity)]
    fn next_internal(&mut self) -> Result<Option<(&'a T::Key, &'a T::Value)>, Error> {
        let Some(info) = self.info.next() else {
            self.data.next_none()?;
            return Ok(None);
        };
        let info = info?;
        let (key, val) = self.data.next_pair(info.key_len(), info.value_len())?;
        // Safety: we constructed our slices using the provided length numbers.
        unsafe { Ok(Some((info.read_key(key)?, info.read_value(val)?))) }
    }

    #[allow(clippy::type_complexity)]
    fn next_back_internal(&mut self) -> Result<Option<(&'a T::Key, &'a T::Value)>, Error> {
        let Some(info) = self.info.next_back() else {
            self.data.next_none()?;
            return Ok(None);
        };
        let info = info?;
        let (key, val) = self.data.next_pair_back(info.key_len(), info.value_len())?;
        // Safety: we constructed our slices using the provided length numbers.
        unsafe { Ok(Some((info.read_key(key)?, info.read_value(val)?))) }
    }
}

impl<'a, T: PageLayout> Iterator for PageIter<'a, T> {
    type Item = Result<(&'a T::Key, &'a T::Value), Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_internal().transpose()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.info.len() + 1))
    }
}

impl<'a, T: PageLayout> DoubleEndedIterator for PageIter<'a, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.next_back_internal().transpose()
    }
}

pub enum Entry<'a, 'k, T: PageLayout> {
    Occupied(OccupiedEntry<'a, T>),
    Vacant(VacantEntry<'a, 'k, T>),
}

/// An occupied entry in the map, ready to be inspected and modified.
pub struct OccupiedEntry<'a, T: PageLayout> {
    info: RevSizedArrayMut<'a, T>,
    trailer: &'a mut TwoArrayTrailer,
    kv: KeyValArrayMut<'a>,
}

impl<'a, T: PageLayout> OccupiedEntry<'a, T> {
    /// Get a reference to the key in the entry.
    pub fn key(&self) -> &T::Key {
        unsafe {
            let info = self.info.get();
            info.read_key(self.kv.key()).unwrap_unchecked()
        }
    }

    /// Get a reference to the value in the entry.
    pub fn get(&self) -> &T::Value {
        unsafe {
            let info = self.info.get();
            info.read_value(self.kv.val()).unwrap_unchecked()
        }
    }

    /// Get a mutable reference to the value in the entry.
    pub fn get_mut(&mut self) -> &mut T::Value {
        unsafe {
            let info = self.info.get_mut();
            info.update_value(self.kv.val_mut()).unwrap_unchecked()
        }
    }

    /// Delete the entire entry.
    pub fn delete(mut self) {
        // Delete the values from both arrays, then update the trailer lengths.
        unsafe {
            self.info.back_delete();
            let delta = self.kv.delete();
            self.trailer.add_to_lower_len(-delta);
            self.trailer.add_to_upper_len(-1);
        }
    }

    /// Replace the value with a new value.
    pub fn replace(&mut self, new_value: &T::Value) -> Result<(), Error> {
        let new_len = T::determine_value_len(new_value)?;
        let delta = (new_len as isize) - (self.kv.val().len() as isize);
        unsafe {
            // Check for the right size before resizing
            let free = CONTENT_SIZE - self.trailer.lengths_unchecked().total::<u8, T>();
            if (free as isize) < delta {
                return Err(Error::OutofSpace(delta as usize));
            }
            self.kv.resize(delta);
            self.trailer.add_to_lower_len(delta);

            // Update the value
            self.info
                .get_mut()
                .write_value(new_value, self.kv.val_mut());
            Ok(())
        }
    }
}

/// An empty entry in the map, ready to be filled.
pub struct VacantEntry<'a, 'k, T: PageLayout> {
    info: RevSizedArrayMut<'a, T>,
    trailer: &'a mut TwoArrayTrailer,
    kv: KeyValArrayMut<'a>,
    key: &'k T::Key,
}

impl<'a, 'k, T: PageLayout> VacantEntry<'a, 'k, T> {
    /// Get the key for this vacant entry.
    pub fn key(&self) -> &T::Key {
        unsafe { self.info.get().read_key(self.kv.key()).unwrap_unchecked() }
    }

    /// Insert a value into this entry, transforming into an occupied entry.
    pub fn insert(mut self, value: &T::Value) -> Result<OccupiedEntry<'a, T>, Error> {
        // Length calculations and checking
        let key_len = T::determine_key_len(self.key)?;
        let val_len = T::determine_value_len(value)?;
        let total_len = unsafe { self.trailer.lengths_unchecked().total::<u8, T>() };
        let free = CONTENT_SIZE - total_len;
        let needed = key_len + val_len + core::mem::size_of::<T>();
        if needed < free {
            return Err(Error::OutofSpace(needed));
        }

        unsafe {
            // Create the key-value allocation and initialize the info.
            self.kv.back_insert(key_len, val_len);
            self.trailer.add_to_lower_len((key_len + val_len) as isize);
            self.trailer.add_to_upper_len(1);
            self.info.back_insert(T::default());

            // Write out our key and value.
            let info = self.info.get_mut();
            info.write_key(self.key, self.kv.key_mut());
            info.write_value(value, self.kv.val_mut());
        }

        Ok(OccupiedEntry {
            info: self.info,
            trailer: self.trailer,
            kv: self.kv,
        })
    }
}
