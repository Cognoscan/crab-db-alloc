use core::{cmp::Ordering, marker::PhantomData, slice};

use crab_dads_traits::{PAGE_4K, Region};

use crate::{
    arrays::{KeyValArrayMut, KeyValArrayMutResize, RevSizedArray, RevSizedArrayMutResize},
    page::{PageLayout, PageLayoutVectored, PageMap, TRAILER_SIZE},
    ByteFormatter, Error, TwoArrayTrailer,
};

#[derive(Clone, Debug)]
struct Cutpoint {
    /// Number of bytes to take off the end of the lower byte region
    lower_len: usize,
    /// Number of bytes to take off the end of the upper byte region
    upper_bytes: usize,
}

pub enum Balance<'a, 'b, T: PageLayout> {
    Merged(PageMapMut<'a, T>),
    Balanced {
        lower: PageMapMut<'a, T>,
        higher: PageMapMut<'b, T>,
    },
}

/// A single mutable Page Map, providing access to sorted key-value pairs
/// within a memory region, along with the ability to insert and remove pairs.
#[repr(C)]
pub struct PageMapMut<'a, T: PageLayout> {
    page: *mut u8,
    len: usize,
    layout: PhantomData<&'a mut T>,
}

impl<'a, T: PageLayout> core::fmt::Debug for PageMapMut<'a, T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // SAFETY: the page trailer and basic integrity was verified when making a PageMap.
        let (lower, upper) = unsafe {
            let lengths = self.page_trailer().lengths_unchecked();
            let upper_bytes = lengths.upper_bytes();
            let lower = slice::from_raw_parts(self.page, lengths.lower_bytes());
            let upper = slice::from_raw_parts(
                self.page.add(self.trailer_offset() - upper_bytes),
                upper_bytes,
            );
            (lower, upper)
        };
        f.debug_struct(core::any::type_name::<Self>())
            .field("trailer", self.page_trailer())
            .field("lower_bytes", &ByteFormatter::new(lower))
            .field("upper_bytes", &ByteFormatter::new(upper))
            .finish()
    }
}

impl<'a, T: PageLayout> PageMapMut<'a, T> {

    /// Construct a new, empty map.
    pub fn new(page: &mut Region, page_type: u8) -> Self {
        let mut ret = Self {
            page: page.as_mut_ptr(),
            len: page.len(),
            layout: PhantomData,
        };
        let trailer = ret.page_trailer_mut();
        trailer.page_type = page_type;
        trailer.set_lower_len(0);
        trailer.set_upper_len(0);
        ret
    }

    pub fn to_page(self) -> &'a mut Region {
        // SAFETY: originally constructed with a region, and page/len never
        // change, so we can always go back to being a region again.
        unsafe { Region::from_raw_parts_mut(self.page, self.len) }
    }

    /// Convert a page into a `PageMapMut`.
    pub fn from_page(page: &mut Region) -> Result<Self, Error> {
        let ret = Self {
            page: page.as_mut_ptr(),
            len: page.len(),
            layout: PhantomData,
        };
        let trailer = ret.page_trailer();
        trailer.lengths_layout::<T>(ret.trailer_offset())?;
        Ok(ret)
    }

    fn trailer_offset(&self) -> usize {
        self.len - TRAILER_SIZE
    }

    /// Borrow for immutable use
    pub fn as_const(&self) -> &PageMap<'_, T> {
        // SAFETY: These types have the same layout and point to data with the
        // same page layout, ensuring all the same invariants are satisfied.
        unsafe { &*(self as *const PageMapMut<T> as *const PageMap<T>) }
    }

    fn find_cutpoint(&self, target: usize, max: usize) -> Result<Cutpoint, Error> {
        unsafe {
            let lengths = self.as_const().page_trailer().lengths_unchecked();
            let info = slice::from_raw_parts(
                self.page.add(self.trailer_offset() - lengths.upper_bytes()) as *mut T,
                lengths.upper::<T>(),
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
                    let mut new_upper_len_bytes = lengths.upper_bytes() - info.remaining_bytes();
                    // Determine if we actually take this final key-value pair or
                    // not. Choose whatever gets us closer to an even split.
                    if ((add_len + move_amount - target) > (target - move_amount))
                        || ((move_amount + add_len) > max)
                    {
                        // We don't want to take it.
                        new_upper_len_bytes -= core::mem::size_of::<T>();
                    } else {
                        // We do want to take it
                        taken_lower += pair_len;
                    }
                    if taken_lower > lengths.lower_bytes() {
                        return Err(Error::DataCorruption(
                            "Page cutpoint took more bytes than are in the lower page region",
                        ));
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
    pub fn split_to<'b>(&mut self, page: &mut Region) -> Result<PageMapMut<'b, T>, Error> {
        assert!(self.len <= page.len(), "New page must be at least as large as current page");
        let trailer = self.page_trailer();
        let page_type = trailer.page_type;

        unsafe {
            let lengths = trailer.lengths_unchecked();

            let mut new_page = PageMapMut::new(page, page_type);

            // Find the point at which we'll split the page
            let total_len = lengths.total();
            let cutpoint = self.find_cutpoint(total_len / 2, total_len)?;

            // Copy the data over
            let split_lower_len = lengths.lower_bytes() - cutpoint.lower_len;
            let upper_len_bytes = lengths.upper_bytes();
            let split_upper_len_bytes = upper_len_bytes - cutpoint.upper_bytes;
            core::ptr::copy_nonoverlapping(
                self.page.add(split_lower_len),
                new_page.page,
                cutpoint.lower_len,
            );
            core::ptr::copy_nonoverlapping(
                self.page.add(self.trailer_offset() - upper_len_bytes),
                new_page.page.add(self.trailer_offset() - cutpoint.upper_bytes),
                cutpoint.upper_bytes,
            );

            // Update both trailers
            debug_assert!(
                split_upper_len_bytes % core::mem::size_of::<T>() == 0,
                "Current page's upper length is not multiple of layout type"
            );
            debug_assert!(
                cutpoint.upper_bytes % core::mem::size_of::<T>() == 0,
                "New page's upper length is not multiple of layout type"
            );
            let trailer = self.page_trailer_mut();
            trailer.set_lower_len(split_lower_len as u16);
            trailer.set_upper_len(split_upper_len_bytes as u16);
            let new_trailer = new_page.page_trailer_mut();
            new_trailer.set_lower_len(cutpoint.lower_len as u16);
            new_trailer.set_upper_len(cutpoint.upper_bytes as u16);

            debug_assert!(
                self.as_const().verify().is_ok(),
                "newly split page should be valid"
            );
            debug_assert!(
                new_page.as_const().verify().is_ok(),
                "split-up page should be valid"
            );

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
    pub unsafe fn balance<'b>(
        mut self,
        mut higher: PageMapMut<'b, T>,
    ) -> Result<Balance<'a, 'b, T>, Error> {
        #[cfg(debug_assertions)]
        {
            let max0 = self.as_const().iter().next_back();
            let min1 = higher.as_const().iter().next();
            if let (Some(Ok((k0, _))), Some(Ok((k1, _)))) = (max0, min1) {
                debug_assert!(k0 < k1, "higher-numbered page fed to balancing function has a key lower than the highest key in this page, k0 = {k0:?}, k1 = {k1:?}");
            }
        }
        unsafe {
            let self_len = self.data_len();
            let higher_len = higher.data_len();

            if (self_len + higher_len) < self.trailer_offset() {
                // Copy the data
                let self_len = self.page_trailer().lengths_unchecked();
                let higher_len = higher.page_trailer().lengths_unchecked();
                core::ptr::copy_nonoverlapping(
                    higher.page,
                    self.page.add(self_len.lower_bytes()),
                    higher_len.lower_bytes(),
                );
                let upper_copy_len = higher_len.upper_bytes();
                core::ptr::copy_nonoverlapping(
                    higher.page.add(self.trailer_offset() - upper_copy_len),
                    self.page
                        .add(self.trailer_offset() - upper_copy_len - self_len.upper_bytes()),
                    upper_copy_len,
                );

                // Update the lengths
                let trailer_offset = self.trailer_offset();
                let trailer = self.page_trailer_mut();
                trailer.add_to_lower_len(higher_len.lower_bytes() as isize);
                trailer.add_to_upper_len(higher_len.upper_bytes() as isize);
                debug_assert!(trailer.lengths_layout::<T>(trailer_offset).is_ok());

                // Verify after changing
                debug_assert!(
                    self.as_const().verify().is_ok(),
                    "merged page should still be valid"
                );

                Ok(Balance::Merged(self))
            } else if self_len > higher_len {
                // Move from self to the higher page

                let cutpoint =
                    self.find_cutpoint((self_len - higher_len) / 2, higher.free_space())?;
                let self_len = self.page_trailer().lengths_unchecked();
                let higher_len = higher.page_trailer().lengths_unchecked();

                // Make room and then copy the lower data
                core::ptr::copy(
                    higher.page,
                    higher.page.add(cutpoint.lower_len),
                    higher_len.lower_bytes(),
                );
                core::ptr::copy_nonoverlapping(
                    self.page.add(self_len.lower_bytes() - cutpoint.lower_len),
                    higher.page,
                    cutpoint.lower_len,
                );

                // Make room and then copy the upper data
                let higher_upper_bytes = higher_len.upper_bytes();
                core::ptr::copy(
                    higher.page.add(self.trailer_offset() - higher_upper_bytes),
                    higher
                        .page
                        .add(self.trailer_offset() - higher_upper_bytes - cutpoint.upper_bytes),
                    higher_upper_bytes,
                );
                core::ptr::copy_nonoverlapping(
                    self.page.add(self.trailer_offset() - self_len.upper_bytes()),
                    higher.page.add(self.trailer_offset() - cutpoint.upper_bytes),
                    cutpoint.upper_bytes,
                );

                let lower_delta = cutpoint.lower_len as isize;
                let upper_delta = cutpoint.upper_bytes as isize;

                // Update the lower page's lengths
                let trailer_offset = self.trailer_offset();
                let trailer = self.page_trailer_mut();
                trailer.add_to_lower_len(-lower_delta);
                trailer.add_to_upper_len(-upper_delta);
                debug_assert!(trailer.lengths_layout::<T>(trailer_offset).is_ok());

                // Update the higher page's lengths
                let trailer_offset = higher.trailer_offset();
                let trailer = higher.page_trailer_mut();
                trailer.add_to_lower_len(lower_delta);
                trailer.add_to_upper_len(upper_delta);
                debug_assert!(trailer.lengths_layout::<T>(trailer_offset).is_ok());

                // Verify after changing
                debug_assert!(
                    self.as_const().verify().is_ok(),
                    "balanced lower page should still be valid"
                );
                debug_assert!(
                    higher.as_const().verify().is_ok(),
                    "balanced higher page should still be valid"
                );

                Ok(Balance::Balanced {
                    lower: self,
                    higher,
                })
            } else {
                // Move from the higher page to self

                let cutpoint =
                    higher.find_cutpoint((higher_len - self_len) / 2, self.free_space())?;
                let self_len = self.page_trailer().lengths_unchecked();
                let higher_len = higher.page_trailer().lengths_unchecked();

                // Copy the lower data, then delete it from the higher page
                core::ptr::copy_nonoverlapping(
                    higher.page,
                    self.page.add(self_len.lower_bytes()),
                    cutpoint.lower_len,
                );
                core::ptr::copy(
                    higher.page.add(cutpoint.lower_len),
                    higher.page,
                    higher_len.lower_bytes() - cutpoint.lower_len,
                );

                // Copy the upper data, then delete it from the higher page
                let higher_upper_bytes = higher_len.upper_bytes();
                let self_upper_bytes = self_len.upper_bytes();
                core::ptr::copy_nonoverlapping(
                    higher.page.add(self.trailer_offset() - cutpoint.upper_bytes),
                    self.page
                        .add(self.trailer_offset() - self_upper_bytes - cutpoint.upper_bytes),
                    cutpoint.upper_bytes,
                );
                core::ptr::copy(
                    higher.page.add(self.trailer_offset() - higher_upper_bytes),
                    higher
                        .page
                        .add(self.trailer_offset() - higher_upper_bytes + cutpoint.upper_bytes),
                    higher_upper_bytes - cutpoint.upper_bytes,
                );

                let lower_delta = cutpoint.lower_len as isize;
                let upper_delta = cutpoint.upper_bytes as isize;

                // Update this page's lengths
                let trailer_offset = self.trailer_offset();
                let trailer = self.page_trailer_mut();
                trailer.add_to_lower_len(lower_delta);
                trailer.add_to_upper_len(upper_delta);
                debug_assert!(trailer.lengths_layout::<T>(trailer_offset).is_ok());

                // Update the higher page's lengths
                let trailer_offset = higher.trailer_offset();
                let trailer = higher.page_trailer_mut();
                trailer.add_to_lower_len(-lower_delta);
                trailer.add_to_upper_len(-upper_delta);
                debug_assert!(trailer.lengths_layout::<T>(trailer_offset).is_ok());

                // Verify after changing
                debug_assert!(
                    self.as_const().verify().is_ok(),
                    "balanced lower page should still be valid"
                );
                debug_assert!(
                    higher.as_const().verify().is_ok(),
                    "balanced higher page should still be valid"
                );

                Ok(Balance::Balanced {
                    lower: self,
                    higher,
                })
            }
        }
    }

    /// Borrow the trailer.
    pub fn page_trailer(&self) -> &TwoArrayTrailer {
        // SAFETY: constructing this struct required using a `Region`, where the
        // base pointer is page-aligned, and the length is a multiple of 4kiB.
        // This is enough to ensure this pointer math will always point to an
        // aligned trailer.
        unsafe {
            &*(self
                .page
                .byte_add(self.trailer_offset())
                as *const TwoArrayTrailer)
        }
    }

    /// Borrow the trailer for modification.
    pub fn page_trailer_mut(&mut self) -> &mut TwoArrayTrailer {
        // SAFETY: constructing this struct required using a `Region`, where the
        // base pointer is page-aligned, and the length is a multiple of 4kiB.
        // This is enough to ensure this pointer math will always point to an
        // aligned trailer.
        unsafe {
            &mut *(self
                .page
                .byte_add(self.trailer_offset())
                as *mut TwoArrayTrailer)
        }
    }

    /// Get how much free space is in the page.
    pub fn free_space(&self) -> usize {
        let lengths = unsafe { self.as_const().page_trailer().lengths_unchecked() };
        self.trailer_offset() - lengths.total()
    }

    /// Get if the page should be rebalanced / merged
    pub fn should_balance(&self) -> bool {
        self.free_space() > ((3 * self.trailer_offset()) / 4)
    }

    /// Get how many bytes of data are in the page.
    pub fn data_len(&self) -> usize {
        // SAFETY: lengths were checked on construction of this struct, and no
        // operation within the struct will make them become invalid.
        unsafe { self.page_trailer().lengths_unchecked().total() }
    }

    /// Get how many 4K pages are in the page.
    pub fn num_4k_pages(&self) -> usize {
        self.len.div_ceil(PAGE_4K)
    }

    /// Iterate over the data within the map, with mutable access to the values.
    pub fn iter_mut(&mut self) -> PageIterMut<'_, T> {
        unsafe {
            let lengths = self.page_trailer().lengths_unchecked();
            let data =
                KeyValArrayMut::new(slice::from_raw_parts_mut(self.page, lengths.lower_bytes()));
            let info = RevSizedArray::new(slice::from_raw_parts(
                self.page.add(self.trailer_offset() - lengths.upper_bytes()) as *const T,
                lengths.upper::<T>(),
            ));
            PageIterMut { info, data }
        }
    }

    /// Get an entry in the page.
    pub fn entry<'k>(self, key: &'k T::Key) -> Result<Entry<'a, 'k, T>, Error> {
        unsafe {
            // Extract the trailer and info inside it
            let trailer = &mut *(self
                .page
                .byte_add(self.trailer_offset())
                as *mut TwoArrayTrailer);
            let lengths = trailer.lengths_unchecked();

            // Construct the two array iterators
            let mut kv = crate::arrays::KeyValArrayMutResize::new(slice::from_raw_parts_mut(
                self.page,
                lengths.lower_bytes(),
            ));
            let info = slice::from_raw_parts_mut(
                self.page.add(self.trailer_offset() - lengths.upper_bytes()) as *mut T,
                lengths.upper::<T>(),
            );
            let mut info = crate::arrays::RevSizedArrayMutResize::new(info);

            while let Some(i) = info.next_back() {
                let i = i?;
                kv.next_pair_back(i.key_len(), i.value_len())?;
                match i.read_key(kv.key()).cmp(key) {
                    Ordering::Equal => {
                        return Ok(Entry::Occupied(OccupiedEntry {
                            page: self.page,
                            len: self.len,
                            first: info.remaining_bytes() == 0,
                            trailer,
                            kv,
                            info,
                        }))
                    }
                    Ordering::Less => {
                        return Ok(Entry::Vacant(VacantEntry {
                            page: self.page,
                            len: self.len,
                            first: false,
                            trailer,
                            kv,
                            info,
                            key,
                        }))
                    }
                    Ordering::Greater => (),
                }
            }
            kv.next_pair_back_none()?;
            Ok(Entry::Vacant(VacantEntry {
                page: self.page,
                len: self.len,
                first: true,
                info,
                trailer,
                kv,
                key,
            }))
        }
    }
}

impl<'a, T: PageLayout> IntoIterator for PageMapMut<'a, T> {
    type IntoIter = PageIterMut<'a, T>;
    type Item = Result<(&'a T::Key, &'a mut T::Value), Error>;
    /// Turn this page into an iterator over the map, with mutable access to the values.
    fn into_iter(self) -> PageIterMut<'a, T> {
        unsafe {
            let lengths = self.page_trailer().lengths_unchecked();
            let data =
                KeyValArrayMut::new(slice::from_raw_parts_mut(self.page, lengths.lower_bytes()));
            let info = RevSizedArray::new(slice::from_raw_parts(
                self.page.add(self.trailer_offset() - lengths.upper_bytes()) as *const T,
                lengths.upper::<T>(),
            ));
            PageIterMut { info, data }
        }
    }
}

pub struct PageIterMut<'a, T: PageLayout> {
    info: RevSizedArray<'a, T>,
    data: KeyValArrayMut<'a>,
}

impl<'a, T: PageLayout> PageIterMut<'a, T> {
    #[allow(clippy::type_complexity)]
    fn next_internal(&mut self) -> Result<Option<(&'a T::Key, &'a mut T::Value)>, Error> {
        let Some(info) = self.info.next() else {
            self.data.next_none()?;
            return Ok(None);
        };
        let info = info?;
        let (raw_key, raw_val) = self.data.next_pair(info.key_len(), info.value_len())?;
        // Safety: we constructed our slices using the provided length numbers.
        unsafe {
            let key = info.read_key(raw_key);
            let val = info.update_value(raw_val);
            Ok(Some((key, val)))
        }
    }

    #[allow(clippy::type_complexity)]
    fn next_back_internal(&mut self) -> Result<Option<(&'a T::Key, &'a mut T::Value)>, Error> {
        let Some(info) = self.info.next_back() else {
            self.data.next_none()?;
            return Ok(None);
        };
        let info = info?;
        let (raw_key, raw_val) = self.data.next_pair_back(info.key_len(), info.value_len())?;
        // Safety: we constructed our slices using the provided length numbers.
        unsafe {
            let key = info.read_key(raw_key);
            let val = info.update_value(raw_val);
            Ok(Some((key, val)))
        }
    }
}

impl<'a, T: PageLayout> Iterator for PageIterMut<'a, T> {
    type Item = Result<(&'a T::Key, &'a mut T::Value), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_internal().transpose()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.info.len() + 1))
    }
}

impl<'a, T: PageLayout> DoubleEndedIterator for PageIterMut<'a, T> {
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
    page: *mut u8,
    len: usize,
    first: bool,
    info: RevSizedArrayMutResize<'a, T>,
    trailer: &'a mut TwoArrayTrailer,
    kv: KeyValArrayMutResize<'a>,
}

impl<'a, T: PageLayout> OccupiedEntry<'a, T> {
    /// Returns true if this is the first entry in the page.
    pub fn first(&self) -> bool {
        self.first
    }

    /// Get a reference to the key in the entry.
    pub fn key(&self) -> &T::Key {
        unsafe {
            let info = self.info.get();
            info.read_key(self.kv.key())
        }
    }

    /// Get a reference to the value in the entry.
    pub fn get(&self) -> &T::Value {
        unsafe {
            let info = self.info.get();
            info.read_value(self.kv.val())
        }
    }

    /// Get a mutable reference to the value in the entry.
    pub fn get_mut(&mut self) -> &mut T::Value {
        unsafe {
            let info = self.info.get_mut();
            info.update_value(self.kv.val_mut())
        }
    }

    /// Delete the entire entry.
    pub fn delete(mut self) -> PageMapMut<'a, T> {
        // Delete the values from both arrays, then update the trailer lengths.
        unsafe {
            self.info.back_delete();
            let delta = self.kv.delete();
            self.trailer.add_to_lower_len(-delta);
            self.trailer
                .add_to_upper_len(-(core::mem::size_of::<T>() as isize));
        }

        PageMapMut {
            layout: PhantomData,
            page: self.page,
            len: self.len,
        }
    }

    /// Replace the value with a new value.
    pub fn replace(&mut self, new_value: &T::Value) -> Result<(), Error> {
        let new_len = T::determine_value_len(new_value)?;
        let delta = (new_len as isize) - (self.kv.val().len() as isize);
        unsafe {
            // Check for the right size before resizing
            let free = self.len - TRAILER_SIZE - self.trailer.lengths_unchecked().total();
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

    /// Drop the entry and return to being a regular page.
    pub fn to_page(self) -> PageMapMut<'a, T> {
        PageMapMut {
            layout: PhantomData,
            page: self.page,
            len: self.len,
        }
    }
}

impl<'a, T> OccupiedEntry<'a, T>
where
    T: PageLayoutVectored,
{
    pub fn replace_vectored(&mut self, new_value: &[&T::Value]) -> Result<(), Error> {
        let new_len = T::determine_value_len_vectored(new_value)?;
        let delta = (new_len as isize) - (self.kv.val().len() as isize);
        unsafe {
            // Check for the right size before resizing
            let free = self.len - TRAILER_SIZE - self.trailer.lengths_unchecked().total();
            if (free as isize) < delta {
                return Err(Error::OutofSpace(delta as usize));
            }
            self.kv.resize(delta);
            self.trailer.add_to_lower_len(delta);

            // Update the value
            self.info
                .get_mut()
                .write_value_vectored(new_value, self.kv.val_mut());
            Ok(())
        }
    }
}

/// An empty entry in the map, ready to be filled.
pub struct VacantEntry<'a, 'k, T: PageLayout> {
    page: *mut u8,
    len: usize,
    first: bool,
    info: RevSizedArrayMutResize<'a, T>,
    trailer: &'a mut TwoArrayTrailer,
    kv: KeyValArrayMutResize<'a>,
    key: &'k T::Key,
}

impl<'a, 'k, T: PageLayout> VacantEntry<'a, 'k, T> {
    /// Get if this will become the first entry in the page when inserted into.
    pub fn first(&self) -> bool {
        self.first
    }

    /// Get the key for this vacant entry.
    pub fn key(&self) -> &T::Key {
        unsafe { self.info.get().read_key(self.kv.key()) }
    }

    /// Insert a value into this entry, transforming into an occupied entry.
    pub fn insert(mut self, value: &T::Value) -> Result<OccupiedEntry<'a, T>, (Self, Error)> {
        // Length calculations and checking
        let key_len = match T::determine_key_len(self.key) {
            Ok(len) => len,
            Err(e) => return Err((self, e)),
        };
        let val_len = match T::determine_value_len(value) {
            Ok(len) => len,
            Err(e) => return Err((self, e)),
        };
        let total_len = unsafe { self.trailer.lengths_unchecked().total() };
        let free = self.len - TRAILER_SIZE - total_len;
        let needed = key_len + val_len + core::mem::size_of::<T>();
        if needed > free {
            return Err((self, Error::OutofSpace(needed)));
        }

        unsafe {
            // Create the key-value allocation and initialize the info.
            self.kv.back_insert(key_len, val_len);
            self.trailer.add_to_lower_len((key_len + val_len) as isize);
            self.trailer
                .add_to_upper_len(core::mem::size_of::<T>() as isize);
            self.info.back_insert(T::default());

            // Write out our key and value.
            let info = self.info.get_mut();
            info.write_key(self.key, self.kv.key_mut());
            info.write_value(value, self.kv.val_mut());
        }

        Ok(OccupiedEntry {
            page: self.page,
            len: self.len,
            first: self.first,
            info: self.info,
            trailer: self.trailer,
            kv: self.kv,
        })
    }

    /// Drop the entry and return to being a regular page.
    pub fn to_page(self) -> PageMapMut<'a, T> {
        PageMapMut {
            layout: PhantomData,
            page: self.page,
            len: self.len,
        }
    }
}

impl<'a, 'k, T> VacantEntry<'a, 'k, T>
where
    T: PageLayoutVectored,
{
    pub fn insert_vectored(
        mut self,
        value: &[&T::Value],
    ) -> Result<OccupiedEntry<'a, T>, (Self, Error)> {
        // Length calculations and checking
        let key_len = match T::determine_key_len(self.key) {
            Ok(len) => len,
            Err(e) => return Err((self, e)),
        };
        let val_len = match T::determine_value_len_vectored(value) {
            Ok(len) => len,
            Err(e) => return Err((self, e)),
        };
        let total_len = unsafe { self.trailer.lengths_unchecked().total() };
        let free = self.len - TRAILER_SIZE - total_len;
        let needed = key_len + val_len + core::mem::size_of::<T>();
        if needed < free {
            return Err((self, Error::OutofSpace(needed)));
        }

        unsafe {
            // Create the key-value allocation and initialize the info.
            self.kv.back_insert(key_len, val_len);
            self.trailer.add_to_lower_len((key_len + val_len) as isize);
            self.trailer
                .add_to_upper_len(core::mem::size_of::<T>() as isize);
            self.info.back_insert(T::default());

            // Write out our key and value.
            let info = self.info.get_mut();
            info.write_key(self.key, self.kv.key_mut());
            info.write_value_vectored(value, self.kv.val_mut());
        }

        Ok(OccupiedEntry {
            first: self.first,
            page: self.page,
            len: self.len,
            info: self.info,
            trailer: self.trailer,
            kv: self.kv,
        })
    }
}
