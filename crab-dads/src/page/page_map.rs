use core::{cmp::Ordering, marker::PhantomData, slice};

use crab_dads_traits::Region;

use crate::{
    arrays::{KeyValArray, RevSizedArray},
    page::TRAILER_SIZE,
    ByteFormatter, Error, TwoArrayTrailer,
};

use super::{PageLayout, PageMapMut};

/// A single Page Map, providing immutable access to sorted key-value pairs
/// within a memory region.
#[repr(C)]
pub struct PageMap<'a, T: PageLayout> {
    page: *const u8,
    len: usize,
    layout: PhantomData<&'a T>,
}

impl<'a, T: PageLayout> Clone for PageMap<'a, T> {
    fn clone(&self) -> Self {
        Self {
            page: self.page,
            len: self.len,
            layout: PhantomData,
        }
    }
}

impl<'a, T: PageLayout> core::fmt::Debug for PageMap<'a, T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // SAFETY: the page trailer and basic integrity was verified when making a PageMap.
        let (lower, upper) = unsafe {
            let lengths = self.page_trailer().lengths_unchecked();
            let upper_bytes = lengths.upper_bytes();
            let lower = slice::from_raw_parts(self.page, lengths.lower_bytes());
            let upper = slice::from_raw_parts(
                self.page.add(self.len - TRAILER_SIZE - upper_bytes),
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

impl<'a, T: PageLayout> PageMap<'a, T> {
    /// Convert a page into a PageMap.
    /// 
    /// Verifies the page has correct physical layout, but does not check that
    /// the keys are ordered, which requires calling [`PageMap::verify`] after
    /// this.
    pub fn from_page(page: &Region) -> Result<Self, Error> {
        let ret = Self {
            page: page.as_ptr(),
            len: page.len(),
            layout: PhantomData,
        };
        let trailer = ret.page_trailer();
        trailer.lengths_layout::<T>(page.len() - TRAILER_SIZE)?;
        Ok(ret)
    }

    /// Get the trailer data for this map.
    pub fn page_trailer(&self) -> &'a TwoArrayTrailer {
        // SAFETY: constructing this struct required using a `Region`, where the
        // base pointer is page-aligned, and the length is a multiple of 4kiB.
        // This is enough to ensure this pointer math will always point to an
        // aligned trailer.
        unsafe { &*(self.page.byte_add(self.len - TRAILER_SIZE) as *const TwoArrayTrailer) }
    }

    /// Iterate over the data within the map.
    pub fn iter(&self) -> PageIter<'a, T> {
        unsafe {
            let lengths = self.page_trailer().lengths_unchecked();
            let data = KeyValArray::new(slice::from_raw_parts(self.page, lengths.lower_bytes()));
            let info = RevSizedArray::new(slice::from_raw_parts(
                self.page
                    .add(self.len - TRAILER_SIZE - lengths.upper_bytes())
                    as *const T,
                lengths.upper::<T>(),
            ));
            PageIter { info, data }
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn get_pair(&self, key: &T::Key) -> Result<Option<(&'a T::Key, &'a T::Value)>, Error> {
        for res in self.iter() {
            let (k, v) = res?;
            match k.cmp(key) {
                Ordering::Equal => return Ok(Some((k, v))),
                Ordering::Less => (),
                Ordering::Greater => return Ok(None),
            }
        }
        Ok(None)
    }

    pub fn get(&self, key: &T::Key) -> Result<Option<&'a T::Value>, Error> {
        let res = self.get_pair(key)?;
        let Some((_, v)) = res else { return Ok(None) };
        Ok(Some(v))
    }

    fn as_region(&self) -> &Region {
        // SAFETY: originally constructed with a region, and page/len never
        // change, so we can always go back to being a region again.
        unsafe { Region::from_raw_parts(self.page, self.len)}
    }

    /// Copy a page's content to a new page. The new page must be at least as
    /// large as the current page.
    pub fn copy_to<'b>(&self, dst: &'b mut Region) -> Result<PageMapMut<'b, T>, Error> {
        super::copy_page(self.as_region(), dst)?;
        PageMapMut::from_page(dst)
    }

    /// Verify the keys are all in order.
    pub fn verify(&self) -> Result<(), Error> {
        let mut last_key = None;
        for res in self.iter() {
            let (k, _) = res?;
            if let Some(last_key) = last_key {
                if last_key >= k {
                    return Err(Error::DataCorruption("Key ordering is incorrect"));
                }
            }
            last_key = Some(k);
        }
        Ok(())
    }
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
        unsafe { Ok(Some((info.read_key(key), info.read_value(val)))) }
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
        unsafe { Ok(Some((info.read_key(key), info.read_value(val)))) }
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
