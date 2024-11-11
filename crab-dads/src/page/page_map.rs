use core::{cmp::Ordering, marker::PhantomData, slice};

use crate::{
    arrays::{KeyValArray, RevSizedArray},
    Error, TwoArrayTrailer, PAGE_4K,
};

use super::{PageLayout, PageMapMut, CONTENT_SIZE};

#[repr(transparent)]
pub struct PageMap<'a, T: PageLayout> {
    layout: PhantomData<&'a T>,
    page: *const u8,
}

impl<'a, T: PageLayout> Clone for PageMap<'a, T> {
    fn clone(&self) -> Self {
        Self {
            page: self.page,
            layout: PhantomData,
        }
    }
}

impl<'a, T: PageLayout> PageMap<'a, T> {
    /// Convert a page into a PageMap
    pub fn from_page(page: &'a [u8; PAGE_4K]) -> Result<Self, Error> {
        let ret = Self {
            page: page.as_ptr(),
            layout: PhantomData,
        };
        let trailer = ret.page_trailer();
        trailer.lengths::<u8, T>(CONTENT_SIZE)?;
        Ok(ret)
    }

    /// Get the trailer data for this map.
    pub fn page_trailer(&self) -> &'a TwoArrayTrailer {
        unsafe { &*(self.page.byte_add(CONTENT_SIZE) as *const TwoArrayTrailer) }
    }

    /// Iterate over the data within the map.
    pub fn iter(&self) -> PageIter<'a, T> {
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

    #[allow(clippy::type_complexity)]
    pub fn get_pair(&self, key: &T::Key) -> Result<Option<(&'a T::Key, &'a T::Value)>, Error> {
        for res in self.iter() {
            let (k,v) = res?;
            match k.cmp(key) {
                Ordering::Equal => return Ok(Some((k,v))),
                Ordering::Greater => (),
                Ordering::Less => return Ok(None),
            }
        }
        Ok(None)
    }

    pub fn get(&self, key: &T::Key) -> Result<Option<&'a T::Value>, Error> {
        let res = self.get_pair(key)?;
        let Some((_, v)) = res else { return Ok(None) };
        Ok(Some(v))
    }

    /// Copy a page's content to a new page.
    pub fn copy_to<'b>(&self, dst: &'b mut [u8; PAGE_4K]) -> PageMapMut<'b, T> {
        unsafe {
            let lengths = self.page_trailer().lengths_unchecked();
            core::ptr::copy_nonoverlapping(self.page, dst.as_mut_ptr(), lengths.lower);
            let upper_bytes = lengths.upper_bytes::<T>() + core::mem::size_of::<TwoArrayTrailer>();
            let upper_offset = CONTENT_SIZE - upper_bytes;
            core::ptr::copy_nonoverlapping(
                self.page.add(upper_offset),
                dst.as_mut_ptr().add(upper_offset),
                upper_bytes,
            );
            PageMapMut {
                page: dst.as_mut_ptr(),
                layout: PhantomData,
            }
        }
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
