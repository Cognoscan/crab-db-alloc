mod reader;
mod writer;
use std::{borrow::Borrow, ops::RangeBounds};

pub use reader::*;
pub use writer::*;

use crate::{page, Error};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct BlockRange {
    pub start: u64,
    pub len: usize,
}

impl BlockRange {
    pub fn new(start: u64, len: usize) -> Self {
        Self { start, len }
    }
}

#[derive(Debug)]
pub struct WritableBlock {
    pub page: u64,
    pub start: *mut u8,
    pub len: usize,
}

pub struct BTreeVarU64Mut<'a, W: RawWrite> {
    page: u64,
    writer: &'a mut W,
}

impl<'a, W: RawWrite> BTreeVarU64Mut<'a, W> {
    pub fn new(writer: &'a mut W, page: u64) -> Self {
        Self { writer, page }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<u64>, Error> {
        BTreeRead::<page::LayoutVarU64, page::LayoutVarU64, W>::load(self.writer, self.page)?
            .get(key)
    }

    /*
    pub fn iter<'s, 'b, R: RangeBounds<&'b [u8]>>(
        &'s self,
        range: R,
    ) -> Result<impl Iterator<Item = Result<(&'b [u8], u64), Error>>, Error>
    where
        'a: 'b,
        's: 'b,
    {
        BTreeRead::<page::LayoutVarU64, page::LayoutVarU64, W>::load(self.writer, self.page)?
            .iter(range)
    }
    */
}

pub struct BTreeVarU64<'a, R: RawRead>(BTreeRead<'a, page::LayoutVarU64, page::LayoutVarU64, R>);

impl<'a, R: RawRead> BTreeVarU64<'a, R> {
    pub fn new(reader: &'a R, page: u64) -> Result<Self, Error> {
        Ok(Self(BTreeRead::load(reader, page)?))
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<u64>, Error> {
        self.0.get(key)
    }

    pub fn range<T, RANGE>(
        &self,
        range: RANGE,
    ) -> Result<impl Iterator<Item = Result<(&'a [u8], u64), Error>>, Error>
    where
        RANGE: RangeBounds<T>,
        &'a [u8]: Borrow<T>,
        T: Ord + ?Sized,
    {
        self.0.range(range)
    }
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeMap, BTreeSet};

    use crate::PAGE_4K;

    use super::*;

    #[derive(Default)]
    struct FakeBackend {
        pages: BTreeMap<u64, Vec<u8>>,
        counter: u64,
        dirty: BTreeSet<u64>,
    }

    impl RawRead for FakeBackend {
        fn load(&self, range: BlockRange) -> Option<&[u8]> {
            self.pages
                .get(&range.start)
                .and_then(|mem| mem.get(..range.len))
        }
    }

    impl RawWrite for FakeBackend {
        fn allocate(&mut self, size: usize) -> Option<WritableBlock> {
            let page = self.counter;
            self.counter += (size as u64 + 4095) >> 12;
            let mut memory = vec![0; size];
            let start = memory.as_mut_ptr();
            self.pages.insert(page, memory);
            self.dirty.insert(page);
            Some(WritableBlock {
                page,
                start,
                len: size,
            })
        }

        fn deallocate(&mut self, memory: BlockRange) {
            self.pages.remove(&memory.start);
            self.dirty.remove(&memory.start);
        }

        fn load_mut(&mut self, page: u64) -> Option<LoadMut> {
            if self.dirty.contains(&page) {
                let start = self.pages.get_mut(&page).map(|mem| mem.as_mut_ptr())?;
                Some(LoadMut::Dirty(WritableBlock {
                    page,
                    start,
                    len: PAGE_4K,
                }))
            } else {
                let write = self.allocate(PAGE_4K)?;
                let read = self.pages.get(&page).map(|mem| mem.as_ptr())?;
                Some(LoadMut::Clean { write, read })
            }
        }
    }

    #[test]
    fn simple_iter() {
        let backend = FakeBackend::default();

        let tree = BTreeVarU64::new(&backend, 0).unwrap();
        let empty: &[u8] = &[];
        let range = tree.range(empty..&[0u8, 1u8]).unwrap();
        for result in range {
            let (k,v) = result.unwrap();
            println!("k={:?}, v={}", k,v);
        }
    }
}
