use std::{marker::PhantomData, ops::RangeBounds};

use crate::{page::PageLayout, BTreeError, Error};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct BlockRange {
    pub start: u64,
    pub len: usize,
}

impl BlockRange {
    pub fn new(start: u64, len: usize) -> Self {
        Self { start, len }
    }
}

/// Access to a backing reader.
pub trait RawRead {
    /// Load a memory range. If out of range of the backing store, it should
    /// return None.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the requested range won't be used mutably
    /// elsewhere in the program - the RawRead object doesn't need to enforce
    /// this.
    unsafe fn load(&self, range: BlockRange) -> Option<&[u8]>;
}

pub struct BTreeRead<'a, B, L, R>
where
    B: PageLayout<'a, Value = u64>,
    L: PageLayout<'a, Key = B::Key>,
    R: RawRead,
{
    reader: &'a R,
    root: &'a [u8],
    branches: PhantomData<B>,
    leaf: PhantomData<L>,
}

impl<'a, B, L, R> BTreeRead<'a, B, L, R>
where
    B: PageLayout<'a, Value = u64>,
    L: PageLayout<'a, Key = B::Key>,
    R: RawRead,
{
    /// Load in the root page of a tree.
    ///
    /// # Safety
    ///
    /// The provided page (and any child pages it may later navigate to) must
    /// all not be used mutably elsewhere in the program.
    pub unsafe fn load(reader: &'a R, page: u64) -> Result<Self, BTreeError> {
        let start = page << 12;
        let root = reader
            .load(BlockRange { start, len: 4096 })
            .ok_or(BTreeError::DataCorruption { trace: Vec::new() })?;
        Ok(Self {
            reader,
            root,
            branches: PhantomData,
            leaf: PhantomData,
        })
    }

    fn iter(&self, range: R) -> Result<BTreeIter<'a, B, L, R>, BTreeError>
    where
        R: RangeBounds<L::Key>,
    {
        todo!()
        //Ok(BTreeIter {
        //    inner: (),
        //    left: (),
        //    right: (),
        //})
    }
}

pub struct BTreeIter<'a, B, L, R>
where
    B: PageLayout<'a, Value = u64>,
    L: PageLayout<'a, Key = B::Key>,
    R: RawRead,
{
    inner: BTreeRead<'a, B, L, R>,
    left: Vec<&'a [u8]>,
    right: Vec<&'a [u8]>,
}

impl<'a, B, L, R> Iterator for BTreeIter<'a, B, L, R>
where
    B: PageLayout<'a, Value = u64>,
    L: PageLayout<'a, Key = B::Key>,
    R: RawRead,
{
    type Item = Result<(L::Key, B::Value), BTreeError>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
