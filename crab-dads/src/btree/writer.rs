use core::slice;

use crate::{
    page::{self, PageLayout, PageMap},
    Error, PAGE_4K,
};

use super::{reader::ReadPage, BlockRange, RawRead, WritableBlock};

pub enum LoadMut {
    Clean { write: WritableBlock, read: *const u8 },
    Dirty(WritableBlock),
}

pub trait RawWrite: RawRead {
    /// Load a memory range for writing. If out of range of the backing store,
    /// it should return None. If the range that's been requested is not
    /// available for writing, it should return the [`Clean`][LoadMut::Clean]
    /// result with a newly allocated page to write to. If the range is
    /// available for writing, then [`Dirty`][LoadMut::Dirty] should be returned
    /// instead.
    fn load_mut(&mut self, page: u64) -> Option<LoadMut>;

    /// Allocate a region of memory for writing to.
    fn allocate(&mut self, size: usize) -> Option<WritableBlock>;

    /// Deallocate a region of memory that was previously allocated through
    /// `load_mut` or `allocate`.
    fn deallocate(&mut self, memory: BlockRange);
}

pub struct BTreeWrite<'a, B, L, W>
where
    B: PageLayout<'a, Value = u64>,
    L: PageLayout<'a, Key = B::Key>,
    W: RawWrite,
{
    writer: &'a mut W,
    root: WritePage<'a, B, L>,
}

pub(crate) enum WritePage<'a, B, L>
where
    B: PageLayout<'a, Value = u64>,
    L: PageLayout<'a, Key = B::Key>,
{
    Branch(&'a mut PageMap<'a, B>),
    Leaf(&'a mut PageMap<'a, L>),
}

impl<'a, B, L> TryFrom<&'a mut [u8]> for WritePage<'a, B, L>
where
    B: PageLayout<'a, Value = u64>,
    L: PageLayout<'a, Key = B::Key>,
{
    type Error = Error;

    fn try_from(value: &'a mut [u8]) -> Result<Self, Self::Error> {
        let page_type = page::page_type(value)?;
        Ok(if (page_type & 1) == 1 {
            WritePage::Leaf(value.try_into()?)
        } else {
            WritePage::Branch(value.try_into()?)
        })
    }
}

impl<'a, B, L> WritePage<'a, B, L>
where
    B: PageLayout<'a, Value = u64>,
    L: PageLayout<'a, Key = B::Key>,
{

    fn try_load<W: RawWrite>(writer: &'a mut W, page: u64) -> Result<Self, Error> {
        todo!()
    }
}

impl<'a, B, L, W> BTreeWrite<'a, B, L, W>
where
    B: PageLayout<'a, Value = u64>,
    L: PageLayout<'a, Key = B::Key>,
    W: RawWrite,
{
    /// Load in the root page of a tree.
    ///
    /// # Safety
    ///
    /// The provided page (and any child pages it may later navigate to) must
    /// all not be used mutably elsewhere in the program.
    pub unsafe fn load(writer: &'a mut W, page: u64) -> Result<(Self, Option<u64>), Error> {
        let start = page * (PAGE_4K as u64);
        let root = writer.load_mut(start).ok_or(Error::DataCorruption)?;
        let (root, new_page) = match root {
            LoadMut::Dirty(d) => (
                WritePage::try_from(slice::from_raw_parts_mut(d.start, PAGE_4K))?,
                None,
            ),
            LoadMut::Clean { write, read } => {
                let block = slice::from_raw_parts_mut(write.start, PAGE_4K);
                let read = slice::from_raw_parts(read, PAGE_4K);
                let page_num = write.page;
                let root = match ReadPage::try_from(read)? {
                    ReadPage::Branch(b) => WritePage::Branch::<B, L>(b.copy_to(block)?),
                    ReadPage::Leaf(l) => WritePage::Leaf(l.copy_to(block)?),
                };
                (root, Some(page_num))
            }
        };

        Ok((Self { writer, root }, new_page))
    }

}

