use core::marker::PhantomData;

use crate::{page::{self, PageLayout}, Error, PAGE_4K};

use super::{BlockRange, RawRead, WritableBlock};

pub enum LoadMut<'a> {
    Clean {
        write: WritableBlock<'a>,
        read: &'a [u8],
    },
    Dirty(WritableBlock<'a>),
}

pub trait RawWrite: RawRead {
    /// Load a memory range for writing. If out of range of the backing store,
    /// it should return None. If the range that's been requested is not
    /// available for writing, it should return the [`Clean`][LoadMut::Clean]
    /// result with a newly allocated page to write to. If the range is
    /// available for writing, then [`Dirty`][LoadMut::Dirty] should be returned
    /// instead.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the requested range won't be used mutably in
    /// a different thread - the `RawWrite` object doesn't need to enforce this.
    unsafe fn load_mut(&mut self, page: u64) -> Option<LoadMut>;

    /// Allocate a region of memory for writing to.
    fn allocate(&mut self, size: usize) -> Option<WritableBlock>;

    /// Deallocate a region of memory that was previously allocated through
    /// `load_mut` or `allocate`.
    /// 
    /// # Safety
    /// 
    /// The caller must ensure the region of memory was previously allocated by
    /// this writer's underlying datastore, using `load_mut` or `allocate`.
    unsafe fn deallocate(&mut self, memory: BlockRange);
}

pub struct BTreeWrite<'a, B, L, W>
where
    B: PageLayout<'a, Value = u64>,
    L: PageLayout<'a, Key = B::Key>,
    W: RawWrite,
{
    writer: &'a mut W,
    root: WritableBlock<'a>,
    branches: PhantomData<B>,
    leaf: PhantomData<L>,
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
    pub unsafe fn load(writer: &'a mut W, page: u64) -> Result<Self, Error> {
        let start = page * (PAGE_4K as u64);
        let root = writer
            .load_mut(start)
            .ok_or(Error::DataCorruption)?;
        let root = match root {
            LoadMut::Dirty(d) => d,
            LoadMut::Clean { write, read } => {
                page::copy_page(read, write.block)?;
                write
            }
        };
        todo!()
    }
}
