use alloc::vec::Vec;
use core::{marker::PhantomData, slice};

use crate::{
    page::{self, PageLayout, PageMap, PageMapMut},
    Error,
};

use super::{reader::ReadPage, BTreeRead, LoadMut, RawWrite};

pub struct BTreeWrite<'a, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    W: RawWrite,
{
    writer: &'a W,
    root: u64,
    root_data: PhantomData<WritePage<'a, B, L>>,
}

pub(crate) enum WritePage<'a, B, L>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
{
    Branch(PageMapMut<'a, B>),
    Leaf(PageMapMut<'a, L>),
}

impl<'a, B, L> WritePage<'a, B, L>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
{
    fn try_load<W: RawWrite>(writer: &'a W, page: u64) -> Result<(Self, Option<u64>), Error> {
        unsafe {
            match writer.load_page_mut(page)? {
                LoadMut::Clean {
                    write,
                    write_page,
                    read,
                } => {
                    if (page::page_type(read) & 1) == 1 {
                        let read: PageMap<'a, L> = PageMap::from_page(read)?;
                        let write = read.copy_to(write);
                        Ok((WritePage::Leaf(write), Some(write_page)))
                    } else {
                        let read: PageMap<'a, B> = PageMap::from_page(read)?;
                        let write = read.copy_to(write);
                        Ok((WritePage::Branch(write), Some(write_page)))
                    }
                }
                LoadMut::Dirty(d) => {
                    if (page::page_type(d) & 1) == 1 {
                        Ok((WritePage::Leaf(PageMapMut::from_page(d)?), None))
                    } else {
                        Ok((WritePage::Branch(PageMapMut::from_page(d)?), None))
                    }
                }
            }
        }
    }
}

impl<'a, B, L, W> BTreeWrite<'a, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    W: RawWrite,
{
    /// Load in the root page of a tree.
    ///
    /// # Safety
    ///
    /// The provided page (and any child pages it may later navigate to) must
    /// all not be used mutably elsewhere in the program.
    pub unsafe fn load(writer: &'a W, page: u64) -> Result<(Self, Option<u64>), Error> {
        let (_, new_page) = WritePage::<B, L>::try_load(writer, page)?;
        let root = new_page.unwrap_or(page);
        Ok((
            Self {
                writer,
                root,
                root_data: PhantomData,
            },
            new_page,
        ))
    }

    pub fn as_read(&self) -> BTreeRead<'_, B, L, W> {
        unsafe { BTreeRead::load(self.writer, self.root).unwrap() }
    }

    pub fn entry<'b, 'k>(&'b mut self, key: &'k L::Key) -> Result<Entry<'b, 'k, B, L, W>, Error> {
        let (base, new_page) = WritePage::<B, L>::try_load(self.writer, self.root)?;
        // Single-leaf case

        loop {
            let base = match base {
                WritePage::Leaf(mut l) => match l.entry(key)? {
                    page::Entry::Occupied(e) => {
                        // Safety: This only works because we're mutably borrowing
                        // from this B-Tree until modification is done, and
                        // PageMapMut doesn't have any internal state that we need
                        // to maintain.
                        return Ok(Entry::Occupied(OccupiedEntry {
                            writer: self.writer,
                            entry: e,
                            leaf: l,
                            parents: Vec::new(),
                        }));
                    }
                    page::Entry::Vacant(e) => {
                        return Ok(Entry::Vacant(VacantEntry {
                            writer: self.writer,
                            entry: e,
                            leaf: l,
                            parents: Vec::new(),
                        }));
                    }
                },
                WritePage::Branch(b) => b,
            };

            base.iter_mut()
        }

        todo!()
    }
}

pub enum Entry<'a, 'k, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    W: RawWrite,
{
    Occupied(OccupiedEntry<'a, B, L, W>),
    Vacant(VacantEntry<'a, 'k, B, L, W>),
}

pub struct OccupiedEntry<'a, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    W: RawWrite,
{
    writer: &'a W,
    entry: page::OccupiedEntry<'a, L>,
    leaf: PageMapMut<'a, L>,
    parents: Vec<PageMapMut<'a, B>>,
}

pub struct VacantEntry<'a, 'k, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    W: RawWrite,
{
    writer: &'a W,
    entry: page::VacantEntry<'a, 'k, L>,
    leaf: PageMapMut<'a, L>,
    parents: Vec<PageMapMut<'a, B>>,
}
