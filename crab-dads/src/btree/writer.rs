use alloc::vec::Vec;
use core::{marker::PhantomData, slice};

use crate::{
    page::{self, PageLayout, PageLayoutVectored, PageMap, PageMapMut},
    Error,
};

use super::{reader::ReadPage, BTreeRead, LoadMutPage, RawWrite};

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
            match writer.load_mut_page(page)? {
                LoadMutPage::Clean {
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
                LoadMutPage::Dirty(d) => {
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
        let (mut page, _) = WritePage::<B, L>::try_load(self.writer, self.root)?;
        let mut page_num = self.root;

        let mut parents = Vec::new();
        let mut depth = 0;

        loop {
            let mut branch_page = match page {
                WritePage::Leaf(l) => match l.entry(key)? {
                    page::Entry::Occupied(e) => {
                        // Safety: This only works because we're mutably borrowing
                        // from this B-Tree until modification is done, and
                        // PageMapMut doesn't have any internal state that we need
                        // to maintain.
                        return Ok(Entry::Occupied(OccupiedEntry {
                            content: EntryContent {
                                writer: self.writer,
                                leaf_data: PhantomData,
                                leaf: page_num,
                                parents,
                                key,
                            },
                            entry: e,
                        }));
                    }
                    page::Entry::Vacant(e) => {
                        return Ok(Entry::Vacant(VacantEntry {
                            content: EntryContent {
                                writer: self.writer,
                                leaf_data: PhantomData,
                                leaf: page_num,
                                parents,
                                key,
                            },
                            entry: e,
                        }));
                    }
                },
                WritePage::Branch(b) => b,
            };

            // Seek the appropriate sub-page in the branch.
            let mut val = None;
            for res in branch_page.iter_mut().rev() {
                let (k, v) = res?;
                if k <= key {
                    val = Some(v);
                    break;
                }
            }
            let val = val.ok_or(Error::DataCorruption)?;

            // Load the next page
            let (write_page, write_page_num) = WritePage::<B, L>::try_load(self.writer, *val)?;
            page = write_page;
            if let Some(write_page_num) = write_page_num {
                *val = write_page_num;
            }

            // Store the branch page off for potential future use
            let branch_page_num = page_num;
            page_num = *val;
            parents.push((branch_page, branch_page_num));

            // There's no way on earth you've got more than 2^64 items in your
            // tree, something is screwy.
            depth += 1;
            if depth > 64 {
                return Err(Error::DataCorruption);
            }
        }
    }
}

pub enum Entry<'a, 'k, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    W: RawWrite,
{
    Occupied(OccupiedEntry<'a, 'k, B, L, W>),
    Vacant(VacantEntry<'a, 'k, B, L, W>),
}

struct EntryContent<'a, 'k, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    W: RawWrite,
{
    writer: &'a W,
    leaf: u64,
    leaf_data: PhantomData<L>,
    parents: Vec<(PageMapMut<'a, B>, u64)>,
    key: &'k L::Key,
}

impl<'a, 'k, B, L, W> Drop for EntryContent<'a, 'k, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    W: RawWrite,
{
    fn drop(&mut self) {
        unsafe {
            self.writer.unload_mut_page(self.leaf);
            for page in self.parents.iter() {
                self.writer.unload_mut_page(page.1);
            }
        }
    }
}

pub struct OccupiedEntry<'a, 'k, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    W: RawWrite,
{
    content: EntryContent<'a, 'k, B, L, W>,
    entry: page::OccupiedEntry<'a, L>,
}

impl<'a, 'k, B, L, W> OccupiedEntry<'a, 'k, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    W: RawWrite,
{
    pub fn key(&self) -> &B::Key {
        self.entry.key()
    }

    pub fn get(&self) -> &L::Value {
        self.entry.get()
    }

    pub fn get_mut(&mut self) -> &mut L::Value {
        self.entry.get_mut()
    }

    pub fn delete(mut self) -> Result<(), Error> {
        self.entry.delete();
        todo!("Check for if we should rebalance this page or not")
    }

    pub fn replace(mut self, new_value: &L::Value) -> Result<Self, Error> {
        // Try and replace normally first
        match self.entry.replace(new_value) {
            Ok(()) => return Ok(self),
            Err(Error::OutofSpace(_)) => (),
            Err(e) => return Err(e),
        }

        // We need to split the page
        let mut leaf = self.entry.to_page();
        let (new_page, mut new_page_num) = self.content.writer.allocate_page()?;
        let mut new_page = leaf.split_to(new_page)?;

        // Try and insert on the first branch
        let Some(p) = self.content.parents.pop() else {
            // We need to create a branch where none previously existed, and we
            // need to use this page to do it, since the parent to this whole
            // tree is assuming that *this* page is the root of the tree.

            // Copy the page to a new leaf.
            let (copy_leaf, copy_leaf_num) = self.content.writer.allocate_page()?;
            let copy_leaf = leaf.as_const().copy_to(copy_leaf);
            let page_type = leaf.page_trailer().page_type & 0xFE;

            // Create the branch
            let branch: PageMapMut<'_, B> = PageMapMut::new(leaf.to_page(), page_type);

            // Load in the first page's info
            let (k, _) = copy_leaf
                .as_const()
                .iter()
                .next()
                .ok_or(Error::DataCorruption)??;
            let branch = match branch.entry(k)? {
                page::Entry::Occupied(_) => return Err(Error::DataCorruption),
                page::Entry::Vacant(v) => v.insert(&copy_leaf_num)?.to_page(),
            };

            // Load in the second page's info.
            let (k2, _) = new_page
                .as_const()
                .iter()
                .next()
                .ok_or(Error::DataCorruption)??;
            let branch = match branch.entry(k)? {
                page::Entry::Occupied(_) => return Err(Error::DataCorruption),
                page::Entry::Vacant(v) => v.insert(&new_page_num)?.to_page(),
            };

            // Push the new branch on.
            self.content.parents.push((branch, self.content.leaf));

            // Figure out which of the two leafs we should insert into, and do so.
            let e = if self.content.key < k2 {
                unsafe {
                    self.content.writer.unload_mut_page(new_page_num);
                }
                copy_leaf.entry(self.content.key)
            } else {
                unsafe {
                    self.content.writer.unload_mut_page(copy_leaf_num);
                }
                new_page.entry(self.content.key)
            };
            let e = match e? {
                page::Entry::Occupied(mut e) => {
                    e.replace(new_value)?;
                    e
                }
                page::Entry::Vacant(_) => return Err(Error::DataCorruption),
            };
            return Ok(Self {
                content: self.content,
                entry: e,
            });
        };

        // Now we insert the split page into the tree, which might be recursive....

        // Retry the replacement
        todo!()
    }
}

impl<'a, 'k, B, L, W> OccupiedEntry<'a, 'k, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayoutVectored + PageLayout<Key = B::Key>,
    W: RawWrite,
{
    pub fn replace_vectored(&mut self, new_value: &[&L::Value]) -> Result<(), Error> {
        todo!()
    }
}

pub struct VacantEntry<'a, 'k, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    W: RawWrite,
{
    content: EntryContent<'a, 'k, B, L, W>,
    entry: page::VacantEntry<'a, 'k, L>,
}
