use alloc::vec::Vec;

use crate::{
    page::{self, Balance, PageLayout, PageLayoutVectored, PageMap, PageMapMut},
    Error, PAGE_4K,
};

use super::{reader::ReadPage, BTreeRead, LoadMutPage, RawWrite};

pub struct BTreeWrite<'a, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    W: RawWrite,
{
    writer: &'a W,
    branches: Vec<(PageMapMut<'a, B>, u64)>,
    leaf: Option<(PageMapMut<'a, L>, u64)>,
    root: u64,
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
        let (root, new_page) = WritePage::<B, L>::try_load(writer, page)?;
        let root_page_num = new_page.unwrap_or(page);
        let mut s = Self {
            writer,
            branches: Vec::new(),
            leaf: None,
            root: page,
        };
        match root {
            WritePage::Branch(b) => s.branches.push((b, root_page_num)),
            WritePage::Leaf(l) => s.leaf = Some((l, root_page_num)),
        };
        Ok((s, new_page))
    }

    /// Turn into a temporary reader
    pub fn as_read(&mut self) -> BTreeRead<'_, B, L, W> {
        // Loan out the root page
        let root = if let Some(l) = &self.leaf {
            ReadPage::Leaf(l.0.as_const().clone())
        } else {
            ReadPage::Branch(
                self.branches
                    .first()
                    .expect("Branch vec must be nonempty")
                    .0
                    .as_const()
                    .clone(),
            )
        };
        unsafe { BTreeRead::from_parts(self.writer, root) }
    }

    pub fn entry<'b, 'k>(
        &'b mut self,
        key: &'k L::Key,
    ) -> Result<Entry<'a, 'b, 'k, B, L, W>, Error> {
        // Clear out any descent into the tree that we'd previously done
        self.branches.truncate(1);

        // Extract our root page
        let (mut page, mut page_num) = if let Some(l) = self.leaf.take() {
            (WritePage::Leaf(l.0), l.1)
        } else if let Some(b) = self.branches.pop() {
            (WritePage::Branch(b.0), b.1)
        } else {
            (WritePage::try_load(self.writer, self.root)?.0, self.root)
        };

        let mut depth = 0;
        let mut first = false;
        loop {
            let mut branch_page = match page {
                WritePage::Leaf(l) => match l.entry(key)? {
                    page::Entry::Occupied(e) => {
                        // Safety: This only works because we're mutably borrowing
                        // from this B-Tree until modification is done, and
                        // PageMapMut doesn't have any internal state that we need
                        // to maintain.
                        return Ok(Entry::Occupied(OccupiedEntry {
                            tree: self,
                            key,
                            entry: e,
                            entry_page_num: page_num,
                            first,
                        }));
                    }
                    page::Entry::Vacant(e) => {
                        return Ok(Entry::Vacant(VacantEntry {
                            tree: self,
                            key,
                            entry: e,
                            entry_page_num: page_num,
                            first,
                        }));
                    }
                },
                WritePage::Branch(b) => b,
            };

            // Seek the appropriate sub-page in the branch.
            let mut val = None;
            first = true;
            for res in branch_page.iter_mut().rev() {
                let (k, v) = res?;
                val = Some(v);
                if k <= key {
                    first = false;
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
            let new_page_num = *val;
            self.branches.push((branch_page, page_num));
            page_num = new_page_num;

            // There's no way on earth you've got more than 2^64 items in your
            // tree, something is screwy.
            depth += 1;
            if depth > 64 {
                return Err(Error::DataCorruption);
            }
        }
    }

    fn branch_insert(
        &mut self,
        branch: (PageMapMut<'a, B>, u64),
        insert: (&B::Key, u64),
    ) -> Result<(PageMapMut<'a, B>, u64), Error> {
        let page::Entry::Vacant(vacant) = branch.0.entry(insert.0)? else {
            return Err(Error::DataCorruption);
        };
        match vacant.insert(&insert.1) {
            Ok(t) => Ok((t.to_page(), branch.1)),
            Err((t, Error::OutofSpace(_))) => {
                let new_branch = self.writer.allocate_page()?;
                let mut old_branch = (t.to_page(), branch.1);
                let new_branch = (old_branch.0.split_to(new_branch.0)?, new_branch.1);
                let (k2, _) = new_branch
                    .0
                    .as_const()
                    .iter()
                    .next()
                    .ok_or(Error::InvalidState("split branch shouldn't be empty"))??;

                // Insert into the next level up
                let old_branch = match self.branches.pop() {
                    None => {
                        // Root page. To keep the root page at the same page
                        // number, we've got to copy it over to a new page, then
                        // put both that new page and the higher page in.
                        let copy_branch = self.writer.allocate_page()?;
                        let copy_branch = (
                            old_branch.0.as_const().copy_to(copy_branch.0),
                            copy_branch.1,
                        );
                        let page_type = old_branch.0.page_trailer().page_type;

                        // Create the branch
                        let mut branch = (
                            PageMapMut::new(old_branch.0.to_page(), page_type),
                            old_branch.1,
                        );

                        // Load in the first page's info
                        let (k, _) = copy_branch
                            .0
                            .as_const()
                            .iter()
                            .next()
                            .ok_or(Error::DataCorruption)??;
                        branch.0 = match branch.0.entry(k)? {
                            page::Entry::Occupied(_) => return Err(Error::DataCorruption),
                            page::Entry::Vacant(v) => {
                                v.insert(&old_branch.1).map_err(|(_, e)| e)?.to_page()
                            }
                        };
                        let b = self.branch_insert(branch, (k2, new_branch.1))?;
                        self.branches.push(b);
                        copy_branch
                    }
                    Some(b) => {
                        let b = self.branch_insert(b, (k2, new_branch.1))?;
                        self.branches.push(b);
                        old_branch
                    }
                };

                // Complete the update
                let mut branch = if insert.0 < k2 {
                    old_branch
                } else {
                    new_branch
                };
                let page::Entry::Vacant(vacant) = branch.0.entry(insert.0)? else {
                    return Err(Error::DataCorruption);
                };
                branch.0 = vacant.insert(&insert.1).map_err(|(_, e)| e)?.to_page();
                Ok(branch)
            }
            Err((_, e)) => Err(e),
        }
    }

    fn split_leaf(
        &mut self,
        mut leaf: (PageMapMut<'a, L>, u64),
        key: &L::Key,
    ) -> Result<(PageMapMut<'a, L>, u64), Error> {
        // We need to split the page
        let new_leaf = self.writer.allocate_page()?;
        let new_leaf = (leaf.0.split_to(new_leaf.0)?, new_leaf.1);
        let (k2, _) = new_leaf
            .0
            .as_const()
            .iter()
            .next()
            .ok_or(Error::InvalidState("split leaf shouldn't be empty"))??;

        let leaf = match self.branches.pop() {
            None => {
                // Root page. To keep the root page at the same page
                // number, we've got to copy it over to a new page, then
                // put both that new page and the higher page in.
                let copy_leaf = self.writer.allocate_page()?;
                let copy_leaf = (leaf.0.as_const().copy_to(copy_leaf.0), copy_leaf.1);
                let page_type = leaf.0.page_trailer().page_type & 0xFE;

                // Create the branch
                let mut branch = (PageMapMut::new(leaf.0.to_page(), page_type), leaf.1);

                // Load in the first page's info
                let (k, _) = copy_leaf
                    .0
                    .as_const()
                    .iter()
                    .next()
                    .ok_or(Error::DataCorruption)??;
                branch.0 = match branch.0.entry(k)? {
                    page::Entry::Occupied(_) => return Err(Error::DataCorruption),
                    page::Entry::Vacant(v) => v.insert(&leaf.1).map_err(|(_, e)| e)?.to_page(),
                };
                let b = self.branch_insert(branch, (k2, new_leaf.1))?;
                self.branches.push(b);
                copy_leaf
            }
            Some(b) => {
                let b = self.branch_insert(b, (k2, new_leaf.1))?;
                self.branches.push(b);
                leaf
            }
        };

        Ok(if key < k2 { leaf } else { new_leaf })
    }

    fn replace_branch_first(&mut self, old_key: &L::Key, new_key: &L::Key) -> Result<(), Error> {
        let Some((b, b_num)) = self.branches.pop() else {
            return Ok(());
        };

        // Replace our own branch's key-value pair
        let e = match b.entry(old_key)? {
            page::Entry::Occupied(e) => e,
            page::Entry::Vacant(v) => {
                self.branches.push((v.to_page(), b_num));
                return Ok(());
            }
        };
        let page = *e.get();
        let b = e.delete();

        // Calling branch_insert will automatically handle expanding and
        // splitting branch pages as needed.
        let b = self.branch_insert((b, b_num), (new_key, page))?;

        // Recurse down, replacing the first key-value pair on every branch
        self.replace_branch_first(old_key, new_key)?;

        self.branches.push(b);
        Ok(())
    }

    /// Try to rebalance the pages around the given key. This should unwind the
    /// tree in the process.
    fn balance(&mut self, key: &L::Key) -> Result<(), Error> {
        // Balance from the next branch up. If we can't go up, we tried to
        // balance the root, which we can't do, so just stop.
        let Some(mut branch) = self.branches.pop() else {
            return Ok(());
        };

        // Extract a pair of pages that are next to each other and can be balanced.
        let mut v0: Option<u64> = None;
        let mut prev: Option<(&B::Key, &mut u64)> = None;
        for res in branch.0.iter_mut().rev() {
            let (k, v) = res?;
            if k <= key && prev.is_some() {
                v0 = Some(*v);
                break;
            }
            prev = Some((k, v));
        }
        let Some(prev) = prev else {
            return Ok(());
        };
        let Some(v0) = v0 else {
            return Ok(());
        };

        // Try to balance them.
        //
        // Inside this is an annoying turn we have to take: we have to find the
        // key of the page that's been rebalanced or merged, and we have to find
        // it inside the balanced/merged page(s). Otherwise, we'd try to delete
        // an entry by first finding it using the key of that entry.

        let page0 = WritePage::<B, L>::try_load(self.writer, v0)?;
        let page1 = WritePage::<B, L>::try_load(self.writer, *prev.1)?;
        match (page0.0, page1.0) {
            (WritePage::Branch(b0), WritePage::Branch(b1)) => {
                match unsafe { b0.balance(b1)? } {
                    Balance::Balanced { lower, higher } => {
                        let lower = lower.as_const();
                        let higher = higher.as_const();
                        let (new_key, _) = higher.iter().next().ok_or(Error::DataCorruption)??;

                        let page_with_key = if prev.0 < new_key { lower } else { higher };
                        let old_key = page_with_key
                            .get_pair(prev.0)?
                            .ok_or(Error::DataCorruption)?
                            .0;
                        let page::Entry::Occupied(e) = branch.0.entry(old_key)? else {
                            return Err(Error::DataCorruption);
                        };

                        // Do the replacement
                        let higher_page_num = *e.get();
                        branch.0 = e.delete();
                        self.branch_insert(branch, (new_key, higher_page_num))?;
                    }
                    Balance::Merged(lower) => {
                        let freed_page = *prev.1;

                        let lower = lower.as_const();
                        let old_key = lower.get_pair(prev.0)?.ok_or(Error::DataCorruption)?.0;
                        let page::Entry::Occupied(e) = branch.0.entry(old_key)? else {
                            return Err(Error::DataCorruption);
                        };

                        branch.0 = e.delete();
                        unsafe {
                            self.writer.deallocate_page(freed_page)?;
                        }

                        // This may make this branch relevant for a balancing. Repeat the process
                        self.balance(key)?;
                    }
                }
            }
            (WritePage::Leaf(l0), WritePage::Leaf(l1)) => {
                match unsafe { l0.balance(l1)? } {
                    Balance::Balanced { lower, higher } => {
                        let lower = lower.as_const();
                        let higher = higher.as_const();
                        let (new_key, _) = higher.iter().next().ok_or(Error::DataCorruption)??;

                        let page_with_key = if prev.0 < new_key { lower } else { higher };
                        let old_key = page_with_key
                            .get_pair(prev.0)?
                            .ok_or(Error::DataCorruption)?
                            .0;
                        let page::Entry::Occupied(e) = branch.0.entry(old_key)? else {
                            return Err(Error::DataCorruption);
                        };

                        // Do the replacement
                        let higher_page_num = *e.get();
                        branch.0 = e.delete();
                        self.branch_insert(branch, (new_key, higher_page_num))?;
                    }
                    Balance::Merged(lower) => {
                        let freed_page = *prev.1;

                        let lower = lower.as_const();
                        let old_key = lower.get_pair(prev.0)?.ok_or(Error::DataCorruption)?.0;
                        let page::Entry::Occupied(e) = branch.0.entry(old_key)? else {
                            return Err(Error::DataCorruption);
                        };

                        branch.0 = e.delete();
                        unsafe {
                            self.writer.deallocate_page(freed_page)?;
                        }

                        // This may make this branch relevant for a balancing. Repeat the process
                        self.balance(key)?;
                    }
                }
            }
            _ => return Err(Error::DataCorruption),
        }
        Ok(())
    }
}

pub enum Entry<'a, 't, 'k, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    W: RawWrite,
{
    Occupied(OccupiedEntry<'a, 't, 'k, B, L, W>),
    Vacant(VacantEntry<'a, 't, 'k, B, L, W>),
}

pub struct OccupiedEntry<'a, 't, 'k, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    W: RawWrite,
{
    tree: &'t mut BTreeWrite<'a, B, L, W>,
    key: &'k L::Key,
    entry: page::OccupiedEntry<'a, L>,
    entry_page_num: u64,
    first: bool,
}

impl<'a, 't, 'k, B, L, W> OccupiedEntry<'a, 't, 'k, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    W: RawWrite,
{
    pub fn key(&self) -> &L::Key {
        self.entry.key()
    }

    pub fn get(&self) -> &L::Value {
        self.entry.get()
    }

    pub fn get_mut(&mut self) -> &mut L::Value {
        self.entry.get_mut()
    }

    pub fn delete(self) -> Result<(), Error> {
        // Delete the entry, and fix up the tree if it was the first entry in
        // the page.
        let mut page = self.entry.delete();
        if self.first {
            let (new_key, _) = page.iter_mut().next().ok_or(Error::DataCorruption)??;
            self.tree.replace_branch_first(self.key, new_key)?;
        }

        // Check if we have a page that's a good candidate for rebalancing.
        if page.free_space() > (PAGE_4K * 3 / 4) {
            self.tree.balance(self.key)?;
        }
        Ok(())
    }

    pub fn replace(mut self, new_value: &L::Value) -> Result<Self, Error> {
        // Try and replace normally first
        match self.entry.replace(new_value) {
            Ok(()) => return Ok(self),
            Err(Error::OutofSpace(_)) => (),
            Err(e) => return Err(e),
        }

        // We need to split the page.
        let leaf = self
            .tree
            .split_leaf((self.entry.to_page(), self.entry_page_num), self.key)?;
        let page::Entry::Occupied(mut entry) = leaf.0.entry(self.key)? else {
            return Err(Error::InvalidState(
                "Split a page but we couldn't re-locate the entry inside it",
            ));
        };
        entry.replace(new_value)?;
        Ok(Self {
            tree: self.tree,
            key: self.key,
            entry,
            entry_page_num: leaf.1,
            first: self.first,
        })
    }
}

impl<'a, 't, 'k, B, L, W> OccupiedEntry<'a, 't, 'k, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayoutVectored + PageLayout<Key = B::Key>,
    W: RawWrite,
{
    pub fn replace_vectored(mut self, new_value: &[&L::Value]) -> Result<Self, Error> {
        // Try and replace normally first
        match self.entry.replace_vectored(new_value) {
            Ok(()) => return Ok(self),
            Err(Error::OutofSpace(_)) => (),
            Err(e) => return Err(e),
        }

        // We need to split the page.
        let leaf = self
            .tree
            .split_leaf((self.entry.to_page(), self.entry_page_num), self.key)?;
        let page::Entry::Occupied(mut entry) = leaf.0.entry(self.key)? else {
            return Err(Error::InvalidState(
                "Split a page but we couldn't re-locate the entry inside it",
            ));
        };
        entry.replace_vectored(new_value)?;
        Ok(Self {
            tree: self.tree,
            key: self.key,
            entry,
            entry_page_num: leaf.1,
            first: self.first,
        })
    }
}

pub struct VacantEntry<'a, 't, 'k, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    W: RawWrite,
{
    tree: &'t mut BTreeWrite<'a, B, L, W>,
    key: &'k L::Key,
    entry: page::VacantEntry<'a, 'k, L>,
    entry_page_num: u64,
    first: bool,
}

impl<'a, 't, 'k, B, L, W> VacantEntry<'a, 't, 'k, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    W: RawWrite,
{
    pub fn key(&self) -> &L::Key {
        self.key
    }

    pub fn insert(self, new_value: &L::Value) -> Result<OccupiedEntry<'a, 't, 'k, B, L, W>, Error> {
        // Handle the case where we're inserting right at the very front of the
        // tree.
        let entry = if self.first {
            let mut leaf = self.entry.to_page();
            let (old_key, _) = leaf.iter_mut().next().ok_or(Error::InvalidState("Expected page to have at least one entry present when inserting to the front of a leaf"))??;
            self.tree.replace_branch_first(old_key, self.key)?;

            let page::Entry::Vacant(v) = leaf.entry(self.key)? else {
                return Err(Error::InvalidState(
                    "Expected vacant entry to insert into, but it got filled somehow",
                ));
            };
            v
        } else {
            self.entry
        };

        let entry = match entry.insert(new_value) {
            Ok(entry) => {
                return Ok(OccupiedEntry {
                    tree: self.tree,
                    key: self.key,
                    entry,
                    entry_page_num: self.entry_page_num,
                    first: self.first,
                })
            }
            Err((entry, Error::OutofSpace(_))) => entry,
            Err((_, e)) => return Err(e),
        };

        // We need to split the page.
        let leaf = self
            .tree
            .split_leaf((entry.to_page(), self.entry_page_num), self.key)?;
        let page::Entry::Vacant(entry) = leaf.0.entry(self.key)? else {
            return Err(Error::InvalidState(
                "Split a page but we couldn't re-locate the vacant entry inside it",
            ));
        };
        let entry = entry.insert(new_value).map_err(|(_, e)| e)?;
        Ok(OccupiedEntry {
            tree: self.tree,
            key: self.key,
            entry,
            entry_page_num: leaf.1,
            first: self.first,
        })
    }
}

impl<'a, 't, 'k, B, L, W> VacantEntry<'a, 't, 'k, B, L, W>
where
    B: PageLayout<Value = u64>,
    L: PageLayoutVectored + PageLayout<Key = B::Key>,
    W: RawWrite,
{
    pub fn insert_vectored(
        self,
        new_value: &[&L::Value],
    ) -> Result<OccupiedEntry<'a, 't, 'k, B, L, W>, Error> {
        // Handle the case where we're inserting right at the very front of the
        // tree.
        let entry = if self.first {
            let mut leaf = self.entry.to_page();
            let (old_key, _) = leaf.iter_mut().next().ok_or(Error::InvalidState("Expected page to have at least one entry present when inserting to the front of a leaf"))??;
            self.tree.replace_branch_first(old_key, self.key)?;

            let page::Entry::Vacant(v) = leaf.entry(self.key)? else {
                return Err(Error::InvalidState(
                    "Expected vacant entry to insert into, but it got filled somehow",
                ));
            };
            v
        } else {
            self.entry
        };

        let entry = match entry.insert_vectored(new_value) {
            Ok(entry) => {
                return Ok(OccupiedEntry {
                    tree: self.tree,
                    key: self.key,
                    entry,
                    entry_page_num: self.entry_page_num,
                    first: self.first,
                })
            }
            Err((entry, Error::OutofSpace(_))) => entry,
            Err((_, e)) => return Err(e),
        };

        // We need to split the page.
        let leaf = self
            .tree
            .split_leaf((entry.to_page(), self.entry_page_num), self.key)?;
        let page::Entry::Vacant(entry) = leaf.0.entry(self.key)? else {
            return Err(Error::InvalidState(
                "Split a page but we couldn't re-locate the vacant entry inside it",
            ));
        };
        let entry = entry.insert_vectored(new_value).map_err(|(_, e)| e)?;
        Ok(OccupiedEntry {
            tree: self.tree,
            key: self.key,
            entry,
            entry_page_num: leaf.1,
            first: self.first,
        })
    }
}
