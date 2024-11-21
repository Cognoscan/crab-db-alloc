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
                        writer.deallocate_page(page)?;
                        Ok((WritePage::Leaf(write), Some(write_page)))
                    } else {
                        let read: PageMap<'a, B> = PageMap::from_page(read)?;
                        let write = read.copy_to(write);
                        writer.deallocate_page(page)?;
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
            root: root_page_num,
        };
        match root {
            WritePage::Branch(b) => s.branches.push((b, root_page_num)),
            WritePage::Leaf(l) => s.leaf = Some((l, root_page_num)),
        };
        Ok((s, new_page))
    }

    /// Turn into a temporary reader
    pub fn as_read(&mut self) -> BTreeRead<'_, B, L, W> {
        // Clear out any descent into the tree that we'd previously done
        self.branches.truncate(1);

        // Loan out the root page
        let root = if let Some(l) = &self.leaf {
            ReadPage::Leaf(l.0.as_const().clone())
        } else if let Some(b) = self.branches.last() {
            ReadPage::Branch(b.0.as_const().clone())
        } else {
            unsafe {
                ReadPage::try_load(self.writer, self.root).expect("root page should be valid")
            }
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
                        }));
                    }
                    page::Entry::Vacant(e) => {
                        return Ok(Entry::Vacant(VacantEntry {
                            tree: self,
                            key,
                            entry: e,
                            entry_page_num: page_num,
                        }));
                    }
                },
                WritePage::Branch(b) => b,
            };

            // Seek the appropriate sub-page in the branch.
            let mut val = None;
            for res in branch_page.iter_mut().rev() {
                let (k, v) = res?;
                val = Some(v);
                if k <= key {
                    break;
                }
            }
            let val = val.ok_or(Error::DataCorruption("A branch page was somehow empty"))?;

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
                return Err(Error::DataCorruption("unreasonably large B-Tree depth"));
            }
        }
    }

    fn branch_insert(
        &mut self,
        branch: (PageMapMut<'a, B>, u64),
        insert: (&B::Key, u64),
    ) -> Result<(PageMapMut<'a, B>, u64), Error> {
        // Try and do the insertion normally first
        let page::Entry::Vacant(vacant) = branch.0.entry(insert.0)? else {
            return Err(Error::DataCorruption(
                "Branch insertion found an occupied entry it was directed to create",
            ));
        };
        let vacant = match vacant.insert(&insert.1) {
            Ok(t) => return Ok((t.to_page(), branch.1)),
            Err((t, Error::OutofSpace(_))) => t,
            Err((_, e)) => return Err(e),
        };

        // Branch is out of space, time to split it up

        let new_branch = self.writer.allocate_page()?;
        let mut old_branch = (vacant.to_page(), branch.1);
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
                let mut root_branch = (
                    PageMapMut::new(old_branch.0.to_page(), page_type),
                    old_branch.1,
                );

                // Load in the first page's info
                let (k, _) =
                    copy_branch
                        .0
                        .as_const()
                        .iter()
                        .next()
                        .ok_or(Error::DataCorruption(
                            "Sub-page for new branch has no entries",
                        ))??;
                root_branch.0 = match root_branch.0.entry(k)? {
                    page::Entry::Occupied(_) => {
                        return Err(Error::DataCorruption("brand new branch had occupied entry"))
                    }
                    page::Entry::Vacant(v) => {
                        v.insert(&copy_branch.1).map_err(|(_, e)| e)?.to_page()
                    }
                };
                let b = self.branch_insert(root_branch, (k2, new_branch.1))?;
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
            return Err(Error::DataCorruption(
                "branch insertion expected a branch with a vacancy for the provided key",
            ));
        };
        branch.0 = vacant.insert(&insert.1).map_err(|(_, e)| e)?.to_page();
        Ok(branch)
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
                let (k, _) =
                    copy_leaf
                        .0
                        .as_const()
                        .iter()
                        .next()
                        .ok_or(Error::DataCorruption(
                            "Sub-leaf for new branch has no entries",
                        ))??;
                branch.0 = match branch.0.entry(k)? {
                    page::Entry::Occupied(_) => {
                        return Err(Error::DataCorruption("brand new branch had occupied entry"))
                    }
                    page::Entry::Vacant(v) => v.insert(&copy_leaf.1).map_err(|(_, e)| e)?.to_page(),
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
        let Some(branch) = self.branches.pop() else {
            return Ok(());
        };

        // Replace our own branch's key-value pair
        let e = match branch.0.entry(old_key)? {
            page::Entry::Occupied(e) => e,
            page::Entry::Vacant(v) => {
                self.branches.push((v.to_page(), branch.1));
                return Ok(());
            }
        };
        let first = e.first();
        let page = *e.get();
        let branch = (e.delete(), branch.1);

        // Calling branch_insert will automatically handle expanding and
        // splitting branch pages as needed.
        let branch = self.branch_insert(branch, (new_key, page))?;

        // Recurse down, replacing the first key-value pair on every branch - IF
        // we know we have to keep going.
        if first {
            self.replace_branch_first(old_key, new_key)?;
        }

        self.branches.push(branch);
        Ok(())
    }

    /// Check if we can push down the root of the tree by one level or not.
    fn reduce_depth(&mut self) -> Result<(), Error> {
        self.branches.truncate(1);

        // Extract our root page
        let (mut page, _) = if self.leaf.is_some() {
            return Ok(());
        } else if let Some(b) = self.branches.pop() {
            b
        } else {
            match WritePage::<B, L>::try_load(self.writer, self.root)?.0 {
                WritePage::Branch(b) => (b, self.root),
                _ => return Ok(()),
            }
        };

        let mut iter = page.iter_mut();
        let (_, first) = iter
            .next()
            .ok_or(Error::DataCorruption("branch should never be empty"))??;
        let first = *first;

        // If it's not the only value present, we're done.
        if iter.next().is_some() {
            return Ok(());
        }

        // Only page left? Time to pull that page up into the current page instead.
        let (sub_page, _) = WritePage::<B, L>::try_load(self.writer, first)?;
        match sub_page {
            WritePage::Branch(b) => {
                let root = b.as_const().copy_to(page.to_page());
                unsafe {
                    self.writer.deallocate_page(first)?;
                }
                self.branches.push((root, self.root));
            }
            WritePage::Leaf(l) => {
                let root = l.as_const().copy_to(page.to_page());
                unsafe {
                    self.writer.deallocate_page(first)?;
                }
                self.leaf = Some((root, self.root));
            }
        }

        Ok(())
    }

    /// Try to rebalance the pages around the given key. This should unwind the
    /// tree in the process.
    fn balance(&mut self, key: &L::Key) -> Result<bool, Error> {
        // Balance from the next branch up. If we can't go up, we tried to
        // balance the root, which we can't do, so just stop.
        let Some(mut branch) = self.branches.pop() else {
            return Ok(true);
        };

        // Extract a pair of pages that are next to each other and can be balanced.
        let mut v0: Option<(&B::Key, &mut u64)> = None;
        let mut v1: Option<(&B::Key, &mut u64)> = None;
        for res in branch.0.iter_mut().rev() {
            let (k, v) = res?;
            v1 = v0;
            v0 = Some((k, v));
            if k <= key && v1.is_some() {
                break;
            }
        }
        let Some(v0) = v0 else {
            return Ok(true);
        };
        let Some(v1) = v1 else {
            return Ok(true);
        };

        // Load the pages, replacing the page addresses in the process if needed.
        let page0 = WritePage::<B, L>::try_load(self.writer, *v0.1)?;
        let page1 = WritePage::<B, L>::try_load(self.writer, *v1.1)?;
        if let Some(new_page0) = page0.1 {
            *v0.1 = new_page0;
        }
        if let Some(new_page1) = page1.1 {
            *v1.1 = new_page1;
        }

        // Try to balance them.
        //
        // Inside this is an annoying turn we have to take: we have to find the
        // key of the page that's been rebalanced or merged, and we have to find
        // it inside the balanced/merged page(s). Otherwise, we'd try to delete
        // an entry by first finding it using the key of that entry.

        match (page0.0, page1.0) {
            (WritePage::Branch(b0), WritePage::Branch(b1)) => {
                match unsafe { b0.balance(b1)? } {
                    Balance::Balanced { lower, higher } => {
                        let lower = lower.as_const();
                        let higher = higher.as_const();
                        let (new_key, _) = higher.iter().next().ok_or(Error::DataCorruption(
                            "Balanced higher page should still have entries",
                        ))??;

                        let page_with_key = if v1.0 < new_key { lower } else { higher };
                        let old_key = page_with_key
                            .get_pair(v1.0)?
                            .ok_or(Error::DataCorruption(
                                "Balanced branch is missing expected pair",
                            ))?
                            .0;
                        let page::Entry::Occupied(e) = branch.0.entry(old_key)? else {
                            return Err(Error::DataCorruption(
                                "Branch holding balanced pages should still have entries for both",
                            ));
                        };

                        // Do the replacement
                        let higher_page_num = *e.get();
                        branch.0 = e.delete();
                        self.branch_insert(branch, (new_key, higher_page_num))?;
                        Ok(false)
                    }
                    Balance::Merged(lower) => {
                        let freed_page = *v1.1;

                        let lower = lower.as_const();
                        let old_key = lower
                            .get_pair(v1.0)?
                            .ok_or(Error::DataCorruption(
                                "Merged branch is missing expected pair",
                            ))?
                            .0;
                        let page::Entry::Occupied(e) = branch.0.entry(old_key)? else {
                            return Err(Error::DataCorruption(
                                "Branch holding merged pages should still have entries for both",
                            ));
                        };

                        branch.0 = e.delete();
                        unsafe {
                            self.writer.deallocate_page(freed_page)?;
                        }

                        // This may make this branch relevant for a balancing. Repeat the process
                        if branch.0.free_space() > (PAGE_4K * 3 / 4) {
                            self.balance(key)
                        }
                        else {
                            Ok(true)
                        }
        
                    }
                }
            }
            (WritePage::Leaf(l0), WritePage::Leaf(l1)) => {
                match unsafe { l0.balance(l1)? } {
                    Balance::Balanced { lower, higher } => {
                        let lower = lower.as_const();
                        let higher = higher.as_const();
                        let (new_key, _) = higher.iter().next().ok_or(Error::DataCorruption(
                            "balanced upper leaf page should not be empty",
                        ))??;

                        let page_with_key = if v1.0 < new_key { lower } else { higher };
                        let old_key = page_with_key
                            .get_pair(v1.0)?
                            .ok_or(Error::DataCorruption("Balanced leaf pages should still have the old first key of the higher page"))?
                            .0;
                        let page::Entry::Occupied(e) = branch.0.entry(old_key)? else {
                            return Err(Error::DataCorruption("Branch holding balanced pages should still have old key of the upper page"));
                        };

                        // Do the replacement
                        let higher_page_num = *e.get();
                        branch.0 = e.delete();
                        self.branch_insert(branch, (new_key, higher_page_num))?;
                        Ok(false)
                    }
                    Balance::Merged(lower) => {
                        let freed_page = *v1.1;

                        let lower = lower.as_const();
                        let old_key = lower
                            .get_pair(v1.0)?
                            .ok_or(Error::DataCorruption(
                                "Merged branch is missing expected pair",
                            ))?
                            .0;
                        let page::Entry::Occupied(e) = branch.0.entry(old_key)? else {
                            return Err(Error::DataCorruption("Branch holding merged pages should still have old key of the upper page"));
                        };

                        branch.0 = e.delete();
                        unsafe {
                            self.writer.deallocate_page(freed_page)?;
                        }

                        // This may make this branch relevant for a balancing. Repeat the process
                        if branch.0.free_space() > (PAGE_4K * 3 / 4) {
                            self.balance(key)
                        }
                        else {
                            Ok(true)
                        }
                    }
                }
            }
            _ => {
                Err(Error::DataCorruption(
                    "Found a branch and a leaf page sharing the same hierarchy level in the tree",
                ))
            }
        }
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
        let first = self.entry.first();
        let mut page = self.entry.delete();
        if first {
            if let Some(new) = page.iter_mut().next() {
                let (new_key, _) = new?;
                self.tree.replace_branch_first(self.key, new_key)?;
            }
        }

        // Check if we have a page that's a good candidate for rebalancing.
        #[allow(clippy::collapsible_if)]
        if page.free_space() > (PAGE_4K * 3 / 4) {
            if self.tree.balance(self.key)? {
                // Balancing may have eliminated the top of the tree. Check that now.
                self.tree.reduce_depth()?;
            }
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
        let entry = match self.entry.insert(new_value) {
            Ok(entry) => {
                // If we're the new first entry, see if there was an *old* first
                // entry and make sure to update any parent branches.
                let entry = if entry.first() {
                    let mut leaf = entry.to_page();
                    let mut iter = leaf.iter_mut();
                    let _ = iter.next().ok_or(Error::InvalidState("After inserting a key-value pair into a page, there should be at least one present"))??;
                    if let Some(old_pair) = iter.next() {
                        let (old_key, _) = old_pair?;
                        self.tree.replace_branch_first(old_key, self.key)?;
                    }
                    let page::Entry::Occupied(v) = leaf.entry(self.key)? else {
                        return Err(Error::InvalidState(
                            "Expected occupied entry we just inserted into, but it was empty somehow",
                        ));
                    };
                    v
                } else {
                    entry
                };
                return Ok(OccupiedEntry {
                    tree: self.tree,
                    key: self.key,
                    entry,
                    entry_page_num: self.entry_page_num,
                });
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

        // If we're the new first entry, see if there was an *old* first
        // entry and make sure to update any parent branches.
        let entry = if entry.first() {
            let mut leaf = entry.to_page();
            let mut iter = leaf.iter_mut();
            let _ = iter.next().ok_or(Error::InvalidState("After inserting a key-value pair into a page, there should be at least one present"))??;
            if let Some(old_pair) = iter.next() {
                let (old_key, _) = old_pair?;
                self.tree.replace_branch_first(old_key, self.key)?;
            }
            let page::Entry::Occupied(v) = leaf.entry(self.key)? else {
                return Err(Error::InvalidState(
                    "Expected occupied entry we just inserted into, but it was empty somehow",
                ));
            };
            v
        } else {
            entry
        };

        Ok(OccupiedEntry {
            tree: self.tree,
            key: self.key,
            entry,
            entry_page_num: leaf.1,
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
        let entry = match self.entry.insert_vectored(new_value) {
            Ok(entry) => {
                // If we're the new first entry, see if there was an *old* first
                // entry and make sure to update any parent branches.
                let entry = if entry.first() {
                    let mut leaf = entry.to_page();
                    let mut iter = leaf.iter_mut();
                    let _ = iter.next().ok_or(Error::InvalidState("After inserting a key-value pair into a page, there should be at least one present"))??;
                    if let Some(old_pair) = iter.next() {
                        let (old_key, _) = old_pair?;
                        self.tree.replace_branch_first(old_key, self.key)?;
                    }
                    let page::Entry::Occupied(v) = leaf.entry(self.key)? else {
                        return Err(Error::InvalidState(
                            "Expected occupied entry we just inserted into, but it was empty somehow",
                        ));
                    };
                    v
                } else {
                    entry
                };
                return Ok(OccupiedEntry {
                    tree: self.tree,
                    key: self.key,
                    entry,
                    entry_page_num: self.entry_page_num,
                });
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

        // If we're the new first entry, see if there was an *old* first
        // entry and make sure to update any parent branches.
        let entry = if entry.first() {
            let mut leaf = entry.to_page();
            let mut iter = leaf.iter_mut();
            let _ = iter.next().ok_or(Error::InvalidState("After inserting a key-value pair into a page, there should be at least one present"))??;
            if let Some(old_pair) = iter.next() {
                let (old_key, _) = old_pair?;
                self.tree.replace_branch_first(old_key, self.key)?;
            }
            let page::Entry::Occupied(v) = leaf.entry(self.key)? else {
                return Err(Error::InvalidState(
                    "Expected occupied entry we just inserted into, but it was empty somehow",
                ));
            };
            v
        } else {
            entry
        };

        Ok(OccupiedEntry {
            tree: self.tree,
            key: self.key,
            entry,
            entry_page_num: leaf.1,
        })
    }
}
