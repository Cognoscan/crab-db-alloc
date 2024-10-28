use core::ops::Bound;
use std::{collections::VecDeque, marker::PhantomData, ops::RangeBounds};

use crate::{
    page::{self, PageIter, PageLayout},
    Error, PAGE_4K,
};

use super::BlockRange;

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

fn trim_leaf<I, R, K, V>(iter: &mut I, range: &R) -> Result<(), Error>
where
    I: Iterator<Item = Result<(K, V), Error>> + Clone,
    R: RangeBounds<K>,
    K: Ord,
{
    // Trim the front
    let mut peek = iter.clone();
    while let Some(result) = peek.next() {
        let (k, _) = result?;
        match range.start_bound() {
            Bound::Unbounded => break,
            Bound::Excluded(b) => {
                if k > *b {
                    break;
                }
            }
            Bound::Included(b) => {
                if k >= *b {
                    break;
                }
            }
        }
        *iter = peek.clone();
    }

    // Trim the back
    let mut peek = iter.clone();
    while let Some(result) = peek.next() {
        let (k, _) = result?;
        match range.end_bound() {
            Bound::Unbounded => break,
            Bound::Excluded(b) => {
                if k < *b {
                    break;
                }
            }
            Bound::Included(b) => {
                if k >= *b {
                    break;
                }
            }
        }
        *iter = peek.clone();
    }

    Ok(())
}

fn trim_branch<I, R, K, V>(iter: &mut I, range: &R) -> Result<(), Error>
where
    I: Iterator<Item = Result<(K, V), Error>> + Clone,
    R: RangeBounds<K>,
    K: Ord,
{
    // Trim the front
    let mut peek = iter.clone();
    while let Some(result) = peek.next() {
        let (k, _) = result?;
        match range.start_bound() {
            Bound::Unbounded => break,
            Bound::Excluded(b) | Bound::Included(b) => {
                if k >= *b {
                    break;
                }
            }
        }
        *iter = peek.clone();
    }

    // Trim the back
    let mut peek = iter.clone();
    while let Some(result) = peek.next() {
        let (k, _) = result?;
        match range.end_bound() {
            Bound::Unbounded => break,
            Bound::Excluded(b) | Bound::Included(b) => {
                if k <= *b {
                    break;
                }
            }
        }
        *iter = peek.clone();
    }

    Ok(())
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
    pub unsafe fn load(reader: &'a R, page: u64) -> Result<Self, Error> {
        let start = page * (PAGE_4K as u64);
        let root = reader
            .load(BlockRange { start, len: PAGE_4K })
            .ok_or(Error::DataCorruption)?;
        Ok(Self {
            reader,
            root,
            branches: PhantomData,
            leaf: PhantomData,
        })
    }

    pub fn iter(&self, range: R) -> Result<BTreeIter<'a, B, L, R>, Error>
    where
        R: RangeBounds<L::Key>,
    {
        // Check for the single-leaf case
        if (page::page_trailer(self.root)?.page_type & 1) == 1 {
            let mut iter: PageIter<'a, L> = PageIter::iter_page(self.root)?;

            trim_leaf(&mut iter, &range)?;

            return Ok(BTreeIter {
                reader: self.reader,
                state: BTreeIterState::Leaf(iter),
            });
        }

        // Grab the base branch and trim it
        let mut base: PageIter<'a, B> = PageIter::iter_page(self.root)?;
        trim_branch(&mut base, &range)?;

        let mut left = VecDeque::with_capacity(8);
        left.push_back(base);

        // Delve down the left-hand side of the tree, pushing branches onto the
        // queue as we descend. Because it's possible to hit a page and find
        // nothing inside it is in range, we have to support popping back up the
        // tree when we hit empty iterators.
        let left_leaf = loop {
            // If we've gone 64 levels deep in a tree, something exceptionally
            // suspicious is happening.
            if left.len() > 64 {
                return Err(Error::DataCorruption);
            }

            // Fetch the next page address
            let page_addr = loop {
                let Some(iter) = left.back_mut() else {
                    return Ok(BTreeIter {
                        reader: self.reader,
                        state: BTreeIterState::Empty,
                    });
                };
                let Some(page) = iter.next() else {
                    left.pop_back();
                    continue;
                };
                break page?.1 * (PAGE_4K as u64);
            };

            // Retrieve the memory of the page
            let page_mem = unsafe {
                self.reader
                    .load(BlockRange::new(page_addr, PAGE_4K))
                    .ok_or(Error::DataCorruption)?
            };

            // Handle when we finally hit a leaf
            if (page::page_trailer(page_mem)?.page_type & 1) == 1 {
                let mut iter: PageIter<'a, L> = PageIter::iter_page(page_mem)?;
                trim_leaf(&mut iter, &range)?;
                break iter;
            }

            // Parse as a branch, push onto the vec.
            let mut iter: PageIter<'a, B> = PageIter::iter_page(page_mem)?;
            trim_branch(&mut iter, &range)?;
            left.push_back(iter);
        };

        // Descend on the right side this time, zippering up the left-hand side
        // as we go.
        let mut right: VecDeque<PageIter<'a, B>> = VecDeque::with_capacity(8);
        let right_leaf = loop {
            if right.len() > 64 {
                return Err(Error::DataCorruption);
            }

            let page = loop {
                if let Some(iter) = right.back_mut() {
                    if let Some(page) = iter.next_back() {
                        break page;
                    }
                    right.pop_back();
                } else {
                    let Some(iter) = left.front_mut() else {
                        // Single page case - we ended up descending on the
                        // exact same path as the left-hand side.
                        return Ok(BTreeIter {
                            reader: self.reader,
                            state: BTreeIterState::Leaf(left_leaf),
                        });
                    };
                    if let Some(page) = iter.next_back() {
                        break page;
                    };
                    left.pop_front();
                }
            };
            let page_addr = page?.1 * (PAGE_4K as u64);

            // Retrieve the memory of the page
            let page_mem = unsafe {
                self.reader
                    .load(BlockRange::new(page_addr, PAGE_4K))
                    .ok_or(Error::DataCorruption)?
            };

            // Handle when we finally hit a leaf
            if (page::page_trailer(page_mem)?.page_type & 1) == 1 {
                let mut iter: PageIter<'a, L> = PageIter::iter_page(page_mem)?;
                trim_leaf(&mut iter, &range)?;
                break iter;
            }

            // Parse as a branch, push onto the vec.
            let mut iter: PageIter<'a, B> = PageIter::iter_page(page_mem)?;
            trim_branch(&mut iter, &range)?;
            right.push_back(iter);
        };

        Ok(BTreeIter {
            reader: self.reader,
            state: BTreeIterState::Full(BTreeIterFull {
                left,
                right,
                left_leaf,
                right_leaf,
            }),
        })
    }
}

pub struct BTreeIter<'a, B, L, R>
where
    B: PageLayout<'a, Value = u64>,
    L: PageLayout<'a, Key = B::Key>,
    R: RawRead,
{
    reader: &'a R,
    state: BTreeIterState<'a, B, L>,
}

enum BTreeIterState<'a, B, L>
where
    B: PageLayout<'a, Value = u64>,
    L: PageLayout<'a, Key = B::Key>,
{
    Empty,
    Leaf(PageIter<'a, L>),
    Full(BTreeIterFull<'a, B, L>),
}

struct BTreeIterFull<'a, B, L>
where
    B: PageLayout<'a, Value = u64>,
    L: PageLayout<'a, Key = B::Key>,
    {
        left: VecDeque<PageIter<'a, B>>,
        right: VecDeque<PageIter<'a, B>>,
        left_leaf: PageIter<'a, L>,
        right_leaf: PageIter<'a, L>,
}

impl<'a, B, L, R> BTreeIter<'a, B, L, R>
where
    B: PageLayout<'a, Value = u64>,
    L: PageLayout<'a, Key = B::Key>,
    R: RawRead,
{
    #[allow(clippy::type_complexity)]
    fn next_internal(&mut self) -> Result<Option<(L::Key, L::Value)>, Error> {
        let full = match &mut self.state {
            BTreeIterState::Empty => return Ok(None),
            BTreeIterState::Leaf(l) => return l.next().transpose(),
            BTreeIterState::Full(f) => f,
        };

        if let Some(v) = full.left_leaf.next().transpose()? {
            return Ok(Some(v));
        }

        loop {
            let page = loop {
                if let Some(iter) = full.left.back_mut() {
                    if let Some(page) = iter.next() {
                        break page;
                    }
                    full.left.pop_back();
                } else {
                    let Some(iter) = full.right.front_mut() else {
                        let ret = full.right_leaf.next().transpose()?;
                        self.state = BTreeIterState::Leaf(full.right_leaf.clone());
                        return Ok(ret);
                    };
                    if let Some(page) = iter.next() {
                        break page;
                    }
                    full.right.pop_front();
                }
            };
            let page_addr = page?.1 * (PAGE_4K as u64);

            let page_mem = unsafe {
                self.reader.load(BlockRange::new(page_addr, PAGE_4K)).ok_or(Error::DataCorruption)?
            };

            // Extract our next leaf and start iterating on it.
            if (page::page_trailer(page_mem)?.page_type & 1) == 1 {
                let mut iter: PageIter<'a, L> = PageIter::iter_page(page_mem)?;
                let ret = iter.next().transpose();
                full.left_leaf = iter;
                return ret;
            }

            // Parse as a branch, push onto the stack.
            let iter: PageIter<'a, B> = PageIter::iter_page(page_mem)?;
            full.left.push_back(iter);
        }
    }

    #[allow(clippy::type_complexity)]
    fn next_back_internal(&mut self) -> Result<Option<(L::Key, L::Value)>, Error> {
        let full = match &mut self.state {
            BTreeIterState::Empty => return Ok(None),
            BTreeIterState::Leaf(l) => return l.next_back().transpose(),
            BTreeIterState::Full(f) => f,
        };

        if let Some(v) = full.right_leaf.next_back().transpose()? {
            return Ok(Some(v));
        }

        loop {
            let page = loop {
                if let Some(iter) = full.right.back_mut() {
                    if let Some(page) = iter.next_back() {
                        break page;
                    }
                    full.right.pop_back();
                } else {
                    let Some(iter) = full.left.front_mut() else {
                        let ret = full.left_leaf.next_back().transpose()?;
                        self.state = BTreeIterState::Leaf(full.left_leaf.clone());
                        return Ok(ret);
                    };
                    if let Some(page) = iter.next_back() {
                        break page;
                    }
                    full.left.pop_front();
                }
            };
            let page_addr = page?.1 * (PAGE_4K as u64);

            let page_mem = unsafe {
                self.reader.load(BlockRange::new(page_addr, PAGE_4K)).ok_or(Error::DataCorruption)?
            };

            // Extract our next leaf and start iterating on it.
            if (page::page_trailer(page_mem)?.page_type & 1) == 1 {
                let mut iter: PageIter<'a, L> = PageIter::iter_page(page_mem)?;
                let ret = iter.next_back().transpose();
                full.right_leaf = iter;
                return ret;
            }

            // Parse as a branch, push onto the stack.
            let iter: PageIter<'a, B> = PageIter::iter_page(page_mem)?;
            full.right.push_back(iter);
        }
    }
}

impl<'a, B, L, R> Iterator for BTreeIter<'a, B, L, R>
where
    B: PageLayout<'a, Value = u64>,
    L: PageLayout<'a, Key = B::Key>,
    R: RawRead,
{
    type Item = Result<(L::Key, L::Value), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_internal().transpose()
    }

}

impl<'a, B, L, R> DoubleEndedIterator for BTreeIter<'a, B, L, R>
where
    B: PageLayout<'a, Value = u64>,
    L: PageLayout<'a, Key = B::Key>,
    R: RawRead,
{

    fn next_back(&mut self) -> Option<Self::Item> {
        self.next_back_internal().transpose()
    }

}