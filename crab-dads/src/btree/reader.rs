use alloc::collections::VecDeque;
use core::{
    borrow::Borrow,
    cmp::Ordering,
    ops::{Bound, RangeBounds},
};

use crate::{
    page::{self, PageIter, PageLayout, PageMap},
    Error,
};

use super::RawRead;

fn trim_leaf<'a, I, R, K, V, Q>(iter: &mut I, range: &R) -> Result<(), Error>
where
    I: Iterator<Item = Result<(&'a K, &'a V), Error>> + Clone,
    R: RangeBounds<Q>,
    K: Borrow<Q> + Ord + ?Sized + 'a,
    V: ?Sized + 'a,
    Q: Ord + ?Sized,
{
    // Trim the front
    let mut peek = iter.clone();
    while let Some(result) = peek.next() {
        let (k, _) = result?;
        match range.start_bound() {
            Bound::Unbounded => break,
            Bound::Excluded(b) => {
                if k.borrow() > b {
                    break;
                }
            }
            Bound::Included(b) => {
                if k.borrow() >= b {
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
                if k.borrow() < b {
                    break;
                }
            }
            Bound::Included(b) => {
                if k.borrow() >= b {
                    break;
                }
            }
        }
        *iter = peek.clone();
    }

    Ok(())
}

fn trim_branch<'a, I, R, K, V, Q>(iter: &mut I, range: &R) -> Result<(), Error>
where
    I: Iterator<Item = Result<(&'a K, &'a V), Error>> + Clone,
    R: RangeBounds<Q>,
    K: Borrow<Q> + Ord + ?Sized + 'a,
    V: ?Sized + 'a,
    Q: Ord + ?Sized,
{
    // Trim the front.
    // We look two keys ahead - if we are greater than the key we take, that
    // means that the previous page contains keys we want to iterate on, which
    // means we need to have not taken that previous page from the iterator.
    let mut peek = iter.clone();
    let mut peek2 = iter.clone();
    while let Some(result) = peek2.next() {
        let (k, _) = result?;
        match range.start_bound() {
            Bound::Unbounded => break,
            Bound::Excluded(b) | Bound::Included(b) => {
                let k: &Q = k.borrow();
                match k.cmp(b) {
                    Ordering::Greater => (),
                    Ordering::Equal => {
                        *iter = peek;
                        break;
                    }
                    Ordering::Less => break,
                }
            }
        }
        *iter = core::mem::replace(&mut peek, peek2.clone());
    }

    // Trim the back
    let mut peek = iter.clone();
    while let Some(result) = peek.next() {
        let (k, _) = result?;
        match range.end_bound() {
            Bound::Unbounded => break,
            Bound::Excluded(b) => {
                if k.borrow() < b {
                    break;
                }
            }
            Bound::Included(b) => {
                if k.borrow() <= b {
                    break;
                }
            }
        }
        *iter = peek.clone();
    }

    Ok(())
}

pub struct BTreeRead<'a, B, L, R>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    R: RawRead,
{
    reader: &'a R,
    root: ReadPage<'a, B, L>,
}

#[derive(Clone)]
pub(crate) enum ReadPage<'a, B, L>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
{
    Branch(PageMap<'a, B>),
    Leaf(PageMap<'a, L>),
}

impl<'a, B, L> ReadPage<'a, B, L>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
{
    pub unsafe fn try_load<R: RawRead>(reader: &'a R, page: u64) -> Result<Self, Error> {
        unsafe {
            let page_ptr = reader.load_page(page)?;
            if (page::page_type(page_ptr) & 1) == 1 {
                Ok(ReadPage::Leaf(PageMap::from_page(page_ptr)?))
            } else {
                Ok(ReadPage::Branch(PageMap::from_page(page_ptr)?))
            }
        }
    }
}

impl<'a, B, L, R> BTreeRead<'a, B, L, R>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    R: RawRead,
{
    /// Load in the root page of a tree.
    ///
    /// # Safety
    ///
    /// The root page must have come from either a parent tree or be the root
    /// page of the database.
    pub unsafe fn load(reader: &'a R, page: u64) -> Result<Self, Error> {
        unsafe {
            let root = ReadPage::try_load(reader, page)?;
            Ok(Self { reader, root })
        }
    }

    pub(crate) unsafe fn from_parts(reader: &'a R, root: ReadPage<'a, B, L>) -> Self {
        Self { reader, root }
    }

    /// Fetch the value for a key.
    pub fn get<Q>(&self, key: &Q) -> Result<Option<&L::Value>, Error>
    where
        L::Key: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut page: ReadPage<B, L> = self.root.clone();
        'outer: for _ in 0..64 {
            match page {
                ReadPage::Branch(b) => {
                    for result in b.iter().rev() {
                        let (k, v) = result?;
                        if k.borrow() <= key {
                            page = unsafe { ReadPage::try_load(self.reader, *v)? };
                            continue 'outer;
                        }
                    }
                    return Ok(None);
                }
                ReadPage::Leaf(l) => {
                    println!("leaf");
                    for result in l.iter() {
                        let (k, v) = result?;
                        let k: &Q = k.borrow();
                        match k.cmp(key) {
                            Ordering::Equal => {
                                // SAFETY: We know the page holding v is valid
                                // and immutable as long as we have the reader,
                                // so we can extract the object directly and
                                // give it a new lifetime.
                                let v = unsafe { &*(v as *const L::Value) };
                                return Ok(Some(v));
                            }
                            Ordering::Less => continue,
                            Ordering::Greater => return Ok(None),
                        }
                    }
                    return Ok(None);
                }
            }
        }
        Err(Error::DataCorruption("B-Tree depth for `get` is unreasonably large"))
    }

    pub fn range<T, RANGE>(&self, range: RANGE) -> Result<BTreeIter<'a, B, L, R>, Error>
    where
        T: Ord + ?Sized,
        L::Key: Borrow<T> + Ord,
        RANGE: RangeBounds<T>,
    {
        // Check for the single-leaf case
        let base = match &self.root {
            ReadPage::Leaf(l) => {
                let mut iter = l.iter();
                trim_leaf(&mut iter, &range)?;
                return Ok(BTreeIter {
                    reader: self.reader,
                    state: BTreeIterState::Leaf(iter),
                });
            }
            ReadPage::Branch(b) => b,
        };

        // Grab the base branch and trim it
        let mut base = base.iter();
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
                return Err(Error::DataCorruption("B-Tree depth for left-side iteration is unreasonably large"))
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
                break *(page?.1);
            };

            let new_page = unsafe { ReadPage::try_load(self.reader, page_addr)? };
            match new_page {
                ReadPage::Branch(b) => {
                    let mut iter = b.iter();
                    trim_branch(&mut iter, &range)?;
                    left.push_back(iter);
                }
                ReadPage::Leaf(l) => {
                    let mut iter = l.iter();
                    trim_leaf(&mut iter, &range)?;
                    break iter;
                }
            }
        };

        // Descend on the right side this time, zippering up the left-hand side
        // as we go.
        let mut right: VecDeque<PageIter<'a, B>> = VecDeque::with_capacity(8);
        let right_leaf = loop {
            if right.len() > 64 {
                return Err(Error::DataCorruption("B-Tree depth for right-side iteration is unreasonably large"))
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
            let page_addr = *(page?.1);

            let new_page = unsafe { ReadPage::try_load(self.reader, page_addr)? };
            match new_page {
                ReadPage::Branch(b) => {
                    let mut iter = b.iter();
                    trim_branch(&mut iter, &range)?;
                    right.push_back(iter);
                }
                ReadPage::Leaf(l) => {
                    let mut iter = l.iter();
                    trim_leaf(&mut iter, &range)?;
                    break iter;
                }
            }
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

    pub fn debug_dump(&self) -> Result<(), Error> {
        let base = match &self.root {
            ReadPage::Leaf(l) => {
                eprintln!("Root Leaf:");
                eprintln!("{:#?}", l);
                return Ok(());
            }
            ReadPage::Branch(b) => {
                eprintln!("Root Branch:");
                eprintln!("{:#?}", b);
                b
            }
        };

        let branch = base.iter();
        let mut stack = Vec::with_capacity(8);
        stack.push(branch);
        loop {
            let Some(branch) = stack.last_mut() else {
                return Ok(());
            };
            let Some(page) = branch.next() else {
                stack.pop();
                continue;
            };
            let page_addr = *(page?.1);

            let new_page = unsafe { ReadPage::<B,L>::try_load(self.reader, page_addr)? };

            match new_page {
                ReadPage::Branch(b) => {
                    eprintln!("Branch ({page_addr}):");
                    eprintln!("{:#?}", b);
                    let iter = b.iter();
                    stack.push(iter);
                },
                ReadPage::Leaf(l) => {
                    eprintln!("Leaf ({page_addr}):");
                    eprintln!("{:#?}", l);
                },
            }

            if stack.len() > 64 {
                return Err(Error::DataCorruption("debug B-Tree depth is unreasonably large"))
            }
        }
    }
}

pub struct BTreeIter<'a, B, L, R>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
    R: RawRead,
{
    reader: &'a R,
    state: BTreeIterState<'a, B, L>,
}

enum BTreeIterState<'a, B, L>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
{
    Empty,
    Leaf(PageIter<'a, L>),
    Full(BTreeIterFull<'a, B, L>),
}

struct BTreeIterFull<'a, B, L>
where
    B: PageLayout<Value = u64>,
    L: PageLayout<Key = B::Key>,
{
    left: VecDeque<PageIter<'a, B>>,
    right: VecDeque<PageIter<'a, B>>,
    left_leaf: PageIter<'a, L>,
    right_leaf: PageIter<'a, L>,
}

impl<'a, B, L, R> BTreeIter<'a, B, L, R>
where
    B: PageLayout<Value = u64> + 'a,
    L: PageLayout<Key = B::Key> + 'a,
    R: RawRead,
{
    #[allow(clippy::type_complexity)]
    fn next_internal(&mut self) -> Result<Option<(&'a L::Key, &'a L::Value)>, Error> {
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
            let page_addr = *(page?.1);

            let new_page = unsafe { ReadPage::try_load(self.reader, page_addr)? };
            match new_page {
                ReadPage::Branch(b) => full.left.push_back(b.iter()),
                ReadPage::Leaf(l) => {
                    let mut iter = l.iter();
                    let ret = iter.next().transpose();
                    full.left_leaf = iter;
                    return ret;
                }
            }
        }
    }

    #[allow(clippy::type_complexity)]
    fn next_back_internal(&mut self) -> Result<Option<(&'a L::Key, &'a L::Value)>, Error> {
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
            let page_addr = *(page?.1);

            let new_page = unsafe { ReadPage::try_load(self.reader, page_addr)? };
            match new_page {
                ReadPage::Branch(b) => full.right.push_back(b.iter()),
                ReadPage::Leaf(l) => {
                    let mut iter = l.iter();
                    let ret = iter.next_back().transpose();
                    full.right_leaf = iter;
                    return ret;
                }
            }
        }
    }
}

impl<'a, B, L, R> Iterator for BTreeIter<'a, B, L, R>
where
    B: PageLayout<Value = u64> + 'a,
    L: PageLayout<Key = B::Key> + 'a,
    R: RawRead,
{
    type Item = Result<(&'a L::Key, &'a L::Value), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_internal().transpose()
    }
}

impl<'a, B, L, R> DoubleEndedIterator for BTreeIter<'a, B, L, R>
where
    B: PageLayout<Value = u64> + 'a,
    L: PageLayout<Key = B::Key> + 'a,
    R: RawRead,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.next_back_internal().transpose()
    }
}
