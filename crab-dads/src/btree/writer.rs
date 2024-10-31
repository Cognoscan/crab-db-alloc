use core::slice;

use crate::{
    page::{self, PageLayout, PageMap, PageMapMut},
    Error,
};

use super::{reader::ReadPage, BTreeRead, LoadMut, RawWrite};

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
    Branch(PageMapMut<'a, B>),
    Leaf(PageMapMut<'a, L>),
}

impl<'a, B, L> WritePage<'a, B, L>
where
    B: PageLayout<'a, Value = u64>,
    L: PageLayout<'a, Key = B::Key>,
{
    fn try_load<W: RawWrite>(writer: &mut W, page: u64) -> Result<(Self, Option<u64>), Error> {
        unsafe {
            match writer.load_page_mut(page)? {
                LoadMut::Clean {
                    write,
                    write_page,
                    read,
                } => {
                    if (page::page_type(read) & 1) == 1 {
                        let read: PageMap<'a, L> = PageMap::from_ptr(read)?;
                        let write = read.copy_to(write);
                        Ok((WritePage::Leaf(write), Some(write_page)))
                    } else {
                        let read: PageMap<'a, B> = PageMap::from_ptr(read)?;
                        let write = read.copy_to(write);
                        Ok((WritePage::Branch(write), Some(write_page)))
                    }
                }
                LoadMut::Dirty(d) => {
                    if (page::page_type(d) & 1) == 1 {
                        Ok((WritePage::Leaf(PageMapMut::from_ptr(d)?), None))
                    } else {
                        Ok((WritePage::Branch(PageMapMut::from_ptr(d)?), None))
                    }
                }
            }
        }
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
        let (root, new_page) = WritePage::try_load(writer, page)?;
        Ok((Self { writer, root }, new_page))
    }

    pub fn as_read<'b, B2, L2>(&'b self) -> BTreeRead<'b, B2, L2, W>
    where
        B2: PageLayout<'b, Key = B::Key, Value = u64>,
        L2: PageLayout<'b, Key = B::Key, Value = L::Value>,
    {
        let root = unsafe {
            match &self.root {
                WritePage::Branch(p) => ReadPage::Branch(
                    (*(p as *const PageMapMut<'a, B> as *const PageMap<'b, B2>)).clone(),
                ),
                WritePage::Leaf(p) => ReadPage::Leaf(
                    (*(p as *const PageMapMut<'a, L> as *const PageMap<'b, L2>)).clone(),
                ),
            }
        };
        unsafe { BTreeRead::from_parts(self.writer, root) }
    }
    //pub fn as_read(&self) -> BTreeRead<'a, B, L, W>
    //{
    //    let root = unsafe {match &self.root {
    //        WritePage::Branch(p) => ReadPage::Branch(p.as_const().clone()),
    //        WritePage::Leaf(p) => ReadPage::Leaf(p.as_const().clone()),
    //    }};
    //    unsafe {
    //        BTreeRead::from_parts(&self.writer, root)
    //    }
    //}
}
