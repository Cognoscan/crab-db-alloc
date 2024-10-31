mod reader;
mod writer;
use core::{borrow::Borrow, ops::RangeBounds};

pub use reader::*;
pub use writer::*;

use crate::{page, Error, StorageError};

/// Access to a backing reader.
pub unsafe trait RawRead {
    /// Load a 4 kiB page.
    ///
    /// # Safety
    ///
    /// Only pages reachable through reading other pages with `load_page` or the
    /// root database page may be loaded with this function.
    unsafe fn load_page(&self, page: u64) -> Result<*const u8, StorageError>;
}

pub enum LoadMut {
    Clean {
        write: *mut u8,
        write_page: u64,
        read: *const u8,
    },
    Dirty(*mut u8),
}

/// Implements the writeable portion of a page-backed database.
///
/// # Safety
///
/// It's complicated. This is really meant for the `crab-db` approach to page
/// allocation, but roughly:
///
/// - There should be one writer, and one or more readers.
/// - While a `RawWrite` is active, it should not provide writeable pages that a
///   reader might potentially see.
/// - All handed out pointers should point to memory that is 4 kiB in size.
/// - When a writer "commits" all the work that has been done, it should become
///   visible to other readers that are opened up after the commit.
/// - If put into persistent storage, either the system guarantees the backing
///   file hasn't been touched by any other program, or it does active
///   verification checking to ensure that any pages allocated by `RawWrite` are
///   never provided to a reader via `RawRead` or `load_page_mut`'s
///   `LoadMut::Clean` return value.
pub unsafe trait RawWrite: RawRead {
    /// Load a page for writing. If the range that's been requested is not
    /// available for writing, it should return the [`Clean`][LoadMut::Clean]
    /// result with a newly allocated page to write to. If the page is
    /// available for writing, then [`Dirty`][LoadMut::Dirty] should be returned
    /// instead.
    ///
    /// # Safety
    ///
    /// Only pages reachable through reading other pages with `load_page_mut` or
    /// the root database page may be loaded with this function.
    unsafe fn load_page_mut(&mut self, page: u64) -> Result<LoadMut, StorageError>;

    /// Allocate a page for writing.
    fn allocate_page(&mut self) -> Result<(*mut u8, u64), StorageError>;

    /// Deallocate a page previously allocated by `load_mut` or `allocate`.
    ///
    /// # Safety
    ///
    /// This must only be called with page numbers that were allocated, and can
    /// only be called with them once.
    unsafe fn deallocate_page(&mut self, page: u64) -> Result<(), StorageError>;
}

pub struct BTreeVarU64Mut<'a, W: RawWrite>(
    BTreeWrite<'a, page::LayoutVarU64, page::LayoutVarU64, W>,
);

impl<'a, W: RawWrite> BTreeVarU64Mut<'a, W> {

    /// Load in the root page of a tree.
    ///
    /// # Safety
    ///
    /// The provided page (and any child pages it may later navigate to) must
    /// all not be used mutably elsewhere in the program.
    pub unsafe fn load(writer: &'a mut W, page: u64) -> Result<(Self, Option<u64>), Error> {
        let (s, page) = BTreeWrite::load(writer, page)?;
        Ok((Self(s), page))
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<u64>, Error> {
        self.0.as_read::<page::LayoutVarU64, page::LayoutVarU64>().get(key)
    }

    /*
    pub fn iter<'s, 'b, R: RangeBounds<&'b [u8]>>(
        &'s self,
        range: R,
    ) -> Result<impl Iterator<Item = Result<(&'b [u8], u64), Error>>, Error>
    where
        'a: 'b,
        's: 'b,
    {
        BTreeRead::<page::LayoutVarU64, page::LayoutVarU64, W>::load(self.writer, self.page)?
            .iter(range)
    }
    */
}

pub struct BTreeVarU64<'a, R: RawRead>(BTreeRead<'a, page::LayoutVarU64, page::LayoutVarU64, R>);

impl<'a, R: RawRead> BTreeVarU64<'a, R> {
    pub unsafe fn new(reader: &'a R, page: u64) -> Result<Self, Error> {
        unsafe { Ok(Self(BTreeRead::load(reader, page)?)) }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<u64>, Error> {
        self.0.get(key)
    }

    pub fn range<T, RANGE>(
        &self,
        range: RANGE,
    ) -> Result<impl Iterator<Item = Result<(&'a [u8], u64), Error>>, Error>
    where
        RANGE: RangeBounds<T>,
        &'a [u8]: Borrow<T>,
        T: Ord + ?Sized,
    {
        self.0.range(range)
    }
}

#[cfg(test)]
mod test {
    extern crate std;
    use std::collections::{BTreeMap, BTreeSet};
    use std::prelude::rust_2021::*;
    use std::println;

    use crate::PAGE_4K;

    use super::*;

    #[derive(Default)]
    struct FakeBackend {
        pages: BTreeMap<u64, Box<[u8; PAGE_4K]>>,
        counter: u64,
        dirty: BTreeSet<u64>,
    }

    impl FakeBackend {
        fn commit(&mut self) {
            self.dirty.clear();
        }

        fn restart(&mut self) {
            for page in self.dirty.iter() {
                self.pages.remove(page);
            }
            self.dirty.clear();
        }
    }

    unsafe impl RawRead for FakeBackend {
        unsafe fn load_page(&self, page: u64) -> Result<*const u8, StorageError> {
            self.pages
                .get(&page)
                .map(|mem| mem.as_ptr())
                .ok_or(StorageError::OutOfRange(page))
        }
    }

    unsafe impl RawWrite for FakeBackend {
        fn allocate_page(&mut self) -> Result<(*mut u8, u64), StorageError> {
            let page = self.counter;
            self.counter += 1;
            let mut memory = Box::new([0u8; PAGE_4K]);
            let ptr: *mut u8 = memory.as_mut_ptr();
            self.pages.insert(page, memory);
            self.dirty.insert(page);
            Ok((ptr, page))
        }

        unsafe fn deallocate_page(&mut self, page: u64) -> Result<(), StorageError> {
            if self.pages.remove(&page).is_none() {
                return Err(StorageError::Corruption("Unexpected page deallocated"));
            }
            self.dirty.remove(&page);
            Ok(())
        }

        unsafe fn load_page_mut(&mut self, page: u64) -> Result<LoadMut, StorageError> {
            if self.dirty.contains(&page) {
                let ret = self.pages.get(&page).ok_or(StorageError::Corruption(
                    "Tried to load a dirty page that wasn't in the page store",
                ))?;
                let ptr = (*ret).as_ptr() as *mut u8;
                Ok(LoadMut::Dirty(ptr))
            } else {
                let read = self.load_page(page)?;
                let (write, write_page) = self.allocate_page()?;
                Ok(LoadMut::Clean {
                    write,
                    write_page,
                    read,
                })
            }
        }
    }

    #[test]
    fn simple_iter() {
        let backend = FakeBackend::default();

        let tree = unsafe { BTreeVarU64::new(&backend, 0).unwrap() };
        let empty: &[u8] = &[];
        let range = tree.range(empty..&[0u8, 1u8]).unwrap();
        for result in range {
            let (k, v) = result.unwrap();
            println!("k={:?}, v={}", k, v);
        }
    }
}
