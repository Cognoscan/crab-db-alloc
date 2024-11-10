mod reader;
mod writer;

pub use reader::*;
pub use writer::*;

use crate::{StorageError, PAGE_4K};

/// Access to a backing reader.
///
/// # Safety
///
/// It's complicated. This is really meant for the `crab-db` approach to page
/// allocation, but roughly:
///
/// - There should be one writer (`RawWrite`), and one or more readers
///   (`RawRead`).
/// - While a `RawWrite` is active, it should not provide writeable pages that a
///   reader might potentially see.
/// - All returned memory must be 4kiB-page-aligned.
/// - When a writer "commits" all the work that has been done, it should become
///   visible to other readers that are opened up after the commit.
/// - If put into persistent storage, either the system guarantees the backing
///   file hasn't been touched by any other program, or it does active
///   verification checking to ensure that any pages allocated by `RawWrite` are
///   never provided to a reader via `RawRead` or `load_page_mut`'s
///   `LoadMut::Clean` return value.
pub unsafe trait RawRead {
    /// Load a memory region.
    ///
    /// # Safety
    ///
    /// Only regions reachable through reading other regions with `load` or the
    /// root database page may be loaded with this function.
    unsafe fn load(&self, page: u64, num_pages: usize) -> Result<&[u8], StorageError>;

    /// Load a 4 kiB page.
    ///
    /// # Safety
    ///
    /// Only pages reachable through reading other pages with `load_page` or the
    /// root database page may be loaded with this function.
    unsafe fn load_page(&self, page: u64) -> Result<&[u8; PAGE_4K], StorageError> {
        unsafe { Ok(&*(self.load(page, 1)?.as_ptr() as *const [u8; 4096])) }
    }
}

pub enum LoadMutPage<'a> {
    Clean {
        write: &'a mut [u8; PAGE_4K],
        write_page: u64,
        read: &'a [u8; PAGE_4K],
    },
    Dirty(&'a mut [u8; PAGE_4K]),
}

pub enum LoadMut<'a> {
    Clean {
        write: &'a mut [u8],
        write_page: u64,
        read: &'a [u8],
    },
    Dirty(&'a mut [u8]),
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
/// - All handed out memory must be 4kiB-page-aligned
/// - When a writer "commits" all the work that has been done, it should become
///   visible to other readers that are opened up after the commit.
/// - If put into persistent storage, either the system guarantees the backing
///   file hasn't been touched by any other program, or it does active
///   verification checking to ensure that any pages allocated by `RawWrite` are
///   never provided to a reader via `RawRead` or `load_page_mut`'s
///   `LoadMut::Clean` return value.
pub unsafe trait RawWrite: RawRead {
    /// Load a memory region for writing. If the range that's been requested is
    /// not available for writing, it should return the
    /// [`Clean`][LoadMut::Clean] result with a newly allocated region to write
    /// to. If the region is available for writing, then
    /// [`Dirty`][LoadMut::Dirty] should be returned instead.
    ///
    /// # Safety
    ///
    /// Only regions reachable through reading the root database page and its
    /// children may be loaded with this function - i.e. only regions that were
    /// previously allocated through this writer. The `num_pages` amount must
    /// exactly match the number of pages that were requested during allocation.
    ///
    /// When the loaded mutable memory is dropped, `unload_mut` must
    /// also be called in order for the allocator to track and detect erronious
    /// multiple views into a mutable memory region.
    unsafe fn load_mut(&self, page: u64, num_pages: usize) -> Result<LoadMut, StorageError>;

    /// Unload a memory region that was loaded with `load_mut`.
    ///
    /// # Safety
    ///
    /// The provided region info must have come via `load_mut`, but should use
    /// the returned `write` page's number, which may not be the page number
    /// provided to `load_mut`.
    unsafe fn unload_mut(&self, page: u64, num_pages: usize);

    /// Allocate a memory region for writing.
    fn allocate(&self, num_pages: usize) -> Result<(&mut [u8], u64), StorageError>;

    /// Deallocate a region previously allocated by `load_mut` or `allocate`.
    ///
    /// # Safety
    ///
    /// This must only be called with page numbers that were allocated, and can
    /// only be called with them once.
    unsafe fn deallocate(&self, page: u64, num_pages: usize) -> Result<(), StorageError>;

    /// Load a page for writing. If the range that's been requested is not
    /// available for writing, it should return the
    /// [`Clean`][LoadMutPage::Clean] result with a newly allocated page to
    /// write to. If the page is available for writing, then
    /// [`Dirty`][LoadMutPage::Dirty] should be returned instead.
    ///
    /// # Safety
    ///
    /// Only pages reachable through reading the root database page and its
    /// children may be loaded with this function - i.e. only pages that were
    /// previously allocated through this writer.
    unsafe fn load_mut_page(&self, page: u64) -> Result<LoadMutPage, StorageError> {
        unsafe {
            match self.load_mut(page, 1)? {
                LoadMut::Clean {
                    write,
                    write_page,
                    read,
                } => Ok(LoadMutPage::Clean {
                    write: &mut *(write.as_mut_ptr() as *mut [u8; 4096]),
                    write_page,
                    read: &*(read.as_ptr() as *const [u8; 4096]),
                }),
                LoadMut::Dirty(d) => Ok(LoadMutPage::Dirty(
                    &mut *(d.as_mut_ptr() as *mut [u8; 4096]),
                )),
            }
        }
    }

    /// Unload a page that was loaded with `load_mut_page`.
    ///
    /// # Safety
    ///
    /// The provided page info must have come via `load_mut_page`, but should
    /// use the returned `write` page's number, which may not be the page number
    /// provided to `load_mut_page`.
    unsafe fn unload_mut_page(&self, page: u64) {
        unsafe {
            self.unload_mut(page, 1);
        }
    }

    /// Allocate a page for writing.
    fn allocate_page(&self) -> Result<(&mut [u8; 4096], u64), StorageError> {
        unsafe {
            let (data, page) = self.allocate(1)?;
            Ok((&mut *(data.as_mut_ptr() as *mut [u8; 4096]), page))
        }
    }

    /// Deallocate a page previously allocated by `load_mut_page` or `allocate_page`.
    ///
    /// # Safety
    ///
    /// This must only be called with page numbers that were allocated, and can
    /// only be called with them once.
    unsafe fn deallocate_page(&self, page: u64) -> Result<(), StorageError> {
        unsafe { self.deallocate(page, 1) }
    }
}

#[cfg(test)]
mod test {
    extern crate std;
}
