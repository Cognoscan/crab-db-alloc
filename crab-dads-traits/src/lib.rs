/*!
Traits and structures used to operate the B-Tree implementation in `crab-dads`.

The two core traits are [`RawRead`] and [`RawWrite`]. [`RawWrite`] is used to
allocate and deallocate memory regions, and to load previously allocated ones
for the purposes of updating them. [`RawRead`] is used to load and read
allocated memory regions. There is assumed to be some synchronization mechanism
that allows for a [`RawWrite`] implementor to commit all operations, such that
they become visible to implementors of [`RawRead`] that are pointed to the same
memory space.
*/
#![no_std]

pub const PAGE_4K: usize = 4096;

mod error;
pub use error::StorageError;

#[cfg(feature = "test-alloc")]
pub mod test_alloc;

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
        unsafe { Ok(&*(self.load(page, 1)?.as_ptr() as *const [u8; PAGE_4K])) }
    }
}

/// A loaded mutable 4kiB page.
///
/// If the requested page is not marked as dirty (i.e. it hasn't been loaded by
/// the writer), then the clean version of the page is returned along with a new
/// allocation to write the page to.
///
/// Once the dirty page has been set up, the clean page should be freed up by
/// calling [`RawWrite::deallocate_page`] on the writer.
#[must_use]
pub enum LoadMutPage<'a> {
    Clean {
        write: &'a mut [u8; PAGE_4K],
        write_page: u64,
        read: &'a [u8; PAGE_4K],
    },
    Dirty(&'a mut [u8; PAGE_4K]),
}

/// A loaded mutable memory region
///
/// If the requested region is not marked as dirty (i.e. it hasn't been loaded
/// by the writer), then the clean version of the region is returned along with
/// a new allocation to write the region to.
///
/// Once the dirty region has been set up, the clean region should be freed up
/// by calling [`RawWrite::deallocate`] on the writer.
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
    unsafe fn load_mut(&self, page: u64, num_pages: usize) -> Result<LoadMut<'_>, StorageError>;

    /// Allocate a memory region for writing.
    /// 
    /// All allocations will be unique, and correspondingly have unique `u64`
    /// values returned. After this allocation is dropped, its contents may
    /// later be read back with [`load_mut`][Self::load_mut].
    #[allow(clippy::mut_from_ref)]
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
    unsafe fn load_mut_page(&self, page: u64) -> Result<LoadMutPage<'_>, StorageError> {
        unsafe {
            match self.load_mut(page, 1)? {
                LoadMut::Clean {
                    write,
                    write_page,
                    read,
                } => Ok(LoadMutPage::Clean {
                    write: &mut *(write.as_mut_ptr() as *mut [u8; PAGE_4K]),
                    write_page,
                    read: &*(read.as_ptr() as *const [u8; PAGE_4K]),
                }),
                LoadMut::Dirty(d) => Ok(LoadMutPage::Dirty(
                    &mut *(d.as_mut_ptr() as *mut [u8; PAGE_4K]),
                )),
            }
        }
    }

    /// Allocate a page for writing.
    #[allow(clippy::mut_from_ref)]
    fn allocate_page(&self) -> Result<(&mut [u8; PAGE_4K], u64), StorageError> {
        unsafe {
            let (data, page) = self.allocate(1)?;
            Ok((&mut *(data.as_mut_ptr() as *mut [u8; PAGE_4K]), page))
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
