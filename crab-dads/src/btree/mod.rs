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
#[allow(dead_code)]
mod test {
    extern crate std;
    use core::{alloc::GlobalAlloc, cell::UnsafeCell};
    use std::prelude::rust_2021::*;
    use std::sync::RwLock;

    use std::vec;
    use std::dbg;

    use alloc::{collections::{btree_map::BTreeMap, vec_deque::VecDeque}, sync::Arc};

    use crate::{
        page::{LayoutU64U64, LayoutU64Var, PageMapMut},
        Error,
    };

    use super::*;

    #[derive(Clone)]
    struct BasicDbInner {
        root: u64,
        memory: BTreeMap<u64, Box<[u8]>>,
        checkouts: Vec<(u64, u64)>,
        commit: u64,
    }

    struct CheckoutFmt<'a>(&'a [(u64,u64)]);
    impl<'a> std::fmt::Debug for CheckoutFmt<'a> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("[ ")?;
            for (commit, count) in self.0 {
                write!(f, "{}:{}, ", commit, count)?;
            }
            f.write_str("]")
        }
    }

    struct MemoryFmt<'a>(&'a BTreeMap<u64, Box<[u8]>>);
    impl<'a> std::fmt::Debug for MemoryFmt<'a> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            for page in self.0.iter() {
                writeln!(f, "Page {}", page.0)?;
                f.write_str("    ")?;
                for (idx, byte) in page.1.iter().enumerate() {
                    write!(f, "{:02x}", byte)?;
                    if (idx & 0x3) == 3 { f.write_str(" ")?; }
                    if (idx & 0x1F) == 0x1F { 
                        writeln!(f)?;
                        f.write_str("    ")?;
                    }
                }
                writeln!(f)?;
            }
            Ok(())
        }
    }

    impl std::fmt::Debug for BasicDbInner {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("BasicDbInner")
            .field("root", &self.root)
            .field("commit", &self.commit)
            .field("checkouts", &CheckoutFmt(self.checkouts.as_slice()))
            .field("memory", &MemoryFmt(&self.memory))
            .finish()
        }
    }

    #[derive(Debug)]
    struct BasicDbRead {
        inner: Arc<RwLock<BasicDbInner>>,
        root: u64,
        commit: u64,
    }

    impl Clone for BasicDbRead {
        fn clone(&self) -> Self {
            let mut inner = self.inner.write().unwrap();

            let commit = inner.commit;
            let co = inner.checkouts.iter_mut().rev().find(|(c, _)| c == &commit);
            if let Some(co) = co {
                co.1 += 1;
            } else {
                inner.checkouts.push((commit, 1));
            }
            let root = inner.root;

            Self {
                inner: self.inner.clone(),
                root,
                commit,
            }
        }
    }

    impl BasicDbRead {
        pub fn reload(self) -> Self {
            let new = self.clone();
            drop(self);
            new
        }

        pub fn tree(&self) -> Result<BTreeRead<'_, LayoutU64U64, LayoutU64Var, Self>, Error> {
            unsafe { BTreeRead::load(self, self.root) }
        }
    }

    unsafe impl RawRead for BasicDbRead {
        unsafe fn load(&self, page: u64, num_pages: usize) -> Result<&[u8], StorageError> {
            let inner = self.inner.read().unwrap();
            let mem = inner
                .memory
                .get(&page)
                .ok_or(StorageError::OutOfRange(page))?;
            if mem.len() != (num_pages * PAGE_4K) {
                return Err(StorageError::Corruption(
                    "Incorrect size for the requested page",
                ));
            }
            // We pinky-promised that we won't drop this memory until this
            // reader's checkout advances (or it is dropped)
            unsafe { Ok(core::slice::from_raw_parts(mem.as_ptr(), mem.len())) }
        }
    }

    impl Drop for BasicDbRead {
        fn drop(&mut self) {
            let mut inner = self.inner.write().unwrap();

            let old_co = inner
                .checkouts
                .iter_mut()
                .position(|(c, _)| c == &self.commit)
                .unwrap();
            inner.checkouts[old_co].1 -= 1;
            if inner.checkouts[old_co].1 == 0 {
                inner.checkouts.remove(old_co);
            }
        }
    }

    fn alloc_paged(pages: usize) -> Box<[u8]> {
        unsafe {
            let len = PAGE_4K * pages;
            let ptr =
                std::alloc::alloc(core::alloc::Layout::from_size_align_unchecked(len, PAGE_4K));
            Box::from_raw(core::ptr::slice_from_raw_parts_mut(ptr, len))
        }
    }

    fn new_db() -> (BasicDbRead, BasicDbWrite) {
        let mut memory = BTreeMap::new();
        let mut mem = alloc_paged(1);
        PageMapMut::<LayoutU64Var>::new(unsafe { &mut *(mem.as_mut_ptr() as *mut [u8; 4096]) }, 1);
        memory.insert(0, mem);

        let inner = Arc::new(RwLock::new(BasicDbInner {
            root: 0,
            memory,
            checkouts: vec![(0, 1)],
            commit: 0,
        }));
        let read = BasicDbRead {
            inner: inner.clone(),
            root: 0,
            commit: 0,
        };

        let write = BasicDbWrite {
            inner,
            cell: UnsafeCell::new(BasicDbWriteCell {
                dirty: BTreeMap::new(),
                to_drop: VecDeque::new(),
                page_num: 1,
                root: 0,
            }),
            commit: 0,
            starting_page_num: 1,
            starting_root: 0,
        };
        (read, write)
    }

    #[derive(Debug)]
    struct BasicDbWrite {
        inner: Arc<RwLock<BasicDbInner>>,
        cell: UnsafeCell<BasicDbWriteCell>,
        commit: u64,
        starting_page_num: u64,
        starting_root: u64,
    }

    struct BasicDbWriteCell {
        page_num: u64,
        dirty: BTreeMap<u64, Box<[u8]>>,
        to_drop: VecDeque<(u64, Vec<u64>)>,
        root: u64,
    }

    impl std::fmt::Debug for BasicDbWriteCell {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("BasicDbWriteCell")
            .field("root", &self.root)
            .field("page_num", &self.page_num)
            .field("dirty", &MemoryFmt(&self.dirty))
            .finish_non_exhaustive()
        }
    }

    impl BasicDbWrite {
        fn commit(&mut self) {
            let mut inner = self.inner.write().unwrap();

            // Move the dirty pages into the full tree map.
            let dirty = &mut self.cell.get_mut().dirty;
            while let Some(d) = dirty.pop_last() {
                inner.memory.insert(d.0, d.1);
            }

            // Update the rest of our state.
            self.starting_page_num = self.cell.get_mut().page_num;
            self.starting_root = self.cell.get_mut().root;
            self.commit += 1;
            inner.root = self.cell.get_mut().root;
            inner.commit += 1;

            // Ditch any unused pages
            let oldest_co = inner.checkouts.first().map(|(c,_)| *c).unwrap_or(u64::MAX);
            while let Some(d) = self.cell.get_mut().to_drop.pop_front() {
                if d.0 >= oldest_co {
                    self.cell.get_mut().to_drop.push_front(d);
                    break;
                }
                for d in d.1 {
                    inner.memory.remove(&d);
                }
            }
        }

        fn reset(&mut self) {
            let cell = self.cell.get_mut();
            if let Some((co, _)) = cell.to_drop.back() {
                if *co == (self.commit+1) {
                    cell.to_drop.pop_back();
                }
            }
            cell.dirty.clear();
            cell.page_num = self.starting_page_num;
            cell.root = self.starting_root;
        }

        fn tree(&mut self) -> Result<BTreeWrite<'_, LayoutU64U64, LayoutU64Var, Self>, Error> {
            unsafe {
                let (tree, root) = BTreeWrite::load(self, (*self.cell.get()).root)?;
                if let Some(root) = root {
                    (*self.cell.get()).root = root;
                }
                Ok(tree)
            }
        }

    }

    unsafe impl RawRead for BasicDbWrite {
        unsafe fn load(&self, page: u64, num_pages: usize) -> Result<&[u8], StorageError> {
            let inner = self.inner.read().unwrap();
            let mem = inner
                .memory
                .get(&page)
                .ok_or(StorageError::OutOfRange(page))?;
            if mem.len() != (num_pages * PAGE_4K) {
                return Err(StorageError::Corruption(
                    "Incorrect size for the requested page",
                ));
            }
            // We pinky-promised that we won't drop this memory until this
            // reader's checkout advances (or it is dropped)
            unsafe { Ok(core::slice::from_raw_parts(mem.as_ptr(), mem.len())) }
        }
    }

    unsafe impl RawWrite for BasicDbWrite {
        fn allocate(&self, num_pages: usize) -> Result<(&mut [u8], u64), StorageError> {
            unsafe {
                let page_num = (*self.cell.get()).page_num;
                (*self.cell.get()).page_num += 1;

                let mut mem = alloc_paged(num_pages);
                let raw = core::slice::from_raw_parts_mut(mem.as_mut_ptr(), mem.len());
                (*self.cell.get()).dirty.insert(page_num, mem);
                Ok((raw, page_num))
            }
        }

        unsafe fn deallocate(&self, page: u64, _num_pages: usize) -> Result<(), StorageError> {
            unsafe {
                if (*self.cell.get()).dirty.remove(&page).is_some() {
                    return Ok(());
                }
                let to_drop = &mut (*self.cell.get()).to_drop;
                if let Some(td) = to_drop.back_mut() {
                    if td.0 == (self.commit+1) {
                        td.1.push(page);
                    }
                    else {
                        to_drop.push_back((self.commit+1, vec![page]));
                    }
                }
                else {
                    to_drop.push_back((self.commit+1, vec![page]));
                }
                Ok(())
            }
        }

        unsafe fn load_mut(&self, page: u64, num_pages: usize) -> Result<LoadMut, StorageError> {
            unsafe {
                if let Some(p) = (*self.cell.get()).dirty.get_mut(&page) {
                    return Ok(LoadMut::Dirty(core::slice::from_raw_parts_mut(
                        p.as_mut_ptr(),
                        p.len(),
                    )));
                }
                let read = self.load(page, num_pages)?;
                let (write, write_page) = self.allocate(num_pages)?;
                Ok(LoadMut::Clean {
                    write,
                    write_page,
                    read,
                })
            }
        }
    }

    #[test]
    fn debug_allocator() {
        let (reader, mut writer) = new_db();
        let p = writer.allocate_page().unwrap();
        let p_num = p.1;
        let (p, _) = p.0.split_at_mut(4);
        p.copy_from_slice(&[5,6,7,8]);
        writer.commit();
        unsafe {
            let p = reader.load(p_num, 1).unwrap();
            let (p, _) = p.split_at(4);
            dbg!(p);
        }
    }

    #[test]
    fn sequential_insert() {
        let (reader, mut writer) = new_db();
        let mut tree = writer.tree().unwrap();
        let i_len = 10000;
        for i in 0..i_len {

            match tree.entry(&i).unwrap() {
                Entry::Occupied(_) => panic!("All entries should be empty right now"),
                Entry::Vacant(v) => {
                    v.insert(i.to_le_bytes().as_slice()).unwrap();
                }
            }
        }
        writer.commit();

        dbg!(&writer);

        let reader = reader.reload();
        let tree = reader.tree().unwrap();
        for i in 0..i_len {
            dbg!(i);
            let val = tree.get(&i).unwrap().unwrap();
            assert_eq!(val, i.to_le_bytes().as_slice());
        }
    }

    #[test]
    fn sequential_insert_rev() {
        let (reader, mut writer) = new_db();
        let mut tree = writer.tree().unwrap();
        let i_len = 227;
        for i in (0..i_len).rev() {

            match tree.entry(&i).unwrap() {
                Entry::Occupied(_) => panic!("All entries should be empty right now"),
                Entry::Vacant(v) => {
                    v.insert(i.to_le_bytes().as_slice()).unwrap();
                }
            }
        }
        writer.commit();

        //dbg!(&writer);

        let reader = reader.reload();
        let tree = reader.tree().unwrap();
        tree.debug_dump().unwrap();
        for i in (0..i_len).rev() {
            let Some(val) = tree.get(&i).expect("no error") else {
                panic!("expected to get a value for {}", i);
            };
            assert_eq!(val, i.to_le_bytes().as_slice());
        }
    }
}
