extern crate std;
use std::{
    boxed::Box,
    cell::RefCell,
    collections::{btree_map::BTreeMap, vec_deque::VecDeque},
    sync::{Arc, RwLock},
    vec,
    vec::Vec,
};

use super::*;

#[derive(Clone)]
struct BasicDbInner {
    root: u64,
    memory: BTreeMap<u64, Box<[u8]>>,
    checkouts: Vec<(u64, u64)>,
    commit: u64,
}

struct CheckoutFmt<'a>(&'a [(u64, u64)]);
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
                if (idx & 0x3) == 3 {
                    f.write_str(" ")?;
                }
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

/// A simple database reader unit.
///
/// Reads are synchronized with the writer whenever [`reload`][Self::reload] is
/// called.
pub struct BasicDbRead {
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
    /// Update this reader to pull in the latest commits from the writer.
    pub fn reload(self) -> Self {
        let new = self.clone();
        drop(self);
        new
    }

    /// Get the root page of the database.
    pub fn root(&self) -> u64 {
        self.root
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
        let ptr = std::alloc::alloc(core::alloc::Layout::from_size_align_unchecked(len, PAGE_4K));
        Box::from_raw(core::ptr::slice_from_raw_parts_mut(ptr, len))
    }
}

/// Construct a new memory-only, simplified database.
pub fn new_db<F>(init: F) -> (BasicDbRead, BasicDbWrite)
where
    F: Fn(&mut [u8; PAGE_4K]),
{
    let mut memory = BTreeMap::new();
    let mut mem = alloc_paged(1);
    unsafe {
        init(&mut *(mem.as_mut_ptr() as *mut [u8; 4096]));
    }
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
        cell: RefCell::new(BasicDbWriteCell {
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

/// A simple in-memory writer for a database.
#[derive(Debug)]
pub struct BasicDbWrite {
    inner: Arc<RwLock<BasicDbInner>>,
    cell: RefCell<BasicDbWriteCell>,
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
            .field("to_drop", &self.to_drop)
            .finish_non_exhaustive()
    }
}

impl BasicDbWrite {
    /// Get how many pages are currently in the entire database.
    pub fn page_count(&self) -> usize {
        let mem_len = self.inner.read().unwrap().memory.len();
        let dirty_len = self.cell.borrow().dirty.len();
        mem_len + dirty_len
    }

    /// Commit all outstanding write operations to the database.
    pub fn commit(&mut self) {
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
        let oldest_co = inner.checkouts.first().map(|(c, _)| *c).unwrap_or(u64::MAX);
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

    /// Reset and forget all outstanding writes to the database.
    pub fn reset(&mut self) {
        let cell = self.cell.get_mut();
        if let Some((co, _)) = cell.to_drop.back() {
            if *co == (self.commit + 1) {
                cell.to_drop.pop_back();
            }
        }
        cell.dirty.clear();
        cell.page_num = self.starting_page_num;
        cell.root = self.starting_root;
    }

    pub fn root(&self) -> u64 {
        self.cell.borrow().root
    }

    pub fn update_root(&self, root: u64) {
        self.cell.borrow_mut().root = root;
    }
}

unsafe impl RawRead for BasicDbWrite {
    unsafe fn load(&self, page: u64, num_pages: usize) -> Result<&[u8], StorageError> {
        let inner = self.inner.read().unwrap();
        let cell: &BasicDbWriteCell = &self.cell.borrow();
        let mem = inner
            .memory
            .get(&page)
            .or_else(|| cell.dirty.get(&page))
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
            let page_num = self.cell.borrow().page_num;
            self.cell.borrow_mut().page_num += 1;

            let mut mem = alloc_paged(num_pages);
            let raw = core::slice::from_raw_parts_mut(mem.as_mut_ptr(), mem.len());
            self.cell.borrow_mut().dirty.insert(page_num, mem);
            Ok((raw, page_num))
        }
    }

    unsafe fn deallocate(&self, page: u64, _num_pages: usize) -> Result<(), StorageError> {
        if (self.cell.borrow_mut().dirty.remove(&page)).is_some() {
            return Ok(());
        }
        let to_drop = &mut self.cell.borrow_mut().to_drop;
        if let Some(td) = to_drop.back_mut() {
            if td.0 == (self.commit + 1) {
                td.1.push(page);
            } else {
                to_drop.push_back((self.commit + 1, vec![page]));
            }
        } else {
            to_drop.push_back((self.commit + 1, vec![page]));
        }
        Ok(())
    }

    unsafe fn load_mut(&self, page: u64, num_pages: usize) -> Result<LoadMut<'_>, StorageError> {
        unsafe {
            if let Some(p) = self.cell.borrow_mut().dirty.get_mut(&page) {
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
