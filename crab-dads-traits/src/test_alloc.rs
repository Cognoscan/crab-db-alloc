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
    memory: BTreeMap<u64, Box<[u8]>>,
    checkouts: BTreeMap<u64, u64>,
    commit: u64,
    root_data: Vec<u64>,
    used_pages: u64,
}

struct CheckoutFmt<'a>(&'a BTreeMap<u64, u64>);
impl<'a> std::fmt::Debug for CheckoutFmt<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[ ")?;
        for (commit, count) in self.0.iter() {
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
            .field("root_data", &self.root_data)
            .field("commit", &self.commit)
            .field("checkouts", &CheckoutFmt(&self.checkouts))
            .field("memory", &MemoryFmt(&self.memory))
            .finish()
    }
}

/// A simple database reader unit.
#[derive(Clone)]
pub struct BasicDbReader {
    inner: Arc<RwLock<BasicDbInner>>,
}

impl<'a> StorageReader for &'a BasicDbReader {
    type R = BasicDbRead<'a>;

    fn read(self) -> Self::R {
        let mut inner = self.inner.write().unwrap();

        let commit = inner.commit;
        *inner.checkouts.entry(commit).or_default() += 1;
        let root_data = inner.root_data.clone();
        BasicDbRead {
            inner: self,
            commit,
            root_data,
        }
    }
}

/// A simple database read transaction.
pub struct BasicDbRead<'a> {
    inner: &'a BasicDbReader,
    commit: u64,
    root_data: Vec<u64>,
}

impl<'a> Drop for BasicDbRead<'a> {
    fn drop(&mut self) {
        let Ok(mut inner) = self.inner.inner.write() else {
            return;
        };
        let count = inner
            .checkouts
            .entry(self.commit)
            .and_modify(|x| *x -= 1)
            .or_default();
        if *count == 0 {
            inner.checkouts.remove(&self.commit);
        }
    }
}

unsafe impl<'a> ReadTxn for BasicDbRead<'a> {
    unsafe fn load(&self, region: u64) -> Result<&Region, StorageError> {
        let inner = self.inner.inner.read().unwrap();
        let mem = inner
            .memory
            .get(&region)
            .ok_or(StorageError::OutOfRange(region))?;
        // We won't drop this memory until this
        // reader's checkout advances (or it is dropped)
        unsafe { Ok(Region::from_raw_parts(mem.as_ptr(), mem.len())) }
    }

    fn root_data(&self) -> &[u64] {
        &self.root_data
    }

    fn elapsed(&self) -> u64 {
        let write_commit = self.inner.inner.read().unwrap().commit;
        write_commit - self.commit
    }
}

fn alloc_paged(pages: usize) -> Box<[u8]> {
    let len = PAGE_4K * pages;
    unsafe {
        // Unsafe ops are to ensure the allocated memory is page-aligned.
        let ptr = std::alloc::alloc(core::alloc::Layout::from_size_align_unchecked(len, PAGE_4K));
        Box::from_raw(core::ptr::slice_from_raw_parts_mut(ptr, len))
    }
}

/// Construct a new memory-only, simplified database.
pub fn new_db() -> (BasicDbReader, BasicDbWriter) {
    // Actual interior state of the database
    let memory = BTreeMap::new();
    let root_data = Vec::new();

    // Construct the interior shared state.
    let inner = Arc::new(RwLock::new(BasicDbInner {
        memory,
        checkouts: BTreeMap::new(),
        root_data: root_data.clone(),
        commit: 0,
        used_pages: 0,
    }));

    let read = BasicDbReader {
        inner: inner.clone(),
    };

    let write = BasicDbWriter {
        inner,
        page_num: 0,
        to_drop: VecDeque::new(),
    };
    (read, write)
}

/// A simple in-memory writer for a database.
#[derive(Debug)]
pub struct BasicDbWriter {
    inner: Arc<RwLock<BasicDbInner>>,
    page_num: u64,
    to_drop: VecDeque<(u64, Vec<u64>)>,
}

impl<'a> StorageWriter for &'a mut BasicDbWriter {
    type W = BasicDbWrite<'a>;
    fn write(self) -> Self::W {
        let root_data = self.inner.read().unwrap().root_data.clone();
        let cell = RefCell::new(BasicDbWriteCell {
            page_num: self.page_num,
            dirty: BTreeMap::new(),
            to_drop: Vec::new(),
            new_pages: 0,
        });
        BasicDbWrite {
            inner: self,
            cell,
            root_data,
        }
    }

    fn stats(self) -> StorageStats {
        let inner = self.inner.read().unwrap();
        let used_pages = inner.used_pages;
        let writes = inner.commit;
        drop(inner);
        StorageStats {
            total_pages: used_pages,
            free_pages: 0,
            uncommitted_pages: 0,
            used_pages,
            storage_pages: 0,
            writes,
            uncommitted_writes: 0,
        }
    }
}

#[derive(Debug)]
pub struct BasicDbWrite<'a> {
    inner: &'a mut BasicDbWriter,
    cell: RefCell<BasicDbWriteCell>,
    root_data: Vec<u64>,
}

struct BasicDbWriteCell {
    page_num: u64,
    dirty: BTreeMap<u64, Box<[u8]>>,
    to_drop: Vec<u64>,
    new_pages: u64,
}

impl std::fmt::Debug for BasicDbWriteCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BasicDbWriteCell")
            .field("page_num", &self.page_num)
            .field("dirty", &MemoryFmt(&self.dirty))
            .field("to_drop", &self.to_drop)
            .finish_non_exhaustive()
    }
}

unsafe impl<'a> ReadTxn for BasicDbWrite<'a> {
    unsafe fn load(&self, region: u64) -> Result<&Region, StorageError> {
        let inner = self.inner.inner.read().unwrap();
        let cell: &BasicDbWriteCell = &self.cell.borrow();
        let mem = inner
            .memory
            .get(&region)
            .or_else(|| cell.dirty.get(&region))
            .ok_or(StorageError::OutOfRange(region))?;
        // We pinky-promised that we won't drop this memory until this
        // reader's checkout advances (or it is dropped)
        unsafe { Ok(Region::from_raw_parts(mem.as_ptr(), mem.len())) }
    }

    fn root_data(&self) -> &[u64] {
        &self.root_data
    }

    fn elapsed(&self) -> u64 {
        0
    }
}

unsafe impl<'a> WriteTxn for BasicDbWrite<'a> {

    fn added_pages(&self) -> u64 {
        self.cell.borrow().new_pages        
    }

    fn allocate(&self, num_pages: usize) -> Result<(&mut Region, u64), StorageError> {
        assert!(num_pages > 0);
        unsafe {
            // Allocate the memory
            let mut mem = alloc_paged(num_pages);
            let raw = Region::from_raw_parts_mut(mem.as_mut_ptr(), mem.len());
            // Update the inner write transaction
            let mut cell = self.cell.borrow_mut();
            let region = cell.page_num;
            cell.page_num += 1;
            cell.dirty.insert(region, mem);
            cell.new_pages += num_pages as u64;
            Ok((raw, region))
        }
    }

    unsafe fn deallocate(&self, region: u64) -> Result<(), StorageError> {
        if (self.cell.borrow_mut().dirty.remove(&region)).is_some() {
            return Ok(());
        }
        self.cell.borrow_mut().to_drop.push(region);
        Ok(())
    }

    unsafe fn load_mut(&self, region: u64) -> Result<LoadMut<'_>, StorageError> {
        unsafe {
            if let Some(p) = self.cell.borrow_mut().dirty.get_mut(&region) {
                return Ok(LoadMut::Dirty(Region::from_raw_parts_mut(
                    p.as_mut_ptr(),
                    p.len(),
                )));
            }
            let read = self.load(region)?;
            let (write, write_page) = self.allocate(read.num_pages())?;
            Ok(LoadMut::Clean {
                write,
                write_page,
                read,
            })
        }
    }

    /// Commit all outstanding write operations to the database.
    fn commit(self, data: &[u64]) -> Result<(), StorageError> {
        // Decompose our transaction into its inner content
        let writer = self.inner;
        let mut db = writer.inner.write().unwrap();
        let write = self.cell.into_inner();

        // Move the dirty pages into the full tree map.
        for (page, data) in write.dirty.into_iter() {
            db.memory.insert(page, data);
        }

        // Update the root info of the database
        db.commit += 1;
        db.root_data.clear();
        db.root_data.extend_from_slice(data);
        db.used_pages += write.new_pages;

        // See if we can work through any of the "to_free" lists
        let oldest_checkout = db.checkouts.keys().next().copied().unwrap_or(u64::MAX);
        while let Some(drop_list) = writer
            .to_drop
            .pop_front_if(|(commit, _)| oldest_checkout > *commit)
        {
            let list = drop_list.1;
            for page in list {
                let _ = db.memory.remove(&page);
            }
        }

        // Update the writer-only content
        writer.to_drop.push_back((db.commit, write.to_drop));
        writer.page_num = write.page_num;
        Ok(())
    }
}
