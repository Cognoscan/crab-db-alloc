#![allow(dead_code)]
#![allow(unused_variables)]

/*
- 6 bytes for a page number (48 bits)
- Max size is thus 2^60, or 1 EiB (1024*1024 TiB)
- 47 sub-blocks are thus needed - the last one can never be filled because we
  pre-alloc the first 128 kiB for the root page. Also because who the heck puts
  that much storage at the behest of a single machine???
 */

const NUM_ALLOCS: usize = 47;

use std::{
    cmp::Ordering, collections::{BTreeMap, BTreeSet}, fmt::{self}, ops::{Deref, DerefMut}, path::Path, sync::{mpsc, Arc, Mutex}
};

use error::FormatError;
use memmap2::{MmapMut, MmapOptions, MmapRaw};

pub mod int_page;
pub mod block;
pub mod block_owned;
pub mod pages;
mod error;
pub mod storage;

pub use error::AllocError;
use storage::StorageInner;

/// The maximum allocation size - 1 MiB
pub const BLOCK_SIZE: usize = 1 << 20;

/// The minimum database size
pub const MIN_DB_SIZE: usize = 4 << 20;

/// A single page - should always be 4 kiB
pub const PAGE_SIZE: usize = 1 << 12;

/// A single page cluster - should be 16 kiB
pub const CLUSTER_SIZE: usize = 4 * PAGE_SIZE;

/// The size of a root page in the backing file
pub const ROOT_SIZE: usize = CLUSTER_SIZE;

/// The size of all root pages in the backing file
pub const ROOT_MAP_SIZE: usize = ROOT_SIZE * 2;

/// Struct for pulling memory right off of a memory map
#[derive(Clone)]
struct RawMemory {
    maps: Vec<&'static [u8]>,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct BlockRange {
    pub start: usize,
    pub len: usize,
}

impl BlockRange {
    pub fn new(start: usize, len: usize) -> Self {
        Self {
            start,
            len
        }
    }
}

impl RawMemory {
    unsafe fn get_mut_slice(
        &self,
        range: BlockRange,
    ) -> Result<Option<&'static mut [u8]>, AllocError> {
        let mut start = 0;
        for map in self.maps.iter() {
            let end = start + map.len();
            if range.start < end {
                let lower = range.start - start;
                let upper = range.start - start + range.len;
                let m = map.get(lower..upper).ok_or(AllocError::InvalidAccess {
                    offset: range.start,
                    len: range.len,
                })?;
                let len = m.len();
                let ptr = m.as_ptr() as *mut u8;
                return Ok(Some(std::slice::from_raw_parts_mut(ptr, len)));
            }
            start = end;
        }
        Ok(None)
    }

    /// Get a block of memory from the memory maps. Fails if the requested range is outside the
    /// memory map range, it is split across memory maps, or the backing memory map tracker was
    /// poisoned by a separate thread.
    pub unsafe fn get(
        &mut self,
        core: &Arc<DbCore>,
        range: BlockRange,
    ) -> Result<&'static mut [u8], AllocError> {
        // Check maps first
        if let Some(s) = self.get_mut_slice(range)? {
            return Ok(s);
        }

        // We ran out of maps, check the inner storage to see if we since got more
        let Ok(inner) = core.storage.lock() else {
            return Err(AllocError::Other("Backing memory's Mutex was poisoned"));
        };
        self.maps = inner.get_maps();

        // Recheck maps
        if let Some(s) = self.get_mut_slice(range)? {
            return Ok(s);
        }

        // At this point, give up. We should never actually hit this unless
        // something has gone horrifically wrong with the system that uses this
        // struct.
        Err(AllocError::InvalidAccess {
            offset: range.start,
            len: range.len,
        })
    }
}

/// Tracking of the actual state of a page that's in the "free" table
enum FreePageState {
    /// Page is already allocated and cannot be used.
    Allocated,
    /// Page will be free after the oldest reader has moved past the given transaction ID.
    FreeAfter(u64),
}

struct IdTracker {
    /// The newest ID that's been written out
    newest: u64,
    /// The oldest ID that's being used somewhere
    oldest: u64,
    /// Track which IDs are currently checked out
    tracker: Vec<(u64, usize)>,
}

impl IdTracker {
    pub fn new(id: u64) -> Self {
        Self {
            newest: id,
            oldest: id,
            tracker: Vec::new(),
        }
    }

    fn find_id(&self, id: u64) -> Option<usize> {
        self.tracker.iter().position(|(list_id, _)| *list_id == id)
    }

    pub fn newest_id(&self) -> u64 {
        self.newest
    }

    pub fn oldest_id(&self) -> u64 {
        self.oldest
    }

    pub fn set_newest(&mut self, newest: u64) {
        self.newest = newest;
    }

    /// Check a reader out at the current newest ID
    pub fn checkout(&mut self) -> u64 {
        // Record that we're checking out at a given ID
        if let Some(pos) = self.find_id(self.newest) {
            let tracker: &mut (u64, usize) = &mut self.tracker[pos];
            if tracker.0 == self.newest {
                tracker.1 += 1;
            } else {
                self.tracker.push((self.newest, 1));
            }
        } else {
            self.tracker.push((self.newest, 1));
        }
        self.newest
    }

    /// Check a reader back in
    pub fn checkin(&mut self, id: u64) {
        // Locate the checkout ID in the list
        let Some(pos) = self.find_id(id) else {
            panic!("Tried to check in an ID that was never checked out");
        };
        // Decrement the checkout ID, and if we drop an ID from the list, it's up to us to increment
        // the oldest ID known
        self.tracker[pos].1 -= 1;
        if self.tracker[pos].1 == 0 {
            self.tracker.swap_remove(pos);
            self.oldest = self
                .tracker
                .iter()
                .fold(self.newest, |acc, (id, _)| acc.min(*id));
        }
    }
}

#[derive(Default, Clone, Debug)]
struct PageReadTracker {
    read: BTreeMap<u64, usize>,
    write: BTreeMap<u64, usize>,
    done: BTreeSet<u64>,
}

impl PageReadTracker {
    /// Register a page for long term read checkout
    pub fn checkout(&mut self, page: u64) {
        if let Some(cnt) = self.write.get_mut(&page) {
            *cnt += 1;
        } else if let Some(cnt) = self.read.get_mut(&page) {
            *cnt += 1;
        } else if self.done.remove(&page) {
            self.write.insert(page, 1);
        } else {
            self.read.insert(page, 1);
        }
    }

    /// Check a page back in after concluding the long-term read
    pub fn checkin(&mut self, page: u64) {
        if let Some(cnt) = self.read.get_mut(&page) {
            *cnt -= 1;
            if *cnt == 0 {
                self.read.remove(&page);
            }
        } else if let Some(cnt) = self.write.get_mut(&page) {
            *cnt -= 1;
            if *cnt == 0 {
                self.write.remove(&page);
                self.done.insert(page);
            }
        } else {
            panic!("Read page checkin failed: the page to be checked in wasn't in either checkout list");
        }
    }

    /// Update the writer's list of checked-out pages
    pub fn update_writer(&mut self, map: &mut BTreeSet<u64>) {
        for (page, cnt) in self.read.iter() {
            map.insert(*page);
            self.write.insert(*page, *cnt);
        }
        self.read.clear();
        for page in self.done.iter() {
            map.remove(page);
        }
        self.done.clear();
    }
}

// What are our actual synchronization points:
//
// Everyone needs the backing storage, otherwise the maps could be released!
//
// | Object     | ID Tracker | Root Data | Read Tracker | WriteFree| Storage |
// | ---------- | ---------- | --------- | ------------ | -------- | ------- |
// | ReadUnit   | X          | X         | X            |          | X       |
// | Readers    | X          |           | X            |          | X       |
// | ReadBlock  |            |           | X            |          | X       |
// | WriteUnit  | X          | X         | X            | X        | X       |
// | WriteAlloc |            |           |              | X        | X       |
// | CommitUnit | X          | X         |              |          | X       |

struct DbCore {
    root: Mutex<RootData>,
    read_pages: Mutex<PageReadTracker>,
    storage: Mutex<StorageInner>,
}

struct RootCheckout {
    id: u64,
    root: Vec<u8>,
    freelist: u64,
}

#[derive(Clone, Copy, bytemuck::Zeroable, bytemuck::Pod)]
#[repr(C)]
struct RootHeader {
    file_type: [u8; 8],
    len: u16,
    version: u8,
    _reserved0: u8,
    _reserved1: u32,
    file_len: u64,
    id: u64,
    freelist: u64,
}

/// The Root data that we track and use to synchronize between readers, the writer, and the committer.
struct RootData {
    /// ID tracking
    id_tracker: IdTracker,
    /// The remaining root data from the most recent writer
    root: Vec<u8>,
    /// The freelist page
    freelist: u64,
    /// The loaded file type
    file_type: [u8; 8],
    /// The stored file size
    file_len: u64,
}


impl RootData {
    /// Create a brand new root data structure
    pub fn new(file_type: &[u8; 8], freelist: u64, file_len: u64) -> Self {
        Self {
            file_type: file_type.to_owned(),
            id_tracker: IdTracker::new(0),
            root: Vec::new(),
            freelist,
            file_len,
        }
    }

    pub fn load(root: &[u8]) -> Result<Self, AllocError> {
        let (header, rem) = root.split_at(std::mem::size_of::<RootHeader>());
        let header: &RootHeader = bytemuck::from_bytes(header);
        if header.version != 1 {
            return Err(AllocError::Open(std::io::Error::other(
                "Unrecognized version number in header",
            )));
        }
        let len = header.len as usize;
        let Some(root_data) = rem.get(0..len) else {
            return Err(AllocError::Open(std::io::Error::other(
                "Invalid length of header data",
            )));
        };
        let Some(hash) = rem.get(len..(len + 8)) else {
            return Err(AllocError::Open(std::io::Error::other(
                "xxHash missing from end of header data",
            )));
        };
        let hash = u64::from_le_bytes(hash.try_into().unwrap());

        let Some(data_for_hash) = root.get(0..(std::mem::size_of::<RootHeader>() + len)) else {
            return Err(AllocError::Open(std::io::Error::other(
                "Couldn't grab data to perform xxHash",
            )));
        };

        let nominal_hash = xxhash_rust::xxh3::xxh3_64(data_for_hash);

        if nominal_hash != hash {
            return Err(AllocError::Open(std::io::Error::other(
                "Invalid xxHash of header data",
            )));
        }

        Ok(Self {
            file_type: header.file_type,
            id_tracker: IdTracker::new(header.id),
            root: root_data.to_vec(),
            freelist: header.freelist,
            file_len: header.file_len,
        })
    }

    pub fn store(&self, dst: &mut Vec<u8>) -> Result<(), AllocError> {
        let len = u16::try_from(self.root.len()).map_err(|_| {
            AllocError::Other("Tried to write out root page data that's at least 64kiB long")
        })?;
        let header = RootHeader {
            file_type: self.file_type,
            len,
            version: 1,
            _reserved0: 0,
            _reserved1: 0,
            id: self.id_tracker.newest,
            freelist: self.freelist,
            file_len: self.file_len,
        };

        dst.clear();
        dst.extend_from_slice(bytemuck::bytes_of(&header));
        dst.extend_from_slice(&self.root);
        let hash = xxhash_rust::xxh3::xxh3_64(dst);
        dst.extend_from_slice(hash.to_le_bytes().as_slice());
        Ok(())
    }

    /// Check out for a reader
    pub fn checkout(&mut self) -> RootCheckout {
        let id = self.id_tracker.checkout();
        RootCheckout {
            freelist: self.freelist,
            id,
            root: self.root.clone(),
        }
    }

    /// Check in for a reader
    pub fn checkin(&mut self, co: &RootCheckout) {
        self.id_tracker.checkin(co.id);
    }

    /// Update from a writer
    pub fn update(&mut self, update: &RootCheckout) {
        self.root.clear();
        self.root.extend_from_slice(&update.root);
        self.id_tracker.set_newest(update.id);
        self.freelist = update.freelist;
    }
}

/// A unit for spawning read transactions
pub struct ReadUnit {
    storage: RawMemory,
    core: Arc<DbCore>,
}

impl ReadUnit {
    /// Spawn a read transaction
    pub fn reader(&self) -> ReadTxn {
        let core = self.core.clone();
        ReadTxn {
            storage: self.storage.clone(),
            core,
            root: self.core.root.lock().unwrap().checkout(),
        }
    }
}

impl Clone for ReadUnit {
    fn clone(&self) -> Self {
        let core = self.core.clone();
        Self {
            storage: self.storage.clone(),
            core,
        }
    }
}

/// An active read transaction. Prevents the allocator from reusing any pages that have been freed
/// since the start of this transaction.
pub struct ReadTxn {
    storage: RawMemory,
    core: Arc<DbCore>,
    root: RootCheckout,
}

impl Drop for ReadTxn {
    fn drop(&mut self) {
        self.core.root.lock().unwrap().checkin(&self.root);
    }
}

impl ReadTxn {
    /// Read an arbitrary point of memory in the memory map.
    unsafe fn read(&mut self, range: BlockRange) -> Result<&'static [u8], AllocError> {
        self.storage
            .get(&self.core, range)
            .map(|x: &'static mut [u8]| x as &'static [u8])
    }

    /// Check out a point in memory for long-term reads.
    ///
    /// This doesn't check to make sure the range is page-aligned - this must be upheld by the
    /// caller. The page range must also be a region that was previously allocated.
    unsafe fn get_block(&mut self, range: BlockRange) -> Result<ReadBlock, AllocError> {
        let mem = self
            .storage
            .get(&self.core, range)
            .map(|x: &'static mut [u8]| x as &'static [u8])?;
        Ok(ReadBlock {
            mem,
            page: range.start as u64,
            core: self.core.clone(),
        })
    }
}

struct ReadBlock {
    mem: &'static [u8],
    page: u64,
    core: Arc<DbCore>,
}

impl Drop for ReadBlock {
    fn drop(&mut self) {
        self.core.read_pages.lock().unwrap().checkin(self.page);
    }
}

impl Deref for ReadBlock {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.mem
    }
}

impl AsRef<[u8]> for ReadBlock {
    fn as_ref(&self) -> &[u8] {
        self.mem
    }
}

impl fmt::Debug for ReadBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadBlock")
            .field("page", &self.page)
            .field("size", &self.mem.len())
            .finish()
    }
}

/// A long-term allocated block of memory that hasn't yet been committed to the database.
///
/// Write Allocations enable multithreaded bulk writes. Many can be set up at once with [`WriteTxn::write_alloc`]
pub struct WriteAlloc {
    mem: &'static mut [u8],
    page: u64,
    chan: mpsc::Sender<u64>,
    core: Arc<DbCore>,
}

impl WriteAlloc {
    /// Get the page number that was allocated.
    fn page(&self) -> u64 {
        self.page
    }
}

impl Drop for WriteAlloc {
    /// Release the allocated page back to the allocator when dropped
    fn drop(&mut self) {
        let _ = self.chan.send(self.page);
    }
}

impl Deref for WriteAlloc {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.mem
    }
}

impl DerefMut for WriteAlloc {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.mem
    }
}

impl AsRef<[u8]> for WriteAlloc {
    fn as_ref(&self) -> &[u8] {
        self.mem
    }
}

impl AsMut<[u8]> for WriteAlloc {
    fn as_mut(&mut self) -> &mut [u8] {
        self.mem
    }
}

pub struct WriteUnitInner {
    /// Track which pages in the free list are not actually free
    taken: BTreeSet<u64>,
    /// Handle to our central synchronization primitive
    core: Arc<DbCore>,
    /// Root data (only used by the Write Transaction)
    root: RootCheckout,
    /// Track which pages are marked as dirty
    dirty: BTreeSet<u64>,
    /// Secondary "taken" tracker for use during transactions
    taken_txn: BTreeSet<u64>,
    /// List of available 4kiB pages
    available_4k: Vec<u64>,
    /// List of available 16kiB page clusters (with 4-bit tracking info in LSBs)
    available_16k: Vec<u64>,
    /// List of available blocks
    available_blocks: Vec<u64>,
    /// List of allocations that were requested
    alloc_req: Vec<WriteAlloc>,
    /// List of allocations that will hopefully be committed
    alloc_completions: Vec<WriteAlloc>,
    /// Sender to hand out to the write allocators (indicating when things become free)
    alloc_send: mpsc::Sender<u64>,
    /// Receiver to pick up when a write allocation is dropped
    alloc_recv: mpsc::Receiver<u64>,
    /// Sender to punch holes in the filesystem when freeing up a block
    hole_punch_req: mpsc::Sender<u64>,
    /// Receiver of completed hole punching operations
    hole_punch_resp: mpsc::Receiver<u64>,
    /// List of hole punch requests we'll send out on committing a transaction
    hole_punch_future_req: Vec<u64>,
}

pub struct WriteUnit(WriteUnitInner);
pub struct WriteTxn(WriteUnitInner);

impl WriteUnit {
    pub fn write(mut self) -> WriteTxn {
        // Process any pending operations from readers, write allocations, and the committer
        while let Ok(page) = self.0.hole_punch_resp.try_recv() {
            self.0.taken.remove(&page);
        }
        while let Ok(page) = self.0.alloc_recv.try_recv() {
            self.0.taken.remove(&page);
        }
        let mut read_pages = self.0.core.read_pages.lock().unwrap();
        read_pages.update_writer(&mut self.0.taken);
        drop(read_pages);

        // Clear out all the transaction working data before starting a new transaction
        self.0.dirty.clear();
        self.0.taken_txn.clear();
        self.0.available_4k.clear();
        self.0.available_16k.clear();
        self.0.available_blocks.clear();
        self.0.alloc_req.clear();
        self.0.alloc_completions.clear();
        self.0.hole_punch_future_req.clear();

        WriteTxn(self.0)
    }
}

/// Allocation information
pub struct Alloc {
    /// The byte offset to the page
    pub page: u64,
    /// The allocated number of bytes (always in increments of 4096)
    pub len: usize,
}

impl WriteTxn {
    /// Allocate a new page
    pub fn txn_allocate(&mut self, len: u64) -> Result<Alloc, AllocError> {
        let page = 0;
        self.0.dirty.insert(page);
        todo!("Actually write the allocator")
    }

    /// Allocate a page for writing by any thread at any point in time.
    ///
    /// Requested allocations are provided once the current write transaction is committed.
    ///
    /// The allocated data is not committed until the [`WriteAlloc`] is returned to an active
    /// [`WriteTxn`] and [`WriteTxn::commit`] is called.
    pub fn new_allocation(&mut self, len: u64) -> Result<(), AllocError> {
        todo!("Actually write the allocator")
    }

    /// Put a written-out allocation into this transaction
    pub fn use_allocation(&mut self, alloc: WriteAlloc) {
        self.0.alloc_completions.push(alloc);
    }

    /// Determine if the provided page is marked as dirty or not
    pub fn is_dirty(&self, page: u64) -> bool {
        self.0.dirty.contains(&page)
    }

    /// Commit the transaction to the database and optionally return the requested long-term allocations.
    pub fn commit(self, root_data: &[u8]) -> (WriteUnit, Vec<WriteAlloc>) {
        todo!("Push the remaining 4k page allocations into the allocator");
        /*
        todo!("Commit the requested allocations into the taken marker");
        todo!("Commit the completed allocations");
        todo!("Update the transaction ID number and the new freelist base page");
        //
        // Hole punch requests
        for page in self.0.hole_punch_future_req.iter().copied() {
            let _ = self.0.hole_punch_req.send(page);
            self.0.taken.insert(page);
        }
        todo!()
        */
    }

    /// Abort the current transaction, undoing all transaction operations and returning any written-out allocation.
    /// 
    /// This will panic if this is called on the first transaction on a brand-new database.
    pub fn abort(mut self) -> (WriteUnit, Vec<WriteAlloc>) {
        if self.0.root.id == 0 {
            panic!("Can't abort the very first transaction of the database");
        }
        self.0.dirty.clear();
        self.0.alloc_req.clear();
        let ret = std::mem::take(&mut self.0.alloc_completions);
        (WriteUnit(self.0), ret)
    }
}

/// Handles committing completed write transactions to disk.
///
/// This unit is basically another open read transaction, representing any future program that will
/// open the database. When it commits to disk, it grabs the current completed transaction, flushes
/// everything to disk synchronously and advances to that completed transaction ID.
///
/// For anonymous memory maps, no sync to disk occurs, but this does still need to be called.
///
/// Committing after every write transaction is generally a good idea, though this should be done in
/// a separate thread, as this is a blocking operation.
pub struct CommitUnit {
    /// The current "checked-out" ID we're holding onto
    id: u64,
    /// The data to commit to the root page
    commit_data: Vec<u8>,
    /// Any pending hole punch operations
    hole_punch_req: mpsc::Receiver<u64>,
    /// Completed hole punch operations
    hole_punch_resp: mpsc::Sender<u64>,
    /// The two root pages to write to
    root0: &'static mut [u8],
    root1: &'static mut [u8],
    write_root0: bool,
    /// Access to the core database synchronization primitives
    core: Arc<DbCore>,
}

impl CommitUnit {
    pub fn commit(&mut self) -> Result<(), AllocError> {
        // Acquire our next transaction ID now, as we're about to commit everything up to this
        // point. We also need to grab the current state of the Root that we want to write out.
        let new_id = {
            let mut mutex = self.core.root.lock().unwrap();
            mutex.store(&mut self.commit_data)?;
            let new_id = mutex.id_tracker.checkout();
            drop(mutex);
            new_id
        };

        // Perform the main flush
        let res = {
            let mutex = self.core.storage.lock().unwrap();
            let res = mutex.flush();
            drop(mutex);
            res
        };
        if res.is_err() {
            // We failed to sync, so we need to undo our new checkout and retain the old one.
            // This probably isn't recoverable, but just in case, we should act as correctly as possible.
            self.core.root.lock().unwrap().id_tracker.checkin(new_id);
            return res;
        }

        // Update the tree root
        let root_write = if self.write_root0 { &mut self.root0 } else { &mut self.root1 };
        let Some(root_write) = root_write.get_mut(0..self.commit_data.len()) else {
            self.core.root.lock().unwrap().id_tracker.checkin(new_id);
            return Err(AllocError::Other(
                "Tried to write root data that was too large for the root page",
            ));
        };
        root_write.copy_from_slice(&self.commit_data);

        // Flush the tree root
        let root_block = BlockRange::new(if self.write_root0 { 0 } else { ROOT_SIZE }, ROOT_SIZE);
        let res = {
            let mutex = self.core.storage.lock().unwrap();
            let res = mutex.flush_range(root_block);
            drop(mutex);
            res
        };
        if res.is_err() {
            // We failed to sync, so we need to undo our new checkout and retain the old one.
            // This probably isn't recoverable, but just in case, we should act as correctly as possible.
            self.core.root.lock().unwrap().id_tracker.checkin(new_id);
            return res;
        }

        // Swap in the new read transaction id
        self.core.root.lock().unwrap().id_tracker.checkin(self.id);
        self.id = new_id;
        Ok(())
    }
}

type AllocTuple = (ReadUnit, WriteUnit, CommitUnit);

#[derive(Clone, Debug)]
struct OpenOptions {
    size: Option<usize>,
    file_type: [u8; 8],
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            size: None,
            file_type: *b"crab-db\0",
        }
    }
}

impl OpenOptions {
    /// Set the desired size of the opened file allocator. If one isn't
    /// provided, this will default to the minimum of 16 MiB, or if the file
    /// exists, then the file size. If the file exists and has a larger size
    /// than the one set here, then the file's size is used instead.
    pub fn size(&mut self, size: usize) -> &mut Self {
        self.size = Some(size);
        self
    }

    /// Set the desired file type header for creating a new database. If one isn't specified, this
    /// will default to the byte string "crab-db\0".
    pub fn file_type(&mut self, file_type: &[u8; 8]) -> &mut Self {
        self.file_type = *file_type;
        self
    }
    
    /// Open an anonymous memory map isntead of an on-disk file.
    pub fn open_anon(&self) -> Result<AllocTuple, AllocError> {
        let size = self.size.unwrap_or_default().max(MIN_DB_SIZE);
        let map = MmapRaw::from(
            MmapMut::map_anon(size).map_err(|e| AllocError::AllocFailed {
                requested: size,
                source: e,
            })?,
        );
        let storage = StorageInner::init(map, None);
        todo!()
    }

    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<AllocTuple, AllocError> {
        use fs4::fs_std::FileExt;

        if (page_size::get() != PAGE_SIZE) && (page_size::get() != CLUSTER_SIZE) {
            return Err(AllocError::Other("System page size is neither 4kiB nor 16kiB."));
        }

        // Open and lock the file
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(AllocError::Open)?;
        file.try_lock_exclusive().map_err(AllocError::Lock)?;

        // Figure out the file size and resize as needed.
        let file_size = file.metadata().map_err(AllocError::Open)?.len();
        if file_size > (usize::MAX as u64) {
            return Err(AllocError::Other(
                "The file is larger than can be memory-mapped in this architecture",
            ));
        }
        let file_size = file_size as usize;
        let is_new = file_size == 0;
        if (file_size > 0 && file_size < MIN_DB_SIZE) || ((file_size & (BLOCK_SIZE - 1)) != 0) {
            return Err(AllocError::DataFormat(error::FormatError::FileSize));
        }
        let requested_size = (self.size.unwrap_or(MIN_DB_SIZE) & !(BLOCK_SIZE - 1))
            .max(MIN_DB_SIZE)
            .max(file_size);
        if requested_size != file_size {
            file.set_len(file_size as u64)
                .map_err(|e| AllocError::ResizeFailed {
                    size: file_size,
                    requested: requested_size,
                    source: e,
                })?;
        }

        let map = MmapOptions::new()
            .len(requested_size)
            .map_raw(&file)
            .map_err(|e| AllocError::AllocFailed {
                requested: requested_size,
                source: e,
            })?;
        

        let storage = StorageInner::init(map, Some(file));
        let read_storage = RawMemory {
            maps: unsafe { storage.get_maps() },
        };

        let commit_root0 = unsafe { read_storage.get_mut_slice(BlockRange::new(0, ROOT_SIZE)).unwrap().unwrap() };
        let commit_root1 = unsafe { read_storage.get_mut_slice(BlockRange::new(ROOT_SIZE, ROOT_SIZE)).unwrap().unwrap() };
        let (mut root, commit_write_root0) = if is_new {
            (RootData::new(&self.file_type, 0, 0), true)
        } else {
            let root0 = RootData::load(commit_root0);
            let root1 = RootData::load(commit_root1);
            match (root0, root1) {
                (Err(e0), Err(e1)) => return Err(e0),
                (Ok(root), Err(_)) => (root, false),
                (Err(_), Ok(root)) => (root, true),
                (Ok(root0), Ok(root1)) => {
                    match root0.id_tracker.newest.cmp(&root1.id_tracker.newest) {
                        Ordering::Equal => return Err(AllocError::DataFormat(FormatError::DuplicateIds)),
                        Ordering::Greater => (root0, false),
                        Ordering::Less => (root1, true),
                    }
                }
            }
        };
        let commit_id = root.id_tracker.checkout();

        let write_root_checkout = if is_new {
            RootCheckout {
                id: root.id_tracker.newest,
                root: Vec::new(),
                freelist: ROOT_MAP_SIZE as u64,
            }
        }
        else {
            RootCheckout {
                id: root.id_tracker.newest,
                root: root.root.clone(),
                freelist: root.freelist,
            }
        };

        let core = Arc::new(DbCore {
            root: Mutex::new(root),
            read_pages: Mutex::new(PageReadTracker::default()),
            storage: Mutex::new(storage),
        });

        let (alloc_send, alloc_recv) = mpsc::channel();
        let (write_hole_punch_req, commit_hole_punch_req) = mpsc::channel();
        let (commit_hole_punch_resp, write_hole_punch_resp) = mpsc::channel();

        let write = WriteTxn(WriteUnitInner {
            taken: BTreeSet::new(),
            core: core.clone(),
            root: write_root_checkout,
            dirty: BTreeSet::new(),
            taken_txn: BTreeSet::new(),
            available_4k: Vec::new(),
            available_16k: Vec::new(),
            available_blocks: Vec::new(),
            alloc_req: Vec::new(),
            alloc_completions: Vec::new(),
            alloc_send,
            alloc_recv,
            hole_punch_req: write_hole_punch_req,
            hole_punch_resp: write_hole_punch_resp,
            hole_punch_future_req: Vec::new(),
        });

        if is_new {
            // If we're brand new, forcibly set up our freelist and then populate in our initial pages
            todo!("Set up the freelist");
            /*
            if page_size::get() == PAGE_SIZE {
                for page in ((ROOT_MAP_SIZE as u64)..(BLOCK_SIZE as u64)).step_by(PAGE_SIZE) {
                    write.0.available_4k.push(page);
                }
            }
            else {
                for page in ((ROOT_MAP_SIZE as u64)..(BLOCK_SIZE as u64)).step_by(CLUSTER_SIZE) {
                    write.0.available_16k.push(page);
                }
            }
            for page in ((BLOCK_SIZE as u64)..(requested_size as u64)).step_by(BLOCK_SIZE) {
                write.0.available_blocks.push(page);
            }
            */
        }
        else if requested_size != file_size {
            // If we're not brand new, and our 

        }
        // If the actual file size

        let read = ReadUnit {
            storage: read_storage,
            core: core.clone(),
        };

        let commit = CommitUnit {
            id: commit_id,
            commit_data: Vec::new(),
            hole_punch_req: commit_hole_punch_req,
            hole_punch_resp: commit_hole_punch_resp,
            root0: commit_root0,
            root1: commit_root1,
            write_root0: commit_write_root0,
            core,
        };

        // Determine if we have

        //let root = RootData::load()
        todo!()
    }
}

pub fn alloc_anon(size: usize) -> Result<AllocTuple, AllocError> {
    OpenOptions::default().size(size).open_anon()
}

pub fn alloc_open<P: AsRef<Path>>(path: P) -> Result<AllocTuple, AllocError> {
    OpenOptions::default().open(path)
}

// Page numbers are up to 6 bytes - the upper 2 bytes are for other shit.
// For the root page, the entry format is:
// 6 bytes pointing to the sub-page
// 2 bytes indicating # of entries in page, but uppermost bit indicates if leaf of branch.
// 8 bytes of xxhash

// The root page of the allocator is:
// - 16-byte entries for each possible sub-page - there are 47 total (6 bytes of page number, highest can never exist)
// - 16-byte header for the "to-free" list
// - 16-byte entries pointing to each "to-free" list

// Each freelist is actually a btreemap, potentially terminating

pub struct Allocator {}

pub struct AllocInfo {
    addr: u64,
    pages: usize,
}
