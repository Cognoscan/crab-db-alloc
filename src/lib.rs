#![allow(dead_code)]
#![allow(unused_variables)]

use std::{
    fs::File,
    path::Path,
    sync::{Arc, RwLock},
};
use thiserror::Error;

use memmap2::{MmapMut, MmapOptions, MmapRaw, RemapOptions};

pub mod block;
pub mod block_owned;

/// The minimum database size
pub const MIN_DB_SIZE: usize = 4 << 20;

/// The size of a root page in the backing file
pub const ROOT_SIZE: usize = 1 << 16;

/// The size of all root pages in the backing file
pub const ROOT_MAP_SIZE: usize = (1 << 16) * 2;

struct StorageInner {
    maps: Vec<MmapRaw>,
    file: Option<File>,
}

enum ExpandStorage {
    ReplaceLastMap(&'static mut [u8]),
    NewMap(&'static mut [u8]),
}

impl StorageInner {
    pub fn init(map: MmapRaw, file: Option<File>) -> Self {
        Self {
            maps: vec![map],
            file,
        }
    }

    /// Extract raw slices pointing to the the memory maps with unbounded
    /// lifetimes.
    ///
    /// # Safety
    ///
    /// The caller MUST ensure the returned references don't outlive the memory
    /// maps. This can be done by ensuring all of these references are dropped
    /// before the backing memory map is, and ensuring that the caller never
    /// presents it as a 'static to anything that doesn't uphold the same
    /// condition.
    pub unsafe fn get_maps(&self) -> Vec<&'static [u8]> {
        self.maps
            .iter()
            .map(|m| {
                let len = m.len();
                let ptr = m.as_ptr();
                std::slice::from_raw_parts(ptr, len)
            })
            .collect()
    }

    /// Expand the backing storage, either by expanding the file and then memory
    /// mapping it if this is file-backed, or by creating a new anonymous memory
    /// map if there is no backing file.
    pub unsafe fn expand(&mut self, new_alloc: usize) -> Result<ExpandStorage, AllocError> {
        if let Some(file) = self.file.as_ref() {
            // Resize the file first
            let current_size = file.metadata().map_err(AllocError::Open)?.len();
            file.set_len(new_alloc as u64 + current_size).map_err(|e| {
                AllocError::ResizeFailed {
                    size: current_size as usize,
                    requested: current_size as usize + new_alloc,
                    source: e,
                }
            })?;
            // Update the metadata in order to get the new file size stored
            file.sync_all().map_err(AllocError::Sync)?;

            // On Linux, we might be able to just expand the last memory map
            #[cfg(target_os = "linux")]
            {
                let map = self.maps.last_mut().unwrap_unchecked();
                let new_size = map.len() + new_alloc;
                if map
                    .remap(new_size, RemapOptions::new().may_move(false))
                    .is_ok()
                {
                    let slice = std::slice::from_raw_parts_mut(map.as_mut_ptr(), map.len());
                    return Ok(ExpandStorage::ReplaceLastMap(slice));
                }
            }

            let map = MmapOptions::new()
                .offset(current_size)
                .len(new_alloc)
                .map_raw(file)
                .map_err(|e| AllocError::AllocFailed {
                    requested: new_alloc,
                    source: e,
                })?;
            let ret = std::slice::from_raw_parts_mut(map.as_mut_ptr(), new_alloc);
            self.maps.push(map);
            Ok(ExpandStorage::NewMap(ret))
        } else {
            // On Linux, we might be able to just expand the last memory map
            #[cfg(target_os = "linux")]
            {
                let map = self.maps.last_mut().unwrap_unchecked();
                let new_size = map.len() + new_alloc;
                if map
                    .remap(new_size, RemapOptions::new().may_move(false))
                    .is_ok()
                {
                    let slice = std::slice::from_raw_parts_mut(map.as_mut_ptr(), map.len());
                    return Ok(ExpandStorage::ReplaceLastMap(slice));
                }
            }

            let map = MmapRaw::from(MmapMut::map_anon(new_alloc).map_err(|e| {
                AllocError::AllocFailed {
                    requested: new_alloc,
                    source: e,
                }
            })?);
            let ret = std::slice::from_raw_parts_mut(map.as_mut_ptr(), new_alloc);
            self.maps.push(map);
            Ok(ExpandStorage::NewMap(ret))
        }
    }

    pub fn hole_punch(&mut self, _hole: BlockRange) -> Result<(), AllocError> {
        todo!()
    }

    /// Flush all memory maps
    #[cfg(not(windows))]
    pub fn flush(&self) -> Result<(), AllocError> {
        if self.file.is_none() {
            return Ok(());
        }
        for map in self.maps.iter() {
            map.flush().map_err(AllocError::Sync)?;
        }
        Ok(())
    }

    /// Flush all memory maps
    #[cfg(windows)]
    pub fn flush(&self) -> Result<(), AllocError> {
        if self.file.is_none() {
            return Ok(());
        }
        // On Windows, the way we actually flush maps to disk is by flushing
        // every map with an async call, then synchronizing on the file handle
        // itself. Thus, we only need to call the synchronous flush on the final
        // map.
        let (last, rest) = self.maps.split_last().unwrap_unchecked();
        for map in rest.iter() {
            map.flush_async().map_err(AllocError::Sync)?;
        }
        last.flush().map_err(AllocError::Sync)?;
        Ok(())
    }
}

struct Reader {
    /// Raw maps to the backing store. This should be first so that it's always
    /// dropped before the actual backing store is.
    maps: Vec<&'static [u8]>,
    /// The backing store we can read from.
    inner: Arc<RwLock<StorageInner>>,
}

struct BlockRange {
    start: usize,
    len: usize,
}

impl Reader {
    unsafe fn get(&mut self, range: BlockRange) -> Result<&[u8], AllocError> {
        // Check maps first
        let mut start = 0;
        for map in self.maps.iter() {
            let end = start + map.len();
            if range.start < end {
                let lower = range.start - start;
                let upper = range.start - start + range.len;
                return map.get(lower..upper).ok_or(AllocError::InvalidAccess {
                    offset: range.start,
                    len: range.len,
                });
            }
            start = end;
        }

        // We ran out of maps, check the inner storage to see if we since got more
        let Ok(inner) = self.inner.read() else {
            return Err(AllocError::Other("Backing memory's RwLock was poisoned"));
        };
        self.maps = inner.get_maps();

        // Recheck maps
        let mut start = 0;
        for map in self.maps.iter() {
            let end = start + map.len();
            if range.start < end {
                let lower = range.start - start;
                let upper = range.start - start + range.len;
                return map.get(lower..upper).ok_or(AllocError::InvalidAccess {
                    offset: range.start,
                    len: range.len,
                });
            }
            start = end;
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

impl Clone for Reader {
    fn clone(&self) -> Self {
        // Make sure we clone the backing store before the maps
        let inner = self.inner.clone();
        let maps = self.maps.clone();
        Self { inner, maps }
    }
}

pub struct ReadUnit {
    storage: Reader,
}

pub struct ReadTxn {}

pub struct WriteTxn {}

pub struct WriteUnit {}

pub struct CommitUnit {}

type AllocTuple = (ReadUnit, WriteUnit, CommitUnit);

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AllocError {
    /// Couldn't open the backing file
    #[error("Opening the backing file failed")]
    Open(#[source] std::io::Error),
    /// Couldn't lock the backing file
    #[error("Failed to lock the backing file for exclusive use")]
    Lock(#[source] std::io::Error),
    /// Couldn't synchronize to the backing file
    #[error("Synchronizing to the backing file failed")]
    Sync(#[source] std::io::Error),
    /// Couldn't resize the backing file
    #[error(
        "Can't resize the backing file. Have 0x{size:x} bytes, wanted to get 0x{requested:x} bytes"
    )]
    ResizeFailed {
        size: usize,
        requested: usize,
        source: std::io::Error,
    },
    /// Couldn't allocate any more space
    #[error("Can't allocate any more memory map space. Tried to get 0x{requested:x} bytes")]
    AllocFailed {
        requested: usize,
        source: std::io::Error,
    },
    #[error("Punching a hole in the sparse memory map failed")]
    HolePunch(#[source] std::io::Error),
    /// Other, miscellaneous errors
    #[error("Other: {0}")]
    Other(&'static str),
    #[error("Invalid access on the memory map was attempted. Tried to get slice at offset 0x{offset:x} with length 0x{len:x}")]
    InvalidAccess { offset: usize, len: usize },
}

#[derive(Default, Clone, Debug)]
struct OpenOptions {
    size: Option<usize>,
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
        use fs4::FileExt;

        // Open and lock the file
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
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
        let mut size = self.size.unwrap_or_default().max(MIN_DB_SIZE);
        if size > file_size {
            file.set_len(size as u64)
                .map_err(|e| AllocError::ResizeFailed {
                    size: file_size,
                    requested: size,
                    source: e,
                })?;
            size = file_size;
        }
        let root_map = MmapOptions::new()
            .len(ROOT_MAP_SIZE)
            .map_raw(&file)
            .map_err(|e| AllocError::AllocFailed {
                requested: ROOT_MAP_SIZE,
                source: e,
            })?;
        let map = MmapOptions::new()
            .offset(ROOT_MAP_SIZE as u64)
            .len(size - ROOT_MAP_SIZE)
            .map_raw(&file)
            .map_err(|e| AllocError::AllocFailed {
                requested: size - ROOT_MAP_SIZE,
                source: e,
            })?;
        let storage = StorageInner::init(map, Some(file));
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
