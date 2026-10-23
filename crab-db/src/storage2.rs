use std::{
    fs::File,
    path::Path,
    sync::{Arc, Mutex, RwLock},
};

use memmap2::{MmapMut, MmapOptions, MmapRaw, RemapOptions};

use crate::{AllocError, BlockRange, MIN_DB_SIZE, ROOT_SIZE};

pub(crate) enum ExpandStorage {
    ReplaceLastMap(&'static mut [u8]),
    NewMap(&'static mut [u8]),
}

// Committer:
// - Needs files and memory maps to perform its job
// - Exclusive control of the first two page regions, in order to perform a commit
//   - We can get this by separately mapping the first two pages, and giving that map to just the committer.
// - Processes hole punches, taking them from a provided list and moving them to a "completed" list.
//   - Hole punch requires memory maps, but not the backing file
// - Hole punch and commit both take the components for an extended period of time.
//
// Writer:
// - Needs 2 MiB page list (shared with write allocator system)
// - Needs memory maps and file in order to get pages, and to resize.
//     - We need to think about who gets what operations here.
//
// Reader:
// - Needs memory maps to get pages.
//
// WriteAllocator:
// - Needs 2 MiB page list
// - Needs memory maps and file in order to get pages, and to resize.
//
// File/Memory map operations:
// - Get mut page - writer, writeallocator, committer
// - Get page - writer, reader, writeallocator
// - Expand storage - writer, writeallocator
// - datasync, file sync - committer
//
// It kind of seems like each unit wants direct access to the memory maps, but
// for different purposes. Unsurprising. The shared state is much smaller:
// - Write/WriteAllocator needs:
//     - File to do a resize - can be separate from a msync call, I think.
//     - Way to communicate when a new memory map has landed, and where it is
//     - Way to communicate when a full msync is needed due to a resize
//     - Way to get new memory maps that have landed
// - Reader needs:
//     - Way to get new memory maps that have landed
// - Committer needs:
//     - Way to get new memory maps that have landed
//     - Way to get information about needing a full msync
//
// So on opening a file, we create:
// - Arc<(Mutex<SharedObj>, AromicU64)>
// - CommitObj
//     - Arc<RwLock<Memory Maps>> - new memory maps, full msync discovered by seeing the map length has changed since last commit. Read only.
//         - Lock contention can get bad when a commit is happening and the writer needs to expand the file. It doesn't _have_ to contend though.
//         - Perhaps we do Arc<(RwLock<MmapRaw>, RwLock<MmapRaw>)>? That should
//         reduce contention significantly. I think we wrap the memory map
//         thing overall though.
//     - File - Just for when we need to sync the metadata as well.
//
// This tracks all allocated memory maps and holds onto the optional backing file. Readers,
// writers, and committers each should wrap this struct.
//
//
// So, the question is, when I need to make a commit, that means I have to grab
// the memory maps and file, right? Because I need to force a data sync and a
// file sync.
// Also, if I want to hole-punch, I need to grab all of this. Hole-punching
// also sort of lies outside of the reader/writer/committer set, but can
// interfere with writers and committers as I need to grab the appropriate
// memory map and punch it.
// Actually I think I should qualify it as a change the committer makes, as
// it's something that hits the backing file / memory maps. That's more the
// purview of the committer.

pub struct StorageInner {
    /// The main set of memory maps
    maps: RwLock<Vec<MmapRaw>>,
    /// Memory maps that haven't yet been added into the main map set. Normally
    /// empty, briefly present until a committer can move them to `maps`.
    new_maps: RwLock<Vec<MmapRaw>>,
    /// Tracks the size of the memory maps. Locked to ensure only one writer at
    /// a time can expand the memory maps.
    expand_tracker: Mutex<u64>,
    /// The underlying file backing the memory maps, if one exists.
    file: Option<File>,
}

/// Write access to the storage system. Does not manage memory.
#[derive(Clone)]
pub struct StorageWriter {
    inner: Arc<StorageInner>,
}

/// Read access to the storage system. Does not manage memory.
#[derive(Clone)]
pub struct StorageReader {
    inner: Arc<StorageInner>,
}

/// Commit handling for the storage system. Does not manage memory.
pub struct StorageCommitter {
    inner: Arc<StorageInner>,
    page0: MmapMut,
    page1: MmapMut,
    file_type: [u8; 8],
}

/// Configuration for opening a backing storage system.
#[derive(Clone, Debug)]
pub struct OpenOptions {
    size: Option<usize>,
    file_type: [u8; 8],
}

/// The three types of opened storage components.
pub struct StorageTuple {
    pub writer: StorageWriter,
    pub reader: StorageReader,
    pub committer: StorageCommitter,
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

    pub fn open_anon(&self) -> Result<StorageTuple, AllocError> {
        fn map_anon(size: usize) -> Result<MmapMut, AllocError> {
            MmapMut::map_anon(size).map_err(|e| AllocError::AllocFailed {
                requested: size,
                source: e,
            })
        }
        let size = self.size.unwrap_or_default().max(MIN_DB_SIZE);
        let page0 = map_anon(ROOT_SIZE)?;
        let page1 = map_anon(ROOT_SIZE)?;
        let map = MmapRaw::from(map_anon(size)?);
        let inner = Arc::new(StorageInner {
            maps: RwLock::new(vec![map]),
            new_maps: RwLock::new(Vec::new()),
            expand_tracker: Mutex::new(0),
            file: None,
        });
        let writer = StorageWriter {
            inner: inner.clone(),
        };
        let reader = StorageReader {
            inner: inner.clone(),
        };
        let committer = StorageCommitter {
            inner: inner.clone(),
            page0,
            page1,
            file_type: self.file_type,
        };
        Ok(StorageTuple {
            writer,
            reader,
            committer,
        })
    }

    pub fn open_file<P: AsRef<Path>>(&self, path: P) -> Result<StorageTuple, AllocError> {
        //if (page_size::get() != PAGE_SIZE) && (page_size::get() != CLUSTER_SIZE) {
        //    return Err(AllocError::Other("System page size is neither 4kiB nor 16kiB."));
        //}
        todo!()
    }
}

pub(crate) struct OldStorageInner {
    maps: Vec<MmapRaw>,
    file: Option<File>,
}

impl OldStorageInner {
    /// Initialize with a memory map and an optional backing file.
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

    fn get_map_range(&self, range: BlockRange) -> Result<(&MmapRaw, BlockRange), AllocError> {
        let mut start = 0;
        for map in self.maps.iter() {
            let end = start + map.len();
            if range.start < end {
                if (range.start + range.len) > end {
                    return Err(AllocError::InvalidAccess {
                        offset: range.start,
                        len: range.len,
                    });
                }
                return Ok((
                    map,
                    BlockRange {
                        start: range.start - start,
                        len: range.len,
                    },
                ));
            }
            start = end;
        }
        Err(AllocError::InvalidAccess {
            offset: range.start,
            len: range.len,
        })
    }

    /// Expand the backing storage, either by expanding the file and then memory
    /// mapping it if this is file-backed, or by creating a new anonymous memory
    /// map if there is no backing file.
    pub unsafe fn expand(&mut self, new_alloc: usize) -> Result<ExpandStorage, AllocError> {
        // Is this file-backed?
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
            // We're an anonymous memory map, expand that or create a new anonymous map
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

    /// Punch a hole in a memory map.
    ///
    /// For a file-backed map, this should tell the file system to remove the selected range from
    /// the file, provided the file system supports sparse files.
    ///
    /// The hole request should not span multiple memory maps. In general, this means punching holes of size 2 MiB
    ///
    /// For an anonymous memory map, this frees a section of memory back to the system.
    ///
    /// In both of the above cases, this often results in a complete TLB flush. Because of this, and
    /// the tracking information needed for sparse maps, it's recommended that a block range be on
    /// the order of 1 MiB or larger.
    ///
    /// # Safety
    ///
    /// This function is functionally writing junk to the range requested (though the OS will
    /// generally zero out the memory). As such, it is up to the caller to ensure that there no
    /// other writers or readers that have borrowed this chunk of the maps.
    pub unsafe fn hole_punch(&self, hole: BlockRange) -> Result<(), AllocError> {
        // Begin by finding the hole to punch
        let (map, hole) = self.get_map_range(hole)?;
        if self.file.is_some() {
            #[cfg(not(windows))]
            map.unchecked_advise_range(memmap2::UncheckedAdvice::Remove, hole.start, hole.len)
                .map_err(AllocError::HolePunch)?;
        } else {
            #[cfg(not(windows))]
            map.unchecked_advise_range(memmap2::UncheckedAdvice::Free, hole.start, hole.len)
                .map_err(AllocError::HolePunch)?;
        }
        Ok(())
    }

    /// Flush all memory maps.
    #[cfg(not(windows))]
    pub fn flush(&self) -> Result<(), AllocError> {
        if self.file.is_none() {
            return Ok(());
        }
        // On POSIX, we just flush each memory map in sequence, ensuring any
        // modifications to any of them land on the backing storage.
        for map in self.maps.iter() {
            map.flush().map_err(AllocError::Sync)?;
        }
        Ok(())
    }

    /// Flush all memory maps.
    #[cfg(windows)]
    pub fn flush(&self) -> Result<(), AllocError> {
        if self.file.is_none() {
            return Ok(());
        }
        // On Windows, the way we actually flush maps to disk is by flushing
        // every map with an async call, then synchronizing on the file handle
        // itself. Thus, we only need to call the synchronous flush on the final
        // map.
        //
        // What memmap2 does here is call `FlushViewOfFile` for each async call,
        // then the last flush calls `FlushViewOfFile` to the map followed by
        // `FlushFileBuffers`. See the Windows `memoryapi.h` documentation for
        // why this is the process.
        //
        // Safety: We will always have at least one memory map in our storage.
        // All initializers for this struct guarantee it, and the list of maps
        // only ever grows.
        let (last, rest) = unsafe { self.maps.split_last().unwrap_unchecked() };
        for map in rest.iter() {
            map.flush_async().map_err(AllocError::Sync)?;
        }
        last.flush().map_err(AllocError::Sync)?;
        Ok(())
    }

    /// Flush a range within a single memory map. Errors if the range crosses memory maps.
    pub fn flush_range(&self, range: BlockRange) -> Result<(), AllocError> {
        if self.file.is_none() {
            return Ok(());
        }

        let mut start = 0;
        for map in self.maps.iter() {
            let end = start + map.len();
            if range.start < end {
                if (range.start + range.len) > end {
                    return Err(AllocError::InvalidAccess {
                        offset: range.start,
                        len: range.len,
                    });
                }
                map.flush_range(range.start - start, range.len)
                    .map_err(AllocError::Sync)?;
                return Ok(());
            }
            start = end;
        }
        Err(AllocError::InvalidAccess {
            offset: range.start,
            len: range.len,
        })
    }
}
