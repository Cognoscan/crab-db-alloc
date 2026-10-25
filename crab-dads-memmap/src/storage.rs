use std::fs::File;

use memmap2::{MmapMut, MmapOptions, MmapRaw, RemapOptions};

use crate::{AllocError, BlockRange};

pub(crate) enum ExpandStorage {
    ReplaceLastMap(&'static mut [u8]),
    NewMap(&'static mut [u8]),
}

/// This tracks all allocated memory maps and holds onto the optional backing file. Readers,
/// writers, and committers each should wrap this struct.
pub(crate) struct StorageInner {
    maps: Vec<MmapRaw>,
    file: Option<File>,
}

impl StorageInner {

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
    pub unsafe fn hole_punch(&mut self, mut hole: BlockRange) -> Result<(), AllocError> {
        let mut idx = 0;
        for map in self.maps.iter_mut() {
            if hole.start >= (idx + map.len()) {
                idx += map.len();
                continue;
            }
            let start = hole.start - idx;
            let len = hole.len.min(map.len() - start);
            if self.file.is_some() {
                #[cfg(not(windows))]
                map.unchecked_advise_range(memmap2::UncheckedAdvice::Remove, start, len)
                    .map_err(AllocError::HolePunch)?;
            } else {
                #[cfg(not(windows))]
                map.unchecked_advise_range(memmap2::UncheckedAdvice::Free, start, len)
                    .map_err(AllocError::HolePunch)?;
            }
            hole.start += len;
            hole.len -= len;
            if hole.len == 0 {
                break;
            }
        }
        Ok(())
    }

    /// Flush all memory maps.
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
        let (last, rest) = self.maps.split_last().unwrap_unchecked();
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
