/*!
Components definining the interface into a transactional storage system.

`crab-dads` requires a storage system, which must handle allocating and
deallocating regions of memory. Memory [`Region`]s are 4 kiB aligned memory that
are sized in multiples of 4 kiB. Allocated regions are persistent; even after
dropping the reference, the storage system must hold onto their content until
they are manually deallocated. Regions can be retrieved and deallocated by
referring to their associated [`u64`] value, referred to as a Region ID.

A storage system consists of at least two core structures, a
[reader][StorageReader] and a [writer][StorageWriter]. There can only ever be
one writer, but multiple readers are permitted. A writer can initiate a
[write transaction][WriteTxn], which can read existing regions, deallocate
regions, and allocate new regions. A write transaction can be
[committed][WriteTxn::commit], which makes new allocations visible to readers,
and removes their ability to read deallocated regions. A reader can initiate a
[read transaction][ReadTxn], which captures the state of the storage system at
its most recent commit, allowing regions to be read without worrying about them
being deallocated. Transactions always have some data associated with them,
stored as a slice of [`u64`] values, which can be used to determine where to
start reading and writing within the storage system.

A storage system may also have durable backing storage, like a hard drive.
Backing storage can be used to persist a storage system, such that its state can
be restored even after a program finishes. This restore operation can be thought
of as a read transaction, where the reader is some future program. A
[committer][StorageCommitter] is an additional structure that updates the
backing storage. Each call to [StorageCommitter::commit] points its future read
transaction to the most recent committed write transaction. This is how the
"Durability" property of an ACID database can be upheld.

Some storage systems may also support making new allocations outside of a write
transaction, using an implementor of [`WriteAllocator`]. These
[`WriteAllocation`]s can be submitted as part of a write transaction in the same
storage system. It's write transaction struct must also implement
[`WriteTxnAllocation`] as part of this. These allocations only can become
visible to future read transactions once they have been submitted to a write
transaction, and that write transaction has been committed. There can be
multiple [`WriteAllocator`] instances per storage system, allowing for
multi-threaded allocations and multi-threaded large writes.

Some storage systems may also support holding on to a read-only memory region
after a read transaction is completed, where only the one region is guaranteed
to be preserved instead of the entire state of the storage system. These regions
implement [`ReadAllocation`], and can be retrieved from a read transaction that
implements [`ReadTxnAllocation`].

*/
#![no_std]

pub const PAGE_4K: usize = 4096;

mod error;

use core::ops::{Deref, DerefMut};

pub use error::StorageError;

#[cfg(feature = "test-alloc")]
pub mod test_alloc;

/// A memory region that is 4 kiB aligned and sized in multiples of 4 kiB.
#[repr(transparent)]
pub struct Region([u8]);

impl Region {

    /// Return the number of 4 kiB pages in this region.
    pub fn num_pages(&self) -> usize {
        self.len() / PAGE_4K
    }
    /// Construct a Region from a raw pointer and a length.
    ///
    /// # Safety
    /// This carries the same requirements of [`core::slice::from_raw_parts`],
    /// and also requires that the raw pointer must be 4 kiB aligned, and the
    /// length must be a multiple of 4 kiB.
    pub unsafe fn from_raw_parts<'a>(ptr: *const u8, len: usize) -> &'a Region {
        debug_assert!(len.is_multiple_of(PAGE_4K), "Must be a multiple of 4 kiB page size");
        debug_assert!(
            ptr.align_offset(PAGE_4K) == 0,
            "Region alignment isn't to 4 kiB boundary"
        );
        // SAFETY: Region is marked as #[repr(transparent)] so conversion
        // from a *mut [u8] to a *mut Region is OK here.
        unsafe { &*(core::ptr::slice_from_raw_parts(ptr, len) as *const Region) }
    }

    /// Construct a mutable `Region` from a raw pointer and a length.
    ///
    /// # Safety
    /// This carries the same requirements of
    /// [`core::slice::from_raw_parts_mut`], and also requires that the raw
    /// pointer must be 4 kiB aligned, and the length must be a multiple of 4
    /// kiB.
    pub unsafe fn from_raw_parts_mut<'a>(ptr: *mut u8, len: usize) -> &'a mut Region {
        debug_assert!(len.is_multiple_of(PAGE_4K), "Must be a multiple of 4 kiB page size");
        debug_assert!(
            ptr.align_offset(PAGE_4K) == 0,
            "Region alignment isn't to 4 kiB boundary"
        );
        // SAFETY: RegionMut is marked as #[repr(transparent)] so conversion
        // from a *mut [u8] to a *mut RegionMut is OK here.
        unsafe { &mut *(core::ptr::slice_from_raw_parts_mut(ptr, len) as *mut Region) }
    }
}

impl AsRef<[u8]> for Region {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsMut<[u8]> for Region {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl Deref for Region {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Region {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Access to a backing reader.
///
/// # Safety
///
/// Implementors must ensure that loadable regions are never mutably loaded by
/// any [`WriteTxn`] implementation that points to the same storage system.
/// Loadable regions are regions that were allocated by a [`WriteTxn`]
/// implementation, committed, and haven't been deallocated in a transaction
/// yet.
pub unsafe trait ReadTxn {
    /// Load a memory region.
    ///
    /// # Safety
    ///
    /// Only regions reachable through reading other regions with `load` or the
    /// root database page may be loaded with this function.
    unsafe fn load(&self, region: u64) -> Result<&Region, StorageError>;

    /// Get the root data associated with this read point.
    fn root_data(&self) -> &[u64];

    /// Return how many write transactions have elapsed since opening this transaction.
    fn elapsed(&self) -> u64;

    /// Tell the reader that a region may be needed soon, and should be made
    /// available. This is primarily for readers that know they'll soon be
    /// reading a large known set of regions. It shouldn't be used for
    /// individual regions that are immediately loaded.
    ///
    /// This is advisory. Implementors can always do nothing.
    #[allow(unused_variables)]
    fn will_need(&self, region: u64) {}

    /// Tell the reader that a region won't be needed soon, and can be freed up.
    /// This is primarily for readers that are streaming regions of data, and
    /// don't expect to return to them soon.
    ///
    /// This is advisory. Implementors can always do nothing.
    #[allow(unused_variables)]
    fn wont_need(&self, region: u64) {}
}

/// A loaded mutable memory region. Always 4-kiB aligned.
///
/// If the requested region is not marked as dirty (i.e. it hasn't been loaded
/// by the writer), then the clean version of the region is returned along with
/// a new allocation to write the region to.
///
/// Once the dirty region has been set up, the clean region should be freed up
/// by calling [`WriteTxn::deallocate`] on the writer.
pub enum LoadMut<'a> {
    Clean {
        write: &'a mut Region,
        write_page: u64,
        read: &'a Region,
    },
    Dirty(&'a mut Region),
}

/// A trait describing a write transaction.
///
/// Implementors of this trait act as a write transaction for a transactional
/// storage system. Stored memory regions can be loaded, new regions can be
/// allocated, and existing regions can be deallocated. When dropped, the
/// actions taken on this transaction are discarded by the storage system. When
/// committed, the actions become visible to all subsequent read & write
/// transactions, as well as any commit operations on durable backing storage.
///
/// # Safety
///
/// Implementors of this trait must ensure new memory region allocations are
/// unique and do not overlap with any other memory regions. When regions
/// are loaded, regions must be mutable _only_ within this transaction,
/// and should be allocated within this transaction. The
/// [`load_mut`][WriteTxn::load_mut] function returns an enumeration to help
/// uphold this.
///
/// When regions are deallocated, implementors may not remove underlying memory
/// until all read transactions that could refer to it are also closed. An
/// implementor may assume that if no read transactions (or backing committers)
/// exist that are older than the deallocating write transaction, then the
/// underlying memory is no longer in use, and can be removed or reused by the
/// storage system.
pub unsafe trait WriteTxn: ReadTxn {
    /// Load a memory region for writing. If the range that's been requested is
    /// not available for writing, it should return the
    /// [`Clean`][LoadMut::Clean] result with a newly allocated region to write
    /// to. If the region is available for writing, then
    /// [`Dirty`][LoadMut::Dirty] should be returned instead.
    ///
    /// # Safety
    ///
    /// Only region IDs provided from [`Self::allocate`] can be used here, and
    /// they must not be used after [`Self::deallocate`] is called with a region
    /// ID.
    unsafe fn load_mut(&self, region: u64) -> Result<LoadMut<'_>, StorageError>;

    /// Allocate a memory region for writing, in increments of 4kiB pages.
    /// `num_pages` must be non-zero.
    ///
    /// All allocations will be unique, and correspondingly have unique `u64`
    /// values returned. After this allocation is dropped, its contents may
    /// later be read back with [`load_mut`][Self::load_mut].
    ///
    /// Allocations may be larger than the number of 4kiB pages requested.
    // mut_from_ref is allowed here specifically because we are an allocator,
    // and return different mutable objects on each allocation.
    #[allow(clippy::mut_from_ref)]
    fn allocate(&self, num_pages: usize) -> Result<(&mut Region, u64), StorageError>;

    /// Deallocate a region previously allocated by `load_mut` or `allocate`.
    ///
    /// # Safety
    ///
    /// This must only be called with page numbers that were allocated, and can
    /// only be called with them once.
    unsafe fn deallocate(&self, region: u64) -> Result<(), StorageError>;

    /// Commit this sequence of allocations, loads, and deallocations, making it
    /// visible to [readers][StorageReader] and [committers][StorageCommitter]
    /// once this function completes. The associated data is also made visible.
    fn commit(self, data: &[u64]) -> Result<(), StorageError>;

    /// Return the number of new pages that have been allocated since this
    /// transaction was opened. This includes any added external allocations.
    fn added_pages(&self) -> u64;
}

/// A [`WriteTxn`] implementor that also supports adding write allocations.
pub trait WriteTxnAllocation: WriteTxn {
    type WA: WriteAllocation;

    /// Add an allocation into a write transaction, making it permanent if the
    /// write transaction is committed.
    ///
    /// Implementors should verify that the allocation and the write transaction
    /// both came from the same storage system.
    unsafe fn add_alloc(&self, alloc: Self::WA) -> Result<(), StorageError>;
}

pub trait WriteAllocation: DerefMut<Target = Region> {
    /// Get the region ID
    fn region_u64(&self) -> u64;
}

pub trait WriteAllocator: Clone {
    /// The write allocation type.
    type WA: WriteAllocation;

    /// Allocate a new region of memory for eventual use in a write transaction.
    fn allocate(&self, num_pages: usize) -> Result<Self::WA, StorageError>;
}

/// Trait for holding a long-term readable memory region.
///
/// Read allocations hold a memory region from a particular read transaction,
/// ensuring they are available without forcing the storage system to hold on to
/// everything else from that trasnaction.
pub trait ReadAllocation: Deref<Target = Region> {
    /// Get the region ID
    fn region_u64(&self) -> u64;
}

/// Trait for retrieving a [`ReadAllocation`] during a read transaction.
pub trait ReadTxnAllocation: ReadTxn {
    /// The type returned on loading a read allocation.
    type RA: ReadAllocation;

    /// Load a specific region for long-term read availability.
    fn load_alloc(&self, region: u64) -> Result<Self::RA, StorageError>;
}

/// Trait for a storage system struct that supports starting write transactions.
/// 
/// This is generally implemented on a mutable borrow of a struct.
pub trait StorageWriter {
    /// The write transaction type.
    type W: WriteTxn;

    /// Start a write transaction.
    fn write(self) -> Self::W;

    /// Extract database statistics. Can be useful for determining when to
    /// trigger a commit to durable storage.
    fn stats(&self) -> StorageStats;
}

/// Statistics on the database, which may be useful for determining when to
/// commit to durable storage, or for reporting system overhead.
/// 
/// The sum of `free_pages` and `used_pages` should always match `total_pages`.
/// `uncommitted_pages` and `storage_pages` are always less than `used_pages`.
#[derive(Clone, Debug)]
pub struct StorageStats {
    /// The total number of pages in the database.
    pub total_pages: u64,
    /// How many free pages are in the database.
    pub free_pages: u64,
    /// How many pages have been written but not yet committed to durable storage yet.
    /// Storage systems that do not have durable storage (i.e. no
    /// [`StorageCommitter`]) will always return 0.
    pub uncommitted_pages: u64,
    /// How many pages are currently allocated in the database, including
    /// internal storage system pages.
    pub used_pages: u64,
    /// How many internal pages are in use by the internal storage system.
    pub storage_pages: u64,
    /// How many write transactions have happened over the database lifetime.
    pub writes: u64,
    /// How many write transactions have been made but not committed to durable
    /// storage yet. Storage systems that do not have durable storage (i.e. no
    /// [`StorageCommitter`]) will always return 0.
    pub uncommitted_writes: u64,
}

/// Trait for a storage system struct that supports starting read transactions.
pub trait StorageReader: Clone {
    // The read transaction type.
    type R: ReadTxn;

    /// Start a read transaction.
    fn read(self) -> Self::R;
}

/// Trait for a storage system struct that supports committing to durable
/// storage.
pub trait StorageCommitter {
    /// Commit any completed write transactions to backing storage, along with any relevant ancillary data.
    fn commit(&self, data: &[u8]) -> Result<(), StorageError>;
}
