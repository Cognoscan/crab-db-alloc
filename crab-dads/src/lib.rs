//#![no_std]
#![warn(unsafe_op_in_unsafe_fn)]

extern crate alloc;

pub mod arrays;
mod trailer;
pub use trailer::*;
pub mod btree;
pub mod page;

#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageError {
    /// I/O error in storage system.
    Io(&'static str),
    /// Database corruption detected.
    Corruption(&'static str),
    /// Rust memory safety violation detected.
    Safety(&'static str),
    /// Out of range request was made.
    OutOfRange(u64),
}

impl From<StorageError> for Error {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

impl core::fmt::Display for StorageError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(s) => write!(f, "I/O Error: {}", s),
            Self::Corruption(s) => write!(f, "Database corruption: {}", s),
            Self::Safety(s) => write!(f, "Safety violation: {}", s),
            Self::OutOfRange(r) => write!(
                f,
                "Page outside of storage range was requested: Page 0x{:x}",
                r
            ),
        }
    }
}

impl core::error::Error for StorageError {}

/// Error that can be returned while processing a data page
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// Out of space; needed at least N bytes.
    OutofSpace(usize),
    /// Data format on disk was corrupted somehow.
    DataCorruption,
    /// A requested write operation was too large to fit within the disk map.
    WriteTooLarge,
    /// Storage system-related error.
    Storage(StorageError),
    /// Expected to perform an operation like a split or rebalance, but nothing
    /// could be done.
    UnexpectedNoOp,
    /// Attempted to perform an invalid operation (ex. trying to resize a u64 key).
    IncorrectOperation,
    /// Database structure entered into an invalid state.
    InvalidState(&'static str),
}

impl core::error::Error for Error {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        if let Self::Storage(e) = self {
            Some(e)
        } else {
            None
        }
    }
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::OutofSpace(s) => write!(f, "No space left in page to insert, needed {s} bytes"),
            Self::DataCorruption => f.write_str("Data Corruption"),
            Self::WriteTooLarge => f.write_str("Provided Key/Value is too large to fit in the map"),
            Self::Storage(_) => f.write_str("Storage system error"),
            Self::UnexpectedNoOp => f.write_str(
                "Expected to perform an operation (page split, page rebalance) but couldn't",
            ),
            Self::IncorrectOperation => {
                f.write_str("attempted to perform a nonsensical operation on the database")
            }
            Self::InvalidState(s) => write!(f, "Invalid database system state: {}", s),
        }
    }
}

/// 4 kiB page. Standard on most architectures, particularly x64, RISC-V, and
/// non-Apple ARM.
const PAGE_4K: usize = 1 << 12;
