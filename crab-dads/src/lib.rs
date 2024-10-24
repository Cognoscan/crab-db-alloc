use thiserror::Error;

mod trailer;
//pub mod pages;
pub mod arrays;
pub use trailer::*;
pub mod btree;
pub mod page;

/// Error that can be returned while processing a data page
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    #[error("No space left in page to insert, needed {0} bytes")]
    OutofSpace(usize),
    #[error("Data Corruption")]
    DataCorruption,
    #[error("Provided Key/Value is too large to fit in the map")]
    WriteTooLarge,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BTreeError {
    OutofSpace(usize),
    DataCorruption { trace: Vec<u64> },
    WriteTooLarge,
}

impl core::fmt::Display for BTreeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::OutofSpace(s) => write!(
                f,
                "Backing memory ran out of space when requesting {s} bytes"
            ),
            Self::DataCorruption { trace } => {
                write!(f, "Data corruption detected, page backtrace: [ ")?;
                for t in trace {
                    write!(f, "0x{t:x}, ")?;
                }
                write!(f, "]")
            }
            Self::WriteTooLarge => write!(
                f,
                "Provided key or value was too large to fit within the tree"
            ),
        }
    }
}

impl From<Error> for BTreeError {
    fn from(value: Error) -> Self {
        match value {
            Error::DataCorruption => Self::DataCorruption { trace: Vec::new() },
            Error::OutofSpace(s) => Self::OutofSpace(s),
            Error::WriteTooLarge => Self::WriteTooLarge,
        }
    }
}

/// 4 kiB page. Standard on most architectures, particularly x64, RISC-V, and
/// non-Apple ARM.
pub const PAGE_4K: usize = 1 << 12;

/// 16 kiB page. Used primarily by MacOS, iOS, and (potentially) some
/// Android devices after 2024.
pub const PAGE_16K: usize = 1 << 14;
