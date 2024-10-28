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
    #[error("Provided a page that wasn't 4kiB in size")]
    IncorrectPageSize,
}

/// 4 kiB page. Standard on most architectures, particularly x64, RISC-V, and
/// non-Apple ARM.
const PAGE_4K: usize = 1 << 12;
