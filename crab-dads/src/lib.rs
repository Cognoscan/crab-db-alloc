use thiserror::Error;

mod trailer;
pub mod arrays;
pub use trailer::*;

/// Error that can be returned while processing a data page
#[derive(Debug, Error, PartialEq, Eq)]
pub enum Error {
    #[error("No space left in page to insert")]
    OutofSpace,
    #[error("Data Corruption")]
    DataCorruption,
}

/// 4 kiB page. Standard on most architectures, particularly x64, RISC-V, and
/// non-Apple ARM.
pub const PAGE_4K: usize = 1 << 12;

/// 16 kiB page. Used primarily by MacOS, iOS, and (potentially) some
/// Android devices after 2024.
pub const PAGE_16K: usize = 1 << 14;
