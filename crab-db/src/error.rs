use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AllocError {
    /// Couldn't open the backing file
    #[error("Opening the backing file failed")]
    Open(#[source] std::io::Error),
    /// Couldn't parse the backing file
    #[error("Error inside the DB format")]
    DataFormat(#[source] FormatError),
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

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FormatError {
    #[error("Root pages have duplicate transaction IDs")]
    DuplicateIds,
    #[error("No root data page has a valid hash")]
    RootHash,
    #[error("File size is incorrect - too small or not a valid number of 1 MiB blocks")]
    FileSize,
    #[error("Invalid page type {0}")]
    PageType(u8),
    #[error("Invalid Leaf Page")]
    LeafPage,
    #[error("Invalid Branch Page")]
    BranchPage,
}
