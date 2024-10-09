use thiserror::Error;

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
    #[error("Data integrity hash failed for data at offset 0x{offset:x} with length {len}")]
    HashFailed { offset: usize, len: usize }
}
