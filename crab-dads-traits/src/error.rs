#[cfg(feature = "std")]
extern crate std;

/// A storage system error.
///
/// Most errors indicate the storage system cannot be used and are not
/// recoverable. The exceptions are [`Corruption`][corrupt] and
/// [`TooLarge`][large].
/// 
/// [`Corruption`][corrupt] indicates the storage is corrupted, rather than it
/// being operated incorrectly. To a user, this can indicate the problem lies
/// with the backing storage (eg. a file, a hard drive, etc.) instead of the
/// program.
/// 
/// [`TooLarge`][large] indicates a requested allocation size was too large. A
/// program using the storage system may use this to split its requested
/// allocation into a series of smaller allocations, instead of failing.
/// 
/// [corrupt]: StorageError::Corruption
/// [large]: StorageError::Corruption
#[derive(Debug)]
#[non_exhaustive]
pub enum StorageError {
    /// I/O error in storage system.
    Io(&'static str),
    /// Database corruption detected.
    Corruption(&'static str),
    /// Rust memory safety violation detected.
    Safety(&'static str),
    /// Out of range load request was made.
    OutOfRange(u64),
    /// Requested number of pages in an allocation was too large.
    TooLarge { req: usize, max: usize },
    /// Storage-system catch-all error
    Other(&'static str),
    /// I/O error from the OS.
    #[cfg(feature = "std")]
    OsIo(std::io::Error),
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
            Self::TooLarge { req, max } => write!(
                f,
                "Requested number of pages ({req}) is larger than maximum supported ({max}"
            ),
            Self::Other(s) => write!(f, "Other error: {}", s),
            #[cfg(feature = "std")]
            Self::OsIo(_) => write!(f, "OS I/O Error"),
        }
    }
}

impl core::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        #[cfg(feature = "std")]
        if let Self::OsIo(e) = self {
            return Some(e);
        };

        None
    }
}
