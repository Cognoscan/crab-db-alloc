#[cfg(feature = "std")]
extern crate std;

#[derive(Debug)]
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
            #[cfg(feature = "std")]
            Self::OsIo(_) => write!(f, "OS I/O Error"),
        }
    }
}

impl core::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        #[cfg(feature = "std")]
        if let Self::OsIo(e) = self { return Some(e) };

        None
    }
}
