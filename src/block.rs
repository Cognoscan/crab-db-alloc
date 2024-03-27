use std::{ops::Deref, sync::Arc};

/// An API for interfacing with a memory-backed block of immutable bytes.
pub trait BlockApi: Send + Sync {
    /// Clone the reference to this block, without copying data
    fn clone(&self) -> Box<dyn BlockApi>;

    /// Get the block
    fn block(&self) -> &[u8];
}

impl Deref for dyn BlockApi {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.block()
    }
}

/// An immutable block of bytes, from a larger backing memory.
pub struct Block(Box<dyn BlockApi>);

impl Block {
    /// Put the raw boxed API trait in this more convenient wrapper
    pub fn from_api(api: Box<dyn BlockApi>) -> Self {
        Self(api)
    }
}

impl Deref for Block {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl Clone for Block {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.block().fmt(f)
    }
}

impl BlockApi for Arc<[u8]> {
    fn block(&self) -> &[u8] {
        self.deref()
    }

    fn clone(&self) -> Box<dyn BlockApi> {
        Box::new(std::clone::Clone::clone(self))
    }
}

impl From<Box<[u8]>> for Block {
    fn from(value: Box<[u8]>) -> Self {
        let api: Arc<[u8]> = value.into();
        Self(Box::new(api))
    }
}

impl From<Arc<[u8]>> for Block {
    fn from(value: Arc<[u8]>) -> Self {
        Self(Box::new(value))
    }
}

impl From<Vec<u8>> for Block {
    fn from(value: Vec<u8>) -> Self {
        value.into_boxed_slice().into()
    }
}

impl BlockApi for &'static [u8] {
    fn block(&self) -> &[u8] {
        self
    }

    fn clone(&self) -> Box<dyn BlockApi> {
        Box::new(*self)
    }
}

impl From<&'static [u8]> for Block {
    fn from(value: &'static [u8]) -> Self {
        Self(Box::new(value))
    }
}