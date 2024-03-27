use std::ops::{Deref, DerefMut};

pub trait OwnedBlockApi: Send + Sync {
    /// Mutably get the block
    fn block_mut(&mut self) -> &mut [u8];

    /// Get the block
    fn block(&self) -> &[u8];

    /// Commit the block back to the backing memory, giving the length of how
    /// much data to keep.
    fn commit(self, size: usize);

    /// Forget this allocation - don't commit anything to memory and yield all
    /// data back to the backing memory's allocator.
    fn forget(self);
}

pub struct OwnedBlock(Box<dyn OwnedBlockApi>);

impl OwnedBlock {
    /// Put the raw boxed API trait in this more convenient wrapper
    pub fn from_api(api: Box<dyn OwnedBlockApi>) -> Self {
        Self(api)
    }
}

impl Deref for OwnedBlock {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.0.block()
    }
}

impl DerefMut for OwnedBlock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.block_mut()
    }
}

impl std::fmt::Debug for OwnedBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.block().fmt(f)
    }
}
