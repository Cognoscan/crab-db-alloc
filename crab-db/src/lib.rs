use bytemuck::AnyBitPattern;
use crab_dads_traits::{StorageError, StorageWriter, WriteTxn};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CrabError {
    #[error("Crab-DB corruption: {0}")]
    Corruption(&'static str),
    #[error("Storage error")]
    Storage(#[from] StorageError)
}

#[repr(C)]
#[derive(Clone, Copy, AnyBitPattern)]
struct RootData {
    root_page: U64,
}

impl RootData {
    const SIZE: usize = core::mem::size_of::<Self>();

    fn read(data: &[u64]) -> Result<Self, CrabError> {
        let Ok(val) = bytemuck::try_from_bytes::<Self>(bytemuck::cast_slice::<u64,u8>(data)) else {
            return Err(CrabError::Corruption("Root data struct is corrupt"));
        };
        let this = *val;
        Ok(this)
    }

    fn write(&self) -> &[u64] {
        bytemuck::bytes_of(self)
    }
}

pub struct CrabDbWriter<W>
where
    W: StorageWriter + Send + Sync,
{
    inner: W,
    id: u64,
}

impl<W> CrabDbWriter<W>
where
    W: StorageWriter + Send + Sync,
{
    pub fn write(&mut self) {}
}

pub struct CrabDbWrite<W>
where
    W: WriteTxn + Send + Sync,
{
    inner: W,
    id: u64,
    root_data: RootData,
}

impl<W> CrabDbWrite<W> where W: WriteTxn + Send + Sync {

    pub fn commit(self) -> Result<(), StorageError> {
        self.inner.commit(&[])
    }
}
