
mod reader;
mod writer;
pub use reader::*;
pub use writer::*;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct BlockRange {
    pub start: u64,
    pub len: usize,
}

impl BlockRange {
    pub fn new(start: u64, len: usize) -> Self {
        Self { start, len }
    }
}

#[derive(Debug)]
pub struct WritableBlock<'a> {
    pub page: u64,
    pub block: &'a mut [u8]
}