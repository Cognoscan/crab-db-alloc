use crate::{error::FormatError, AllocError, BlockRange};

const TYPE_BRANCH: u8 = 1;
const TYPE_LEAF: u8 = 2;

pub(crate) struct Leaf {
    mem: &'static mut [u8],
}

#[repr(C)]
pub struct LeafHeader {
    pub page_type: u8,
    pub flags: u8,
    pub items: u16,
    pub key_size: u16,
    pub value_size: u16,
}

impl Leaf {
    /// Interpret a raw block of memory into a leaf page.
    ///
    /// # Safety
    ///
    /// The memory *must* be 4096 bytes in size, and this struct must be dropped before the backing
    /// memory is.
    pub unsafe fn new(mem: &'static mut [u8]) -> Self {
        Self { mem }
    }

    pub fn initialize(&mut self, key_size: u16, value_size: u16, complex: bool) {
        *self.header_mut() = LeafHeader {
            page_type: TYPE_LEAF,
            flags: if complex { 1 } else { 0 },
            items: 0,
            key_size,
            value_size,
        };
    }

    fn header(&self) -> &LeafHeader {
        unsafe { &*(self.mem.as_ptr() as *const LeafHeader) }
    }

    fn header_mut(&mut self) -> &mut LeafHeader {
        unsafe { &mut *(self.mem.as_mut_ptr() as *mut LeafHeader) }
    }

    fn try_insert(&mut self, key: &[u8], val: &[u8]) -> Result<(), AllocError> {
        todo!()
    }
}

enum Entry<'a> {
    Page(u64),
    Block(BlockRange),
    Value(&'a [u8]),
}
