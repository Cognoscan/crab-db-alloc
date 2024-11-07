/*!
Unsafe structures for handling arrays of values within a memory region.
 */
mod keyval_array;
mod rev_sized_array;

pub use keyval_array::{KeyValArray, KeyValArrayMut, KeyValArrayMutResize};
pub use rev_sized_array::{RevSizedArray, RevSizedArrayMut, RevSizedArrayMutResize};
