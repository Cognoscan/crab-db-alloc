/*!
Unsafe structures for handling arrays of values within a memory region.
 */
mod rev_sized_array;
mod keyval_array;

pub use rev_sized_array::{RevSizedArray, RevSizedArrayMut};
pub use keyval_array::{KeyValArray, KeyValArrayMut, KeyValEntry};