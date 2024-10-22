/*!
Unsafe structures for handling arrays of values within a memory region.
 */
mod sized_array;
mod unsized_array;
mod rev_sized_array;
mod keyval_array;

pub use sized_array::SizedArray;
pub use unsized_array::UnsizedArray;
pub use rev_sized_array::{RevSizedArray, RevSizedArrayMut};
pub use keyval_array::{KeyValArray, KeyValArrayMut};