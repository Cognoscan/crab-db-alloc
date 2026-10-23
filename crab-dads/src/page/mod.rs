/*!
Individual Page maps, used to construct a full B-Tree.

# Layout

A page is between 4 & 64 kiB in size, containing two arrays and some page-level
information in a trailer. The page-level information is in a 8-byte struct at
the end of the page, reproduced below. One array grows up from the bottom (eg.
offset 0 towards max) and can have variable-length content. The other array
grows from the top downwards (eg. offset max down towards offset 0) and stores
fixed-length content only. The lengths (in bytes) of these arrays are stored in
the trailer.

```Rust
#[repr(C)]
pub struct TwoArrayTrailer {
    /// Optional checksum
    checksum: u64,
    /// TXN counter
    txn: u64,
    /// lower array length (grows up from start of the page)
    lower_len: u16,
    /// upper array length (grows down from end, minus this trailer)
    upper_len: u16,
    unused0: u16,
    unused1: u8,
    /// The page type identifier
    pub page_type: u8,
}
```

Bit 0 of page type identifier is used by B-Tree implementations to distinguish
between branch nodes (bit is cleared) and leaf nodes (bit is set).

The lower array is guaranteed to start out 4kiB-aligned, with individual alignment depd

The actual content of the arrays is dependent on the [`PageLayout`], which must
be determined from the page type byte in the trailer. The lower array is
guaranteed to start out 4kiB-aligned, and any alignment of individual elements
is determined by the page layout. The upper array is guaranteed to start out
8-byte aligned, though alignment of individual elements may be less than this
depending on page layout.

The pre-defined layouts are:

- [`LayoutU64U64`] - `u64` keys are stored in the upper array, `u64` values are
  stored in the lower array.
- [`LayoutU64Var`] - `u16` lengths are stored in the upper array, specifying the
  length of the value. The lower array stores `u64` keys followed by
  variable-byte-length values. While the value can be any byte length, it is
  always stored in memory to be 8-byte-aligned, adding padding bytes to the
  end as needed.
- [`LayoutVarU64`] - `u16` lengths are stored in the upper array, specifying the
  length of the key. The lower array stores variable-length keys followed by
  `u64` values. While the key can be any byte length, it is always stored in
  memory to be 8-byte-aligned, adding padding bytes to the end as needed.

These layouts are sufficient to create 3 kinds of B-Trees:

| Key    | Value  | Branch Layout    | Leaf Layout      |
| --     | --     | --               | --               |
| `u64`  | `u64`  | [`LayoutU64U64`] | [`LayoutU64U64`] |
| `u64`  | `[u8]` | [`LayoutU64U64`] | [`LayoutU64Var`] |
| `[u8]` | `u64`  | [`LayoutVarU64`] | [`LayoutVarU64`] |

Other kinds are possible, but these are some of the most directly useful for
building databases.

 */

mod page_map;
mod page_map_mut;
mod traits;
mod u64_u64;
mod u64_var;
mod var_u64;
use crab_dads_traits::Region;
pub use page_map::*;
pub use page_map_mut::*;
pub use traits::*;
pub use u64_u64::*;
pub use u64_var::*;
pub use var_u64::*;

const TRAILER_SIZE: usize = core::mem::size_of::<TwoArrayTrailer>();

/// The maximum allowed variable-length size, assuming either [`LayoutU64Var`]
/// or [`LayoutVarU64`].
pub const MAX_VAR_SIZE: usize = 1008;

use crate::{Error, TwoArrayTrailer};

/// Get a page's trailer.
pub fn trailer(page: &Region) -> &TwoArrayTrailer {
    let page: &[u8] = page.as_ref();
    assert!(page.len() > TRAILER_SIZE, "Page is much too small");
    unsafe { &*(page.as_ptr().byte_add(page.len() - TRAILER_SIZE) as *const TwoArrayTrailer) }
}

/// Get a page's type byte.
pub fn page_type(page: &Region) -> u8 {
    let trailer = trailer(page);
    trailer.page_type
}

/// Copy all occupied space from one page to a new page.
pub fn copy_page(src: &Region, dst: &mut Region) -> Result<(), Error> {
    if dst.len() < src.len() {
        return Err(Error::InvalidAllocation);
    }
    // Figure out how large the data is and calculate the upper offset
    let trailer = trailer(src);
    let lengths = trailer.lengths(src.len() - TRAILER_SIZE)?;
    let upper_len = lengths.upper_bytes() + core::mem::size_of::<TwoArrayTrailer>();
    let upper_offset = src.len() - upper_len;
    let new_upper_offset = dst.len() - upper_len;

    // Perform the two copies - lower_data and upper_data + trailer
    unsafe {
        core::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), lengths.lower_bytes());
        core::ptr::copy_nonoverlapping(
            src.as_ptr().add(upper_offset),
            dst.as_mut_ptr().add(new_upper_offset),
            upper_len,
        );
    }
    Ok(())
}
