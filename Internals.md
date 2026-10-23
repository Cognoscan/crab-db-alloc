# `carb-db` Internals

- `crab-db`: The main database crate.
- `crab-dads`: The B-Tree map code.
- `crab-dads-traits`: Core traits backing storage must implement for the B-Tree 
  implementation in `crab-dads`.

The code has been split out to make it easier to both use and test the B-Tree 
implementation separately from the completed database.

## `crab-dads-traits`

Largely this crate is just to split the storage APIs required by `crab-dads` 
into its own crate, to potentially support other backing storage for a B-Tree in 
the future. It also includes an in-memory-only allocator to support testing of 
`crab-dads`.

## `crab-dads`

The B-Tree itself is page-oriented, and always uses 4-kiB pages. It is expected 
that the backing storage is capable of splitting up whatever actual page size is 
used by the system into 4-kiB pages as required.

The top level B-Tree code is under [`btree`](crab-dads/src/btree), and consists 
of a reader and writer. These fetch and manipulate the individual pages of the 
B-Tree.

The page portion of the B-Tree code is under the [`page`](crab-dads/src/page) 
module. The actual layout of the page involves two different resizable arrays, 
which are in the [`arrays`](crab-dads/src/arrays) module, and a page trailer, in 
[`trailer.rs`].
