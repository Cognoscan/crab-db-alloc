# crab-db, a memory-mapped key-value database

This is another memory-mapped key-value database, in the same vein as 
[LMDB](https://www.symas.com/lmdb). The supported tree types are:

- `U64Set: u64 -> ()`
- `U64U64: u64 -> u64`
- `U64Tree: u64 -> subtree`
- `U64Bytes: u64 -> Vec<u8>`
- `BytesSet: Vec<u8> -> ()`
- `BytesU64: Vec<u8> -> u64`
- `BytesTree: Vec<u8> -> subtree`

## Features

- ACID key-value database
- Memory-mapped for efficient RAM usage
- Small codebase
- Nestable databases
- Parallel writing for large values
- Clean-sheet design in Rust

## What does it NOT do?

- No query engine
- No data serialization system
- No compression
- No encryption
- Doesn't support multiple writers for small values
- Doesn't support nested transactions
- Not write-optimized, but read-optimized

## What workloads is it good for?

If your workload primarily has write transactions of a megabyte or more, and
leans more towards frequent reads than it does frequenty writes, then crab-db
will probably work well for you. It'll work even better if your read/write
workload sequentially operates over keys. Massive write throughput is possible
if your system has some idea of the value size ahead of time, and can execute
many large writes in a single transaction.

If your workload is instead many small transactions, or is primarily
write-oriented (eg. for data logging), consider alternate databases,
particularly ones that incorporate a log-structured merge tree approach.

With the above said, though, write amplification is pretty hard to avoid with
modern storage anyway, so finding a way to restructure your workload to have
larger transaction sizes may be to your advantage regardless.

## Memory-Mapped

crab-db is memory-mapped, which means it stores everything as single flat file
that's loaded into program memory. The OS's page cache handles all caching,
which means it can be quite memory friendly and cooperative with other programs.
By deferring to the OS for cache management and loading, we don't need to guess
at what our own cache sizes should be, or try to infer them based on global RAM
usage.

If you're using crab-db in low-resource contexts, like a phone, this can be
quite helpful, as it runs no risk of blowing out RAM utilization.

## Small Codebase

There are many complex databases, with enormous codebases supporting incredible
performance across a vast range of use cases. That's not crab-db. The codebase
is relatively small, and only supports a basic ACID key-value store. For more
complex features like serialization, encryption, compression, and so on, look
for a database that builds on top of crab-db.

## Nestable Databases

Many key-value databases provide only a single global key-value store, or an
"environment" containing multiple key-value stores. crab-db goes a step
further and allows for arbitrary nesting of key-value stores, meaning a "value"
can be either a byte vector, or an entire sub-database.

Sub-databases mean that key "prefixes" can be split up, effectively supporting
things that would look like:

```Rust
BTreeMap<Vec<u8>, BTreeMap<Vec<u8>, BTreeMap<Vec<u8>>>
```

This may or may not be useful, depending on how many keys are under each prefix 
on average. crab-db always separates out sub-databases from their parent 
database, so small numbers of keys-value pairs in a sub-database (eg. under 4kiB 
of data) can be a waste of space.
