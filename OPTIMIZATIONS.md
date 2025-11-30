# ZeroDB Performance Optimizations

This document tracks all performance optimizations for ZeroDB, comparing against LMDB as the reference implementation.

## Current Performance Status

| Operation | ZeroDB | LMDB | Ratio | Status |
|-----------|--------|------|-------|--------|
| Sequential Writes (100) | ~480ms | ~468ms | 97% | Good |
| Point Lookup | ~150ns | ~130ns | 87% | Good |

## Implemented Optimizations

### 1. Single fsync per commit
- **Impact**: ~50% improvement
- **Description**: Combined data page writes and meta page write into single fsync
- **Before**: Two fsyncs (data pages, then meta page)
- **After**: Single fdatasync after all writes
- **File**: `src/env.rs` - `commit_txn()`

### 2. fdatasync instead of fsync
- **Impact**: ~10-20% improvement
- **Description**: Use `sync_data()` instead of `sync_all()` to skip metadata sync
- **File**: `src/mmap.rs` - `DataFile::sync_data()`

### 3. Batch I/O writes
- **Impact**: ~15% improvement
- **Description**: Batch all dirty page writes into single syscall sequence
- **File**: `src/mmap.rs` - `DataFile::write_batch()`

### 4. Page buffer pool
- **Impact**: ~5% improvement
- **Description**: Reuse page buffers across transactions to avoid heap allocations
- **Files**: `src/alloc.rs` - `PagePool`, `src/env.rs` - `get_page_buffer()`

### 5. Sorted freelist allocation
- **Impact**: Better cache locality
- **Description**: Allocate lowest page numbers first for sequential access patterns
- **File**: `src/alloc.rs` - `alloc_from_freelist()`

### 6. Removed redundant mmap copy
- **Impact**: ~50% improvement (was causing 2x regression)
- **Description**: After file write + fsync, mmap sees changes automatically
- **File**: `src/env.rs` - `commit_txn()`

### 7. WRITEMAP mode
- **Impact**: Reduces syscalls
- **Description**: Write directly to mmap instead of file I/O, use msync
- **File**: `src/env.rs` - `commit_txn()` WRITEMAP branch

---

## Pending Optimizations

### 8. Cursor page caching
- **Impact**: 20-30% read improvement
- **Status**: IMPLEMENTED
- **Description**: LRU cache for recently accessed pages in cursor
- **Implementation**: `PageCache` struct with configurable capacity (default 16 pages)
- **Files**: `src/btree/cursor.rs` - `PageCache`, `search_cached()`

### High Priority (Significant Impact Expected)

#### 9. Spill dirty pages to disk
- **Expected Impact**: Enables large transactions
- **Description**: When dirty page count exceeds threshold, write some to disk
- **LMDB**: `MDB_TXN_SPILLS` - spills oldest dirty pages
- **Files**: `src/txn.rs`, `src/env.rs`

#### 10. Nested transaction optimization
- **Expected Impact**: Better subtransaction performance
- **Description**: Share dirty pages between parent and child transactions
- **Files**: `src/txn.rs` - `RwTxn::nested()`

### Medium Priority

#### 11. Branch prediction hints
- **Expected Impact**: 5-10% improvement
- **Description**: Use `likely`/`unlikely` hints for hot paths
- **Implementation**:
  ```rust
  #[cold]
  fn handle_error() { ... }

  if unlikely(page.is_overflow()) { ... }
  ```
- **Files**: All hot paths

#### 12. Prefetch pages
- **Expected Impact**: 10-20% for sequential scans
- **Description**: Prefetch next pages during iteration
- **Implementation**:
  ```rust
  fn prefetch_page(pgno: PageNo) {
      #[cfg(target_arch = "x86_64")]
      unsafe {
          std::arch::x86_64::_mm_prefetch(ptr, _MM_HINT_T0);
      }
  }
  ```
- **Files**: `src/btree/cursor.rs`

#### 13. Inline small values in leaf nodes
- **Expected Impact**: 10-15% for small values
- **Description**: Store values < 64 bytes directly in leaf node instead of separate allocation
- **LMDB**: `F_DUPDATA` with inline data
- **Files**: `src/page/node.rs`, `src/btree/node.rs`

#### 14. Compact leaf node format
- **Expected Impact**: Better cache utilization
- **Description**: Pack keys and values more efficiently
- **Current**: Fixed-size slots
- **Optimal**: Variable-size with offset table
- **Files**: `src/page/node.rs`

#### 15. Reader table optimization
- **Expected Impact**: Faster read transaction creation
- **Description**: Use lock-free reader slots like LMDB
- **LMDB**: Shared memory reader table with atomic operations
- **Files**: `src/env.rs`, new `src/reader.rs`

### Low Priority (Minor Impact)

#### 16. Custom memory allocator
- **Expected Impact**: 5% improvement
- **Description**: Use arena allocator for transaction-local allocations
- **Files**: `src/alloc.rs`

#### 17. SIMD key comparison
- **Expected Impact**: 5-10% for large keys
- **Description**: Use SIMD for memcmp on keys > 16 bytes
- **Files**: `src/btree/search.rs`

#### 18. Lazy freelist loading
- **Expected Impact**: Faster environment open
- **Description**: Load freelist on-demand instead of at startup
- **Files**: `src/env.rs`, `src/alloc.rs`

#### 19. Copy-on-write page references
- **Expected Impact**: Reduced memory copies
- **Description**: Use `Cow<[u8]>` for page data
- **Files**: `src/txn.rs`, `src/btree/cursor.rs`

### 20. Mmap advice hints
- **Impact**: OS-level optimization
- **Status**: IMPLEMENTED
- **Description**: Use `madvise()` for access pattern hints
- **Implementation**: `MmapAdvice` enum with Normal/Sequential/Random/WillNeed/DontNeed
- **Files**: `src/mmap.rs` - `advise()`, `advise_range()`, `prefetch()`

---

## Platform-Specific Optimizations

### Linux

#### 21. io_uring for async I/O
- **Expected Impact**: 30-50% for batch writes
- **Description**: Use io_uring for batched async writes
- **Requires**: Linux 5.1+

#### 22. `O_DIRECT` mode
- **Expected Impact**: Bypass page cache for large DBs
- **Description**: Direct I/O to avoid double-buffering

### macOS

### 23. F_FULLFSYNC
- **Impact**: Correct durability on macOS
- **Status**: IMPLEMENTED
- **Description**: macOS `fsync()` only flushes to drive cache, not to platters
- **Implementation**: `fcntl(fd, F_FULLFSYNC)` in `sync()` and `sync_data()`
- **Files**: `src/mmap.rs` - `DataFile::sync()`, `DataFile::sync_data()`

### Windows

#### 24. Overlapped I/O
- **Expected Impact**: Better async write performance
- **Description**: Use Windows async I/O APIs

---

## Benchmarking Checklist

When implementing optimizations, measure:

- [ ] Sequential writes (100, 500, 1000 items)
- [ ] Random reads (1000, 10000 items)
- [ ] Sequential iteration (full table scan)
- [ ] Mixed workload (50% read, 25% write, 25% delete)
- [ ] Transaction overhead (empty commit)
- [ ] Large value handling (1KB, 10KB, 100KB values)
- [ ] Memory usage under load

## How to Run Benchmarks

```bash
# All comparison benchmarks
cargo bench --bench comparison

# Specific benchmark group
cargo bench --bench comparison -- "sequential_writes"
cargo bench --bench comparison -- "random_reads"
cargo bench --bench comparison -- "iteration"

# With specific count
cargo bench --bench comparison -- "sequential_writes/.*/100"
```

## References

- [LMDB Source Code](https://github.com/LMDB/lmdb)
- [LMDB Technical Paper](http://www.lmdb.tech/doc/)
- [Database Internals Book](https://www.databass.dev/)
- [Linux io_uring](https://kernel.dk/io_uring.pdf)
