# ZeroDB Action Plan

A systematic plan to build a pure-Rust port of LMDB with heed compatibility.

## Phase Overview

```
Phase 1: Core Data Structures     ████████░░░░░░░░░░░░
Phase 2: Storage Engine           ████████████░░░░░░░░
Phase 3: Transaction System       ████████████████░░░░
Phase 4: Cursor & Operations      ████████████████████
Phase 5: Testing & Benchmarking   ████████████████████
Phase 6: Heed Compatibility       ████████████████████
```

---

## Phase 1: Core Data Structures

**Goal**: Implement the fundamental on-disk data structures that LMDB uses.

### 1.1 Page Structures

```
src/
├── page/
│   ├── mod.rs
│   ├── header.rs      # PageHeader struct
│   ├── meta.rs        # MetaPage struct
│   ├── branch.rs      # BranchPage and BranchNode
│   ├── leaf.rs        # LeafPage and LeafNode
│   ├── overflow.rs    # OverflowPage struct
│   └── subpage.rs     # SubPage for inline duplicates
```

**Tasks**:
- [ ] Define `PageHeader` with correct byte layout (16 bytes)
- [ ] Define `PageFlags` bitflags matching LMDB exactly
- [ ] Implement `MetaPage` with magic number validation
- [ ] Implement `BranchNode` encoding/decoding
- [ ] Implement `LeafNode` encoding/decoding with all flag variants
- [ ] Implement `OverflowPage` for large values
- [ ] Add `SubPage` for inline duplicate storage
- [ ] Write serialization/deserialization using zerocopy or manual byte manipulation
- [ ] Unit tests for each structure's binary compatibility

**Key Constants**:
```rust
pub const PAGE_HEADER_SIZE: usize = 16;
pub const META_MAGIC: u32 = 0xBEEFC0DE;
pub const META_VERSION: u32 = 1;
pub const MAX_KEY_SIZE: usize = 511;
```

### 1.2 Database Info Structure

**Tasks**:
- [ ] Define `DbInfo` struct (pad, flags, depth, branch_pages, leaf_pages, overflow_pages, entries, root)
- [ ] Implement serialization matching LMDB layout exactly

### 1.3 Error Types

**Tasks**:
- [ ] Define error enum matching all LMDB error codes
- [ ] Implement `std::error::Error` and `Display`
- [ ] Add conversion from system errors

---

## Phase 2: Storage Engine

**Goal**: Implement file management, memory mapping, and page allocation.

### 2.1 Memory Mapping

```
src/
├── mmap/
│   ├── mod.rs
│   ├── mapping.rs     # Cross-platform mmap abstraction
│   └── file.rs        # File operations
```

**Tasks**:
- [ ] Create cross-platform mmap abstraction (use `memmap2` crate or manual)
- [ ] Implement read-only mapping for default mode
- [ ] Implement read-write mapping for WRITEMAP mode
- [ ] Handle map resizing
- [ ] Proper munmap on drop

### 2.2 Page Allocator

```
src/
├── alloc/
│   ├── mod.rs
│   ├── allocator.rs   # Page allocation logic
│   └── freelist.rs    # Freelist management
```

**Tasks**:
- [ ] Implement page allocation (extend file or reuse freed)
- [ ] Implement freelist database (internal B+tree)
- [ ] Track dirty pages during write transactions
- [ ] Handle loose pages (freed within current transaction)
- [ ] Implement spill mechanism for large transactions

### 2.3 Environment

```
src/
├── env/
│   ├── mod.rs
│   ├── environment.rs  # Main Env struct
│   └── options.rs      # EnvOpenOptions
```

**Tasks**:
- [ ] Implement `Env` struct with mmap handle, lock file, meta cache
- [ ] Implement `EnvOpenOptions` with all LMDB flags
- [ ] Create data file if not exists
- [ ] Create lock file
- [ ] Initialize meta pages on new database
- [ ] Validate existing database on open
- [ ] Implement `env_sync()` / `force_sync()`
- [ ] Implement `env_info()` and `env_stat()`

---

## Phase 3: Transaction System

**Goal**: Implement MVCC transactions with copy-on-write semantics.

### 3.1 Transaction Core

```
src/
├── txn/
│   ├── mod.rs
│   ├── read.rs        # Read-only transactions
│   ├── write.rs       # Read-write transactions
│   └── nested.rs      # Nested transaction support
```

**Tasks**:
- [ ] Implement `RoTxn` (read-only transaction)
  - [ ] Capture meta page snapshot
  - [ ] Register in reader table
  - [ ] Implement `abort()` / drop
- [ ] Implement `RwTxn` (read-write transaction)
  - [ ] Acquire writer lock
  - [ ] Copy pages on write
  - [ ] Track dirty pages
  - [ ] Implement `commit()`
  - [ ] Implement `abort()` / drop
- [ ] Implement nested transactions
  - [ ] Child dirty page tracking
  - [ ] Merge on child commit
  - [ ] Discard on child abort

### 3.2 Reader Table

**Tasks**:
- [ ] Implement reader slot management in lock file
- [ ] Thread ID tracking
- [ ] Find minimum active reader txnid
- [ ] Handle stale readers (process died)

### 3.3 Commit Protocol

**Tasks**:
- [ ] Implement page writing (dirty pages to file)
- [ ] Implement meta page alternation (0 ↔ 1)
- [ ] Implement fsync logic with NO_SYNC / NO_META_SYNC flags
- [ ] Atomic commit guarantee

---

## Phase 4: B+Tree & Cursor Operations

**Goal**: Implement the B+tree and all cursor operations.

### 4.1 B+Tree Core

```
src/
├── btree/
│   ├── mod.rs
│   ├── tree.rs        # B+tree operations
│   ├── search.rs      # Binary search in pages
│   ├── insert.rs      # Key/value insertion
│   ├── delete.rs      # Key/value deletion
│   ├── split.rs       # Page splitting
│   └── merge.rs       # Page merging (optional)
```

**Tasks**:
- [ ] Implement binary search within page
- [ ] Implement tree traversal (root to leaf)
- [ ] Implement page split algorithm
- [ ] Handle overflow pages for large values
- [ ] Support custom comparison functions

### 4.2 Cursor Implementation

```
src/
├── cursor/
│   ├── mod.rs
│   ├── cursor.rs      # Main cursor struct
│   ├── ops.rs         # Cursor operations
│   └── dup.rs         # Duplicate key handling
```

**Tasks**:
- [ ] Implement `Cursor` struct with page stack
- [ ] Implement all cursor operations:
  - [ ] `FIRST`, `LAST`
  - [ ] `NEXT`, `PREV`
  - [ ] `SET`, `SET_RANGE`
  - [ ] `GET_CURRENT`
  - [ ] `NEXT_DUP`, `PREV_DUP`
  - [ ] `FIRST_DUP`, `LAST_DUP`
  - [ ] `NEXT_NODUP`, `PREV_NODUP`
  - [ ] `GET_MULTIPLE` (for DUPFIXED)

### 4.3 Database Operations

```
src/
├── db/
│   ├── mod.rs
│   ├── database.rs    # Database handle
│   └── operations.rs  # get, put, del, etc.
```

**Tasks**:
- [ ] Implement `Database` struct (dbi handle)
- [ ] Implement `get(key)` - single value lookup
- [ ] Implement `put(key, value, flags)` - insert/update
- [ ] Implement `del(key, value)` - delete
- [ ] Implement `drop()` - delete entire database
- [ ] Support all put flags (NO_OVERWRITE, APPEND, etc.)

### 4.4 Duplicate Key Support (DUPSORT)

**Tasks**:
- [ ] Implement sub-page storage for small duplicate sets
- [ ] Implement sub-database storage for large duplicate sets
- [ ] Handle conversion between sub-page and sub-database
- [ ] Support DUPFIXED optimization

---

## Phase 5: Testing & Benchmarking

**Goal**: Ensure correctness and measure performance against LMDB.

### 5.1 Unit Tests

**Tasks**:
- [ ] Page structure serialization tests
- [ ] Binary compatibility tests with LMDB files
- [ ] B+tree operation tests
- [ ] Transaction isolation tests
- [ ] Crash recovery tests

### 5.2 Integration Tests

**Tasks**:
- [ ] Create test file with LMDB, read with zerodb
- [ ] Create test file with zerodb, read with LMDB (via heed)
- [ ] Concurrent reader tests
- [ ] Large value tests (overflow pages)
- [ ] Many duplicates tests

### 5.3 Property-Based Tests

**Tasks**:
- [ ] Random key/value insertion/deletion
- [ ] Verify tree invariants after each operation
- [ ] Test with various page sizes

### 5.4 Benchmarks

```
benches/
├── basic_ops.rs       # get, put, delete
├── iteration.rs       # cursor traversal
├── concurrent.rs      # multi-reader performance
└── large_values.rs    # overflow page performance
```

**Benchmark Scenarios**:
- [ ] Sequential key insertion
- [ ] Random key insertion
- [ ] Sequential reads
- [ ] Random reads
- [ ] Range scans
- [ ] Large value handling (1KB, 10KB, 100KB, 1MB)
- [ ] Multi-reader throughput

---

## Phase 6: Heed Compatibility

**Goal**: Create a heed-compatible interface.

### 6.1 Trait Implementation

**Tasks**:
- [ ] Study heed-traits crate
- [ ] Implement encoding/decoding traits
- [ ] Match heed's API signatures

### 6.2 API Wrapper

```
src/
├── heed_compat/
│   ├── mod.rs
│   ├── env.rs         # Env matching heed::Env
│   ├── txn.rs         # Txn matching heed::RoTxn/RwTxn
│   ├── database.rs    # Database matching heed::Database
│   └── cursor.rs      # Cursor types
```

**Tasks**:
- [ ] Create `Env` wrapper matching heed API
- [ ] Create `RoTxn`/`RwTxn` wrappers
- [ ] Create `Database<KC, DC>` generic wrapper
- [ ] Implement iterator types matching heed
- [ ] Support all heed features

---

## Implementation Order (Critical Path)

```
Week 1-2: Phase 1 (Data Structures)
    │
    ▼
Week 3-4: Phase 2 (Storage Engine)
    │
    ▼
Week 5-6: Phase 3 (Transactions)
    │
    ├─────────────────────────────┐
    ▼                             │
Week 7-8: Phase 4 (B+Tree)        │
    │                             │
    ▼                             │
Week 9: Basic End-to-End Test ◄───┘
    │
    ├── Simple put/get working
    ├── Small to medium values
    └── Single-threaded
    │
    ▼
Week 10-11: Phase 4 continued (Cursors, DUPSORT)
    │
    ▼
Week 12: Large Value Support (Overflow Pages)
    │
    ▼
Week 13-14: Phase 5 (Testing & Benchmarks)
    │
    ▼
Week 15-16: Phase 6 (Heed Compatibility)
```

---

## Minimum Viable Product (MVP)

The first testable version should support:

- [x] Read existing LMDB files (meta page parsing)
- [ ] Create new database files
- [ ] Single-threaded read/write transactions
- [ ] Basic put/get/delete operations
- [ ] Small values (no overflow)
- [ ] Single database per environment

---

## Technical Decisions

### Crate Dependencies

```toml
[dependencies]
# Memory mapping
memmap2 = "0.9"

# Bitflags for page/env/db flags
bitflags = "2.0"

# Cross-platform file locking
fs2 = "0.4"

# Optional: zero-copy serialization
zerocopy = { version = "0.7", optional = true }

# Page size detection
page_size = "0.6"

[dev-dependencies]
tempfile = "3.0"
criterion = "0.5"
proptest = "1.0"
```

### Unsafe Usage Policy

Minimize unsafe, isolate in dedicated modules:
- `mmap/` - Memory mapping operations
- `page/` - Raw byte access for page reading

### Error Handling

Use `thiserror` for error definitions, match LMDB error codes exactly for compatibility.

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Binary incompatibility | Extensive tests with real LMDB files |
| Performance gap | Profile early, optimize hot paths |
| Edge cases in B+tree | Property-based testing |
| Platform differences | CI testing on Linux, macOS, Windows |
| Memory safety | Minimize unsafe, use miri for testing |

---

## Success Criteria

1. **Correctness**: Pass all LMDB compatibility tests
2. **Performance**: Within 20% of LMDB for common operations
3. **Compatibility**: Drop-in replacement for heed backend
4. **Reliability**: No data corruption under stress tests
5. **Large Values**: Efficient handling of values up to 1GB
