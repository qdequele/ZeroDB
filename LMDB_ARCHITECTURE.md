# LMDB Architecture Review

This document provides a comprehensive review of LMDB (Lightning Memory-Mapped Database) internals for the zerodb pure-Rust port project.

## Table of Contents

1. [Overview](#overview)
2. [File Structure](#file-structure)
3. [Page Architecture](#page-architecture)
4. [B+Tree Structure](#btree-structure)
5. [Transaction System (MVCC)](#transaction-system-mvcc)
6. [Memory Mapping](#memory-mapping)
7. [Freelist Management](#freelist-management)
8. [Cursors](#cursors)
9. [Limits and Constants](#limits-and-constants)
10. [Configuration Options](#configuration-options)
11. [Error Codes](#error-codes)
12. [Locking and Concurrency](#locking-and-concurrency)

---

## Overview

LMDB is a B+tree-based key-value store that uses memory-mapped files and copy-on-write semantics for ACID transactions. Key characteristics:

- **Single-level store**: No separate log files, no compaction
- **Memory-mapped**: Data accessed directly via mmap
- **Copy-on-write**: MVCC without read locks
- **Single writer, multiple readers**: No read blocking
- **Fully serializable transactions**
- **Zero-copy reads**: Data returned directly from mmap

### Design Philosophy

1. **Simplicity**: Single file for data, minimal configuration
2. **Performance**: Memory-mapped access, no buffer cache management
3. **Reliability**: Atomic commits via meta page alternation
4. **Durability**: fsync guarantees on commit

---

## File Structure

### Data File (`data.mdb`)

```
+------------------+
|   Meta Page 0    |  Page 0
+------------------+
|   Meta Page 1    |  Page 1
+------------------+
|   Free List DB   |  Root stored in meta
+------------------+
|   Main DB Root   |  Root stored in meta
+------------------+
|   Named DBs...   |  Additional B+trees
+------------------+
|   Data Pages     |  Branch, Leaf, Overflow
+------------------+
|   Free Pages     |  Available for reuse
+------------------+
```

### Lock File (`lock.mdb`)

- Reader table: tracks active readers and their transaction IDs
- Mutex: coordinates writer access
- Fixed-size, created at environment open

### File Layout Details

| Offset | Content |
|--------|---------|
| 0 | Meta Page 0 |
| page_size | Meta Page 1 |
| 2 * page_size | First data page (usually freelist root) |

---

## Page Architecture

### Page Header

Every page starts with a header:

```rust
struct PageHeader {
    /// Page number (pgno)
    page_no: u64,        // 8 bytes - position in file
    /// Padding/reserved
    pad: u16,            // 2 bytes
    /// Page flags
    flags: u16,          // 2 bytes
    /// Lower bound of free space
    lower: u16,          // 2 bytes - pointer to end of entries
    /// Upper bound of free space
    upper: u16,          // 2 bytes - pointer to start of data
}
// Total: 16 bytes
```

### Page Flags

```rust
bitflags! {
    struct PageFlags: u16 {
        const BRANCH      = 0x01;  // Branch page
        const LEAF        = 0x02;  // Leaf page
        const OVERFLOW    = 0x04;  // Overflow page
        const META        = 0x08;  // Meta page
        const DIRTY       = 0x10;  // Page modified in txn
        const LEAF2       = 0x20;  // Leaf with DUPFIXED data
        const SUBP        = 0x40;  // Sub-page inside leaf
        const LOOSE       = 0x4000; // Page freed in txn
        const KEEP        = 0x8000; // Page kept after txn
    }
}
```

### Page Types

#### 1. Meta Page (0x08)

```rust
struct MetaPage {
    header: PageHeader,
    /// Magic number: 0xBEEFC0DE
    magic: u32,
    /// Format version (1)
    version: u32,
    /// Fixed address for FIXEDMAP
    address: *mut c_void,
    /// Size of mmap region
    map_size: usize,
    /// Info about freelist DB
    free_db: DbInfo,
    /// Info about main DB
    main_db: DbInfo,
    /// Last page used
    last_pgno: u64,
    /// Last txn ID
    last_txnid: u64,
}

struct DbInfo {
    /// Padding
    pad: u32,
    /// Database flags
    flags: u16,
    /// Depth of B+tree
    depth: u16,
    /// Number of branch pages
    branch_pages: u64,
    /// Number of leaf pages
    leaf_pages: u64,
    /// Number of overflow pages
    overflow_pages: u64,
    /// Number of entries
    entries: u64,
    /// Root page number
    root: u64,
}
```

#### 2. Branch Page (0x01)

Branch pages contain keys and child page pointers:

```
+------------------+
|   Page Header    |
+------------------+
|   Node Ptrs[]    |  Array of offsets to nodes
+------------------+
|   Free Space     |
+------------------+
|   Node Data      |  Keys and child page numbers
+------------------+
```

Branch Node structure:
```rust
struct BranchNode {
    /// Lower 32 bits of child page number
    lo: u32,
    /// Upper 16 bits of child page + key length
    hi_and_ksize: u16,  // (pgno_hi << 12) | ksize
    /// Key data follows (variable length)
    key: [u8],
}
```

#### 3. Leaf Page (0x02)

Leaf pages contain keys and values:

```rust
struct LeafNode {
    /// Low 32 bits of data size OR child page number
    lo: u32,
    /// High bits + key size: (size_hi << 12) | ksize
    hi_and_ksize: u16,
    /// Flags
    flags: u16,
    /// Key data (followed by value data)
    key_and_data: [u8],
}
```

Leaf Node Flags:
```rust
const F_BIGDATA: u16 = 0x01;   // Data stored on overflow page
const F_SUBDATA: u16 = 0x02;   // Data is a sub-database
const F_DUPDATA: u16 = 0x04;   // Data has duplicates (sub-page or sub-tree)
```

#### 4. Overflow Page (0x04)

For values larger than ~(page_size - header) / 4:

```rust
struct OverflowPage {
    header: PageHeader,
    /// Number of overflow pages
    pages: u32,
    /// Padding
    padding: [u8; 4],
    /// Data continues for `pages * page_size - header_size` bytes
    data: [u8],
}
```

#### 5. Sub-Page (0x40)

Inline sub-page for small duplicate sets:

```rust
struct SubPage {
    /// Number of entries
    num_entries: u16,
    /// Padding
    pad: u16,
    /// Lower bound
    lower: u16,
    /// Upper bound
    upper: u16,
    /// Node entries follow
    nodes: [Node],
}
```

### Page Size

- Default: OS page size (typically 4096 bytes)
- Must be power of 2
- Minimum: 512 bytes
- Maximum: 65536 bytes (32768 for WRITEMAP)

---

## B+Tree Structure

### Tree Properties

- **Order**: Variable (based on key sizes)
- **Fan-out**: Typically 100-500 keys per branch page
- **Height**: Usually 2-4 levels for millions of entries
- **Sorted**: Keys maintained in lexicographical order (or custom comparator)

### Node Layout Within Page

```
Page Layout:
+------------------------------------------+
| Header (16 bytes)                        |
+------------------------------------------+
| Node Pointers: [u16; num_keys]           |  Offsets from page start
+------------------------------------------+
|              Free Space                  |
+------------------------------------------+
| Node N data   (grows downward)           |
| ...                                      |
| Node 1 data                              |
| Node 0 data                              |
+------------------------------------------+
```

### Key-Value Storage

**Small values** (< node threshold):
```
[LeafNode header][key bytes][value bytes]
```

**Large values** (overflow):
```
[LeafNode header with F_BIGDATA][key bytes][overflow pgno: u64]
```

### Duplicate Keys (DUPSORT)

When `MDB_DUPSORT` is set:

1. **Few duplicates**: Values stored in sub-page inline
2. **Many duplicates**: Values stored in nested B+tree (sub-database)

```
Leaf node with duplicates:
[LeafNode with F_DUPDATA][key][SubPage or SubDB root]
```

---

## Transaction System (MVCC)

### Transaction Structure

```rust
struct Transaction {
    /// Transaction ID
    txnid: u64,
    /// Parent transaction (for nested)
    parent: Option<Box<Transaction>>,
    /// Environment handle
    env: *mut Env,
    /// Root page of free list
    free_root: u64,
    /// Root page of main DB
    main_root: u64,
    /// Array of database handles
    dbs: Vec<DbInfo>,
    /// Dirty pages list
    dirty_pages: Vec<DirtyPage>,
    /// Spill pages (written to disk mid-txn)
    spill_pages: Vec<u64>,
    /// Loose pages (freed in this txn)
    loose_pages: Vec<u64>,
    /// Flags (read-only, etc.)
    flags: u32,
}
```

### MVCC Copy-on-Write

1. **Read Transaction**:
   - Captures current meta page
   - Sees consistent snapshot
   - Never blocks, never blocked

2. **Write Transaction**:
   - Only one active at a time
   - Copies pages on modification
   - Tracks dirty pages
   - Commits atomically via meta page switch

### Commit Process

```
1. Allocate pages for dirty data
2. Write dirty pages to file
3. fsync() data pages (unless NO_SYNC)
4. Write new meta page (alternating 0/1)
5. fsync() meta page (unless NO_META_SYNC)
6. Update meta page in memory
7. Release writer lock
```

### Meta Page Alternation

```
Commit N:   Meta[0] active, txnid=N
Commit N+1: Meta[1] active, txnid=N+1
Commit N+2: Meta[0] active, txnid=N+2
...
```

On recovery, the meta page with the higher valid txnid is used.

### Nested Transactions

- Child inherits parent's view
- Child's dirty pages tracked separately
- On child commit: merge into parent
- On child abort: discard child's changes
- Max nesting depth: limited by stack/memory

---

## Memory Mapping

### Mapping Strategy

```rust
enum MappingMode {
    /// Read-only mmap, write via system calls
    ReadOnlyMap,
    /// Writable mmap (MDB_WRITEMAP)
    WriteMap,
}
```

### Read-Only Mapping (Default)

- File mapped read-only
- Write operations use pwrite() system calls
- Dirty pages allocated from heap
- Committed pages written to file, then visible via mmap

### Writable Mapping (WRITEMAP)

- File mapped read-write
- Direct modification of mapped memory
- Requires msync() for durability
- More efficient but less safe (corruption visible immediately)

### Map Size

- Determines maximum database size
- Must be set before opening (or use mdb_env_set_mapsize)
- Growing requires closing and reopening
- Readers must handle MAP_RESIZED

---

## Freelist Management

### Freelist Database

- Internal B+tree database
- Keys: transaction IDs
- Values: list of freed page numbers

### Page Lifecycle

```
Allocated → In Use → Freed → In Freelist → Reusable → Allocated
```

### Freelist Entry

```rust
struct FreelistEntry {
    /// Transaction ID when pages were freed
    txnid: u64,
    /// Page numbers that were freed
    pages: Vec<u64>,
}
```

### Page Reclamation

Pages become reusable when:
1. They were freed by a committed transaction
2. No active reader has a txnid ≤ the freeing txnid

### Freelist Operations

```rust
// On page free during write txn:
fn free_page(txn: &mut Txn, pgno: u64) {
    txn.loose_pages.push(pgno);
}

// On commit:
fn commit_freelist(txn: &mut Txn) {
    if !txn.loose_pages.is_empty() {
        freelist_db.put(txn.txnid, txn.loose_pages);
    }
}

// On allocating new page:
fn alloc_page(txn: &mut Txn) -> u64 {
    // First check freelist for reusable pages
    if let Some(pgno) = find_reusable_page(txn) {
        return pgno;
    }
    // Otherwise extend the file
    txn.last_pgno += 1;
    txn.last_pgno
}
```

---

## Cursors

### Cursor Structure

```rust
struct Cursor {
    /// Transaction this cursor belongs to
    txn: *mut Transaction,
    /// Database this cursor is on
    dbi: u32,
    /// Current position in tree (page stack)
    stack: [CursorPage; MAX_CURSOR_STACK],
    /// Current stack depth
    top: i32,
    /// Cursor flags
    flags: u32,
}

struct CursorPage {
    /// Page pointer
    page: *mut Page,
    /// Index of current node
    index: u16,
}

const MAX_CURSOR_STACK: usize = 32; // Max tree depth
```

### Cursor Operations

| Operation | Description |
|-----------|-------------|
| `FIRST` | Move to first entry |
| `LAST` | Move to last entry |
| `NEXT` | Move to next entry |
| `PREV` | Move to previous entry |
| `SET` | Position at exact key |
| `SET_RANGE` | Position at key or next greater |
| `GET_CURRENT` | Return current key/value |
| `GET_MULTIPLE` | Return multiple dup values (DUPFIXED) |
| `NEXT_DUP` | Next duplicate of current key |
| `PREV_DUP` | Previous duplicate of current key |
| `FIRST_DUP` | First duplicate of current key |
| `LAST_DUP` | Last duplicate of current key |
| `NEXT_NODUP` | Next key (skip duplicates) |
| `PREV_NODUP` | Previous key (skip duplicates) |

### Tree Traversal

```rust
fn cursor_set(cursor: &mut Cursor, key: &[u8]) -> Result<()> {
    cursor.top = 0;
    let mut page = cursor.txn.root_page();

    loop {
        let idx = binary_search(page, key);
        cursor.stack[cursor.top] = CursorPage { page, index: idx };

        if page.is_leaf() {
            break;
        }

        cursor.top += 1;
        page = get_child_page(page, idx);
    }

    Ok(())
}
```

---

## Limits and Constants

### Hard Limits

| Limit | Value | Notes |
|-------|-------|-------|
| Max key size | 511 bytes (default) | Can compile with larger |
| Max DBs | 32767 | Named databases per env |
| Max readers | 126 (default) | Configurable |
| Max page size | 65536 bytes | |
| Min page size | 512 bytes | |
| Max value size | < 4GB | Limited by overflow pages |
| Max txn dirty pages | ~0.1% of map size | TXN_FULL error |

### Key Size Calculation

Default maximum key size:
```rust
const MAX_KEY_SIZE: usize = 511;

// With DUPSORT, value size is also limited
// (treated as key in sub-database)
```

For larger keys (compile-time option):
```rust
// MDB_MAXKEYSIZE can be defined up to:
const MAX_POSSIBLE_KEY_SIZE: usize = 32767; // (page_size / 2) - overhead
```

### Database Capacity

```
Theoretical max entries ≈ (map_size / avg_entry_size)
Practical limit: billions of entries
```

---

## Configuration Options

### Environment Flags

```rust
bitflags! {
    pub struct EnvFlags: u32 {
        /// Fixed mmap address (experimental)
        const FIXED_MAP    = 0x01;
        /// No environment directory (single file)
        const NO_SUB_DIR   = 0x4000;
        /// Don't fsync after commit
        const NO_SYNC      = 0x10000;
        /// Read-only environment
        const READ_ONLY    = 0x20000;
        /// Don't fsync meta page after commit
        const NO_META_SYNC = 0x40000;
        /// Use writable mmap
        const WRITE_MAP    = 0x80000;
        /// Async msync with WRITE_MAP
        const MAP_ASYNC    = 0x100000;
        /// Disable thread-local storage
        const NO_TLS       = 0x200000;
        /// Don't use locks (caller manages)
        const NO_LOCK      = 0x400000;
        /// Disable read-ahead
        const NO_READ_AHEAD= 0x800000;
        /// Don't init malloc'd memory
        const NO_MEM_INIT  = 0x1000000;
        /// Open previous snapshot
        const PREV_SNAPSHOT= 0x2000000;
    }
}
```

### Database Flags

```rust
bitflags! {
    pub struct DatabaseFlags: u32 {
        /// Reverse string comparison
        const REVERSE_KEY  = 0x02;
        /// Allow duplicate keys
        const DUP_SORT     = 0x04;
        /// Integer keys (native byte order)
        const INTEGER_KEY  = 0x08;
        /// Fixed-size duplicate data
        const DUP_FIXED    = 0x10;
        /// Integer duplicate data
        const INTEGER_DUP  = 0x20;
        /// Reverse duplicate comparison
        const REVERSE_DUP  = 0x40;
        /// Create DB if not exists
        const CREATE       = 0x40000;
    }
}
```

### Put Flags

```rust
bitflags! {
    pub struct PutFlags: u32 {
        /// Don't write if key/data exists (DUPSORT)
        const NO_DUP_DATA  = 0x20;
        /// Don't overwrite existing key
        const NO_OVERWRITE = 0x10;
        /// Append to end (bulk load optimization)
        const APPEND       = 0x10000;
        /// Append dup to end
        const APPEND_DUP   = 0x40000;
        /// Reserve space, return pointer
        const RESERVE      = 0x10000;
        /// Store multiple values (DUPFIXED)
        const MULTIPLE     = 0x80000;
    }
}
```

---

## Error Codes

```rust
pub enum Error {
    /// Key/data pair already exists
    KeyExist,           // -30799
    /// Key/data pair not found
    NotFound,           // -30798
    /// Page not found (corruption)
    PageNotFound,       // -30797
    /// Page type mismatch (corruption)
    Corrupted,          // -30796
    /// Fatal environment error
    Panic,              // -30795
    /// Database version mismatch
    VersionMismatch,    // -30794
    /// File not valid LMDB format
    Invalid,            // -30793
    /// Map size reached
    MapFull,            // -30792
    /// Max databases reached
    DbsFull,            // -30791
    /// Max readers reached
    ReadersFull,        // -30790
    /// Too many TLS keys (Windows)
    TlsFull,            // -30789
    /// Too many dirty pages
    TxnFull,            // -30788
    /// Cursor stack overflow
    CursorFull,         // -30787
    /// Page full (internal)
    PageFull,           // -30786
    /// Map resized by another process
    MapResized,         // -30785
    /// Operation incompatible with DB
    Incompatible,       // -30784
    /// Invalid reader slot reuse
    BadRslot,           // -30783
    /// Transaction must abort
    BadTxn,             // -30782
    /// Invalid key/value size
    BadValSize,         // -30781
    /// Bad DBI handle
    BadDbi,             // -30780
    /// Unexpected error
    Problem,            // -30779
}
```

---

## Locking and Concurrency

### Reader Lock Table

```rust
struct ReaderTable {
    /// Number of reader slots
    num_readers: u32,
    /// Reader entries
    readers: [ReaderEntry; MAX_READERS],
}

struct ReaderEntry {
    /// Thread ID that owns this slot
    tid: u64,
    /// Process ID
    pid: u32,
    /// Transaction ID being read
    txnid: u64,
}
```

### Writer Mutex

- Only one writer at a time
- Uses file locking or platform mutex
- Writers block on acquiring mutex
- Writers never block readers

### Concurrency Model

```
                    ┌──────────────────────────────────────┐
                    │          LMDB Environment            │
                    │                                      │
  ┌─────────┐      │   ┌─────────────────────────────┐   │
  │ Reader 1│──────┼──►│  Snapshot (txnid=100)       │   │
  └─────────┘      │   └─────────────────────────────┘   │
                    │                                      │
  ┌─────────┐      │   ┌─────────────────────────────┐   │
  │ Reader 2│──────┼──►│  Snapshot (txnid=99)        │   │
  └─────────┘      │   └─────────────────────────────┘   │
                    │                                      │
  ┌─────────┐      │   ┌─────────────────────────────┐   │
  │ Writer  │──────┼──►│  Working Copy (txnid=101)   │──┼──► Commit
  └─────────┘      │   └─────────────────────────────┘   │
                    │                                      │
                    └──────────────────────────────────────┘
```

### Lock-Free Reading

1. Reader atomically reads meta page
2. Reader increments its slot's txnid
3. Reader accesses data via mmap
4. Writer checks all reader txnids before freeing pages

---

## Additional Implementation Notes

### Comparison Functions

Default comparison: memcmp (lexicographic)

Custom comparators:
```rust
type CompareFunc = fn(&[u8], &[u8]) -> Ordering;

// Built-in integer comparison for INTEGER_KEY
fn integer_compare(a: &[u8], b: &[u8]) -> Ordering {
    let a_int = native_int(a);
    let b_int = native_int(b);
    a_int.cmp(&b_int)
}
```

### Overflow Page Threshold

Value goes to overflow when:
```rust
fn needs_overflow(key_size: usize, data_size: usize, page_size: usize) -> bool {
    let node_size = LEAF_NODE_HEADER + key_size + data_size;
    let threshold = (page_size - PAGE_HEADER) / 4;
    node_size > threshold
}
```

### Page Split Strategy

When leaf page is full:
1. Find split point (middle by size)
2. Allocate new sibling page
3. Move half the entries
4. Insert separator key in parent
5. Recurse if parent full (may cause root split)

---

## References

- [LMDB Official Documentation](http://www.lmdb.tech/doc/)
- [LMDB Source Code](https://github.com/LMDB/lmdb)
- [Howard Chu's LMDB Talks](https://www.youtube.com/results?search_query=howard+chu+lmdb)
- [Paper: LMDB: Lightning Memory-Mapped Database](https://www.openldap.org/pub/hyc/lmdb.pdf)
