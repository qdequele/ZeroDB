# INTERNAL.md - ZeroDB Internal Architecture

This document provides a comprehensive overview of ZeroDB's internal architecture, design decisions, and implementation details. It serves as a complete reference for understanding how the library works internally.

## Table of Contents

1. [Project Overview](#project-overview)
2. [Core Architecture](#core-architecture)
3. [Data Structures](#data-structures)
4. [Storage Layer](#storage-layer)
5. [Transaction System](#transaction-system)
6. [B+Tree Implementation](#btree-implementation)
7. [Memory Management](#memory-management)
8. [Performance Optimizations](#performance-optimizations)
9. [Concurrency Model](#concurrency-model)
10. [Error Handling](#error-handling)
11. [Testing Strategy](#testing-strategy)
12. [Performance Benchmarks](#performance-benchmarks)
13. [Known Limitations](#known-limitations)
14. [Future Improvements](#future-improvements)

## Project Overview

ZeroDB is a pure Rust implementation of LMDB (Lightning Memory-Mapped Database) with modern performance optimizations. It provides:

- **ACID Transactions**: Full transactional semantics with MVCC
- **Type Safety**: Compile-time type checking for keys and values
- **High Performance**: Memory-mapped I/O with zero-copy operations
- **LMDB Compatibility**: API compatibility with LMDB for easy migration
- **Modern Optimizations**: SIMD, NUMA awareness, adaptive page sizing

### Key Design Principles

1. **Zero-Copy Operations**: Minimize data copying through memory mapping
2. **Copy-on-Write**: Ensure transaction isolation without blocking readers
3. **Page-Based Storage**: All data organized in fixed-size pages (4KB default)
4. **Type Safety**: Leverage Rust's type system for compile-time guarantees
5. **Performance First**: Optimize for common workloads (sequential writes, random reads)

## Core Architecture

### Module Organization

```
src/
├── lib.rs              # Public API and re-exports
├── env.rs              # Environment management (type-state pattern)
├── txn.rs              # Transaction system (MVCC)
├── db.rs               # Database abstraction layer
├── btree.rs            # B+Tree implementation (core data structure)
├── page.rs             # Page management and structure
├── io.rs               # I/O backend (memory-mapped files)
├── error.rs            # Error types and handling
├── meta.rs             # Metadata and database info
├── catalog.rs          # Database catalog management
├── cursor.rs           # Cursor-based iteration
├── freelist.rs         # Free page management
├── segregated_freelist.rs # Size-segregated allocation
├── overflow.rs         # Large value storage
├── reader.rs           # Reader tracking for MVCC
├── checksum.rs         # Data integrity checks
├── comparator.rs       # Key comparison logic
├── simd.rs             # SIMD optimizations
├── numa.rs             # NUMA-aware allocation
├── batch_commit.rs     # Batch write operations
├── bloom_filter.rs     # Bloom filter for lookups
├── adaptive_page.rs    # Dynamic page sizing
└── [other modules]     # Supporting functionality
```

### High-Level Data Flow

```
User Request → Database API → Transaction → B+Tree → Page Management → I/O Layer
     ↑                                                                    ↓
     └─────────────── Response ←─── Copy-on-Write ←─── Memory Mapping ←──┘
```

## Data Structures

### Page Structure

The fundamental unit of storage is a 4KB page with the following layout:

```rust
#[repr(C)]
pub struct Page {
    pub header: PageHeader,           // 16 bytes
    pub data: [u8; PAGE_DATA_SIZE],   // 4080 bytes (4096 - 16)
}

#[repr(C)]
pub struct PageHeader {
    pub pgno: u64,        // Page number
    pub flags: PageFlags, // Page type and state
    pub num_keys: u16,    // Number of keys in page
    pub lower: u16,       // Lower bound of free space
    pub upper: u16,       // Upper bound of free space
    pub overflow: u64,    // Overflow page reference
    pub next_pgno: u64,   // Next page in chain
    pub prev_pgno: u64,   // Previous page in chain
    pub checksum: u32,    // Data integrity check
}
```

### B+Tree Node Structure

Each page contains B+Tree nodes with the following layout:

```rust
#[repr(C)]
pub struct NodeHeader {
    pub flags: NodeFlags, // Node type and flags
    pub ksize: u16,       // Key size
    pub lo: u16,          // Value size (lower 16 bits)
    pub hi: u16,          // Value size (upper 16 bits)
}

// Node layout: [NodeHeader][Key][Value/PageRef]
```

### Page Types

- **Branch Pages**: Internal B+Tree nodes containing key/page pairs
- **Leaf Pages**: Data pages containing key/value pairs
- **Overflow Pages**: Storage for values larger than page capacity
- **Meta Pages**: Database metadata and configuration
- **Free Pages**: Unused pages tracked by freelist

## Storage Layer

### Memory-Mapped I/O

ZeroDB uses memory-mapped files for high-performance I/O:

```rust
pub struct MmapBackend {
    file: File,                    // Underlying file
    mmap: Arc<Mutex<MmapMut>>,    // Memory-mapped region
    file_size: AtomicU64,         // Current file size
    page_size: usize,             // Page size (4KB)
}
```

**Advantages:**
- Zero-copy reads from disk
- OS-level caching and prefetching
- Efficient random access
- Automatic page management

**Limitations:**
- File size must be pre-allocated
- Memory pressure can cause page eviction
- Not optimal for network storage (EBS)

### I/O Backend Abstraction

The `IoBackend` trait allows for different storage implementations:

```rust
pub trait IoBackend: Send + Sync {
    fn read_page(&self, page_id: PageId) -> Result<Box<Page>>;
    fn write_page(&self, page: &Page) -> Result<()>;
    fn sync(&self) -> Result<()>;
    fn size_in_pages(&self) -> u64;
    fn grow(&self, new_size: u64) -> Result<()>;
}
```

**Current Implementation:**
- `MmapBackend`: Memory-mapped files (default)
- Future: Direct I/O, io_uring, async I/O for cloud storage

## Transaction System

### MVCC (Multi-Version Concurrency Control)

ZeroDB implements MVCC for transaction isolation:

```rust
pub struct Transaction<'env, M: mode::Mode> {
    data: TxnData<'env>,           // Shared transaction data
    mode_data: ModeData<'env>,     // Mode-specific data
}

pub enum ModeData<'env> {
    Read { reader_slot: Option<usize> },
    Write {
        _write_guard: MutexGuard<'env, ()>,
        dirty: Box<DirtyPages>,
        freelist: FreeList,
        next_pgno: PageId,
    },
}
```

### Transaction Types

1. **Read Transactions**:
   - Snapshot isolation
   - No blocking of other transactions
   - Can run concurrently with other readers
   - See consistent state at transaction start

2. **Write Transactions**:
   - Exclusive access (single writer at a time)
   - Copy-on-write for isolation
   - Atomic commit or rollback
   - Block other writers but not readers

### Copy-on-Write Implementation

```rust
impl<'env> Transaction<'env, Write> {
    pub fn get_page_cow(&mut self, page_id: PageId) -> Result<(PageId, &mut Page)> {
        // 1. Check if page is already dirty
        if let Some(page) = self.dirty.get(&page_id) {
            return Ok((page_id, page));
        }
        
        // 2. Copy original page
        let original = self.get_page(page_id)?;
        let new_page_id = self.alloc_page(original.header.flags)?;
        
        // 3. Copy data and mark as dirty
        let mut new_page = self.get_page_mut(new_page_id)?;
        *new_page = original.clone();
        new_page.header.pgno = new_page_id.0;
        
        // 4. Track in dirty pages
        self.dirty.mark_dirty(new_page_id, Box::new(new_page.clone()));
        
        Ok((new_page_id, self.dirty.get_mut(&new_page_id).unwrap()))
    }
}
```

### Reader Tracking

```rust
pub struct ReaderTable {
    readers: Vec<AtomicU64>,       // Transaction IDs of active readers
    oldest_reader: AtomicU64,      // Oldest active reader ID
}
```

**Purpose:**
- Track active read transactions
- Determine when pages can be freed
- Ensure no reader sees freed pages

## B+Tree Implementation

### Core B+Tree Structure

```rust
pub struct BTree<C = LexicographicComparator> {
    _phantom: PhantomData<C>,
}

impl<C: Comparator> BTree<C> {
    pub fn insert(
        txn: &mut Transaction<'_, Write>,
        root: &mut PageId,
        db_info: &mut DbInfo,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        // 1. Find insertion point
        // 2. Insert into leaf page
        // 3. Handle page splits if needed
        // 4. Update tree structure
    }
}
```

### Page Splitting

When a page becomes full, it's split into two pages:

```rust
fn split_leaf_page(
    txn: &mut Transaction<'_, Write>,
    page_id: PageId,
    new_key: &[u8],
    new_value: &[u8],
) -> Result<SplitResult> {
    // 1. Allocate new page
    // 2. Redistribute keys between pages
    // 3. Insert new key/value
    // 4. Update parent with separator key
    // 5. Handle parent splits recursively
}
```

### Key Features

1. **Sorted Storage**: Keys are always sorted within pages
2. **Variable Key/Value Sizes**: Efficient storage of different sizes
3. **Overflow Support**: Large values stored in separate pages
4. **Duplicate Handling**: Support for duplicate keys with dupsort
5. **Cursor Navigation**: Efficient forward/backward iteration

## Memory Management

### Free Page Management

Two freelist implementations:

1. **Simple Freelist** (`freelist.rs`):
   ```rust
   pub struct FreeList {
       free_pages: Vec<PageId>,
       pending_free: Vec<PageId>,
       oldest_reader: TransactionId,
   }
   ```

2. **Segregated Freelist** (`segregated_freelist.rs`):
   ```rust
   pub struct SegregatedFreeList {
       size_classes: Vec<Vec<PageId>>,
       pending_free: HashMap<usize, Vec<PageId>>,
       oldest_reader: TransactionId,
   }
   ```

**Benefits of Segregated Freelist:**
- Better allocation performance
- Reduced fragmentation
- Size-specific optimization

### Page Allocation

```rust
impl<'env> Transaction<'env, Write> {
    pub fn alloc_page(&mut self, flags: PageFlags) -> Result<(PageId, &mut Page)> {
        // 1. Try to reuse from freelist
        if let Some(page_id) = self.freelist.alloc_page() {
            return self.get_page_mut(page_id);
        }
        
        // 2. Allocate new page from end of file
        let page_id = self.next_pgno;
        self.next_pgno = PageId(page_id.0 + 1);
        
        // 3. Grow file if needed
        self.grow_if_needed(page_id)?;
        
        // 4. Initialize new page
        let page = self.get_page_mut(page_id)?;
        page.header = PageHeader::new(page_id.0, flags);
        
        Ok((page_id, page))
    }
}
```

### Overflow Page Management

Large values are stored in chains of overflow pages:

```rust
pub struct OverflowHeader {
    pub next_page: u64,    // Next overflow page (0 if last)
    pub total_size: u64,   // Total value size (first page only)
}

pub fn write_overflow_value(
    txn: &mut Transaction<'_, Write>,
    value: &[u8],
) -> Result<PageId> {
    // 1. Calculate number of pages needed
    // 2. Allocate overflow pages
    // 3. Write data with headers
    // 4. Chain pages together
    // 5. Return first page ID
}
```

## Performance Optimizations

### SIMD Optimizations

```rust
#[cfg(feature = "simd")]
pub mod simd {
    use std::simd::*;
    
    pub fn memcmp_simd(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        // Use SIMD for fast byte comparison
    }
    
    pub fn find_zero_simd(data: &[u8]) -> Option<usize> {
        // Use SIMD to find null bytes
    }
}
```

### NUMA-Aware Allocation

```rust
pub struct NumaPageAllocator {
    topology: Arc<NumaTopology>,
    node_pools: RwLock<HashMap<NumaNode, Vec<Box<Page>>>>,
    max_pages_per_node: usize,
}

impl NumaPageAllocator {
    pub fn alloc_page(
        &self,
        page_id: PageId,
        flags: PageFlags,
        preferred_node: Option<NumaNode>,
    ) -> Box<Page> {
        // Allocate on preferred NUMA node for better performance
    }
}
```

### Adaptive Page Sizing

```rust
pub struct AdaptivePageManager {
    current_size: AtomicUsize,
    pattern_detector: AccessPatternDetector,
    value_size_tracker: ValueSizeTracker,
    perf_monitor: PerformanceMonitor,
}

impl AdaptivePageManager {
    pub fn adapt(&self) -> Option<PageSize> {
        // Dynamically adjust page size based on workload
    }
}
```

### Bloom Filters

```rust
pub struct BloomFilter {
    bits: Vec<u64>,
    hash_functions: Vec<u64>,
    size: usize,
}

impl BloomFilter {
    pub fn might_contain(&self, key: &[u8]) -> bool {
        // Fast probabilistic membership test
    }
}
```

## Concurrency Model

### Reader-Writer Lock Pattern

```rust
pub struct EnvInner {
    write_lock: Mutex<()>,         // Exclusive write access
    readers: ReaderTable,          // Concurrent read access
    txn_id: AtomicU64,            // Global transaction counter
}
```

**Concurrency Rules:**
1. Multiple readers can access simultaneously
2. Only one writer at a time
3. Writers block other writers but not readers
4. Readers see consistent snapshots

### Transaction Isolation Levels

- **Snapshot Isolation**: Each transaction sees a consistent snapshot
- **Serializable**: Achieved through write serialization
- **Durability**: Configurable through durability modes

### Durability Modes

```rust
pub enum DurabilityMode {
    NoSync,        // No durability guarantees
    AsyncFlush,    // OS-managed flushing
    SyncData,      // Sync data pages only
    FullSync,      // Full ACID compliance
}
```

## Error Handling

### Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(String),
    
    #[error("Key not found")]
    KeyNotFound,
    
    #[error("Page not found: {0}")]
    PageNotFound(PageId),
    
    #[error("Database corruption: {details}")]
    Corruption { details: String, page_id: Option<PageId> },
    
    #[error("Map full")]
    MapFull,
    
    #[error("Readers full")]
    ReadersFull,
    
    #[error("Transaction full: {size} pages")]
    TxnFull { size: usize },
    
    #[error("Invalid parameter: {0}")]
    InvalidParameter(&'static str),
    
    #[error("Custom error: {0}")]
    Custom(String),
}
```

### Error Recovery

1. **Corruption Detection**: Checksums on pages and metadata
2. **Transaction Rollback**: Automatic rollback on error
3. **Reader Recovery**: Graceful handling of reader failures
4. **Page Validation**: Validation of page headers and data

## Testing Strategy

### Test Categories

1. **Unit Tests**: Module-level functionality
2. **Integration Tests**: End-to-end database operations
3. **Property Tests**: QuickCheck-based property verification
4. **Stress Tests**: High-load and edge case testing
5. **Performance Tests**: Benchmarking and regression testing

### Test Coverage

```rust
// Example integration test
#[test]
fn test_concurrent_readers() -> Result<()> {
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    let db = env.create_database::<String, String>(&mut txn, None)?;
    
    // Spawn multiple reader threads
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let env = env.clone();
            thread::spawn(move || {
                let txn = env.read_txn()?;
                // Perform reads
                Ok(())
            })
        })
        .collect();
    
    // Verify all readers complete successfully
    for handle in handles {
        handle.join().unwrap()?;
    }
    
    Ok(())
}
```

### LMDB Compatibility Tests

```rust
#[test]
fn test_lmdb_compatibility() {
    // Compare ZeroDB behavior with LMDB
    // Ensure API compatibility
    // Verify same results for same operations
}
```

## Performance Benchmarks

### Benchmark Categories

1. **Database Comparison**: ZeroDB vs LMDB, RocksDB, redb, sled
2. **ZeroDB Performance**: Internal performance analysis
3. **Concurrency Tests**: Multi-threaded performance
4. **Memory Usage**: Memory efficiency analysis

### Key Metrics

- **Throughput**: Operations per second
- **Latency**: Response time percentiles
- **Memory Usage**: Peak and steady-state memory
- **Scalability**: Performance with increasing data size
- **Concurrency**: Performance with multiple threads

### Benchmark Results

```bash
# Run full benchmark suite
cargo bench

# Run specific benchmarks
cargo bench --bench database_comparison
cargo bench --bench zerodb_performance

# Generate performance reports
python3 scripts/generate-perf-dashboard.py
```

## Known Limitations

### Current Limitations

1. **Single Write Lock**: Only one write transaction at a time
2. **Memory-Mapped I/O**: Not optimal for network storage
3. **Page Size**: Fixed 4KB pages (configurable but not dynamic)
4. **Database Limits**: Maximum 128 named databases
5. **Transaction Size**: No built-in transaction page limits

### Performance Considerations

1. **Write Amplification**: Copy-on-write can cause write amplification
2. **Memory Pressure**: Large databases can cause memory pressure
3. **Fragmentation**: Long-running databases may experience fragmentation
4. **Network Storage**: Memory mapping not optimal for EBS/network storage

### Compatibility Notes

1. **LMDB Compatibility**: API compatible but not 100% feature parity
2. **Platform Support**: Primarily tested on Linux, limited Windows support
3. **Architecture Support**: Optimized for x86_64, other architectures may be slower

## Future Improvements

### Planned Features

1. **Multiple I/O Backends**:
   - Direct I/O for cloud storage
   - io_uring for high-performance I/O
   - Async I/O support

2. **Enhanced Concurrency**:
   - Database-level write locks
   - Concurrent write transactions
   - Optimistic concurrency control

3. **Advanced Optimizations**:
   - Compression support
   - Encryption support
   - Advanced indexing (secondary indexes)

4. **Monitoring and Observability**:
   - Built-in metrics collection
   - Performance profiling tools
   - Health check endpoints

### Performance Roadmap

1. **SIMD Optimizations**: Expand SIMD usage throughout codebase
2. **Memory Management**: Improved memory allocation strategies
3. **Caching**: Multi-level caching system
4. **Parallel Processing**: Parallel B+Tree operations

### Architecture Evolution

1. **Modular Design**: Plugin-based architecture for extensions
2. **API Evolution**: Backward-compatible API improvements
3. **Configuration**: Runtime configuration management
4. **Tooling**: Enhanced development and debugging tools

## Conclusion

ZeroDB represents a modern, type-safe implementation of LMDB with significant performance optimizations. Its architecture prioritizes:

- **Performance**: Memory-mapped I/O, SIMD, NUMA awareness
- **Safety**: Rust's type system, comprehensive error handling
- **Compatibility**: LMDB API compatibility for easy migration
- **Extensibility**: Modular design for future enhancements

The codebase is well-tested, thoroughly documented, and designed for production use. While there are some limitations compared to more mature databases, ZeroDB provides a solid foundation for embedded key-value storage with modern performance characteristics.

For questions about the implementation or contributing to the project, refer to the main README.md and CLAUDE.md files for development guidelines.