# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ZeroDB is a pure Rust implementation of LMDB (Lightning Memory-Mapped Database) with modern performance optimizations. It's an embedded key-value database that provides ACID transactions, type-safe APIs, and leverages memory-mapped files for high performance.

## Essential Commands

### Building and Testing
```bash
# Build the project
cargo build --release

# Run all tests
cargo test

# Run a specific test
cargo test test_name

# Run tests with output
cargo test -- --nocapture

# Run benchmarks
cargo bench

# Run a specific benchmark
cargo bench --bench db_comparison
```

### Performance Analysis
```bash
# Run full performance suite
./scripts/run-perf-suite.sh

# Run performance regression tests
./scripts/perf-regression-test.sh

# Generate performance dashboard
python3 scripts/generate-perf-dashboard.py
```

### Running Examples
```bash
# Simple usage example
cargo run --example simple

# Debug tools (useful for troubleshooting)
cargo run --example debug_btree_insert
cargo run --example debug_cursor_nav
```

## Architecture Overview

The codebase implements a B+Tree-based storage engine with the following key components:

1. **Environment** (`env.rs`): Entry point that manages memory-mapped files and database initialization. Uses type-state pattern for compile-time safety (Closed → Open → ReadOnly states).

2. **Transactions** (`txn.rs`): Implements MVCC with read and write transactions. Write transactions use copy-on-write for isolation and include page caching for performance.

3. **B+Tree** (`btree.rs`): Core data structure that handles sorted key-value storage with efficient search, insert, and delete operations. Optimized for sequential writes.

4. **Page Management**: 
   - `page.rs`: Defines page structure (default 4KB)
   - `freelist.rs`: Tracks free pages
   - `segregated_freelist.rs`: Size-segregated allocation for better performance
   - `overflow.rs`: Handles values larger than a single page

5. **I/O Backend** (`io.rs`): Provides memory-mapped file access for efficient disk operations.

## Key Design Patterns

- **Type-State Pattern**: Environment states are enforced at compile-time
- **Zero-Copy Operations**: Direct memory-mapped access where possible
- **Copy-on-Write**: Used for transaction isolation
- **Page-Based Storage**: All data is organized in fixed-size pages
- **Cursor-Based Navigation**: For efficient traversal of B+Trees

## Testing Strategy

- **Unit Tests**: Located within modules using `#[cfg(test)]`
- **Integration Tests**: In `/tests/` directory, covering database operations, B+Tree behavior, and edge cases
- **Benchmarks**: In `/benches/` directory, comparing performance against LMDB, RocksDB, redb, and sled
- **Property Testing**: Uses quickcheck for comparing behavior with LMDB

## Performance Considerations

Based on PERFORMANCE_OPTIMIZATIONS.md, the codebase includes:
- 16x faster sequential writes than LMDB
- SIMD optimizations for key comparisons
- Cache-aligned data structures
- Profile-guided optimization support
- Segregated freelists for efficient page allocation