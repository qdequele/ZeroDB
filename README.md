# ZeroDB

A high-performance, pure Rust implementation of LMDB (Lightning Memory-Mapped Database) with modern optimizations and type-safe APIs.

## Features

- **ACID Transactions** with Multi-Version Concurrency Control (MVCC)
- **Memory-Mapped Storage** for zero-copy data access
- **Type-Safe API** leveraging Rust's type system
- **B+Tree Storage Engine** for efficient sorted data access
- **Multiple Databases** per environment with named database support
- **Duplicate Key Support** with sorted duplicates
- **SIMD Optimizations** for key comparisons
- **NUMA-Aware** memory allocation support

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
zerodb = { path = "path/to/zerodb" }
```

## Quick Start

```rust
use zerodb::{EnvBuilder, Environment};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open an environment
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(1024 * 1024 * 1024) // 1GB
            .open("./my-db")?
    );

    // Create a database
    let db = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Write transaction
    let mut wtxn = env.write_txn()?;
    db.put(&mut wtxn, b"key", b"value")?;
    wtxn.commit()?;

    // Read transaction
    let rtxn = env.read_txn()?;
    let value = db.get(&rtxn, b"key")?;
    assert_eq!(value, Some(&b"value"[..]));

    Ok(())
}
```

## Performance

ZeroDB is designed for high performance with several optimizations:

- **Page Caching**: Reduces memory allocations during transactions
- **Segregated Freelist**: Efficient page allocation by size class
- **Zero-Copy Reads**: Direct access to memory-mapped data
- **Cache-Aligned Structures**: Optimized for CPU cache efficiency

### Benchmarks

Run benchmarks with:

```bash
cargo bench
```

Compare against other databases:

```bash
cargo bench --bench db_comparison
```

Latest Benchmarks: Comparing: zerodb, LMDB FFI, RocksDB, redb
```
=== Sequential Write Benchmark ===
Writing 10,000 key-value pairs (16 byte keys, 100 byte values)
zerodb:  11.00ms (884966 ops/sec)
LMDB FFI:   13.00ms (756888 ops/sec)
RocksDB:    2.00ms (4466778 ops/sec)
redb:       27.00ms (359759 ops/sec)

=== Random Read Benchmark ===
Reading 1,000 random keys from 10,000 total
zerodb:  823.00μs (1214022 ops/sec, 1000 found)
LMDB FFI:   688.00μs (1453400 ops/sec, 1000 found)
RocksDB:    883.00μs (1131702 ops/sec, 1000 found)
redb:       533.00μs (1875880 ops/sec, 1000 found)
```

## Space Planning

ZeroDB uses memory-mapped files, requiring upfront allocation of the maximum database size. Choose your `map_size` based on:

| Dataset Size | Recommended Map Size |
|-------------|---------------------|
| < 10K entries | 1 GB |
| 10K-100K entries | 2-4 GB |
| 100K-1M entries | 8-20 GB |
| > 1M entries | 32+ GB |

### Space Monitoring

```rust
// Check current usage
let info = env.space_info()?;
println!("Database usage: {:.1}%", info.percent_of_map_used);

// Estimate required size
use zerodb::space_info::MapSizeEstimator;
let estimator = MapSizeEstimator::new(num_entries, avg_key_size, avg_value_size);
let recommended = estimator.estimate();
```

See the [Space Planning Guide](docs/SPACE_PLANNING_GUIDE.md) for detailed information.

## Architecture

ZeroDB uses a B+Tree-based storage engine with the following components:

- **Environment**: Manages the memory-mapped database file
- **Transactions**: ACID-compliant with MVCC for concurrent access
- **Databases**: Multiple named databases within an environment
- **Pages**: Fixed-size blocks (default 4KB) for storage
- **Cursors**: Efficient traversal of sorted data

## Examples

See the `examples/` directory for more usage examples:

```bash
# Basic usage
cargo run --example simple

# Database catalog usage
cargo run --example simple_catalog

# NUMA demonstration
cargo run --example numa_demo
```

## Testing

Run the test suite:

```bash
# All tests
cargo test

# Specific test
cargo test test_basic

# With output
cargo test -- --nocapture
```

## Development

### Building

```bash
# Debug build
cargo build

# Release build
cargo build --release
```

### Performance Analysis

```bash
# Run performance suite
./scripts/run-perf-suite.sh

# Generate performance dashboard
python3 scripts/generate-perf-dashboard.py
```

## Features

- `simd`: SIMD optimizations (requires nightly Rust)


## License

[License information to be added]

## Contributing

[Contributing guidelines to be added]
