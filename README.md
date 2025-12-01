# ZeroDB

A pure-Rust implementation of LMDB (Lightning Memory-Mapped Database) with full Heed API compatibility.

[![Crates.io](https://img.shields.io/crates/v/zerodb.svg)](https://crates.io/crates/zerodb)
[![Documentation](https://docs.rs/zerodb/badge.svg)](https://docs.rs/zerodb)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Overview

ZeroDB is a memory-mapped key-value store that provides:

- **ACID transactions** with MVCC (Multi-Version Concurrency Control)
- **Zero-copy reads** via memory-mapped files
- **Single writer, multiple readers** without read blocking
- **Heed API compatibility** for drop-in replacement
- **Pure Rust** implementation with minimal unsafe code

## Features

- Full B+tree implementation with efficient page splitting
- Overflow page support for large values (up to available disk space)
- Named database support (multiple B+trees per environment)
- Nested transactions
- Custom key comparators
- Type-safe database API with compile-time encoding/decoding
- Cross-platform support (Linux, macOS, Windows)

## Installation

Add ZeroDB to your `Cargo.toml`:

```toml
[dependencies]
zerodb = "0.1"
```

For serde support:

```toml
[dependencies]
zerodb = { version = "0.1", features = ["serde"] }
```

## Quick Start

```rust
use zerodb::{EnvOpenOptions, Database};
use zerodb::types::{Str, U32};

fn main() -> zerodb::Result<()> {
    // Create a temporary directory for the database
    let path = tempfile::tempdir()?;

    // Open the environment
    let env = unsafe { EnvOpenOptions::new().open(path.path())? };

    // Create a typed database
    let mut wtxn = env.write_txn()?;
    let db: Database<Str, U32> = env.create_database(&mut wtxn, Some("my-db"))?;

    // Write some data
    db.put(&mut wtxn, "hello", &42)?;
    db.put(&mut wtxn, "world", &100)?;
    wtxn.commit()?;

    // Read data back
    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "hello")?, Some(42));
    assert_eq!(db.get(&rtxn, "world")?, Some(100));

    // Iterate over all entries
    for result in db.iter(&rtxn)? {
        let (key, value) = result?;
        println!("{}: {}", key, value);
    }

    Ok(())
}
```

## Type System

ZeroDB provides a rich type system for keys and values, compatible with Heed:

```rust
use zerodb::types::*;

// String types
let db: Database<Str, Str> = /* ... */;           // UTF-8 strings
let db: Database<Bytes, Bytes> = /* ... */;       // Raw bytes

// Integer types (native endian)
let db: Database<U32, U64> = /* ... */;           // u32 keys, u64 values
let db: Database<I32, I64> = /* ... */;           // Signed integers

// Big-endian integers (for lexicographic ordering)
let db: Database<BEU32, Str> = /* ... */;         // Big-endian u32 keys
let db: Database<BEU64, Bytes> = /* ... */;       // Big-endian u64 keys

// Unit type for sets
let db: Database<Str, Unit> = /* ... */;          // Key-only storage
```

### Serde Support

With the `serde` feature enabled:

```rust
use zerodb::types::{SerdeJson, SerdeBincode};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct User {
    name: String,
    age: u32,
}

// JSON encoding
let db: Database<Str, SerdeJson<User>> = /* ... */;

// Bincode encoding (more efficient)
let db: Database<Str, SerdeBincode<User>> = /* ... */;
```

## Configuration

### Environment Options

```rust
use zerodb::{EnvOpenOptions, EnvFlags};

let env = unsafe {
    EnvOpenOptions::new()
        .map_size(1024 * 1024 * 1024)  // 1 GB max size
        .max_dbs(10)                    // Up to 10 named databases
        .max_readers(126)               // Max concurrent readers
        .flags(EnvFlags::NO_SUB_DIR)    // Single file mode
        .open(path)?
};
```

### Database Flags

```rust
use zerodb::DatabaseFlags;

// Create database with specific flags
let db: Database<Str, Str> = env.database_options()
    .flags(DatabaseFlags::empty())
    .create(&mut wtxn)?;
```

## Transactions

### Read Transactions

```rust
let rtxn = env.read_txn()?;
let value = db.get(&rtxn, "key")?;
// rtxn is automatically aborted on drop
```

### Write Transactions

```rust
let mut wtxn = env.write_txn()?;
db.put(&mut wtxn, "key", &value)?;
db.delete(&mut wtxn, "key")?;
wtxn.commit()?;  // Or abort on drop
```

### Nested Transactions

```rust
let mut wtxn = env.write_txn()?;
db.put(&mut wtxn, "key1", &1)?;

{
    let mut nested = env.nested_write_txn(&mut wtxn)?;
    db.put(&mut nested, "key2", &2)?;
    nested.commit()?;  // Merge into parent
}

wtxn.commit()?;
```

## Iteration

```rust
let rtxn = env.read_txn()?;

// Forward iteration
for result in db.iter(&rtxn)? {
    let (key, value) = result?;
}

// Reverse iteration
for result in db.rev_iter(&rtxn)? {
    let (key, value) = result?;
}

// Range queries
for result in db.range(&rtxn, "a".."z")? {
    let (key, value) = result?;
}

// Prefix scanning
for result in db.prefix_iter(&rtxn, "user:")? {
    let (key, value) = result?;
}
```

## Performance

ZeroDB aims for performance parity with LMDB:

| Operation | ZeroDB | LMDB | Ratio |
|-----------|--------|------|-------|
| Sequential Writes (100) | ~436ms | ~372ms | 85% |
| Point Lookup | ~150ns | ~130ns | 87% |
| Read Transaction | ~14ns | ~37ns | 264% |

Run benchmarks with:

```bash
cargo bench --bench comparison
```

## Safety

The `EnvOpenOptions::open()` method is marked `unsafe` because:

1. The database file must not be opened multiple times simultaneously
2. The memory-mapped region must remain valid for the lifetime of the environment
3. The caller must ensure proper file permissions and locking

## Comparison with Heed

ZeroDB is designed as a drop-in replacement for Heed's LMDB backend:

| Feature | Heed (LMDB) | ZeroDB |
|---------|-------------|--------|
| Language | C + Rust bindings | Pure Rust |
| API | Typed Database | Compatible |
| File format | LMDB | LMDB-compatible |
| Transactions | MVCC | MVCC |
| Large values | Overflow pages | Overflow pages |

## Migrating from Heed

```rust
// Before (Heed)
use heed::{EnvOpenOptions, Database};
use heed::types::{Str, U32};

// After (ZeroDB)
use zerodb::{EnvOpenOptions, Database};
use zerodb::types::{Str, U32};

// The rest of your code remains the same!
```

## Architecture

ZeroDB follows LMDB's design:

- **B+tree storage** with copy-on-write pages
- **Dual meta pages** for atomic commits
- **Memory-mapped I/O** for zero-copy reads
- **Page-level locking** with single writer

See [LMDB_ARCHITECTURE.md](LMDB_ARCHITECTURE.md) for detailed internals.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [LMDB](https://github.com/LMDB/lmdb) by Howard Chu and Symas Corp
- [Heed](https://github.com/meilisearch/heed) by Meilisearch
