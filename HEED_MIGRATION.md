# Migrating from heed to ZeroDB

This guide helps you migrate code from heed (LMDB FFI wrapper) to ZeroDB (pure Rust implementation).

## Compatibility Overview

ZeroDB provides similar functionality to heed but with a different API design. The libraries are **not drop-in compatible**, but migration is straightforward for most use cases.

### Key Differences

| Feature | heed | ZeroDB |
|---------|------|---------|
| Implementation | FFI to LMDB | Pure Rust |
| Safety | Requires `unsafe` for env | Safe API throughout |
| Type System | Type wrappers (`heed::types::*`) | Trait-based (`Key`/`Value` traits) |
| Iteration | Rust iterators | Explicit cursors |
| Database Creation | Through transaction | Direct on environment |

## API Mapping

### Environment

```rust
// heed
let env = unsafe {
    heed::EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024 * 1024)
        .max_dbs(10)
        .open(path)?
};

// ZeroDB
let env = EnvBuilder::new()
    .map_size(10 * 1024 * 1024 * 1024)
    .max_dbs(10)
    .open(path)?;
```

### Transactions

```rust
// heed
let rtxn = env.read_txn()?;
let mut wtxn = env.write_txn()?;

// ZeroDB - Original API
let rtxn = env.read_txn()?;
let mut wtxn = env.write_txn()?;

// ZeroDB - With heed-compatible names
let rtxn = env.read_txn()?;
let mut wtxn = env.write_txn()?;  // Note: requires &mut env in ZeroDB
```

### Database Creation

```rust
// heed
let mut wtxn = env.write_txn()?;
let db: heed::Database<heed::types::Str, heed::types::Bytes> = 
    env.create_database(&mut wtxn, Some("my-db"))?;
wtxn.commit()?;

// ZeroDB
let db = Database::<String, Vec<u8>>::open(
    &env,
    Some("my-db"),
    DatabaseFlags::CREATE
)?;
```

### Basic Operations

```rust
// Put - Nearly identical
db.put(&mut wtxn, key, value)?;  // Both

// Get - Nearly identical  
let value = db.get(&rtxn, key)?; // Both

// Delete - Nearly identical
db.delete(&mut wtxn, key)?;      // Both

// Clear - Nearly identical
db.clear(&mut wtxn)?;            // Both
```

### Iteration

ZeroDB provides both cursor-based and iterator-based APIs:

```rust
// heed - Iterator-based
for result in db.iter(&rtxn)? {
    let (key, value) = result?;
    // process
}

// ZeroDB - Cursor-based (original API)
let mut cursor = db.cursor(&rtxn)?;
while let Some((key, value)) = cursor.next()? {
    // process
}

// ZeroDB - Iterator-based (with cursor_iter module)
use zerodb::cursor_iter;
for result in cursor_iter::iter(&db, &rtxn)? {
    let (key, value) = result?;
    // process
}
```

### Range Queries

```rust
// heed
for result in db.range(&rtxn, "a".."z")? {
    let (key, value) = result?;
}

// ZeroDB - Cursor-based
let mut cursor = db.cursor(&rtxn)?;
cursor.seek(&"a")?;
while let Some((key, value)) = cursor.current()? {
    if key >= "z" {
        break;
    }
    cursor.next()?;
}

// ZeroDB - Iterator-based (with cursor_iter module)
use zerodb::cursor_iter;
for result in cursor_iter::range(&db, &rtxn, &"a"..&"z")? {
    let (key, value) = result?;
}
```

### Type Handling

```rust
// heed - Type wrappers
use heed::types::{Str, U32, Bytes};
let db: Database<Str, U32<BigEndian>> = ...;

// ZeroDB - Direct types with trait impls
let db: Database<String, u32> = ...;
// Or with custom types that implement Key/Value traits
```

## Feature Comparison

### Features in Both
- ACID transactions
- Multiple named databases
- Cursors for iteration
- Zero-copy reads
- MVCC (Multiple readers, single writer)

### ZeroDB-Specific Features
- No unsafe code required
- Durability modes (NoSync, NoMetaSync, Sync)
- Checksums for data integrity
- Segregated freelist optimization
- NUMA-aware allocation
- Cursor-based updates/deletes

### heed-Specific Features
- Lazy database creation
- More iterator adapters
- Closer to LMDB C API
- Proven in production (Meilisearch)

## Migration Strategy

1. **Start with data types**: Map heed type wrappers to ZeroDB types
2. **Update environment creation**: Remove `unsafe` blocks
3. **Change transaction methods**: `read_txn()` → `read_txn()`
4. **Convert iterations**: Replace iterator chains with cursor loops
5. **Test thoroughly**: Especially cursor-based operations

## Performance Considerations

- ZeroDB has comparable read performance (4+ GB/s for large values)
- Write performance is similar (400+ MB/s for large values)
- No FFI overhead in ZeroDB
- Both use memory-mapped I/O

## Example Migration

### Before (heed)
```rust
let env = unsafe { heed::EnvOpenOptions::new()
    .map_size(1_000_000_000)
    .open("data.mdb")? };

let mut wtxn = env.write_txn()?;
let db = env.create_database::<Str, Bytes>(&mut wtxn, Some("words"))?;
db.put(&mut wtxn, "hello", b"world")?;
wtxn.commit()?;

let rtxn = env.read_txn()?;
for result in db.iter(&rtxn)? {
    let (word, data) = result?;
    println!("{}: {} bytes", word, data.len());
}
```

### After (ZeroDB)
```rust
let env = EnvBuilder::new()
    .map_size(1_000_000_000)
    .open("data.mdb")?;

let db = Database::<String, Vec<u8>>::open(
    &env, Some("words"), DatabaseFlags::CREATE)?;

let mut wtxn = env.write_txn()?;
db.put(&mut wtxn, "hello".to_string(), b"world".to_vec())?;
wtxn.commit()?;

let rtxn = env.read_txn()?;
let mut cursor = db.cursor(&rtxn)?;
while let Some((word, data)) = cursor.next()? {
    println!("{}: {} bytes", word, data.len());
}
```

## Summary

While ZeroDB and heed are not API-compatible, they provide similar functionality. Migration requires updating method names, converting iterators to cursors, and adjusting type handling. The core operations (put, get, delete) remain nearly identical, making basic migration straightforward.