# API Compatibility Comparison: ZeroDB vs heed

This document compares the API compatibility between ZeroDB and heed, examining the main interfaces and their method signatures.

## Overview

Both ZeroDB and heed provide Rust bindings for LMDB-style databases, but with different approaches:
- **heed**: FFI wrapper around LMDB with type-safe abstractions
- **ZeroDB**: Pure Rust implementation with LMDB-compatible semantics

## API Comparison

### Environment

| Feature | heed | ZeroDB | Compatibility |
|---------|------|---------|---------------|
| **Builder Pattern** | `EnvOpenOptions::new()` | `EnvBuilder::new()` | ✅ Similar |
| **Open Environment** | `unsafe { options.open(path)? }` | `options.open(path)?` | ✅ Similar (no unsafe in ZeroDB) |
| **Map Size** | `.map_size(bytes)` | `.map_size(bytes)` | ✅ Identical |
| **Max Readers** | `.max_readers(n)` | `.max_readers(n)` | ✅ Identical |
| **Max DBs** | `.max_dbs(n)` | `.max_dbs(n)` | ✅ Identical |

**ZeroDB Extensions:**
- `.durability(mode)` - Control sync behavior
- `.checksum_mode(mode)` - Data integrity options
- `.use_segregated_freelist(bool)` - Performance optimization
- `.use_numa(bool)` - NUMA-aware allocation

### Database

| Feature | heed | ZeroDB | Compatibility |
|---------|------|---------|---------------|
| **Create Database** | `env.create_database(&mut wtxn, name)?` | `Database::open(&env, name, flags)?` | ⚠️ Different API |
| **Type Parameters** | `Database<K, V>` | `Database<K, V, C>` | ⚠️ ZeroDB adds comparator |
| **Database Name** | `Option<&str>` | `Option<&str>` | ✅ Identical |

### Transactions

| Feature | heed | ZeroDB | Compatibility |
|---------|------|---------|---------------|
| **Read Transaction** | `env.read_txn()?` | `env.read_txn()?` | ⚠️ Different method name |
| **Write Transaction** | `env.write_txn()?` | `env.write_txn()?` | ⚠️ Different method name |
| **Commit** | `txn.commit()?` | `txn.commit()?` | ✅ Identical |
| **Abort** | `txn.abort()` | `txn.abort()` | ✅ Identical |
| **Type Aliases** | `RoTxn<'env>`, `RwTxn<'env>` | `RoTxn<'env>`, `RwTxn<'env>` | ✅ Identical |

### Database Operations

| Feature | heed | ZeroDB | Compatibility |
|---------|------|---------|---------------|
| **Put** | `db.put(&mut wtxn, key, value)?` | `db.put(&mut txn, key, value)?` | ✅ Identical |
| **Get** | `db.get(&rtxn, key)?` | `db.get(&txn, key)?` | ✅ Identical |
| **Delete** | `db.delete(&mut wtxn, key)?` | `db.delete(&mut txn, key)?` | ✅ Identical |
| **Clear** | `db.clear(&mut wtxn)?` | `db.clear(&mut txn)?` | ✅ Identical |
| **Return Types** | Returns raw bytes | Returns typed values | ⚠️ Different |

### Cursor Operations

| Feature | heed | ZeroDB | Compatibility |
|---------|------|---------|---------------|
| **Create Cursor** | `db.iter(&rtxn)?` | `db.cursor(&txn)?` | ⚠️ Different API |
| **Iteration** | Iterator pattern | Cursor pattern | ⚠️ Different approach |
| **First/Last** | Via iterator | `cursor.first()`, `cursor.last()` | ⚠️ Different |
| **Next/Prev** | Iterator next() | `cursor.next()`, `cursor.prev()` | ⚠️ Different |
| **Seek** | Range iteration | `cursor.seek(key)` | ⚠️ Different |
| **Current** | N/A | `cursor.current()` | ❌ ZeroDB only |
| **Put via Cursor** | N/A | `cursor.put(k, v)` | ❌ ZeroDB only |
| **Delete via Cursor** | N/A | `cursor.delete()` | ❌ ZeroDB only |

### Type System

| Feature | heed | ZeroDB | Compatibility |
|---------|------|---------|---------------|
| **Key/Value Types** | Uses `heed::types::*` | Trait-based `Key`/`Value` | ⚠️ Different |
| **Encoding** | Type wrappers (Str, U32, etc.) | `.encode()` trait methods | ⚠️ Different |
| **Zero-Copy** | Yes, returns `&[u8]` | `Cow<'_, [u8]>` for flexibility | ⚠️ Different |

## Migration Guide

### From heed to ZeroDB

```rust
// heed
let env = unsafe { EnvOpenOptions::new().open(path)? };
let mut wtxn = env.write_txn()?;
let db: Database<Str, U32<NativeEndian>> = env.create_database(&mut wtxn, None)?;
db.put(&mut wtxn, "key", &42)?;
wtxn.commit()?;

// ZeroDB equivalent
let env = EnvBuilder::new().open(path)?;
let mut txn = env.write_txn()?;
let db: Database<String, u32> = Database::open(&env, None, DatabaseFlags::CREATE)?;
db.put(&mut txn, "key".to_string(), 42u32)?;
txn.commit()?;
```

### Key Differences

1. **Safety**: ZeroDB doesn't require `unsafe` for opening environments
2. **Transaction naming**: `write_txn()` → `write_txn()`, `read_txn()` → `read_txn()`
3. **Database creation**: Different API pattern
4. **Iteration**: heed uses Rust iterators, ZeroDB uses cursor pattern
5. **Type handling**: heed uses type wrappers, ZeroDB uses traits

## Feature Comparison

| Feature | heed | ZeroDB |
|---------|------|---------|
| LMDB compatibility | FFI wrapper | Pure Rust implementation |
| Performance | Native LMDB speed | Optimized with SIMD |
| Type safety | Type wrappers | Trait-based |
| Cursor control | Limited | Full control |
| Advanced features | Basic LMDB | Segregated freelist, NUMA, checksums |
| Platform support | Requires LMDB | Pure Rust, all platforms |

## Conclusion

While ZeroDB and heed share similar high-level concepts, they have significant API differences:

- **Method names** differ in several places (transaction creation, cursor operations)
- **Database creation** uses different patterns
- **Cursor API** is completely different (iterator vs explicit cursor)
- **Type system** approaches differ (wrappers vs traits)
- **Safety model** differs (unsafe requirements in heed)

ZeroDB provides more features and control but requires code changes to migrate from heed. The core operations (put, get, delete) are largely compatible, making basic migration straightforward, but cursor-based code will need significant rewrites.