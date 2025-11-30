//! Comparison benchmarks: ZeroDB vs LMDB (heed) vs RocksDB.
//!
//! These benchmarks compare the performance of basic operations across
//! different key-value stores.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use std::cell::RefCell;
use std::collections::BTreeMap;
use tempfile::tempdir;

// ZeroDB imports
use zerodb::{
    btree::{CursorOps, CursorState, Node, PageBuilder},
    error::Result as ZeroResult,
    page::PageNo,
    EnvOpenOptions as ZeroEnvOptions,
};

// LMDB (heed) imports
use heed::{EnvOpenOptions as HeedEnvOptions, Database as HeedDatabase};
use heed::types::*;

// RocksDB imports
use rocksdb::DB as RocksDB;

/// Helper for ZeroDB page storage.
struct ZeroPageStore {
    pages: RefCell<BTreeMap<PageNo, Vec<u8>>>,
    next_pgno: RefCell<PageNo>,
}

impl ZeroPageStore {
    fn new(start_pgno: PageNo) -> Self {
        Self {
            pages: RefCell::new(BTreeMap::new()),
            next_pgno: RefCell::new(start_pgno),
        }
    }

    fn get(&self, pgno: PageNo) -> ZeroResult<Vec<u8>> {
        self.pages
            .borrow()
            .get(&pgno)
            .cloned()
            .ok_or(zerodb::Error::Corrupted)
    }

    fn alloc(&self) -> ZeroResult<PageNo> {
        let mut next = self.next_pgno.borrow_mut();
        let pgno = *next;
        *next += 1;
        Ok(pgno)
    }

    fn set(&self, pgno: PageNo, data: Vec<u8>) -> ZeroResult<()> {
        self.pages.borrow_mut().insert(pgno, data);
        Ok(())
    }
}

// ============================================================================
// Sequential Write Benchmarks
// ============================================================================

fn bench_sequential_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_writes");

    // Note: Sequential writes with fsync are slow, use smaller counts
    for count in [100, 500] {
        group.throughput(Throughput::Elements(count as u64));

        // ZeroDB
        group.bench_with_input(
            BenchmarkId::new("zerodb", count),
            &count,
            |b, &count| {
                b.iter(|| {
                    let dir = tempdir().unwrap();
                    let env = unsafe {
                        ZeroEnvOptions::new()
                            .map_size(100 * 1024 * 1024) // 100MB
                            .open(dir.path())
                            .unwrap()
                    };

                    for i in 0..count {
                        let mut wtxn = env.write_txn().unwrap();
                        let (_pgno, data) = wtxn.alloc_page().unwrap();
                        let key = format!("key{:08}", i);
                        let value = format!("value{:08}", i);
                        data[0..key.len()].copy_from_slice(key.as_bytes());
                        data[64..64 + value.len()].copy_from_slice(value.as_bytes());
                        wtxn.commit().unwrap();
                    }
                    black_box(env)
                })
            },
        );

        // LMDB (heed)
        group.bench_with_input(
            BenchmarkId::new("lmdb_heed", count),
            &count,
            |b, &count| {
                b.iter(|| {
                    let dir = tempdir().unwrap();
                    let env = unsafe {
                        HeedEnvOptions::new()
                            .map_size(100 * 1024 * 1024)
                            .open(dir.path())
                            .unwrap()
                    };
                    let mut wtxn = env.write_txn().unwrap();
                    let db: HeedDatabase<Str, Str> = env.create_database(&mut wtxn, None).unwrap();
                    wtxn.commit().unwrap();

                    for i in 0..count {
                        let mut wtxn = env.write_txn().unwrap();
                        let key = format!("key{:08}", i);
                        let value = format!("value{:08}", i);
                        db.put(&mut wtxn, &key, &value).unwrap();
                        wtxn.commit().unwrap();
                    }
                    black_box(env)
                })
            },
        );

        // RocksDB
        group.bench_with_input(
            BenchmarkId::new("rocksdb", count),
            &count,
            |b, &count| {
                b.iter(|| {
                    let dir = tempdir().unwrap();
                    let db = RocksDB::open_default(dir.path()).unwrap();

                    for i in 0..count {
                        let key = format!("key{:08}", i);
                        let value = format!("value{:08}", i);
                        db.put(key.as_bytes(), value.as_bytes()).unwrap();
                    }
                    black_box(db)
                })
            },
        );
    }

    group.finish();
}

// ============================================================================
// Batch Write Benchmarks
// ============================================================================

fn bench_batch_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_writes");

    for count in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(count as u64));

        // LMDB (heed) - batch in single transaction
        group.bench_with_input(
            BenchmarkId::new("lmdb_heed", count),
            &count,
            |b, &count| {
                b.iter(|| {
                    let dir = tempdir().unwrap();
                    let env = unsafe {
                        HeedEnvOptions::new()
                            .map_size(100 * 1024 * 1024)
                            .open(dir.path())
                            .unwrap()
                    };
                    let mut wtxn = env.write_txn().unwrap();
                    let db: HeedDatabase<Str, Str> = env.create_database(&mut wtxn, None).unwrap();

                    for i in 0..count {
                        let key = format!("key{:08}", i);
                        let value = format!("value{:08}", i);
                        db.put(&mut wtxn, &key, &value).unwrap();
                    }
                    wtxn.commit().unwrap();
                    black_box(env)
                })
            },
        );

        // RocksDB - batch write
        group.bench_with_input(
            BenchmarkId::new("rocksdb", count),
            &count,
            |b, &count| {
                b.iter(|| {
                    let dir = tempdir().unwrap();
                    let db = RocksDB::open_default(dir.path()).unwrap();
                    let mut batch = rocksdb::WriteBatch::default();

                    for i in 0..count {
                        let key = format!("key{:08}", i);
                        let value = format!("value{:08}", i);
                        batch.put(key.as_bytes(), value.as_bytes());
                    }
                    db.write(batch).unwrap();
                    black_box(db)
                })
            },
        );
    }

    group.finish();
}

// ============================================================================
// Random Read Benchmarks
// ============================================================================

fn bench_random_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_reads");
    let count = 1000;

    // Prepare keys
    let keys: Vec<String> = (0..count).map(|i| format!("key{:08}", i)).collect();
    let values: Vec<String> = (0..count).map(|i| format!("value{:08}", i)).collect();

    // LMDB (heed)
    {
        let dir = tempdir().unwrap();
        let env = unsafe {
            HeedEnvOptions::new()
                .map_size(100 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        };
        let mut wtxn = env.write_txn().unwrap();
        let db: HeedDatabase<Str, Str> = env.create_database(&mut wtxn, None).unwrap();
        for (k, v) in keys.iter().zip(values.iter()) {
            db.put(&mut wtxn, k, v).unwrap();
        }
        wtxn.commit().unwrap();

        group.throughput(Throughput::Elements(count as u64));
        group.bench_function("lmdb_heed", |b| {
            b.iter(|| {
                let rtxn = env.read_txn().unwrap();
                for key in &keys {
                    let val = db.get(&rtxn, key).unwrap();
                    black_box(val);
                }
                rtxn.commit().unwrap();
            })
        });
    }

    // RocksDB
    {
        let dir = tempdir().unwrap();
        let db = RocksDB::open_default(dir.path()).unwrap();
        for (k, v) in keys.iter().zip(values.iter()) {
            db.put(k.as_bytes(), v.as_bytes()).unwrap();
        }

        group.bench_function("rocksdb", |b| {
            b.iter(|| {
                for key in &keys {
                    let val = db.get(key.as_bytes()).unwrap();
                    black_box(val);
                }
            })
        });
    }

    group.finish();
}

// ============================================================================
// Sequential Read (Iteration) Benchmarks
// ============================================================================

fn bench_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("iteration");
    let count = 10000;

    // Prepare keys
    let keys: Vec<String> = (0..count).map(|i| format!("key{:08}", i)).collect();
    let values: Vec<String> = (0..count).map(|i| format!("value{:08}", i)).collect();

    // LMDB (heed)
    {
        let dir = tempdir().unwrap();
        let env = unsafe {
            HeedEnvOptions::new()
                .map_size(100 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        };
        let mut wtxn = env.write_txn().unwrap();
        let db: HeedDatabase<Str, Str> = env.create_database(&mut wtxn, None).unwrap();
        for (k, v) in keys.iter().zip(values.iter()) {
            db.put(&mut wtxn, k, v).unwrap();
        }
        wtxn.commit().unwrap();

        group.throughput(Throughput::Elements(count as u64));
        group.bench_function("lmdb_heed", |b| {
            b.iter(|| {
                let rtxn = env.read_txn().unwrap();
                let mut count = 0;
                for result in db.iter(&rtxn).unwrap() {
                    let (k, v) = result.unwrap();
                    black_box((k, v));
                    count += 1;
                }
                rtxn.commit().unwrap();
                black_box(count)
            })
        });
    }

    // RocksDB
    {
        let dir = tempdir().unwrap();
        let db = RocksDB::open_default(dir.path()).unwrap();
        for (k, v) in keys.iter().zip(values.iter()) {
            db.put(k.as_bytes(), v.as_bytes()).unwrap();
        }

        group.bench_function("rocksdb", |b| {
            b.iter(|| {
                let mut count = 0;
                let iter = db.iterator(rocksdb::IteratorMode::Start);
                for item in iter {
                    let (k, v) = item.unwrap();
                    black_box((&k, &v));
                    count += 1;
                }
                black_box(count)
            })
        });
    }

    group.finish();
}

// ============================================================================
// Transaction Overhead Benchmarks
// ============================================================================

fn bench_transaction_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction_overhead");

    // ZeroDB
    {
        let dir = tempdir().unwrap();
        let env = unsafe {
            ZeroEnvOptions::new()
                .map_size(100 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        };

        group.bench_function("zerodb_read_txn", |b| {
            b.iter(|| {
                let rtxn = env.read_txn().unwrap();
                black_box(rtxn.txnid());
                rtxn.commit().unwrap();
            })
        });

        group.bench_function("zerodb_write_txn_empty", |b| {
            b.iter(|| {
                let wtxn = env.write_txn().unwrap();
                wtxn.commit().unwrap();
            })
        });
    }

    // LMDB (heed)
    {
        let dir = tempdir().unwrap();
        let env = unsafe {
            HeedEnvOptions::new()
                .map_size(100 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        };

        group.bench_function("lmdb_heed_read_txn", |b| {
            b.iter(|| {
                let rtxn = env.read_txn().unwrap();
                black_box(&rtxn);
                rtxn.commit().unwrap();
            })
        });

        group.bench_function("lmdb_heed_write_txn_empty", |b| {
            b.iter(|| {
                let wtxn = env.write_txn().unwrap();
                wtxn.commit().unwrap();
            })
        });
    }

    group.finish();
}

// ============================================================================
// Point Lookup Benchmarks
// ============================================================================

fn bench_point_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_lookup");
    let count = 10000;

    // Prepare data
    let keys: Vec<String> = (0..count).map(|i| format!("key{:08}", i)).collect();
    let values: Vec<String> = (0..count).map(|i| format!("value{:08}", i)).collect();

    // LMDB (heed)
    {
        let dir = tempdir().unwrap();
        let env = unsafe {
            HeedEnvOptions::new()
                .map_size(100 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        };
        let mut wtxn = env.write_txn().unwrap();
        let db: HeedDatabase<Str, Str> = env.create_database(&mut wtxn, None).unwrap();
        for (k, v) in keys.iter().zip(values.iter()) {
            db.put(&mut wtxn, k, v).unwrap();
        }
        wtxn.commit().unwrap();

        // Lookup middle key
        let lookup_key = &keys[count / 2];

        group.bench_function("lmdb_heed", |b| {
            b.iter(|| {
                let rtxn = env.read_txn().unwrap();
                let val = db.get(&rtxn, lookup_key).unwrap();
                black_box(val);
                rtxn.commit().unwrap();
            })
        });
    }

    // RocksDB
    {
        let dir = tempdir().unwrap();
        let db = RocksDB::open_default(dir.path()).unwrap();
        for (k, v) in keys.iter().zip(values.iter()) {
            db.put(k.as_bytes(), v.as_bytes()).unwrap();
        }

        let lookup_key = &keys[count / 2];

        group.bench_function("rocksdb", |b| {
            b.iter(|| {
                let val = db.get(lookup_key.as_bytes()).unwrap();
                black_box(val);
            })
        });
    }

    // ZeroDB B+tree search (in-memory simulation)
    {
        let page_size = 4096;
        let store = ZeroPageStore::new(2);
        let root_pgno = store.alloc().unwrap();
        let mut builder = PageBuilder::new_leaf(root_pgno, page_size);

        // Add as many keys as will fit
        let num_keys = 100; // Limited by page size
        for i in 0..num_keys {
            let key = format!("key{:08}", i).into_bytes();
            let value = format!("value{:08}", i).into_bytes();
            builder.add_leaf(&Node::leaf(key, value)).unwrap();
        }
        store.set(root_pgno, builder.finish()).unwrap();

        let lookup_key = format!("key{:08}", num_keys / 2).into_bytes();

        group.bench_function("zerodb_btree_search", |b| {
            b.iter(|| {
                let mut state = CursorState::new(root_pgno);
                let result = CursorOps::search(
                    &mut state,
                    &lookup_key,
                    page_size,
                    |pgno| store.get(pgno),
                ).unwrap();
                black_box(result)
            })
        });
    }

    group.finish();
}

// ============================================================================
// Mixed Workload Benchmarks
// ============================================================================

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    let initial_count = 1000;
    let ops_count = 100;

    // LMDB (heed) - 50% reads, 25% writes, 25% deletes
    {
        group.bench_function("lmdb_heed", |b| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let env = unsafe {
                        HeedEnvOptions::new()
                            .map_size(100 * 1024 * 1024)
                            .open(dir.path())
                            .unwrap()
                    };
                    let mut wtxn = env.write_txn().unwrap();
                    let db: HeedDatabase<Str, Str> = env.create_database(&mut wtxn, None).unwrap();
                    for i in 0..initial_count {
                        let key = format!("key{:08}", i);
                        let value = format!("value{:08}", i);
                        db.put(&mut wtxn, &key, &value).unwrap();
                    }
                    wtxn.commit().unwrap();
                    (dir, env, db)
                },
                |(_dir, env, db)| {
                    for i in 0..ops_count {
                        match i % 4 {
                            0 | 1 => {
                                // Read
                                let key = format!("key{:08}", i % initial_count);
                                let rtxn = env.read_txn().unwrap();
                                let _val = db.get(&rtxn, &key).unwrap();
                                rtxn.commit().unwrap();
                            }
                            2 => {
                                // Write
                                let key = format!("key{:08}", initial_count + i);
                                let value = format!("value{:08}", i);
                                let mut wtxn = env.write_txn().unwrap();
                                db.put(&mut wtxn, &key, &value).unwrap();
                                wtxn.commit().unwrap();
                            }
                            3 => {
                                // Delete
                                let key = format!("key{:08}", i % initial_count);
                                let mut wtxn = env.write_txn().unwrap();
                                let _ = db.delete(&mut wtxn, &key);
                                wtxn.commit().unwrap();
                            }
                            _ => unreachable!(),
                        }
                    }
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }

    // RocksDB
    {
        group.bench_function("rocksdb", |b| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let db = RocksDB::open_default(dir.path()).unwrap();
                    for i in 0..initial_count {
                        let key = format!("key{:08}", i);
                        let value = format!("value{:08}", i);
                        db.put(key.as_bytes(), value.as_bytes()).unwrap();
                    }
                    (dir, db)
                },
                |(_dir, db)| {
                    for i in 0..ops_count {
                        match i % 4 {
                            0 | 1 => {
                                // Read
                                let key = format!("key{:08}", i % initial_count);
                                let _val = db.get(key.as_bytes()).unwrap();
                            }
                            2 => {
                                // Write
                                let key = format!("key{:08}", initial_count + i);
                                let value = format!("value{:08}", i);
                                db.put(key.as_bytes(), value.as_bytes()).unwrap();
                            }
                            3 => {
                                // Delete
                                let key = format!("key{:08}", i % initial_count);
                                let _ = db.delete(key.as_bytes());
                            }
                            _ => unreachable!(),
                        }
                    }
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_writes,
    bench_batch_writes,
    bench_random_reads,
    bench_iteration,
    bench_transaction_overhead,
    bench_point_lookup,
    bench_mixed_workload,
);

criterion_main!(benches);
