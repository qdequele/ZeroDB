//! Comprehensive database comparison benchmark
//!
//! Compares ZeroDB against LMDB (heed), RocksDB, redb, and sled across various workloads

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use redb::ReadableTable;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

// Test configurations
const SMALL_VALUE: usize = 100;
const MEDIUM_VALUE: usize = 1_000;
const LARGE_VALUE: usize = 10_000;

const SMALL_DATASET: usize = 1_000;
const MEDIUM_DATASET: usize = 10_000;
const LARGE_DATASET: usize = 100_000;

fn bench_sequential_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_writes");
    group.sample_size(10);

    for &dataset_size in &[SMALL_DATASET, MEDIUM_DATASET, LARGE_DATASET] {
        for &value_size in &[SMALL_VALUE, MEDIUM_VALUE, LARGE_VALUE] {
            let id = format!("{}_keys_{}_bytes", dataset_size, value_size);

            // ZeroDB
            group.bench_with_input(
                BenchmarkId::new("zerodb", &id),
                &(dataset_size, value_size),
                |b, &(size, val_size)| {
                    b.iter_batched(
                        || {
                            let dir = TempDir::new().unwrap();
                            let env = Arc::new(
                                zerodb::EnvBuilder::new()
                                    .map_size(2 * 1024 * 1024 * 1024) // 2GB to handle large dataset
                                    .open(dir.path())
                                    .unwrap(),
                            );
                            let db = {
                                let mut txn = env.write_txn().unwrap();
                                let db: zerodb::db::Database<Vec<u8>, Vec<u8>> =
                                    env.create_database(&mut txn, None).unwrap();
                                txn.commit().unwrap();
                                db
                            };
                            (dir, env, db)
                        },
                        |(_dir, env, db)| {
                            let value = vec![0u8; val_size];

                            let mut txn = env.write_txn().unwrap();
                            for i in 0..size {
                                let key = format!("key_{:08}", i).into_bytes();
                                if let Err(e) = db.put(&mut txn, key.clone(), value.clone())
                                {
                                    eprintln!("ZeroDB put error at key {}: {:?}", i, e);
                                    panic!("Failed to insert: {:?}", e);
                                }
                            }
                            txn.commit().unwrap();
                        },
                        BatchSize::SmallInput,
                    );
                },
            );

            // LMDB (heed)
            group.bench_with_input(
                BenchmarkId::new("lmdb", &id),
                &(dataset_size, value_size),
                |b, &(size, val_size)| {
                    b.iter_batched(
                        || {
                            let dir = TempDir::new().unwrap();
                            let env = unsafe {
                                heed::EnvOpenOptions::new()
                                    .map_size(2 * 1024 * 1024 * 1024) // 2GB to handle large dataset
                                    .open(dir.path())
                                    .unwrap()
                            };
                            let mut wtxn = env.write_txn().unwrap();
                            let db: heed::Database<heed::types::Str, heed::types::Str> =
                                env.create_database(&mut wtxn, None).unwrap();
                            wtxn.commit().unwrap();
                            (dir, env, db)
                        },
                        |(_dir, env, db)| {
                            let mut wtxn = env.write_txn().unwrap();
                            let value = "x".repeat(val_size);

                            for i in 0..size {
                                let key = format!("key_{:08}", i);
                                db.put(&mut wtxn, &key, &value).unwrap();
                            }

                            wtxn.commit().unwrap();
                        },
                        BatchSize::SmallInput,
                    );
                },
            );

            // RocksDB
            group.bench_with_input(
                BenchmarkId::new("rocksdb", &id),
                &(dataset_size, value_size),
                |b, &(size, val_size)| {
                    b.iter_batched(
                        || {
                            let dir = TempDir::new().unwrap();
                            let db = rocksdb::DB::open_default(dir.path()).unwrap();
                            (dir, db)
                        },
                        |(_dir, db)| {
                            let value = vec![0u8; val_size];

                            for i in 0..size {
                                let key = format!("key_{:08}", i);
                                db.put(key.as_bytes(), &value).unwrap();
                            }
                        },
                        BatchSize::SmallInput,
                    );
                },
            );

            // redb
            group.bench_with_input(
                BenchmarkId::new("redb", &id),
                &(dataset_size, value_size),
                |b, &(size, val_size)| {
                    b.iter_batched(
                        || {
                            let dir = TempDir::new().unwrap();
                            let db_path = dir.path().join("db");
                            let db = redb::Database::create(&db_path).unwrap();
                            let table_def: redb::TableDefinition<&[u8], &[u8]> =
                                redb::TableDefinition::new("bench");
                            (dir, db, table_def)
                        },
                        |(_dir, db, table_def)| {
                            let write_txn = db.begin_write().unwrap();
                            {
                                let mut table = write_txn.open_table(table_def).unwrap();
                                let value = vec![0u8; val_size];

                                for i in 0..size {
                                    let key = format!("key_{:08}", i);
                                    table.insert(key.as_bytes(), &value[..]).unwrap();
                                }
                            }
                            write_txn.commit().unwrap();
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_random_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_writes");
    group.sample_size(10);

    for &value_size in &[SMALL_VALUE, MEDIUM_VALUE] {
        for &dataset_size in &[SMALL_DATASET, MEDIUM_DATASET] {
            let id = format!("{}_keys_{}_bytes", dataset_size, value_size);

            // ZeroDB
            group.bench_with_input(
                BenchmarkId::new("zerodb", &id),
                &(dataset_size, value_size),
                |b, &(size, val_size)| {
                    b.iter_batched(
                        || {
                            let dir = TempDir::new().unwrap();
                            let env = Arc::new(
                                zerodb::EnvBuilder::new()
                                    .map_size(2 * 1024 * 1024 * 1024) // 2GB to handle large dataset
                                    .open(dir.path())
                                    .unwrap(),
                            );
                            let db = {
                                let mut txn = env.write_txn().unwrap();
                                let db: zerodb::db::Database<Vec<u8>, Vec<u8>> =
                                    env.create_database(&mut txn, None).unwrap();
                                txn.commit().unwrap();
                                db
                            };

                            let mut rng = StdRng::seed_from_u64(42);
                            let keys: Vec<Vec<u8>> = (0..size)
                                .map(|_| {
                                    let key = rng.gen::<u64>();
                                    format!("key_{:016}", key).into_bytes()
                                })
                                .collect();

                            (dir, env, db, keys)
                        },
                        |(_dir, env, db, keys)| {
                            let value = vec![0u8; val_size];

                            // Process all keys in a single transaction like other databases
                            let mut txn = env.write_txn().unwrap();
                            for key in keys {
                                db.put(&mut txn, key.clone(), value.clone()).unwrap();
                            }
                            txn.commit().unwrap();
                        },
                        BatchSize::SmallInput,
                    );
                },
            );

            // LMDB
            group.bench_with_input(
                BenchmarkId::new("lmdb", &id),
                &(dataset_size, value_size),
                |b, &(size, val_size)| {
                    b.iter_batched(
                        || {
                            let dir = TempDir::new().unwrap();
                            let env = unsafe {
                                heed::EnvOpenOptions::new()
                                    .map_size(2 * 1024 * 1024 * 1024) // 2GB to handle large dataset
                                    .open(dir.path())
                                    .unwrap()
                            };
                            let mut wtxn = env.write_txn().unwrap();
                            let db: heed::Database<heed::types::Str, heed::types::Str> =
                                env.create_database(&mut wtxn, None).unwrap();
                            wtxn.commit().unwrap();

                            let mut rng = StdRng::seed_from_u64(42);
                            let keys: Vec<String> = (0..size)
                                .map(|_| format!("key_{:016}", rng.gen::<u64>()))
                                .collect();

                            (dir, env, db, keys)
                        },
                        |(_dir, env, db, keys)| {
                            let mut wtxn = env.write_txn().unwrap();
                            let value = "x".repeat(val_size);

                            for key in keys {
                                db.put(&mut wtxn, &key, &value).unwrap();
                            }

                            wtxn.commit().unwrap();
                        },
                        BatchSize::SmallInput,
                    );
                },
            );

            // RocksDB
            group.bench_with_input(
                BenchmarkId::new("rocksdb", &id),
                &(dataset_size, value_size),
                |b, &(size, val_size)| {
                    b.iter_batched(
                        || {
                            let dir = TempDir::new().unwrap();
                            let db = rocksdb::DB::open_default(dir.path()).unwrap();

                            let mut rng = StdRng::seed_from_u64(42);
                            let keys: Vec<String> = (0..size)
                                .map(|_| format!("key_{:016}", rng.gen::<u64>()))
                                .collect();

                            (dir, db, keys)
                        },
                        |(_dir, db, keys)| {
                            let value = vec![0u8; val_size];

                            for key in keys {
                                db.put(key.as_bytes(), &value).unwrap();
                            }
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_random_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_reads");

    for &dataset_size in &[SMALL_DATASET, MEDIUM_DATASET] {
        let id = format!("{}_keys", dataset_size);

        // ZeroDB
        group.bench_with_input(BenchmarkId::new("zerodb", &id), &dataset_size, |b, &size| {
            let dir = TempDir::new().unwrap();
            let env = Arc::new(
                zerodb::EnvBuilder::new().map_size(1024 * 1024 * 1024).open(dir.path()).unwrap(),
            );
            let db = {
                let mut txn = env.write_txn().unwrap();
                let db: zerodb::db::Database<Vec<u8>, Vec<u8>> =
                    env.create_database(&mut txn, None).unwrap();

                // Populate
                let value = vec![0u8; SMALL_VALUE];
                for i in 0..size {
                    let key = format!("key_{:08}", i).into_bytes();
                    db.put(&mut txn, key, value.clone()).unwrap();
                }

                txn.commit().unwrap();
                db
            };

            let mut rng = StdRng::seed_from_u64(42);
            let read_keys: Vec<Vec<u8>> = (0..size / 10)
                .map(|_| {
                    let idx = rng.gen_range(0..size);
                    format!("key_{:08}", idx).into_bytes()
                })
                .collect();

            b.iter(|| {
                let txn = env.read_txn().unwrap();
                let mut sum = 0u64;

                for key in &read_keys {
                    if let Some(val) = db.get(&txn, key).unwrap() {
                        sum += val.len() as u64;
                    }
                }

                sum
            });
        });

        // LMDB
        group.bench_with_input(BenchmarkId::new("lmdb", &id), &dataset_size, |b, &size| {
            let dir = TempDir::new().unwrap();
            let env = unsafe {
                heed::EnvOpenOptions::new().map_size(1024 * 1024 * 1024).open(dir.path()).unwrap()
            };
            let mut wtxn = env.write_txn().unwrap();
            let db: heed::Database<heed::types::Str, heed::types::Str> =
                env.create_database(&mut wtxn, None).unwrap();
            wtxn.commit().unwrap();

            // Populate
            {
                let mut wtxn = env.write_txn().unwrap();
                let value = "x".repeat(SMALL_VALUE);
                for i in 0..size {
                    let key = format!("key_{:08}", i);
                    db.put(&mut wtxn, &key, &value).unwrap();
                }
                wtxn.commit().unwrap();
            }

            let mut rng = StdRng::seed_from_u64(42);
            let read_keys: Vec<String> = (0..size / 10)
                .map(|_| {
                    let idx = rng.gen_range(0..size);
                    format!("key_{:08}", idx)
                })
                .collect();

            b.iter(|| {
                let rtxn = env.read_txn().unwrap();
                let mut sum = 0u64;

                for key in &read_keys {
                    if let Some(val) = db.get(&rtxn, key.as_str()).unwrap() {
                        sum += val.len() as u64;
                    }
                }

                sum
            });
        });

        // RocksDB
        group.bench_with_input(BenchmarkId::new("rocksdb", &id), &dataset_size, |b, &size| {
            let dir = TempDir::new().unwrap();
            let db = rocksdb::DB::open_default(dir.path()).unwrap();

            // Populate
            let value = vec![0u8; SMALL_VALUE];
            for i in 0..size {
                let key = format!("key_{:08}", i);
                db.put(key.as_bytes(), &value).unwrap();
            }

            let mut rng = StdRng::seed_from_u64(42);
            let read_keys: Vec<String> = (0..size / 10)
                .map(|_| {
                    let idx = rng.gen_range(0..size);
                    format!("key_{:08}", idx)
                })
                .collect();

            b.iter(|| {
                let mut sum = 0u64;

                for key in &read_keys {
                    if let Ok(Some(val)) = db.get(key.as_bytes()) {
                        sum += val.len() as u64;
                    }
                }

                sum
            });
        });

        // redb
        group.bench_with_input(BenchmarkId::new("redb", &id), &dataset_size, |b, &size| {
            let dir = TempDir::new().unwrap();
            let db_path = dir.path().join("db");
            let db = redb::Database::create(&db_path).unwrap();
            let table_def: redb::TableDefinition<&[u8], &[u8]> =
                redb::TableDefinition::new("bench");

            // Populate
            {
                let write_txn = db.begin_write().unwrap();
                {
                    let mut table = write_txn.open_table(table_def).unwrap();
                    let value = [0u8; SMALL_VALUE];

                    for i in 0..size {
                        let key = format!("key_{:08}", i);
                        table.insert(key.as_bytes(), &value[..]).unwrap();
                    }
                }
                write_txn.commit().unwrap();
            }

            let mut rng = StdRng::seed_from_u64(42);
            let read_keys: Vec<String> = (0..size / 10)
                .map(|_| {
                    let idx = rng.gen_range(0..size);
                    format!("key_{:08}", idx)
                })
                .collect();

            b.iter(|| {
                let read_txn = db.begin_read().unwrap();
                let table = read_txn.open_table(table_def).unwrap();
                let mut sum = 0u64;

                for key in &read_keys {
                    if let Ok(Some(val)) = table.get(key.as_bytes()) {
                        sum += val.value().len() as u64;
                    }
                }

                sum
            });
        });
    }

    group.finish();
}

fn bench_concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_reads");
    group.sample_size(10);

    let dataset_size = SMALL_DATASET;

    for &num_readers in &[2, 4, 8] {
        let id = format!("{}_readers", num_readers);

        // ZeroDB
        group.bench_with_input(BenchmarkId::new("zerodb", &id), &num_readers, |b, &readers| {
            let dir = TempDir::new().unwrap();
            let env = Arc::new(
                zerodb::EnvBuilder::new()
                    .map_size(1024 * 1024 * 1024)
                    .max_readers(128)
                    .open(dir.path())
                    .unwrap(),
            );
            let db = {
                let mut txn = env.write_txn().unwrap();
                let db: zerodb::db::Database<Vec<u8>, Vec<u8>> =
                    env.create_database(&mut txn, None).unwrap();

                // Populate
                let value = vec![0u8; SMALL_VALUE];
                for i in 0..dataset_size {
                    let key = format!("key_{:08}", i).into_bytes();
                    db.put(&mut txn, key, value.clone()).unwrap();
                }

                txn.commit().unwrap();
                db
            };

            b.iter(|| {
                let handles: Vec<_> = (0..readers)
                    .map(|_| {
                        let env = env.clone();
                        let db = db.clone();

                        thread::spawn(move || {
                            let txn = env.read_txn().unwrap();
                            let mut sum = 0u64;

                            // Each reader does 100 reads
                            for i in 0..100 {
                                let key = format!("key_{:08}", i % dataset_size).into_bytes();
                                if let Some(val) = db.get(&txn, &key).unwrap() {
                                    sum += val.len() as u64;
                                }
                            }
                            sum
                        })
                    })
                    .collect();

                // Wait for all readers
                let total: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();

                total
            });
        });

        // LMDB
        group.bench_with_input(BenchmarkId::new("lmdb", &id), &num_readers, |b, &readers| {
            let dir = TempDir::new().unwrap();
            let env = Arc::new(unsafe {
                heed::EnvOpenOptions::new()
                    .map_size(1024 * 1024 * 1024)
                    .max_readers(128)
                    .open(dir.path())
                    .unwrap()
            });
            let mut wtxn = env.write_txn().unwrap();
            let db: heed::Database<heed::types::Str, heed::types::Str> =
                env.create_database(&mut wtxn, None).unwrap();
            wtxn.commit().unwrap();

            // Populate
            {
                let mut wtxn = env.write_txn().unwrap();
                let value = "x".repeat(SMALL_VALUE);
                for i in 0..dataset_size {
                    let key = format!("key_{:08}", i);
                    db.put(&mut wtxn, &key, &value).unwrap();
                }
                wtxn.commit().unwrap();
            }

            b.iter(|| {
                let handles: Vec<_> = (0..readers)
                    .map(|_| {
                        let env = env.clone();

                        thread::spawn(move || {
                            let rtxn = env.read_txn().unwrap();
                            let mut sum = 0u64;

                            for i in 0..100 {
                                let key = format!("key_{:08}", i % dataset_size);
                                if let Some(val) = db.get(&rtxn, &key).unwrap() {
                                    sum += val.len() as u64;
                                }
                            }
                            sum
                        })
                    })
                    .collect();

                let total: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();

                total
            });
        });
    }

    group.finish();
}

fn bench_full_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_scan");

    for &dataset_size in &[SMALL_DATASET, MEDIUM_DATASET] {
        let id = format!("{}_keys", dataset_size);

        // ZeroDB
        group.bench_with_input(BenchmarkId::new("zerodb", &id), &dataset_size, |b, &size| {
            let dir = TempDir::new().unwrap();
            let env = Arc::new(
                zerodb::EnvBuilder::new().map_size(1024 * 1024 * 1024).open(dir.path()).unwrap(),
            );
            let db = {
                let mut txn = env.write_txn().unwrap();
                let db: zerodb::db::Database<Vec<u8>, Vec<u8>> =
                    env.create_database(&mut txn, None).unwrap();

                // Populate
                let value = vec![0u8; SMALL_VALUE];
                for i in 0..size {
                    let key = format!("key_{:08}", i).into_bytes();
                    db.put(&mut txn, key, value.clone()).unwrap();
                }

                txn.commit().unwrap();
                db
            };

            b.iter(|| {
                let txn = env.read_txn().unwrap();
                let mut count = 0;

                let mut cursor = db.cursor(&txn).unwrap();
                if cursor.first_raw().unwrap().is_some() {
                    count += 1;
                    while cursor.next_raw().unwrap().is_some() {
                        count += 1;
                    }
                }

                assert_eq!(count, size);
                count
            });
        });

        // LMDB
        group.bench_with_input(BenchmarkId::new("lmdb", &id), &dataset_size, |b, &size| {
            let dir = TempDir::new().unwrap();
            let env = unsafe {
                heed::EnvOpenOptions::new().map_size(1024 * 1024 * 1024).open(dir.path()).unwrap()
            };
            let mut wtxn = env.write_txn().unwrap();
            let db: heed::Database<heed::types::Str, heed::types::Str> =
                env.create_database(&mut wtxn, None).unwrap();
            wtxn.commit().unwrap();

            // Populate
            {
                let mut wtxn = env.write_txn().unwrap();
                let value = "x".repeat(SMALL_VALUE);
                for i in 0..size {
                    let key = format!("key_{:08}", i);
                    db.put(&mut wtxn, &key, &value).unwrap();
                }
                wtxn.commit().unwrap();
            }

            b.iter(|| {
                let rtxn = env.read_txn().unwrap();
                let mut count = 0;

                for item in db.iter(&rtxn).unwrap() {
                    let (_, _) = item.unwrap();
                    count += 1;
                }

                assert_eq!(count, size);
                count
            });
        });

        // RocksDB
        group.bench_with_input(BenchmarkId::new("rocksdb", &id), &dataset_size, |b, &size| {
            let dir = TempDir::new().unwrap();
            let db = rocksdb::DB::open_default(dir.path()).unwrap();

            // Populate
            let value = vec![0u8; SMALL_VALUE];
            for i in 0..size {
                let key = format!("key_{:08}", i);
                db.put(key.as_bytes(), &value).unwrap();
            }

            b.iter(|| {
                let iter = db.iterator(rocksdb::IteratorMode::Start);
                let mut count = 0;

                for item in iter {
                    let (_, _) = item.unwrap();
                    count += 1;
                }

                assert_eq!(count, size);
                count
            });
        });

        // redb
        group.bench_with_input(BenchmarkId::new("redb", &id), &dataset_size, |b, &size| {
            let dir = TempDir::new().unwrap();
            let db_path = dir.path().join("db");
            let db = redb::Database::create(&db_path).unwrap();
            let table_def: redb::TableDefinition<&[u8], &[u8]> =
                redb::TableDefinition::new("bench");

            // Populate
            {
                let write_txn = db.begin_write().unwrap();
                {
                    let mut table = write_txn.open_table(table_def).unwrap();
                    let value = [0u8; SMALL_VALUE];

                    for i in 0..size {
                        let key = format!("key_{:08}", i);
                        table.insert(key.as_bytes(), &value[..]).unwrap();
                    }
                }
                write_txn.commit().unwrap();
            }

            b.iter(|| {
                let read_txn = db.begin_read().unwrap();
                let table = read_txn.open_table(table_def).unwrap();
                let mut count = 0;

                for item in table.iter().unwrap() {
                    let (_, _) = item.unwrap();
                    count += 1;
                }

                assert_eq!(count, size);
                count
            });
        });
    }

    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.sample_size(10);

    // 80% reads, 20% writes
    let dataset_size = SMALL_DATASET;

    // ZeroDB - limited batch size
    group.bench_function("zerodb", |b| {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            zerodb::EnvBuilder::new().map_size(1024 * 1024 * 1024).open(dir.path()).unwrap(),
        );
        let db = {
            let mut txn = env.write_txn().unwrap();
            let db: zerodb::db::Database<Vec<u8>, Vec<u8>> =
                env.create_database(&mut txn, None).unwrap();

            // Prepopulate
            let value = vec![0u8; SMALL_VALUE];
            for i in 0..dataset_size {
                let key = format!("key_{:08}", i).into_bytes();
                db.put(&mut txn, key, value.clone()).unwrap();
            }

            txn.commit().unwrap();
            db
        };

        let mut rng = StdRng::seed_from_u64(42);

        b.iter(|| {
            let mut total_ops = 0;

            // 10 batches of operations
            for _ in 0..10 {
                if rng.gen_bool(0.2) {
                    // Write batch
                    let mut txn = env.write_txn().unwrap();
                    for _ in 0..100 {
                        let key = format!("key_{:08}", rng.gen_range(0..dataset_size)).into_bytes();
                        let value = (0..SMALL_VALUE).map(|_| rng.gen::<u8>()).collect::<Vec<u8>>();
                        db.put(&mut txn, key, value).unwrap();
                        total_ops += 1;
                    }
                    txn.commit().unwrap();
                } else {
                    // Read batch
                    let txn = env.read_txn().unwrap();
                    for _ in 0..100 {
                        let key = format!("key_{:08}", rng.gen_range(0..dataset_size)).into_bytes();
                        db.get(&txn, &key).unwrap();
                        total_ops += 1;
                    }
                }
            }

            total_ops
        });
    });

    // LMDB
    group.bench_function("lmdb", |b| {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(unsafe {
            heed::EnvOpenOptions::new().map_size(1024 * 1024 * 1024).open(dir.path()).unwrap()
        });
        let mut wtxn = env.write_txn().unwrap();
        let db: heed::Database<heed::types::Str, heed::types::Str> =
            env.create_database(&mut wtxn, None).unwrap();
        wtxn.commit().unwrap();

        // Prepopulate
        {
            let mut wtxn = env.write_txn().unwrap();
            let value = "x".repeat(SMALL_VALUE);
            for i in 0..dataset_size {
                let key = format!("key_{:08}", i);
                db.put(&mut wtxn, &key, &value).unwrap();
            }
            wtxn.commit().unwrap();
        }

        let mut rng = StdRng::seed_from_u64(42);

        b.iter(|| {
            let mut total_ops = 0;

            for _ in 0..10 {
                if rng.gen_bool(0.2) {
                    // Write batch
                    let mut wtxn = env.write_txn().unwrap();
                    for _ in 0..100 {
                        let key = format!("key_{:08}", rng.gen_range(0..dataset_size));
                        let value = "x".repeat(SMALL_VALUE);
                        db.put(&mut wtxn, &key, &value).unwrap();
                        total_ops += 1;
                    }
                    wtxn.commit().unwrap();
                } else {
                    // Read batch
                    let rtxn = env.read_txn().unwrap();
                    for _ in 0..100 {
                        let key = format!("key_{:08}", rng.gen_range(0..dataset_size));
                        db.get(&rtxn, &key).unwrap();
                        total_ops += 1;
                    }
                }
            }

            total_ops
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_writes,
    bench_random_writes,
    bench_random_reads,
    bench_concurrent_reads,
    bench_full_scan,
    bench_mixed_workload
);

criterion_main!(benches);

