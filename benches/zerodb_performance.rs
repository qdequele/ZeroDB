//! ZeroDB-specific performance benchmarks
//!
//! Comprehensive benchmarks for tracking ZeroDB performance across versions.
//! Useful for regression testing and optimization validation.

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;
use zerodb::{db::Database, env::DurabilityMode, EnvBuilder};

// Value sizes to test
const SMALL_VALUE: usize = 64;
const MEDIUM_VALUE: usize = 512;
const INLINE_LIMIT: usize = 1024;
const OVERFLOW_SMALL: usize = 2048;
const OVERFLOW_LARGE: usize = 16384;

// Dataset sizes
const TINY_DATASET: usize = 100;
const SMALL_DATASET: usize = 1_000;
const MEDIUM_DATASET: usize = 10_000;
const LARGE_DATASET: usize = 100_000;

fn bench_btree_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_operations");

    // Sequential insert performance
    for &size in &[TINY_DATASET, SMALL_DATASET, MEDIUM_DATASET, LARGE_DATASET] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("sequential_insert", size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let dir = TempDir::new().unwrap();
                    let env = Arc::new(
                        EnvBuilder::new().map_size(100 * 1024 * 1024).open(dir.path()).unwrap(),
                    );
                    let db = {
                        let mut txn = env.write_txn().unwrap();
                        let db: Database<Vec<u8>, Vec<u8>> =
                            env.create_database(&mut txn, None).unwrap();
                        txn.commit().unwrap();
                        db
                    };
                    (dir, env, db)
                },
                |(_dir, env, db)| {
                    let mut txn = env.write_txn().unwrap();
                    for i in 0..size {
                        db.put(&mut txn, i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec())
                            .unwrap();
                    }
                    txn.commit().unwrap();
                },
                BatchSize::SmallInput,
            );
        });
    }

    // Page split performance
    group.bench_function("force_page_splits", |b| {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(EnvBuilder::new().map_size(100 * 1024 * 1024).open(dir.path()).unwrap());
        let db = {
            let mut txn = env.write_txn().unwrap();
            let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
            txn.commit().unwrap();
            db
        };

        b.iter(|| {
            let mut txn = env.write_txn().unwrap();

            // Insert values that will force splits
            let key_size = 100;
            let value_size = 100;

            for i in 0..50 {
                let key = vec![i as u8; key_size];
                let value = vec![i as u8; value_size];
                db.put(&mut txn, key, value).unwrap();
            }

            txn.commit().unwrap();

            // Clean up for next iteration
            let mut txn = env.write_txn().unwrap();
            for i in 0..50 {
                let key = vec![i as u8; key_size];
                db.delete(&mut txn, &key).unwrap();
            }
            txn.commit().unwrap();
        });
    });

    // Rebalancing performance
    group.bench_function("rebalancing_operations", |b| {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(EnvBuilder::new().map_size(100 * 1024 * 1024).open(dir.path()).unwrap());
        let db = {
            let mut txn = env.write_txn().unwrap();
            let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();

            // Fill with data
            let value = vec![0u8; 100];
            for i in 0..1000u32 {
                db.put(&mut txn, i.to_be_bytes().to_vec(), value.clone()).unwrap();
            }
            txn.commit().unwrap();
            db
        };

        b.iter(|| {
            let mut txn = env.write_txn().unwrap();

            // Delete entries to trigger rebalancing
            for i in (100..200u32).step_by(2) {
                db.delete(&mut txn, &i.to_be_bytes().to_vec()).unwrap();
            }

            txn.commit().unwrap();

            // Reinsert for next iteration
            let mut txn = env.write_txn().unwrap();
            let value = vec![0u8; 100];
            for i in (100..200u32).step_by(2) {
                db.put(&mut txn, i.to_be_bytes().to_vec(), value.clone()).unwrap();
            }
            txn.commit().unwrap();
        });
    });

    group.finish();
}

fn bench_page_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("page_allocation");

    // Freelist performance
    group.bench_function("freelist_allocation", |b| {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(100 * 1024 * 1024)
                .use_segregated_freelist(true)
                .open(dir.path())
                .unwrap(),
        );
        let db = {
            let mut txn = env.write_txn().unwrap();
            let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
            txn.commit().unwrap();
            db
        };

        // Pre-fragment the freelist
        {
            let mut txn = env.write_txn().unwrap();
            for i in 0..500u32 {
                let size = if i % 3 == 0 {
                    100
                } else if i % 3 == 1 {
                    1000
                } else {
                    5000
                };
                db.put(&mut txn, i.to_be_bytes().to_vec(), vec![i as u8; size]).unwrap();
            }
            txn.commit().unwrap();

            let mut txn = env.write_txn().unwrap();
            for i in (0..500u32).step_by(2) {
                db.delete(&mut txn, &i.to_be_bytes().to_vec()).unwrap();
            }
            txn.commit().unwrap();
        }

        let mut counter = 1000u32;

        b.iter(|| {
            let mut txn = env.write_txn().unwrap();

            // Allocate pages of different sizes
            for i in 0..10 {
                let size = match i % 3 {
                    0 => 100,
                    1 => 1000,
                    _ => 5000,
                };
                db.put(
                    &mut txn,
                    (counter + i).to_be_bytes().to_vec(),
                    vec![(counter + i) as u8; size],
                )
                .unwrap();
            }

            txn.commit().unwrap();

            // Delete for next iteration
            let mut txn = env.write_txn().unwrap();
            for i in 0..10 {
                db.delete(&mut txn, &(counter + i).to_be_bytes().to_vec()).unwrap();
            }
            txn.commit().unwrap();

            counter += 100;
        });
    });

    // COW page allocation
    group.bench_function("cow_page_allocation", |b| {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(EnvBuilder::new().map_size(100 * 1024 * 1024).open(dir.path()).unwrap());
        let db = {
            let mut txn = env.write_txn().unwrap();
            let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();

            // Pre-populate
            for i in 0..100u32 {
                db.put(&mut txn, i.to_be_bytes().to_vec(), vec![i as u8; 1000]).unwrap();
            }
            txn.commit().unwrap();
            db
        };

        b.iter(|| {
            // Hold a reader to force COW
            let read_txn = env.read_txn().unwrap();

            let mut txn = env.write_txn().unwrap();

            // Modify existing pages
            for i in 0..50u32 {
                db.put(&mut txn, i.to_be_bytes().to_vec(), vec![(i + 100) as u8; 1000]).unwrap();
            }

            txn.commit().unwrap();
            drop(read_txn);
        });
    });

    group.finish();
}

fn bench_overflow_handling(c: &mut Criterion) {
    let mut group = c.benchmark_group("overflow_handling");

    for &size in &[OVERFLOW_SMALL, OVERFLOW_LARGE] {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("write_overflow", size), &size, |b, &size| {
            let dir = TempDir::new().unwrap();
            let env =
                Arc::new(EnvBuilder::new().map_size(200 * 1024 * 1024).open(dir.path()).unwrap());
            let db = {
                let mut txn = env.write_txn().unwrap();
                let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
                txn.commit().unwrap();
                db
            };

            let value = vec![0xAB; size];
            let mut counter = 0u32;

            b.iter(|| {
                let mut txn = env.write_txn().unwrap();

                for i in 0..10 {
                    db.put(&mut txn, (counter + i).to_be_bytes().to_vec(), value.clone()).unwrap();
                }

                txn.commit().unwrap();
                counter += 10;
            });
        });

        group.bench_with_input(BenchmarkId::new("read_overflow", size), &size, |b, &size| {
            let dir = TempDir::new().unwrap();
            let env =
                Arc::new(EnvBuilder::new().map_size(200 * 1024 * 1024).open(dir.path()).unwrap());
            let db = {
                let mut txn = env.write_txn().unwrap();
                let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();

                // Pre-populate with overflow values
                let value = vec![0xAB; size];
                for i in 0..100u32 {
                    db.put(&mut txn, i.to_be_bytes().to_vec(), value.clone()).unwrap();
                }

                txn.commit().unwrap();
                db
            };

            b.iter(|| {
                let txn = env.read_txn().unwrap();
                let mut sum = 0u64;

                for i in 0..100u32 {
                    if let Some(val) = db.get(&txn, &i.to_be_bytes().to_vec()).unwrap() {
                        sum += val.len() as u64;
                    }
                }

                sum
            });
        });
    }

    group.finish();
}

fn bench_cursor_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("cursor_operations");

    // Forward iteration
    for &size in &[SMALL_DATASET, MEDIUM_DATASET] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("forward_iteration", size), &size, |b, &size| {
            let dir = TempDir::new().unwrap();
            let env =
                Arc::new(EnvBuilder::new().map_size(100 * 1024 * 1024).open(dir.path()).unwrap());
            let db = {
                let mut txn = env.write_txn().unwrap();
                let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();

                for i in 0..size {
                    db.put(&mut txn, i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec()).unwrap();
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
    }

    // Reverse iteration
    group.bench_function("reverse_iteration", |b| {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(EnvBuilder::new().map_size(100 * 1024 * 1024).open(dir.path()).unwrap());
        let db = {
            let mut txn = env.write_txn().unwrap();
            let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();

            for i in 0..SMALL_DATASET {
                db.put(&mut txn, i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec()).unwrap();
            }

            txn.commit().unwrap();
            db
        };

        b.iter(|| {
            let txn = env.read_txn().unwrap();
            let mut count = 0;

            let mut cursor = db.cursor(&txn).unwrap();
            if cursor.last_raw().unwrap().is_some() {
                count += 1;
                while cursor.prev_raw().unwrap().is_some() {
                    count += 1;
                }
            }

            assert_eq!(count, SMALL_DATASET);
            count
        });
    });

    // Range queries
    group.bench_function("range_queries", |b| {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(EnvBuilder::new().map_size(100 * 1024 * 1024).open(dir.path()).unwrap());
        let db = {
            let mut txn = env.write_txn().unwrap();
            let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();

            for i in 0..MEDIUM_DATASET {
                db.put(&mut txn, i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec()).unwrap();
            }

            txn.commit().unwrap();
            db
        };

        b.iter(|| {
            let txn = env.read_txn().unwrap();
            let mut count = 0;

            // Query middle 20% of data
            let start = (MEDIUM_DATASET as u32) * 2 / 5;
            let end = (MEDIUM_DATASET as u32) * 3 / 5;

            let mut cursor = db.cursor(&txn).unwrap();
            // Seek to start position
            if cursor.seek(&start.to_be_bytes().to_vec()).unwrap().is_some() {
                count += 1;
                // Continue until we reach end
                while let Some((k, _)) = cursor.next_raw().unwrap() {
                    let key_val = u32::from_be_bytes(k[..4].try_into().unwrap());
                    if key_val >= end {
                        break;
                    }
                    count += 1;
                }
            }

            count
        });
    });

    // Seek performance
    group.bench_function("cursor_seek", |b| {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(EnvBuilder::new().map_size(100 * 1024 * 1024).open(dir.path()).unwrap());
        let db = {
            let mut txn = env.write_txn().unwrap();
            let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();

            for i in 0..MEDIUM_DATASET {
                db.put(&mut txn, i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec()).unwrap();
            }

            txn.commit().unwrap();
            db
        };

        let mut rng = StdRng::seed_from_u64(42);
        let seek_targets: Vec<u32> =
            (0..100).map(|_| rng.gen_range(0..MEDIUM_DATASET as u32)).collect();

        b.iter(|| {
            let txn = env.read_txn().unwrap();
            let mut cursor = db.cursor(&txn).unwrap();
            let mut found = 0;

            for &target in &seek_targets {
                if cursor.seek(&target.to_be_bytes().to_vec()).unwrap().is_some() {
                    found += 1;
                }
            }

            found
        });
    });

    group.finish();
}

fn bench_transaction_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction_overhead");

    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().map_size(100 * 1024 * 1024).open(dir.path()).unwrap());
    let db = {
        let mut txn = env.write_txn().unwrap();
        let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
        txn.commit().unwrap();
        db
    };

    // Read transaction creation
    group.bench_function("read_txn_creation", |b| {
        b.iter(|| {
            let _txn = env.read_txn().unwrap();
        });
    });

    // Write transaction creation and commit
    group.bench_function("write_txn_empty_commit", |b| {
        b.iter(|| {
            let txn = env.write_txn().unwrap();
            txn.commit().unwrap();
        });
    });

    // Write transaction with operations
    for &ops in &[1, 10, 100] {
        group.bench_with_input(BenchmarkId::new("write_txn_with_ops", ops), &ops, |b, &ops| {
            let mut counter = 0u32;

            b.iter(|| {
                let mut txn = env.write_txn().unwrap();

                for i in 0..ops {
                    db.put(
                        &mut txn,
                        (counter + i as u32).to_be_bytes().to_vec(),
                        (i as u32).to_be_bytes().to_vec(),
                    )
                    .unwrap();
                }

                txn.commit().unwrap();
                counter += ops as u32;
            });
        });
    }

    // Transaction abort cost
    group.bench_function("write_txn_abort", |b| {
        b.iter(|| {
            let mut txn = env.write_txn().unwrap();
            db.put(&mut txn, 0u64.to_be_bytes().to_vec(), vec![0]).unwrap();
            drop(txn); // Implicit abort
        });
    });

    group.finish();
}

fn bench_durability_modes(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability_modes");
    group.sample_size(10);

    for mode in &[
        DurabilityMode::NoSync,
        DurabilityMode::AsyncFlush,
        DurabilityMode::SyncData,
        DurabilityMode::FullSync,
    ] {
        group.bench_with_input(
            BenchmarkId::new("commit_latency", format!("{:?}", mode)),
            mode,
            |b, mode| {
                let dir = TempDir::new().unwrap();
                let env = Arc::new(
                    EnvBuilder::new()
                        .map_size(100 * 1024 * 1024)
                        .durability(*mode)
                        .open(dir.path())
                        .unwrap(),
                );
                let db = {
                    let mut txn = env.write_txn().unwrap();
                    let db: Database<Vec<u8>, Vec<u8>> =
                        env.create_database(&mut txn, None).unwrap();
                    txn.commit().unwrap();
                    db
                };

                let value = vec![0u8; 1000];
                let mut counter = 0u32;

                b.iter(|| {
                    let mut txn = env.write_txn().unwrap();

                    for i in 0..100 {
                        db.put(&mut txn, (counter + i).to_be_bytes().to_vec(), value.clone())
                            .unwrap();
                    }

                    txn.commit().unwrap();
                    counter += 100;
                });
            },
        );
    }

    group.finish();
}

fn bench_concurrent_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_operations");
    group.sample_size(10);

    // Reader scalability
    for &num_readers in &[1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("reader_scalability", num_readers),
            &num_readers,
            |b, &num_readers| {
                let dir = TempDir::new().unwrap();
                let env = Arc::new(
                    EnvBuilder::new()
                        .map_size(100 * 1024 * 1024)
                        .max_readers(128)
                        .open(dir.path())
                        .unwrap(),
                );
                let db = {
                    let mut txn = env.write_txn().unwrap();
                    let db: Database<Vec<u8>, Vec<u8>> =
                        env.create_database(&mut txn, None).unwrap();

                    for i in 0..MEDIUM_DATASET {
                        db.put(&mut txn, i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec())
                            .unwrap();
                    }

                    txn.commit().unwrap();
                    db
                };

                b.iter(|| {
                    let handles: Vec<_> = (0..num_readers)
                        .map(|thread_id| {
                            let env = env.clone();
                            let db = db.clone();

                            thread::spawn(move || {
                                let txn = env.read_txn().unwrap();
                                let mut sum = 0u64;

                                // Each reader does different work to avoid cache effects
                                let start = (thread_id * 1000) % MEDIUM_DATASET;
                                for i in 0..1000u32 {
                                    let key = ((start + i as usize) % MEDIUM_DATASET) as u32;
                                    if let Some(val) =
                                        db.get(&txn, &key.to_be_bytes().to_vec()).unwrap()
                                    {
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
            },
        );
    }

    // Write contention
    group.bench_function("write_contention", |b| {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(EnvBuilder::new().map_size(100 * 1024 * 1024).open(dir.path()).unwrap());
        let db = {
            let mut txn = env.write_txn().unwrap();
            let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
            txn.commit().unwrap();
            db
        };

        let counter = Arc::new(std::sync::atomic::AtomicU32::new(0));

        b.iter(|| {
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    let env = env.clone();
                    let db = db.clone();
                    let counter = counter.clone();

                    thread::spawn(move || {
                        for _ in 0..5 {
                            let mut txn = env.write_txn().unwrap();
                            let key = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            db.put(
                                &mut txn,
                                key.to_be_bytes().to_vec(),
                                key.to_be_bytes().to_vec(),
                            )
                            .unwrap();
                            txn.commit().unwrap();
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        });
    });

    group.finish();
}

fn bench_memory_efficiency(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_efficiency");

    // Small values - should be inline
    group.bench_function("inline_value_storage", |b| {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(EnvBuilder::new().map_size(100 * 1024 * 1024).open(dir.path()).unwrap());
        let db = {
            let mut txn = env.write_txn().unwrap();
            let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
            txn.commit().unwrap();
            db
        };

        let value = vec![0xAB; SMALL_VALUE];
        let mut counter = 0u32;

        b.iter(|| {
            let mut txn = env.write_txn().unwrap();

            for i in 0..100u32 {
                db.put(&mut txn, (counter + i).to_be_bytes().to_vec(), value.clone()).unwrap();
            }

            txn.commit().unwrap();
            counter += 100;
        });
    });

    // Values at inline threshold
    group.bench_function("inline_threshold_values", |b| {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(EnvBuilder::new().map_size(100 * 1024 * 1024).open(dir.path()).unwrap());
        let db = {
            let mut txn = env.write_txn().unwrap();
            let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
            txn.commit().unwrap();
            db
        };

        // Test values just below and at the inline limit
        let values = [vec![0xAA; INLINE_LIMIT - 100],
            vec![0xBB; INLINE_LIMIT - 1],
            vec![0xCC; INLINE_LIMIT]];

        let mut counter = 0u32;

        b.iter(|| {
            let mut txn = env.write_txn().unwrap();

            for (i, value) in values.iter().enumerate() {
                db.put(&mut txn, (counter + i as u32).to_be_bytes().to_vec(), value.clone())
                    .unwrap();
            }

            txn.commit().unwrap();
            counter += 10;
        });
    });

    group.finish();
}

fn bench_special_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("special_patterns");

    // Append-only workload
    group.bench_function("append_only", |b| {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(EnvBuilder::new().map_size(100 * 1024 * 1024).open(dir.path()).unwrap());
        let db = {
            let mut txn = env.write_txn().unwrap();
            let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
            txn.commit().unwrap();
            db
        };

        let value = vec![0u8; MEDIUM_VALUE];
        let timestamp = Arc::new(std::sync::atomic::AtomicU64::new(0));

        b.iter(|| {
            let mut txn = env.write_txn().unwrap();

            for _ in 0..100 {
                let ts = timestamp.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                db.put(&mut txn, ts.to_be_bytes().to_vec(), value.clone()).unwrap();
            }

            txn.commit().unwrap();
        });
    });

    // Update-heavy workload
    group.bench_function("update_heavy", |b| {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(EnvBuilder::new().map_size(100 * 1024 * 1024).open(dir.path()).unwrap());
        let db = {
            let mut txn = env.write_txn().unwrap();
            let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();

            // Pre-populate
            let value = vec![0u8; MEDIUM_VALUE];
            for i in 0..100u32 {
                db.put(&mut txn, i.to_be_bytes().to_vec(), value.clone()).unwrap();
            }

            txn.commit().unwrap();
            db
        };

        let mut rng = StdRng::seed_from_u64(42);

        b.iter(|| {
            let mut txn = env.write_txn().unwrap();

            for _ in 0..50 {
                let key = rng.gen_range(0..100u32);
                let value = vec![rng.gen::<u8>(); MEDIUM_VALUE];
                db.put(&mut txn, key.to_be_bytes().to_vec(), value).unwrap();
            }

            txn.commit().unwrap();
        });
    });

    // Skewed access pattern (80/20 rule)
    group.bench_function("skewed_access", |b| {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(EnvBuilder::new().map_size(100 * 1024 * 1024).open(dir.path()).unwrap());
        let db = {
            let mut txn = env.write_txn().unwrap();
            let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();

            // Pre-populate
            for i in 0..SMALL_DATASET {
                db.put(&mut txn, i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec()).unwrap();
            }

            txn.commit().unwrap();
            db
        };

        let mut rng = StdRng::seed_from_u64(42);

        b.iter(|| {
            let txn = env.read_txn().unwrap();
            let mut sum = 0u64;

            for _ in 0..100 {
                // 80% of accesses to 20% of keys
                let key = if rng.gen_bool(0.8) {
                    rng.gen_range(0..SMALL_DATASET / 5) as u32
                } else {
                    rng.gen_range(0..SMALL_DATASET) as u32
                };

                if let Some(val) = db.get(&txn, &key.to_be_bytes().to_vec()).unwrap() {
                    sum += val.len() as u64;
                }
            }

            sum
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_btree_operations,
    bench_page_allocation,
    bench_overflow_handling,
    bench_cursor_operations,
    bench_transaction_overhead,
    bench_durability_modes,
    bench_concurrent_operations,
    bench_memory_efficiency,
    bench_special_patterns
);

criterion_main!(benches);

