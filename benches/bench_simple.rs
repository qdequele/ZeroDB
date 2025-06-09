//! Simple benchmark comparison between zerodb and LMDB
//!
//! This provides a quick performance comparison without full criterion setup

use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn format_duration(d: Duration) -> String {
    if d.as_secs() > 0 {
        format!("{:.2}s", d.as_secs_f64())
    } else if d.as_millis() > 0 {
        format!("{:.2}ms", d.as_millis() as f64)
    } else {
        format!("{:.2}μs", d.as_micros() as f64)
    }
}

fn bench_sequential_writes() {
    println!("\n=== Sequential Write Benchmark ===");
    println!("Writing 100,000 key-value pairs (16 byte keys, 100 byte values)");

    let data: Vec<(Vec<u8>, Vec<u8>)> = (0..100_000)
        .map(|i| {
            let key = format!("key_{:08}", i).into_bytes();
            let value = vec![i as u8; 100];
            (key, value)
        })
        .collect();

    // Benchmark zerodb
    {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            zerodb::env::EnvBuilder::new().map_size(100 * 1024 * 1024).open(dir.path()).unwrap(),
        );

        // Open database
        let db = zerodb::db::Database::<Vec<u8>, Vec<u8>>::open(
            &env,
            None,
            zerodb::db::DatabaseFlags::CREATE,
        )
        .unwrap();

        let start = Instant::now();
        let mut txn = env.begin_write_txn().unwrap();

        for (key, value) in &data {
            db.put(&mut txn, key.clone(), value.clone()).unwrap();
        }

        txn.commit().unwrap();
        let duration = start.elapsed();

        println!(
            "zerodb: {} ({:.0} ops/sec)",
            format_duration(duration),
            100_000.0 / duration.as_secs_f64()
        );
    }

    // Benchmark LMDB (heed FFI)
    {
        let dir = TempDir::new().unwrap();
        let env = unsafe {
            heed::EnvOpenOptions::new().map_size(100 * 1024 * 1024).open(dir.path()).unwrap()
        };

        let start = Instant::now();
        let mut txn = env.write_txn().unwrap();
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> =
            env.create_database(&mut txn, None).unwrap();

        for (key, value) in &data {
            db.put(&mut txn, key, value).unwrap();
        }

        txn.commit().unwrap();
        let duration = start.elapsed();

        println!(
            "LMDB:   {} ({:.0} ops/sec)",
            format_duration(duration),
            100_000.0 / duration.as_secs_f64()
        );
    }
}

fn bench_random_reads() {
    println!("\n=== Random Read Benchmark ===");
    println!("Reading 1,000 random keys from 10,000 entries");

    // Prepare data
    let all_keys: Vec<Vec<u8>> =
        (0..10_000).map(|i| format!("key_{:08}", i).into_bytes()).collect();

    let read_indices: Vec<usize> = (0..1_000).map(|i| (i * 7) % 10_000).collect();

    // Setup zerodb
    let core_dir = TempDir::new().unwrap();
    let core_env = Arc::new(
        zerodb::env::EnvBuilder::new().map_size(100 * 1024 * 1024).open(core_dir.path()).unwrap(),
    );

    // Open database
    let core_db = zerodb::db::Database::<Vec<u8>, Vec<u8>>::open(
        &core_env,
        None,
        zerodb::db::DatabaseFlags::CREATE,
    )
    .unwrap();

    // Populate zerodb
    {
        let mut txn = core_env.begin_write_txn().unwrap();

        for (i, key) in all_keys.iter().enumerate() {
            let value = vec![i as u8; 100];
            core_db.put(&mut txn, key.clone(), value).unwrap();
        }

        txn.commit().unwrap();
    }

    // Setup LMDB
    let lmdb_dir = TempDir::new().unwrap();
    let lmdb_env = unsafe {
        heed::EnvOpenOptions::new().map_size(100 * 1024 * 1024).open(lmdb_dir.path()).unwrap()
    };

    // Populate LMDB
    {
        let mut txn = lmdb_env.write_txn().unwrap();
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> =
            lmdb_env.create_database(&mut txn, None).unwrap();

        for (i, key) in all_keys.iter().enumerate() {
            let value = vec![i as u8; 100];
            db.put(&mut txn, key, &value).unwrap();
        }

        txn.commit().unwrap();
    }

    // Benchmark zerodb reads
    {
        let txn = core_env.begin_txn().unwrap();

        let start = Instant::now();
        let mut found = 0;

        for &idx in &read_indices {
            if core_db.get(&txn, &all_keys[idx]).unwrap().is_some() {
                found += 1;
            }
        }

        let duration = start.elapsed();
        println!(
            "zerodb: {} ({:.0} ops/sec, {} found)",
            format_duration(duration),
            1_000.0 / duration.as_secs_f64(),
            found
        );
    }

    // Benchmark LMDB reads
    {
        let txn = lmdb_env.read_txn().unwrap();
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> =
            lmdb_env.open_database(&txn, None).unwrap().unwrap();

        let start = Instant::now();
        let mut found = 0;

        for &idx in &read_indices {
            if db.get(&txn, &all_keys[idx]).unwrap().is_some() {
                found += 1;
            }
        }

        let duration = start.elapsed();
        println!(
            "LMDB:   {} ({:.0} ops/sec, {} found)",
            format_duration(duration),
            1_000.0 / duration.as_secs_f64(),
            found
        );
    }
}

fn main() {
    println!("ZeroDB vs LMDB Benchmark");
    println!("========================");

    bench_sequential_writes();
    bench_random_reads();

    println!("\nBenchmark complete!");
}
