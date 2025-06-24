//! Test zero-copy read performance

use std::sync::Arc;
use std::time::Instant;
use zerodb::db::Database;
use zerodb::EnvBuilder;

#[test]
fn test_zero_copy_read_performance() -> Result<(), Box<dyn std::error::Error>> {
    // Create temporary directory
    let dir = tempfile::tempdir()?;
    let env = Arc::new(
        EnvBuilder::new()
            .max_dbs(10)
            .map_size(100 * 1024 * 1024) // 100MB
            .open(dir.path())?,
    );

    // Create database and insert test data
    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut wtxn = env.write_txn()?;
        let db = env.create_database(&mut wtxn, None)?;

        // Insert test data
        for i in 0..10000 {
            let key = format!("key_{:06}", i).into_bytes();
            let value = format!("value_{:06}", i).into_bytes();
            db.put(&mut wtxn, key, value)?;
        }

        wtxn.commit()?;
        db
    };

    // Test read performance
    let rtxn = env.read_txn()?;

    // Warm up
    for i in 0..100 {
        let key = format!("key_{:06}", i).into_bytes();
        let _ = db.get(&rtxn, &key)?;
    }

    // Measure read performance
    let iterations = 100000;
    let start = Instant::now();

    for i in 0..iterations {
        let key_idx = i % 10000;
        let key = format!("key_{:06}", key_idx).into_bytes();
        let value = db.get(&rtxn, &key)?;

        assert!(value.is_some(), "Key not found: {:?}", key);
    }

    let elapsed = start.elapsed();
    let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();
    let ns_per_op = elapsed.as_nanos() as f64 / iterations as f64;

    // Performance assertions - should be fast
    assert!(ops_per_sec > 100_000.0, "Expected >100k ops/sec, got {:.0}", ops_per_sec);
    assert!(ns_per_op < 10_000.0, "Expected <10µs per op, got {:.0}ns", ns_per_op);

    Ok(())
}

#[test]
fn test_zero_copy_large_values() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(500 * 1024 * 1024) // 500MB
            .open(dir.path())?,
    );

    let db: Database<String, Vec<u8>> = {
        let mut wtxn = env.write_txn()?;
        let db = env.create_database(&mut wtxn, None)?;

        // Insert large values
        for i in 0..100 {
            let key = format!("large_{:03}", i);
            let value = vec![i as u8; 100_000]; // 100KB values
            db.put(&mut wtxn, key, value)?;
        }

        wtxn.commit()?;
        db
    };

    // Test reading large values
    let rtxn = env.read_txn()?;
    let start = Instant::now();

    for i in 0..1000 {
        let key_idx = i % 100;
        let key = format!("large_{:03}", key_idx);
        let value = db.get(&rtxn, &key)?;
        
        assert!(value.is_some());
        assert_eq!(value.unwrap().len(), 100_000);
    }

    let elapsed = start.elapsed();
    
    // Should handle large values efficiently
    assert!(elapsed.as_secs() < 2, "Large value reads took too long: {:?}", elapsed);

    Ok(())
}

#[test]
fn test_zero_copy_cursor_iteration() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(100 * 1024 * 1024)
            .open(dir.path())?,
    );

    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut wtxn = env.write_txn()?;
        let db = env.create_database(&mut wtxn, None)?;

        // Insert sequential data
        for i in 0..10000 {
            let key = format!("seq_{:06}", i).into_bytes();
            let value = format!("data_{:06}", i).into_bytes();
            db.put(&mut wtxn, key, value)?;
        }

        wtxn.commit()?;
        db
    };

    // Test cursor iteration performance
    let rtxn = env.read_txn()?;
    let mut cursor = db.cursor(&rtxn)?;
    
    let start = Instant::now();
    let mut count = 0;

    cursor.first()?;
    count += 1;
    
    while cursor.next_raw()?.is_some() {
        count += 1;
    }

    let elapsed = start.elapsed();

    assert_eq!(count, 10000);
    
    // Cursor iteration should be fast
    let ms_per_10k = elapsed.as_millis();
    assert!(ms_per_10k < 100, "Cursor iteration took {}ms for 10k entries", ms_per_10k);

    Ok(())
}