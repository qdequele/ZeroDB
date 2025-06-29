//! Test space monitoring and estimation utilities

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{db::Database, space_info::MapSizeEstimator, EnvBuilder};

#[test]
fn test_space_monitoring_during_inserts() {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(100 * 1024 * 1024) // 100MB for testing
            .open(dir.path())
            .unwrap(),
    );
    
    // Create database
    let db = {
        let mut txn = env.write_txn().unwrap();
        let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
        txn.commit().unwrap();
        db
    };
    
    // Check initial space
    let initial_info = env.space_info().unwrap();
    println!("Initial state:\n{}", initial_info);
    assert!(initial_info.percent_of_map_used < 1.0);
    
    // Insert data and monitor space usage
    let value_size = 10_000; // 10KB values
    let value = vec![0u8; value_size];
    
    for batch in 0..5 {
        let mut txn = env.write_txn().unwrap();
        
        // Insert 100 entries per batch
        for i in 0..100 {
            let key_num = batch * 100 + i;
            let key = format!("key_{:08}", key_num).into_bytes();
            db.put(&mut txn, key, value.clone()).unwrap();
        }
        
        txn.commit().unwrap();
        
        // Check space after each batch
        let info = env.space_info().unwrap();
        println!("\nAfter batch {} ({} total entries):", batch, (batch + 1) * 100);
        println!("  Used pages: {}", info.used_pages);
        println!("  Database size: {} KB", info.db_size_bytes / 1024);
        println!("  Map usage: {:.1}%", info.percent_of_map_used);
        println!("  Pages remaining: {}", info.pages_remaining());
        
        // Check if we're approaching capacity
        if info.is_near_capacity(80.0) {
            println!("  WARNING: Approaching capacity!");
        }
    }
}

#[test]
fn test_map_size_estimation() {
    println!("\n=== Map Size Estimation Examples ===\n");
    
    // Example 1: Small entries
    let estimator = MapSizeEstimator::new(1_000_000, 32, 100);
    println!("Example 1: 1M small entries (32B key, 100B value)");
    println!("{}\n", estimator.breakdown());
    
    // Example 2: Medium entries
    let estimator = MapSizeEstimator::new(100_000, 64, 1024);
    println!("Example 2: 100K medium entries (64B key, 1KB value)");
    println!("{}\n", estimator.breakdown());
    
    // Example 3: Large entries (the benchmark case)
    let estimator = MapSizeEstimator::new(100_000, 16, 100_000);
    println!("Example 3: 100K large entries (16B key, 100KB value)");
    println!("{}\n", estimator.breakdown());
    
    // Example 4: Mixed workload
    let estimator = MapSizeEstimator {
        num_entries: 50_000,
        avg_key_size: 128,
        avg_value_size: 10_000,
        btree_overhead_factor: 2.0, // Higher overhead for mixed workload
        metadata_overhead_pages: 2000,
        safety_margin: 1.5, // 50% safety margin
    };
    println!("Example 4: 50K mixed entries with custom parameters");
    println!("{}\n", estimator.breakdown());
}

#[test]
fn test_estimate_entries_remaining() {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(50 * 1024 * 1024) // 50MB
            .open(dir.path())
            .unwrap(),
    );
    
    let info = env.space_info().unwrap();
    
    // Test different entry sizes
    let test_sizes = vec![
        ("Small (100B)", 100),
        ("Medium (1KB)", 1024),
        ("Large (10KB)", 10 * 1024),
        ("Very Large (100KB)", 100 * 1024),
    ];
    
    println!("\nEstimated entries remaining for 50MB database:");
    for (name, size) in test_sizes {
        let remaining = info.estimate_entries_remaining(size);
        println!("  {}: {} entries", name, remaining);
    }
}

#[test]
fn test_space_info_near_limit() {
    // Test the exact scenario from the bug
    let dir = TempDir::new().unwrap();
    let map_size = 2 * 1024 * 1024 * 1024; // 2GB like in benchmark
    
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(map_size)
            .open(dir.path())
            .unwrap(),
    );
    
    // Create database
    let db = {
        let mut txn = env.write_txn().unwrap();
        let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
        txn.commit().unwrap();
        db
    };
    
    // Insert until we approach the limit
    let value_size = 100_000; // 100KB values
    let value = vec![0u8; value_size];
    let mut txn = env.write_txn().unwrap();
    
    println!("\nInserting 100KB values into 2GB database:");
    
    for i in 0..30000 {
        if i % 1000 == 0 {
            // Check space periodically
            if let Ok(info) = env.space_info() {
                if info.percent_of_map_used > 90.0 {
                    println!("\nStopping at key {} - database {:.1}% full", i, info.percent_of_map_used);
                    println!("Estimated entries remaining: {}", 
                             info.estimate_entries_remaining(value_size));
                    break;
                }
            }
        }
        
        let key = format!("key_{:08}", i).into_bytes();
        match db.put(&mut txn, key.clone(), value.clone()) {
            Ok(_) => {
                if i == 20970 {
                    // Check space near the failure point
                    if let Ok(info) = env.space_info() {
                        println!("\nAt key 20970 (near original failure):");
                        println!("  Database usage: {:.1}%", info.percent_of_map_used);
                        println!("  Pages remaining: {}", info.pages_remaining());
                    }
                }
            }
            Err(e) => {
                println!("\nError at key {}: {:?}", i, e);
                if let Ok(info) = env.space_info() {
                    println!("Final space info:\n{}", info);
                }
                break;
            }
        }
    }
    
    // Don't commit - just testing
}