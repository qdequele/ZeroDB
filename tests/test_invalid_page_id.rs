//! Test to reproduce InvalidPageId error during sequential writes

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{db::Database, EnvBuilder};

#[test]
fn test_reproduce_invalid_page_id_error() {
    // Test parameters from the failing benchmark
    const DATASET_SIZE: usize = 100_000;
    const VALUE_SIZE: usize = 100_000;
    
    println!("Starting test with {} keys, {} bytes per value", DATASET_SIZE, VALUE_SIZE);
    
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(12 * 1024 * 1024 * 1024) // 12GB map size to accommodate 100K * 100KB = ~10GB
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
    
    // Try to insert sequential keys with large values
    let value = vec![0u8; VALUE_SIZE];
    let mut txn = env.write_txn().unwrap();
    
    for i in 0..DATASET_SIZE {
        if i % 1000 == 0 {
            println!("Progress: {} / {} keys inserted", i, DATASET_SIZE);
        }
        
        let key = format!("key_{:08}", i).into_bytes();
        match db.put(&mut txn, key.clone(), value.clone()) {
            Ok(_) => {
                // Success
            }
            Err(e) => {
                eprintln!("Error at key {}: {:?}", i, e);
                panic!("Failed to insert at key {}: {:?}", i, e);
            }
        }
    }
    
    println!("Committing transaction...");
    txn.commit().unwrap();
    println!("Test completed successfully!");
}

#[test]
fn test_gradual_insert_with_large_values() {
    // Test with smaller batches to see if the error occurs at a specific point
    const BATCH_SIZE: usize = 5000;
    const VALUE_SIZE: usize = 100_000;
    const TOTAL_KEYS: usize = 25000; // Around where the error occurred
    
    println!("Testing gradual inserts with batch size {} and value size {}", BATCH_SIZE, VALUE_SIZE);
    
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(3 * 1024 * 1024 * 1024) // 3GB map size to accommodate the test data
            .open(dir.path())
            .unwrap(),
    );
    
    let db = {
        let mut txn = env.write_txn().unwrap();
        let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
        txn.commit().unwrap();
        db
    };
    
    let value = vec![0u8; VALUE_SIZE];
    let mut key_count = 0;
    
    for batch in 0..(TOTAL_KEYS / BATCH_SIZE) {
        println!("Starting batch {} ({} - {} keys)", batch, key_count, key_count + BATCH_SIZE);
        
        let mut txn = env.write_txn().unwrap();
        
        for _i in 0..BATCH_SIZE {
            let key = format!("key_{:08}", key_count).into_bytes();
            
            match db.put(&mut txn, key.clone(), value.clone()) {
                Ok(_) => {
                    key_count += 1;
                }
                Err(e) => {
                    eprintln!("Error at key {}: {:?}", key_count, e);
                    panic!("Failed to insert at key {}: {:?}", key_count, e);
                }
            }
        }
        
        println!("Committing batch {}...", batch);
        match txn.commit() {
            Ok(_) => println!("Batch {} committed successfully", batch),
            Err(e) => {
                eprintln!("Failed to commit batch {}: {:?}", batch, e);
                panic!("Transaction commit failed: {:?}", e);
            }
        }
    }
    
    println!("Test completed successfully with {} keys inserted!", key_count);
}

#[test] 
fn test_exact_failure_point() {
    // Test specifically around key 20976 where the error occurred
    const FAILURE_KEY: usize = 20976;
    const START_KEY: usize = 20970;
    const END_KEY: usize = 20980;
    const VALUE_SIZE: usize = 100_000;
    
    println!("Testing around failure key {} with value size {}", FAILURE_KEY, VALUE_SIZE);
    
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(3 * 1024 * 1024 * 1024) // 3GB map size to accommodate the test data
            .open(dir.path())
            .unwrap(),
    );
    
    let db = {
        let mut txn = env.write_txn().unwrap();
        let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
        txn.commit().unwrap();
        db
    };
    
    // Insert all keys in a single transaction like the benchmark
    println!("Inserting {} keys with value size {} bytes...", FAILURE_KEY + 5, VALUE_SIZE);
    let value = vec![0u8; VALUE_SIZE];
    let mut txn = env.write_txn().unwrap();
    
    for i in 0..(FAILURE_KEY + 5) {
        if i % 1000 == 0 {
            println!("Progress: {} keys inserted", i);
        }
        
        // More detailed logging around the failure point
        if i >= START_KEY && i <= END_KEY {
            println!("Inserting key {} (PageId calculation: key_num * value_size / page_size = {} * {} / 4096 = ~{})", 
                i, i, VALUE_SIZE, (i * VALUE_SIZE) / 4096);
        }
        
        let key = format!("key_{:08}", i).into_bytes();
        match db.put(&mut txn, key.clone(), value.clone()) {
            Ok(_) => {
                if i >= START_KEY && i <= END_KEY {
                    println!("✓ Successfully inserted key {}", i);
                }
            }
            Err(e) => {
                eprintln!("\n❌ Error at key {}: {:?}", i, e);
                eprintln!("PageId in error: {:?}", match &e {
                    zerodb::error::Error::InvalidPageId(pid) => Some(pid),
                    _ => None,
                });
                
                // Calculate approximate page usage
                let approx_pages_used = (i * VALUE_SIZE) / 4096;
                eprintln!("Approximate pages used so far: {}", approx_pages_used);
                eprintln!("PageId 524802 in hex: 0x{:X}", 524802);
                
                panic!("Failed to insert at key {}: {:?}", i, e);
            }
        }
    }
    
    println!("\nAll keys inserted successfully! Attempting to commit transaction...");
    match txn.commit() {
        Ok(_) => println!("✓ Transaction committed successfully!"),
        Err(e) => panic!("Failed to commit transaction: {:?}", e),
    }
}