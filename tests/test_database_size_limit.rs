//! Test database size limit enforcement

use zerodb::env::{EnvBuilder, DurabilityMode};
use zerodb::db::Database;
use zerodb::error::Error;
use tempfile::TempDir;
use std::sync::Arc;

#[test]
fn test_map_size_limit_enforcement() {
    let dir = TempDir::new().unwrap();
    
    // Create environment with a small map size (2MB)
    let env = Arc::new(EnvBuilder::new()
        .map_size(2 * 1024 * 1024) // 2MB map size
        .durability(DurabilityMode::NoSync)
        .open(dir.path())
        .unwrap());
    
    // Create a database
    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.write_txn().unwrap();
        let db = env.create_database(&mut txn, None).unwrap();
        txn.commit().unwrap();
        db
    };
    
    // Write data until we hit the map size limit
    let value = vec![0u8; 1024]; // 1KB value
    let mut key_counter = 0u64;
    let mut hit_limit = false;
    
    loop {
        let mut txn = env.write_txn().unwrap();
        
        // Try to insert multiple entries in one transaction
        for _ in 0..50 {
            let key = key_counter.to_be_bytes().to_vec();
            match db.put(&mut txn, key, value.clone()) {
                Ok(()) => {
                    key_counter += 1;
                }
                Err(Error::MapFull) => {
                    hit_limit = true;
                    break;
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
        
        if hit_limit {
            txn.abort();
            break;
        }
        
        // Commit the transaction
        match txn.commit() {
            Ok(()) => {}
            Err(Error::MapFull) => {
                break;
            }
            Err(e) => {
                panic!("Unexpected error during commit: {:?}", e);
            }
        }
        
        // Safety check to prevent infinite loop
        if key_counter > 5000 {
            panic!("Wrote too much data without hitting limit");
        }
    }
    
    // Successfully hit the map size limit
    println!("Successfully hit map size limit after {} entries", key_counter);
    assert!(key_counter > 0); // Should have written at least some data
}

#[test]
fn test_map_size_limit_with_overflow_pages() {
    let dir = TempDir::new().unwrap();
    
    // Create environment with a small map size (3MB)
    let env = Arc::new(EnvBuilder::new()
        .map_size(3 * 1024 * 1024) // 3MB map size
        .durability(DurabilityMode::NoSync)
        .open(dir.path())
        .unwrap());
    
    // Create a database
    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.write_txn().unwrap();
        let db = env.create_database(&mut txn, None).unwrap();
        txn.commit().unwrap();
        db
    };
    
    // Write large values that require overflow pages
    let large_value = vec![0u8; 10 * 1024]; // 10KB value (requires overflow)
    let mut key_counter = 0u64;
    
    loop {
        let mut txn = env.write_txn().unwrap();
        
        let key = key_counter.to_be_bytes().to_vec();
        match db.put(&mut txn, key, large_value.clone()) {
            Ok(()) => {
                key_counter += 1;
            }
            Err(Error::MapFull) => {
                txn.abort();
                break;
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
        
        // Commit the transaction
        match txn.commit() {
            Ok(()) => {}
            Err(Error::MapFull) => {
                break;
            }
            Err(e) => {
                panic!("Unexpected error during commit: {:?}", e);
            }
        }
        
        // Safety check
        if key_counter > 500 {
            panic!("Wrote too much data without hitting limit");
        }
    }
    
    // Successfully hit the map size limit with overflow pages
    println!("Successfully hit map size limit after {} large entries", key_counter);
    assert!(key_counter > 0); // Should have written at least some data
}

#[test]
fn test_large_map_size_allows_more_data() {
    let dir = TempDir::new().unwrap();
    
    // Create environment with a larger map size
    let env = Arc::new(EnvBuilder::new()
        .map_size(10 * 1024 * 1024) // 10MB map size
        .durability(DurabilityMode::NoSync)
        .open(dir.path())
        .unwrap());
    
    // Create a database
    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.write_txn().unwrap();
        let db = env.create_database(&mut txn, None).unwrap();
        txn.commit().unwrap();
        db
    };
    
    // Write a reasonable amount of data
    let value = vec![0u8; 100]; // Small 100 byte value
    
    // Write data in smaller batches
    for batch in 0..10 {
        let mut txn = env.write_txn().unwrap();
        
        for i in 0u64..100 {
            let key_num = batch * 100 + i;
            let key = key_num.to_be_bytes().to_vec();
            db.put(&mut txn, key, value.clone()).unwrap();
        }
        
        txn.commit().unwrap();
    }
    
    // Verify data
    let txn = env.read_txn().unwrap();
    for i in 0u64..1000 {
        let key = i.to_be_bytes().to_vec();
        assert_eq!(db.get(&txn, &key).unwrap(), Some(value.clone()));
    }
}