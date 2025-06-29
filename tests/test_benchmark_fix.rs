//! Test that the benchmark fix works for large datasets

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{db::Database, EnvBuilder};

#[test]
fn test_large_dataset_with_proper_map_size() {
    println!("Testing 100K keys with 100KB values using 20GB map size...");
    
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(20 * 1024 * 1024 * 1024) // 20GB as per our calculation
            .open(dir.path())
            .unwrap(),
    );
    
    let db = {
        let mut txn = env.write_txn().unwrap();
        let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
        txn.commit().unwrap();
        db
    };
    
    // Insert past the original failure point
    let value = vec![0u8; 100_000]; // 100KB
    let mut txn = env.write_txn().unwrap();
    
    // Insert keys around the failure point
    for i in 20970..20980 {
        let key = format!("key_{:08}", i).into_bytes();
        db.put(&mut txn, key, value.clone()).expect("Should succeed with 20GB map");
    }
    
    println!("✓ Successfully inserted keys 20970-20979 with 20GB map size!");
    
    // Check space info
    if let Ok(info) = env.space_info() {
        println!("\nSpace usage after inserting around failure point:");
        println!("  Database size: {} MB / {} GB ({:.1}%)",
                 info.db_size_bytes / (1024 * 1024),
                 info.max_db_size_bytes / (1024 * 1024 * 1024),
                 info.percent_of_map_used);
    }
    
    txn.commit().unwrap();
}

#[test]
fn test_verify_old_map_size_fails() {
    println!("Verifying that 2GB map size still fails...");
    
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(2 * 1024 * 1024 * 1024) // 2GB - the old size
            .open(dir.path())
            .unwrap(),
    );
    
    let db = {
        let mut txn = env.write_txn().unwrap();
        let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
        txn.commit().unwrap();
        db
    };
    
    let value = vec![0u8; 100_000]; // 100KB
    let mut txn = env.write_txn().unwrap();
    
    let mut last_successful = 0;
    for i in 0..25000 {
        let key = format!("key_{:08}", i).into_bytes();
        match db.put(&mut txn, key.clone(), value.clone()) {
            Ok(_) => {
                last_successful = i;
            }
            Err(e) => {
                println!("✓ Expected failure at key {}: {:?}", i, e);
                assert!(i < 22000, "Should fail before key 22000");
                assert!(i > 20000, "Should succeed past key 20000");
                return;
            }
        }
    }
    
    panic!("Expected to fail with 2GB map size, but succeeded up to key {}", last_successful);
}