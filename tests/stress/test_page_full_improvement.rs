//! Test to verify page full improvements

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{EnvBuilder, Result};

#[test]
fn test_page_capacity_improvements() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(100 * 1024 * 1024) // 100MB
            .open(dir.path())?,
    );

    // Create database
    let db: zerodb::db::Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    let mut successful_inserts = 0;
    let mut txn = env.write_txn()?;
    
    // Insert entries with gradually increasing sizes
    for i in 0..200 {
        let key = format!("test_key_{:04}", i);
        let value_size = 100 + i * 20; // Start at 100 bytes, increase by 20 each time
        let value = vec![b'x'; value_size];
        
        match db.put(&mut txn, key.clone(), value) {
            Ok(_) => {
                successful_inserts += 1;
            }
            Err(_) => {
                // Expected to eventually fail as values get too large
                break;
            }
        }
    }
    
    txn.commit()?;
    
    // Should have successfully inserted many entries
    assert!(successful_inserts > 50, "Expected at least 50 successful inserts, got {}", successful_inserts);
    
    Ok(())
}

#[test]
fn test_random_inserts_page_capacity() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(100 * 1024 * 1024)
            .open(dir.path())?,
    );

    let db: zerodb::db::Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    let mut txn = env.write_txn()?;
    let mut random_success = 0;
    
    use rand::{Rng, SeedableRng};
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    
    for _ in 0..1000 {
        let key_num: u32 = rng.gen_range(10000..20000);
        let key = format!("random_{:06}", key_num);
        let value_size = rng.gen_range(50..300);
        let value = vec![b'r'; value_size];
        
        match db.put(&mut txn, key, value) {
            Ok(_) => random_success += 1,
            Err(_) => {
                // May fail due to page capacity, but should handle many inserts
                break;
            }
        }
    }
    
    txn.commit()?;
    
    // Should handle most random inserts
    assert!(random_success > 800, "Expected at least 800 successful random inserts, got {}", random_success);
    
    Ok(())
}

#[test]
fn test_mixed_operations_page_capacity() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(100 * 1024 * 1024)
            .open(dir.path())?,
    );

    let db: zerodb::db::Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Mix inserts, updates, and deletes
    let mut txn = env.write_txn()?;
    
    // Initial inserts
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = format!("value_{}", i);
        db.put(&mut txn, key, value)?;
    }
    
    // Updates (should reuse space)
    for i in 0..50 {
        let key = format!("key_{:03}", i);
        let value = format!("updated_value_{}_with_longer_content", i);
        db.put(&mut txn, key, value)?;
    }
    
    // Deletes (should free space)
    for i in (25..75).step_by(2) {
        let key = format!("key_{:03}", i);
        db.delete(&mut txn, &key)?;
    }
    
    // More inserts (should reuse freed space)
    for i in 100..150 {
        let key = format!("key_{:03}", i);
        let value = format!("new_value_{}", i);
        db.put(&mut txn, key, value)?;
    }
    
    txn.commit()?;
    
    // Verify final state
    let txn = env.read_txn()?;
    let mut count = 0;
    let mut cursor = db.cursor(&txn)?;
    if cursor.first()?.is_some() {
        count = 1;
        while cursor.next_raw()?.is_some() {
            count += 1;
        }
    }
    
    // Should have: 100 initial - 25 deleted + 50 new = 125 entries
    assert_eq!(count, 125, "Expected 125 entries after mixed operations");
    
    Ok(())
}