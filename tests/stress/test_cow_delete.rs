//! Test that COW works correctly for delete operations

use std::sync::Arc;
use zerodb::{db::Database, env::EnvBuilder, error::Result};

#[test]
fn test_cow_delete_operations() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(100 * 1024 * 1024)
            .use_segregated_freelist(true)
            .open(dir.path())?,
    );
    
    // Create database
    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Insert and delete entries
    {
        let mut txn = env.write_txn()?;
        
        // Insert 20 entries with small values
        for i in 0..20 {
            let key = format!("key_{:08}", i).into_bytes();
            let value = vec![42u8; 50]; // Small values
            db.put(&mut txn, key, value)?;
        }
        
        // Delete every other entry
        for i in (0..20).step_by(2) {
            let key = format!("key_{:08}", i).into_bytes();
            let deleted = db.delete(&mut txn, &key)?;
            assert!(deleted, "Key_{:08} should have been deleted", i);
        }
        
        txn.commit()?;
    }
    
    // Verify remaining entries
    {
        let txn = env.read_txn()?;
        let mut count = 0;
        let mut cursor = db.cursor(&txn)?;
        
        if let Some((key, _value)) = cursor.first()? {
            count += 1;
            let key_str = String::from_utf8_lossy(&key);
            assert!(key_str.ends_with('1') || key_str.ends_with('3') || 
                    key_str.ends_with('5') || key_str.ends_with('7') || 
                    key_str.ends_with('9'), "Found unexpected key: {}", key_str);
            
            while let Some((key, _value)) = cursor.next_raw()? {
                count += 1;
                let key_str = String::from_utf8_lossy(&key);
                assert!(key_str.ends_with('1') || key_str.ends_with('3') || 
                        key_str.ends_with('5') || key_str.ends_with('7') || 
                        key_str.ends_with('9'), "Found unexpected key: {}", key_str);
            }
        }
        
        assert_eq!(count, 10, "Expected 10 entries, found {}", count);
    }
    
    Ok(())
}

#[test]
fn test_cow_delete_with_concurrent_reader() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(100 * 1024 * 1024)
            .open(dir.path())?,
    );
    
    let db: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Insert initial data
    {
        let mut txn = env.write_txn()?;
        for i in 0..10 {
            db.put(&mut txn, format!("key{}", i), format!("value{}", i))?;
        }
        txn.commit()?;
    }
    
    // Start a read transaction
    let read_txn = env.read_txn()?;
    
    // Count entries before deletion
    let mut before_count = 0;
    let mut cursor = db.cursor(&read_txn)?;
    if cursor.first()?.is_some() {
        before_count = 1;
        while cursor.next_raw()?.is_some() {
            before_count += 1;
        }
    }
    assert_eq!(before_count, 10);
    
    // Delete some entries in a new transaction
    {
        let mut txn = env.write_txn()?;
        for i in [2, 4, 6, 8] {
            db.delete(&mut txn, &format!("key{}", i))?;
        }
        txn.commit()?;
    }
    
    // The old read transaction should still see all entries
    let mut old_count = 0;
    let mut cursor = db.cursor(&read_txn)?;
    if cursor.first()?.is_some() {
        old_count = 1;
        while cursor.next_raw()?.is_some() {
            old_count += 1;
        }
    }
    assert_eq!(old_count, 10, "COW: old reader should still see all entries");
    
    // New read transaction should see fewer entries
    let new_txn = env.read_txn()?;
    let mut new_count = 0;
    let mut cursor = db.cursor(&new_txn)?;
    if cursor.first()?.is_some() {
        new_count = 1;
        while cursor.next_raw()?.is_some() {
            new_count += 1;
        }
    }
    assert_eq!(new_count, 6, "New reader should see 6 entries after deletion");
    
    Ok(())
}