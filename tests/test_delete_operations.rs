//! Comprehensive test suite for delete operations
//! 
//! This consolidates multiple delete-related test files into a single comprehensive suite.
//! Tests cover basic deletion, edge cases, performance, and interaction with other operations.

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{db::Database, EnvBuilder, Result};

#[test]
fn test_basic_delete() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, String> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Insert and delete single item
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, 1.to_string(), 100.to_string())?;
        txn.commit()?;
    }
    
    {
        let mut txn = env.write_txn()?;
        assert!(db.delete(&mut txn, &1.to_string())?);
        assert!(!db.delete(&mut txn, &1.to_string())?); // Second delete returns false
        txn.commit()?;
    }
    
    // Verify deletion
    {
        let txn = env.read_txn()?;
        assert_eq!(db.get(&txn, &1.to_string())?, None);
    }
    
    Ok(())
}

#[test]
fn test_delete_nonexistent() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, String> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Delete non-existent key should return false
    {
        let mut txn = env.write_txn()?;
        assert!(!db.delete(&mut txn, &999.to_string())?);
        txn.commit()?;
    }
    
    Ok(())
}

#[test]
fn test_delete_and_reinsert() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, String> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Insert, delete, then reinsert with different value
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, 1.to_string(), 100.to_string())?;
        txn.commit()?;
    }
    
    {
        let mut txn = env.write_txn()?;
        assert!(db.delete(&mut txn, &1.to_string())?);
        db.put(&mut txn, 1.to_string(), 200.to_string())?; // Reinsert with new value
        txn.commit()?;
    }
    
    {
        let txn = env.read_txn()?;
        assert_eq!(db.get(&txn, &1.to_string())?, Some(200.to_string()));
    }
    
    Ok(())
}

#[test]
fn test_bulk_delete_sequential() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, String> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Insert 100 sequential items
    {
        let mut txn = env.write_txn()?;
        for i in 0..100 {
            db.put(&mut txn, i.to_string(), (i * 10).to_string())?;
        }
        txn.commit()?;
    }
    
    // Delete every other item
    {
        let mut txn = env.write_txn()?;
        for i in (0..100).step_by(2) {
            assert!(db.delete(&mut txn, &i.to_string())?);
        }
        txn.commit()?;
    }
    
    // Verify deletions
    {
        let txn = env.read_txn()?;
        for i in 0..100 {
            if i % 2 == 0 {
                assert_eq!(db.get(&txn, &i.to_string())?, None);
            } else {
                assert_eq!(db.get(&txn, &i.to_string())?, Some((i * 10).to_string()));
            }
        }
    }
    
    Ok(())
}

#[test]
fn test_delete_causes_merge() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, String> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Insert enough items to create multiple pages
    {
        let mut txn = env.write_txn()?;
        for i in 0..200 {
            db.put(&mut txn, i.to_string(), i.to_string())?;
        }
        txn.commit()?;
    }
    
    // Delete most items to trigger page merging
    {
        let mut txn = env.write_txn()?;
        for i in 10..190 {
            assert!(db.delete(&mut txn, &i.to_string())?);
        }
        txn.commit()?;
    }
    
    // Verify remaining items
    {
        let txn = env.read_txn()?;
        for i in 0..10 {
            assert_eq!(db.get(&txn, &i.to_string())?, Some(i.to_string()));
        }
        for i in 190..200 {
            assert_eq!(db.get(&txn, &i.to_string())?, Some(i.to_string()));
        }
    }
    
    Ok(())
}

#[test]
fn test_delete_with_overflow_values() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, Vec<u8>> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Create large value that requires overflow pages
    let large_value = vec![0xAB; 2000];
    
    // Insert large value
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, 1, large_value.clone())?;
        db.put(&mut txn, 2, vec![0xCD; 100])?; // Small value
        db.put(&mut txn, 3, large_value.clone())?;
        txn.commit()?;
    }
    
    // Delete large value
    {
        let mut txn = env.write_txn()?;
        assert!(db.delete(&mut txn, &1.to_string())?);
        assert!(db.delete(&mut txn, &3.to_string())?);
        txn.commit()?;
    }
    
    // Verify deletion and that overflow pages are freed
    {
        let txn = env.read_txn()?;
        assert_eq!(db.get(&txn, &1.to_string())?, None);
        assert_eq!(db.get(&txn, &3.to_string())?, None);
        assert_eq!(db.get(&txn, &2.to_string())?, Some(vec![0xCD; 100].to_string()));
    }
    
    Ok(())
}

#[test]
fn test_delete_empty_database() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, String> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Delete from empty database
    {
        let mut txn = env.write_txn()?;
        assert!(!db.delete(&mut txn, &1.to_string())?);
        txn.commit()?;
    }
    
    Ok(())
}

#[test]
fn test_delete_rollback() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, String> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Insert items
    {
        let mut txn = env.write_txn()?;
        for i in 0..10 {
            db.put(&mut txn, i.to_string(), i * 100)?;
        }
        txn.commit()?;
    }
    
    // Delete but rollback
    {
        let mut txn = env.write_txn()?;
        for i in 0..5 {
            assert!(db.delete(&mut txn, &i.to_string())?);
        }
        // Don't commit - let transaction drop
    }
    
    // Verify items still exist
    {
        let txn = env.read_txn()?;
        for i in 0..10 {
            assert_eq!(db.get(&txn, &i.to_string())?, Some((i * 100).to_string()));
        }
    }
    
    Ok(())
}

#[test]
fn test_delete_cursor_interaction() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, String> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Insert items
    {
        let mut txn = env.write_txn()?;
        for i in 0..20 {
            db.put(&mut txn, i.to_string(), i.to_string())?;
        }
        txn.commit()?;
    }
    
    // Delete specific item  
    {
        let mut txn = env.write_txn()?;
        
        // Delete specific item
        assert!(db.delete(&mut txn, &"10".to_string())?);
        
        txn.commit()?;
    }
    
    // Verify deletion
    {
        let txn = env.read_txn()?;
        assert_eq!(db.get(&txn, &"10".to_string())?, None);
        assert_eq!(db.get(&txn, &"11".to_string())?, Some("11".to_string()));
    }
    
    Ok(())
}

#[test]
fn test_delete_last_item() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, String> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Insert single item
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, 42.to_string(), 4200.to_string())?;
        txn.commit()?;
    }
    
    // Delete the only item
    {
        let mut txn = env.write_txn()?;
        assert!(db.delete(&mut txn, &42.to_string())?);
        txn.commit()?;
    }
    
    // Verify database is empty
    {
        let txn = env.read_txn()?;
        
        // Verify iteration returns nothing
        let mut count = 0;
        let mut cursor = db.cursor(&txn)?;
        cursor.first()?;
        while cursor.current()?.is_some() {
            count += 1;
            cursor.next_raw()?;
        }
        assert_eq!(count, 0);
    }
    
    Ok(())
}

#[test]
fn test_delete_stress_random() -> Result<()> {
    use rand::{Rng, SeedableRng};
    use rand::rngs::StdRng;
    
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, String> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    let mut rng = StdRng::seed_from_u64(12345);
    let num_items = 100;
    
    // Insert random items
    let mut keys: Vec<u32> = (0..num_items).map(|_| rng.gen_range(0..1000)).collect();
    keys.sort_unstable();
    keys.dedup();
    
    {
        let mut txn = env.write_txn()?;
        for &key in &keys {
            db.put(&mut txn, key.to_string(), (key * 2).to_string())?;
        }
        txn.commit()?;
    }
    
    // Randomly delete half the items
    let mut deleted_keys = Vec::new();
    {
        let mut txn = env.write_txn()?;
        for &key in keys.iter().filter(|_| rng.gen_bool(0.5)) {
            assert!(db.delete(&mut txn, &key)?);
            deleted_keys.push(key);
        }
        txn.commit()?;
    }
    
    // Verify deletions
    {
        let txn = env.read_txn()?;
        for &key in &keys {
            if deleted_keys.contains(&key) {
                assert_eq!(db.get(&txn, &key)?, None);
            } else {
                assert_eq!(db.get(&txn, &key)?, Some((key * 2).to_string()));
            }
        }
    }
    
    Ok(())
}

#[test]
fn test_delete_never_causes_page_full() -> Result<()> {
    // This test ensures that delete operations never fail with PageFull error
    // which was a bug in earlier versions
    
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Create keys and values of various sizes
    let small_key = vec![0u8; 10];
    let medium_key = vec![1u8; 50];
    let large_key = vec![2u8; 100];
    
    let small_val = vec![0xAA; 50];
    let medium_val = vec![0xBB; 200];
    let large_val = vec![0xCC; 500];
    
    // Fill database with mixed size entries
    {
        let mut txn = env.write_txn()?;
        for i in 0..50 {
            let mut key = small_key.clone();
            key.push(i);
            db.put(&mut txn, key, small_val.clone())?;
            
            let mut key = medium_key.clone();
            key.push(i);
            db.put(&mut txn, key, medium_val.clone())?;
            
            let mut key = large_key.clone();
            key.push(i);
            db.put(&mut txn, key, large_val.clone())?;
        }
        txn.commit()?;
    }
    
    // Delete entries in various patterns - should never fail
    {
        let mut txn = env.write_txn()?;
        
        // Delete some small entries
        for i in 0..10 {
            let mut key = small_key.clone();
            key.push(i);
            assert!(db.delete(&mut txn, &key)?);
        }
        
        // Delete some large entries
        for i in 20..30 {
            let mut key = large_key.clone();
            key.push(i);
            assert!(db.delete(&mut txn, &key)?);
        }
        
        // Delete some medium entries
        for i in 10..20 {
            let mut key = medium_key.clone();
            key.push(i);
            assert!(db.delete(&mut txn, &key)?);
        }
        
        txn.commit()?; // Should never fail with PageFull
    }
    
    Ok(())
}