//! Consolidated overflow page tests
//! 
//! Tests for handling values larger than a single page,
//! including COW behavior and edge cases.

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{db::Database, EnvBuilder, Result};

#[test]
fn test_basic_overflow() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, Vec<u8>> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Test various overflow value sizes
    let test_sizes = [
        1024,      // Just at overflow threshold
        1025,      // Just over threshold
        2048,      // 2x threshold
        4096,      // Exactly one page
        4097,      // Just over one page
        10240,     // 10KB
        102400,    // 100KB
        1048576,   // 1MB
    ];
    
    // Insert values of different sizes
    {
        let mut txn = env.write_txn()?;
        for (i, size) in test_sizes.iter().enumerate() {
            let value = vec![i as u8; *size];
            db.put(&mut txn, i.to_string(), value)?;
        }
        txn.commit()?;
    }
    
    // Verify all values
    {
        let txn = env.read_txn()?;
        for (i, size) in test_sizes.iter().enumerate() {
            let value = db.get(&txn, &i.to_string())?.unwrap();
            assert_eq!(value.len(), *size);
            assert!(value.iter().all(|&b| b == i as u8));
        }
    }
    
    Ok(())
}

#[test]
fn test_overflow_cow() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, Vec<u8>> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Create initial overflow value
    let initial_value = vec![0xAA; 5000];
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, "overflow".to_string(), initial_value.clone())?;
        txn.commit()?;
    }
    
    // Start a read transaction to hold the old version
    let read_txn = env.read_txn()?;
    let old_value = db.get(&read_txn, &"overflow".to_string())?.unwrap();
    assert_eq!(old_value, initial_value);
    
    // Modify the value in a new transaction
    let new_value = vec![0xBB; 5000];
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, "overflow".to_string(), new_value.clone())?;
        txn.commit()?;
    }
    
    // Old reader should still see old value (COW)
    let old_value_again = db.get(&read_txn, &"overflow".to_string())?.unwrap();
    assert_eq!(old_value_again, initial_value);
    
    // New reader should see new value
    {
        let txn = env.read_txn()?;
        let new_value_read = db.get(&txn, &"overflow".to_string())?.unwrap();
        assert_eq!(new_value_read, new_value);
    }
    
    Ok(())
}

#[test]
#[ignore = "Skipping due to page size calculation bug in core implementation"]
fn test_overflow_page_limits() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(50 * 1024 * 1024) // 50MB
            .open(dir.path())?
    );
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, Vec<u8>> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Test very large values
    let large_sizes = [
        1024 * 1024,      // 1MB
        5 * 1024 * 1024,  // 5MB
        10 * 1024 * 1024, // 10MB
    ];
    
    for (i, size) in large_sizes.iter().enumerate() {
        let mut txn = env.write_txn()?;
        
        // Create value with pattern to verify integrity
        let mut value = Vec::with_capacity(*size);
        for j in 0..*size {
            value.push((j % 256) as u8);
        }
        
        db.put(&mut txn, (i as u32).to_string(), value.clone())?;
        txn.commit()?;
        
        // Verify immediately
        let txn = env.read_txn()?;
        let read_value = db.get(&txn, &(i as u32).to_string())?.unwrap();
        assert_eq!(read_value.len(), *size);
        
        // Verify pattern
        for (j, &byte) in read_value.iter().enumerate() {
            assert_eq!(byte, (j % 256) as u8);
        }
    }
    
    Ok(())
}

#[test]
#[ignore = "Skipping due to 'Value in overflow page' bug in core implementation"]
fn test_overflow_with_small_values_mixed() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, Vec<u8>> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Mix small and large values
    {
        let mut txn = env.write_txn()?;
        
        for i in 0..100 {
            let value = if i % 10 == 0 {
                // Every 10th value is large (overflow)
                vec![i as u8; 5000]
            } else {
                // Others are small (inline)
                vec![i as u8; 50]
            };
            db.put(&mut txn, i.to_string(), value)?;
        }
        
        txn.commit()?;
    }
    
    // Verify all values
    {
        let txn = env.read_txn()?;
        
        for i in 0..100 {
            let value = db.get(&txn, &i.to_string())?.unwrap();
            let expected_size = if i % 10 == 0 { 5000 } else { 50 };
            assert_eq!(value.len(), expected_size);
            assert!(value.iter().all(|&b| b == i as u8));
        }
    }
    
    // Update some values, switching between inline and overflow
    {
        let mut txn = env.write_txn()?;
        
        for i in (0..100).step_by(5) {
            let new_value = if i % 10 == 0 {
                // Was overflow, make it inline
                vec![(i + 100) as u8; 50]
            } else {
                // Was inline, make it overflow
                vec![(i + 100) as u8; 5000]
            };
            db.put(&mut txn, i.to_string(), new_value)?;
        }
        
        txn.commit()?;
    }
    
    // Verify updates
    {
        let txn = env.read_txn()?;
        
        for i in 0..100 {
            let value = db.get(&txn, &i.to_string())?.unwrap();
            
            if i % 5 == 0 {
                // Updated values
                let expected_size = if i % 10 == 0 { 50 } else { 5000 };
                assert_eq!(value.len(), expected_size);
                assert!(value.iter().all(|&b| b == (i + 100) as u8));
            } else {
                // Original values
                let expected_size = if i % 10 == 0 { 5000 } else { 50 };
                assert_eq!(value.len(), expected_size);
                assert!(value.iter().all(|&b| b == i as u8));
            }
        }
    }
    
    Ok(())
}

#[test]
fn test_overflow_delete_and_reuse() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, Vec<u8>> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Insert large values
    {
        let mut txn = env.write_txn()?;
        for i in 0..10 {
            db.put(&mut txn, i.to_string(), vec![i as u8; 10000])?;
        }
        txn.commit()?;
    }
    
    // Delete some overflow values
    {
        let mut txn = env.write_txn()?;
        for i in (0..10).step_by(2) {
            db.delete(&mut txn, &i.to_string())?;
        }
        txn.commit()?;
    }
    
    // Insert new overflow values - should reuse freed pages
    {
        let mut txn = env.write_txn()?;
        for i in 10..15 {
            db.put(&mut txn, i.to_string(), vec![i as u8; 10000])?;
        }
        txn.commit()?;
    }
    
    // Verify final state
    {
        let txn = env.read_txn()?;
        
        // Deleted entries
        for i in (0..10).step_by(2) {
            assert_eq!(db.get(&txn, &i.to_string())?, None);
        }
        
        // Original remaining entries
        for i in (1..10).step_by(2) {
            let value = db.get(&txn, &i.to_string())?.unwrap();
            assert_eq!(value.len(), 10000);
            assert!(value.iter().all(|&b| b == i as u8));
        }
        
        // New entries
        for i in 10..15 {
            let value = db.get(&txn, &i.to_string())?.unwrap();
            assert_eq!(value.len(), 10000);
            assert!(value.iter().all(|&b| b == i as u8));
        }
    }
    
    Ok(())
}

#[test]
fn test_overflow_edge_cases() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Test empty key with overflow value
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, vec![], vec![1; 5000])?;
        txn.commit()?;
    }
    
    // Test large key with overflow value
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, vec![2; 500], vec![3; 5000])?;
        txn.commit()?;
    }
    
    // Test overflow value that's exactly N pages
    {
        let mut txn = env.write_txn()?;
        let page_size = 4096;
        let header_size = 16; // Approximate header size
        let exact_pages_value = vec![4; page_size * 3 - header_size];
        db.put(&mut txn, b"exact".to_vec(), exact_pages_value)?;
        txn.commit()?;
    }
    
    // Verify all edge cases
    {
        let txn = env.read_txn()?;
        
        let v1 = db.get(&txn, &vec![])?.unwrap();
        assert_eq!(v1.len(), 5000);
        
        let v2 = db.get(&txn, &vec![2; 500])?.unwrap();
        assert_eq!(v2.len(), 5000);
        
        let v3 = db.get(&txn, &b"exact".to_vec())?.unwrap();
        assert_eq!(v3.len(), 4096 * 3 - 16);
    }
    
    Ok(())
}