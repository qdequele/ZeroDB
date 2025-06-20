//! Consolidated freelist management tests
//! 
//! Tests free page tracking, allocation, and reuse across transactions.

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{db::Database, EnvBuilder, Result};

#[test]
fn test_basic_freelist_operations() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, Vec<u8>> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Fill database to allocate pages
    {
        let mut txn = env.write_txn()?;
        for i in 0..100 {
            db.put(&mut txn, i.to_string(), vec![i as u8; 100])?;
        }
        txn.commit()?;
    }
    
    // Delete half the entries to free pages
    {
        let mut txn = env.write_txn()?;
        for i in (0..100).step_by(2) {
            db.delete(&mut txn, &i.to_string())?;
        }
        txn.commit()?;
    }
    
    // Insert new data - should reuse freed pages
    {
        let mut txn = env.write_txn()?;
        for i in 100..150 {
            db.put(&mut txn, i.to_string(), vec![i as u8; 100])?;
        }
        txn.commit()?;
    }
    
    // Verify final state
    {
        let txn = env.read_txn()?;
        
        // Deleted entries
        for i in (0..100).step_by(2) {
            assert!(db.get(&txn, &i.to_string())?.is_none());
        }
        
        // Original remaining entries
        for i in (1..100).step_by(2) {
            assert!(db.get(&txn, &i.to_string())?.is_some());
        }
        
        // New entries
        for i in 100..150 {
            assert!(db.get(&txn, &i.to_string())?.is_some());
        }
    }
    
    Ok(())
}

#[test]
fn test_freelist_persistence() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    
    // Phase 1: Create data and free some pages
    {
        let env = Arc::new(EnvBuilder::new().open(&path)?);
        let db = {
            let mut txn = env.write_txn()?;
            let db: Database<String, Vec<u8>> = env.create_database(&mut txn, None)?;
            txn.commit()?;
            db
        };
        
        // Add data
        {
            let mut txn = env.write_txn()?;
            for i in 0..200 {
                db.put(&mut txn, i.to_string(), vec![i as u8; 500])?;
            }
            txn.commit()?;
        }
        
        // Delete some to create free pages
        {
            let mut txn = env.write_txn()?;
            for i in 50..150 {
                db.delete(&mut txn, &i.to_string())?;
            }
            txn.commit()?;
        }
    }
    
    // Phase 2: Reopen and verify freelist is restored
    {
        let env = Arc::new(EnvBuilder::new().open(&path)?);
        let db: Database<String, Vec<u8>> = {
            let txn = env.read_txn()?;
            env.open_database(&txn, None)?
        };
        
        // Add new data - should reuse freed pages
        {
            let mut txn = env.write_txn()?;
            for i in 200..250 {
                db.put(&mut txn, i.to_string(), vec![i as u8; 500])?;
            }
            txn.commit()?;
        }
        
        // Verify
        {
            let txn = env.read_txn()?;
            
            // Original data (not deleted)
            for i in 0..50 {
                assert!(db.get(&txn, &i.to_string())?.is_some());
            }
            for i in 150..200 {
                assert!(db.get(&txn, &i.to_string())?.is_some());
            }
            
            // Deleted range
            for i in 50..150 {
                assert!(db.get(&txn, &i.to_string())?.is_none());
            }
            
            // New data
            for i in 200..250 {
                assert!(db.get(&txn, &i.to_string())?.is_some());
            }
        }
    }
    
    Ok(())
}

#[test]
fn test_freelist_transaction_isolation() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, String> = env.create_database(&mut txn, None)?;
        
        // Initial data
        for i in 0..100 {
            db.put(&mut txn, i.to_string(), (i * 10).to_string())?;
        }
        txn.commit()?;
        db
    };
    
    // Start a read transaction
    let read_txn = env.read_txn()?;
    
    // Delete data in a write transaction
    {
        let mut txn = env.write_txn()?;
        for i in 0..50 {
            db.delete(&mut txn, &i.to_string())?;
        }
        txn.commit()?;
    }
    
    // The freed pages should not be reused while reader is active
    {
        let mut txn = env.write_txn()?;
        for i in 100..150 {
            db.put(&mut txn, i.to_string(), (i * 10).to_string())?;
        }
        txn.commit()?;
    }
    
    // Reader should still see all original data
    for i in 0..100 {
        assert_eq!(db.get(&read_txn, &i)?, Some(i * 10));
    }
    
    // Drop reader
    drop(read_txn);
    
    // Now freed pages can be reclaimed
    {
        let mut txn = env.write_txn()?;
        for i in 150..200 {
            db.put(&mut txn, i.to_string(), (i * 10).to_string())?;
        }
        txn.commit()?;
    }
    
    Ok(())
}

#[test]
fn test_segregated_freelist() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .use_segregated_freelist(true)
            .open(dir.path())?
    );
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, Vec<u8>> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Create different sized allocations
    {
        let mut txn = env.write_txn()?;
        
        // Small values (inline)
        for i in 0..50 {
            db.put(&mut txn, i.to_string(), vec![i as u8; 50])?;
        }
        
        // Medium values (single overflow page)
        for i in 50..100 {
            db.put(&mut txn, i.to_string(), vec![i as u8; 2000])?;
        }
        
        // Large values (multiple overflow pages)
        for i in 100..120 {
            db.put(&mut txn, i.to_string(), vec![i as u8; 10000])?;
        }
        
        txn.commit()?;
    }
    
    // Delete entries of different sizes
    {
        let mut txn = env.write_txn()?;
        
        // Delete some small entries
        for i in (10..30).step_by(2) {
            db.delete(&mut txn, &i.to_string())?;
        }
        
        // Delete some medium entries
        for i in (60..80).step_by(2) {
            db.delete(&mut txn, &i.to_string())?;
        }
        
        // Delete some large entries
        for i in 105..110 {
            db.delete(&mut txn, &i.to_string())?;
        }
        
        txn.commit()?;
    }
    
    // Allocate new entries - segregated freelist should match sizes efficiently
    {
        let mut txn = env.write_txn()?;
        
        // New small entries should reuse small freed pages
        for i in 200..210 {
            db.put(&mut txn, i.to_string(), vec![i as u8; 50])?;
        }
        
        // New medium entries should reuse medium freed pages
        for i in 210..220 {
            db.put(&mut txn, i.to_string(), vec![i as u8; 2000])?;
        }
        
        // New large entries should reuse large freed pages
        for i in 220..225 {
            db.put(&mut txn, i.to_string(), vec![i as u8; 10000])?;
        }
        
        txn.commit()?;
    }
    
    Ok(())
}

#[test]
fn test_freelist_exhaustion_recovery() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(5 * 1024 * 1024) // Small map size
            .open(dir.path())?
    );
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, Vec<u8>> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Fill database until nearly full
    let mut last_key = 0;
    loop {
        let mut txn = env.write_txn()?;
        
        match db.put(&mut txn, last_key, vec![0xFF; 1000]) {
            Ok(_) => {
                txn.commit()?;
                last_key += 1;
            }
            Err(_) => {
                // Database is full
                break;
            }
        }
    }
    
    println!("Database filled with {} entries", last_key);
    
    // Delete half the entries to free space
    {
        let mut txn = env.write_txn()?;
        for i in (0..last_key).step_by(2) {
            db.delete(&mut txn, &i.to_string())?;
        }
        txn.commit()?;
    }
    
    // Should be able to insert again
    {
        let mut txn = env.write_txn()?;
        for i in last_key..last_key + 20 {
            db.put(&mut txn, i.to_string(), vec![0xAA; 1000])?;
        }
        txn.commit()?;
    }
    
    Ok(())
}

#[test]
fn test_freelist_page_coalescing() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let db = {
        let mut txn = env.write_txn()?;
        let db: Database<String, Vec<u8>> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Create fragmented free space
    {
        let mut txn = env.write_txn()?;
        
        // Insert entries that will span multiple pages
        for i in 0..50 {
            db.put(&mut txn, i * 3, vec![i as u8; 2000])?;
            db.put(&mut txn, i * 3 + 1, vec![i as u8; 100])?;
            db.put(&mut txn, i * 3 + 2, vec![i as u8; 500])?;
        }
        
        txn.commit()?;
    }
    
    // Delete entries to create fragmented free space
    {
        let mut txn = env.write_txn()?;
        
        // Delete in a pattern that creates gaps
        for i in 0..50 {
            db.delete(&mut txn, &(i * 3 + 1))?; // Delete the small entries
        }
        
        txn.commit()?;
    }
    
    // Try to allocate larger entries that might benefit from coalescing
    {
        let mut txn = env.write_txn()?;
        
        for i in 200..210 {
            db.put(&mut txn, i.to_string(), vec![i as u8; 3000])?;
        }
        
        txn.commit()?;
    }
    
    // Verify all operations succeeded
    {
        let txn = env.read_txn()?;
        
        // Check remaining original entries
        for i in 0..50 {
            assert!(db.get(&txn, &(i * 3))?.is_some());
            assert!(db.get(&txn, &(i * 3 + 1))?.is_none());
            assert!(db.get(&txn, &(i * 3 + 2))?.is_some());
        }
        
        // Check new entries
        for i in 200..210 {
            let val = db.get(&txn, &i.to_string())?.unwrap();
            assert_eq!(val.len(), 3000);
        }
    }
    
    Ok(())
}