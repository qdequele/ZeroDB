//! Test if segregated freelist is causing the issue

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{
    db::Database,
    env::EnvBuilder,
    error::Result,
};

#[test]
fn test_without_segregated_freelist_100_entries() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(200 * 1024 * 1024)
            // NO segregated freelist
            .open(dir.path())?,
    );
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // Insert 100 entries in batches of 25
    for batch_num in 0..4 {
        let mut txn = env.write_txn()?;
        
        for i in 0..25 {
            let idx = batch_num * 25 + i;
            let key = format!("size_50_key_{:04}", idx).into_bytes();
            let value = vec![42u8; 50];
            db.put(&mut txn, key, value)?;
        }
        
        txn.commit()?;
        
        // Verify
        let read_txn = env.read_txn()?;
        let mut cursor = db.cursor(&read_txn)?;
        let mut count = 0;
        if let Ok(Some(_)) = cursor.first() {
            count += 1;
            while let Ok(Some(_)) = cursor.next_raw() {
                count += 1;
            }
        }
        eprintln!("After batch {}, database has {} entries (expected {})", 
            batch_num, count, (batch_num + 1) * 25);
    }
    
    // Final verification
    let read_txn = env.read_txn()?;
    let mut cursor = db.cursor(&read_txn)?;
    let mut count = 0;
    if let Ok(Some(_)) = cursor.first() {
        count += 1;
        while let Ok(Some(_)) = cursor.next_raw() {
            count += 1;
        }
    }
    
    eprintln!("Final count without segregated freelist: {} entries", count);
    assert_eq!(count, 100, "Should have all 100 entries");
    
    Ok(())
}

#[test]
fn test_segregated_freelist_limits() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(200 * 1024 * 1024)
            .use_segregated_freelist(true)
            .open(dir.path())?,
    );
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // Try to find the exact limit by inserting one entry at a time
    let mut successful_inserts = 0;
    
    for i in 0..200 {
        let mut txn = env.write_txn()?;
        
        let key = format!("key_{:04}", i.to_string()).into_bytes();
        let value = vec![42u8; 50];
        
        match db.put(&mut txn, key.clone(), value) {
            Ok(_) => {
                match txn.commit() {
                    Ok(_) => {
                        // Verify it was actually persisted
                        let read_txn = env.read_txn()?;
                        let mut cursor = db.cursor(&read_txn)?;
                        let mut count = 0;
                        if let Ok(Some(_)) = cursor.first() {
                            count += 1;
                            while let Ok(Some(_)) = cursor.next_raw() {
                                count += 1;
                            }
                        }
                        
                        if count == successful_inserts + 1 {
                            successful_inserts += 1;
                            if i % 10 == 0 {
                                eprintln!("Successfully persisted entry {}", i);
                            }
                        } else {
                            eprintln!("SILENT DATA LOSS at entry {}! Expected {} entries, got {}", 
                                i, successful_inserts + 1, count);
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Commit failed at entry {}: {:?}", i, e);
                        break;
                    }
                }
            }
            Err(e) => {
                eprintln!("Insert failed at entry {}: {:?}", i, e);
                break;
            }
        }
    }
    
    eprintln!("Maximum successful inserts with segregated freelist: {}", successful_inserts);
    
    Ok(())
}