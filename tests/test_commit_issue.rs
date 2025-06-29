//! Test commit issue where entries are lost

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{
    db::Database,
    env::EnvBuilder,
    error::Result,
};

#[test]
fn test_direct_batch_75_to_100() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(200 * 1024 * 1024)
            
            .open(dir.path())?,
    );
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // Try inserting keys 75-99 directly
    let mut txn = env.write_txn()?;
    
    for i in 75..100 {
        let key = format!("size_50_key_{:04}", i.to_string()).into_bytes();
        let value = vec![42u8; 50];
        db.put(&mut txn, key.clone(), value)?;
        eprintln!("Inserted {}", String::from_utf8_lossy(&key));
    }
    
    eprintln!("About to commit transaction with 25 entries");
    txn.commit()?;
    eprintln!("Transaction committed");
    
    // Check what was actually persisted
    let read_txn = env.read_txn()?;
    let mut cursor = db.cursor(&read_txn)?;
    let mut count = 0;
    let mut keys = Vec::new();
    
    if let Ok(Some((k, _))) = cursor.first() {
        keys.push(String::from_utf8_lossy(&k).to_string());
        count += 1;
        while let Ok(Some((k, _))) = cursor.next_raw() {
            keys.push(String::from_utf8_lossy(k).to_string());
            count += 1;
        }
    }
    
    eprintln!("Database has {} entries", count);
    eprintln!("Keys: {:?}", keys);
    
    assert_eq!(count, 25, "Should have 25 entries");
    
    Ok(())
}

#[test]
fn test_cumulative_insert_pattern() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(200 * 1024 * 1024)
            
            .open(dir.path())?,
    );
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // Insert 75 entries first (like the failing test)
    for batch_num in 0..3 {
        let mut txn = env.write_txn()?;
        
        for i in 0..25 {
            let idx = batch_num * 25 + i;
            let key = format!("size_50_key_{:04}", idx).into_bytes();
            let value = vec![42u8; 50];
            db.put(&mut txn, key, value)?;
        }
        
        txn.commit()?;
        eprintln!("Committed batch {}, total should be {}", batch_num, (batch_num + 1) * 25);
    }
    
    // Verify we have 75 entries
    let read_txn = env.read_txn()?;
    let mut cursor = db.cursor(&read_txn)?;
    let mut count = 0;
    if let Ok(Some(_)) = cursor.first() {
        count += 1;
        while let Ok(Some(_)) = cursor.next_raw() {
            count += 1;
        }
    }
    eprintln!("After 3 batches, database has {} entries", count);
    assert_eq!(count, 75);
    
    // Now try the 4th batch
    let mut txn = env.write_txn()?;
    let mut inserted_in_batch = 0;
    
    for i in 75..100 {
        let key = format!("size_50_key_{:04}", i.to_string()).into_bytes();
        let value = vec![42u8; 50];
        match db.put(&mut txn, key.clone(), value) {
            Ok(_) => {
                inserted_in_batch += 1;
            }
            Err(e) => {
                eprintln!("Failed at key {}: {:?}", String::from_utf8_lossy(&key), e);
                return Err(e);
            }
        }
    }
    
    eprintln!("Inserted {} entries in 4th batch", inserted_in_batch);
    
    match txn.commit() {
        Ok(_) => eprintln!("4th batch committed successfully"),
        Err(e) => {
            eprintln!("4th batch commit failed: {:?}", e);
            return Err(e);
        }
    }
    
    // Check final count
    let read_txn = env.read_txn()?;
    let mut cursor = db.cursor(&read_txn)?;
    let mut final_count = 0;
    let mut last_key = None;
    
    if let Ok(Some((_k, _))) = cursor.first() {
        final_count += 1;
        while let Ok(Some((k, _))) = cursor.next_raw() {
            last_key = Some(k);
            final_count += 1;
        }
    }
    
    eprintln!("Final database has {} entries", final_count);
    if let Some(k) = last_key {
        eprintln!("Last key: {}", String::from_utf8_lossy(k));
    }
    
    if final_count != 100 {
        eprintln!("ERROR: Lost {} entries during commit!", 100 - final_count);
    }
    
    Ok(())
}