//! Test the exact scenario from the stress test

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{
    db::Database,
    env::EnvBuilder,
    error::Result,
};

#[test]
fn test_exact_stress_scenario() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(200 * 1024 * 1024)
            
            .open(dir.path())?,
    );
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // First, insert 100 entries with value_size=50 in batches of 25
    let value_size = 50;
    eprintln!("Testing value_size={}", value_size);
    
    let batch_size = 25;
    for batch_start in (0..100).step_by(batch_size) {
        let mut txn = env.write_txn()?;
        
        let batch_end = std::cmp::min(batch_start + batch_size, 100);
        eprintln!("Inserting batch {} to {}", batch_start, batch_end);
        
        for i in batch_start..batch_end {
            let key = format!("size_{}_key_{:04}", value_size, i.to_string()).into_bytes();
            let value = vec![42u8; value_size];
            match db.put(&mut txn, key.clone(), value) {
                Ok(_) => {},
                Err(e) => {
                    eprintln!("Failed to insert key {} in batch: {:?}", 
                        String::from_utf8_lossy(&key), e);
                    return Err(e);
                }
            }
        }
        
        txn.commit()?;
        
        // Verify the batch was committed
        let read_txn = env.read_txn()?;
        let mut cursor = db.cursor(&read_txn)?;
        let mut count = 0;
        if let Ok(Some(_)) = cursor.first() {
            count += 1;
            while let Ok(Some(_)) = cursor.next_raw() {
                count += 1;
            }
        }
        eprintln!("After batch ending at {}, database has {} entries", batch_end, count);
    }
    
    eprintln!("Successfully inserted 100 entries with value_size=50");
    
    // Now try to insert a single entry with value_size=100
    let value_size = 100;
    eprintln!("Testing value_size={}", value_size);
    
    let mut txn = env.write_txn()?;
    
    let key = format!("size_{}_key_{:04}", value_size, 0).into_bytes();
    let value = vec![42u8; value_size];
    
    eprintln!("About to insert key: {}", String::from_utf8_lossy(&key));
    
    match db.put(&mut txn, key.clone(), value) {
        Ok(_) => {
            eprintln!("Successfully inserted first entry with value_size=100");
            txn.commit()?;
        }
        Err(e) => {
            eprintln!("Failed to insert first entry with value_size=100: {:?}", e);
            
            // Let's check the database state
            drop(txn);
            
            // First check with a fresh read transaction
            let read_txn = env.read_txn()?;
            let mut cursor = db.cursor(&read_txn)?;
            let mut count = 0;
            let mut first_key = None;
            let mut last_key: Option<Vec<u8>> = None;
            
            if let Ok(Some((k, _))) = cursor.first() {
                first_key = Some(k.clone());
                count += 1;
                while let Ok(Some((k, _))) = cursor.next_raw() {
                    last_key = Some(k.to_vec());
                    count += 1;
                }
            }
            
            if last_key.is_none() && first_key.is_some() {
                last_key = first_key.clone();
            }
            
            eprintln!("Database currently has {} entries", count);
            if let Some(k) = first_key {
                eprintln!("First key: {}", String::from_utf8_lossy(&k));
            }
            if let Some(k) = last_key {
                eprintln!("Last key: {}", String::from_utf8_lossy(&k));
            }
            
            // Check keys around where we're trying to insert
            let target_key = format!("size_{}_key_{:04}", 100, 0).into_bytes();
            eprintln!("Trying to insert key: {}", String::from_utf8_lossy(&target_key));
            
            return Err(e);
        }
    }
    
    Ok(())
}

#[test]
fn test_with_smaller_batches() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(200 * 1024 * 1024)
            
            .open(dir.path())?,
    );
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // Insert entries with value_size=50 in smaller batches
    let value_size = 50;
    
    for batch_num in 0..10 {
        let mut txn = env.write_txn()?;
        
        for i in 0..10 {
            let idx = batch_num * 10 + i;
            let key = format!("size_{}_key_{:04}", value_size, idx).into_bytes();
            let value = vec![42u8; value_size];
            db.put(&mut txn, key, value)?;
        }
        
        txn.commit()?;
    }
    
    eprintln!("Successfully inserted 100 entries with value_size=50 in batches of 10");
    
    // Now try value_size=100
    let value_size = 100;
    let mut txn = env.write_txn()?;
    
    for i in 0..10 {
        let key = format!("size_{}_key_{:04}", value_size, i.to_string()).into_bytes();
        let value = vec![42u8; value_size];
        
        match db.put(&mut txn, key.clone(), value) {
            Ok(_) => eprintln!("Inserted entry {} with value_size=100", i),
            Err(e) => {
                eprintln!("Failed at entry {} with value_size=100: {:?}", i, e);
                return Err(e);
            }
        }
    }
    
    txn.commit()?;
    eprintln!("Successfully inserted entries with value_size=100");
    
    Ok(())
}