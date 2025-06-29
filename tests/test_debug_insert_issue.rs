//! Debug test for insert issue

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{
    db::Database,
    env::EnvBuilder,
    error::Result,
};

#[test]
fn test_debug_insert_issue() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(200 * 1024 * 1024)
            
            .open(dir.path())?,
    );
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // First insert 50 entries with 50-byte values
    let mut txn = env.write_txn()?;
    
    for i in 0..50 {
        let key = format!("size_50_key_{:04}", i.to_string()).into_bytes();
        let value = vec![42u8; 50];
        db.put(&mut txn, key, value)?;
    }
    
    println!("Successfully inserted 50 entries with 50-byte values");
    
    // Now try to insert entries with 100-byte values
    for i in 0..10 {
        let key = format!("size_100_key_{:04}", i.to_string()).into_bytes();
        let value = vec![42u8; 100];
        match db.put(&mut txn, key.clone(), value) {
            Ok(_) => println!("Inserted entry {} with 100-byte value", i),
            Err(e) => {
                eprintln!("Failed at entry {} with 100-byte value: {:?}", i, e);
                return Err(e);
            }
        }
    }
    
    txn.commit()?;
    Ok(())
}

#[test]
fn test_without_segregated_freelist() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(200 * 1024 * 1024)
            // Don't use segregated freelist
            .open(dir.path())?,
    );
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // Try the same pattern
    let mut txn = env.write_txn()?;
    
    for i in 0..50 {
        let key = format!("size_50_key_{:04}", i.to_string()).into_bytes();
        let value = vec![42u8; 50];
        db.put(&mut txn, key, value)?;
    }
    
    println!("Successfully inserted 50 entries with 50-byte values (no segregated freelist)");
    
    // Now try to insert entries with 100-byte values
    for i in 0..100 {
        let key = format!("size_100_key_{:04}", i.to_string()).into_bytes();
        let value = vec![42u8; 100];
        match db.put(&mut txn, key.clone(), value) {
            Ok(_) => {
                if i % 10 == 0 {
                    println!("Inserted entry {} with 100-byte value", i);
                }
            }
            Err(e) => {
                eprintln!("Failed at entry {} with 100-byte value: {:?}", i, e);
                return Err(e);
            }
        }
    }
    
    txn.commit()?;
    println!("Successfully inserted all entries without segregated freelist");
    Ok(())
}