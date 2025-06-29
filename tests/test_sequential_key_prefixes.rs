//! Test sequential inserts with different key prefixes

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{
    db::Database,
    env::EnvBuilder,
    error::Result,
};

#[test]
fn test_different_prefixes_fresh_db() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(200 * 1024 * 1024)
            
            .open(dir.path())?,
    );
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // Start directly with size_100 prefix
    let mut txn = env.write_txn()?;
    
    for i in 0..100 {
        let key = format!("size_100_key_{:04}", i.to_string()).into_bytes();
        let value = vec![42u8; 100];
        match db.put(&mut txn, key.clone(), value) {
            Ok(_) => {
                if i % 10 == 0 {
                    println!("Inserted entry {} with size_100 prefix", i);
                }
            }
            Err(e) => {
                eprintln!("Failed at entry {} with size_100 prefix: {:?}", i, e);
                return Err(e);
            }
        }
    }
    
    txn.commit()?;
    println!("Successfully inserted 100 entries with size_100 prefix in fresh DB");
    Ok(())
}

#[test]
fn test_prefix_transition_issue() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(200 * 1024 * 1024)
            
            .open(dir.path())?,
    );
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // First, insert with prefix "a_"
    let mut txn = env.write_txn()?;
    for i in 0..50 {
        let key = format!("a_{:04}", i.to_string()).into_bytes();
        let value = vec![42u8; 50];
        db.put(&mut txn, key, value)?;
    }
    txn.commit()?;
    println!("Inserted 50 entries with 'a_' prefix");
    
    // Now try with prefix "z_" (far from "a_" in sort order)
    let mut txn = env.write_txn()?;
    for i in 0..50 {
        let key = format!("z_{:04}", i.to_string()).into_bytes();
        let value = vec![42u8; 100];
        match db.put(&mut txn, key.clone(), value) {
            Ok(_) => {
                if i % 10 == 0 {
                    println!("Inserted entry {} with 'z_' prefix", i);
                }
            }
            Err(e) => {
                eprintln!("Failed at entry {} with 'z_' prefix: {:?}", i, e);
                return Err(e);
            }
        }
    }
    txn.commit()?;
    
    println!("Successfully handled prefix transition");
    Ok(())
}

#[test]
fn test_interleaved_prefixes() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(200 * 1024 * 1024)
            
            .open(dir.path())?,
    );
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // Insert entries with alternating prefixes
    let mut txn = env.write_txn()?;
    for i in 0..20 {
        // Alternate between different prefixes
        let prefix = match i % 3 {
            0 => "aaa",
            1 => "mmm",
            _ => "zzz",
        };
        
        let key = format!("{}_{:04}", prefix, i.to_string()).into_bytes();
        let value = vec![42u8; 100];
        match db.put(&mut txn, key.clone(), value) {
            Ok(_) => println!("Inserted {}", String::from_utf8_lossy(&key)),
            Err(e) => {
                eprintln!("Failed at entry {} with prefix {}: {:?}", i, prefix, e);
                return Err(e);
            }
        }
    }
    
    txn.commit()?;
    println!("Successfully handled interleaved prefixes");
    Ok(())
}