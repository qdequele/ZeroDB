//! Test that COW works correctly for delete operations

use std::sync::Arc;
use zerodb::{db::Database, env::EnvBuilder, error::Result};

fn main() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(100 * 1024 * 1024)
            .use_segregated_freelist(true)
            .open(dir.path())?,
    );
    
    // Create database
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // Test with smaller values that should work
    let mut txn = env.write_txn()?;
    
    println!("Inserting 20 entries with small values...");
    for i in 0..20 {
        let key = format!("key_{:08}", i.to_string()).into_bytes();
        let value = vec![42u8; 50]; // Small values
        db.put(&mut txn, key, value)?;
    }
    
    println!("Deleting every other entry...");
    for i in (0..20).step_by(2) {
        let key = format!("key_{:08}", i.to_string()).into_bytes();
        match db.delete(&mut txn, &key) {
            Ok(true) => println!("Deleted key_{:08}", i.to_string()),
            Ok(false) => println!("Key_{:08} not found", i.to_string()),
            Err(e) => {
                println!("ERROR deleting key_{:08}: {:?}", i, e);
                return Err(e);
            }
        }
    }
    
    println!("Committing transaction...");
    txn.commit()?;
    
    // Verify remaining entries
    let txn = env.read_txn()?;
    let mut count = 0;
    let mut cursor = db.cursor(&txn)?;
    
    if let Ok(Some((key, _value))) = cursor.first() {
        count += 1;
        println!("Found: {}", String::from_utf8_lossy(&key));
        
        while let Ok(Some((key, _value))) = cursor.next_raw() {
            count += 1;
            println!("Found: {}", String::from_utf8_lossy(&key));
        }
    }
    
    println!("\nTotal entries remaining: {} (expected 10)", count);
    
    if count == 10 {
        println!("Success! COW delete works correctly.");
    } else {
        println!("Error: Expected 10 entries, found {}", count);
    }
    
    Ok(())
}