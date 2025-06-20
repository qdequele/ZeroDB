//! Simple example demonstrating basic ZeroDB usage
//! 
//! This example shows:
//! - Opening an environment
//! - Creating a database
//! - Basic CRUD operations
//! - Iteration over entries
//! - Error handling

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{EnvBuilder, Result, env::DurabilityMode};

fn main() -> Result<()> {
    // Create temporary directory for the database
    let dir = TempDir::new().unwrap();

    // Open environment with configuration
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_readers(126)            // Maximum concurrent readers
            .max_dbs(10)                 // Maximum named databases
            .durability(DurabilityMode::AsyncFlush) // Default durability
            .open(dir.path())?,
    );

    println!("Environment opened at: {:?}", dir.path());

    // Create a typed database (String keys, String values)
    let db = {
        let mut txn = env.write_txn()?;
        let db: zerodb::db::Database<String, String> = env.create_database(&mut txn, None)?;
        txn.commit()?;
        println!("Database created");
        db
    };

    // Insert some data
    {
        let mut txn = env.write_txn()?;
        
        // Insert key-value pairs
        db.put(&mut txn, "hello".to_string(), "world".to_string())?;
        db.put(&mut txn, "foo".to_string(), "bar".to_string())?;
        db.put(&mut txn, "rust".to_string(), "zerodb".to_string())?;
        
        // Update an existing value
        db.put(&mut txn, "hello".to_string(), "updated world".to_string())?;
        
        txn.commit()?;
        println!("Data inserted and updated");
    }

    // Read operations
    {
        let txn = env.read_txn()?;

        // Get individual values
        let val1 = db.get(&txn, &"hello".to_string())?;
        println!("hello => {:?}", val1);

        let val2 = db.get(&txn, &"foo".to_string())?;
        println!("foo => {:?}", val2);

        // Try to get a non-existent key
        let val3 = db.get(&txn, &"missing".to_string())?;
        println!("missing => {:?}", val3);
        
        // Check if a key exists
        if db.get(&txn, &"rust".to_string())?.is_some() {
            println!("Key 'rust' exists in the database");
        }
    }

    // Iterate over all entries
    {
        let txn = env.read_txn()?;
        
        println!("\nIterating over all entries:");
        let mut cursor = db.cursor(&txn)?;
        let mut i = 0;
        cursor.first()?;
        while let Some((key, value)) = cursor.current()? {
            println!("  {}: {} => {}", i, String::from_utf8_lossy(&key), value);
            i += 1;
            if cursor.next_entry()?.is_none() {
                break;
            }
        }
    }

    // Delete operations
    {
        let mut txn = env.write_txn()?;
        
        // Delete a key
        let deleted = db.delete(&mut txn, &"foo".to_string())?;
        println!("\nDeleted 'foo': {}", deleted);
        
        txn.commit()?;
    }

    // Verify deletion
    {
        let txn = env.read_txn()?;
        let val = db.get(&txn, &"foo".to_string())?;
        println!("After deletion, foo => {:?}", val);
    }

    // Range-like iteration using cursor seek
    {
        let txn = env.read_txn()?;
        
        println!("\nManual iteration starting from 'h':");
        let mut cursor = db.cursor(&txn)?;
        if let Some((k, v)) = cursor.seek(&"h".to_string())? {
            let key_str = String::from_utf8_lossy(&k);
            println!("  {} => {}", key_str, v);
            while let Some((k, v)) = cursor.next_entry()? {
                let key_str = String::from_utf8_lossy(&k);
                if !key_str.starts_with('h') {
                    break;
                }
                println!("  {} => {}", key_str, v);
            }
        }
    }

    Ok(())
}
