//! Test for overflow page COW issue

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::db::Database;
use zerodb::env::EnvBuilder;
use zerodb::page::MAX_VALUE_SIZE;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing overflow COW issue...");

    // Create a temporary directory
    let dir = TempDir::new()?;
    println!("Created temp dir at: {:?}", dir.path());

    // Create environment
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .open(dir.path())?,
    );
    println!("Environment created");

    // Create a database
    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, Some("test_db"))?;
        txn.commit()?;
        db
    };

    // Create a large value that will require overflow pages
    let large_value = vec![0xAB; MAX_VALUE_SIZE * 2];

    println!("Inserting value with overflow pages...");
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, b"key1".to_vec(), large_value.clone())?;
        txn.commit()?;
    }

    // Now start a new transaction and try to modify the page
    println!("Starting new transaction to test COW...");
    {
        let mut txn = env.write_txn()?;

        // This should trigger COW when we try to modify the page
        println!("Attempting to insert another key (triggering COW)...");
        match db.put(&mut txn, b"key2".to_vec(), b"small".to_vec()) {
            Ok(_) => println!("Success!"),
            Err(e) => {
                println!("Error during COW: {:?}", e);
                return Err(e.into());
            }
        }

        txn.commit()?;
    }

    // Verify both keys exist
    {
        let txn = env.read_txn()?;
        assert!(db.get(&txn, &b"key1".to_vec())?.is_some());
        assert!(db.get(&txn, &b"key2".to_vec())?.is_some());
    }

    println!("Test passed!");
    Ok(())
}
