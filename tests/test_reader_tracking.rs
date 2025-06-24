//! Test reader tracking

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use zerodb::db::Database;
use zerodb::env::EnvBuilder;

#[test]
fn test_reader_tracking_with_concurrent_writes() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create a database
    let db: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, Some("test_db"))?;
        txn.commit()?;
        db
    };

    // Insert some data
    {
        let mut txn = env.write_txn()?;
        for i in 0..10 {
            db.put(&mut txn, format!("key_{}", i), format!("value_{}", i))?;
        }
        txn.commit()?;
    }

    // Create a long-running reader in another thread
    let env_clone = env.clone();
    let reader_handle = thread::spawn(move || {
        let txn = env_clone.read_txn().unwrap();
        
        // Hold the transaction for a while
        thread::sleep(Duration::from_secs(2));
        
        drop(txn);
    });

    // Give the reader thread time to start
    thread::sleep(Duration::from_millis(500));

    // Try to create write transactions that would reuse pages
    for i in 0..5 {
        let mut txn = env.write_txn()?;

        // Delete and re-insert to generate free pages
        let key = format!("key_{}", i);
        db.delete(&mut txn, &key)?;
        db.put(&mut txn, key, format!("new_value_{}", i))?;

        txn.commit()?;

        thread::sleep(Duration::from_millis(200));
    }

    // Wait for reader thread to finish
    reader_handle.join().unwrap();

    // Verify data integrity
    {
        let txn = env.read_txn()?;
        for i in 0..10 {
            let key = format!("key_{}", i);
            match db.get(&txn, &key)? {
                Some(value) => {
                    let expected =
                        if i < 5 { format!("new_value_{}", i) } else { format!("value_{}", i) };
                    assert_eq!(value, expected, "Wrong value for key {}", key);
                }
                None => panic!("Key {} not found", key),
            }
        }
    }

    Ok(())
}

#[test]
fn test_reader_tracking_page_reuse() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create a database
    let db: Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, Some("test_db"))?;
        txn.commit()?;
        db
    };

    // Initial data
    {
        let mut txn = env.write_txn()?;
        for i in 0..20 {
            let key = format!("key_{:02}", i);
            let value = vec![i as u8; 512]; // Larger values to ensure page allocation
            db.put(&mut txn, key, value)?;
        }
        txn.commit()?;
    }

    // Start a reader
    let reader_txn = env.read_txn()?;

    // Delete and reinsert data while reader is active
    {
        let mut txn = env.write_txn()?;
        
        // Delete some entries
        for i in 0..10 {
            let key = format!("key_{:02}", i);
            db.delete(&mut txn, &key)?;
        }
        
        // Insert new entries
        for i in 20..30 {
            let key = format!("key_{:02}", i);
            let value = vec![i as u8; 512];
            db.put(&mut txn, key, value)?;
        }
        
        txn.commit()?;
    }

    // Drop reader - pages can now be reclaimed
    drop(reader_txn);

    // Verify final state
    {
        let txn = env.read_txn()?;
        
        // First 10 keys should be deleted
        for i in 0..10 {
            let key = format!("key_{:02}", i);
            assert!(db.get(&txn, &key)?.is_none(), "Key {} should be deleted", key);
        }
        
        // Keys 10-19 should still exist
        for i in 10..20 {
            let key = format!("key_{:02}", i);
            let value = db.get(&txn, &key)?;
            assert!(value.is_some(), "Key {} should exist", key);
            assert_eq!(value.unwrap()[0], i as u8);
        }
        
        // Keys 20-29 should exist
        for i in 20..30 {
            let key = format!("key_{:02}", i);
            let value = db.get(&txn, &key)?;
            assert!(value.is_some(), "Key {} should exist", key);
            assert_eq!(value.unwrap()[0], i as u8);
        }
    }

    Ok(())
}