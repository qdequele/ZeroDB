//! Test B+Tree splitting behavior

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::db::Database;
use zerodb::env::EnvBuilder;

#[test]
fn test_btree_split_insertion() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create a database
    let db: Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, Some("test_db"))?;
        txn.commit()?;
        db
    };

    // Insert entries until we trigger a split
    let num_entries = 60; // Should be enough to trigger at least one split

    {
        let mut txn = env.write_txn()?;

        for i in 0..num_entries {
            let key = format!("key_{:03}", i.to_string());
            let value = vec![i as u8; 100];
            db.put(&mut txn, key.clone(), value)?;
        }

        let final_info = txn.db_info(Some("test_db"))?;
        assert!(final_info.entries == num_entries as u64);
        assert!(final_info.depth > 1); // Should have split

        txn.commit()?;
    }

    Ok(())
}

#[test]
fn test_btree_split_verify_entries() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create a database
    let db: Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, Some("test_db"))?;
        txn.commit()?;
        db
    };

    let num_entries = 60;

    // Insert entries
    {
        let mut txn = env.write_txn()?;
        for i in 0..num_entries {
            let key = format!("key_{:03}", i.to_string());
            let value = vec![i as u8; 100];
            db.put(&mut txn, key.clone(), value)?;
        }
        txn.commit()?;
    }

    // Verify all entries are still there
    {
        let txn = env.read_txn()?;
        let mut cursor = db.cursor(&txn)?;

        let mut found_keys = Vec::new();
        while let Some((key, _value)) = cursor.next_raw()? {
            found_keys.push(String::from_utf8_lossy(key).to_string());
        }

        assert_eq!(found_keys.len(), num_entries);

        // Check if all keys are present
        for i in 0..num_entries {
            let expected_key = format!("key_{:03}", i.to_string());
            assert!(found_keys.contains(&expected_key), "Missing key: {}", expected_key);
        }

        // Also check ordering
        let mut sorted = found_keys.clone();
        sorted.sort();
        assert_eq!(found_keys, sorted, "Entries are not in correct order");
    }

    Ok(())
}

#[test]
fn test_btree_split_random_access() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create a database
    let db: Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, Some("test_db"))?;
        txn.commit()?;
        db
    };

    let num_entries = 60;

    // Insert entries
    {
        let mut txn = env.write_txn()?;
        for i in 0..num_entries {
            let key = format!("key_{:03}", i.to_string());
            let value = vec![i as u8; 100];
            db.put(&mut txn, key.clone(), value)?;
        }
        txn.commit()?;
    }

    // Random access test
    {
        let txn = env.read_txn()?;

        let test_keys = vec![0, 15, 30, 45, 59];
        for i in test_keys {
            let key = format!("key_{:03}", i.to_string());
            match db.get(&txn, &key)? {
                Some(value) => {
                    assert_eq!(value[0], i as u8, "Wrong value for key {}", key);
                }
                None => {
                    panic!("Key {} not found", key);
                }
            }
        }
    }

    Ok(())
}