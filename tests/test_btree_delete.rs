//! Test B+Tree deletion behavior

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::db::Database;
use zerodb::env::EnvBuilder;

#[test]
fn test_btree_delete_even_entries() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create a database
    let db: Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, Some("test_db"))?;
        txn.commit()?;
        db
    };

    // Insert entries
    let num_entries = 50; // Enough to cause splits
    {
        let mut txn = env.write_txn()?;

        for i in 0..num_entries {
            let key = format!("key_{:03}", i.to_string());
            let value = vec![i as u8; 256]; // Same size as freelist test
            db.put(&mut txn, key, value)?;
        }

        txn.commit()?;
    }

    // Verify all entries before deletion
    {
        let txn = env.read_txn()?;
        let mut cursor = db.cursor(&txn)?;
        let mut count = 0;

        while let Some((_key, _value)) = cursor.next_raw()? {
            count += 1;
        }
        assert_eq!(count, num_entries);
    }

    // Delete even entries
    {
        let mut txn = env.write_txn()?;
        let mut deleted = 0;

        for i in (0..num_entries).step_by(2) {
            let key = format!("key_{:03}", i.to_string());
            if db.delete(&mut txn, &key)? {
                deleted += 1;
            }
        }

        assert_eq!(deleted, num_entries / 2);
        txn.commit()?;
    }

    // Verify remaining entries
    {
        let txn = env.read_txn()?;
        let mut cursor = db.cursor(&txn)?;
        let mut remaining = Vec::new();

        while let Some((key, _value)) = cursor.next_raw()? {
            let key_str = String::from_utf8_lossy(key).to_string();
            remaining.push(key_str);
        }

        // Check that we have the right entries
        let mut expected = Vec::new();
        for i in (1..num_entries).step_by(2) {
            expected.push(format!("key_{:03}", i.to_string()));
        }

        expected.sort();
        remaining.sort();

        assert_eq!(expected, remaining, "Wrong entries remain after deletion");
    }

    Ok(())
}

#[test]
fn test_btree_delete_random_access() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create a database
    let db: Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, Some("test_db"))?;
        txn.commit()?;
        db
    };

    let num_entries = 50;

    // Insert entries
    {
        let mut txn = env.write_txn()?;
        for i in 0..num_entries {
            let key = format!("key_{:03}", i.to_string());
            let value = vec![i as u8; 256];
            db.put(&mut txn, key, value)?;
        }
        txn.commit()?;
    }

    // Delete even entries
    {
        let mut txn = env.write_txn()?;
        for i in (0..num_entries).step_by(2) {
            let key = format!("key_{:03}", i.to_string());
            db.delete(&mut txn, &key)?;
        }
        txn.commit()?;
    }

    // Random access test
    {
        let txn = env.read_txn()?;

        // Test some odd entries (should exist)
        for i in [1, 5, 9, 13, 17] {
            if i < num_entries {
                let key = format!("key_{:03}", i.to_string());
                let value = db.get(&txn, &key)?;
                assert!(value.is_some(), "Key {} should exist", key);
                assert_eq!(value.unwrap()[0], i as u8);
            }
        }

        // Test some even entries (should not exist)
        for i in [0, 4, 8, 12, 16] {
            if i < num_entries {
                let key = format!("key_{:03}", i.to_string());
                let value = db.get(&txn, &key)?;
                assert!(value.is_none(), "Key {} should be deleted", key);
            }
        }
    }

    Ok(())
}