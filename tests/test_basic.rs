//! Basic test to verify database operations work

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::db::Database;
use zerodb::env::EnvBuilder;
use zerodb::error::Result;

#[test]
fn test_single_entry_operations() -> Result<()> {
    // Create a temporary directory
    let dir = TempDir::new().unwrap();

    // Create environment
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .open(dir.path())?,
    );

    // Create a database
    let db: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, Some("test_db"))?;
        txn.commit()?;
        db
    };

    // Insert a single entry
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, "hello".to_string(), "world".to_string())?;
        txn.commit()?;
    }

    // Read the entry
    {
        let txn = env.read_txn()?;
        let value = db.get(&txn, &"hello".to_string())?;
        assert_eq!(value, Some("world".to_string()));
    }

    Ok(())
}

#[test]
fn test_multiple_entries() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())?,
    );

    let db: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Insert multiple entries
    {
        let mut txn = env.write_txn()?;
        for i in 0..5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            db.put(&mut txn, key, value)?;
        }
        txn.commit()?;
    }

    // Read all entries with cursor
    {
        let txn = env.read_txn()?;
        let mut cursor = db.cursor(&txn)?;
        
        let mut count = 0;
        while let Some((key, value)) = cursor.next_raw()? {
            let key_str = String::from_utf8_lossy(key);
            assert!(key_str.starts_with("key"));
            assert!(String::from_utf8_lossy(&value).starts_with("value"));
            count += 1;
        }
        assert_eq!(count, 5);
    }

    Ok(())
}

#[test]
fn test_delete_operations() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())?,
    );

    let db: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Insert entries
    {
        let mut txn = env.write_txn()?;
        for i in 0..3 {
            db.put(&mut txn, format!("key{}", i), format!("value{}", i))?;
        }
        txn.commit()?;
    }

    // Delete an entry
    {
        let mut txn = env.write_txn()?;
        let deleted = db.delete(&mut txn, &"key1".to_string())?;
        assert!(deleted);
        txn.commit()?;
    }

    // Verify deletion
    {
        let txn = env.read_txn()?;
        assert_eq!(db.get(&txn, &"key1".to_string())?, None);
        assert_eq!(db.get(&txn, &"key0".to_string())?, Some("value0".to_string()));
        assert_eq!(db.get(&txn, &"key2".to_string())?, Some("value2".to_string()));
    }

    Ok(())
}

#[test]
fn test_named_database() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())?,
    );

    // Create named database
    let db1: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, Some("db1"))?;
        txn.commit()?;
        db
    };

    // Create another named database
    let db2: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, Some("db2"))?;
        txn.commit()?;
        db
    };

    // Insert different data in each database
    {
        let mut txn = env.write_txn()?;
        db1.put(&mut txn, "key".to_string(), "value1".to_string())?;
        db2.put(&mut txn, "key".to_string(), "value2".to_string())?;
        txn.commit()?;
    }

    // Verify data isolation
    {
        let txn = env.read_txn()?;
        assert_eq!(db1.get(&txn, &"key".to_string())?, Some("value1".to_string()));
        assert_eq!(db2.get(&txn, &"key".to_string())?, Some("value2".to_string()));
    }

    Ok(())
}