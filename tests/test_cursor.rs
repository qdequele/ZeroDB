//! Test cursor operations thoroughly

use std::sync::Arc;
use zerodb::error::Result;
use zerodb::{Database, EnvBuilder};

#[test]
fn test_cursor_forward_iteration() -> Result<()> {
    // Create environment
    let dir = tempfile::tempdir().unwrap();
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create database with test data
    let db: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;

        // Insert data in non-sequential order to test sorting
        let data = vec![
            ("key05", "value05"),
            ("key02", "value02"),
            ("key08", "value08"),
            ("key01", "value01"),
            ("key04", "value04"),
            ("key09", "value09"),
            ("key03", "value03"),
            ("key07", "value07"),
            ("key06", "value06"),
        ];

        for (k, v) in data {
            db.put(&mut txn, k.to_string(), v.to_string())?;
        }

        txn.commit()?;
        db
    };

    // Test forward iteration
    {
        let txn = env.read_txn()?;
        let mut cursor = db.cursor(&txn)?;

        let mut keys = Vec::new();
        if let Some((k, _v)) = cursor.first()? {
            keys.push(String::from_utf8_lossy(&k).to_string());
        }

        while let Some((k, _v)) = cursor.next_entry()? {
            keys.push(String::from_utf8_lossy(&k).to_string());
        }

        // Verify keys are in sorted order
        let expected: Vec<String> = (1..=9).map(|i| format!("key0{}", i)).collect();
        assert_eq!(keys, expected);
    }

    Ok(())
}

#[test]
fn test_cursor_backward_iteration() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    let db: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;

        for i in 1..=5 {
            db.put(&mut txn, format!("key{:02}", i), format!("value{:02}", i))?;
        }

        txn.commit()?;
        db
    };

    // Test backward iteration
    {
        let txn = env.read_txn()?;
        let mut cursor = db.cursor(&txn)?;

        let mut keys = Vec::new();
        if let Some((k, _v)) = cursor.last()? {
            keys.push(String::from_utf8_lossy(&k).to_string());
        }

        while let Some((k, _v)) = cursor.prev()? {
            keys.push(String::from_utf8_lossy(&k).to_string());
        }

        // Verify keys are in reverse sorted order
        let expected: Vec<String> = (1..=5).rev().map(|i| format!("key{:02}", i)).collect();
        assert_eq!(keys, expected);
    }

    Ok(())
}

#[test]
fn test_cursor_seek_operations() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    let db: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;

        for i in [1, 3, 5, 7, 9] {
            db.put(&mut txn, format!("key{:02}", i), format!("value{:02}", i))?;
        }

        txn.commit()?;
        db
    };

    {
        let txn = env.read_txn()?;
        let mut cursor = db.cursor(&txn)?;

        // Seek to existing key
        let result = cursor.seek(&"key05".to_string())?;
        assert_eq!(
            result.map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v)),
            Some(("key05".to_string(), "value05".to_string()))
        );

        // Seek to non-existing key (should find next)
        let result = cursor.seek(&"key06".to_string())?;
        assert_eq!(
            result.map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v)),
            Some(("key07".to_string(), "value07".to_string()))
        );

        // Seek before first
        let result = cursor.seek(&"key00".to_string())?;
        assert_eq!(
            result.map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v)),
            Some(("key01".to_string(), "value01".to_string()))
        );

        // Seek after last
        let result = cursor.seek(&"key99".to_string())?;
        assert_eq!(result, None);
    }

    Ok(())
}

#[test]
fn test_cursor_mixed_navigation() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    let db: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;

        for i in 1..=9 {
            db.put(&mut txn, format!("key{:02}", i), format!("value{:02}", i))?;
        }

        txn.commit()?;
        db
    };

    {
        let txn = env.read_txn()?;
        let mut cursor = db.cursor(&txn)?;

        // Start at first
        cursor.first()?;
        assert_eq!(
            cursor.current()?.map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v)),
            Some(("key01".to_string(), "value01".to_string()))
        );

        // Move forward twice
        cursor.next_raw()?;
        cursor.next_raw()?;
        assert_eq!(
            cursor.current()?.map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v)),
            Some(("key03".to_string(), "value03".to_string()))
        );

        // Move back once
        cursor.prev()?;
        assert_eq!(
            cursor.current()?.map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v)),
            Some(("key02".to_string(), "value02".to_string()))
        );

        // Jump to middle
        cursor.seek(&"key05".to_string())?;
        assert_eq!(
            cursor.current()?.map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v)),
            Some(("key05".to_string(), "value05".to_string()))
        );
    }

    Ok(())
}

#[test]
fn test_cursor_edge_cases() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    let db: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;

        for i in 1..=3 {
            db.put(&mut txn, format!("key{}", i), format!("value{}", i))?;
        }

        txn.commit()?;
        db
    };

    {
        let txn = env.read_txn()?;
        let mut cursor = db.cursor(&txn)?;

        // Multiple prev from first
        cursor.first()?;
        let result = cursor.prev()?;
        assert_eq!(result, None);

        // Multiple next from last
        cursor.last()?;
        let result = cursor.next_raw()?;
        assert_eq!(result, None);

        // Current with no position
        let fresh_cursor = db.cursor(&txn)?;
        let result = fresh_cursor.current()?;
        assert_eq!(result, None);
    }

    Ok(())
}

#[test]
fn test_cursor_modifications() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    let db: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;

        for i in [1, 3, 5, 7, 9] {
            db.put(&mut txn, format!("key{:02}", i), format!("value{:02}", i))?;
        }

        txn.commit()?;
        db
    };

    {
        let txn = env.write_txn()?;
        let mut cursor = db.cursor(&txn)?;

        // Add new entry
        cursor.put(&"key04".to_string(), &"value04".to_string())?;
        assert_eq!(
            cursor.current()?.map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v)),
            Some(("key04".to_string(), "value04".to_string()))
        );

        // Navigate and update
        cursor.seek(&"key03".to_string())?;
        cursor.update(&"updated_value03".to_string())?;
        assert_eq!(
            cursor.current()?.map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v)),
            Some(("key03".to_string(), "updated_value03".to_string()))
        );

        // Navigate and delete
        cursor.seek(&"key07".to_string())?;
        let deleted = cursor.delete()?;
        assert!(deleted);

        txn.commit()?;
    }

    // Verify modifications
    {
        let txn = env.read_txn()?;

        // Check added key
        assert_eq!(db.get(&txn, &"key04".to_string())?, Some("value04".to_string()));

        // Check updated key
        assert_eq!(db.get(&txn, &"key03".to_string())?, Some("updated_value03".to_string()));

        // Check deleted key
        assert_eq!(db.get(&txn, &"key07".to_string())?, None);
    }

    Ok(())
}