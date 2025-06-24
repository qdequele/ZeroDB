use std::sync::Arc;
use zerodb::{Database, EnvBuilder};

#[test]
fn test_copy_on_write_basic() -> Result<(), Box<dyn std::error::Error>> {
    // Create temporary directory
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create database
    let db: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Insert data
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, "key1".to_string(), "value1".to_string())?;
        db.put(&mut txn, "key2".to_string(), "value2".to_string())?;
        txn.commit()?;
    }

    // Read data
    {
        let txn = env.read_txn()?;
        let val1 = db.get(&txn, &"key1".to_string())?;
        assert_eq!(val1, Some("value1".to_string()));

        let val2 = db.get(&txn, &"key2".to_string())?;
        assert_eq!(val2, Some("value2".to_string()));
    }

    Ok(())
}

#[test]
fn test_copy_on_write_update() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    let db: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Insert initial data
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, "key1".to_string(), "value1".to_string())?;
        db.put(&mut txn, "key2".to_string(), "value2".to_string())?;
        txn.commit()?;
    }

    // Test update with COW
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, "key1".to_string(), "updated_value1".to_string())?;
        txn.commit()?;
    }

    // Read updated data
    {
        let txn = env.read_txn()?;
        let val1 = db.get(&txn, &"key1".to_string())?;
        assert_eq!(val1, Some("updated_value1".to_string()));
        
        // key2 should remain unchanged
        let val2 = db.get(&txn, &"key2".to_string())?;
        assert_eq!(val2, Some("value2".to_string()));
    }

    Ok(())
}

#[test]
fn test_copy_on_write_isolation() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    let db: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Insert initial data
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, "key1".to_string(), "value1".to_string())?;
        txn.commit()?;
    }

    // Start a read transaction
    let read_txn = env.read_txn()?;
    let initial_value = db.get(&read_txn, &"key1".to_string())?;
    assert_eq!(initial_value, Some("value1".to_string()));

    // Update data in a new write transaction
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, "key1".to_string(), "updated_value1".to_string())?;
        txn.commit()?;
    }

    // The read transaction should still see the old value (isolation)
    let isolated_value = db.get(&read_txn, &"key1".to_string())?;
    assert_eq!(isolated_value, Some("value1".to_string()));

    // Drop the read transaction
    drop(read_txn);

    // New read transaction should see the updated value
    {
        let txn = env.read_txn()?;
        let updated_value = db.get(&txn, &"key1".to_string())?;
        assert_eq!(updated_value, Some("updated_value1".to_string()));
    }

    Ok(())
}