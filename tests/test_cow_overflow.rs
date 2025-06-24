use std::sync::Arc;
use zerodb::{Database, EnvBuilder};

#[test]
fn test_cow_with_overflow_pages() -> Result<(), Box<dyn std::error::Error>> {
    // Create temporary directory
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create database
    let db: Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Insert small value first
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, "small_key".to_string(), vec![1u8; 100])?;
        txn.commit()?;
    }

    // Insert large value that needs overflow
    let large_value = vec![0xAB; 5000]; // 5KB
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, "large_key".to_string(), large_value.clone())?;
        txn.commit()?;
    }

    // Read values back
    {
        let txn = env.read_txn()?;

        let small = db.get(&txn, &"small_key".to_string())?;
        assert!(small.is_some());
        assert_eq!(small.unwrap().len(), 100);

        let large = db.get(&txn, &"large_key".to_string())?;
        assert!(large.is_some());
        assert_eq!(large.unwrap(), large_value);
    }

    Ok(())
}

#[test]
fn test_cow_update_overflow_value() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    let db: Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Insert large value
    let large_value = vec![0xAB; 5000]; // 5KB
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, "large_key".to_string(), large_value.clone())?;
        txn.commit()?;
    }

    // Update large value (test COW with overflow)
    let updated_value = vec![0xCD; 6000]; // 6KB
    {
        let mut txn = env.write_txn()?;

        // First, check what we have before update
        let before = db.get(&txn, &"large_key".to_string())?;
        assert_eq!(before, Some(large_value));

        db.put(&mut txn, "large_key".to_string(), updated_value.clone())?;

        // Check immediately after update (before commit)
        let after = db.get(&txn, &"large_key".to_string())?;
        assert_eq!(after, Some(updated_value.clone()));

        txn.commit()?;
    }

    // Read updated value
    {
        let txn = env.read_txn()?;
        let large = db.get(&txn, &"large_key".to_string())?;
        assert_eq!(large, Some(updated_value));
    }

    Ok(())
}

#[test]
fn test_cow_overflow_isolation() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    let db: Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Insert large value
    let large_value = vec![0xAB; 5000]; // 5KB
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, "large_key".to_string(), large_value.clone())?;
        txn.commit()?;
    }

    // Start a read transaction
    let read_txn = env.read_txn()?;
    let initial_value = db.get(&read_txn, &"large_key".to_string())?;
    assert_eq!(initial_value, Some(large_value.clone()));

    // Update large value in a new write transaction
    let updated_value = vec![0xCD; 6000]; // 6KB
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, "large_key".to_string(), updated_value.clone())?;
        txn.commit()?;
    }

    // The read transaction should still see the old value (isolation)
    let isolated_value = db.get(&read_txn, &"large_key".to_string())?;
    assert_eq!(isolated_value, Some(large_value));

    // Drop the read transaction
    drop(read_txn);

    // New read transaction should see the updated value
    {
        let txn = env.read_txn()?;
        let updated = db.get(&txn, &"large_key".to_string())?;
        assert_eq!(updated, Some(updated_value));
    }

    Ok(())
}