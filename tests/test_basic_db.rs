use std::sync::Arc;
use zerodb::{Database, EnvBuilder};

#[test]
fn test_basic_db_operations() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create database
    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Try a simple put/get
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, b"test".to_vec(), b"value".to_vec())?;
        txn.commit()?;
    }

    {
        let txn = env.read_txn()?;
        let val = db.get(&txn, &b"test".to_vec())?;
        assert_eq!(val, Some(b"value".to_vec()));
    }

    Ok(())
}

#[test]
fn test_large_value_overflow() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create database
    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Insert large value that needs overflow
    {
        let mut txn = env.write_txn()?;
        let large = vec![0x42; 5000];
        db.put(&mut txn, b"large".to_vec(), large.clone())?;
        txn.commit()?;
    }

    // Read it back
    {
        let txn = env.read_txn()?;
        let val = db.get(&txn, &b"large".to_vec())?;
        assert!(val.is_some());
        assert_eq!(val.unwrap().len(), 5000);
    }

    Ok(())
}

#[test]
fn test_update_large_value() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create database
    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Insert initial large value
    {
        let mut txn = env.write_txn()?;
        let large = vec![0x42; 5000];
        db.put(&mut txn, b"large".to_vec(), large)?;
        txn.commit()?;
    }

    // Update to larger value
    {
        let mut txn = env.write_txn()?;
        let new_large = vec![0x43; 6000];
        db.put(&mut txn, b"large".to_vec(), new_large)?;
        txn.commit()?;
    }

    // Read updated value
    {
        let txn = env.read_txn()?;
        let val = db.get(&txn, &b"large".to_vec())?;
        assert!(val.is_some());
        let value = val.unwrap();
        assert_eq!(value.len(), 6000);
        assert_eq!(value[0], 0x43);
    }

    Ok(())
}