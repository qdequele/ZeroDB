use std::sync::Arc;
use zerodb::{Database, EnvBuilder};

#[test]
fn test_initial_db_creation() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create database
    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Verify database was created by checking we can use it
    {
        let txn = env.read_txn()?;
        let count = db.len(&txn)?;
        assert_eq!(count, 0);
    }

    Ok(())
}

#[test]
fn test_simple_put_get() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create database
    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Try simple put/get
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, b"test".to_vec(), b"value".to_vec())?;
        txn.commit()?;
    }

    // Read it back
    {
        let txn = env.read_txn()?;
        let val = db.get(&txn, &b"test".to_vec())?;
        assert_eq!(val, Some(b"value".to_vec()));
    }

    Ok(())
}

#[test]
fn test_large_value_insert() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create database
    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Insert simple value first
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, b"test".to_vec(), b"value".to_vec())?;
        txn.commit()?;
    }

    // Now try large value
    {
        let large = vec![0x42; 5000];
        
        // Get db info before insert
        let db_info_before = {
            let txn = env.read_txn()?;
            *txn.db_info(None)?
        };

        // Insert the large value
        {
            let mut txn = env.write_txn()?;
            db.put(&mut txn, b"large".to_vec(), large.clone())?;
            txn.commit()?;
        }

        // Get db info after insert
        let db_info_after = {
            let txn = env.read_txn()?;
            *txn.db_info(None)?
        };
        
        assert_eq!(db_info_after.entries, db_info_before.entries + 1);
    }

    // Verify large value can be read back
    {
        let txn = env.read_txn()?;
        let val = db.get(&txn, &b"large".to_vec())?;
        assert!(val.is_some());
        assert_eq!(val.unwrap().len(), 5000);
    }

    Ok(())
}