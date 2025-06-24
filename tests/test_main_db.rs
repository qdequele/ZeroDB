//! Test the main database first

use zerodb::db::{Database, DatabaseFlags};
use zerodb::error::Result;
use zerodb::EnvBuilder;

#[test]
fn test_main_database_functionality() -> Result<()> {
    // Create environment
    let dir = tempfile::tempdir().unwrap();
    let env = EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?;

    // Open the main database (no name)
    let main_db: Database<Vec<u8>, Vec<u8>> = Database::open(&env, None, DatabaseFlags::empty())?;

    // Store some data in main database
    {
        let mut txn = env.write_txn()?;

        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        main_db.put(&mut txn, key.clone(), value)?;

        // Check we can read it back in same transaction
        let retrieved = main_db.get(&txn, &key)?;
        assert_eq!(retrieved, Some(b"test_value".to_vec()));

        txn.commit()?;
    }

    // Read it back in new transaction
    {
        let txn = env.read_txn()?;

        let key = b"test_key".to_vec();
        let value = main_db.get(&txn, &key)?;
        assert_eq!(value, Some(b"test_value".to_vec()));
    }

    Ok(())
}

#[test]
fn test_named_database_creation() -> Result<()> {
    // Create environment
    let dir = tempfile::tempdir().unwrap();
    let env = EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?;

    // Create a named database
    let named_db: Database<Vec<u8>, Vec<u8>> =
        Database::open(&env, Some("mydb"), DatabaseFlags::CREATE)?;

    // Try to use the named database
    {
        let mut txn = env.write_txn()?;

        let key = b"named_key".to_vec();
        let value = b"named_value".to_vec();

        named_db.put(&mut txn, key.clone(), value)?;

        // Verify in same transaction
        let retrieved = named_db.get(&txn, &key)?;
        assert_eq!(retrieved, Some(b"named_value".to_vec()));

        txn.commit()?;
    }

    // Verify after commit
    {
        let txn = env.read_txn()?;
        let key = b"named_key".to_vec();
        let value = named_db.get(&txn, &key)?;
        assert_eq!(value, Some(b"named_value".to_vec()));
    }

    Ok(())
}