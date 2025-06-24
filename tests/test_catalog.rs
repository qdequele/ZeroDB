//! Test database catalog functionality

use zerodb::db::{Database, DatabaseFlags};
use zerodb::error::Result;
use zerodb::EnvBuilder;

#[test]
fn test_create_named_databases() -> Result<()> {
    // Create a temporary environment
    let dir = tempfile::tempdir().unwrap();
    let env = EnvBuilder::new().map_size(10 * 1024 * 1024).max_dbs(10).open(dir.path())?;

    // Create named databases
    let db1: Database<String, String> = Database::open(&env, Some("users"), DatabaseFlags::CREATE)?;
    let db2: Database<String, String> =
        Database::open(&env, Some("products"), DatabaseFlags::CREATE)?;
    let db3: Database<String, String> =
        Database::open(&env, Some("orders"), DatabaseFlags::CREATE)?;

    // Store data in databases
    {
        let mut txn = env.write_txn()?;

        db1.put(&mut txn, "user1".to_string(), "Alice".to_string())?;
        db1.put(&mut txn, "user2".to_string(), "Bob".to_string())?;

        db2.put(&mut txn, "prod1".to_string(), "Laptop".to_string())?;
        db2.put(&mut txn, "prod2".to_string(), "Mouse".to_string())?;
        db2.put(&mut txn, "prod3".to_string(), "Keyboard".to_string())?;

        db3.put(&mut txn, "order1".to_string(), "user1:prod1".to_string())?;

        txn.commit()?;
    }

    // Read data back
    {
        let txn = env.read_txn()?;

        assert_eq!(db1.get(&txn, &"user1".to_string())?, Some("Alice".to_string()));
        assert_eq!(db2.get(&txn, &"prod2".to_string())?, Some("Mouse".to_string()));
        assert_eq!(db3.get(&txn, &"order1".to_string())?, Some("user1:prod1".to_string()));
    }

    Ok(())
}

#[test]
fn test_database_persistence() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Create and populate databases
    {
        let env = EnvBuilder::new().map_size(10 * 1024 * 1024).max_dbs(10).open(&path)?;

        let db1: Database<String, String> = Database::open(&env, Some("users"), DatabaseFlags::CREATE)?;
        let db2: Database<String, String> =
            Database::open(&env, Some("products"), DatabaseFlags::CREATE)?;

        let mut txn = env.write_txn()?;
        db1.put(&mut txn, "user1".to_string(), "Alice".to_string())?;
        db1.put(&mut txn, "user2".to_string(), "Bob".to_string())?;
        db2.put(&mut txn, "prod1".to_string(), "Laptop".to_string())?;
        db2.put(&mut txn, "prod3".to_string(), "Keyboard".to_string())?;
        txn.commit()?;
    }

    // Reopen environment and verify databases persist
    {
        let env2 = EnvBuilder::new().map_size(10 * 1024 * 1024).max_dbs(10).open(&path)?;

        // Try to open existing databases (without CREATE flag)
        let db1_reopened: Database<String, String> =
            Database::open(&env2, Some("users"), DatabaseFlags::empty())?;
        let db2_reopened: Database<String, String> =
            Database::open(&env2, Some("products"), DatabaseFlags::empty())?;

        // Verify data
        let txn = env2.read_txn()?;
        assert_eq!(db1_reopened.get(&txn, &"user2".to_string())?, Some("Bob".to_string()));
        assert_eq!(db2_reopened.get(&txn, &"prod3".to_string())?, Some("Keyboard".to_string()));
    }

    Ok(())
}

#[test]
fn test_nonexistent_database_error() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let env = EnvBuilder::new().map_size(10 * 1024 * 1024).max_dbs(10).open(dir.path())?;

    // Try to open non-existent database without CREATE
    match Database::<String, String>::open(&env, Some("nonexistent"), DatabaseFlags::empty()) {
        Err(_) => Ok(()), // Expected error
        Ok(_) => panic!("Should have failed to open non-existent database"),
    }
}

#[test]
fn test_list_databases() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let env = EnvBuilder::new().map_size(10 * 1024 * 1024).max_dbs(10).open(dir.path())?;

    // Create several databases
    let _db1: Database<String, String> = Database::open(&env, Some("users"), DatabaseFlags::CREATE)?;
    let _db2: Database<String, String> = Database::open(&env, Some("products"), DatabaseFlags::CREATE)?;
    let _db3: Database<String, String> = Database::open(&env, Some("orders"), DatabaseFlags::CREATE)?;

    // Add some data
    {
        let mut txn = env.write_txn()?;
        _db1.put(&mut txn, "test".to_string(), "value".to_string())?;
        _db2.put(&mut txn, "test".to_string(), "value".to_string())?;
        txn.commit()?;
    }

    // List all databases
    {
        let txn = env.read_txn()?;
        let databases = zerodb::catalog::Catalog::list_databases(&txn)?;

        assert_eq!(databases.len(), 3);
        
        let db_names: Vec<&str> = databases.iter().map(|(name, _)| name.as_str()).collect();
        assert!(db_names.contains(&"users"));
        assert!(db_names.contains(&"products"));
        assert!(db_names.contains(&"orders"));

        // Verify database info
        let users_info = databases.iter()
            .find(|(name, _)| name == "users")
            .map(|(_, info)| info)
            .unwrap();
        assert!(users_info.entries > 0);
    }

    Ok(())
}