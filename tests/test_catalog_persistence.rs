use std::sync::Arc;
use tempfile::TempDir;
use zerodb::db::{Database, DatabaseFlags};
use zerodb::env::EnvBuilder;

#[test]
fn test_catalog_persistence_database_open() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let db_path = dir.path().to_path_buf();

    // Phase 1: Create databases using Database::open (which uses Catalog)
    {
        let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(&db_path)?);

        // Use Database::open which uses the Catalog
        let db1: Database<String, String> =
            Database::open(&env, Some("catalog_db1"), DatabaseFlags::CREATE)?;
        let db2: Database<String, String> =
            Database::open(&env, Some("catalog_db2"), DatabaseFlags::CREATE)?;

        // Add some data
        {
            let mut txn = env.write_txn()?;
            db1.put(&mut txn, "key1".to_string(), "value1".to_string())?;
            db2.put(&mut txn, "key2".to_string(), "value2".to_string())?;
            txn.commit()?;
        }
    }

    // Phase 2: Reopen and try to access the databases
    {
        let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(&db_path)?);

        // Try to open with Database::open (should work)
        let db = Database::<String, String>::open(&env, Some("catalog_db1"), DatabaseFlags::empty())?;
        let txn = env.read_txn()?;
        let val = db.get(&txn, &"key1".to_string())?;
        assert_eq!(val, Some("value1".to_string()));
    }

    Ok(())
}

#[test]
fn test_catalog_persistence_mixed_methods() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let db_path = dir.path().to_path_buf();

    // Create databases using both methods
    {
        let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(&db_path)?);

        // Create with Database::open (Catalog)
        let db1: Database<String, String> =
            Database::open(&env, Some("catalog_db"), DatabaseFlags::CREATE)?;

        // Create with env.create_database
        let db2: Database<String, String> = {
            let mut txn = env.write_txn()?;
            let db = env.create_database(&mut txn, Some("env_db"))?;
            txn.commit()?;
            db
        };

        // Add data to both
        {
            let mut txn = env.write_txn()?;
            db1.put(&mut txn, "key1".to_string(), "value1".to_string())?;
            db2.put(&mut txn, "key2".to_string(), "value2".to_string())?;
            txn.commit()?;
        }
    }

    // Reopen and verify both databases
    {
        let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(&db_path)?);

        // Open catalog database
        let db1 = Database::<String, String>::open(&env, Some("catalog_db"), DatabaseFlags::empty())?;
        
        // Open env database
        let db2 = Database::<String, String>::open(&env, Some("env_db"), DatabaseFlags::empty())?;

        let txn = env.read_txn()?;
        assert_eq!(db1.get(&txn, &"key1".to_string())?, Some("value1".to_string()));
        assert_eq!(db2.get(&txn, &"key2".to_string())?, Some("value2".to_string()));
    }

    Ok(())
}

#[test]
fn test_catalog_list_databases() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let db_path = dir.path().to_path_buf();

    // Create various databases
    {
        let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(&db_path)?);

        // Create with Database::open
        let _db1: Database<String, String> =
            Database::open(&env, Some("catalog_db1"), DatabaseFlags::CREATE)?;
        let _db2: Database<String, String> =
            Database::open(&env, Some("catalog_db2"), DatabaseFlags::CREATE)?;

        // Create with env.create_database
        {
            let mut txn = env.write_txn()?;
            let _db3: Database<String, String> = env.create_database(&mut txn, Some("env_db1"))?;
            let _db4: Database<String, String> = env.create_database(&mut txn, Some("env_db2"))?;
            txn.commit()?;
        }
    }

    // Reopen and list all databases
    {
        let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(&db_path)?);
        let txn = env.read_txn()?;

        let dbs = env.list_databases(&txn)?;
        assert!(dbs.len() >= 4);
        assert!(dbs.contains(&"catalog_db1".to_string()));
        assert!(dbs.contains(&"catalog_db2".to_string()));
        assert!(dbs.contains(&"env_db1".to_string()));
        assert!(dbs.contains(&"env_db2".to_string()));
    }

    Ok(())
}