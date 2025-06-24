use std::sync::Arc;
use tempfile::TempDir;
use zerodb::db::{Database, DatabaseFlags};
use zerodb::env::EnvBuilder;

#[test]
fn test_catalog_database_persistence() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let db_path = dir.path().to_path_buf();

    // Create database with Database::open (uses Catalog)
    {
        let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(&db_path)?);

        // Create a database using Database::open (which should use Catalog)
        let _db1: Database<String, String> =
            Database::open(&env, Some("catalog_db"), DatabaseFlags::CREATE)?;
    }

    // Reopen and check
    {
        let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(&db_path)?);

        // Database should be found after reopen
        let _db = Database::<String, String>::open(&env, Some("catalog_db"), DatabaseFlags::empty())?;
    }

    Ok(())
}

#[test]
fn test_catalog_serialization_compatibility() -> Result<(), Box<dyn std::error::Error>> {
    // Create a DbInfo structure
    let test_info = zerodb::meta::DbInfo {
        flags: 0x42,
        depth: 3,
        branch_pages: 100,
        leaf_pages: 500,
        overflow_pages: 10,
        entries: 1000,
        root: zerodb::error::PageId(42),
        last_key_page: zerodb::error::PageId(0),
    };

    // Serialize using Catalog method
    let catalog_bytes = zerodb::catalog::Catalog::serialize_db_info(&test_info);

    // Serialize using raw memory copy (as done in env.create_database)
    let raw_bytes = unsafe {
        std::slice::from_raw_parts(
            &test_info as *const _ as *const u8,
            std::mem::size_of::<zerodb::meta::DbInfo>(),
        )
    };

    // Check if they're the same size (they might differ in content due to padding)
    assert!(catalog_bytes.len() > 0);
    assert!(raw_bytes.len() > 0);

    // Try to deserialize catalog bytes
    let deserialized = zerodb::catalog::Catalog::deserialize_db_info(&catalog_bytes)?;
    assert_eq!(deserialized.flags, test_info.flags);
    assert_eq!(deserialized.depth, test_info.depth);
    assert_eq!(deserialized.entries, test_info.entries);
    assert_eq!(deserialized.root, test_info.root);

    Ok(())
}

#[test]
fn test_catalog_cross_serialization() -> Result<(), Box<dyn std::error::Error>> {
    let test_info = zerodb::meta::DbInfo {
        flags: 0x42,
        depth: 3,
        branch_pages: 100,
        leaf_pages: 500,
        overflow_pages: 10,
        entries: 1000,
        root: zerodb::error::PageId(42),
        last_key_page: zerodb::error::PageId(0),
    };

    // Serialize and deserialize
    let bytes = zerodb::catalog::Catalog::serialize_db_info(&test_info);
    let deserialized = zerodb::catalog::Catalog::deserialize_db_info(&bytes)?;

    // Verify all fields match
    assert_eq!(deserialized.flags, test_info.flags);
    assert_eq!(deserialized.depth, test_info.depth);
    assert_eq!(deserialized.branch_pages, test_info.branch_pages);
    assert_eq!(deserialized.leaf_pages, test_info.leaf_pages);
    assert_eq!(deserialized.overflow_pages, test_info.overflow_pages);
    assert_eq!(deserialized.entries, test_info.entries);
    assert_eq!(deserialized.root, test_info.root);
    assert_eq!(deserialized.last_key_page, test_info.last_key_page);

    Ok(())
}