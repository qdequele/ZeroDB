//! Test B+Tree rebalancing

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::btree::BTree;
use zerodb::comparator::LexicographicComparator;
use zerodb::db::Database;
use zerodb::env::EnvBuilder;
use zerodb::error::PageId;
use zerodb::meta::DbInfo;

#[test]
fn test_btree_rebalancing_after_deletions() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create a database
    let _db: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, Some("test_db"))?;
        txn.commit()?;
        db
    };

    // Track statistics
    let mut root = PageId(3);
    let mut db_info = DbInfo {
        root,
        leaf_pages: 1,
        ..DbInfo::default()
    };

    // Insert a lot of entries to create a multi-level tree
    {
        let mut txn = env.write_txn()?;

        for i in 0..200 {
            let key = format!("key_{:04}", i.to_string());
            let value = format!("value_{:04}", i.to_string());
            BTree::<LexicographicComparator>::insert(
                &mut txn,
                &mut root,
                &mut db_info,
                key.as_bytes(),
                value.as_bytes(),
            )?;
        }

        assert_eq!(db_info.entries, 200);
        assert!(db_info.depth > 1); // Should have created a multi-level tree
        
        txn.commit()?;
    }

    let _initial_depth = db_info.depth;
    let initial_leaf_pages = db_info.leaf_pages;

    // Delete every other entry to trigger rebalancing
    {
        let mut txn = env.write_txn()?;

        for i in (0..200).step_by(2) {
            let key = format!("key_{:04}", i.to_string());
            BTree::<LexicographicComparator>::delete(
                &mut txn,
                &mut root,
                &mut db_info,
                key.as_bytes(),
            )?;
        }

        assert_eq!(db_info.entries, 100);
        // After deleting half the entries, we might have fewer pages
        assert!(db_info.leaf_pages <= initial_leaf_pages);

        txn.commit()?;
    }

    // Verify remaining entries
    {
        let txn = env.read_txn()?;

        for i in 0..200 {
            let key = format!("key_{:04}", i.to_string());
            match BTree::<LexicographicComparator>::search(&txn, root, key.as_bytes())? {
                Some(_) => {
                    assert!(i % 2 == 1, "Found deleted key: {}", key);
                }
                None => {
                    assert!(i % 2 == 0, "Missing key: {}", key);
                }
            }
        }
    }

    Ok(())
}

#[test]
fn test_btree_rebalancing_tree_shrinkage() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create a database
    let _db: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, Some("test_db"))?;
        txn.commit()?;
        db
    };

    let mut root = PageId(3);
    let mut db_info = DbInfo {
        root,
        leaf_pages: 1,
        ..DbInfo::default()
    };

    // Insert entries
    {
        let mut txn = env.write_txn()?;

        for i in 0..200 {
            let key = format!("key_{:04}", i.to_string());
            let value = format!("value_{:04}", i.to_string());
            BTree::<LexicographicComparator>::insert(
                &mut txn,
                &mut root,
                &mut db_info,
                key.as_bytes(),
                value.as_bytes(),
            )?;
        }

        txn.commit()?;
    }

    // Delete most entries to potentially reduce tree depth
    {
        let mut txn = env.write_txn()?;

        // Delete entries to leave only 20
        for i in 0..180 {
            let key = format!("key_{:04}", i.to_string());
            BTree::<LexicographicComparator>::delete(
                &mut txn,
                &mut root,
                &mut db_info,
                key.as_bytes(),
            )?;
        }

        assert_eq!(db_info.entries, 20);
        // Tree should have shrunk
        assert!(db_info.leaf_pages < 10);

        txn.commit()?;
    }

    // Verify remaining entries
    {
        let txn = env.read_txn()?;

        for i in 180..200 {
            let key = format!("key_{:04}", i.to_string());
            let result = BTree::<LexicographicComparator>::search(&txn, root, key.as_bytes())?;
            assert!(result.is_some(), "Missing key: {}", key);
        }
    }

    Ok(())
}

#[test]
fn test_btree_rebalancing_regrowth() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Create a database
    let _db: Database<String, String> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, Some("test_db"))?;
        txn.commit()?;
        db
    };

    let mut root = PageId(3);
    let mut db_info = DbInfo {
        root,
        leaf_pages: 1,
        ..DbInfo::default()
    };

    // Insert and then delete to create a small tree
    {
        let mut txn = env.write_txn()?;

        // Initial insert
        for i in 0..100 {
            let key = format!("key_{:04}", i.to_string());
            let value = format!("value_{:04}", i.to_string());
            BTree::<LexicographicComparator>::insert(
                &mut txn,
                &mut root,
                &mut db_info,
                key.as_bytes(),
                value.as_bytes(),
            )?;
        }

        // Delete most
        for i in 0..90 {
            let key = format!("key_{:04}", i.to_string());
            BTree::<LexicographicComparator>::delete(
                &mut txn,
                &mut root,
                &mut db_info,
                key.as_bytes(),
            )?;
        }

        txn.commit()?;
    }

    let small_tree_entries = db_info.entries;
    let small_tree_depth = db_info.depth;

    // Insert new entries to test that the tree can grow again
    {
        let mut txn = env.write_txn()?;

        for i in 300..400 {
            let key = format!("key_{:04}", i.to_string());
            let value = format!("new_value_{:04}", i.to_string());
            BTree::<LexicographicComparator>::insert(
                &mut txn,
                &mut root,
                &mut db_info,
                key.as_bytes(),
                value.as_bytes(),
            )?;
        }

        assert_eq!(db_info.entries, small_tree_entries + 100);
        // Tree might have grown in depth
        assert!(db_info.depth >= small_tree_depth);

        txn.commit()?;
    }

    // Verify all entries
    {
        let txn = env.read_txn()?;

        // Check remaining original entries
        for i in 90..100 {
            let key = format!("key_{:04}", i.to_string());
            let result = BTree::<LexicographicComparator>::search(&txn, root, key.as_bytes())?;
            assert!(result.is_some(), "Missing original key: {}", key);
        }

        // Check new entries
        for i in 300..400 {
            let key = format!("key_{:04}", i.to_string());
            let result = BTree::<LexicographicComparator>::search(&txn, root, key.as_bytes())?;
            assert!(result.is_some(), "Missing new key: {}", key);
        }
    }

    Ok(())
}