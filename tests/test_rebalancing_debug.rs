//! Debug B+Tree rebalancing

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::btree::BTree;
use zerodb::comparator::LexicographicComparator;
use zerodb::env::EnvBuilder;
use zerodb::error::PageId;
use zerodb::meta::DbInfo;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing B+Tree rebalancing with debug...\n");

    let dir = TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path())?);

    // Track statistics
    let mut root = PageId(3);
    let mut db_info = DbInfo {
        root,
        leaf_pages: 1,
        ..Default::default()
    };

    // Insert entries to create a decent tree
    println!("1. Inserting 50 entries...");
    {
        let mut txn = env.write_txn()?;

        for i in 0..50 {
            let key = format!("key_{:04}", i.to_string());
            let value = format!("value_{:04}", i.to_string());
            match BTree::<LexicographicComparator>::insert(
                &mut txn,
                &mut root,
                &mut db_info,
                key.as_bytes(),
                value.as_bytes(),
            ) {
                Ok(_) => {}
                Err(e) => {
                    println!("ERROR inserting key_{:04}: {:?}", i, e);
                    return Err(Box::new(e));
                }
            }
        }

        println!("   Entries: {}", db_info.entries);
        println!("   Depth: {}", db_info.depth);
        println!("   Branch pages: {}", db_info.branch_pages);
        println!("   Leaf pages: {}", db_info.leaf_pages);

        txn.commit()?;
    }

    // Delete some entries to trigger rebalancing
    println!("\n2. Deleting entries 10-15 to trigger rebalancing...");
    {
        let mut txn = env.write_txn()?;

        for i in 10..16 {
            let key = format!("key_{:04}", i.to_string());
            println!("   Deleting {}", key);
            match BTree::<LexicographicComparator>::delete(
                &mut txn,
                &mut root,
                &mut db_info,
                key.as_bytes(),
            ) {
                Ok(_) => {}
                Err(e) => {
                    println!("   ERROR deleting {}: {:?}", key, e);
                    return Err(Box::new(e));
                }
            }
        }

        println!("   Entries after deletion: {}", db_info.entries);

        txn.commit()?;
    }

    println!("\nDebug test completed!");

    Ok(())
}
