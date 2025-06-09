use std::path::Path;
use zerodb::{
    env::EnvBuilder,
    error::Result,
    btree::BTree,
    comparator::LexicographicComparator,
    meta::DbInfo,
};
use tempfile::TempDir;

fn main() -> Result<()> {
    // Create a temporary directory for the database
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path();

    println!("Creating database at: {:?}", path);

    // Create environment with small page size to trigger splits more easily
    let env = EnvBuilder::new()
        .map_size(10 * 1024 * 1024) // 10MB
        .open(path)?;

    // Begin write transaction
    let mut txn = env.begin_write_txn()?;
    
    // Initialize database info
    let mut db_info = DbInfo::default();
    db_info.root = zerodb::error::PageId(3); // Main DB root
    db_info.leaf_pages = 1;
    let mut root = db_info.root;

    println!("Initial root: {:?}", root);

    // Insert many keys with small values to create a dense tree
    // This should trigger multiple levels of splits
    let num_keys = 1000;
    for i in 0..num_keys {
        // Use keys that will create a dense branch structure
        let key = format!("{:08}", i); // Fixed width for predictable ordering
        let value = format!("v{}", i);
        
        println!("Inserting key: {}", key);
        
        match BTree::<LexicographicComparator>::insert(
            &mut txn,
            &mut root,
            &mut db_info,
            key.as_bytes(),
            value.as_bytes(),
        ) {
            Ok(_) => {
                if root != db_info.root {
                    println!("Root changed from {:?} to {:?} at key {}", db_info.root, root, i);
                    db_info.root = root;
                }
            }
            Err(e) => {
                eprintln!("Error inserting key {} at index {}: {:?}", key, i, e);
                return Err(e);
            }
        }
    }

    println!("\nFinal statistics:");
    println!("Total entries: {}", db_info.entries);
    println!("Tree depth: {}", db_info.depth);
    println!("Branch pages: {}", db_info.branch_pages);
    println!("Leaf pages: {}", db_info.leaf_pages);
    println!("Final root: {:?}", root);

    // Commit the transaction
    txn.commit()?;

    // Now verify the tree by searching for all keys
    println!("\nVerifying all keys...");
    let txn = env.begin_txn()?;
    let mut missing_keys = Vec::new();
    
    for i in 0..num_keys {
        let key = format!("{:08}", i);
        match BTree::<LexicographicComparator>::search(&txn, root, key.as_bytes())? {
            Some(value) => {
                let expected = format!("v{}", i);
                if value.as_ref() != expected.as_bytes() {
                    println!("Key {} has wrong value: {:?}", key, String::from_utf8_lossy(&value));
                }
            }
            None => {
                missing_keys.push(key);
            }
        }
    }

    if !missing_keys.is_empty() {
        println!("\nMissing {} keys:", missing_keys.len());
        for key in &missing_keys[..10.min(missing_keys.len())] {
            println!("  - {}", key);
        }
        if missing_keys.len() > 10 {
            println!("  ... and {} more", missing_keys.len() - 10);
        }
    } else {
        println!("\nAll {} keys verified successfully!", num_keys);
    }

    Ok(())
}