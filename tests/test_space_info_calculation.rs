//! Test that the space_info calculation is working correctly

use std::sync::Arc;
use zerodb::{db::Database, EnvBuilder};

#[test]
fn test_space_info_calculation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .open(temp_dir.path())
            .unwrap(),
    );

    // Check initial state
    let info = env.space_info().unwrap();
    println!("Initial state:");
    println!("  Total pages: {}", info.total_pages);
    println!("  Used pages: {}", info.used_pages);
    println!("  Free pages: {}", info.free_pages);
    println!("  Percent used: {:.2}%", info.percent_used);
    println!("  Percent of map used: {:.2}%", info.percent_of_map_used);
    
    assert_eq!(info.total_pages, 2560); // 10MB / 4KB
    assert!(info.used_pages < 10); // Should be minimal (just meta pages)
    assert!(info.percent_of_map_used < 1.0); // Should be less than 1%
    
    // Create database and insert data
    let db = {
        let mut txn = env.write_txn().unwrap();
        let db: Database<String, String> = env.create_database(&mut txn, None).unwrap();
        txn.commit().unwrap();
        db
    };
    
    // Insert some data
    {
        let mut txn = env.write_txn().unwrap();
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let value = "x".repeat(1000); // 1KB values
            db.put(&mut txn, key, value).unwrap();
        }
        txn.commit().unwrap();
    }
    
    // Check after inserting data
    let info = env.space_info().unwrap();
    println!("\nAfter inserting 100 x 1KB entries:");
    println!("  Total pages: {}", info.total_pages);
    println!("  Used pages: {}", info.used_pages);
    println!("  Free pages: {}", info.free_pages);
    println!("  Percent used: {:.2}%", info.percent_used);
    println!("  Percent of map used: {:.2}%", info.percent_of_map_used);
    
    // Should have used some pages but not too many
    assert!(info.used_pages > 10); // Should have used more than initial
    assert!(info.used_pages < 150); // Reasonable for 100KB of data + B-tree overhead
    assert!(info.percent_of_map_used > 0.0);
    assert!(info.percent_of_map_used < 10.0); // Should be less than 10%
    
    // Test pages_remaining calculation
    let remaining = info.pages_remaining();
    assert_eq!(remaining, 2560 - info.used_pages);
    
    // Test estimate_entries_remaining
    let estimated = info.estimate_entries_remaining(1024); // 1KB entries
    assert!(estimated > 0);
    assert!(estimated < 2560); // Can't fit more entries than pages
}

#[test]
fn test_space_info_near_capacity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(1 * 1024 * 1024) // 1MB - very small
            .open(temp_dir.path())
            .unwrap(),
    );
    
    let info = env.space_info().unwrap();
    assert_eq!(info.total_pages, 256); // 1MB / 4KB
    
    // Create a mock SpaceInfo near capacity
    let near_capacity = zerodb::space_info::SpaceInfo::new(
        256,  // total_pages
        200,  // used_pages
        56,   // free_pages
        1 * 1024 * 1024, // map_size
    );
    
    assert!(!near_capacity.is_near_capacity(80.0)); // 78% < 80%
    assert!(near_capacity.is_near_capacity(75.0));  // 78% > 75%
    assert!(near_capacity.is_near_capacity(50.0));  // 78% > 50%
}