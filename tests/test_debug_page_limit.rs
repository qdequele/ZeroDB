//! Debug tests to validate page limit hypothesis for InvalidPageId error

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{db::Database, EnvBuilder};

const KB: usize = 1024;
const MB: usize = 1024 * KB;
const GB: usize = 1024 * MB;
const PAGE_SIZE: usize = 4096;

#[test]
fn test_validate_page_limit_hypothesis() {
    // Test our hypothesis that 2GB map = 524,288 pages max
    let map_size = 2 * GB;
    let expected_max_pages = map_size / PAGE_SIZE;
    
    println!("\n=== Page Limit Hypothesis Test ===");
    println!("Map size: {} bytes ({} GB)", map_size, map_size / GB);
    println!("Page size: {} bytes", PAGE_SIZE);
    println!("Expected max pages: {} (0x{:X})", expected_max_pages, expected_max_pages);
    println!("Error PageId: 524802 (0x{:X})", 524802);
    println!("Pages over limit: {}", 524802 - expected_max_pages);
    
    assert_eq!(expected_max_pages, 524288);
    assert_eq!(expected_max_pages, 0x80000);
    assert!(524802 > expected_max_pages);
}

#[test]
fn test_calculate_failure_point() {
    println!("\n=== Failure Point Calculation ===");
    
    let map_size = 2 * GB;
    let value_size = 100 * KB;
    let max_pages = map_size / PAGE_SIZE;
    
    // Account for overhead (meta pages, catalog, freelist, btree nodes)
    let overhead_estimate = 1000; // Conservative estimate
    let available_pages = max_pages - overhead_estimate;
    
    // Each 100KB value needs this many overflow pages
    let pages_per_value = (value_size + PAGE_SIZE - 1) / PAGE_SIZE;
    
    // Theoretical maximum keys
    let max_keys = available_pages / pages_per_value;
    
    println!("Map size: {} GB", map_size / GB);
    println!("Max pages: {}", max_pages);
    println!("Overhead estimate: {} pages", overhead_estimate);
    println!("Available for data: {} pages", available_pages);
    println!("Pages per 100KB value: {}", pages_per_value);
    println!("Theoretical max keys: {}", max_keys);
    println!("Actual failure at key: 20976");
    println!("Difference: {} keys", max_keys as i32 - 20976);
}

#[test]
fn test_with_4gb_map_size() {
    println!("\n=== Testing with 4GB Map Size ===");
    
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(4 * GB) // Double the original size
            .open(dir.path())
            .unwrap(),
    );
    
    let db = {
        let mut txn = env.write_txn().unwrap();
        let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
        txn.commit().unwrap();
        db
    };
    
    // Try to insert past the 2GB failure point
    let value = vec![0u8; 100 * KB];
    let mut txn = env.write_txn().unwrap();
    
    println!("Inserting keys with 4GB map size...");
    for i in 20970..20985 {
        let key = format!("key_{:08}", i).into_bytes();
        match db.put(&mut txn, key.clone(), value.clone()) {
            Ok(_) => {
                if i % 5 == 0 {
                    println!("✓ Key {} inserted successfully", i);
                }
            }
            Err(e) => {
                eprintln!("✗ Error at key {}: {:?}", i, e);
                panic!("Should not fail with 4GB map size");
            }
        }
    }
    
    println!("✓ All keys inserted successfully with 4GB map!");
    txn.commit().unwrap();
}

#[test]
fn test_page_usage_monitoring() {
    println!("\n=== Page Usage Monitoring Test ===");
    
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(1 * GB) // Smaller size to hit limit faster
            .open(dir.path())
            .unwrap(),
    );
    
    let db = {
        let mut txn = env.write_txn().unwrap();
        let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
        txn.commit().unwrap();
        db
    };
    
    let value_size = 100 * KB;
    let value = vec![0u8; value_size];
    let max_pages = (1 * GB) / PAGE_SIZE;
    
    println!("Map size: 1 GB ({} pages)", max_pages);
    println!("Value size: {} KB ({} pages per value)", 
             value_size / KB, (value_size + PAGE_SIZE - 1) / PAGE_SIZE);
    
    let mut last_successful_key = 0;
    
    // Insert until we hit an error
    let mut txn = env.write_txn().unwrap();
    for i in 0..20000 {
        let key = format!("key_{:08}", i).into_bytes();
        match db.put(&mut txn, key.clone(), value.clone()) {
            Ok(_) => {
                last_successful_key = i;
                if i % 1000 == 0 {
                    let pages_used = (i + 1) * ((value_size + PAGE_SIZE - 1) / PAGE_SIZE);
                    let percent_used = (pages_used as f64 / max_pages as f64) * 100.0;
                    println!("Keys: {}, Approx pages used: {} ({:.1}%)", 
                             i + 1, pages_used, percent_used);
                }
            }
            Err(e) => {
                println!("\n✗ Error at key {}: {:?}", i, e);
                println!("Last successful key: {}", last_successful_key);
                
                let pages_used = i * ((value_size + PAGE_SIZE - 1) / PAGE_SIZE);
                let percent_used = (pages_used as f64 / max_pages as f64) * 100.0;
                println!("Approximate pages used at failure: {} ({:.1}%)", 
                         pages_used, percent_used);
                break;
            }
        }
    }
}

#[test]
fn test_exact_space_calculation() {
    println!("\n=== Exact Space Requirements ===");
    
    let scenarios = vec![
        (1 * GB, 10 * KB, 10_000),
        (1 * GB, 50 * KB, 10_000),
        (2 * GB, 100 * KB, 20_000),
        (2 * GB, 100 * KB, 25_000),
        (4 * GB, 100 * KB, 40_000),
        (4 * GB, 1 * MB, 4_000),
    ];
    
    for (map_size, value_size, num_keys) in scenarios {
        let max_pages = map_size / PAGE_SIZE;
        let pages_per_value = (value_size + PAGE_SIZE - 1) / PAGE_SIZE;
        let pages_needed = num_keys * pages_per_value;
        let overhead = 1000; // Estimate
        let total_needed = pages_needed + overhead;
        let fits = total_needed < max_pages;
        
        println!("\nScenario: {} keys × {} KB values in {} GB map",
                 num_keys, value_size / KB, map_size / GB);
        println!("  Pages per value: {}", pages_per_value);
        println!("  Data pages needed: {}", pages_needed);
        println!("  Total with overhead: {}", total_needed);
        println!("  Map capacity: {} pages", max_pages);
        println!("  Fits: {} ({}% full)", 
                 if fits { "YES" } else { "NO" },
                 (total_needed as f64 / max_pages as f64) * 100.0);
    }
}

#[test]
fn test_incremental_page_tracking() {
    println!("\n=== Incremental Page Tracking ===");
    
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(512 * MB) // Small map to see progression
            .open(dir.path())
            .unwrap(),
    );
    
    let db = {
        let mut txn = env.write_txn().unwrap();
        let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
        txn.commit().unwrap();
        db
    };
    
    let value_sizes = vec![4 * KB, 16 * KB, 64 * KB, 256 * KB];
    
    for value_size in value_sizes {
        println!("\n--- Testing with {} KB values ---", value_size / KB);
        
        let value = vec![0u8; value_size];
        let mut txn = env.write_txn().unwrap();
        let mut count = 0;
        
        loop {
            let key = format!("test_{}_{:08}", value_size, count).into_bytes();
            match db.put(&mut txn, key, value.clone()) {
                Ok(_) => {
                    count += 1;
                    if count % 100 == 0 {
                        let pages_used = count * ((value_size + PAGE_SIZE - 1) / PAGE_SIZE);
                        println!("  {} keys -> ~{} pages", count, pages_used);
                    }
                }
                Err(_) => {
                    println!("  Max keys with {} KB values: {}", value_size / KB, count);
                    break;
                }
            }
            
            // Stop at 1000 to avoid long test times
            if count >= 1000 {
                println!("  (Stopped at 1000 for test speed)");
                break;
            }
        }
        
        // Rollback to test next size
        drop(txn);
    }
}