//! Test page allocation fixes

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{
    db::Database,
    env::EnvBuilder,
    error::Result,
};

#[test]
fn test_mixed_key_patterns_no_page_full() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(100 * 1024 * 1024)
            .use_segregated_freelist(true)
            .open(dir.path())?,
    );
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // Test the exact scenario that was failing before our fixes
    // This used to fail with "Page full" errors after ~50-60 entries
    
    // Mix different key patterns in a single transaction
    let mut txn = env.write_txn()?;
    let mut total_inserted = 0;
    
    // Pattern 1: Sequential keys with 50-byte values
    for i in 0..100 {
        let key = format!("size_50_key_{:04}", i.to_string()).into_bytes();
        let value = vec![42u8; 50];
        db.put(&mut txn, key, value)?;
        total_inserted += 1;
    }
    eprintln!("Inserted {} entries with 50-byte values", 100);
    
    // Pattern 2: Sequential keys with 100-byte values
    for i in 0..100 {
        let key = format!("size_100_key_{:04}", i.to_string()).into_bytes();
        let value = vec![43u8; 100];
        db.put(&mut txn, key, value)?;
        total_inserted += 1;
    }
    eprintln!("Inserted {} entries with 100-byte values", 100);
    
    // Pattern 3: Random key patterns with 200-byte values
    for i in 0..50 {
        let key = format!("random_{}_{:04}", i % 10, i.to_string()).into_bytes();
        let value = vec![44u8; 200];
        db.put(&mut txn, key, value)?;
        total_inserted += 1;
    }
    eprintln!("Inserted {} entries with random patterns", 50);
    
    txn.commit()?;
    eprintln!("Successfully inserted {} total entries in single transaction", total_inserted);
    
    // Verify all entries were persisted
    let txn = env.read_txn()?;
    let mut cursor = db.cursor(&txn)?;
    let mut count = 0;
    
    if cursor.first()?.is_some() {
        count = 1;
        while cursor.next_raw()?.is_some() {
            count += 1;
        }
    }
    
    eprintln!("Verified {} entries persisted", count);
    assert_eq!(count, total_inserted, "All entries should be persisted");
    
    Ok(())
}

#[test]
fn test_no_transaction_page_limit() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(1024 * 1024 * 1024) // 1GB
            .open(dir.path())?,
    );
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // With no page limit, we should be able to allocate many pages
    let mut txn = env.write_txn()?;
    let mut inserted = 0;
    
    for i in 0..2000 { // Previously would hit limit around 1024 pages
        let key = format!("key_{:08}", i.to_string()).into_bytes();
        let value = vec![i as u8; 2000]; // Large values to force page allocation
        
        match db.put(&mut txn, key, value) {
            Ok(_) => {
                inserted += 1;
                if inserted % 100 == 0 {
                    eprintln!("Inserted {} entries", inserted);
                }
            }
            Err(e) => {
                eprintln!("Error at insertion {}: {:?}", inserted, e);
                // Should not fail with transaction page limit error anymore
                let error_msg = format!("{:?}", e);
                assert!(!error_msg.contains("Transaction page limit exceeded"), 
                        "Should not hit transaction page limit");
                return Err(e);
            }
        }
    }
    
    eprintln!("Transaction completed with {} insertions", inserted);
    assert_eq!(inserted, 2000, "Should insert all entries without page limit");
    
    txn.commit()?;
    
    Ok(())
}

#[test]
fn test_preemptive_splitting() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // Insert entries that should trigger pre-emptive splitting
    let mut txn = env.write_txn()?;
    
    // Use large enough values to ensure pages reach the 85% threshold
    for i in 0..200 {
        let key = format!("test_{:06}", i.to_string()).into_bytes();
        let value = vec![42u8; 300]; // Moderately large values
        
        match db.put(&mut txn, key, value) {
            Ok(_) => {
                if i % 50 == 0 {
                    eprintln!("Successfully inserted entry {}", i);
                }
            }
            Err(e) => {
                eprintln!("Insert failed at entry {} with error: {:?}", i, e);
                let error_msg = format!("{:?}", e);
                
                // Without page limits, we should only get Page full errors in extreme cases
                if error_msg.contains("Page full") {
                    panic!("Pre-emptive splitting failed! Still getting 'Page full' errors at entry {}", i);
                } else {
                    return Err(e); // Unexpected error
                }
            }
        }
    }
    
    txn.commit()?;
    eprintln!("Pre-emptive splitting test completed successfully");
    
    Ok(())
}