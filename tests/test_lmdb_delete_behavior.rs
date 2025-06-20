//! Test that delete operations behave exactly like LMDB
//! 
//! Key requirement: DELETE OPERATIONS MUST NEVER FAIL WITH "PAGE FULL"

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{
    db::Database,
    env::EnvBuilder,
    error::Result,
};

/// Test mixed insert/delete operations with large values
/// This is the exact scenario that was failing before
#[test]
fn test_mixed_insert_delete_large_values() -> Result<()> {
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
    
    // Test with large values that previously caused "Page full" errors
    let mut txn = env.write_txn()?;
    
    for i in 0..50 {
        let key = format!("key_{:08}", i.to_string()).into_bytes();
        let value = vec![42u8; 500]; // Large values
        db.put(&mut txn, key, value)?;
        
        // Delete every 5th key
        if i > 0 && i % 5 == 0 {
            let del_key = format!("key_{:08}", i - 5).into_bytes();
            // This MUST NOT fail with "Page full"
            let deleted = db.delete(&mut txn, &del_key)?;
            assert!(deleted, "Key should have been deleted");
        }
    }
    
    txn.commit()?;
    
    // Verify final state
    let txn = env.read_txn()?;
    let mut cursor = db.cursor(&txn)?;
    let mut count = 0;
    
    if let Ok(Some(_)) = cursor.first() {
        count += 1;
        while let Ok(Some(_)) = cursor.next_raw() {
            count += 1;
        }
    }
    
    // We inserted 50 keys and deleted 9 keys:
    // When i=5: delete key_00000000
    // When i=10: delete key_00000005
    // When i=15: delete key_00000010
    // When i=20: delete key_00000015
    // When i=25: delete key_00000020
    // When i=30: delete key_00000025
    // When i=35: delete key_00000030
    // When i=40: delete key_00000035
    // When i=45: delete key_00000040
    // Total: 50 - 9 = 41
    assert_eq!(count, 41, "Should have 41 keys remaining");
    
    Ok(())
}

/// Test that pages can remain underflowed when necessary
#[test]
fn test_underflow_tolerance() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())?,
    );
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // Insert entries with very large values
    let mut txn = env.write_txn()?;
    for i in 0..20 {
        let key = format!("key_{:03}", i.to_string()).into_bytes();
        let value = vec![99u8; 1000]; // Very large values
        db.put(&mut txn, key, value)?;
    }
    txn.commit()?;
    
    // Delete most entries - pages will be underflowed and can't be merged
    let mut txn = env.write_txn()?;
    for i in 1..19 {
        let key = format!("key_{:03}", i.to_string()).into_bytes();
        // This MUST succeed even if it leaves pages underflowed
        db.delete(&mut txn, &key)?;
    }
    txn.commit()?;
    
    // Verify remaining entries
    let txn = env.read_txn()?;
    let mut cursor = db.cursor(&txn)?;
    let mut remaining = Vec::new();
    
    if let Ok(Some((key, _))) = cursor.first() {
        remaining.push(String::from_utf8_lossy(&key).to_string());
        while let Ok(Some((key, _))) = cursor.next_raw() {
            remaining.push(String::from_utf8_lossy(&key).to_string());
        }
    }
    
    assert_eq!(remaining, vec!["key_000", "key_019"]);
    
    Ok(())
}

/// Test rapid insert/delete cycles
#[test]
fn test_rapid_insert_delete_cycles() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(50 * 1024 * 1024)
            .open(dir.path())?,
    );
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // Perform multiple insert/delete cycles
    for cycle in 0..5 {
        let mut txn = env.write_txn()?;
        
        // Insert batch
        for i in 0..30 {
            let key = format!("cycle_{}_key_{:03}", cycle, i.to_string()).into_bytes();
            let value = vec![(cycle * 30 + i) as u8; 300];
            db.put(&mut txn, key, value)?;
        }
        
        // Delete half
        for i in (0..30).step_by(2) {
            let key = format!("cycle_{}_key_{:03}", cycle, i.to_string()).into_bytes();
            db.delete(&mut txn, &key)?;
        }
        
        txn.commit()?;
    }
    
    // Count final entries
    let txn = env.read_txn()?;
    let mut cursor = db.cursor(&txn)?;
    let mut count = 0;
    
    if let Ok(Some(_)) = cursor.first() {
        count += 1;
        while let Ok(Some(_)) = cursor.next_raw() {
            count += 1;
        }
    }
    
    // 5 cycles × 15 remaining entries per cycle = 75
    assert_eq!(count, 75, "Should have 75 entries after all cycles");
    
    Ok(())
}

/// Test edge case: delete from nearly empty database
#[test]
fn test_delete_from_nearly_empty() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(EnvBuilder::new().open(dir.path())?);
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    
    // Insert just a few entries
    for i in 0..3 {
        let key = format!("key_{}", i.to_string()).into_bytes();
        let value = vec![i as u8; 500];
        db.put(&mut txn, key, value)?;
    }
    txn.commit()?;
    
    // Delete them one by one
    for i in 0..3 {
        let mut txn = env.write_txn()?;
        let key = format!("key_{}", i.to_string()).into_bytes();
        let deleted = db.delete(&mut txn, &key)?;
        assert!(deleted);
        txn.commit()?;
    }
    
    // Verify empty
    let txn = env.read_txn()?;
    let mut cursor = db.cursor(&txn)?;
    assert!(cursor.first()?.is_none(), "Database should be empty");
    
    Ok(())
}

/// Stress test: Verify NO "Page full" errors ever occur during deletes
#[test]
fn test_no_page_full_on_delete_stress() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(200 * 1024 * 1024)
            .use_segregated_freelist(true)
            .open(dir.path())?,
    );
    
    let mut txn = env.write_txn()?;
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
    txn.commit()?;
    
    // Various value sizes to stress different scenarios
    let value_sizes = [50, 100, 200, 500, 1000, 2000];
    
    for &value_size in &value_sizes {
        eprintln!("Testing value_size={}", value_size);
        
        // Insert entries in smaller batches to avoid page allocation issues
        let batch_size = 25;
        for batch_start in (0..100).step_by(batch_size) {
            let mut txn = env.write_txn()?;
            
            let batch_end = std::cmp::min(batch_start + batch_size, 100);
            for i in batch_start..batch_end {
                let key = format!("size_{}_key_{:04}", value_size, i.to_string()).into_bytes();
                let value = vec![42u8; value_size];
                match db.put(&mut txn, key.clone(), value) {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("Insert failed for value_size={}, key_index={}", value_size, i.to_string());
                        eprintln!("Error: {:?}", e);
                        return Err(e);
                    }
                }
            }
            
            txn.commit()?;
        }
        
        // Delete entries - also in batches to ensure it works
        let mut txn = env.write_txn()?;
        
        // Random delete pattern
        let mut indices: Vec<usize> = (0..100).collect();
        use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
        let mut rng = StdRng::seed_from_u64(12345);
        indices.shuffle(&mut rng);
        
        let mut deleted_count = 0;
        for &i in indices.iter().take(50) {
            let key = format!("size_{}_key_{:04}", value_size, i.to_string()).into_bytes();
            // This is the critical test - delete MUST NEVER fail with "Page full"
            match db.delete(&mut txn, &key) {
                Ok(_) => {
                    deleted_count += 1;
                    // Commit every 20 deletes to avoid transaction size issues
                    if deleted_count % 20 == 0 {
                        txn.commit()?;
                        txn = env.write_txn()?;
                    }
                }
                Err(e) => {
                    eprintln!("Delete failed for value_size={}, key_index={}", value_size, i.to_string());
                    eprintln!("Error: {:?}", e);
                    panic!("Delete failed with error: {:?}. This should NEVER happen!", e);
                }
            }
        }
        
        txn.commit()?;
    }
    
    println!("Stress test passed - no 'Page full' errors during delete operations!");
    
    Ok(())
}