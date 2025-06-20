//! Tests for page capacity improvements and page full fixes

use zerodb::{EnvBuilder, Result};
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn test_conservative_fill_factor() -> Result<()> {
    let tmpdir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(tmpdir.path())?);
    
    // Create database
    let db: zerodb::db::Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    let mut txn = env.write_txn()?;
    
    // Insert entries with increasing sizes to test fill factor
    let mut successful_inserts = 0;
    let base_key = "test_key_";
    let base_value_size = 100;
    
    for i in 0..100 {
        let key = format!("{}{:04}", base_key, i.to_string());
        let value = vec![b'x'; base_value_size + i * 10]; // Gradually increasing value size
        
        match db.put(&mut txn, key.clone(), value) {
            Ok(_) => successful_inserts += 1,
            Err(e) => {
                eprintln!("Insert failed at entry {}: {:?}", i, e);
                break;
            }
        }
    }
    
    txn.commit()?;
    
    // With conservative fill factor, we should handle more entries before page full
    assert!(successful_inserts > 20, "Should insert at least 20 entries, got {}", successful_inserts);
    Ok(())
}

#[test]
fn test_proactive_page_splits() -> Result<()> {
    let tmpdir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(tmpdir.path())?);
    
    // Create database
    let db: zerodb::db::Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Test that pages split before becoming completely full
    let mut txn = env.write_txn()?;
    
    // Insert many medium-sized entries
    let value = vec![b'x'; 200];
    for i in 0..1000 {
        let key = format!("key_{:06}", i.to_string());
        db.put(&mut txn, key, value.clone())?;
    }
    
    txn.commit()?;
    
    // Verify all entries were inserted successfully
    let txn = env.read_txn()?;
    for i in 0..1000 {
        let key = format!("key_{:06}", i.to_string());
        let result = db.get(&txn, &key)?;
        assert_eq!(result.as_ref().map(|v| v.len()), Some(200));
    }
    Ok(())
}

#[test]
fn test_append_mode_optimization() -> Result<()> {
    let tmpdir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(tmpdir.path())?);
    
    // Create database
    let db: zerodb::db::Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Test append mode with sequential keys
    let mut txn = env.write_txn()?;
    
    let value = vec![b'y'; 150];
    for i in 0..2000 {
        let key = format!("{:08}", i.to_string()); // Sequential keys for append mode
        db.put(&mut txn, key, value.clone())?;
    }
    
    txn.commit()?;
    
    // Verify append optimization worked
    let txn = env.read_txn()?;
    let first = db.get(&txn, &"00000000".to_string())?;
    let last = db.get(&txn, &"00001999".to_string())?;
    assert_eq!(first.as_ref().map(|v| v.len()), Some(150));
    assert_eq!(last.as_ref().map(|v| v.len()), Some(150));
    Ok(())
}

#[test]
fn test_random_insert_handling() -> Result<()> {
    let tmpdir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(tmpdir.path())?);
    
    // Create database
    let db: zerodb::db::Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Test random inserts with varying sizes
    let mut txn = env.write_txn()?;
    
    use rand::{Rng, SeedableRng};
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    
    for _ in 0..500 {
        let key_num: u32 = rng.gen_range(0..10000);
        let key = format!("random_{:06}", key_num);
        let value_size = rng.gen_range(50..300);
        let value = vec![b'z'; value_size];
        
        // Random inserts should not cause page full errors with new capacity management
        db.put(&mut txn, key, value)?;
    }
    
    txn.commit()?;
    Ok(())
}

#[test]
fn test_large_value_handling() -> Result<()> {
    let tmpdir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(tmpdir.path())?);
    
    // Create database
    let db: zerodb::db::Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    let mut txn = env.write_txn()?;
    
    // Test mix of small and large values
    for i in 0..100 {
        let key = format!("mixed_{:04}", i.to_string());
        let value = if i % 10 == 0 {
            // Every 10th entry is large (will use overflow pages)
            vec![b'L'; 2000]
        } else {
            // Regular entries
            vec![b's'; 100]
        };
        
        db.put(&mut txn, key, value)?;
    }
    
    txn.commit()?;
    
    // Verify all values
    let txn = env.read_txn()?;
    for i in 0..100 {
        let key = format!("mixed_{:04}", i.to_string());
        let result = db.get(&txn, &key)?;
        let expected_size = if i % 10 == 0 { 2000 } else { 100 };
        assert_eq!(result.as_ref().map(|v| v.len()), Some(expected_size));
    }
    Ok(())
}

#[test]
fn test_page_capacity_edge_cases() -> Result<()> {
    let tmpdir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(tmpdir.path())?);
    
    // Create database
    let db: zerodb::db::Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Test 1: Maximum size entries that still fit inline
    let mut txn = env.write_txn()?;
    
    // Keys and values that approach but don't exceed page capacity
    let max_inline_value_size = 900; // Conservative to account for overhead
    let value = vec![b'M'; max_inline_value_size];
    
    for i in 0..50 {
        let key = format!("max_{:03}", i.to_string());
        db.put(&mut txn, key, value.clone())?;
    }
    
    txn.commit()?;
    
    // Test 2: Entries that just exceed inline capacity
    let mut txn = env.write_txn()?;
    
    let overflow_value = vec![b'O'; 1100]; // Will trigger overflow
    for i in 0..50 {
        let key = format!("overflow_{:03}", i.to_string());
        db.put(&mut txn, key, overflow_value.clone())?;
    }
    
    txn.commit()?;
    Ok(())
}

#[test]
fn test_delete_and_reinsert_capacity() -> Result<()> {
    let tmpdir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new().open(tmpdir.path())?);
    
    // Create database
    let db: zerodb::db::Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Fill pages, delete some entries, then reinsert
    let mut txn = env.write_txn()?;
    
    let value = vec![b'x'; 150];
    for i in 0..500 {
        let key = format!("del_test_{:04}", i.to_string());
        db.put(&mut txn, key, value.clone())?;
    }
    
    // Delete every other entry
    for i in (0..500).step_by(2) {
        let key = format!("del_test_{:04}", i.to_string());
        db.delete(&mut txn, &key)?;
    }
    
    // Reinsert with larger values
    let larger_value = vec![b'X'; 200];
    for i in (0..500).step_by(2) {
        let key = format!("del_test_{:04}", i.to_string());
        db.put(&mut txn, key, larger_value.clone())?;
    }
    
    txn.commit()?;
    Ok(())
}

#[cfg(test)]
mod performance_tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn benchmark_sequential_inserts() -> Result<()> {
        let tmpdir = TempDir::new().unwrap();
        let env = Arc::new(EnvBuilder::new().open(tmpdir.path())?);
        
        // Create database
        let db: zerodb::db::Database<String, Vec<u8>> = {
            let mut txn = env.write_txn()?;
            let db = env.create_database(&mut txn, None)?;
            txn.commit()?;
            db
        };

        let start = Instant::now();
        let mut txn = env.write_txn()?;
        
        let value = vec![b'S'; 100];
        let count = 10_000;
        
        for i in 0..count {
            let key = format!("{:08}", i.to_string());
            db.put(&mut txn, key, value.clone())?;
        }
        
        txn.commit()?;
        let elapsed = start.elapsed();
        
        println!("Sequential insert performance: {} inserts in {:?}", count, elapsed);
        println!("Rate: {:.0} inserts/sec", count as f64 / elapsed.as_secs_f64());
        Ok(())
    }

    #[test] 
    fn benchmark_random_inserts() -> Result<()> {
        let tmpdir = TempDir::new().unwrap();
        let env = Arc::new(EnvBuilder::new().open(tmpdir.path())?);
        
        // Create database
        let db: zerodb::db::Database<String, Vec<u8>> = {
            let mut txn = env.write_txn()?;
            let db = env.create_database(&mut txn, None)?;
            txn.commit()?;
            db
        };

        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        
        let start = Instant::now();
        let mut txn = env.write_txn()?;
        
        let value = vec![b'R'; 100];
        let count = 5_000;
        
        for _ in 0..count {
            let key_num: u32 = rng.gen_range(0..1_000_000);
            let key = format!("{:08}", key_num);
            db.put(&mut txn, key, value.clone())?;
        }
        
        txn.commit()?;
        let elapsed = start.elapsed();
        
        println!("Random insert performance: {} inserts in {:?}", count, elapsed);
        println!("Rate: {:.0} inserts/sec", count as f64 / elapsed.as_secs_f64());
        Ok(())
    }
}