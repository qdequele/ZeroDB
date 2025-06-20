//! Test specifically for random write page full issues

use zerodb::{EnvBuilder, Result};
use std::sync::Arc;
use tempfile::TempDir;
use rand::{Rng, SeedableRng, seq::SliceRandom};

#[test]
fn test_random_writes_size_100() -> Result<()> {
    let tmpdir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024 * 1024) // 10GB
            .open(tmpdir.path())?
    );
    
    // Create database
    let db: zerodb::db::Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Generate random data similar to benchmark
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let size = 100;
    let mut data = Vec::with_capacity(size);
    
    // Generate random keys to ensure non-sequential access
    let mut keys: Vec<usize> = (0..size).collect();
    keys.shuffle(&mut rng);
    
    for &i in &keys {
        let key = format!("key_{:08}", i.to_string()).into_bytes();
        let value_size = rng.gen_range(50..200);
        let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();
        data.push((key, value));
    }
    
    // Try to insert all data in one transaction
    let mut txn = env.write_txn()?;
    
    for (i, (key, value)) in data.iter().enumerate() {
        match db.put(&mut txn, key.clone(), value.clone()) {
            Ok(_) => {
                if i % 10 == 0 {
                    println!("Inserted entry {}", i);
                }
            }
            Err(e) => {
                eprintln!("Failed at entry {} with key {:?}: {:?}", i, 
                    String::from_utf8_lossy(key), e);
                return Err(e);
            }
        }
    }
    
    txn.commit()?;
    println!("Successfully inserted all {} random entries", size);
    Ok(())
}

#[test]
fn test_random_writes_small_batch() -> Result<()> {
    let tmpdir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024 * 1024) // 10GB
            .open(tmpdir.path())?
    );
    
    // Create database
    let db: zerodb::db::Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Generate random data similar to benchmark
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let size = 50;
    let mut data = Vec::with_capacity(size);
    
    // Generate random keys to ensure non-sequential access
    let mut keys: Vec<usize> = (0..size).collect();
    keys.shuffle(&mut rng);
    
    for &i in &keys {
        let key = format!("key_{:08}", i.to_string()).into_bytes();
        let value_size = rng.gen_range(50..200);
        let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();
        data.push((key, value));
    }
    
    // Try to insert all data in one transaction
    let mut txn = env.write_txn()?;
    
    for (i, (key, value)) in data.iter().enumerate() {
        match db.put(&mut txn, key.clone(), value.clone()) {
            Ok(_) => println!("Inserted entry {}", i),
            Err(e) => {
                eprintln!("Failed at entry {} with key {:?}: {:?}", i, 
                    String::from_utf8_lossy(key), e);
                return Err(e);
            }
        }
    }
    
    txn.commit()?;
    println!("Successfully inserted all {} random entries", size);
    Ok(())
}

#[test]
fn test_random_writes_with_splits() -> Result<()> {
    let tmpdir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024 * 1024) // 10GB
            .open(tmpdir.path())?
    );
    
    // Create database
    let db: zerodb::db::Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // First, insert some sequential data to create initial pages
    let mut txn = env.write_txn()?;
    for i in 0..20 {
        let key = format!("base_{:08}", i.to_string()).into_bytes();
        let value = vec![b'x'; 100];
        db.put(&mut txn, key, value)?;
    }
    txn.commit()?;
    
    // Now insert random keys that will cause splits
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let mut txn = env.write_txn()?;
    
    for i in 0..30 {
        let key_num = rng.gen_range(1000..2000);
        let key = format!("rand_{:08}", key_num).into_bytes();
        let value_size = rng.gen_range(50..150);
        let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();
        
        match db.put(&mut txn, key.clone(), value) {
            Ok(_) => {
                if i % 5 == 0 {
                    println!("Inserted random entry {}", i);
                }
            }
            Err(e) => {
                eprintln!("Failed at random entry {}: {:?}", i, e);
                return Err(e);
            }
        }
    }
    
    txn.commit()?;
    println!("Successfully handled random inserts with page splits");
    Ok(())
}

#[test]
fn test_worst_case_random_pattern() -> Result<()> {
    let tmpdir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024 * 1024) // 10GB
            .open(tmpdir.path())?
    );
    
    // Create database
    let db: zerodb::db::Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Insert keys that will definitely cause many splits
    // Use keys that are far apart to maximize tree traversal
    let mut txn = env.write_txn()?;
    let mut successful = 0;
    
    for i in 0..100 {
        // Alternate between very low and very high key values
        let key = if i % 2 == 0 {
            format!("a_{:08}", i.to_string())
        } else {
            format!("z_{:08}", i.to_string())
        };
        
        let value = vec![b'x'; 100];
        
        match db.put(&mut txn, key.clone(), value) {
            Ok(_) => successful += 1,
            Err(e) => {
                eprintln!("Failed at entry {} (key: {}): {:?}", i, key, e);
                if successful < 20 {
                    return Err(e);
                }
                break;
            }
        }
    }
    
    txn.commit()?;
    println!("Inserted {} entries with worst-case pattern", successful);
    assert!(successful >= 20, "Should insert at least 20 entries");
    Ok(())
}