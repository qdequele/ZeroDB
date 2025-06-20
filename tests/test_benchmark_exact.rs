//! Test that exactly mimics the db_comparison benchmark

use zerodb::{EnvBuilder, Result};
use std::sync::Arc;
use tempfile::TempDir;
use rand::{Rng, SeedableRng, seq::SliceRandom};

fn generate_data(size: usize, seed: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut data = Vec::with_capacity(size);

    for i in 0..size {
        let key = format!("key_{:08}", i.to_string()).into_bytes();
        let value_size = rng.gen_range(50..200); // Same as benchmark
        let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();
        data.push((key, value));
    }

    data
}

fn generate_random_data(size: usize, seed: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
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

    data
}

#[test]
fn test_sequential_writes_50() -> Result<()> {
    let data = generate_data(50, 42);
    let temp_dir = TempDir::new().unwrap();
    
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024 * 1024) // 10GB - same as benchmark
            .open(temp_dir.path())?
    );

    let db: zerodb::db::Database<Vec<u8>, Vec<u8>> = env.create_database(
        &mut env.write_txn()?,
        None
    )?;
    
    // Write batch - exactly like benchmark
    let mut txn = env.write_txn()?;
    for (key, value) in &data {
        db.put(&mut txn, key.clone(), value.clone())?;
    }
    txn.commit()?;
    
    println!("Sequential writes of 50 entries succeeded");
    Ok(())
}

#[test]
fn test_random_writes_50_fresh() -> Result<()> {
    // This mimics exactly what the benchmark does
    let data = generate_random_data(50, 42 + 1000000); // Different seed like benchmark
    let temp_dir = TempDir::new().unwrap();
    
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024 * 1024) // 10GB
            .open(temp_dir.path())?
    );

    // Create database
    let mut create_txn = env.write_txn()?;
    let db: zerodb::db::Database<Vec<u8>, Vec<u8>> = env.create_database(&mut create_txn, None)?;
    create_txn.commit()?;
    
    // Write batch - exactly like benchmark
    let mut txn = env.write_txn()?;
    
    println!("Starting to insert {} random entries", data.len());
    for (i, (key, value)) in data.iter().enumerate() {
        match db.put(&mut txn, key.clone(), value.clone()) {
            Ok(_) => {
                if i % 10 == 0 || i < 10 {
                    println!("Inserted entry {} - key: {}", i, String::from_utf8_lossy(key));
                }
            }
            Err(e) => {
                eprintln!("Failed at entry {} - key: {}: {:?}", i, String::from_utf8_lossy(key), e);
                
                // Print some diagnostics
                println!("\nDiagnostics:");
                println!("- Total entries to insert: {}", data.len());
                println!("- Successfully inserted: {}", i);
                println!("- Key that failed: {}", String::from_utf8_lossy(key));
                println!("- Value size: {}", value.len());
                
                return Err(e);
            }
        }
    }
    
    txn.commit()?;
    println!("Random writes of 50 entries succeeded!");
    Ok(())
}

#[test]
fn test_multiple_iterations_same_data() -> Result<()> {
    // Test multiple iterations with the same data like the benchmark
    let data = generate_random_data(50, 42);
    
    for iteration in 0..5 {
        println!("\n=== Iteration {} ===", iteration);
        let temp_dir = TempDir::new().unwrap();
        
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024 * 1024)
                .open(temp_dir.path())?
        );

        let mut create_txn = env.write_txn()?;
        let db: zerodb::db::Database<Vec<u8>, Vec<u8>> = env.create_database(&mut create_txn, None)?;
        create_txn.commit()?;
        
        let mut txn = env.write_txn()?;
        
        for (i, (key, value)) in data.iter().enumerate() {
            match db.put(&mut txn, key.clone(), value.clone()) {
                Ok(_) => {},
                Err(e) => {
                    eprintln!("Iteration {} failed at entry {}: {:?}", iteration, i, e);
                    eprintln!("Key: {}", String::from_utf8_lossy(key));
                    return Err(e);
                }
            }
        }
        
        txn.commit()?;
        println!("Iteration {} succeeded", iteration);
    }
    
    Ok(())
}

#[test]
fn test_gradual_random_writes() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024 * 1024)
            .open(temp_dir.path())?
    );

    let mut create_txn = env.write_txn()?;
    let db: zerodb::db::Database<Vec<u8>, Vec<u8>> = env.create_database(&mut create_txn, None)?;
    create_txn.commit()?;
    
    // Try inserting random entries one by one to find the limit
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let mut successful = 0;
    
    for i in 0..100 {
        let mut txn = env.write_txn()?;
        
        let key_num = rng.gen_range(10000..20000); // Random key
        let key = format!("key_{:08}", key_num).into_bytes();
        let value_size = rng.gen_range(50..200);
        let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();
        
        match db.put(&mut txn, key.clone(), value) {
            Ok(_) => {
                txn.commit()?;
                successful += 1;
                if i % 10 == 0 {
                    println!("Successfully inserted {} random entries", successful);
                }
            }
            Err(e) => {
                println!("Failed at entry {} (after {} successful): {:?}", i, successful, e);
                break;
            }
        }
    }
    
    println!("Total successful random inserts with individual commits: {}", successful);
    assert!(successful >= 50, "Should be able to insert at least 50 entries with individual commits");
    Ok(())
}