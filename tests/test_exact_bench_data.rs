//! Test with the EXACT data pattern from the benchmark

use zerodb::{EnvBuilder, Result};
use std::sync::Arc;
use tempfile::TempDir;
use rand::{Rng, SeedableRng, seq::SliceRandom};

#[test] 
fn test_exact_benchmark_data() -> Result<()> {
    // Generate data EXACTLY like the benchmark
    let size = 50;
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
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

    // Now test in release mode simulation
    for attempt in 0..3 {
        println!("\nAttempt {}", attempt);
        let temp_dir = TempDir::new().unwrap();
        
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024 * 1024) // 10GB - much larger
                .open(temp_dir.path())?
        );

        // Create the database exactly like ZeroDb::new
        let db: zerodb::db::Database<Vec<u8>, Vec<u8>> = zerodb::db::Database::open(
            &env,
            None,
            zerodb::db::DatabaseFlags::CREATE,
        )?;
        
        // Write batch exactly like the benchmark
        let mut txn = env.write_txn()?;

        for (i, (key, value)) in data.iter().enumerate() {
            match db.put(&mut txn, key.clone(), value.clone()) {
                Ok(_) => {
                    if i < 10 || i % 10 == 0 {
                        println!("  Inserted {} - key: {}", i, String::from_utf8_lossy(key));
                    }
                }
                Err(e) => {
                    eprintln!("  Failed at entry {}: {:?}", i, e);
                    eprintln!("  Key: {}, Value size: {}", 
                        String::from_utf8_lossy(key), value.len());
                    
                    // Try to understand the state
                    println!("\n  First 10 keys in data:");
                    for (j, item) in data.iter().take(10).enumerate() {
                        println!("    [{}] {}", j, String::from_utf8_lossy(&item.0));
                    }
                    
                    return Err(e);
                }
            }
        }

        txn.commit()?;
        println!("  Success!");
    }
    
    Ok(())
}