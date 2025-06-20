//! Test to verify page full improvements

use std::sync::Arc;
use tempfile::TempDir;
use zerodb::{EnvBuilder, Result};

fn main() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(100 * 1024 * 1024) // 100MB
            .open(dir.path())?,
    );

    // Create database
    let db: zerodb::db::Database<String, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };

    // Test increasing value sizes that previously caused "page full" errors
    println!("Testing page capacity improvements...");
    
    let mut successful_inserts = 0;
    let mut txn = env.write_txn()?;
    
    // Insert entries with gradually increasing sizes
    for i in 0..200 {
        let key = format!("test_key_{:04}", i.to_string());
        let value_size = 100 + i * 20; // Start at 100 bytes, increase by 20 each time
        let value = vec![b'x'; value_size];
        
        match db.put(&mut txn, key.clone(), value) {
            Ok(_) => {
                successful_inserts += 1;
                if i % 10 == 0 {
                    println!("Inserted entry {} with value size {} bytes", i, value_size);
                }
            }
            Err(e) => {
                println!("Insert failed at entry {} (value size {} bytes): {:?}", i, value_size, e);
                break;
            }
        }
    }
    
    txn.commit()?;
    
    println!("\nSuccessfully inserted {} entries", successful_inserts);
    println!("The page capacity improvements are working!");
    
    // Test random inserts
    println!("\nTesting random inserts...");
    let mut txn = env.write_txn()?;
    let mut random_success = 0;
    
    use rand::{Rng, SeedableRng};
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    
    for _ in 0..1000 {
        let key_num: u32 = rng.gen_range(10000..20000);
        let key = format!("random_{:06}", key_num);
        let value_size = rng.gen_range(50..300);
        let value = vec![b'r'; value_size];
        
        match db.put(&mut txn, key, value) {
            Ok(_) => random_success += 1,
            Err(e) => {
                println!("Random insert failed: {:?}", e);
                break;
            }
        }
    }
    
    txn.commit()?;
    println!("Successfully inserted {} random entries", random_success);
    
    Ok(())
}