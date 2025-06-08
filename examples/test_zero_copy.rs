//! Test zero-copy read performance

use std::time::Instant;
use std::sync::Arc;
use heed_core::EnvBuilder;
use heed_core::db::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create temporary directory
    let dir = tempfile::tempdir()?;
    let env = Arc::new(EnvBuilder::new()
        .max_dbs(10)
        .map_size(100 * 1024 * 1024) // 100MB
        .open(dir.path())?);
    
    // Create database and insert test data
    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut wtxn = env.begin_write_txn()?;
        let db = env.create_database(&mut wtxn, None)?;
        
        // Insert test data
        println!("Inserting test data...");
        for i in 0..10000 {
            let key = format!("key_{:06}", i).into_bytes();
            let value = format!("value_{:06}", i).into_bytes();
            db.put(&mut wtxn, key, value)?;
        }
        
        wtxn.commit()?;
        db
    };
    
    // Test read performance
    println!("\nTesting read performance...");
    let rtxn = env.begin_txn()?;
    
    // Warm up
    for i in 0..100 {
        let key = format!("key_{:06}", i).into_bytes();
        let _ = db.get(&rtxn, &key)?;
    }
    
    // Measure read performance
    let iterations = 100000;
    let start = Instant::now();
    
    for i in 0..iterations {
        let key_idx = i % 10000;
        let key = format!("key_{:06}", key_idx).into_bytes();
        let value = db.get(&rtxn, &key)?;
        
        if value.is_none() {
            eprintln!("Key not found: {:?}", key);
        }
    }
    
    let elapsed = start.elapsed();
    let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();
    let ns_per_op = elapsed.as_nanos() as f64 / iterations as f64;
    
    println!("Read performance with zero-copy:");
    println!("  Total time: {:?}", elapsed);
    println!("  Operations: {}", iterations);
    println!("  Ops/sec: {:.0}", ops_per_sec);
    println!("  ns/op: {:.0}", ns_per_op);
    println!("  µs/op: {:.2}", ns_per_op / 1000.0);
    
    Ok(())
}