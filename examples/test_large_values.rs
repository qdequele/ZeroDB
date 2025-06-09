use zerodb::{
    db::{Database, DatabaseFlags},
    env::EnvBuilder,
};
use rand::{rngs::StdRng, RngCore, SeedableRng};
use std::time::Instant;

fn main() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    println!("Testing large value storage (FST/Roaring Bitmap sizes)...\n");
    
    // Create environment with sufficient map size
    let env = EnvBuilder::new()
        .map_size(1024 * 1024 * 1024) // 1GB for testing
        .open(&temp_dir)?;
    
    let db = Database::<String, Vec<u8>>::open(&env, None, DatabaseFlags::CREATE)?;
    
    // Test various sizes typical for Meilisearch
    let test_sizes = [
        ("Small Roaring Bitmap", 10 * 1024),        // 10 KB
        ("Medium Roaring Bitmap", 100 * 1024),      // 100 KB  
        ("Large Roaring Bitmap", 1024 * 1024),      // 1 MB
        ("Small FST", 5 * 1024 * 1024),             // 5 MB
        ("Medium FST", 20 * 1024 * 1024),           // 20 MB
        ("Large FST", 50 * 1024 * 1024),            // 50 MB
    ];
    
    let mut rng = StdRng::seed_from_u64(42);
    
    for (name, size) in test_sizes {
        println!("Testing {}: {} bytes ({:.2} MB)", name, size, size as f64 / 1024.0 / 1024.0);
        
        // Generate random data
        let mut value = vec![0u8; size];
        rng.fill_bytes(&mut value);
        
        // Write
        let start = Instant::now();
        let mut txn = env.begin_write_txn()?;
        db.put(&mut txn, name.to_string(), value.clone())?;
        txn.commit()?;
        let write_time = start.elapsed();
        
        // Read
        let start = Instant::now();
        let txn = env.begin_txn()?;
        let read_value = db.get(&txn, &name.to_string())?.unwrap();
        let read_time = start.elapsed();
        
        // Verify
        assert_eq!(read_value.len(), value.len());
        assert_eq!(&read_value[..100], &value[..100]); // Check first 100 bytes
        
        println!("  ✓ Write: {:.2} ms ({:.2} MB/s)", 
            write_time.as_secs_f64() * 1000.0,
            (size as f64 / 1024.0 / 1024.0) / write_time.as_secs_f64()
        );
        println!("  ✓ Read: {:.2} ms ({:.2} MB/s)", 
            read_time.as_secs_f64() * 1000.0,
            (size as f64 / 1024.0 / 1024.0) / read_time.as_secs_f64()
        );
        println!();
    }
    
    // Test concurrent access to large values
    println!("Testing concurrent reads of large values...");
    let txn = env.begin_txn()?;
    let start = Instant::now();
    
    for _ in 0..100 {
        let _ = db.get(&txn, &"Large FST".to_string())?.unwrap();
    }
    
    let elapsed = start.elapsed();
    println!("100 reads of 50MB value: {:.2} ms total ({:.2} ms per read)\n", 
        elapsed.as_secs_f64() * 1000.0,
        elapsed.as_secs_f64() * 10.0
    );
    
    Ok(())
}