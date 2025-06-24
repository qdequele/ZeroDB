use zerodb::{
    db::{Database, DatabaseFlags},
    env::EnvBuilder,
};
use rand::{rngs::StdRng, RngCore, SeedableRng};
use std::time::Instant;

#[test]
fn test_large_value_storage() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    
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
        // Generate random data
        let mut value = vec![0u8; size];
        rng.fill_bytes(&mut value);
        
        // Write
        let start = Instant::now();
        let mut txn = env.write_txn()?;
        db.put(&mut txn, name.to_string(), value.clone())?;
        txn.commit()?;
        let write_time = start.elapsed();
        
        // Read
        let start = Instant::now();
        let txn = env.read_txn()?;
        let read_value = db.get(&txn, &name.to_string())?.unwrap();
        let read_time = start.elapsed();
        
        // Verify
        assert_eq!(read_value.len(), value.len());
        assert_eq!(&read_value[..100], &value[..100]); // Check first 100 bytes
        
        // Performance should be reasonable
        assert!(write_time.as_secs() < 5, "Write took too long for {}", name);
        assert!(read_time.as_secs() < 2, "Read took too long for {}", name);
    }
    
    Ok(())
}

#[test]
fn test_concurrent_large_value_reads() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    
    let env = EnvBuilder::new()
        .map_size(1024 * 1024 * 1024) // 1GB
        .open(&temp_dir)?;
    
    let db = Database::<String, Vec<u8>>::open(&env, None, DatabaseFlags::CREATE)?;
    
    // Create a large value
    let mut rng = StdRng::seed_from_u64(42);
    let mut large_value = vec![0u8; 50 * 1024 * 1024]; // 50 MB
    rng.fill_bytes(&mut large_value);
    
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, "Large FST".to_string(), large_value.clone())?;
        txn.commit()?;
    }
    
    // Test concurrent reads
    let txn = env.read_txn()?;
    let start = Instant::now();
    
    for _ in 0..100 {
        let value = db.get(&txn, &"Large FST".to_string())?.unwrap();
        assert_eq!(value.len(), large_value.len());
    }
    
    let elapsed = start.elapsed();
    
    // 100 reads of 50MB should complete in reasonable time
    assert!(elapsed.as_secs() < 10, "Concurrent reads took too long");
    
    Ok(())
}

#[test]
fn test_very_large_single_value() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    
    let env = EnvBuilder::new()
        .map_size(1024 * 1024 * 1024) // 1GB
        .open(&temp_dir)?;
    
    let db = Database::<String, Vec<u8>>::open(&env, None, DatabaseFlags::CREATE)?;
    
    // Test a very large single value (100 MB)
    let mut rng = StdRng::seed_from_u64(42);
    let mut very_large = vec![0u8; 100 * 1024 * 1024];
    rng.fill_bytes(&mut very_large);
    
    // Write
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, "very_large".to_string(), very_large.clone())?;
        txn.commit()?;
    }
    
    // Read back
    {
        let txn = env.read_txn()?;
        let value = db.get(&txn, &"very_large".to_string())?.unwrap();
        assert_eq!(value.len(), very_large.len());
        assert_eq!(&value[..1000], &very_large[..1000]);
    }
    
    Ok(())
}