use zerodb::{
    db::{Database, DatabaseFlags},
    env::EnvBuilder,
};

#[test]
fn test_sequential_inserts_page_full() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    
    // Create environment with large map size
    let env = EnvBuilder::new()
        .map_size(10 * 1024 * 1024 * 1024) // 10GB
        .open(&temp_dir)?;
    
    // Create database
    let db = Database::<Vec<u8>, Vec<u8>>::open(&env, None, DatabaseFlags::CREATE)?;
    
    // Try to insert 1000 items like the benchmark does
    let mut txn = env.write_txn()?;
    
    for i in 0..1000 {
        let key = format!("key_{:08}", i).into_bytes();
        let value = vec![42u8; 100]; // 100-byte value
        
        db.put(&mut txn, key.clone(), value)?;
    }
    
    txn.commit()?;
    
    // Verify data
    let txn = env.read_txn()?;
    let mut count = 0;
    let mut cursor = db.cursor(&txn)?;
    if cursor.first()?.is_some() {
        count = 1;
        while cursor.next_raw()?.is_some() {
            count += 1;
        }
    }
    assert_eq!(count, 1000, "Expected 1000 entries, found {}", count);
    
    Ok(())
}

#[test]
fn test_large_sequential_inserts() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    
    let env = EnvBuilder::new()
        .map_size(10 * 1024 * 1024 * 1024) // 10GB
        .open(&temp_dir)?;
    
    let db = Database::<Vec<u8>, Vec<u8>>::open(&env, None, DatabaseFlags::CREATE)?;
    
    // Insert more entries to stress test page allocation
    let num_entries = 10_000;
    let mut txn = env.write_txn()?;
    
    for i in 0..num_entries {
        let key = format!("key_{:010}", i).into_bytes();
        let value = vec![(i % 256) as u8; 200]; // 200-byte values
        
        db.put(&mut txn, key, value)?;
        
        // Commit periodically to avoid transaction size limits
        if i > 0 && i % 1000 == 0 {
            txn.commit()?;
            txn = env.write_txn()?;
        }
    }
    
    txn.commit()?;
    
    // Verify count
    let txn = env.read_txn()?;
    let mut count = 0;
    let mut cursor = db.cursor(&txn)?;
    if cursor.first()?.is_some() {
        count = 1;
        while cursor.next_raw()?.is_some() {
            count += 1;
        }
    }
    assert_eq!(count, num_entries, "Expected {} entries, found {}", num_entries, count);
    
    Ok(())
}

#[test]
fn test_mixed_size_inserts() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    
    let env = EnvBuilder::new()
        .map_size(1024 * 1024 * 1024) // 1GB
        .open(&temp_dir)?;
    
    let db = Database::<Vec<u8>, Vec<u8>>::open(&env, None, DatabaseFlags::CREATE)?;
    
    let mut txn = env.write_txn()?;
    
    // Insert entries with varying sizes
    for i in 0..500 {
        let key = format!("key_{:06}", i).into_bytes();
        
        // Vary value size: small, medium, large
        let value_size = match i % 3 {
            0 => 50,   // Small
            1 => 500,  // Medium
            _ => 2000, // Large
        };
        
        let value = vec![(i % 256) as u8; value_size];
        db.put(&mut txn, key, value)?;
    }
    
    txn.commit()?;
    
    // Verify all entries exist
    let txn = env.read_txn()?;
    for i in 0..500 {
        let key = format!("key_{:06}", i).into_bytes();
        let value = db.get(&txn, &key)?;
        assert!(value.is_some(), "Key {} not found", i);
        
        let expected_size = match i % 3 {
            0 => 50,
            1 => 500,
            _ => 2000,
        };
        assert_eq!(value.unwrap().len(), expected_size);
    }
    
    Ok(())
}