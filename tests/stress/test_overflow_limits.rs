use zerodb::{EnvBuilder, Result, db::Database};
use tempfile::TempDir;
use std::sync::Arc;

#[test]
fn test_overflow_page_calculations() -> Result<()> {
    // Constants from the codebase
    const PAGE_SIZE: usize = 4096;
    const PAGE_HEADER_SIZE: usize = 48; // size_of::<PageHeader>()
    const OVERFLOW_HEADER_SIZE: usize = 16; // size_of::<OverflowHeader>()
    const MAX_INLINE_VALUE_SIZE: usize = PAGE_SIZE / 4; // 1024 bytes
    
    // Calculate usable space per overflow page
    let data_per_overflow_page = PAGE_SIZE - PAGE_HEADER_SIZE - OVERFLOW_HEADER_SIZE;
    
    // Verify calculations
    assert_eq!(MAX_INLINE_VALUE_SIZE, 1024);
    assert!(data_per_overflow_page > 3000); // Should have most of the page available
    
    // Calculate theoretical maximum value size
    let theoretical_max = u64::MAX;
    let max_pages = theoretical_max / data_per_overflow_page as u64;
    assert!(max_pages > 0);
    
    Ok(())
}

#[test]
fn test_practical_overflow_sizes() -> Result<()> {
    const PAGE_SIZE: usize = 4096;
    const PAGE_HEADER_SIZE: usize = 48;
    const OVERFLOW_HEADER_SIZE: usize = 16;
    
    let data_per_overflow_page = PAGE_SIZE - PAGE_HEADER_SIZE - OVERFLOW_HEADER_SIZE;
    
    // Practical examples for large data structures
    let sizes = vec![
        ("Small FST", 100_000),           // 100 KB
        ("Medium FST", 1_000_000),        // 1 MB
        ("Large FST", 10_000_000),        // 10 MB
        ("Very Large FST", 100_000_000),  // 100 MB
        ("Huge Roaring Bitmap", 500_000_000), // 500 MB
    ];
    
    for (name, size) in sizes {
        let pages_needed = (size + data_per_overflow_page - 1) / data_per_overflow_page;
        let total_space = pages_needed.saturating_mul(PAGE_SIZE);
        let overhead = total_space - size;
        let overhead_percent = (overhead as f64 / size as f64) * 100.0;
        
        // Overhead should be reasonable (less than 5% for large values)
        if size > 1_000_000 {
            assert!(overhead_percent < 5.0, "{} has too much overhead: {:.1}%", name, overhead_percent);
        }
    }
    
    Ok(())
}

#[test]
fn test_actual_overflow_storage() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(1 << 30) // 1 GB
            .open(dir.path())?
    );
    
    // Create a database
    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Test storing progressively larger values
    let test_sizes = vec![
        ("1 KB", 1_024),
        ("10 KB", 10_240),
        ("100 KB", 102_400),
        ("1 MB", 1_048_576),
        ("10 MB", 10_485_760),
    ];
    
    for (name, size) in test_sizes {
        let key = format!("test_{}", size);
        let value = vec![0xAB; size];
        
        // Write
        {
            let mut txn = env.write_txn()?;
            db.put(&mut txn, key.as_bytes().to_vec(), value.clone())?;
            txn.commit()?;
        }
        
        // Verify read
        {
            let read_txn = env.read_txn()?;
            let read_value = db.get(&read_txn, &key.as_bytes().to_vec())?.unwrap();
            assert_eq!(read_value.len(), size, "{} value size mismatch", name);
            assert_eq!(read_value[0], 0xAB);
            assert_eq!(read_value[size-1], 0xAB);
        }
    }
    
    Ok(())
}

#[test]
fn test_very_large_overflow_value() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(1 << 30) // 1 GB
            .open(dir.path())?
    );
    
    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Test a 50MB value (typical large FST size)
    let large_size = 50 * 1024 * 1024;
    let key = b"large_fst";
    let value = vec![0x42; large_size];
    
    // Write
    {
        let mut txn = env.write_txn()?;
        db.put(&mut txn, key.to_vec(), value.clone())?;
        txn.commit()?;
    }
    
    // Read and verify
    {
        let txn = env.read_txn()?;
        let read_value = db.get(&txn, &key.to_vec())?.unwrap();
        assert_eq!(read_value.len(), large_size);
        
        // Check a few spots to ensure data integrity
        assert_eq!(read_value[0], 0x42);
        assert_eq!(read_value[large_size/2], 0x42);
        assert_eq!(read_value[large_size-1], 0x42);
    }
    
    Ok(())
}