use zerodb::{EnvBuilder, Result, db::Database};
use tempfile::TempDir;
use std::sync::Arc;

fn main() -> Result<()> {
    println!("ZeroDB Overflow Page Analysis");
    println!("=============================\n");
    
    // Constants from the codebase
    const PAGE_SIZE: usize = 4096;
    const PAGE_HEADER_SIZE: usize = 48; // size_of::<PageHeader>()
    const OVERFLOW_HEADER_SIZE: usize = 16; // size_of::<OverflowHeader>()
    const MAX_INLINE_VALUE_SIZE: usize = PAGE_SIZE / 4; // 1024 bytes
    
    println!("Page Configuration:");
    println!("  - Page size: {} bytes", PAGE_SIZE);
    println!("  - Page header size: {} bytes", PAGE_HEADER_SIZE);
    println!("  - Overflow header size: {} bytes", OVERFLOW_HEADER_SIZE);
    println!("  - Max inline value size: {} bytes\n", MAX_INLINE_VALUE_SIZE);
    
    // Calculate usable space per overflow page
    let data_per_overflow_page = PAGE_SIZE - PAGE_HEADER_SIZE - OVERFLOW_HEADER_SIZE;
    println!("Overflow Page Capacity:");
    println!("  - Usable data per overflow page: {} bytes", data_per_overflow_page);
    println!("  - Overhead per page: {} bytes ({:.1}%)\n", 
        PAGE_HEADER_SIZE + OVERFLOW_HEADER_SIZE,
        ((PAGE_HEADER_SIZE + OVERFLOW_HEADER_SIZE) as f64 / PAGE_SIZE as f64) * 100.0
    );
    
    // Calculate theoretical maximum value size
    // total_size is stored as u64, so theoretical max is 2^64 - 1
    let theoretical_max = u64::MAX;
    println!("Theoretical Limits:");
    println!("  - Max value size (u64): {} bytes ({:.2} EB)", 
        theoretical_max, 
        theoretical_max as f64 / (1024_f64.powi(6))
    );
    println!("  - Max overflow pages needed: {}", theoretical_max / data_per_overflow_page as u64);
    
    // Practical examples for large data structures
    println!("\nPractical Examples:");
    
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
        
        println!("\n  {}:", name);
        println!("    - Data size: {} bytes ({:.2} MB)", size, size as f64 / (1024.0 * 1024.0));
        println!("    - Overflow pages needed: {}", pages_needed);
        println!("    - Total disk space: {} bytes ({:.2} MB)", total_space, total_space as f64 / (1024.0 * 1024.0));
        println!("    - Storage overhead: {} bytes ({:.1}%)", overhead, overhead_percent);
    }
    
    // Test actual storage
    println!("\n\nTesting Actual Storage:");
    println!("========================\n");
    
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
        println!("Testing {} value...", name);
        
        let key = format!("test_{}", size);
        let value = vec![0xAB; size];
        
        let mut txn = env.write_txn()?;
        match db.put(&mut txn, key.as_bytes().to_vec(), value.clone()) {
            Ok(()) => {
                println!("  ✓ Successfully stored {} value", name);
                txn.commit()?;
                
                // Verify read
                let read_txn = env.read_txn()?;
                match db.get(&read_txn, &key.as_bytes().to_vec()) {
                    Ok(Some(read_value)) => {
                        assert_eq!(read_value.len(), size);
                        println!("  ✓ Successfully read back {} value", name);
                    }
                    Ok(None) => println!("  ✗ Value not found after commit"),
                    Err(e) => println!("  ✗ Read error: {}", e),
                }
            }
            Err(e) => {
                println!("  ✗ Write error: {}", e);
                break;
            }
        }
    }
    
    Ok(())
}