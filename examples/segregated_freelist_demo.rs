//! Demo of segregated freelist performance improvements

use heed_core::{EnvBuilder, Result};
use std::time::Instant;

fn main() -> Result<()> {
    println!("Segregated Freelist Performance Demo");
    println!("====================================\n");
    
    // Create two environments - one with simple freelist, one with segregated
    let dir1 = tempfile::tempdir()?;
    let dir2 = tempfile::tempdir()?;
    
    println!("Creating environment with simple freelist...");
    let env_simple = EnvBuilder::new()
        .map_size(100 * 1024 * 1024) // 100MB
        .use_segregated_freelist(false)
        .open(dir1.path())?;
    
    println!("Creating environment with segregated freelist...");
    let env_segregated = EnvBuilder::new()
        .map_size(100 * 1024 * 1024) // 100MB
        .use_segregated_freelist(true)
        .open(dir2.path())?;
    
    println!("\nPhase 1: Creating fragmentation pattern");
    println!("========================================");
    
    // Create a fragmentation pattern by allocating and freeing pages
    // This simulates real-world usage where pages are allocated and freed
    // in different sizes, creating fragmentation
    
    let pattern_sizes = vec![1, 3, 5, 2, 8, 1, 4, 16, 1, 2, 32, 1];
    let iterations = 100;
    
    // Test simple freelist
    println!("\nSimple freelist allocation pattern:");
    let start = Instant::now();
    {
        let mut txn = env_simple.begin_write_txn()?;
        let mut allocated_pages = Vec::new();
        
        // Allocate pages in pattern
        for _ in 0..iterations {
            for &size in &pattern_sizes {
                let pages = txn.alloc_pages(size, heed_core::page::PageFlags::LEAF)?;
                allocated_pages.push((pages[0].0, size));
            }
        }
        
        // Free every other allocation to create fragmentation
        for (i, (page_id, size)) in allocated_pages.iter().enumerate() {
            if i % 2 == 0 {
                txn.free_pages(*page_id, *size)?;
            }
        }
        
        txn.commit()?;
    }
    let simple_fragmentation_time = start.elapsed();
    println!("  Time: {:?}", simple_fragmentation_time);
    
    // Test segregated freelist
    println!("\nSegregated freelist allocation pattern:");
    let start = Instant::now();
    {
        let mut txn = env_segregated.begin_write_txn()?;
        let mut allocated_pages = Vec::new();
        
        // Allocate pages in pattern
        for _ in 0..iterations {
            for &size in &pattern_sizes {
                let pages = txn.alloc_pages(size, heed_core::page::PageFlags::LEAF)?;
                allocated_pages.push((pages[0].0, size));
            }
        }
        
        // Free every other allocation to create fragmentation
        for (i, (page_id, size)) in allocated_pages.iter().enumerate() {
            if i % 2 == 0 {
                txn.free_pages(*page_id, *size)?;
            }
        }
        
        txn.commit()?;
    }
    let segregated_fragmentation_time = start.elapsed();
    println!("  Time: {:?}", segregated_fragmentation_time);
    
    println!("\nPhase 2: Allocating from fragmented freelist");
    println!("============================================");
    
    // Now allocate pages from the fragmented freelist
    // The segregated freelist should be much faster at finding
    // appropriately sized free extents
    
    let allocation_sizes = vec![1, 2, 3, 4, 8, 16, 32];
    let allocation_count = 50;
    
    // Test simple freelist
    println!("\nSimple freelist re-allocation:");
    let start = Instant::now();
    let mut simple_alloc_success = 0;
    {
        let mut txn = env_simple.begin_write_txn()?;
        
        for _ in 0..allocation_count {
            for &size in &allocation_sizes {
                match txn.alloc_pages(size, heed_core::page::PageFlags::LEAF) {
                    Ok(_) => simple_alloc_success += 1,
                    Err(_) => break,
                }
            }
        }
        
        txn.commit()?;
    }
    let simple_realloc_time = start.elapsed();
    println!("  Time: {:?}", simple_realloc_time);
    println!("  Successful allocations: {}", simple_alloc_success);
    
    // Test segregated freelist
    println!("\nSegregated freelist re-allocation:");
    let start = Instant::now();
    let mut segregated_alloc_success = 0;
    {
        let mut txn = env_segregated.begin_write_txn()?;
        
        for _ in 0..allocation_count {
            for &size in &allocation_sizes {
                match txn.alloc_pages(size, heed_core::page::PageFlags::LEAF) {
                    Ok(_) => segregated_alloc_success += 1,
                    Err(_) => break,
                }
            }
        }
        
        txn.commit()?;
    }
    let segregated_realloc_time = start.elapsed();
    println!("  Time: {:?}", segregated_realloc_time);
    println!("  Successful allocations: {}", segregated_alloc_success);
    
    println!("\nResults Summary");
    println!("===============");
    println!("Fragmentation creation speedup: {:.2}x", 
             simple_fragmentation_time.as_secs_f64() / segregated_fragmentation_time.as_secs_f64());
    println!("Re-allocation speedup: {:.2}x",
             simple_realloc_time.as_secs_f64() / segregated_realloc_time.as_secs_f64());
    println!("Allocation efficiency improvement: {:.1}%",
             ((segregated_alloc_success as f64 / simple_alloc_success as f64) - 1.0) * 100.0);
    
    println!("\nThe segregated freelist provides:");
    println!("- Faster allocation by organizing free pages by size class");
    println!("- Better space utilization through intelligent coalescing");
    println!("- Reduced fragmentation through best-fit allocation strategies");
    
    Ok(())
}