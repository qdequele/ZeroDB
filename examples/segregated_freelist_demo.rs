//! Demo of segregated freelist performance improvements

use std::time::Instant;
use zerodb::{EnvBuilder, Result};

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
    // creating fragmentation

    let iterations = 1000;

    // Test simple freelist
    println!("\nSimple freelist allocation pattern:");
    let start = Instant::now();
    {
        let mut txn = env_simple.write_txn()?;
        let mut allocated_pages = Vec::new();

        // Allocate many pages
        for i in 0..iterations {
            let (page_id, _page) = txn.alloc_page(zerodb::page::PageFlags::LEAF)?;
            allocated_pages.push(page_id);

            // Free every third page to create fragmentation
            if i > 0 && i % 3 == 0 {
                if let Some(old_page) = allocated_pages.get(i - 3) {
                    txn.free_page(*old_page)?;
                }
            }
        }

        txn.commit()?;
    }
    let simple_time = start.elapsed();
    println!("Time: {:?}", simple_time);

    // Test segregated freelist
    println!("\nSegregated freelist allocation pattern:");
    let start = Instant::now();
    {
        let mut txn = env_segregated.write_txn()?;
        let mut allocated_pages = Vec::new();

        // Allocate many pages
        for i in 0..iterations {
            let (page_id, _page) = txn.alloc_page(zerodb::page::PageFlags::LEAF)?;
            allocated_pages.push(page_id);

            // Free every third page to create fragmentation
            if i > 0 && i % 3 == 0 {
                if let Some(old_page) = allocated_pages.get(i - 3) {
                    txn.free_page(*old_page)?;
                }
            }
        }

        txn.commit()?;
    }
    let segregated_time = start.elapsed();
    println!("Time: {:?}", segregated_time);

    println!("\nPhase 2: Allocation performance with fragmentation");
    println!("=================================================");

    // Now test allocation performance with the fragmented freelists
    let alloc_count = 500;

    // Simple freelist
    println!("\nSimple freelist allocations:");
    let start = Instant::now();
    {
        let mut txn = env_simple.write_txn()?;
        for _ in 0..alloc_count {
            let (_page_id, _page) = txn.alloc_page(zerodb::page::PageFlags::LEAF)?;
        }
        txn.commit()?;
    }
    let simple_alloc_time = start.elapsed();
    println!("Time for {} allocations: {:?}", alloc_count, simple_alloc_time);
    println!("Average per allocation: {:?}", simple_alloc_time / alloc_count);

    // Segregated freelist
    println!("\nSegregated freelist allocations:");
    let start = Instant::now();
    {
        let mut txn = env_segregated.write_txn()?;
        for _ in 0..alloc_count {
            let (_page_id, _page) = txn.alloc_page(zerodb::page::PageFlags::LEAF)?;
        }
        txn.commit()?;
    }
    let segregated_alloc_time = start.elapsed();
    println!("Time for {} allocations: {:?}", alloc_count, segregated_alloc_time);
    println!("Average per allocation: {:?}", segregated_alloc_time / alloc_count);

    println!("\nResults Summary");
    println!("===============");
    println!("Fragmentation creation:");
    println!("  Simple:     {:?}", simple_time);
    println!("  Segregated: {:?}", segregated_time);
    println!("  Speedup:    {:.2}x", simple_time.as_secs_f64() / segregated_time.as_secs_f64());

    println!("\nAllocation with fragmentation:");
    println!("  Simple:     {:?}/alloc", simple_alloc_time / alloc_count);
    println!("  Segregated: {:?}/alloc", segregated_alloc_time / alloc_count);
    println!(
        "  Speedup:    {:.2}x",
        simple_alloc_time.as_secs_f64() / segregated_alloc_time.as_secs_f64()
    );

    Ok(())
}
