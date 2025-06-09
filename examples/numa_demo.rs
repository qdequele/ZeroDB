//! NUMA-aware allocation demonstration

use std::thread;
use std::time::Instant;
use zerodb::{EnvBuilder, Result};

fn main() -> Result<()> {
    println!("NUMA-Aware Allocation Demo");
    println!("==========================\n");

    // Detect NUMA topology
    let topology = zerodb::numa::NumaTopology::detect()?;
    println!("System NUMA Configuration:");
    println!("  NUMA nodes: {}", topology.num_nodes);
    println!("  CPU to node mapping: {:?}", topology.cpu_to_node);
    println!("  Node to CPUs: {:?}", topology.node_to_cpus);
    println!();

    // Create two environments - one with NUMA, one without
    let dir1 = tempfile::tempdir()?;
    let dir2 = tempfile::tempdir()?;

    println!("Creating environment without NUMA awareness...");
    let env_regular = EnvBuilder::new()
        .map_size(1024 * 1024 * 1024) // 1GB
        .use_numa(false)
        .open(dir1.path())?;

    println!("Creating environment with NUMA awareness...");
    let env_numa = EnvBuilder::new()
        .map_size(1024 * 1024 * 1024) // 1GB
        .use_numa(true)
        .open(dir2.path())?;

    // Test 1: Sequential page allocation
    println!("\nTest 1: Sequential Page Allocation");
    println!("==================================");

    let page_count = 10000;

    // Test regular allocation
    println!("\nRegular allocation:");
    let start = Instant::now();
    {
        let mut txn = env_regular.begin_write_txn()?;
        for i in 0..page_count {
            let _ = txn.alloc_page(zerodb::page::PageFlags::LEAF)?;
            if i % 1000 == 0 {
                print!(".");
                use std::io::Write;
                std::io::stdout().flush().unwrap();
            }
        }
        txn.commit()?;
    }
    let regular_time = start.elapsed();
    println!("\n  Time: {:?}", regular_time);

    // Test NUMA-aware allocation
    println!("\nNUMA-aware allocation:");
    let start = Instant::now();
    {
        let mut txn = env_numa.begin_write_txn()?;
        for i in 0..page_count {
            let _ = txn.alloc_page(zerodb::page::PageFlags::LEAF)?;
            if i % 1000 == 0 {
                print!(".");
                use std::io::Write;
                std::io::stdout().flush().unwrap();
            }
        }
        txn.commit()?;
    }
    let numa_time = start.elapsed();
    println!("\n  Time: {:?}", numa_time);

    // Test 2: Multi-threaded allocation (simulating NUMA effects)
    if topology.num_nodes > 1 {
        println!("\nTest 2: Multi-threaded Allocation (NUMA effects)");
        println!("================================================");

        let thread_count = num_cpus::get();
        let pages_per_thread = 1000;

        // Test regular allocation with threads
        println!("\nRegular allocation (multi-threaded):");
        let env_regular = std::sync::Arc::new(env_regular);
        let start = Instant::now();

        let handles: Vec<_> = (0..thread_count)
            .map(|_| {
                let env = env_regular.clone();
                thread::spawn(move || {
                    let mut txn = env.begin_write_txn().unwrap();
                    for _ in 0..pages_per_thread {
                        let _ = txn.alloc_page(zerodb::page::PageFlags::LEAF).unwrap();
                    }
                    txn.commit().unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
        let regular_mt_time = start.elapsed();
        println!("  Time: {:?}", regular_mt_time);

        // Test NUMA-aware allocation with threads
        println!("\nNUMA-aware allocation (multi-threaded):");
        let env_numa = std::sync::Arc::new(env_numa);
        let start = Instant::now();

        let handles: Vec<_> = (0..thread_count)
            .map(|t| {
                let env = env_numa.clone();
                let topology = topology.clone();
                thread::spawn(move || {
                    // Set thread affinity to distribute across NUMA nodes
                    let node = zerodb::numa::NumaNode((t % topology.num_nodes) as u32);
                    if let Ok(affinity) = zerodb::numa::NumaAffinity::for_node(node, &topology) {
                        let _ = affinity.apply();
                    }

                    let mut txn = env.begin_write_txn().unwrap();
                    for _ in 0..pages_per_thread {
                        let _ = txn.alloc_page(zerodb::page::PageFlags::LEAF).unwrap();
                    }
                    txn.commit().unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
        let numa_mt_time = start.elapsed();
        println!("  Time: {:?}", numa_mt_time);

        println!(
            "\nMulti-threaded speedup: {:.2}x",
            regular_mt_time.as_secs_f64() / numa_mt_time.as_secs_f64()
        );
    }

    println!("\nResults Summary");
    println!("===============");
    println!(
        "Sequential allocation speedup: {:.2}x",
        regular_time.as_secs_f64() / numa_time.as_secs_f64()
    );

    if topology.num_nodes > 1 {
        println!("\nNUMA-aware allocation provides:");
        println!("- Better memory locality for multi-socket systems");
        println!("- Reduced cross-socket memory traffic");
        println!("- Improved performance for memory-intensive workloads");
    } else {
        println!("\nNote: This system has only 1 NUMA node.");
        println!("NUMA optimizations have minimal effect on single-socket systems.");
    }

    Ok(())
}
