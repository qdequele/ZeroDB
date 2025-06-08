use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use heed_core::{EnvBuilder, page::PageFlags};
use tempfile::TempDir;

fn bench_freelist_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("freelist_allocation");
    
    // Test different allocation patterns
    let patterns = vec![
        ("uniform_small", vec![1; 100]),
        ("uniform_medium", vec![8; 100]),
        ("uniform_large", vec![32; 100]),
        ("mixed_sizes", (1..=32).cycle().take(100).collect()),
        ("fragmented", vec![1, 32, 2, 16, 4, 8, 1, 64, 3, 5]),
    ];
    
    for (pattern_name, allocation_sizes) in patterns {
        // Benchmark simple freelist
        group.bench_with_input(
            BenchmarkId::new("simple", pattern_name),
            &allocation_sizes,
            |b, sizes| {
                let dir = TempDir::new().unwrap();
                let env = EnvBuilder::new()
                    .map_size(100 * 1024 * 1024)
                    .use_segregated_freelist(false)
                    .open(dir.path())
                    .unwrap();
                
                // Pre-fragment the freelist
                {
                    let mut txn = env.begin_write_txn().unwrap();
                    let mut pages = Vec::new();
                    
                    // Allocate many pages
                    for i in 0..1000 {
                        let size = ((i % 32) + 1) as usize;
                        if let Ok(allocated) = txn.alloc_pages(size, PageFlags::LEAF) {
                            pages.push((allocated[0].0, size));
                        }
                    }
                    
                    // Free every other allocation
                    for (i, (page_id, size)) in pages.iter().enumerate() {
                        if i % 2 == 0 {
                            txn.free_pages(*page_id, *size).unwrap();
                        }
                    }
                    
                    txn.commit().unwrap();
                }
                
                b.iter(|| {
                    let mut txn = env.begin_write_txn().unwrap();
                    
                    for &size in sizes {
                        let _ = txn.alloc_pages(size, PageFlags::LEAF);
                    }
                    
                    txn.commit().unwrap();
                });
            },
        );
        
        // Benchmark segregated freelist
        group.bench_with_input(
            BenchmarkId::new("segregated", pattern_name),
            &allocation_sizes,
            |b, sizes| {
                let dir = TempDir::new().unwrap();
                let env = EnvBuilder::new()
                    .map_size(100 * 1024 * 1024)
                    .use_segregated_freelist(true)
                    .open(dir.path())
                    .unwrap();
                
                // Pre-fragment the freelist
                {
                    let mut txn = env.begin_write_txn().unwrap();
                    let mut pages = Vec::new();
                    
                    // Allocate many pages
                    for i in 0..1000 {
                        let size = ((i % 32) + 1) as usize;
                        if let Ok(allocated) = txn.alloc_pages(size, PageFlags::LEAF) {
                            pages.push((allocated[0].0, size));
                        }
                    }
                    
                    // Free every other allocation
                    for (i, (page_id, size)) in pages.iter().enumerate() {
                        if i % 2 == 0 {
                            txn.free_pages(*page_id, *size).unwrap();
                        }
                    }
                    
                    txn.commit().unwrap();
                }
                
                b.iter(|| {
                    let mut txn = env.begin_write_txn().unwrap();
                    
                    for &size in sizes {
                        let _ = txn.alloc_pages(size, PageFlags::LEAF);
                    }
                    
                    txn.commit().unwrap();
                });
            },
        );
    }
    
    group.finish();
}

fn bench_freelist_fragmentation(c: &mut Criterion) {
    let mut group = c.benchmark_group("freelist_fragmentation");
    
    // Benchmark how well each freelist handles fragmentation over time
    group.bench_function("simple_fragmentation", |b| {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new()
            .map_size(100 * 1024 * 1024)
            .use_segregated_freelist(false)
            .open(dir.path())
            .unwrap();
        
        b.iter(|| {
            let mut txn = env.begin_write_txn().unwrap();
            let mut allocated = Vec::new();
            
            // Simulate workload that creates fragmentation
            for i in 0..100 {
                let size = ((i % 16) + 1) as usize;
                if let Ok(pages) = txn.alloc_pages(size, PageFlags::LEAF) {
                    allocated.push((pages[0].0, size, i));
                }
            }
            
            // Free in a pattern that creates fragmentation
            for (page_id, size, index) in &allocated {
                if index % 3 == 0 || index % 5 == 0 {
                    txn.free_pages(*page_id, *size).unwrap();
                }
            }
            
            txn.commit().unwrap();
        });
    });
    
    group.bench_function("segregated_fragmentation", |b| {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new()
            .map_size(100 * 1024 * 1024)
            .use_segregated_freelist(true)
            .open(dir.path())
            .unwrap();
        
        b.iter(|| {
            let mut txn = env.begin_write_txn().unwrap();
            let mut allocated = Vec::new();
            
            // Simulate workload that creates fragmentation
            for i in 0..100 {
                let size = ((i % 16) + 1) as usize;
                if let Ok(pages) = txn.alloc_pages(size, PageFlags::LEAF) {
                    allocated.push((pages[0].0, size, i));
                }
            }
            
            // Free in a pattern that creates fragmentation
            for (page_id, size, index) in &allocated {
                if index % 3 == 0 || index % 5 == 0 {
                    txn.free_pages(*page_id, *size).unwrap();
                }
            }
            
            txn.commit().unwrap();
        });
    });
    
    group.finish();
}

criterion_group!(benches, bench_freelist_allocation, bench_freelist_fragmentation);
criterion_main!(benches);