use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use zerodb::{env::EnvBuilder, page::PageFlags};
use tempfile::TempDir;

fn bench_freelist_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("freelist_allocation");
    
    // Test different allocation patterns
    let patterns = vec![
        ("uniform_small", 100),
        ("uniform_medium", 500),
        ("uniform_large", 1000),
        ("mixed_sizes", 250),
    ];
    
    for (pattern_name, num_allocations) in patterns {
        // Benchmark simple freelist
        group.bench_with_input(
            BenchmarkId::new("simple", pattern_name),
            &num_allocations,
            |b, &num_allocs| {
                let dir = TempDir::new().unwrap();
                let env = EnvBuilder::new()
                    .map_size(100 * 1024 * 1024)
                    .use_segregated_freelist(false)
                    .open(dir.path())
                    .unwrap();
                
                b.iter(|| {
                    let mut txn = env.begin_write_txn().unwrap();
                    let mut pages = Vec::new();
                    
                    // Allocate pages
                    for _ in 0..num_allocs {
                        if let Ok((page_id, _page)) = txn.alloc_page(PageFlags::LEAF) {
                            pages.push(page_id);
                        }
                    }
                    
                    // Free half of them to create fragmentation
                    for (i, page_id) in pages.iter().enumerate() {
                        if i % 2 == 0 {
                            let _ = txn.free_page(*page_id);
                        }
                    }
                    
                    txn.commit().unwrap();
                });
            }
        );
        
        // Benchmark segregated freelist
        group.bench_with_input(
            BenchmarkId::new("segregated", pattern_name),
            &num_allocations,
            |b, &num_allocs| {
                let dir = TempDir::new().unwrap();
                let env = EnvBuilder::new()
                    .map_size(100 * 1024 * 1024)
                    .use_segregated_freelist(true)
                    .open(dir.path())
                    .unwrap();
                
                b.iter(|| {
                    let mut txn = env.begin_write_txn().unwrap();
                    let mut pages = Vec::new();
                    
                    // Allocate pages
                    for _ in 0..num_allocs {
                        if let Ok((page_id, _page)) = txn.alloc_page(PageFlags::LEAF) {
                            pages.push(page_id);
                        }
                    }
                    
                    // Free half of them to create fragmentation
                    for (i, page_id) in pages.iter().enumerate() {
                        if i % 2 == 0 {
                            let _ = txn.free_page(*page_id);
                        }
                    }
                    
                    txn.commit().unwrap();
                });
            }
        );
    }
    
    group.finish();
}

fn bench_freelist_fragmentation(c: &mut Criterion) {
    let mut group = c.benchmark_group("freelist_fragmentation");
    
    // Test allocation performance with different fragmentation levels
    let fragmentation_levels = vec![
        ("low", 10),
        ("medium", 50),
        ("high", 90),
    ];
    
    for (frag_name, frag_percent) in fragmentation_levels {
        group.bench_with_input(
            BenchmarkId::new("simple", frag_name),
            &frag_percent,
            |b, &frag_pct| {
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
                    for _ in 0..1000 {
                        if let Ok((page_id, _page)) = txn.alloc_page(PageFlags::LEAF) {
                            pages.push(page_id);
                        }
                    }
                    
                    // Free based on fragmentation percentage
                    for (i, page_id) in pages.iter().enumerate() {
                        if (i * 100 / pages.len()) < frag_pct as usize {
                            let _ = txn.free_page(*page_id);
                        }
                    }
                    
                    txn.commit().unwrap();
                }
                
                // Benchmark allocations in fragmented state
                b.iter(|| {
                    let mut txn = env.begin_write_txn().unwrap();
                    for _ in 0..100 {
                        let _ = txn.alloc_page(PageFlags::LEAF);
                    }
                    txn.commit().unwrap();
                });
            }
        );
        
        group.bench_with_input(
            BenchmarkId::new("segregated", frag_name),
            &frag_percent,
            |b, &frag_pct| {
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
                    for _ in 0..1000 {
                        if let Ok((page_id, _page)) = txn.alloc_page(PageFlags::LEAF) {
                            pages.push(page_id);
                        }
                    }
                    
                    // Free based on fragmentation percentage
                    for (i, page_id) in pages.iter().enumerate() {
                        if (i * 100 / pages.len()) < frag_pct as usize {
                            let _ = txn.free_page(*page_id);
                        }
                    }
                    
                    txn.commit().unwrap();
                }
                
                // Benchmark allocations in fragmented state
                b.iter(|| {
                    let mut txn = env.begin_write_txn().unwrap();
                    for _ in 0..100 {
                        let _ = txn.alloc_page(PageFlags::LEAF);
                    }
                    txn.commit().unwrap();
                });
            }
        );
    }
    
    group.finish();
}

criterion_group!(benches, bench_freelist_allocation, bench_freelist_fragmentation);
criterion_main!(benches);