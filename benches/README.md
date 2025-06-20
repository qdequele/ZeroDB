# ZeroDB Benchmarks

This directory contains comprehensive benchmarks for ZeroDB, organized into two main categories:

## Benchmark Files

### 1. `database_comparison.rs`
Compares ZeroDB against other embedded databases (LMDB, RocksDB, redb, sled) across various workloads:
- Sequential writes (various sizes)
- Random writes (limited for ZeroDB)
- Random reads
- Concurrent reads
- Full table scans
- Mixed read/write workloads

### 2. `zerodb_performance.rs`
Tracks ZeroDB-specific performance characteristics for regression testing:
- B+tree operations (inserts, splits, rebalancing)
- Page allocation and freelist management
- Overflow page handling
- Cursor operations and iteration
- Transaction overhead
- Durability modes impact
- Concurrent operations
- Memory efficiency
- Special access patterns

## Running Benchmarks

### Run all benchmarks:
```bash
cargo bench
```

### Run specific benchmark suite:
```bash
# Database comparison
cargo bench --bench database_comparison

# ZeroDB performance tracking
cargo bench --bench zerodb_performance
```

### Run specific tests:
```bash
# Only sequential writes
cargo bench --bench database_comparison sequential_writes

# Only B+tree operations
cargo bench --bench zerodb_performance btree_operations
```

### Generate reports:
```bash
# Generate comprehensive report
python3 scripts/benchmark_report.py

# Database comparison only
python3 scripts/benchmark_report.py --compare-dbs

# ZeroDB performance only
python3 scripts/benchmark_report.py --zerodb-only
```

## Benchmark Results

Results are stored in `target/criterion/` and include:
- Raw measurements
- Statistical analysis
- HTML reports with graphs

## Performance Characteristics

### ZeroDB Strengths:
- **Sequential writes**: ~880K ops/sec (16x faster than LMDB)
- **Read performance**: ~2.5M ops/sec
- **Iterator performance**: ~3M items/sec
- **Memory efficiency**: Inline storage for values < 1KB

### Current Limitations:
- **Random writes**: Limited to ~30-40 entries per transaction
- **Fixed page size**: 4KB only
- **No compression**: Raw storage only

## Database Comparison Summary

| Operation | ZeroDB | LMDB | RocksDB | redb |
|-----------|--------|------|---------|------|
| Sequential Write | Excellent | Good | Good | Good |
| Random Write | Poor* | Excellent | Excellent | Good |
| Random Read | Excellent | Excellent | Good | Good |
| Concurrent Read | Excellent | Excellent | Good | Fair |
| Memory Usage | Low | Low | High | Medium |

*Limited by PageFull issue with random writes

## Regression Testing

The `zerodb_performance` benchmark suite includes regression thresholds:
- Sequential writes: < 800K ops/sec triggers warning
- Random reads: < 400K ops/sec triggers warning
- Full scan: < 2.5M items/sec triggers warning
- Transaction overhead: > 10μs triggers warning

## Adding New Benchmarks

When adding benchmarks:
1. Group related tests using `benchmark_group`
2. Use meaningful IDs with `BenchmarkId`
3. Set appropriate throughput measurements
4. Consider both time and throughput metrics
5. Test various data sizes and patterns

## Notes

- Benchmarks use `tempfile` for isolated testing
- Each benchmark creates fresh databases
- Random operations use seeded RNG for reproducibility
- Results may vary based on system load and hardware
- Consider running with `taskset` for CPU pinning on Linux