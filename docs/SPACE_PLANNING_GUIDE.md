# Space Planning Guide for ZeroDB

## Overview

ZeroDB uses memory-mapped files for storage, which means you need to allocate the maximum database size upfront. This guide helps you plan the appropriate `map_size` for your use case.

## Quick Reference

| Use Case | Entries | Key Size | Value Size | Recommended Map Size |
|----------|---------|----------|------------|---------------------|
| Small Dataset | < 10K | < 100B | < 1KB | 1 GB |
| Medium Dataset | 10K-100K | < 100B | 1-10KB | 2-4 GB |
| Large Dataset | 100K-1M | < 100B | 10-100KB | 8-20 GB |
| Very Large Dataset | 1M+ | < 100B | > 100KB | 32+ GB |

## Understanding Space Requirements

### Basic Formula

```
Required Space = (Number of Entries × Average Entry Size × Overhead Factor) + Metadata
```

Where:
- **Average Entry Size** = Key Size + Value Size
- **Overhead Factor** = 1.5-2.0 (for B-tree structure)
- **Metadata** = ~4MB (for database housekeeping)

### Page-Based Calculation

ZeroDB uses 4KB pages. Each entry requires:
- **Small entries** (< 2KB): Multiple entries per page
- **Medium entries** (2-4KB): One entry per page
- **Large entries** (> 4KB): Multiple overflow pages

## Using the MapSizeEstimator

```rust
use zerodb::space_info::MapSizeEstimator;

// Example: 100K entries with 16B keys and 100KB values
let estimator = MapSizeEstimator::new(100_000, 16, 100_000);
let recommended_size = estimator.estimate();
println!("{}", estimator.breakdown());
```

## Common Scenarios

### 1. Key-Value Cache (Small Values)
```rust
// 1M entries, 64B keys, 256B values
let env = EnvBuilder::new()
    .map_size(1 * 1024 * 1024 * 1024) // 1GB
    .open(path)?;
```

### 2. Document Store (Medium Values)
```rust
// 100K documents, 32B keys, 10KB values
let env = EnvBuilder::new()
    .map_size(4 * 1024 * 1024 * 1024) // 4GB
    .open(path)?;
```

### 3. Blob Storage (Large Values)
```rust
// 50K blobs, 16B keys, 1MB values
let env = EnvBuilder::new()
    .map_size(64 * 1024 * 1024 * 1024) // 64GB
    .open(path)?;
```

## Monitoring Space Usage

### Real-time Monitoring
```rust
let info = env.space_info()?;
println!("Database usage: {:.1}% ({} MB / {} GB)",
    info.percent_of_map_used,
    info.db_size_bytes / (1024 * 1024),
    info.max_db_size_bytes / (1024 * 1024 * 1024));

if info.is_near_capacity(80.0) {
    eprintln!("WARNING: Database approaching capacity!");
}
```

### Estimating Remaining Capacity
```rust
let info = env.space_info()?;
let remaining = info.estimate_entries_remaining(avg_entry_size);
println!("Can store approximately {} more entries", remaining);
```

## Error Handling

When the database is full, you'll see:
```
Error: Page ID 524802 exceeds database limit: requested page 524802 
(offset 2149588992 bytes) but database has only 524288 pages 
(2147483648 bytes). Consider increasing map_size.
```

## Best Practices

1. **Plan for Growth**: Allocate 20-50% more space than currently needed
2. **Monitor Usage**: Check space usage regularly, especially before bulk operations
3. **Use Appropriate Sizes**: Don't allocate excessive space - it affects startup time
4. **Consider Memory Limits**: Map size counts towards virtual memory usage

## Performance Considerations

- **Startup Time**: Larger map sizes take longer to initialize
- **Memory Usage**: The entire map size is reserved in virtual memory
- **Page Faults**: First access to new pages may cause page faults
- **OS Limits**: Check system limits with `ulimit -v`

## Platform-Specific Notes

### Linux
- Use `vm.overcommit_memory=1` for large databases
- Check `/proc/sys/vm/max_map_count` for mmap limits

### macOS
- Virtual memory is more flexible
- Still subject to system memory pressure

### Windows
- Requires appropriate page file size
- May need to adjust system virtual memory settings

## Troubleshooting

### "Database Full" Errors
1. Check current usage: `env.space_info()`
2. Estimate required size: Use `MapSizeEstimator`
3. Create new environment with larger map_size
4. Copy data to new environment

### Memory Allocation Failures
- Reduce map_size
- Check system limits: `ulimit -a`
- Ensure sufficient free memory

### Slow Startup
- Consider smaller initial map_size
- Use lazy initialization where possible
- Profile with `RUST_LOG=debug`

## Future Improvements

We're working on:
- Automatic database growth (see issue #XXX)
- Dynamic map resizing
- Incremental growth policies
- Better space prediction algorithms