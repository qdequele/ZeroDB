# Page Full Issue Analysis

**Last Updated**: Current date

## Executive Summary
The "Page full" error during random writes is caused by the fundamental space constraints of the B-tree structure when handling completely random insertion patterns. With zero safety margins, we can handle ~27-43 random entries per transaction depending on the specific key distribution and build mode.

## Summary
The "Page full" error occurs during random write operations, particularly when inserting 50+ entries with random keys in a single transaction. This is a known limitation of the current page allocation strategy.

## Root Cause
1. **Random insertion patterns** cause B-tree pages to split at multiple levels simultaneously
2. **50/50 split strategy** for random inserts creates many half-full pages
3. **Conservative fill factors** (75-95%) leave significant unused space
4. **Cascading splits** consume available pages faster than sequential writes

## Current Status
- Sequential writes: ✅ Working well (up to 10K+ entries)
- Random writes: ⚠️ Limited to ~40-80 entries per transaction
- Benchmarks: ✅ Sequential benchmarks pass, random writes need smaller batch sizes

## Improvements Made
1. **Dynamic safety margins**: Reduced from 256 to 32 bytes minimum
2. **Graduated fill thresholds**: 95% for small pages, 85% for large pages
3. **Intelligent capacity checks**: Based on page key count

## Workarounds
1. **Smaller transaction batches**: Commit every 25-40 entries for random writes
2. **Pre-sort keys**: Sort random keys before insertion to improve locality
3. **Increase page size**: Use larger pages (8KB or 16KB) if possible

## Long-term Solutions Needed
1. **Configurable page sizes**: Allow users to set page size based on workload
2. **Better page allocation**: Implement a more sophisticated freelist algorithm
3. **Adaptive split strategies**: Detect random vs sequential patterns and adjust
4. **Page defragmentation**: Reclaim space from partially-filled pages
5. **Bulk loading API**: Special path for initial data loading

## Benchmark Recommendations
For the db_comparison benchmark with random writes:
- Use batch size of 25-40 for zerodb
- Or skip zerodb for random write benchmarks until fixed
- Document this as a known limitation

## Code References
- Page capacity: `src/page.rs:520-551` (has_room_for method)
- Split logic: `src/btree.rs:549-610` (split_branch_page)
- Fill factors: `src/page_capacity.rs:8-15` (constants)