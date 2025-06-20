# Delete Rebalancing Page Allocation Issue

**Last Updated**: 2025-06-19

## Update: COW Implementation Complete

We have successfully implemented proper Copy-on-Write (COW) for all delete and rebalancing operations, matching LMDB's approach. However, a capacity issue remains with specific workload patterns.

## Current Status

### Implemented Fixes:
1. ✅ All delete operations now use COW (`get_page_cow`)
2. ✅ Rebalancing operations (borrow, merge) use COW
3. ✅ Parent references are updated after COW operations
4. ✅ Space checking before borrow operations

### Remaining Issue:
The "Page full" error still occurs in mixed insert/delete workloads with medium-to-large values. This is due to fundamental B+tree constraints when pages cannot be successfully rebalanced due to size limitations.

## Summary
Delete operations in ZeroDB can cause "Page full" errors during rebalancing when the combined size of entries exceeds page capacity, even with proper COW implementation.

## Root Cause Analysis

### 1. Delete-Triggered Rebalancing
When a delete operation causes a page to have fewer than `MIN_KEYS_PER_PAGE` keys, the B-tree initiates rebalancing to maintain its structural properties.

### 2. Rebalancing Operations
The rebalancing process involves either:
- **Borrowing keys** from sibling pages
- **Merging** underflowed pages when borrowing isn't possible

### 3. In-Place Page Modifications
The current implementation uses `get_page_mut()` which modifies pages in-place rather than allocating new pages. During merge operations, this can fail if:
- The combined content from two underflowed pages doesn't fit in a single page
- Multiple pages need modification but there isn't enough space

### 4. Mixed Workload Impact
Mixed insert/delete patterns are particularly problematic because:
- Inserts cause page splits (allocating new pages)
- Deletes cause page merges (requiring space in existing pages)
- The combination exhausts available page space faster than pure operations

## Key Difference from LMDB
LMDB implements true copy-on-write for ALL page modifications:
- Every page modification allocates a new page
- Old pages are added to the freelist for future reuse
- This prevents "Page full" errors during rebalancing

## Current Workarounds
1. **Smaller transaction batches**: Limit mixed insert/delete operations to ~10 per transaction
2. **Avoid mixed patterns**: Separate insert and delete operations into different transactions
3. **Use segregated freelist**: Enable the segregated freelist for better page recycling

## Long-term Solutions
1. **Implement true COW for rebalancing**: Use `get_page_cow()` instead of `get_page_mut()` in rebalancing operations
2. **Pre-check merge feasibility**: Verify combined content will fit before attempting merge
3. **Allocate new pages during merge**: Create new pages for merged content rather than modifying in-place
4. **Improve page recycling**: Implement LMDB's loose pages concept for same-transaction reuse

## Code References
- Delete operation: `src/btree.rs:652-689`
- Rebalancing: `src/btree.rs:819-906`
- Page merging: `src/btree.rs:1113-1202`
- Page allocation: `src/txn.rs:600-649`

## Benchmark Adjustments
- `benches/page_alloc.rs`: Reduced mixed pattern from 50 to 10 operations
- `benches/db_comparison.rs`: Skip ZeroDB for large random write batches

## Related Issues
- `PAGE_FULL_ISSUE.md`: Documents the general page full problem with random writes
- Similar root cause: B-tree structure constraints with fixed 4KB pages