# LMDB Delete Operation Analysis

## Overview
This document analyzes how LMDB handles delete operations and page rebalancing to ensure ZeroDB can replicate this behavior exactly for production use.

## Key LMDB Concepts

### 1. Page Structure
- **Page Size**: Default 4096 bytes  
- **Minimum Page Usage**: LMDB uses FILL_THRESHOLD = 250 (approximately 25% of page)
- **Branch Pages**: Must have at least 2 keys (minkeys = 2)
- **Leaf Pages**: Must have at least 1 key (minkeys = 1)

### 2. Copy-on-Write (COW)
- Every modification creates a new page
- Original pages are never modified
- Old pages are added to freelist after no readers need them

### 3. Rebalancing Strategy
LMDB's `mdb_rebalance()` function follows this logic:

```
1. If page has enough keys/data → No rebalancing needed
2. If page is root → Special handling (can go to 0 keys for branch)
3. Try to merge with a sibling
4. If merge not possible → Try to move keys from sibling
5. If neither works → Leave page as-is (underflowed)
```

## Critical Insight: LMDB Never Fails on Delete

LMDB **NEVER** returns an error like "Page full" during delete operations because:

1. **Merge Check**: Before merging, LMDB calculates if combined data fits
2. **Move Check**: Before moving keys, LMDB ensures target page has space
3. **Graceful Degradation**: If neither merge nor move is possible, the page remains underflowed

## The Real LMDB Algorithm

Based on LMDB source code analysis:

### mdb_page_merge (simplified):
```c
// Check if pages can be merged
if (PAGEFILL(src_page) + PAGEFILL(dst_page) > PAGESIZE) {
    // Cannot merge - pages too full
    return MDB_SUCCESS;  // Not an error!
}
// Proceed with merge...
```

### mdb_node_move (simplified):
```c
// Check if destination has room
if (node_size > SIZELEFT(dst_page)) {
    // Cannot move - no room
    return MDB_SUCCESS;  // Not an error!
}
// Proceed with move...
```

## Why ZeroDB Fails

ZeroDB is failing because:
1. It's using aggressive MIN_KEYS_PER_PAGE = MAX_KEYS_PER_PAGE / 2
2. With 500-byte values, pages can't hold enough keys to satisfy this minimum
3. When rebalancing fails, we're not handling it gracefully

## The Solution

1. **Use LMDB's fill threshold approach** (25% of page) instead of key count
2. **Never fail during delete** - if rebalancing isn't possible, leave page underflowed
3. **Implement proper space checks** before attempting any operation

## Test Cases Needed

1. **Large Value Test**: Insert/delete with values that take >25% of page
2. **Mixed Workload Test**: Sequential inserts with random deletes
3. **Stress Test**: Verify no "Page full" errors ever occur during deletes
4. **Fill Ratio Test**: Verify pages can go below 25% fill when necessary