# ZeroDB Test Suite Documentation

This document provides an overview of the ZeroDB test suite, explaining the purpose of each test category and how to run them effectively.

## Test Categories

### 1. **Basic Operations** (`test_basic*.rs`)
Tests fundamental database operations:
- `test_basic.rs` - Core CRUD operations
- `test_basic_db.rs` - Database creation and basic put/get
- `test_initial_db.rs` - Initial database setup
- `test_main_db.rs` - Main database functionality

### 2. **B+Tree Operations** (`test_btree*.rs`, `test_rebalancing*.rs`)
Tests the B+tree implementation:
- `test_btree_delete.rs` - B+tree deletion behavior
- `test_btree_split.rs` - Page splitting logic
- `test_rebalancing.rs` - Tree rebalancing operations
- `test_rebalancing_debug.rs` - Detailed rebalancing analysis

### 3. **Catalog and Named Databases** (`test_catalog*.rs`)
Tests database catalog functionality:
- `test_catalog.rs` - Basic catalog operations
- `test_catalog_fix.rs` - Catalog bug fixes and edge cases
- `test_catalog_persistence.rs` - Catalog persistence across reopens

### 4. **Concurrency** (`test_concurrency.rs`)
Comprehensive concurrent operation tests:
- Multiple concurrent readers
- Reader-writer isolation
- Write serialization
- Transaction isolation
- Reader tracking and limits

### 5. **Copy-on-Write (COW)** (`test_cow*.rs`)
Tests COW implementation:
- `test_cow.rs` - Basic COW functionality
- `test_cow_overflow.rs` - COW with overflow pages

### 6. **Cursor Operations** (`test_cursor*.rs`)
Tests cursor navigation and operations:
- `test_cursor.rs` - Comprehensive cursor tests
- `test_cursor_vs_get_discrepancy.rs` - Cursor vs get consistency

### 7. **Delete Operations** (`test_delete_operations.rs`)
Consolidated delete operation tests covering:
- Basic deletion
- Bulk deletes
- Delete with rebalancing
- Delete with overflow values
- Delete edge cases

### 8. **Duplicate Sort** (`test_dupsort.rs`)
Tests DUPSORT functionality for duplicate keys

### 9. **Error Handling** (`test_error_handling.rs`)
Tests error conditions and edge cases:
- Resource limits
- Invalid operations
- Recovery scenarios
- Edge case handling

### 10. **Freelist Management** (`test_freelist*.rs`)
Tests free page management:
- `test_freelist_basic.rs` - Basic freelist operations
- `test_freelist_persistence.rs` - Freelist persistence
- `test_freelist_simple.rs` - Simple freelist scenarios
- `test_freelist_state.rs` - Freelist state management
- `test_segregated_freelist_issue.rs` - Segregated freelist bugs

### 11. **I/O Operations** (`test_io.rs`)
Tests I/O backend functionality

### 12. **LMDB Compatibility** (`lmdb_comparison.rs`, `quickcheck_comparison.rs`)
Ensures compatibility with LMDB:
- `lmdb_comparison.rs` - Direct behavior comparison
- `quickcheck_comparison.rs` - Property-based testing
- `test_lmdb_delete_behavior.rs` - Delete behavior compatibility

### 13. **Overflow Pages** (`test_overflow*.rs`, `test_simple_overflow.rs`)
Tests handling of large values:
- `test_overflow_issue.rs` - Overflow page issues
- `test_overflow_cow.rs` - COW with overflow
- `test_simple_overflow.rs` - Basic overflow functionality

### 14. **Page Management** (`test_page*.rs`)
Tests page allocation and capacity:
- `test_page_capacity.rs` - Page capacity calculations
- `test_page_capacity_fix.rs` - Capacity bug fixes
- `test_page_allocation_fixes.rs` - Allocation improvements
- `test_random_writes_page_full.rs` - PageFull error scenarios

### 15. **Reader Tracking** (`test_reader_tracking.rs`)
Tests reader transaction tracking and cleanup

### 16. **Stress Tests** (`tests/stress/`)
Performance and stress tests moved from examples:
- `test_cow_delete.rs` - COW delete stress
- `test_large_values.rs` - Large value handling
- `test_overflow_limits.rs` - Overflow limits
- `test_page_full.rs` - PageFull scenarios
- `test_zero_copy.rs` - Zero-copy performance

### 17. **Transaction Management** (`test_commit_issue.rs`)
Tests transaction commit and rollback

### 18. **Other Tests**
- `test_copy.rs` - Database copy operations
- `test_sequential_key_prefixes.rs` - Sequential key handling
- `test_debug.rs` - General debugging tests
- `test_debug_insert_issue.rs` - Insert issue debugging

## Running Tests

### Run All Tests
```bash
cargo test
```

### Run Specific Test Category
```bash
# Run all delete tests
cargo test delete

# Run all concurrent tests
cargo test concurrency

# Run all freelist tests
cargo test freelist
```

### Run Single Test
```bash
cargo test test_concurrent_readers
```

### Run Tests with Output
```bash
cargo test -- --nocapture
```

### Run Tests in Release Mode
```bash
cargo test --release
```

### Run Stress Tests
```bash
cargo test --test test_large_values -- --nocapture
```

## Test Environment Variables

- `RUST_TEST_THREADS=1` - Run tests sequentially
- `RUST_LOG=debug` - Enable debug logging
- `RUST_BACKTRACE=1` - Show backtrace on panic

## Known Test Limitations

1. **Random Write Tests**: Limited to ~30-40 entries per transaction due to page allocation constraints
2. **Platform-Specific Tests**: Some tests may behave differently on different platforms
3. **Concurrency Tests**: May be flaky under heavy system load

## Adding New Tests

When adding new tests:

1. Place in appropriate category or create new file if needed
2. Use descriptive test names
3. Add documentation comments explaining what the test verifies
4. Clean up temporary files/directories
5. Handle platform differences appropriately
6. Avoid hardcoded timeouts where possible

## Test Coverage

Current test coverage includes:
- ✅ Basic CRUD operations
- ✅ B+tree operations and edge cases
- ✅ Concurrent access patterns
- ✅ Error handling and recovery
- ✅ LMDB compatibility
- ✅ Performance characteristics
- ⚠️ Limited random write scenarios
- ⚠️ Limited large-scale stress tests

## Continuous Integration

Tests are run automatically on:
- Every pull request
- Every commit to main branch
- Multiple platforms (Linux, macOS, Windows)
- Multiple Rust versions (stable, beta)

## Debugging Test Failures

1. Run with `--nocapture` to see println! output
2. Use `RUST_LOG=debug` for detailed logging
3. Run single test in isolation
4. Check for resource cleanup issues
5. Verify platform-specific behavior