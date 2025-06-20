# ZeroDB Production Readiness Action Plan

## Overview
This document outlines the specific tasks needed to make ZeroDB production-ready. All tasks will be implemented using Claude Code. The plan is organized by priority, with critical safety issues first.

## Task List

### 🔴 Critical Safety Fixes (Data Loss Prevention)

- [ ] **1. Enable Checksums by Default**
  - File: `src/env.rs`
  - Change default from `ChecksumMode::None` to `ChecksumMode::Fast`
  - Add checksum validation in `src/io.rs` read_page()
  - Add checksum validation in transaction page reads

- [ ] **2. Add Transaction Page Limits**
  - File: `src/txn.rs`
  - Make MAX_TXN_PAGES configurable via EnvConfig
  - Add clear error message when limit exceeded
  - Default: 10,000 pages per transaction
  - Add to EnvBuilder API

- [ ] **3. Fix Random Write Performance (PAGE_FULL errors)**
  - Files: `src/btree.rs`, `src/page.rs`
  - Current issue: Only ~30-40 random writes per transaction
  - Fix page utilization calculation in has_room_for()
  - Implement pre-emptive splitting at 85% full
  - Add better page allocation strategy for random patterns
  - Test with benchmark_workaround.rs

### 🟠 Error Handling Overhaul

- [ ] **4. Replace unwrap() in Transaction Module**
  - File: `src/txn.rs`
  - Replace all unwrap() with ? operator
  - Add context to errors using Error::Custom
  - Handle RwLock poisoning gracefully
  - Count: ~50 unwrap calls

- [ ] **5. Replace unwrap() in BTree Module**
  - File: `src/btree.rs`
  - Replace all unwrap() with proper error propagation
  - Add error context for debugging
  - Count: ~80 unwrap calls

- [ ] **6. Replace unwrap() in Page Module**
  - File: `src/page.rs`
  - Focus on node operations
  - Add bounds checking before unwrap
  - Count: ~40 unwrap calls

- [ ] **7. Replace unwrap() in Remaining Modules**
  - Files: `src/cursor.rs`, `src/db.rs`, `src/overflow.rs`, etc.
  - Systematic replacement with error handling
  - Add #![warn(clippy::unwrap_used)] to src/lib.rs
  - Count: ~460 remaining unwraps

### 🟡 Memory Safety & Security

- [ ] **8. Fix Memory Safety Issues**
  - Add memory barriers for mmap resize operations
  - Fix race condition in Environment resize
  - Add bounds validation for all unsafe blocks
  - Validate page IDs before dereferencing
  - Fix use-after-free risks in page references
  - Files: `src/io.rs`, `src/env.rs`, `src/page.rs`

- [ ] **9. Add Input Validation**
  - Validate key/value sizes don't exceed limits
  - Check for integer overflow in size calculations
  - Add maximum database size enforcement
  - Validate page IDs are within bounds
  - Files: `src/env.rs`, `src/txn.rs`, `src/btree.rs`

- [ ] **10. Fix Security Vulnerabilities**
  - Set file permissions to 0600 on database creation
  - Add resource exhaustion protection
  - Fix integer overflow in page number calculations
  - Add stack depth limits for recursive operations
  - Files: `src/env.rs`, `src/btree.rs`

### 🟢 Recovery & Reliability Tools

- [ ] **11. Create Database Verification Tool**
  - New file: `src/bin/zerodb-check.rs`
  - Verify page checksums
  - Check B+tree structure integrity
  - Validate free list consistency
  - Report corruption details
  - Add --fix flag for basic repairs

- [ ] **12. Add Corruption Detection**
  - Add page header validation
  - Check B+tree invariants during operations
  - Detect partial writes
  - Add corruption error type
  - Files: `src/page.rs`, `src/btree.rs`, `src/error.rs`

- [ ] **13. Implement Basic Recovery**
  - Skip corrupted pages during startup
  - Rebuild free list if corrupted
  - Add recovery mode flag
  - Log recovery actions
  - File: `src/env.rs`

### 🔵 Production Features

- [ ] **14. Add Resource Limits**
  - Maximum database size configuration
  - Per-transaction memory limits
  - Configurable page allocation limits
  - Stack depth limits for recursion
  - File: `src/env.rs` (EnvConfig)

- [ ] **15. Fix Concurrency Issues**
  - Use SeqCst for critical atomic operations
  - Add generation counters to prevent ABA
  - Fix PID reuse in reader tracking
  - Proper memory barriers
  - Files: `src/env.rs`, `src/reader.rs`

- [ ] **16. Add Metrics and Monitoring**
  - Transaction counters
  - Page allocation statistics
  - Error counters by type
  - Performance histograms
  - New file: `src/metrics.rs`

- [ ] **17. Platform-Specific Fixes**
  - Fix endianness consistency
  - Runtime SIMD feature detection
  - Platform-specific file locking
  - Files: `src/checksum.rs`, `src/simd.rs`

### ⚪ Nice-to-Have Improvements

- [ ] **18. Add Diagnostic Tools**
  - Database statistics command
  - Page dump utility
  - Transaction history viewer
  - New file: `src/bin/zerodb-stat.rs`

- [ ] **19. Improve Error Messages**
  - Add more context to all errors
  - Include page IDs and operation details
  - Better corruption reporting
  - File: `src/error.rs`

- [ ] **20. Create Backup/Restore Tools**
  - Online backup capability
  - Point-in-time recovery
  - New file: `src/bin/zerodb-backup.rs`

## Implementation Order

### Phase 1: Stop Crashes (Implement First)
1. Enable checksums by default ✓
2. Add transaction page limits
3. Fix random write PAGE_FULL errors
4. Replace unwrap() in critical paths (txn.rs, btree.rs)

### Phase 2: Data Integrity
5. Complete unwrap() replacement
6. Add input validation
7. Fix memory safety issues
8. Create verification tool

### Phase 3: Production Hardening
9. Add recovery mechanisms
10. Fix concurrency issues
11. Add resource limits
12. Implement metrics

### Phase 4: Polish
13. Platform fixes
14. Diagnostic tools
15. Documentation

## Testing Strategy

Each fix should include:
- Unit tests for the specific issue
- Integration test demonstrating the fix
- Benchmark to ensure no performance regression

## Success Criteria

ZeroDB is production-ready when:
- [ ] Zero unwrap() calls in production code paths
- [ ] Checksums enabled and validated by default
- [ ] Random writes support 10K+ entries per transaction
- [ ] Database verification tool exists and passes
- [ ] Recovery from corruption is possible
- [ ] Resource limits prevent DoS
- [ ] All critical security issues resolved
- [ ] Metrics available for monitoring

## Quick Wins to Implement Now

1. **Enable checksums**: Change one line in env.rs
2. **Add page limits**: Already have MAX_TXN_PAGES, just make it configurable
3. **Add lint rule**: `#![warn(clippy::unwrap_used)]` to catch new unwraps
4. **Fix file permissions**: Add .mode(0o600) to file creation

## Notes

- Each task is designed to be completed with Claude Code
- Tasks are independent where possible to allow parallel work
- Critical fixes are prioritized to prevent data loss
- Performance benchmarks should be run after each major change
- The existing test suite should pass after each change
