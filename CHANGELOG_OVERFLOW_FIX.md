# Integer Overflow Protection Fixes

## Summary
Fixed multiple potential integer overflow vulnerabilities in page number calculations throughout the codebase. These vulnerabilities could have led to memory corruption, crashes, or undefined behavior when handling large page IDs or database sizes.

## Changes Made

### 1. Fixed page offset calculations in `io.rs`
- **Location**: `MmapBackend` methods
- **Issue**: Page IDs were multiplied by PAGE_SIZE without overflow checks
- **Fix**: Added `checked_mul()` for all page ID to offset conversions
- **Methods affected**:
  - `validate_page_id()`
  - `get_page_slice_mut()`
  - `read_page()`
  - `get_page_ref()`
  - `write_page()`
  - `grow()`
  - `prefetch_pages()`

### 2. Fixed file size calculations
- **Location**: `MmapBackend::with_options()`
- **Issue**: File size alignment calculation could overflow
- **Fix**: Added checked arithmetic for minimum size and page alignment

### 3. Fixed database size limit checks in `env.rs`
- **Location**: `EnvInner::check_database_size_limit()`
- **Issue**: Size calculations could overflow when checking limits
- **Fix**: Added checked arithmetic for all size calculations

### 4. Fixed test code
- **Location**: `tests/stress/test_overflow_limits.rs` and `tests/test_overflow_pages.rs`
- **Issue**: Test calculations could overflow
- **Fix**: Used `saturating_mul()` for test calculations

## Security Impact

These fixes prevent potential security vulnerabilities where:
1. An attacker could craft page IDs that cause integer overflow
2. The overflow could lead to out-of-bounds memory access
3. This could potentially be exploited for arbitrary code execution

## Testing

Added new test file `tests/test_integer_overflow_protection.rs` that specifically tests:
- Page IDs that would cause overflow when multiplied by PAGE_SIZE
- Very large page IDs that are within u64 range but beyond reasonable file sizes
- Proper error handling returns `Error::InvalidPageId` instead of panicking

## Recommendations

1. Consider adding debug assertions in critical arithmetic operations
2. Use saturating or checked arithmetic by default for all page-related calculations
3. Add fuzzing tests that specifically target arithmetic operations
4. Consider using a newtype wrapper for page offsets that enforces safe arithmetic

## Code Pattern to Avoid
```rust
// BAD - can overflow
let offset = page_id.0 as usize * PAGE_SIZE;

// GOOD - checked arithmetic
let offset = (page_id.0 as usize)
    .checked_mul(PAGE_SIZE)
    .ok_or(Error::InvalidPageId(page_id))?;
```