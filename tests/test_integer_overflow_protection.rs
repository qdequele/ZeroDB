//! Test integer overflow protection in page calculations

use zerodb::{EnvBuilder, Result, Error};
use zerodb::error::PageId;
use tempfile::TempDir;
use std::sync::Arc;

#[test]
fn test_page_id_multiplication_overflow() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(1 << 20) // 1 MB
            .open(dir.path())?
    );

    // Test with a page ID that would cause overflow when multiplied by PAGE_SIZE
    // u64::MAX / 4096 is approximately 4_503_599_627_370_495
    // Any page ID larger than this would overflow when multiplied by 4096
    let overflow_page_id = PageId(u64::MAX / 4096 + 1);
    
    // Try to read a page with an overflow-inducing ID
    let txn = env.read_txn()?;
    let read_result = txn.get_page(overflow_page_id);
    
    // Should get an InvalidPageId error, not panic or undefined behavior
    match read_result {
        Err(Error::InvalidPageId(_)) => {
            // Expected error
        }
        Ok(_) => panic!("Should have rejected overflow page ID"),
        Err(e) => panic!("Got unexpected error: {:?}", e),
    }

    Ok(())
}

#[test]
fn test_large_page_id_bounds_check() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(1 << 20) // 1 MB
            .open(dir.path())?
    );

    // Test with very large page IDs that are still within u64 range
    // but would produce offsets beyond any reasonable file size
    let large_page_ids = vec![
        PageId(1_000_000_000),      // 1 billion pages = ~4 TB
        PageId(1_000_000_000_000),  // 1 trillion pages = ~4 PB
        PageId(u64::MAX / 2),       // Half of u64::MAX
    ];

    for page_id in large_page_ids {
        let txn = env.read_txn()?;
        let read_result = txn.get_page(page_id);
        match read_result {
            Err(Error::InvalidPageId(_)) => {
                // Expected error
            }
            Ok(_) => panic!("Should have rejected large page ID: {:?}", page_id),
            Err(e) => panic!("Got unexpected error for page {:?}: {:?}", page_id, e),
        }
    }

    Ok(())
}

#[test]
fn test_page_offset_calculation_safety() -> Result<()> {
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(1 << 20) // 1 MB
            .open(dir.path())?
    );

    // Test edge cases around the overflow boundary
    let page_size = 4096u64;
    let max_safe_page_id = u64::MAX / page_size;
    
    let test_page_ids = vec![
        PageId(max_safe_page_id - 1),  // Just below overflow
        PageId(max_safe_page_id),      // Exactly at boundary
        PageId(max_safe_page_id + 1),  // Just above overflow
    ];

    for page_id in test_page_ids {
        let txn = env.read_txn()?;
        let read_result = txn.get_page(page_id);
        
        // All of these should fail with InvalidPageId since they're beyond the file size
        assert!(
            matches!(read_result, Err(Error::InvalidPageId(_))),
            "Expected InvalidPageId error for page {:?}", page_id
        );
    }

    Ok(())
}

// Note: We can't directly test prefetch_pages and grow() as they're internal methods.
// The overflow protection is still tested through the page ID validation tests above.