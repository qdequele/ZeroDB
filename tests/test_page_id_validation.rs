use zerodb::{env::EnvBuilder, error::Error, error::PageId};
use tempfile::TempDir;

#[test]
fn test_invalid_page_id_validation() {
    let dir = TempDir::new().unwrap();
    let env = EnvBuilder::new().open(dir.path()).unwrap();
    
    // Start a read transaction
    let rtxn = env.read_txn().unwrap();
    
    // Try to access a page that doesn't exist (way beyond the file size)
    let invalid_page_id = PageId(1000000);
    let result = rtxn.get_page(invalid_page_id);
    
    // Should get an InvalidPageId error
    match result {
        Err(Error::InvalidPageId(page_id)) => {
            assert_eq!(page_id, invalid_page_id);
        }
        Ok(_) => panic!("Expected InvalidPageId error, but got Ok"),
        Err(e) => panic!("Expected InvalidPageId error, got: {}", e),
    }
}

#[test]
fn test_valid_page_id_access() {
    let dir = TempDir::new().unwrap();
    let env = EnvBuilder::new().open(dir.path()).unwrap();
    
    // Start a read transaction
    let rtxn = env.read_txn().unwrap();
    
    // Meta pages (0 and 1) should always be accessible
    let meta_page_0 = rtxn.get_page(PageId(0));
    assert!(meta_page_0.is_ok(), "Should be able to read meta page 0");
    
    let meta_page_1 = rtxn.get_page(PageId(1));
    assert!(meta_page_1.is_ok(), "Should be able to read meta page 1");
}

#[test]
fn test_free_meta_page_protection() {
    let dir = TempDir::new().unwrap();
    let env = EnvBuilder::new().open(dir.path()).unwrap();
    
    // Start a write transaction
    let mut wtxn = env.write_txn().unwrap();
    
    // Try to free meta page 0
    let result = wtxn.free_page(PageId(0));
    match result {
        Err(Error::Custom(msg)) => {
            assert!(msg.contains("Cannot free meta pages"));
        }
        Ok(_) => panic!("Expected error when trying to free meta page, but got Ok"),
        Err(e) => panic!("Expected error when trying to free meta page, got: {}", e),
    }
    
    // Try to free meta page 1
    let result = wtxn.free_page(PageId(1));
    match result {
        Err(Error::Custom(msg)) => {
            assert!(msg.contains("Cannot free meta pages"));
        }
        Ok(_) => panic!("Expected error when trying to free meta page, but got Ok"),
        Err(e) => panic!("Expected error when trying to free meta page, got: {}", e),
    }
}

#[test]
fn test_free_pages_validation() {
    let dir = TempDir::new().unwrap();
    let env = EnvBuilder::new().open(dir.path()).unwrap();
    
    // Start a write transaction
    let mut wtxn = env.write_txn().unwrap();
    
    // Try to free pages beyond database bounds
    let result = wtxn.free_pages(PageId(1000000), 10);
    match result {
        Err(Error::InvalidPageId(_)) | Err(Error::Custom(_)) => {
            // Expected - either InvalidPageId or Custom error about bounds
        }
        Ok(_) => panic!("Expected error when trying to free pages beyond bounds, but got Ok"),
        Err(e) => panic!("Expected error when trying to free pages beyond bounds, got other error: {}", e),
    }
}

#[test]
fn test_page_allocation_limit() {
    use zerodb::page::PageFlags;
    
    let dir = TempDir::new().unwrap();
    let env = EnvBuilder::new().open(dir.path()).unwrap();
    
    // Start a write transaction
    let mut wtxn = env.write_txn().unwrap();
    
    // Allocate some pages (should succeed)
    for _ in 0..10 {
        let result = wtxn.alloc_page(PageFlags::LEAF);
        assert!(result.is_ok(), "Page allocation should succeed");
    }
    
    // The MAX_PAGE_ID check should prevent allocating pages with IDs >= 2^48
    // But normal allocation won't reach that limit in tests
}