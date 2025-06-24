//! Test the I/O backend implementation

use zerodb::error::PageId;
use zerodb::io::{IoBackend, MmapBackend};
use zerodb::page::{Page, PageFlags};

#[test]
fn test_basic_io_operations() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary file
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("test.db");

    // Create the backend
    let backend = MmapBackend::with_options(&path, 1024 * 1024)?;

    // Create and write some pages
    for i in 0..5 {
        let page = Page::new(PageId(i), PageFlags::LEAF);
        backend.write_page(&page)?;
    }

    // Sync to disk
    backend.sync()?;

    // Read the pages back
    for i in 0..5 {
        let page = backend.read_page(PageId(i))?;
        assert_eq!(page.header.pgno, i);
        assert_eq!(page.header.flags, PageFlags::LEAF);
    }

    Ok(())
}

#[test]
fn test_backend_growth() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary file
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("test.db");

    // Create the backend
    let backend = MmapBackend::with_options(&path, 1024 * 1024)?;

    // Test growing the file
    let initial_size = backend.size_in_pages();
    backend.grow(initial_size * 2)?;
    assert_eq!(backend.size_in_pages(), initial_size * 2);

    // Write a page in the new area
    let new_page_id = PageId(initial_size + 10);
    let page = Page::new(new_page_id, PageFlags::BRANCH);
    backend.write_page(&page)?;

    // Read it back
    let read_page = backend.read_page(new_page_id)?;
    assert_eq!(read_page.header.pgno, new_page_id.0);
    assert_eq!(read_page.header.flags, PageFlags::BRANCH);

    Ok(())
}