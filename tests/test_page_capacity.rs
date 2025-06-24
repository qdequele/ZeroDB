//! Test how many entries fit in a page

use zerodb::error::PageId;
use zerodb::page::{Page, PageFlags, PageHeader, PAGE_SIZE};

#[test]
fn test_page_capacity_basic() -> Result<(), Box<dyn std::error::Error>> {
    // Verify basic page properties
    assert_eq!(PAGE_SIZE, 4096);
    assert!(PageHeader::SIZE > 0);
    assert!(PAGE_SIZE > PageHeader::SIZE);

    // Create a test page
    let page = Page::new(PageId(1), PageFlags::LEAF);
    
    assert_eq!(page.header.lower, PageHeader::SIZE as u16);
    assert_eq!(page.header.upper, PAGE_SIZE as u16);
    assert_eq!(page.header.free_space() as usize, PAGE_SIZE - PageHeader::SIZE);

    Ok(())
}

#[test]
fn test_page_fill_small_entries() -> Result<(), Box<dyn std::error::Error>> {
    let mut page = Page::new(PageId(1), PageFlags::LEAF);

    // Try to add small entries until full
    let mut count = 0;
    for i in 0..1000 {
        let key = format!("k{:02}", i);
        let value = vec![i as u8; 8]; // Small 8 byte values

        match page.add_node_sorted(key.as_bytes(), &value) {
            Ok(_) => {
                count += 1;
            }
            Err(_) => {
                break;
            }
        }
    }

    // Should fit many small entries
    assert!(count > 50, "Should fit at least 50 small entries, got {}", count);
    
    // Page should be nearly full
    assert!(page.header.free_space() < 100, "Page should be nearly full");

    Ok(())
}

#[test]
fn test_page_fill_medium_entries() -> Result<(), Box<dyn std::error::Error>> {
    let mut page = Page::new(PageId(1), PageFlags::LEAF);

    // Try to add medium-sized entries until full
    let mut count = 0;
    for i in 0..1000 {
        let key = format!("key_{:03}", i);
        let value = vec![i as u8; 64]; // 64 byte values

        match page.add_node_sorted(key.as_bytes(), &value) {
            Ok(_) => {
                count += 1;
            }
            Err(_) => {
                break;
            }
        }
    }

    // Should fit a reasonable number of medium entries
    assert!(count > 20, "Should fit at least 20 medium entries, got {}", count);
    assert!(count < 100, "Should fit less than 100 medium entries, got {}", count);

    Ok(())
}

#[test]
fn test_page_fill_large_entries() -> Result<(), Box<dyn std::error::Error>> {
    let mut page = Page::new(PageId(1), PageFlags::LEAF);

    // Try to add large entries until full
    let mut count = 0;
    for i in 0..100 {
        let key = format!("bigkey_{:03}", i);
        let value = vec![i as u8; 256]; // 256 byte values

        match page.add_node_sorted(key.as_bytes(), &value) {
            Ok(_) => {
                count += 1;
            }
            Err(_) => {
                break;
            }
        }
    }

    // Should fit fewer large entries
    assert!(count >= 5, "Should fit at least 5 large entries, got {}", count);
    assert!(count < 20, "Should fit less than 20 large entries, got {}", count);

    Ok(())
}

#[test]
fn test_page_space_efficiency() -> Result<(), Box<dyn std::error::Error>> {
    let mut page = Page::new(PageId(1), PageFlags::LEAF);

    let mut count = 0;
    let entry_size = 50; // Fixed size for predictability
    
    for i in 0..1000 {
        let key = format!("k{:04}", i);
        let value = vec![0u8; entry_size];

        match page.add_node_sorted(key.as_bytes(), &value) {
            Ok(_) => {
                count += 1;
            }
            Err(_) => {
                break;
            }
        }
    }

    // Calculate space used
    let _ptr_space = count * 2; // Each pointer is 2 bytes
    let initial_free = PAGE_SIZE - PageHeader::SIZE;
    let remaining_free = page.header.free_space() as usize;
    let used_space = initial_free - remaining_free;

    // Efficiency should be high (most of the page should be used)
    let efficiency = (used_space as f64 / initial_free as f64) * 100.0;
    assert!(efficiency > 90.0, "Page efficiency should be >90%, got {:.1}%", efficiency);

    Ok(())
}

#[test]
fn test_page_exact_fit() -> Result<(), Box<dyn std::error::Error>> {
    let mut page = Page::new(PageId(1), PageFlags::LEAF);

    // Try to fill page with entries that should exactly fit
    // Each entry needs: 2 bytes pointer + key length + value length + node overhead
    let key = b"test";
    let value = vec![0u8; 100];
    
    let mut count = 0;
    let mut last_free_space = page.header.free_space();
    
    while count < 100 {
        match page.add_node_sorted(key, &value) {
            Ok(_) => {
                count += 1;
                let new_free_space = page.header.free_space();
                assert!(new_free_space < last_free_space, "Free space should decrease");
                last_free_space = new_free_space;
            }
            Err(_) => {
                break;
            }
        }
    }

    // Should have added some entries
    assert!(count > 0, "Should fit at least one entry");
    
    // Free space should be minimal
    assert!(page.header.free_space() < 200, "Should have minimal free space left");

    Ok(())
}