//! Overflow page management for large values
//!
//! When a value is too large to fit in a regular page, it's stored
//! in overflow pages. The node in the regular page contains a reference
//! to the first overflow page.

use crate::error::{Error, PageId, Result};
use crate::page::{PageFlags, PAGE_SIZE};
use crate::txn::{mode::Mode, Transaction};

/// Maximum value size that fits in a regular page
/// We need space for: NodeHeader (8 bytes) + key + value + page header + pointers
/// Using LMDB's approach: PAGE_SIZE / 2 for better utilization
pub const MAX_INLINE_VALUE_SIZE: usize = PAGE_SIZE / 2;

/// Maximum number of overflow pages allowed in a chain
/// No artificial limit - supports values up to available memory/disk
pub const MAX_OVERFLOW_PAGES: usize = usize::MAX;

/// Overflow page header
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct OverflowHeader {
    /// Next overflow page (0 if this is the last page)
    pub next_page: u64,
    /// Total size of the value (only set in first overflow page)
    pub total_size: u64,
}

impl OverflowHeader {
    /// Size of overflow header
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

/// Write a large value to overflow pages (LMDB-style with consecutive pages)
pub fn write_overflow_value_lmdb<'txn>(
    txn: &mut Transaction<'txn, crate::txn::Write>,
    value: &[u8],
) -> Result<(PageId, u32)> {
    // Calculate pages needed (raw data directly after page header)
    let data_per_page = PAGE_SIZE - crate::page::PageHeader::SIZE;
    let num_pages = value.len().div_ceil(data_per_page);
    
    if num_pages == 0 {
        return Err(Error::InvalidParameter("Empty value for overflow"));
    }
    
    // Allocate consecutive pages
    let first_page_id = txn.alloc_consecutive_pages(num_pages, PageFlags::OVERFLOW)?;
    
    // Write data directly to pages
    let mut offset = 0;
    for i in 0..num_pages {
        let page_id = PageId(first_page_id.0 + i as u64);
        let page = txn.get_consecutive_page_mut(page_id)?;
        
        // First page stores total overflow count
        if i == 0 {
            page.header.overflow = num_pages as u32;
        } else {
            // Continuation pages have overflow count of 1 (LMDB compatibility)
            page.header.overflow = 1;
        }
        
        // Calculate how much data goes in this page
        let remaining = value.len() - offset;
        let chunk_size = remaining.min(data_per_page);
        
        // Copy raw data directly (no header)
        page.data[..chunk_size].copy_from_slice(&value[offset..offset + chunk_size]);
        
        offset += chunk_size;
    }
    
    Ok((first_page_id, num_pages as u32))
}

/// Read a large value from overflow pages (LMDB-style)
pub fn read_overflow_value_lmdb<'txn, M: Mode>(
    txn: &'txn Transaction<'txn, M>,
    first_page_id: PageId,
    overflow_count: Option<u32>,
    value_size: Option<usize>,
) -> Result<Vec<u8>> {
    let first_page = txn.get_page(first_page_id)?;
    
    // Check it's an overflow page
    if !first_page.header.flags.contains(PageFlags::OVERFLOW) {
        return Err(Error::Corruption {
            details: "Expected overflow page".into(),
            page_id: Some(first_page_id),
        });
    }
    
    // Get overflow count from first page if not provided
    let num_pages = overflow_count.unwrap_or(first_page.header.overflow) as usize;
    if num_pages == 0 {
        return Err(Error::Corruption {
            details: "Invalid overflow count".into(),
            page_id: Some(first_page_id),
        });
    }
    
    let data_per_page = PAGE_SIZE - crate::page::PageHeader::SIZE;
    let mut result = if let Some(size) = value_size {
        Vec::with_capacity(size)
    } else {
        Vec::new()
    };
    
    // Read data from consecutive pages
    for i in 0..num_pages {
        let page_id = PageId(first_page_id.0 + i as u64);
        let page = txn.get_page(page_id)?;
        
        if !page.header.flags.contains(PageFlags::OVERFLOW) {
            return Err(Error::Corruption {
                details: format!("Expected overflow page at index {}", i),
                page_id: Some(page_id),
            });
        }
        
        // For the last page, we need to figure out actual data size
        let data_len = if i == num_pages - 1 && value_size.is_some() {
            // Last page might be partial - calculate remaining bytes
            let total_read = i * data_per_page;
            let remaining = value_size.expect("value_size should be Some in last page case") - total_read;
            remaining.min(data_per_page)
        } else {
            data_per_page
        };
        
        result.extend_from_slice(&page.data[..data_len]);
    }
    
    Ok(result)
}

/// Free overflow pages (LMDB-style with consecutive pages)
pub fn free_overflow_chain_lmdb(
    txn: &mut Transaction<'_, crate::txn::Write>,
    first_page_id: PageId,
    overflow_count: u32,
) -> Result<()> {
    if overflow_count == 0 {
        return Err(Error::InvalidParameter("Invalid overflow count"));
    }
    
    // Free all consecutive pages
    txn.free_pages(first_page_id, overflow_count as usize)
}

/// Copy overflow pages for Copy-on-Write (LMDB-style)
pub fn copy_overflow_chain_lmdb(
    txn: &mut Transaction<'_, crate::txn::Write>,
    old_first_page_id: PageId,
    overflow_count: u32,
) -> Result<PageId> {
    if overflow_count == 0 {
        return Err(Error::InvalidParameter("Invalid overflow count"));
    }
    
    // Allocate new consecutive pages
    let new_first_page_id = txn.alloc_consecutive_pages(overflow_count as usize, PageFlags::OVERFLOW)?;
    
    // Copy data from old pages to new pages
    let data_per_page = PAGE_SIZE - crate::page::PageHeader::SIZE;
    
    for i in 0..overflow_count {
        let old_page_id = PageId(old_first_page_id.0 + i as u64);
        let new_page_id = PageId(new_first_page_id.0 + i as u64);
        
        // Read old page and copy needed data
        let (old_flags, old_overflow, old_data) = {
            let old_page = txn.get_page(old_page_id)?;
            let mut data_copy = vec![0u8; data_per_page];
            data_copy.copy_from_slice(&old_page.data[..data_per_page]);
            (old_page.header.flags, old_page.header.overflow, data_copy)
        };
        
        // Get new page and copy data
        let new_page = txn.get_consecutive_page_mut(new_page_id)?;
        new_page.header.flags = old_flags;
        new_page.header.overflow = old_overflow;
        
        // Copy raw data
        new_page.data[..data_per_page].copy_from_slice(&old_data);
    }
    
    Ok(new_first_page_id)
}

// Keep old functions for backward compatibility but mark as deprecated

#[deprecated(note = "Use write_overflow_value_lmdb for LMDB-style consecutive allocation")]
/// Write a large value to overflow pages
pub fn write_overflow_value<'txn>(
    txn: &mut Transaction<'txn, crate::txn::Write>,
    value: &[u8],
) -> Result<PageId> {
    let total_size = value.len();
    // Account for both PageHeader and OverflowHeader
    let data_per_page = PAGE_SIZE
        .checked_sub(crate::page::PageHeader::SIZE)
        .and_then(|s| s.checked_sub(OverflowHeader::SIZE))
        .ok_or_else(|| Error::Custom("Overflow page size calculation overflow".into()))?;
    let num_pages = total_size.div_ceil(data_per_page);

    if num_pages == 0 {
        return Err(Error::InvalidParameter("Empty value for overflow"));
    }

    // No longer enforcing artificial limits - support up to available memory/disk

    let mut first_page_id = None;
    let mut prev_page_id = None;
    let mut offset = 0;

    // Allocate and write overflow pages
    for i in 0..num_pages {
        let (page_id, page) = txn.alloc_page(PageFlags::OVERFLOW)?;

        if i == 0 {
            first_page_id = Some(page_id);
            // Set overflow count in the first page
            page.header.overflow = num_pages as u32;
        } else {
            // Continuation pages have overflow count of 1
            page.header.overflow = 1;
        }

        // Calculate how much data goes in this page
        let chunk_size = std::cmp::min(data_per_page, total_size.saturating_sub(offset));
        let _is_last = i == num_pages - 1;

        // Write overflow header
        let header = OverflowHeader {
            next_page: 0, // Will be updated when we allocate next page
            total_size: if i == 0 { total_size as u64 } else { 0 },
        };

        unsafe {
            let header_ptr = page.data.as_mut_ptr() as *mut OverflowHeader;
            *header_ptr = header;

            // Write data after header
            let data_ptr = page.data.as_mut_ptr().add(OverflowHeader::SIZE);
            std::ptr::copy_nonoverlapping(value.as_ptr().add(offset), data_ptr, chunk_size);
        }

        // Update previous page's next pointer
        if let Some(prev_id) = prev_page_id {
            let prev_page = txn.get_page_mut(prev_id)?;
            unsafe {
                let header_ptr = prev_page.data.as_mut_ptr() as *mut OverflowHeader;
                (*header_ptr).next_page = page_id.0;
            }
        }

        prev_page_id = Some(page_id);
        offset = offset.saturating_add(chunk_size);
    }

    first_page_id.ok_or_else(|| Error::Custom("Failed to allocate first overflow page".into()))
}

/// Read a large value from overflow pages
pub fn read_overflow_value<'txn, M: Mode>(
    txn: &'txn Transaction<'txn, M>,
    first_page_id: PageId,
) -> Result<Vec<u8>> {
    let first_page = txn.get_page(first_page_id)?;

    // Check it's an overflow page
    if !first_page.header.flags.contains(PageFlags::OVERFLOW) {
        return Err(Error::Corruption {
            details: "Expected overflow page".into(),
            page_id: Some(first_page_id),
        });
    }

    // Read header to get total size
    let header = unsafe { *(first_page.data.as_ptr() as *const OverflowHeader) };

    let total_size = header.total_size as usize;
    // Protect against invalid/corrupt total_size
    if total_size == 0 || total_size > 1_000_000_000 { // 1GB sanity check
        return Err(Error::Corruption {
            details: format!("Invalid overflow value size: {}", total_size),
            page_id: Some(first_page_id),
        });
    }
    let mut result = Vec::with_capacity(total_size);
    // Account for both PageHeader and OverflowHeader
    let data_per_page = PAGE_SIZE
        .checked_sub(crate::page::PageHeader::SIZE)
        .and_then(|s| s.checked_sub(OverflowHeader::SIZE))
        .ok_or_else(|| Error::Custom("Overflow page size calculation overflow".into()))?;

    let mut current_page_id = first_page_id;
    let mut bytes_read = 0;
    let mut _pages_read = 0;

    loop {
        // Track pages read
        _pages_read += 1;
        let page = txn.get_page(current_page_id)?;
        let header = unsafe { *(page.data.as_ptr() as *const OverflowHeader) };

        // Calculate how much data is in this page
        let chunk_size = std::cmp::min(data_per_page, total_size.saturating_sub(bytes_read));

        // Read data
        unsafe {
            let data_ptr = page.data.as_ptr().add(OverflowHeader::SIZE);
            result.extend_from_slice(std::slice::from_raw_parts(data_ptr, chunk_size));
        }

        bytes_read = bytes_read.saturating_add(chunk_size);

        // Move to next page
        if header.next_page == 0 {
            break;
        }
        current_page_id = PageId(header.next_page);
    }

    if bytes_read != total_size {
        return Err(Error::Corruption {
            details: format!(
                "Overflow value size mismatch: expected {}, got {}",
                total_size, bytes_read
            ),
            page_id: Some(first_page_id),
        });
    }

    Ok(result)
}

/// Check if a value should be stored in overflow pages
pub fn needs_overflow(key_size: usize, value_size: usize) -> bool {
    // LMDB-style check: if key + value + headers would take more than 1/2 of page
    match key_size.checked_add(value_size).and_then(|s| s.checked_add(32)) {
        Some(total) => total > MAX_INLINE_VALUE_SIZE,
        None => true, // Overflow in calculation means it definitely needs overflow pages
    }
}

/// Free all overflow pages in a chain
pub fn free_overflow_chain(
    txn: &mut Transaction<'_, crate::txn::Write>,
    first_page_id: PageId,
) -> Result<()> {
    let mut current_page_id = first_page_id;
    let mut _pages_freed = 0;

    loop {
        // Track pages freed
        _pages_freed += 1;
        let page = txn.get_page(current_page_id)?;

        // Check it's an overflow page
        if !page.header.flags.contains(PageFlags::OVERFLOW) {
            return Err(Error::Corruption {
                details: "Expected overflow page in chain".into(),
                page_id: Some(current_page_id),
            });
        }

        // Get next page before freeing this one
        let header = unsafe { *(page.data.as_ptr() as *const OverflowHeader) };
        let next_page = header.next_page;

        // Free current page
        txn.free_page(current_page_id)?;

        // Move to next page
        if next_page == 0 {
            break;
        }
        current_page_id = PageId(next_page);
    }

    Ok(())
}

/// Copy overflow pages for Copy-on-Write (optimized)
pub fn copy_overflow_chain(
    txn: &mut Transaction<'_, crate::txn::Write>,
    old_first_page_id: PageId,
) -> Result<PageId> {
    // Direct page-by-page copy to avoid allocating the entire value
    let mut old_page_id = old_first_page_id;
    let mut new_first_page_id = None;
    let mut prev_new_page_id = None;
    let mut _pages_copied = 0;

    loop {
        // Track pages copied
        _pages_copied += 1;
        // Get the old page and copy necessary data
        let (
            old_flags,
            old_num_keys,
            old_lower,
            old_upper,
            old_overflow,
            old_data,
            next_old_page_id,
        ) = {
            let old_page = txn.get_page(old_page_id)?;
            let next_id = unsafe {
                let next_ptr = old_page.data.as_ptr() as *const u64;
                PageId(*next_ptr)
            };
            (
                old_page.header.flags,
                old_page.header.num_keys,
                old_page.header.lower,
                old_page.header.upper,
                old_page.header.overflow,
                old_page.data,
                next_id,
            )
        };

        // Allocate new page
        let (new_page_id, new_page) = txn.alloc_page(PageFlags::OVERFLOW)?;

        // Copy page header and data directly
        new_page.header.flags = old_flags;
        new_page.header.num_keys = old_num_keys;
        new_page.header.lower = old_lower;
        new_page.header.upper = old_upper;
        new_page.header.overflow = old_overflow;
        new_page.data = old_data;

        // Track first new page
        if new_first_page_id.is_none() {
            new_first_page_id = Some(new_page_id);
        }

        // Update previous page's next pointer
        if let Some(prev_id) = prev_new_page_id {
            let prev_page = txn.get_page_mut(prev_id)?;
            // Set next page ID in the overflow field
            unsafe {
                let next_ptr = prev_page.data.as_mut_ptr() as *mut u64;
                *next_ptr = new_page_id.0;
            }
        }

        // Check if this is the last page
        if next_old_page_id.0 == 0 {
            // This was the last page
            break;
        }

        // Move to next page
        old_page_id = next_old_page_id;
        prev_new_page_id = Some(new_page_id);
    }

    new_first_page_id.ok_or_else(|| Error::Custom("Failed to allocate first overflow page during COW".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::EnvBuilder;
    use tempfile::TempDir;

    #[test]
    fn test_overflow_write_read() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path()).unwrap();

        // Create a large value that requires multiple overflow pages
        let large_value = vec![0xAB; 10 * 1024]; // 10KB

        let mut txn = env.write_txn().unwrap();

        // Write to overflow pages
        let (overflow_id, _) = write_overflow_value_lmdb(&mut txn, &large_value).unwrap();

        // Read back
        let read_value = read_overflow_value_lmdb(&txn, overflow_id, None, Some(large_value.len())).unwrap();
        assert_eq!(read_value, large_value);

        txn.commit().unwrap();

        // Read in new transaction
        let txn = env.read_txn().unwrap();
        let read_value = read_overflow_value_lmdb(&txn, overflow_id, None, Some(large_value.len())).unwrap();
        assert_eq!(read_value, large_value);
    }

    #[test]
    fn test_needs_overflow() {
        assert!(!needs_overflow(10, 100));
        assert!(!needs_overflow(100, 500));
        assert!(needs_overflow(100, 2000));
        assert!(!needs_overflow(500, 1500)); // 2032 bytes is NOT > 2048
    }
}
