//! Consecutive overflow page management
//!
//! This module provides efficient storage for large values using consecutive
//! page allocation, eliminating the complexity of chained page references.

use crate::error::{Error, PageId, Result};
use crate::page::{PageFlags, PAGE_SIZE};
use crate::txn::{mode::Mode, Transaction};

/// Data size per overflow page (page size minus header)
const DATA_PER_PAGE: usize = PAGE_SIZE - crate::page::PageHeader::SIZE;

/// Overflow page management
pub struct OverflowManager;

impl OverflowManager {
    /// Store a large value in consecutive overflow pages
    pub fn store_value<'txn>(
        txn: &mut Transaction<'txn, crate::txn::Write>,
        value: &[u8],
    ) -> Result<(PageId, u32)> {
        if value.is_empty() {
            return Err(Error::InvalidParameter("Cannot store empty value in overflow"));
        }

        // Calculate pages needed
        let pages_needed = value.len().div_ceil(DATA_PER_PAGE);
        
        // Allocate consecutive pages
        let first_page_id = txn.alloc_consecutive_pages(pages_needed, PageFlags::OVERFLOW)?;
        
        // Store data directly in pages
        for (i, chunk) in value.chunks(DATA_PER_PAGE).enumerate() {
            let page_id = PageId(first_page_id.0 + i as u64);
            let page = txn.get_consecutive_page_mut(page_id)?;
            
            // Set overflow count in first page, 1 in continuation pages
            page.header.overflow = if i == 0 { pages_needed as u32 } else { 1 };
            
            // Store raw data
            page.data[..chunk.len()].copy_from_slice(chunk);
            
            // Zero remaining bytes in last page for clean reads
            if chunk.len() < DATA_PER_PAGE {
                page.data[chunk.len()..DATA_PER_PAGE].fill(0);
            }
        }
        
        Ok((first_page_id, pages_needed as u32))
    }

    /// Load a value from consecutive overflow pages
    pub fn load_value<'txn, M: Mode>(
        txn: &'txn Transaction<'txn, M>,
        first_page_id: PageId,
        page_count: u32,
        value_size: Option<usize>,
    ) -> Result<Vec<u8>> {
        if page_count == 0 {
            return Err(Error::InvalidParameter("Invalid page count for overflow"));
        }

        // Pre-allocate result vector
        let mut result = if let Some(size) = value_size {
            Vec::with_capacity(size)
        } else {
            Vec::with_capacity(page_count as usize * DATA_PER_PAGE)
        };
        
        // Read data from consecutive pages
        for i in 0..page_count {
            let page_id = PageId(first_page_id.0 + i as u64);
            let page = txn.get_page(page_id)?;
            
            // Verify this is an overflow page
            if !page.header.flags.contains(PageFlags::OVERFLOW) {
                return Err(Error::Corruption {
                    details: format!("Expected overflow page at {}", page_id.0),
                    page_id: Some(page_id),
                });
            }
            
            // Calculate data length for this page
            let data_len = if let Some(total_size) = value_size {
                if i == page_count - 1 {
                    // Last page - calculate remaining bytes
                    let bytes_read = i as usize * DATA_PER_PAGE;
                    total_size.saturating_sub(bytes_read).min(DATA_PER_PAGE)
                } else {
                    DATA_PER_PAGE
                }
            } else {
                DATA_PER_PAGE
            };
            
            result.extend_from_slice(&page.data[..data_len]);
        }
        
        Ok(result)
    }

    /// Free consecutive overflow pages
    pub fn free_pages(
        txn: &mut Transaction<'_, crate::txn::Write>,
        first_page_id: PageId,
        page_count: u32,
    ) -> Result<()> {
        if page_count == 0 {
            return Ok(());
        }
        
        txn.free_pages(first_page_id, page_count as usize)
    }

    /// Copy overflow pages for Copy-on-Write
    pub fn copy_pages(
        txn: &mut Transaction<'_, crate::txn::Write>,
        old_first_page: PageId,
        page_count: u32,
    ) -> Result<PageId> {
        if page_count == 0 {
            return Err(Error::InvalidParameter("Invalid page count for overflow copy"));
        }
        
        // Allocate new consecutive pages
        let new_first_page = txn.alloc_consecutive_pages(page_count as usize, PageFlags::OVERFLOW)?;
        
        // Copy data from old pages to new pages
        for i in 0..page_count {
            let old_page_id = PageId(old_first_page.0 + i as u64);
            let new_page_id = PageId(new_first_page.0 + i as u64);
            
            // Read old page data
            let old_page = txn.get_page(old_page_id)?;
            let data_copy = old_page.data;
            let old_overflow = old_page.header.overflow;
            
            // Get new page and copy
            let new_page = txn.get_consecutive_page_mut(new_page_id)?;
            new_page.header.overflow = old_overflow;
            new_page.data.copy_from_slice(&data_copy);
        }
        
        Ok(new_first_page)
    }

    /// Check if a value needs overflow storage
    pub fn needs_overflow(key_size: usize, value_size: usize) -> bool {
        // Conservative check: if key + value + node overhead > half page
        let total_size = key_size + value_size + 32; // 32 bytes for various headers
        total_size > PAGE_SIZE / 2
    }

    /// Calculate the number of pages needed for a value
    pub fn pages_needed(value_size: usize) -> u32 {
        if value_size == 0 {
            return 0;
        }
        value_size.div_ceil(DATA_PER_PAGE) as u32
    }

    /// Get the maximum value size that can be stored
    pub fn max_value_size() -> usize {
        // Theoretical maximum: u32::MAX pages
        (u32::MAX as usize) * DATA_PER_PAGE
    }
}

/// Overflow page reference stored in nodes
#[derive(Debug, Clone, Copy)]
pub struct OverflowRef {
    /// First page of the overflow chain
    pub first_page: PageId,
    /// Number of consecutive pages
    pub page_count: u32,
}

impl OverflowRef {
    /// Create a new overflow reference
    pub fn new(first_page: PageId, page_count: u32) -> Self {
        Self { first_page, page_count }
    }

    /// Pack into bytes for storage in nodes
    pub fn pack(&self) -> [u8; 12] {
        let mut bytes = [0u8; 12];
        bytes[0..8].copy_from_slice(&self.first_page.0.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.page_count.to_le_bytes());
        bytes
    }

    /// Unpack from bytes stored in nodes
    pub fn unpack(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 12 {
            return Err(Error::Corruption {
                details: "Invalid overflow reference size".to_string(),
                page_id: None,
            });
        }

        let first_page = PageId(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
        ]));
        
        let page_count = u32::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11],
        ]);

        Ok(Self { first_page, page_count })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::EnvBuilder;
    use tempfile::TempDir;

    #[test]
    fn test_overflow_storage() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new().open(dir.path()).unwrap();

        // Test value that requires multiple pages
        let large_value = vec![0xAB; 10 * 1024]; // 10KB

        let mut txn = env.write_txn().unwrap();

        // Store in overflow pages
        let (first_page, page_count) = OverflowManager::store_value(&mut txn, &large_value).unwrap();
        
        // Load it back
        let loaded_value = OverflowManager::load_value(&txn, first_page, page_count, Some(large_value.len())).unwrap();
        
        assert_eq!(loaded_value, large_value);
        
        txn.commit().unwrap();
    }

    #[test]
    fn test_overflow_ref_serialization() {
        let overflow_ref = OverflowRef::new(PageId(12345), 42);
        let packed = overflow_ref.pack();
        let unpacked = OverflowRef::unpack(&packed).unwrap();
        
        assert_eq!(overflow_ref.first_page, unpacked.first_page);
        assert_eq!(overflow_ref.page_count, unpacked.page_count);
    }

    #[test]
    fn test_needs_overflow() {
        // Small values should not need overflow
        assert!(!OverflowManager::needs_overflow(100, 500));
        
        // Large values should need overflow
        assert!(OverflowManager::needs_overflow(100, 3000));
    }
} 