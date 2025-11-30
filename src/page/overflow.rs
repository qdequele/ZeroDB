//! Overflow page structure for large values.
//!
//! When a value is too large to fit inline in a leaf node, it is stored
//! in one or more overflow pages. The leaf node then contains a pointer
//! to the first overflow page.

use super::{PageHeader, PageNo};
use crate::error::{Error, Result};
use crate::PAGE_HEADER_SIZE;

/// Overflow page header extension size (pages count).
pub const OVERFLOW_HEADER_SIZE: usize = PAGE_HEADER_SIZE + 4;

/// Overflow page structure.
///
/// Overflow pages store large values that don't fit inline in leaf nodes.
/// Multiple consecutive overflow pages can be chained together for very
/// large values.
///
/// Layout:
/// ```text
/// Offset  Size  Field
/// 0       16    Page header (with OVERFLOW flag)
/// 16      4     pages (number of overflow pages including this one)
/// 20      N     data (value bytes)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverflowPage {
    /// Page header.
    pub header: PageHeader,
    /// Number of pages used by this overflow value (including this page).
    pub pages: u32,
    /// The actual data (may span multiple pages).
    pub data: Vec<u8>,
}

impl OverflowPage {
    /// Creates a new overflow page.
    ///
    /// # Arguments
    ///
    /// * `page_no` - The page number of this overflow page
    /// * `data` - The data to store
    /// * `page_size` - The size of each page
    pub fn new(page_no: PageNo, data: Vec<u8>, page_size: usize) -> Self {
        let usable_per_page = page_size - OVERFLOW_HEADER_SIZE;
        let pages = if data.is_empty() {
            1
        } else {
            ((data.len() + usable_per_page - 1) / usable_per_page) as u32
        };

        Self {
            header: PageHeader::new_overflow(page_no),
            pages,
            data,
        }
    }

    /// Calculates the number of pages needed to store the given data size.
    pub fn pages_needed(data_size: usize, page_size: usize) -> u32 {
        let usable_per_page = page_size - OVERFLOW_HEADER_SIZE;
        if data_size == 0 {
            1
        } else {
            ((data_size + usable_per_page - 1) / usable_per_page) as u32
        }
    }

    /// Returns the maximum data that can be stored in a single overflow page.
    pub fn max_data_per_page(page_size: usize) -> usize {
        page_size - OVERFLOW_HEADER_SIZE
    }

    /// Returns the total size in bytes that this overflow value occupies.
    pub fn total_size(&self, page_size: usize) -> usize {
        self.pages as usize * page_size
    }

    /// Reads an overflow page header from a byte slice.
    ///
    /// Note: This only reads the header and page count, not the data.
    /// The data must be read separately based on the page count.
    pub fn read_header_from(data: &[u8]) -> Result<(PageHeader, u32)> {
        if data.len() < OVERFLOW_HEADER_SIZE {
            return Err(Error::Corrupted);
        }

        let header = PageHeader::read_from(&data[0..PAGE_HEADER_SIZE])?;

        if !header.flags.is_overflow() {
            return Err(Error::Corrupted);
        }

        let pages = u32::from_le_bytes([
            data[PAGE_HEADER_SIZE],
            data[PAGE_HEADER_SIZE + 1],
            data[PAGE_HEADER_SIZE + 2],
            data[PAGE_HEADER_SIZE + 3],
        ]);

        Ok((header, pages))
    }

    /// Reads an overflow page from a byte slice.
    ///
    /// # Arguments
    ///
    /// * `data` - The byte slice containing all overflow pages
    /// * `data_size` - The actual size of the stored data
    pub fn read_from(data: &[u8], data_size: usize) -> Result<Self> {
        let (header, pages) = Self::read_header_from(data)?;

        if data.len() < OVERFLOW_HEADER_SIZE + data_size {
            return Err(Error::Corrupted);
        }

        let value_data = data[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + data_size].to_vec();

        Ok(Self {
            header,
            pages,
            data: value_data,
        })
    }

    /// Writes the overflow page header to a byte slice.
    ///
    /// Note: This only writes the header, not the data.
    pub fn write_header_to(&self, data: &mut [u8]) -> Result<()> {
        if data.len() < OVERFLOW_HEADER_SIZE {
            return Err(Error::Corrupted);
        }

        self.header.write_to(&mut data[0..PAGE_HEADER_SIZE])?;
        data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4]
            .copy_from_slice(&self.pages.to_le_bytes());

        Ok(())
    }

    /// Writes the overflow page to a byte slice.
    ///
    /// Writes header and all data. The data slice must be large enough
    /// to hold all overflow pages.
    pub fn write_to(&self, data: &mut [u8], page_size: usize) -> Result<()> {
        let total_size = self.total_size(page_size);
        if data.len() < total_size {
            return Err(Error::Corrupted);
        }

        self.write_header_to(data)?;

        // Write the data after the header
        let data_start = OVERFLOW_HEADER_SIZE;
        data[data_start..data_start + self.data.len()].copy_from_slice(&self.data);

        // Zero out any remaining space in the overflow pages
        for byte in &mut data[data_start + self.data.len()..total_size] {
            *byte = 0;
        }

        Ok(())
    }
}

/// Determines if a value should be stored as overflow based on key and value sizes.
///
/// LMDB uses approximately 1/4 of page size as the threshold.
pub fn should_use_overflow(key_size: usize, value_size: usize, page_size: usize) -> bool {
    // Node header (8 bytes) + key + value must fit in less than 1/4 of the page
    // to leave room for other entries and page header
    let node_size = 8 + key_size + value_size;
    let threshold = (page_size - PAGE_HEADER_SIZE) / 4;
    node_size > threshold
}

/// Calculates the threshold size for values that go to overflow.
pub fn overflow_threshold(page_size: usize) -> usize {
    (page_size - PAGE_HEADER_SIZE) / 4 - 8 // subtract node header
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overflow_page_roundtrip() {
        let page_size = 4096;
        let data = vec![0xAB; 1000];
        let page = OverflowPage::new(10, data.clone(), page_size);

        assert_eq!(page.pages, 1);

        let mut buf = vec![0u8; page.total_size(page_size)];
        page.write_to(&mut buf, page_size).unwrap();

        let recovered = OverflowPage::read_from(&buf, data.len()).unwrap();
        assert_eq!(page.pages, recovered.pages);
        assert_eq!(page.data, recovered.data);
    }

    #[test]
    fn overflow_multi_page() {
        let page_size = 4096;
        let usable = OverflowPage::max_data_per_page(page_size);

        // Data that requires exactly 2 pages
        let data = vec![0xCD; usable + 100];
        let page = OverflowPage::new(20, data.clone(), page_size);

        assert_eq!(page.pages, 2);
        assert_eq!(page.total_size(page_size), 2 * page_size);
    }

    #[test]
    fn pages_needed_calculation() {
        let page_size = 4096;
        let usable = OverflowPage::max_data_per_page(page_size);

        assert_eq!(OverflowPage::pages_needed(0, page_size), 1);
        assert_eq!(OverflowPage::pages_needed(100, page_size), 1);
        assert_eq!(OverflowPage::pages_needed(usable, page_size), 1);
        assert_eq!(OverflowPage::pages_needed(usable + 1, page_size), 2);
        assert_eq!(OverflowPage::pages_needed(usable * 2, page_size), 2);
        assert_eq!(OverflowPage::pages_needed(usable * 2 + 1, page_size), 3);
    }

    #[test]
    fn should_use_overflow_check() {
        let page_size = 4096;
        let threshold = overflow_threshold(page_size);

        // Small values should not use overflow
        assert!(!should_use_overflow(10, 100, page_size));

        // Large values should use overflow
        assert!(should_use_overflow(10, threshold + 100, page_size));
    }

    #[test]
    fn overflow_threshold_value() {
        // For 4096 byte pages:
        // threshold = (4096 - 16) / 4 - 8 = 1020 - 8 = 1012
        let threshold = overflow_threshold(4096);
        assert_eq!(threshold, 1012);
    }
}
