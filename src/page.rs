//! Page management and structures
//!
//! Pages are the fundamental unit of storage in the database.
//! Each page is aligned to the page size (typically 4KB) and contains
//! a header followed by data.

use crate::comparator::{Comparator, LexicographicComparator};
use crate::error::{Error, PageId, PageType, Result};
use crate::page_capacity::PageCapacityConfig;
use bitflags::bitflags;
use static_assertions::const_assert;
use std::borrow::Cow;
use std::mem::{size_of, MaybeUninit};
use std::ptr;
use std::slice;

/// Type alias for split result: (right_nodes, split_key)
type LeafSplitResult = (Vec<(Vec<u8>, Vec<u8>)>, Vec<u8>);

/// Default page size constant
pub const PAGE_SIZE: usize = 4096;

/// Maximum value size that can be stored inline (not in overflow pages)
/// This is roughly 1/4 of a page to allow for reasonable node density
pub const MAX_VALUE_SIZE: usize = PAGE_SIZE / 4;

const_assert!(PAGE_SIZE >= 512);
const_assert!(PAGE_SIZE.is_power_of_two());

bitflags! {
    /// Flags for page types and states
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PageFlags: u16 {
        /// Branch page (internal B+tree node)
        const BRANCH = 0x01;
        /// Leaf page (contains actual data)
        const LEAF = 0x02;
        /// Overflow page (for large values)
        const OVERFLOW = 0x04;
        /// Meta page (database metadata)
        const META = 0x08;
        /// Page is dirty (modified in current transaction)
        const DIRTY = 0x10;
        /// Page has duplicates
        const DUPFIXED = 0x20;
        /// Subtree root page
        const SUBP = 0x40;
        /// Fake leaf page for append mode
        const LOOSE = 0x80;
        /// Persistent flags mask
        const PERSISTENT = Self::BRANCH.bits() | Self::LEAF.bits() |
                          Self::OVERFLOW.bits() | Self::META.bits();
    }
}

/// Page header structure
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    /// Page number
    pub pgno: u64,
    /// Page flags
    pub flags: PageFlags,
    /// Number of items on page
    pub num_keys: u16,
    /// Lower bound of free space
    pub lower: u16,
    /// Upper bound of free space  
    pub upper: u16,
    /// Overflow page count (or parent page for branch)
    pub overflow: u32,
    /// Page checksum (CRC32) - 0 means no checksum
    pub checksum: u32,
    /// Next leaf page (for leaf chaining) - 0 means no next page
    pub next_pgno: u64,
    /// Previous leaf page (for leaf chaining) - 0 means no prev page
    pub prev_pgno: u64,
}

impl PageHeader {
    /// Size of the page header
    pub const SIZE: usize = size_of::<Self>();

    /// Create a new page header
    pub fn new(pgno: u64, flags: PageFlags) -> Self {
        Self {
            pgno,
            flags,
            num_keys: 0,
            lower: Self::SIZE as u16,
            upper: PAGE_SIZE as u16,
            overflow: 0,
            checksum: 0,
            next_pgno: 0,
            prev_pgno: 0,
        }
    }

    /// Get the page type
    pub fn page_type(&self) -> PageType {
        if self.flags.contains(PageFlags::BRANCH) {
            PageType::Branch
        } else if self.flags.contains(PageFlags::LEAF) {
            PageType::Leaf
        } else if self.flags.contains(PageFlags::OVERFLOW) {
            PageType::Overflow
        } else if self.flags.contains(PageFlags::META) {
            PageType::Meta
        } else {
            PageType::Free
        }
    }

    /// Get available space on page
    pub fn free_space(&self) -> usize {
        (self.upper - self.lower) as usize
    }
}

/// A page in the database
#[repr(C, align(4096))]
pub struct Page {
    /// Page header
    pub header: PageHeader,
    /// Page data
    pub data: [u8; PAGE_SIZE - PageHeader::SIZE],
}

// Ensure Page is exactly PAGE_SIZE
const_assert!(size_of::<Page>() == PAGE_SIZE);
const_assert!(std::mem::align_of::<Page>() == PAGE_SIZE);

impl Page {
    /// Create a new empty page
    pub fn new(pgno: PageId, flags: PageFlags) -> Box<Self> {
        let mut page = Box::new(MaybeUninit::<Page>::uninit());
        unsafe {
            let page_ptr = page.as_mut_ptr();

            // Initialize header
            (*page_ptr).header = PageHeader::new(pgno.0, flags);

            // Zero out data section
            ptr::write_bytes((*page_ptr).data.as_mut_ptr(), 0, PAGE_SIZE - PageHeader::SIZE);

            Box::from_raw(Box::into_raw(page).cast::<Page>())
        }
    }

    /// Validate page header for corruption detection
    pub fn validate_header(&self) -> Result<()> {
        // Check that page flags contain exactly one page type
        let type_flags = self.header.flags & PageFlags::PERSISTENT;
        let type_count = type_flags.bits().count_ones();
        if type_count != 1 {
            return Err(Error::Corruption {
                details: format!("Invalid page flags: multiple or no page types set (0x{:04x})", self.header.flags.bits()),
                page_id: Some(PageId(self.header.pgno)),
            });
        }

        // Validate lower/upper bounds
        if self.header.lower < PageHeader::SIZE as u16 {
            return Err(Error::Corruption {
                details: format!("Lower bound {} is less than header size {}", self.header.lower, PageHeader::SIZE),
                page_id: Some(PageId(self.header.pgno)),
            });
        }

        if self.header.upper > PAGE_SIZE as u16 {
            return Err(Error::Corruption {
                details: format!("Upper bound {} exceeds page size {}", self.header.upper, PAGE_SIZE),
                page_id: Some(PageId(self.header.pgno)),
            });
        }

        if self.header.lower > self.header.upper {
            return Err(Error::Corruption {
                details: format!("Lower bound {} exceeds upper bound {}", self.header.lower, self.header.upper),
                page_id: Some(PageId(self.header.pgno)),
            });
        }

        // For branch/leaf pages, validate num_keys against available space
        if self.header.flags.contains(PageFlags::BRANCH) || self.header.flags.contains(PageFlags::LEAF) {
            let offset = if self.header.flags.contains(PageFlags::BRANCH) {
                crate::branch::BranchHeader::SIZE
            } else {
                0
            };

            let min_required_space = offset + (self.header.num_keys as usize * 2);
            let available_space = (self.header.lower as usize).saturating_sub(PageHeader::SIZE);
            
            if min_required_space > available_space {
                return Err(Error::Corruption {
                    details: format!("Not enough space for {} key pointers (need {} bytes, have {})", 
                                   self.header.num_keys, min_required_space, available_space),
                    page_id: Some(PageId(self.header.pgno)),
                });
            }
        }

        // Validate overflow count for overflow pages
        if self.header.flags.contains(PageFlags::OVERFLOW) && self.header.overflow == 0 {
            return Err(Error::Corruption {
                details: "Overflow page has zero overflow count".to_string(),
                page_id: Some(PageId(self.header.pgno)),
            });
        }

        Ok(())
    }

    /// Detect if page has partial write corruption
    /// This checks for zeroed areas that indicate incomplete disk writes
    pub fn detect_partial_write(&self) -> Result<()> {
        // For leaf/branch pages with keys, check that pointer area is not zeroed
        if (self.header.flags.contains(PageFlags::BRANCH) || self.header.flags.contains(PageFlags::LEAF)) 
            && self.header.num_keys > 0 {
            
            let ptrs = self.ptrs();
            if ptrs.is_empty() {
                return Err(Error::Corruption {
                    details: "Pointer array is empty despite num_keys > 0".to_string(),
                    page_id: Some(PageId(self.header.pgno)),
                });
            }

            // Check if all pointers are zero (indicates partial write)
            let all_zero = ptrs.iter().all(|&p| p == 0);
            if all_zero {
                return Err(Error::Corruption {
                    details: "All key pointers are zero - possible partial write".to_string(),
                    page_id: Some(PageId(self.header.pgno)),
                });
            }

            // Check if pointers are within valid range
            for (i, &ptr) in ptrs.iter().enumerate() {
                if ptr != 0 && (ptr < self.header.upper || ptr >= PAGE_SIZE as u16) {
                    return Err(Error::Corruption {
                        details: format!("Key pointer {} at index {} is out of valid range [{}, {})", 
                                       ptr, i, self.header.upper, PAGE_SIZE),
                        page_id: Some(PageId(self.header.pgno)),
                    });
                }
            }
        }

        // For overflow pages, check that data isn't completely zeroed
        if self.header.flags.contains(PageFlags::OVERFLOW) {
            // Skip the overflow header (16 bytes for next_page + total_size)
            let data_start = crate::overflow::OverflowHeader::SIZE;
            let data_end = PAGE_SIZE - PageHeader::SIZE;
            
            if data_end > data_start {
                // Only check a reasonable portion to avoid false positives
                // Check first 256 bytes of actual data
                let check_len = std::cmp::min(256, data_end - data_start);
                let all_zero = self.data[data_start..data_start + check_len].iter().all(|&b| b == 0);
                
                // Only flag as corruption if we have a reasonable amount of data that's all zero
                // and the overflow count indicates this should have data
                if all_zero && self.header.overflow > 0 && check_len >= 64 {
                    return Err(Error::Corruption {
                        details: "Overflow page data is completely zeroed - possible partial write".to_string(),
                        page_id: Some(PageId(self.header.pgno)),
                    });
                }
            }
        }

        Ok(())
    }

    /// Comprehensive page validation combining header, checksum, and partial write detection
    pub fn validate(&self) -> Result<()> {
        // First validate the header structure
        self.validate_header()?;
        
        // Then check for partial writes
        self.detect_partial_write()?;
        
        // Finally validate checksum if present
        use crate::checksum::ChecksummedPage;
        self.validate_checksum()?;
        
        Ok(())
    }

    /// Create a page from a MetaPage
    pub fn from_meta(meta: &crate::meta::MetaPage, page_id: PageId) -> Box<Self> {
        let mut page = Self::new(page_id, PageFlags::META);

        // Copy the meta page data into the page data area (after the header)
        unsafe {
            let meta_ptr = meta as *const crate::meta::MetaPage as *const u8;
            let dst_ptr = page.data.as_mut_ptr();
            ptr::copy_nonoverlapping(meta_ptr, dst_ptr, size_of::<crate::meta::MetaPage>());
        }

        page
    }

    /// Create a page from raw bytes (zero-copy)
    ///
    /// # Safety
    /// The caller must ensure the bytes are properly aligned and valid
    pub unsafe fn from_raw(bytes: &[u8]) -> &Self {
        assert_eq!(bytes.len(), PAGE_SIZE);
        assert_eq!(bytes.as_ptr() as usize % PAGE_SIZE, 0, "Page must be aligned");
        unsafe { &*(bytes.as_ptr() as *const Page) }
    }

    /// Create a mutable page from raw bytes
    ///
    /// # Safety
    /// The caller must ensure the bytes are properly aligned and valid
    pub unsafe fn from_raw_mut(bytes: &mut [u8]) -> &mut Self {
        assert_eq!(bytes.len(), PAGE_SIZE);
        assert_eq!(bytes.as_ptr() as usize % PAGE_SIZE, 0, "Page must be aligned");
        unsafe { &mut *(bytes.as_mut_ptr() as *mut Page) }
    }

    /// Get page as bytes
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self as *const _ as *const u8, PAGE_SIZE) }
    }

    /// Get mutable page as bytes
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self as *mut _ as *mut u8, PAGE_SIZE) }
    }

    /// Get pointer array for keys (for branch/leaf pages)
    pub fn ptrs(&self) -> &[u16] {
        let num_keys = self.header.num_keys as usize;

        // For branch pages using branch_v2, we need to skip the branch header
        let offset = if self.header.flags.contains(PageFlags::BRANCH) {
            crate::branch::BranchHeader::SIZE
        } else {
            0
        };

        // Validate bounds before creating slice
        if offset > self.data.len() || offset + num_keys * 2 > self.data.len() {
            return &[];
        }
        
        unsafe { slice::from_raw_parts(self.data.as_ptr().add(offset) as *const u16, num_keys) }
    }

    /// Get mutable pointer array
    pub fn ptrs_mut(&mut self) -> &mut [u16] {
        let num_keys = self.header.num_keys as usize;

        // For branch pages using branch_v2, we need to skip the branch header
        let offset = if self.header.flags.contains(PageFlags::BRANCH) {
            crate::branch::BranchHeader::SIZE
        } else {
            0
        };

        // Validate bounds before creating slice
        if offset > self.data.len() || offset + num_keys * 2 > self.data.len() {
            return &mut [];
        }
        
        unsafe {
            slice::from_raw_parts_mut(self.data.as_mut_ptr().add(offset) as *mut u16, num_keys)
        }
    }

    /// Get node at index
    #[inline]
    pub fn node(&self, index: usize) -> Result<Node> {
        if index >= self.header.num_keys as usize {
            return Err(Error::InvalidParameter("node index out of bounds"));
        }

        let ptr = self.ptrs()[index];

        // Check that the pointer is within the used area of the page
        // Pointers point to absolute offsets in the page, nodes are stored between header.upper and PAGE_SIZE
        if ptr < self.header.upper || ptr >= PAGE_SIZE as u16 {
            return Err(Error::Corruption {
                details: "Node pointer out of bounds".into(),
                page_id: Some(PageId(self.header.pgno)),
            });
        }

        // Prevent integer underflow
        if (ptr as usize) < PageHeader::SIZE {
            return Err(Error::Corruption {
                details: "Node pointer underflow".into(),
                page_id: Some(PageId(self.header.pgno)),
            });
        }
        
        let data_offset = ptr as usize - PageHeader::SIZE;
        
        // Ensure we have at least NodeHeader size available
        if data_offset + size_of::<NodeHeader>() > self.data.len() {
            return Err(Error::Corruption {
                details: "Node extends beyond page data".into(),
                page_id: Some(PageId(self.header.pgno)),
            });
        }
        
        let node_ptr = unsafe { self.data.as_ptr().add(data_offset) as *const NodeHeader };
        let header = unsafe { *node_ptr };
        
        // Validate node header
        let node_total_size = (header.ksize as usize)
            .checked_add(header.value_size())
            .and_then(|s| s.checked_add(size_of::<NodeHeader>()))
            .ok_or_else(|| Error::Corruption {
                details: "Node size calculation overflow".into(),
                page_id: Some(PageId(self.header.pgno)),
            })?;
        
        if node_total_size > self.data.len().saturating_sub(data_offset) {
            return Err(Error::Corruption {
                details: "Node data extends beyond page".into(),
                page_id: Some(PageId(self.header.pgno)),
            });
        }

        Ok(Node { header, page: self, offset: ptr })
    }

    /// Get a mutable node data reference by index
    pub fn node_data_mut(&mut self, index: usize) -> Result<NodeDataMut> {
        let num_keys = self.header.num_keys as usize;
        if index >= num_keys {
            return Err(Error::InvalidParameter("Node index out of bounds"));
        }

        let ptr = self.ptrs()[index];

        // Check that the pointer is within bounds
        if ptr < self.header.upper || ptr >= PAGE_SIZE as u16 {
            return Err(Error::Corruption {
                details: "Node pointer out of bounds".into(),
                page_id: Some(PageId(self.header.pgno)),
            });
        }

        Ok(NodeDataMut { page: self, offset: ptr })
    }

    /// Add a node to the page at the correct sorted position
    pub fn add_node_sorted(&mut self, key: &[u8], value: &[u8]) -> Result<usize> {
        self.add_node_sorted_with_comparator::<LexicographicComparator>(key, value)
    }

    /// Add a node to the page at the correct sorted position with a custom comparator
    pub fn add_node_sorted_with_comparator<C: Comparator>(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<usize> {
        self.add_node_sorted_internal_with_comparator::<C>(key, value, false, 0)
    }

    /// Add a node with overflow page reference
    pub fn add_node_sorted_overflow(
        &mut self,
        key: &[u8],
        overflow_page_id: PageId,
    ) -> Result<usize> {
        self.add_node_sorted_overflow_with_comparator::<LexicographicComparator>(
            key,
            overflow_page_id,
        )
    }

    /// Add a node with overflow page reference with a custom comparator
    pub fn add_node_sorted_overflow_with_comparator<C: Comparator>(
        &mut self,
        key: &[u8],
        overflow_page_id: PageId,
    ) -> Result<usize> {
        // For overflow nodes, we store the page ID as the "value"
        let page_bytes = overflow_page_id.0.to_le_bytes();
        self.add_node_sorted_internal_with_comparator::<C>(
            key,
            &page_bytes,
            true,
            std::mem::size_of::<u64>(),
        )
    }

    /// Internal method to add a node
    #[allow(dead_code)]
    fn add_node_sorted_internal(
        &mut self,
        key: &[u8],
        value: &[u8],
        is_overflow: bool,
        value_size_override: usize,
    ) -> Result<usize> {
        self.add_node_sorted_internal_with_comparator::<LexicographicComparator>(
            key,
            value,
            is_overflow,
            value_size_override,
        )
    }

    /// Internal method to add a node with a custom comparator
    fn add_node_sorted_internal_with_comparator<C: Comparator>(
        &mut self,
        key: &[u8],
        value: &[u8],
        is_overflow: bool,
        value_size_override: usize,
    ) -> Result<usize> {
        let actual_value_size = if is_overflow { value_size_override } else { value.len() };
        let node_size = NodeHeader::SIZE
            .checked_add(key.len())
            .and_then(|s| s.checked_add(value.len()))
            .ok_or_else(|| Error::Custom("Node size calculation overflow".into()))?;

        // Check if we have space for the new node using fill factor thresholds
        // This uses sophisticated utilization checks rather than exact space
        if !self.has_room_for(key.len(), value.len()) {
            return Err(Error::Custom("Page full".into()));
        }

        // Find insertion position
        let insert_pos = match self.search_key_with_comparator::<C>(key)? {
            SearchResult::Found { index: _ } => {
                return Err(Error::Custom("Key already exists".into()));
            }
            SearchResult::NotFound { insert_pos } => insert_pos,
        };

        // Allocate space from upper bound, ensuring alignment for NodeHeader
        self.header.upper -= node_size as u16;
        // Align to 2-byte boundary for NodeHeader
        if self.header.upper % 2 != 0 {
            self.header.upper -= 1;
        }
        let node_offset = self.header.upper;

        // Write node header
        let mut node_header = NodeHeader {
            flags: NodeFlags::empty(),
            ksize: key.len() as u16,
            lo: (actual_value_size & 0xffff) as u16,
            hi: (actual_value_size >> 16) as u16,
        };

        if is_overflow {
            node_header.flags.insert(NodeFlags::BIGDATA);
        }

        // Validate node_offset to prevent underflow
        if (node_offset as usize) < PageHeader::SIZE {
            return Err(Error::Custom("Page full".into()));
        }
        
        let data_offset = node_offset as usize - PageHeader::SIZE;
        
        // Validate we have enough space in data array
        if data_offset + node_size > self.data.len() {
            return Err(Error::Custom("Page full".into()));
        }
        
        unsafe {
            let node_ptr = self.data.as_mut_ptr().add(data_offset) as *mut NodeHeader;

            // Verify alignment
            debug_assert_eq!(node_ptr as usize % 2, 0, "NodeHeader must be 2-byte aligned");

            *node_ptr = node_header;

            // Write key with bounds check
            let key_ptr = node_ptr.add(1) as *mut u8;
            let key_offset = key_ptr as usize - self.data.as_ptr() as usize;
            let key_end = key_offset
                .checked_add(key.len())
                .ok_or_else(|| Error::Custom("Key end calculation overflow".into()))?;
            if key_end > self.data.len() {
                return Err(Error::Custom("Page full".into()));
            }
            ptr::copy_nonoverlapping(key.as_ptr(), key_ptr, key.len());

            // Write value (or overflow page ID) with bounds check
            let val_ptr = key_ptr.add(key.len());
            let val_offset = val_ptr as usize - self.data.as_ptr() as usize;
            let val_end = val_offset
                .checked_add(value.len())
                .ok_or_else(|| Error::Custom("Value end calculation overflow".into()))?;
            if val_end > self.data.len() {
                return Err(Error::Custom("Page full".into()));
            }
            ptr::copy_nonoverlapping(value.as_ptr(), val_ptr, value.len());
        }

        // Insert pointer at the correct position
        self.insert_ptr(insert_pos, node_offset);
        self.header.num_keys += 1;
        self.header.lower += size_of::<u16>() as u16;

        Ok(insert_pos)
    }

    /// Add a node to the page (unsorted, appends at end)
    pub fn add_node(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.add_node_sorted(key, value)?;
        Ok(())
    }

    /// Insert pointer at index
    fn insert_ptr(&mut self, index: usize, ptr: u16) {
        // Get current count of pointers
        let current_count = self.header.num_keys as usize;

        // Ensure we have space for the new pointer
        assert!(index <= current_count, "Insert index out of bounds");

        // For branch pages using branch_v2, we need to skip the branch header
        let offset = if self.header.flags.contains(PageFlags::BRANCH) {
            crate::branch::BranchHeader::SIZE
        } else {
            0
        };

        // Validate array bounds
        let ptr_array_size = match (current_count + 1).checked_mul(2) {
            Some(s) => s,
            None => panic!("Pointer array size overflow"),
        };
        let required_size = match offset.checked_add(ptr_array_size) {
            Some(s) => s,
            None => panic!("Required size calculation overflow"),
        };
        if required_size > self.data.len() {
            panic!("Pointer array extends beyond page data");
        }
        
        // Get pointer to the start of the pointer array
        let ptrs_ptr = unsafe { self.data.as_mut_ptr().add(offset) as *mut u16 };

        // Shift existing pointers if needed
        if index < current_count {
            unsafe {
                // Validate source and destination ranges
                let src_offset = offset.saturating_add(index.saturating_mul(2));
                let dst_offset = offset.saturating_add((index + 1).saturating_mul(2));
                let copy_size = (current_count - index).saturating_mul(2);
                
                if src_offset.saturating_add(copy_size) > self.data.len() || dst_offset.saturating_add(copy_size) > self.data.len() {
                    panic!("Pointer copy would exceed page bounds");
                }
                
                let src = ptrs_ptr.add(index);
                let dst = ptrs_ptr.add(index + 1);
                ptr::copy(src, dst, current_count - index);
            }
        }

        // Insert the new pointer
        unsafe {
            *ptrs_ptr.add(index) = ptr;
        }
    }

    /// Search for a key using binary search (assumes sorted nodes)
    pub fn search_key(&self, key: &[u8]) -> Result<SearchResult> {
        self.search_key_with_comparator::<LexicographicComparator>(key)
    }

    /// Search for a key using binary search with a custom comparator
    #[inline]
    pub fn search_key_with_comparator<C: Comparator>(&self, key: &[u8]) -> Result<SearchResult> {
        if self.header.num_keys == 0 {
            return Ok(SearchResult::NotFound { insert_pos: 0 });
        }

        // Binary search through sorted nodes
        let mut left = 0;
        let mut right = self.header.num_keys as usize;

        while left < right {
            let mid = left + (right - left) / 2;
            let node = self.node(mid)?;
            let node_key = node.key()?;

            match C::compare(key, node_key) {
                std::cmp::Ordering::Less => right = mid,
                std::cmp::Ordering::Greater => left = mid + 1,
                std::cmp::Ordering::Equal => return Ok(SearchResult::Found { index: mid }),
            }
        }

        Ok(SearchResult::NotFound { insert_pos: left })
    }

    /// Get the middle node for splitting
    pub fn middle_node(&self) -> Result<(Vec<u8>, usize)> {
        let mid_idx = self.header.num_keys as usize / 2;
        let node = self.node(mid_idx)?;
        let key = node.key()?.to_vec();
        Ok((key, mid_idx))
    }

    /// Split this page into two pages, returning nodes for the right page
    pub fn split(&self) -> Result<LeafSplitResult> {
        let mid_idx = self.header.num_keys as usize / 2;
        let mut right_nodes = Vec::new();

        // Collect nodes for the right page
        for i in mid_idx..self.header.num_keys as usize {
            let node = self.node(i)?;
            let key = node.key()?.to_vec();
            let value = node.value()?.into_owned();
            right_nodes.push((key, value));
        }

        // Get the median key
        let median_node = self.node(mid_idx)?;
        let median_key = median_node.key()?.to_vec();

        Ok((right_nodes, median_key))
    }
    
    /// Split this page with smart split point calculation
    pub fn split_with_config(&self, is_append: bool) -> Result<LeafSplitResult> {
        let config = PageCapacityConfig::default();
        let num_keys = self.header.num_keys as usize;
        let split_idx = crate::page_capacity::calculate_split_point(num_keys, is_append, &config);
        
        let mut right_nodes = Vec::new();

        // Collect nodes for the right page
        for i in split_idx..num_keys {
            let node = self.node(i)?;
            let key = node.key()?.to_vec();
            let value = node.value()?.into_owned();
            right_nodes.push((key, value));
        }

        // Get the median key
        let median_node = self.node(split_idx)?;
        let median_key = median_node.key()?.to_vec();

        Ok((right_nodes, median_key))
    }

    /// Check if page has room for an entry of given size
    pub fn has_room_for(&self, key_size: usize, value_size: usize) -> bool {
        let node_size = match NodeHeader::SIZE
            .checked_add(key_size)
            .and_then(|s| s.checked_add(value_size)) {
            Some(size) => size,
            None => return false, // Overflow means it definitely won't fit
        };
        let required_space = match node_size.checked_add(size_of::<u16>()) {
            Some(space) => space,
            None => return false,
        };
        
        // Check absolute free space first
        let free = self.header.free_space();
        if free < required_space {
            return false;
        }
        
        // Calculate current utilization with overflow checks
        let lower_used = self.header.lower.saturating_sub(PageHeader::SIZE as u16) as usize;
        let upper_used = (PAGE_SIZE as u16).saturating_sub(self.header.upper) as usize;
        let used_space = match lower_used.checked_add(upper_used) {
            Some(space) => space,
            None => return false,
        };
        let total_usable = PAGE_SIZE.saturating_sub(PageHeader::SIZE);
        let new_used = match used_space.checked_add(required_space) {
            Some(space) => space,
            None => return false,
        };
        let utilization_after = new_used as f32 / total_usable as f32;
        
        // Very permissive thresholds to maximize page usage
        // For benchmarking, we need to pack pages as full as possible
        let threshold = match self.header.num_keys {
            0..=5 => 0.98,   // Nearly empty pages: allow up to 98%
            6..=15 => 0.96,  // Small pages: allow up to 96%
            16..=30 => 0.94, // Medium pages: allow up to 94%
            _ => 0.92,       // Large pages: allow up to 92%
        };
        
        utilization_after < threshold
    }
    
    /// Check if page should be split proactively based on capacity
    pub fn should_split(&self, next_entry_size: Option<usize>) -> bool {
        // For now, use simple threshold-based approach
        const SPLIT_THRESHOLD: f32 = 0.85; // Split at 85% full
        
        let lower_used = self.header.lower.saturating_sub(PageHeader::SIZE as u16) as usize;
        let upper_used = (PAGE_SIZE as u16).saturating_sub(self.header.upper) as usize;
        let used_space = lower_used.saturating_add(upper_used);
        let total_usable = PAGE_SIZE.saturating_sub(PageHeader::SIZE);
        let utilization = used_space as f32 / total_usable as f32;
        
        // If we have a specific entry size, check if adding it would exceed threshold
        if let Some(size) = next_entry_size {
            let entry_with_ptr = match size.checked_add(size_of::<u16>()) {
                Some(s) => s,
                None => return true, // Overflow means we should split
            };
            let new_used = match used_space.checked_add(entry_with_ptr) {
                Some(s) => s,
                None => return true,
            };
            let new_utilization = new_used as f32 / total_usable as f32;
            return new_utilization >= SPLIT_THRESHOLD;
        }
        
        utilization >= SPLIT_THRESHOLD
    }

    /// Remove nodes starting from index
    pub fn truncate(&mut self, from_index: usize) {
        if from_index >= self.header.num_keys as usize {
            return;
        }

        // Update header
        self.header.num_keys = from_index as u16;

        // Calculate new lower bound, accounting for branch header if present
        let header_offset = if self.header.flags.contains(PageFlags::BRANCH) {
            crate::branch::BranchHeader::SIZE
        } else {
            0
        };

        let ptr_size = match from_index.checked_mul(size_of::<u16>()) {
            Some(s) => s as u16,
            None => return, // Overflow, just leave it as is
        };
        self.header.lower = (PageHeader::SIZE as u16)
            .saturating_add(header_offset as u16)
            .saturating_add(ptr_size);

        // Note: We don't reclaim the space from removed nodes, they'll be
        // overwritten when new nodes are added
    }

    /// Remove a node at the specified index
    pub fn remove_node(&mut self, index: usize) -> Result<()> {
        if index >= self.header.num_keys as usize {
            return Err(Error::InvalidParameter("Node index out of bounds"));
        }

        // For branch pages using branch_v2, we need to skip the branch header
        let offset = if self.header.flags.contains(PageFlags::BRANCH) {
            crate::branch::BranchHeader::SIZE
        } else {
            0
        };

        // Get pointer to the start of the pointer array
        let ptrs_ptr = unsafe { self.data.as_mut_ptr().add(offset) as *mut u16 };

        // Shift pointers after the removed one
        if index < self.header.num_keys as usize - 1 {
            unsafe {
                let src = ptrs_ptr.add(index + 1);
                let dst = ptrs_ptr.add(index);
                ptr::copy(src, dst, self.header.num_keys as usize - index - 1);
            }
        }

        // Update header
        self.header.num_keys -= 1;
        self.header.lower -= size_of::<u16>() as u16;

        // Note: We don't reclaim the space from the removed node, it will be
        // overwritten when new nodes are added

        Ok(())
    }

    /// Clear all nodes from the page
    pub fn clear(&mut self) {
        self.header.num_keys = 0;

        // Calculate initial lower bound, accounting for branch header if present
        let header_offset = if self.header.flags.contains(PageFlags::BRANCH) {
            crate::branch::BranchHeader::SIZE
        } else {
            0
        };

        self.header.lower = PageHeader::SIZE as u16 + header_offset as u16;
        self.header.upper = PAGE_SIZE as u16;
        // Note: We don't need to clear the data, it will be overwritten
    }
}

/// Result of searching for a key in a page
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchResult {
    /// Key was found at index
    Found {
        /// Index of the found key
        index: usize,
    },
    /// Key was not found, would be inserted at position
    NotFound {
        /// Position where the key would be inserted
        insert_pos: usize,
    },
}

bitflags! {
    /// Node flags
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct NodeFlags: u16 {
        /// Node contains a sub-page
        const BIGDATA = 0x01;
        /// Node contains a sub-database
        const SUBDATA = 0x02;
        /// Node contains duplicate data
        const DUPDATA = 0x04;
        /// Node value is in overflow pages
        const OVERFLOW = 0x08;
    }
}

/// Node header within a page
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct NodeHeader {
    /// Node flags
    pub flags: NodeFlags,
    /// Key size
    pub ksize: u16,
    /// Low 16 bits of value size
    pub lo: u16,
    /// High 16 bits of value size
    pub hi: u16,
}

impl NodeHeader {
    /// Size of node header
    pub const SIZE: usize = size_of::<Self>();

    /// Get value size
    pub fn value_size(&self) -> usize {
        (self.lo as usize) | ((self.hi as usize) << 16)
    }

    /// Set value size
    pub fn set_value_size(&mut self, size: usize) {
        self.lo = (size & 0xffff) as u16;
        self.hi = (size >> 16) as u16;
    }
}

/// A node within a page
pub struct Node<'a> {
    /// Node header
    pub header: NodeHeader,
    /// Reference to containing page
    page: &'a Page,
    /// Offset within page data
    offset: u16,
}

impl<'a> Node<'a> {
    /// Get the key bytes
    #[inline]
    pub fn key(&self) -> Result<&'a [u8]> {
        // Prevent underflow
        if (self.offset as usize) < PageHeader::SIZE {
            return Err(Error::Corruption {
                details: "Node offset underflow".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            });
        }
        
        let node_data_offset = (self.offset as usize)
            .checked_sub(PageHeader::SIZE)
            .ok_or_else(|| Error::Corruption {
                details: "Node offset underflow".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            })?;
        let key_offset = node_data_offset
            .checked_add(NodeHeader::SIZE)
            .ok_or_else(|| Error::Corruption {
                details: "Key offset overflow".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            })?;
        let key_len = self.header.ksize as usize;

        // The node data starts at self.offset and extends towards PAGE_SIZE
        // We need to ensure key_offset + key_len doesn't exceed the data array bounds
        let key_end = key_offset
            .checked_add(key_len)
            .ok_or_else(|| Error::Corruption {
                details: "Key end calculation overflow".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            })?;
        if key_offset > self.page.data.len() || key_end > self.page.data.len() {
            return Err(Error::Corruption {
                details: "Node key extends beyond page".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            });
        }

        Ok(unsafe { slice::from_raw_parts(self.page.data.as_ptr().add(key_offset), key_len) })
    }

    /// Get the value bytes
    #[inline]
    pub fn value(&self) -> Result<Cow<'a, [u8]>> {
        let node_data_offset = (self.offset as usize)
            .checked_sub(PageHeader::SIZE)
            .ok_or_else(|| Error::Corruption {
                details: "Node offset underflow".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            })?;
        let val_offset = node_data_offset
            .checked_add(NodeHeader::SIZE)
            .and_then(|o| o.checked_add(self.header.ksize as usize))
            .ok_or_else(|| Error::Corruption {
                details: "Value offset overflow".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            })?;
        let val_len = self.header.value_size();

        if self.header.flags.contains(NodeFlags::BIGDATA) {
            // Value is in overflow pages
            // Read u64 from potentially unaligned location
            let mut pgno_bytes = [0u8; 8];
            unsafe {
                ptr::copy_nonoverlapping(
                    self.page.data.as_ptr().add(val_offset),
                    pgno_bytes.as_mut_ptr(),
                    8,
                );
            }
            let pgno = u64::from_le_bytes(pgno_bytes);
            // For now, return an error indicating overflow pages need to be loaded
            // The caller should use a transaction to load the overflow value
            return Err(Error::Custom(format!("Value in overflow page {}", pgno).into()));
        }

        // Ensure value doesn't extend beyond the data array
        let val_end = val_offset
            .checked_add(val_len)
            .ok_or_else(|| Error::Corruption {
                details: "Value end calculation overflow".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            })?;
        if val_end > self.page.data.len() {
            return Err(Error::Corruption {
                details: "Node value extends beyond page".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            });
        }

        Ok(Cow::Borrowed(unsafe {
            slice::from_raw_parts(self.page.data.as_ptr().add(val_offset), val_len)
        }))
    }

    /// Get page number for branch nodes
    pub fn page_number(&self) -> Result<PageId> {
        if !self.page.header.flags.contains(PageFlags::BRANCH) {
            return Err(Error::InvalidOperation("Not a branch page"));
        }

        // The offset is an absolute position in the page, we need to convert to data array offset
        let data_offset = self.offset as usize - PageHeader::SIZE;
        let val_offset = data_offset + NodeHeader::SIZE + self.header.ksize as usize;

        // Read u64 from potentially unaligned location
        let mut pgno_bytes = [0u8; 8];
        unsafe {
            ptr::copy_nonoverlapping(
                self.page.data.as_ptr().add(val_offset),
                pgno_bytes.as_mut_ptr(),
                8,
            );
        }
        let pgno = u64::from_le_bytes(pgno_bytes);

        Ok(PageId(pgno))
    }

    /// Get overflow page ID if this is an overflow value
    pub fn overflow_page(&self) -> Result<Option<PageId>> {
        if !self.header.flags.contains(NodeFlags::BIGDATA) {
            return Ok(None);
        }

        let node_data_offset = (self.offset as usize)
            .checked_sub(PageHeader::SIZE)
            .ok_or_else(|| Error::Corruption {
                details: "Node offset underflow".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            })?;
        let val_offset = node_data_offset
            .checked_add(NodeHeader::SIZE)
            .and_then(|o| o.checked_add(self.header.ksize as usize))
            .ok_or_else(|| Error::Corruption {
                details: "Value offset overflow".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            })?;

        // Read u64 from potentially unaligned location
        let mut pgno_bytes = [0u8; 8];
        unsafe {
            ptr::copy_nonoverlapping(
                self.page.data.as_ptr().add(val_offset),
                pgno_bytes.as_mut_ptr(),
                8,
            );
        }
        let pgno = u64::from_le_bytes(pgno_bytes);

        Ok(Some(PageId(pgno)))
    }
}

/// Mutable node data accessor
pub struct NodeDataMut<'a> {
    page: &'a mut Page,
    offset: u16,
}

impl<'a> NodeDataMut<'a> {
    /// Set the value of this node
    pub fn set_value(&mut self, new_value: &[u8]) -> Result<()> {
        // Prevent underflow
        if (self.offset as usize) < PageHeader::SIZE {
            return Err(Error::Corruption {
                details: "Node offset underflow".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            });
        }
        
        let node_data_offset = self.offset as usize - PageHeader::SIZE;
        
        // Validate node header is within bounds
        if node_data_offset + size_of::<NodeHeader>() > self.page.data.len() {
            return Err(Error::Corruption {
                details: "Node header extends beyond page".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            });
        }
        
        let node_ptr = unsafe {
            self.page.data.as_ptr().add(node_data_offset) as *const NodeHeader
        };
        let header = unsafe { *node_ptr };

        // Check if new value fits
        let old_value_size = header.value_size();
        if new_value.len() != old_value_size && !header.flags.contains(NodeFlags::BIGDATA) {
            return Err(Error::InvalidParameter("Cannot change value size without reallocation"));
        }

        // Calculate and validate value offset
        let val_offset = node_data_offset
            .checked_add(NodeHeader::SIZE)
            .and_then(|o| o.checked_add(header.ksize as usize))
            .ok_or_else(|| Error::Corruption {
                details: "Value offset overflow".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            })?;
            
        // Validate write bounds
        let copy_len = new_value.len().min(old_value_size);
        if val_offset + copy_len > self.page.data.len() {
            return Err(Error::Corruption {
                details: "Value write would exceed page bounds".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            });
        }
        
        unsafe {
            std::ptr::copy_nonoverlapping(
                new_value.as_ptr(),
                self.page.data.as_mut_ptr().add(val_offset),
                copy_len,
            );
        }

        Ok(())
    }

    /// Set this node to use an overflow page
    pub fn set_overflow(&mut self, overflow_id: PageId) -> Result<()> {
        // Prevent underflow
        if (self.offset as usize) < PageHeader::SIZE {
            return Err(Error::Corruption {
                details: "Node offset underflow".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            });
        }
        
        let node_data_offset = self.offset as usize - PageHeader::SIZE;
        
        // Validate node header is within bounds
        if node_data_offset + size_of::<NodeHeader>() > self.page.data.len() {
            return Err(Error::Corruption {
                details: "Node header extends beyond page".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            });
        }
        
        let node_ptr = unsafe {
            self.page.data.as_mut_ptr().add(node_data_offset) as *mut NodeHeader
        };
        let header = unsafe { &mut *node_ptr };

        // Update flags
        header.flags.insert(NodeFlags::BIGDATA);

        // Calculate and validate value offset
        let val_offset = node_data_offset
            .checked_add(NodeHeader::SIZE)
            .and_then(|o| o.checked_add(header.ksize as usize))
            .ok_or_else(|| Error::Corruption {
                details: "Value offset overflow".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            })?;
            
        // Validate we can write 8 bytes for page ID
        if val_offset + 8 > self.page.data.len() {
            return Err(Error::Corruption {
                details: "Overflow page ID write would exceed page bounds".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            });
        }
        
        let pgno_bytes = overflow_id.0.to_le_bytes();
        unsafe {
            std::ptr::copy_nonoverlapping(
                pgno_bytes.as_ptr(),
                self.page.data.as_mut_ptr().add(val_offset),
                8,
            );
        }

        // Update value size to indicate it's an overflow reference
        let vsize = std::mem::size_of::<u64>() as u32;
        header.lo = vsize as u16;
        header.hi = (vsize >> 16) as u16;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_creation() {
        let page = Page::new(PageId(1), PageFlags::LEAF);
        assert_eq!(page.header.pgno, 1);
        assert_eq!(page.header.flags, PageFlags::LEAF);
        assert_eq!(page.header.num_keys, 0);
        assert_eq!(page.header.free_space(), PAGE_SIZE - PageHeader::SIZE);
    }

    #[test]
    fn test_add_node() {
        let mut page = Page::new(PageId(1), PageFlags::LEAF);

        page.add_node(b"key1", b"value1").unwrap();
        assert_eq!(page.header.num_keys, 1);

        let node = page.node(0).unwrap();
        assert_eq!(node.key().unwrap(), b"key1");
        assert_eq!(node.value().unwrap().as_ref(), b"value1");
    }

    #[test]
    fn test_search_key() {
        let mut page = Page::new(PageId(1), PageFlags::LEAF);

        page.add_node(b"aaa", b"1").unwrap();
        page.add_node(b"ccc", b"3").unwrap();
        page.add_node(b"bbb", b"2").unwrap();

        match page.search_key(b"bbb").unwrap() {
            SearchResult::Found { index } => {
                let node = page.node(index).unwrap();
                assert_eq!(node.key().unwrap(), b"bbb");
            }
            _ => panic!("Key should be found"),
        }

        match page.search_key(b"ddd").unwrap() {
            SearchResult::NotFound { insert_pos } => {
                assert_eq!(insert_pos, 3);
            }
            _ => panic!("Key should not be found"),
        }
    }

    #[test]
    fn test_add_single_node() {
        let mut page = Page::new(PageId(1), PageFlags::LEAF);
        assert_eq!(page.header.num_keys, 0);

        page.add_node_sorted(b"key1", b"value1").unwrap();
        assert_eq!(page.header.num_keys, 1);

        // Check that node can be retrieved
        let node = page.node(0).unwrap();
        assert_eq!(node.key().unwrap(), b"key1");
        assert_eq!(node.value().unwrap().as_ref(), b"value1");
    }
}
