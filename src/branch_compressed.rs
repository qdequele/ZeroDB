//! Compressed branch page implementation with key prefix compression
//! 
//! This implementation compresses keys by storing only the distinguishing
//! prefix needed to separate them from adjacent keys, significantly 
//! improving cache efficiency.

use crate::error::{Error, Result, PageId};
use crate::page::{Page, PageFlags, PageHeader, PAGE_SIZE};
use crate::comparator::Comparator;
use std::cmp::min;

/// Compressed branch header
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct CompressedBranchHeader {
    /// The leftmost child pointer
    pub leftmost_child: PageId,
    /// Number of compressed entries
    pub num_entries: u16,
    /// Offset to key data area
    pub key_area_offset: u16,
}

impl CompressedBranchHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

/// Compressed key entry
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct CompressedEntry {
    /// Prefix length shared with previous key
    pub prefix_len: u8,
    /// Length of unique suffix
    pub suffix_len: u8,
    /// Child page ID
    pub child: PageId,
    /// Offset to suffix data (relative to key area)
    pub suffix_offset: u16,
}

impl CompressedEntry {
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

/// Compressed branch page operations
pub struct CompressedBranchPage;

impl CompressedBranchPage {
    /// Calculate common prefix length between two keys
    fn common_prefix_len(key1: &[u8], key2: &[u8]) -> usize {
        let len = min(key1.len(), key2.len());
        for i in 0..len {
            if key1[i] != key2[i] {
                return i;
            }
        }
        len
    }
    
    /// Initialize a compressed branch page
    pub fn init_root(
        page: &mut Page,
        median_key: &[u8],
        left_child: PageId,
        right_child: PageId,
    ) -> Result<()> {
        page.header.flags = PageFlags::BRANCH;
        page.header.num_keys = 1;
        page.header.lower = PageHeader::SIZE as u16 + CompressedBranchHeader::SIZE as u16;
        page.header.upper = PAGE_SIZE as u16;
        
        // Write header
        let header = CompressedBranchHeader {
            leftmost_child: left_child,
            num_entries: 1,
            key_area_offset: CompressedBranchHeader::SIZE as u16 + CompressedEntry::SIZE as u16,
        };
        
        unsafe {
            let header_ptr = page.data.as_mut_ptr() as *mut CompressedBranchHeader;
            *header_ptr = header;
        }
        
        // Write first entry (no prefix compression for first key)
        let entry = CompressedEntry {
            prefix_len: 0,
            suffix_len: median_key.len() as u8,
            child: right_child,
            suffix_offset: 0,
        };
        
        unsafe {
            let entry_ptr = page.data.as_mut_ptr()
                .add(CompressedBranchHeader::SIZE) as *mut CompressedEntry;
            *entry_ptr = entry;
        }
        
        // Write key data
        let key_area_start = header.key_area_offset as usize;
        unsafe {
            std::ptr::copy_nonoverlapping(
                median_key.as_ptr(),
                page.data.as_mut_ptr().add(key_area_start),
                median_key.len()
            );
        }
        
        page.header.lower = (key_area_start + median_key.len()) as u16;
        
        Ok(())
    }
    
    /// Find child for a given key using compressed entries
    pub fn find_child_compressed<C: Comparator>(
        page: &Page,
        search_key: &[u8],
    ) -> Result<PageId> {
        if !page.header.flags.contains(PageFlags::BRANCH) {
            return Err(Error::InvalidOperation("Not a branch page"));
        }
        
        let header = unsafe {
            &*(page.data.as_ptr() as *const CompressedBranchHeader)
        };
        
        if header.num_entries == 0 {
            return Ok(header.leftmost_child);
        }
        
        // Reconstruct keys on the fly during binary search
        let mut left = 0;
        let mut right = header.num_entries as usize;
        let mut current_key = Vec::new();
        
        while left < right {
            let mid = left + (right - left) / 2;
            
            // Reconstruct key at mid
            current_key.clear();
            for i in 0..=mid {
                let entry = unsafe {
                    &*(page.data.as_ptr()
                        .add(CompressedBranchHeader::SIZE + i * CompressedEntry::SIZE) 
                        as *const CompressedEntry)
                };
                
                // Keep prefix from previous key
                current_key.truncate(entry.prefix_len as usize);
                
                // Add suffix
                let suffix_start = header.key_area_offset as usize + entry.suffix_offset as usize;
                let suffix = unsafe {
                    std::slice::from_raw_parts(
                        page.data.as_ptr().add(suffix_start),
                        entry.suffix_len as usize
                    )
                };
                current_key.extend_from_slice(suffix);
            }
            
            match C::compare(search_key, &current_key) {
                std::cmp::Ordering::Less => right = mid,
                std::cmp::Ordering::Greater => left = mid + 1,
                std::cmp::Ordering::Equal => {
                    let entry = unsafe {
                        &*(page.data.as_ptr()
                            .add(CompressedBranchHeader::SIZE + mid * CompressedEntry::SIZE) 
                            as *const CompressedEntry)
                    };
                    return Ok(entry.child);
                }
            }
        }
        
        // Not found - return appropriate child
        if left == 0 {
            Ok(header.leftmost_child)
        } else {
            let entry = unsafe {
                &*(page.data.as_ptr()
                    .add(CompressedBranchHeader::SIZE + (left - 1) * CompressedEntry::SIZE) 
                    as *const CompressedEntry)
            };
            Ok(entry.child)
        }
    }
    
    /// Add a compressed entry
    pub fn add_compressed_entry(
        page: &mut Page,
        new_key: &[u8],
        child: PageId,
        prev_key: Option<&[u8]>,
    ) -> Result<()> {
        let header = unsafe {
            &mut *(page.data.as_mut_ptr() as *mut CompressedBranchHeader)
        };
        
        // Calculate prefix compression
        let prefix_len = if let Some(prev) = prev_key {
            Self::common_prefix_len(prev, new_key) as u8
        } else {
            0
        };
        
        let suffix_len = (new_key.len() - prefix_len as usize) as u8;
        let suffix = &new_key[prefix_len as usize..];
        
        // Check if we have space
        let entry_space = CompressedEntry::SIZE;
        let key_space = suffix.len();
        let total_needed = page.header.lower as usize + entry_space + key_space;
        
        if total_needed > page.header.upper as usize {
            return Err(Error::Custom("Compressed page full".into()));
        }
        
        // Calculate suffix offset
        let suffix_offset = if header.num_entries == 0 {
            0
        } else {
            // Find end of current key area
            let mut offset = 0;
            for i in 0..header.num_entries {
                let entry = unsafe {
                    &*(page.data.as_ptr()
                        .add(CompressedBranchHeader::SIZE + i as usize * CompressedEntry::SIZE) 
                        as *const CompressedEntry)
                };
                offset += entry.suffix_len as u16;
            }
            offset
        };
        
        // Write entry
        let entry = CompressedEntry {
            prefix_len,
            suffix_len,
            child,
            suffix_offset,
        };
        
        unsafe {
            let entry_ptr = page.data.as_mut_ptr()
                .add(CompressedBranchHeader::SIZE + header.num_entries as usize * CompressedEntry::SIZE) 
                as *mut CompressedEntry;
            *entry_ptr = entry;
        }
        
        // Write suffix
        let suffix_start = header.key_area_offset as usize + suffix_offset as usize;
        unsafe {
            std::ptr::copy_nonoverlapping(
                suffix.as_ptr(),
                page.data.as_mut_ptr().add(suffix_start),
                suffix.len()
            );
        }
        
        // Update header
        header.num_entries += 1;
        page.header.num_keys = header.num_entries;
        page.header.lower = (suffix_start + suffix.len()) as u16;
        
        Ok(())
    }
}