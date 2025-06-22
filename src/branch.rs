//! Improved branch page implementation for B+Tree
//!
//! This implementation uses a more standard approach where branch pages
//! explicitly store n keys and n+1 child pointers.
//!
//! Layout:
//! - First child pointer (8 bytes) at fixed offset
//! - Followed by n pairs of (key, child_pointer)
//!
//! This allows for efficient navigation without special cases for empty keys.

use crate::comparator::{Comparator, LexicographicComparator};
use crate::error::{Error, PageId, Result};
use crate::page::{NodeHeader, Page, PageFlags, PageHeader};
use std::mem::size_of;

/// Type alias for split result: (left_items, split_key, right_page_id)
type SplitResult = (Vec<(Vec<u8>, PageId)>, Vec<u8>, PageId);

/// Branch page header stored at the beginning of page data
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct BranchHeader {
    /// The leftmost child pointer
    pub leftmost_child: PageId,
}

impl BranchHeader {
    /// Size of the branch header in bytes
    pub const SIZE: usize = size_of::<Self>();
}

/// Branch page operations
pub struct BranchPage;

impl BranchPage {
    /// Initialize a new branch page with one key and two children
    pub fn init_root(
        page: &mut Page,
        median_key: &[u8],
        left_child: PageId,
        right_child: PageId,
    ) -> Result<()> {
        // Ensure it's a branch page
        page.header.flags = PageFlags::BRANCH;
        page.header.num_keys = 0;
        page.header.lower = PageHeader::SIZE as u16;
        page.header.upper = crate::page::PAGE_SIZE as u16;

        // Write branch header with leftmost child
        let header = BranchHeader { leftmost_child: left_child };

        unsafe {
            let header_ptr = page.data.as_mut_ptr() as *mut BranchHeader;
            *header_ptr = header;
        }

        // Adjust lower to account for branch header
        page.header.lower = page.header.lower
            .checked_add(BranchHeader::SIZE as u16)
            .ok_or_else(|| Error::Custom("Page lower bound overflow".into()))?;

        // Add the median key with right child
        // In branch pages, we store the child page ID as the "value"
        page.add_node(median_key, &right_child.0.to_le_bytes())?;

        Ok(())
    }

    /// Get the branch header
    fn get_header(page: &Page) -> Result<&BranchHeader> {
        if !page.header.flags.contains(PageFlags::BRANCH) {
            return Err(Error::InvalidOperation("Not a branch page"));
        }

        unsafe { Ok(&*(page.data.as_ptr() as *const BranchHeader)) }
    }

    /// Get the leftmost child of a branch page
    pub fn get_leftmost_child(page: &Page) -> Result<PageId> {
        let header = Self::get_header(page)?;
        Ok(header.leftmost_child)
    }

    /// Get the child at a specific index
    /// Index 0 returns the child after the first key
    pub fn get_child_at(page: &Page, index: usize) -> Result<PageId> {
        if !page.header.flags.contains(PageFlags::BRANCH) {
            return Err(Error::InvalidOperation("Not a branch page"));
        }

        // Get the node at this index
        let node = page.node(index)?;
        node.page_number()
    }

    /// Find the appropriate child for a given key
    pub fn find_child(page: &Page, search_key: &[u8]) -> Result<PageId> {
        Self::find_child_with_comparator::<LexicographicComparator>(page, search_key)
    }

    /// Find the appropriate child for a given key with a custom comparator
    pub fn find_child_with_comparator<C: Comparator>(
        page: &Page,
        search_key: &[u8],
    ) -> Result<PageId> {
        if !page.header.flags.contains(PageFlags::BRANCH) {
            return Err(Error::InvalidOperation("Not a branch page"));
        }

        let header = Self::get_header(page)?;

        // If no keys, return leftmost child
        if page.header.num_keys == 0 {
            return Ok(header.leftmost_child);
        }

        // Binary search through keys
        let mut left = 0;
        let mut right = page.header.num_keys as usize;

        while left < right {
            let mid = left + (right - left) / 2;
            let node = page.node(mid)?;
            let node_key = node.key()?;

            match C::compare(search_key, node_key) {
                std::cmp::Ordering::Less => {
                    right = mid;
                }
                std::cmp::Ordering::Greater => {
                    left = mid + 1;
                }
                std::cmp::Ordering::Equal => {
                    // Key found, return the corresponding child
                    return node.page_number();
                }
            }
        }

        // Key not found
        if left == 0 {
            // Less than all keys, use leftmost child
            Ok(header.leftmost_child)
        } else {
            // Use the child of the previous key
            let node = page.node(left - 1)?;
            node.page_number()
        }
    }

    /// Add a new key and right child after a split
    pub fn add_split_child(page: &mut Page, key: &[u8], right_child: PageId) -> Result<()> {
        if !page.header.flags.contains(PageFlags::BRANCH) {
            return Err(Error::InvalidOperation("Not a branch page"));
        }

        // Add as a normal node (key -> child page ID)
        page.add_node_sorted(key, &right_child.0.to_le_bytes())?;
        Ok(())
    }

    /// Split a branch page
    pub fn split(page: &Page) -> Result<SplitResult> {
        if !page.header.flags.contains(PageFlags::BRANCH) {
            return Err(Error::InvalidOperation("Not a branch page"));
        }

        let _header = Self::get_header(page)?;
        let mid_idx = page.header.num_keys as usize / 2;

        // Get the median key
        let median_node = page.node(mid_idx)?;
        let median_key = median_node.key()?.to_vec();
        let median_child = median_node.page_number()?;

        // Collect entries for the right page
        let mut right_entries = Vec::new();

        // The right page's leftmost child is the median's child
        let right_leftmost = median_child;

        // Collect keys and children after the median
        for i in (mid_idx + 1)..page.header.num_keys as usize {
            let node = page.node(i)?;
            let key = node.key()?.to_vec();
            let child = node.page_number()?;
            right_entries.push((key, child));
        }

        Ok((right_entries, median_key, right_leftmost))
    }

    /// Initialize a branch page from split data
    pub fn init_from_split(
        page: &mut Page,
        leftmost_child: PageId,
        entries: &[(Vec<u8>, PageId)],
    ) -> Result<()> {
        // Ensure it's a branch page
        page.header.flags = PageFlags::BRANCH;
        page.header.num_keys = 0;
        page.header.lower = PageHeader::SIZE as u16;
        page.header.upper = crate::page::PAGE_SIZE as u16;

        // Write branch header
        let header = BranchHeader { leftmost_child };
        unsafe {
            let header_ptr = page.data.as_mut_ptr() as *mut BranchHeader;
            *header_ptr = header;
        }

        // Adjust lower
        page.header.lower = page.header.lower
            .checked_add(BranchHeader::SIZE as u16)
            .ok_or_else(|| Error::Custom("Page lower bound overflow".into()))?;

        // Add all entries
        for (key, child) in entries {
            page.add_node(key, &child.0.to_le_bytes())?;
        }

        Ok(())
    }

    /// Update a child pointer for a given key
    pub fn update_child(page: &mut Page, key: &[u8], new_child: PageId) -> Result<()> {
        if !page.header.flags.contains(PageFlags::BRANCH) {
            return Err(Error::InvalidOperation("Not a branch page"));
        }

        // Find the key
        for i in 0..page.header.num_keys as usize {
            let node = page.node(i)?;
            if node.key()? == key {
                // Update the child pointer (stored as "value")
                let mut node_mut = page.node_data_mut(i)?;
                node_mut.set_value(&new_child.0.to_le_bytes())?;
                return Ok(());
            }
        }

        Err(Error::KeyNotFound)
    }

    /// Replace a key in a branch page (for rebalancing)
    pub fn replace_key(page: &mut Page, old_key: &[u8], new_key: &[u8]) -> Result<()> {
        if !page.header.flags.contains(PageFlags::BRANCH) {
            return Err(Error::InvalidOperation("Not a branch page"));
        }

        // Find the old key
        for i in 0..page.header.num_keys as usize {
            let node = page.node(i)?;
            if node.key()? == old_key {
                // If key sizes are the same, update in place
                if old_key.len() == new_key.len() {
                    // Get node offset
                    let ptr = page.ptrs()[i];
                    let node_offset = (ptr as usize)
                        .checked_sub(PageHeader::SIZE)
                        .and_then(|o| o.checked_add(NodeHeader::SIZE))
                        .ok_or_else(|| Error::Custom("Node offset calculation overflow".into()))?;

                    // Update key in place
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            new_key.as_ptr(),
                            page.data.as_mut_ptr().add(node_offset),
                            new_key.len(),
                        );
                    }
                    return Ok(());
                } else {
                    // Different sizes, need to remove and re-add
                    // Get the child pointer
                    let child = node.page_number()?;

                    // Remove the old entry
                    page.remove_node(i)?;

                    // Add the new entry with the same child
                    page.add_node_sorted(new_key, &child.0.to_le_bytes())?;
                    return Ok(());
                }
            }
        }

        Err(Error::KeyNotFound)
    }

    /// Update the leftmost child pointer
    pub fn update_leftmost_child(page: &mut Page, new_leftmost: PageId) -> Result<()> {
        if !page.header.flags.contains(PageFlags::BRANCH) {
            return Err(Error::InvalidOperation("Not a branch page"));
        }

        unsafe {
            let header_ptr = page.data.as_mut_ptr() as *mut BranchHeader;
            (*header_ptr).leftmost_child = new_leftmost;
        }

        Ok(())
    }

    /// Update a child pointer from old to new
    pub fn update_child_pointer(
        page: &mut Page,
        old_child: PageId,
        new_child: PageId,
    ) -> Result<()> {
        if !page.header.flags.contains(PageFlags::BRANCH) {
            return Err(Error::InvalidOperation("Not a branch page"));
        }

        // Check if it's the leftmost child
        let header = Self::get_header(page)?;
        if header.leftmost_child == old_child {
            return Self::update_leftmost_child(page, new_child);
        }

        // Search in the regular nodes
        for i in 0..page.header.num_keys as usize {
            // First read the current child ID
            let current_child = {
                let node = page.node(i)?;
                node.page_number()?
            };

            if current_child == old_child {
                // Now get mutable access and update
                let mut node = page.node_data_mut(i)?;
                node.set_value(&new_child.0.to_le_bytes())?;
                return Ok(());
            }
        }

        Err(Error::Custom(format!("Child pointer {:?} not found in branch page", old_child).into()))
    }
}
