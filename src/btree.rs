//! B+Tree implementation for database operations

use crate::comparator::{Comparator, LexicographicComparator};
use crate::error::{Error, PageId, Result};
use crate::meta::DbInfo;
use crate::page::{Page, PageFlags, PageHeader, SearchResult, PAGE_SIZE};
use crate::txn::{mode, Transaction, Write};
use std::borrow::Cow;
use std::marker::PhantomData;

/// Maximum number of keys per page (B+Tree order)
/// This is calculated based on page size and typical key/value sizes
pub const MAX_KEYS_PER_PAGE: usize = (PAGE_SIZE - PageHeader::SIZE) / 16;

/// Minimum number of keys per page (except root)
/// DEPRECATED: Use is_underflowed() function instead
pub const MIN_KEYS_PER_PAGE: usize = MAX_KEYS_PER_PAGE / 2;

/// LMDB-style fill threshold: 25% of page must be used
/// Pages below this threshold are candidates for merging
const FILL_THRESHOLD: usize = (PAGE_SIZE - PageHeader::SIZE) / 4;

/// Minimum keys for branch pages (must have at least 2 for tree structure)
const MIN_BRANCH_KEYS: usize = 2;

/// Minimum keys for leaf pages (can go down to 1)
const MIN_LEAF_KEYS: usize = 1;

/// Maximum depth allowed for B+Tree to prevent stack exhaustion
/// This is a safety limit - normal databases should never approach this depth
pub const MAX_TREE_DEPTH: usize = 100;

/// Check if a page is underflowed based on used space, not key count
fn is_underflowed(page: &Page) -> bool {
    // Root page is never considered underflowed
    if page.header.pgno == 3 {
        return false;
    }
    
    let used_space = PAGE_SIZE - page.header.free_space() - PageHeader::SIZE;
    let is_branch = page.header.flags.contains(PageFlags::BRANCH);
    
    // For branch pages, also check minimum key count
    if is_branch && page.header.num_keys < MIN_BRANCH_KEYS as u16 {
        return true;
    }
    
    // For leaf pages, check minimum key count
    if !is_branch && page.header.num_keys < MIN_LEAF_KEYS as u16 {
        return true;
    }
    
    // Check fill threshold
    used_space < FILL_THRESHOLD
}

/// B+Tree operations
pub struct BTree<C = LexicographicComparator> {
    _phantom: PhantomData<C>,
}

/// Helper function to get node value data (handles overflow transparently for splits/merges)
fn get_node_value_data(_page: &Page, node: &crate::page::Node) -> Result<Vec<u8>> {
    // Use the new raw_value_data method which handles both regular and overflow values
    node.raw_value_data()
}

impl<C: Comparator> Default for BTree<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: Comparator> BTree<C> {
    /// Create a new BTree instance
    pub fn new() -> Self {
        Self { _phantom: PhantomData }
    }

    /// Validate B+tree invariants for a given subtree
    /// Returns (min_key, max_key, depth) if valid
    pub fn validate_btree_invariants<M: mode::Mode>(
        txn: &Transaction<'_, M>,
        root: PageId,
    ) -> Result<(Option<Vec<u8>>, Option<Vec<u8>>, usize)> {
        Self::validate_subtree(txn, root, None, None, 0)
    }

    /// Recursively validate a subtree
    fn validate_subtree<M: mode::Mode>(
        txn: &Transaction<'_, M>,
        page_id: PageId,
        parent_min: Option<&[u8]>,
        parent_max: Option<&[u8]>,
        depth: usize,
    ) -> Result<(Option<Vec<u8>>, Option<Vec<u8>>, usize)> {
        // Check depth limit
        if depth > MAX_TREE_DEPTH {
            return Err(Error::Corruption {
                details: format!("Tree depth {} exceeds maximum allowed depth {}", depth, MAX_TREE_DEPTH),
                page_id: Some(page_id),
            });
        }

        let page = txn.get_page(page_id)?;
        
        // Validate page has been properly validated during read
        // (this is done by get_page automatically)

        // Check that page has proper type
        if !page.header.flags.contains(PageFlags::BRANCH) && !page.header.flags.contains(PageFlags::LEAF) {
            return Err(Error::Corruption {
                details: format!("Page {} is neither branch nor leaf", page_id.0),
                page_id: Some(page_id),
            });
        }

        let is_leaf = page.header.flags.contains(PageFlags::LEAF);
        
        // Empty pages are valid (newly created)
        if page.header.num_keys == 0 {
            return Ok((None, None, 1));
        }

        // Get first and last keys
        let first_node = page.node(0)?;
        let first_key = first_node.key()?;
        let last_node = page.node(page.header.num_keys as usize - 1)?;
        let last_key = last_node.key()?;

        // Check key ordering within page
        let mut prev_key = first_key;
        for i in 1..page.header.num_keys as usize {
            let node = page.node(i)?;
            let key = node.key()?;
            
            if C::compare(prev_key, key) != std::cmp::Ordering::Less {
                return Err(Error::Corruption {
                    details: format!("Keys not in sorted order at index {} in page {}", i, page_id.0),
                    page_id: Some(page_id),
                });
            }
            prev_key = key;
        }

        // Check bounds from parent
        if let Some(min) = parent_min {
            if C::compare(first_key, min) != std::cmp::Ordering::Greater 
                && C::compare(first_key, min) != std::cmp::Ordering::Equal {
                return Err(Error::Corruption {
                    details: format!("First key in page {} violates parent minimum bound", page_id.0),
                    page_id: Some(page_id),
                });
            }
        }

        if let Some(max) = parent_max {
            if C::compare(last_key, max) != std::cmp::Ordering::Less {
                return Err(Error::Corruption {
                    details: format!("Last key in page {} violates parent maximum bound", page_id.0),
                    page_id: Some(page_id),
                });
            }
        }

        if is_leaf {
            // Leaf page - check values
            for i in 0..page.header.num_keys as usize {
                let node = page.node(i)?;
                // Validate overflow pages if present
                if let Some(overflow_id) = node.overflow_page()? {
                    // Ensure overflow page ID is valid
                    let inner = txn.data.env.inner();
                    let num_pages = inner.io.size_in_pages();
                    if overflow_id.0 >= num_pages {
                        return Err(Error::Corruption {
                            details: format!("Invalid overflow page ID {} in leaf node", overflow_id.0),
                            page_id: Some(page_id),
                        });
                    }
                }
            }
            Ok((Some(first_key.to_vec()), Some(last_key.to_vec()), 1))
        } else {
            // Branch page - recursively validate children
            let mut max_child_depth = 0;
            let mut first_child_depth = None;

            // Get the leftmost child
            let leftmost_child = crate::branch::BranchPage::get_leftmost_child(page)?;

            // Validate leftmost child with its bounds
            if page.header.num_keys > 0 {
                let first_node = page.node(0)?;
                let first_key = first_node.key()?;
                
                let (_, _, child_depth) = Self::validate_subtree(
                    txn,
                    leftmost_child,
                    parent_min,
                    Some(first_key),
                    depth + 1,
                )?;
                
                first_child_depth = Some(child_depth);
                max_child_depth = child_depth;
            }

            // Validate each child with appropriate bounds
            for i in 0..page.header.num_keys as usize {
                let node = page.node(i)?;
                let key = node.key()?;
                
                // Get child page ID (stored as value in branch nodes)
                let child_id = node.page_number()?;

                // Determine bounds for this child
                let child_min = Some(key);
                let child_max = if i + 1 < page.header.num_keys as usize {
                    let next_node = page.node(i + 1)?;
                    Some(next_node.key()?)
                } else {
                    parent_max
                };

                // Recursively validate child
                let (_, _, child_depth) = Self::validate_subtree(
                    txn,
                    child_id,
                    child_min,
                    child_max,
                    depth + 1,
                )?;

                // Check that all children have same depth
                if let Some(first_depth) = first_child_depth {
                    if child_depth != first_depth {
                        return Err(Error::Corruption {
                            details: format!("Inconsistent child depths in branch page {}: {} vs {}", 
                                           page_id.0, first_depth, child_depth),
                            page_id: Some(page_id),
                        });
                    }
                } else {
                    first_child_depth = Some(child_depth);
                }

                max_child_depth = max_child_depth.max(child_depth);
            }

            Ok((Some(first_key.to_vec()), Some(last_key.to_vec()), max_child_depth + 1))
        }
    }

    /// Insert with append mode optimization
    /// This is much faster for sequential inserts
    pub fn insert_append(
        txn: &mut Transaction<'_, Write>,
        db_info: &mut DbInfo,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        // Check if we can use append optimization
        // This requires that the key is greater than all existing keys
        let can_append = if db_info.last_key_page != PageId(0) {
            // We have a cached last page, check if key is greater
            let last_page = txn.get_page(db_info.last_key_page)?;
            if last_page.header.num_keys > 0 {
                let last_idx = last_page.header.num_keys as usize - 1;
                let last_node = last_page.node(last_idx)?;
                let last_key = last_node.key()?;
                C::compare(key, last_key) == std::cmp::Ordering::Greater
            } else {
                true
            }
        } else {
            // No cached page, need to find the rightmost leaf
            false
        };

        if can_append {
            // Fast path: append to the last page
            let last_page_id = db_info.last_key_page;
            let page = txn.get_page_mut(last_page_id)?;

            // Try to append
            match page.add_node(key, value) {
                Ok(_) => {
                    db_info.entries += 1;
                    return Ok(());
                }
                Err(Error::Custom(msg)) if msg.contains("Page full") => {
                    // Need to split, but we know this key goes to the new right page
                    let split_result = Self::split_leaf_page_append(txn, last_page_id, key, value)?;
                    if let InsertResult::Split { right_page, .. } = split_result {
                        // Update cached last page
                        db_info.last_key_page = right_page;
                        db_info.entries += 1;
                    }
                    return Ok(());
                }
                Err(e) => return Err(e),
            }
        }

        // Fall back to regular insert
        let mut root = db_info.root;
        let _result = Self::insert(txn, &mut root, db_info, key, value)?;
        db_info.root = root;

        // The insert function already handles updating entries count and root splitting
        Ok(())
    }

    /// Split a leaf page optimized for append mode
    fn split_leaf_page_append(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        new_key: &[u8],
        new_value: &[u8],
    ) -> Result<InsertResult> {
        // For append mode, we know the new key goes to the right page
        // So we can optimize the split by moving fewer entries
        let page = txn.get_page(page_id)?;
        let num_keys = page.header.num_keys as usize;

        // Split point: keep most entries in left page since new entries will go right
        let split_point = (num_keys * 3) / 4; // Keep 75% in left page

        // Get the median key
        let median_node = page.node(split_point)?;
        let median_key = median_node.key()?.to_vec();

        // Collect nodes for right page
        let mut right_nodes = Vec::new();
        for i in split_point..num_keys {
            let node = page.node(i)?;
            right_nodes.push((node.key()?.to_vec(), get_node_value_data(page, &node)?));
        }

        // Store original next page before allocation
        let original_next = page.header.next_pgno;

        // Allocate new right page
        let (right_page_id, right_page) = txn.alloc_page(PageFlags::LEAF)?;

        // Update leaf chaining
        right_page.header.prev_pgno = page_id.0;
        right_page.header.next_pgno = original_next;

        // Add nodes to right page
        for (key, value) in &right_nodes {
            right_page.add_node_sorted(key, value)?;
        }

        // Add the new key to right page
        right_page.add_node_sorted(new_key, new_value)?;

        // Update left page
        let left_page = txn.get_page_mut(page_id)?;
        left_page.truncate(split_point);
        left_page.header.next_pgno = right_page_id.0;

        // Update next page's prev pointer if exists
        if original_next != 0 {
            let next_page = txn.get_page_mut(PageId(original_next))?;
            next_page.header.prev_pgno = right_page_id.0;
        }

        Ok(InsertResult::Split { median_key, right_page: right_page_id })
    }
    /// Search for a key in the B+Tree
    #[inline]
    pub fn search<'txn>(
        txn: &'txn Transaction<'txn, impl crate::txn::mode::Mode>,
        root: PageId,
        key: &[u8],
    ) -> Result<Option<Cow<'txn, [u8]>>> {
        let mut current_page_id = root;
        let mut depth = 0;

        loop {
            // Check depth limit to prevent stack exhaustion
            depth += 1;
            if depth > MAX_TREE_DEPTH {
                return Err(Error::Corruption {
                    details: format!("Tree depth exceeds maximum allowed depth ({})", MAX_TREE_DEPTH),
                    page_id: Some(current_page_id),
                });
            }
            let page = txn.get_page(current_page_id)?;

            // Handle empty pages (newly created)
            if page.header.num_keys == 0 && page.header.flags.contains(PageFlags::LEAF) {
                return Ok(None);
            }

            match page.search_key_with_comparator::<C>(key)? {
                SearchResult::Found { index } => {
                    let node = page.node(index)?;

                    if page.header.flags.contains(PageFlags::LEAF) {
                        // Found in leaf page
                        // Check if value is in overflow pages
                        if let Some(overflow_id) = node.overflow_page()? {
                            // Read from overflow pages using LMDB-style
                            // Get the actual value size from the node header
                            let value_size = node.header.value_size();
                            let value = crate::overflow::read_overflow_value_lmdb(txn, overflow_id, None, Some(value_size))?;
                            return Ok(Some(Cow::Owned(value)));
                        } else {
                            // Regular value
                            return Ok(Some(node.value()?));
                        }
                    } else {
                        // In branch page, follow the child pointer
                        current_page_id =
                            crate::branch::BranchPage::find_child_with_comparator::<C>(page, key)?;
                    }
                }
                SearchResult::NotFound { insert_pos: _ } => {
                    if page.header.flags.contains(PageFlags::LEAF) {
                        // Not found in leaf
                        return Ok(None);
                    } else {
                        // In branch page, use the branch helper
                        current_page_id =
                            crate::branch::BranchPage::find_child_with_comparator::<C>(page, key)?;
                    }
                }
            }
        }
    }

    /// Update value for an existing key
    pub fn update_value(
        txn: &mut Transaction<'_, Write>,
        root: PageId,
        key: &[u8],
        new_value: &[u8],
    ) -> Result<()> {
        let mut current_page_id = root;

        loop {
            let page = txn.get_page_mut(current_page_id)?;

            match page.search_key_with_comparator::<C>(key)? {
                SearchResult::Found { index } => {
                    if page.header.flags.contains(PageFlags::LEAF) {
                        // Found in leaf page - update the value
                        let mut node_data = page.node_data_mut(index)?;

                        // Check if we need overflow pages for the new value
                        let max_value_size = crate::page::MAX_VALUE_SIZE;
                        if new_value.len() > max_value_size {
                            // Need overflow pages
                            // Drop the mutable borrow of page before calling write_overflow_value
                            let _ = node_data;
                            let _ = page;

                            let (overflow_id, _) =
                                crate::overflow::write_overflow_value_lmdb(txn, new_value)?;

                            // Re-acquire the page and node
                            let page = txn.get_page_mut(current_page_id)?;
                            let mut node_data = page.node_data_mut(index)?;
                            node_data.set_overflow(overflow_id)?;
                        } else {
                            // Regular value
                            node_data.set_value(new_value)?;
                        }

                        return Ok(());
                    } else {
                        // In branch page, follow the child
                        current_page_id =
                            crate::branch::BranchPage::find_child_with_comparator::<C>(page, key)?;
                    }
                }
                SearchResult::NotFound { insert_pos: _ } => {
                    if page.header.flags.contains(PageFlags::LEAF) {
                        // Key not found in leaf
                        return Err(Error::KeyNotFound);
                    } else {
                        // In branch page, use the branch helper
                        current_page_id =
                            crate::branch::BranchPage::find_child_with_comparator::<C>(page, key)?;
                    }
                }
            }
        }
    }

    /// Insert a key-value pair into the B+Tree with Copy-on-Write
    pub fn insert(
        txn: &mut Transaction<'_, Write>,
        root: &mut PageId,
        db_info: &mut DbInfo,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        // Start insertion from root with COW
        let (new_root, result) = Self::insert_cow(txn, *root, key, value)?;
        *root = new_root;

        match result {
            InsertResult::Updated(old_value) => Ok(old_value),
            InsertResult::Inserted => {
                db_info.entries += 1;
                Ok(None)
            }
            InsertResult::Split { median_key, right_page } => {
                // Root was split, create new root
                let (new_root_id, new_root) = txn.alloc_page(PageFlags::BRANCH)?;

                // Debug: Check the page IDs
                if root.0 == 0 || right_page.0 == 0 {
                    return Err(Error::Corruption {
                        details: format!(
                            "Invalid page IDs during split: old_root={:?}, right_page={:?}",
                            root, right_page
                        ),
                        page_id: Some(*root),
                    });
                }

                // Initialize the new root with the split information
                crate::branch::BranchPage::init_root(
                    new_root,
                    &median_key,
                    *root,      // left child (old root)
                    right_page, // right child (new page)
                )?;

                *root = new_root_id;
                db_info.depth += 1;
                db_info.branch_pages += 1;
                db_info.entries += 1;

                Ok(None)
            }
        }
    }

    /// Insert with Copy-on-Write - returns new page ID and result
    fn insert_cow(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<(PageId, InsertResult)> {
        Self::insert_cow_with_depth(txn, page_id, key, value, 0)
    }

    /// Insert with Copy-on-Write with depth tracking - returns new page ID and result
    fn insert_cow_with_depth(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
        depth: usize,
    ) -> Result<(PageId, InsertResult)> {
        // Check depth limit to prevent stack exhaustion
        if depth > MAX_TREE_DEPTH {
            return Err(Error::Corruption {
                details: format!("Tree depth exceeds maximum allowed depth ({}) during insert", MAX_TREE_DEPTH),
                page_id: Some(page_id),
            });
        }

        let page = txn.get_page(page_id)?;

        if page.header.flags.contains(PageFlags::LEAF) {
            // Insert into leaf page with COW
            Self::insert_into_leaf_cow(txn, page_id, key, value)
        } else {
            // Insert into branch page with COW
            Self::insert_into_branch_cow_with_depth(txn, page_id, key, value, depth)
        }
    }

    /// Insert into a non-full page
    #[allow(dead_code)]
    fn insert_non_full(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<InsertResult> {
        let page = txn.get_page(page_id)?;

        if page.header.flags.contains(PageFlags::LEAF) {
            // Insert into leaf page
            Self::insert_into_leaf(txn, page_id, key, value)
        } else {
            // Insert into branch page
            Self::insert_into_branch(txn, page_id, key, value)
        }
    }

    /// Insert into a leaf page
    #[allow(dead_code)]
    fn insert_into_leaf(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<InsertResult> {
        // Check if value needs overflow pages
        let needs_overflow = crate::overflow::needs_overflow(key.len(), value.len());

        // Check if key already exists
        let search_result = {
            let page = txn.get_page(page_id)?;
            page.search_key_with_comparator::<C>(key)?
        };

        match search_result {
            SearchResult::Found { index } => {
                // Key exists, update value
                // Try to get old value (might be in overflow pages)
                let (old_value, overflow_page_to_free) = {
                    let page = txn.get_page(page_id)?;
                    let node = page.node(index)?;

                    if let Some(overflow_id) = node.overflow_page()? {
                        // Read from overflow pages using LMDB-style
                        let overflow_page = txn.get_page(overflow_id)?;
                        let overflow_count = overflow_page.header.overflow;
                        let value_size = node.header.value_size();
                        (
                            Some(crate::overflow::read_overflow_value_lmdb(txn, overflow_id, Some(overflow_count), Some(value_size))?),
                            Some((overflow_id, overflow_count)),
                        )
                    } else {
                        (node.value().ok().map(|v| v.into_owned()), None)
                    }
                };

                // For value size changes, we need to delete and re-insert
                // First delete the old entry
                {
                    let page = txn.get_page_mut(page_id)?;
                    page.remove_node(index)?;
                }

                // Free overflow pages if any (LMDB-style)
                if let Some((overflow_id, overflow_count)) = overflow_page_to_free {
                    crate::overflow::free_overflow_chain_lmdb(txn, overflow_id, overflow_count)?;
                }

                // Now insert the new value
                Self::insert_into_leaf(txn, page_id, key, value).map(
                    |result| match result {
                        InsertResult::Inserted => InsertResult::Updated(old_value),
                        other => other,
                    },
                )
            }
            SearchResult::NotFound { insert_pos: _ } => {
                // Check if we need to split pre-emptively
                let needs_split = {
                    let page = txn.get_page(page_id)?;
                    let required_size = if needs_overflow {
                        crate::page::NodeHeader::SIZE
                            .checked_add(key.len())
                            .and_then(|s| s.checked_add(8))
                            .and_then(|s| s.checked_add(size_of::<u16>()))
                            .ok_or_else(|| Error::Custom("Node size calculation overflow".into()))?
                    } else {
                        crate::page::NodeHeader::SIZE
                            .checked_add(key.len())
                            .and_then(|s| s.checked_add(value.len()))
                            .and_then(|s| s.checked_add(size_of::<u16>()))
                            .ok_or_else(|| Error::Custom("Node size calculation overflow".into()))?
                    };
                    // Use should_split for pre-emptive splitting at 85% utilization
                    page.should_split(Some(required_size))
                };

                if needs_split {
                    // Page should be split pre-emptively
                    if needs_overflow {
                        let (overflow_id, _) = crate::overflow::write_overflow_value_lmdb(txn, value)?;
                        Self::split_leaf_page_with_overflow(txn, page_id, key, overflow_id, value.len())
                    } else {
                        Self::split_leaf_page(txn, page_id, key, value)
                    }
                } else if needs_overflow {
                    // Page has room, insert with overflow
                    let (overflow_id, _) = crate::overflow::write_overflow_value_lmdb(txn, value)?;
                    let page = txn.get_page_mut(page_id)?;
                    match page.add_node_sorted_overflow_with_size(key, overflow_id, value.len()) {
                        Ok(_) => Ok(InsertResult::Inserted),
                        Err(Error::Custom(msg)) if msg.contains("Page full") => {
                            // Fallback: page became full despite pre-check
                            Self::split_leaf_page_with_overflow(txn, page_id, key, overflow_id, value.len())
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    // Page has room, insert normally
                    let page = txn.get_page_mut(page_id)?;
                    match page.add_node_sorted(key, value) {
                        Ok(_) => Ok(InsertResult::Inserted),
                        Err(Error::Custom(msg)) if msg.contains("Page full") => {
                            // Fallback: page became full despite pre-check
                            Self::split_leaf_page(txn, page_id, key, value)
                        }
                        Err(e) => Err(e),
                    }
                }
            }
        }
    }

    /// Insert into a branch page
    #[allow(dead_code)]
    fn insert_into_branch(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<InsertResult> {
        let page = txn.get_page(page_id)?;

        // Ensure this is actually a branch page
        if !page.header.flags.contains(PageFlags::BRANCH) {
            return Err(Error::Corruption {
                details: format!("Expected branch page but got {:?}", page.header.flags),
                page_id: Some(page_id),
            });
        }

        // Find child to insert into using the branch page logic
        let child_page_id = crate::branch::BranchPage::find_child_with_comparator::<C>(page, key)?;

        // Sanity check: child page ID should never be 0
        if child_page_id.0 == 0 {
            return Err(Error::Corruption {
                details: "Branch page returned invalid child page ID 0".to_string(),
                page_id: Some(page_id),
            });
        }

        // Recursively insert into child
        let child_result = Self::insert_non_full(txn, child_page_id, key, value)?;

        match child_result {
            InsertResult::Updated(old) => Ok(InsertResult::Updated(old)),
            InsertResult::Inserted => Ok(InsertResult::Inserted),
            InsertResult::Split { median_key, right_page } => {
                // Child was split, add median key to this branch
                let page = txn.get_page_mut(page_id)?;

                // Use branch_v2 to add the split child
                match crate::branch::BranchPage::add_split_child(page, &median_key, right_page) {
                    Ok(()) => Ok(InsertResult::Inserted),
                    Err(Error::Custom(msg)) if msg.contains("Page full") => {
                        // This branch is also full, split it
                        Self::split_branch_page(txn, page_id, median_key, right_page)
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Split a leaf page
    fn split_leaf_page(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        new_key: &[u8],
        new_value: &[u8],
    ) -> Result<InsertResult> {
        // Get the page to split
        let page = txn.get_page(page_id)?;

        // Save the original next page for leaf chaining
        let original_next = page.header.next_pgno;

        // Get the nodes that will go to the right page
        let (right_nodes, median_key) = page.split()?;

        // Allocate new right page
        let (right_page_id, right_page) = txn.alloc_page(PageFlags::LEAF)?;

        // Add all nodes to the right page
        for (key, value) in &right_nodes {
            right_page.add_node_sorted(key, value)?;
        }

        // Update leaf chaining pointers: left -> right -> original_next
        {
            // Update right page pointers
            right_page.header.prev_pgno = page_id.0;
            right_page.header.next_pgno = original_next;

            // If there was an original next page, update its prev pointer
            if original_next != 0 {
                let next_page = txn.get_page_mut(PageId(original_next))?;
                next_page.header.prev_pgno = right_page_id.0;
            }
        }

        // Truncate the left page
        let left_page = txn.get_page_mut(page_id)?;
        let mid_idx = left_page.header.num_keys as usize / 2;
        left_page.truncate(mid_idx);

        // Update left page's next pointer
        left_page.header.next_pgno = right_page_id.0;

        // Determine which page to insert the new key into
        if new_key < median_key.as_slice() {
            // Insert into left page
            left_page.add_node_sorted(new_key, new_value)?;
        } else {
            // Insert into right page
            let right_page = txn.get_page_mut(right_page_id)?;
            right_page.add_node_sorted(new_key, new_value)?;
        }

        Ok(InsertResult::Split { median_key, right_page: right_page_id })
    }

    /// Split a branch page
    fn split_branch_page(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        new_key: Vec<u8>,
        new_page: PageId,
    ) -> Result<InsertResult> {
        // Instead of splitting first and then inserting (which might fail),
        // collect all entries including the new one and do a proper balanced split
        
        let page = txn.get_page(page_id)?;
        let leftmost_child = crate::branch::BranchPage::get_leftmost_child(page)?;
        
        // Collect all existing entries
        let mut all_entries = Vec::new();
        for i in 0..page.header.num_keys as usize {
            let node = page.node(i)?;
            all_entries.push((node.key()?.to_vec(), node.page_number()?));
        }
        
        // Add the new entry and sort
        all_entries.push((new_key, new_page));
        all_entries.sort_by(|a, b| a.0.cmp(&b.0));
        
        // Calculate split point - ensure both sides have room to grow
        let total_entries = all_entries.len();
        let split_point = total_entries / 2;
        
        // The entry at split_point will be promoted as median
        let median_key = all_entries[split_point].0.clone();
        let median_child = all_entries[split_point].1;
        
        // Allocate new right page
        let (right_page_id, _) = txn.alloc_page(PageFlags::BRANCH)?;
        
        // Clear and rebuild left page with entries before split point
        {
            let left_page = txn.get_page_mut(page_id)?;
            left_page.clear();
            left_page.header.flags = PageFlags::BRANCH;
            
            // Initialize left page with entries [0..split_point)
            crate::branch::BranchPage::init_from_split(
                left_page,
                leftmost_child,
                &all_entries[..split_point]
            )?;
        }
        
        // Initialize right page with entries (split_point+1..]
        // The median's child becomes the leftmost child of the right page
        {
            let right_page = txn.get_page_mut(right_page_id)?;
            crate::branch::BranchPage::init_from_split(
                right_page,
                median_child,
                &all_entries[(split_point + 1)..]
            )?;
        }
        
        Ok(InsertResult::Split { median_key, right_page: right_page_id })
    }

    /// Split a leaf page with an overflow value
    fn split_leaf_page_with_overflow(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        new_key: &[u8],
        overflow_page_id: PageId,
        value_size: usize,
    ) -> Result<InsertResult> {
        // Get the page to split
        let page = txn.get_page(page_id)?;

        // Get the nodes that will go to the right page
        let (right_nodes, median_key) = page.split()?;

        // Allocate new right page
        let (right_page_id, right_page) = txn.alloc_page(PageFlags::LEAF)?;

        // Add all nodes to the right page
        for (key, value) in &right_nodes {
            right_page.add_node_sorted(key, value)?;
        }

        // Truncate the left page
        let left_page = txn.get_page_mut(page_id)?;
        let mid_idx = left_page.header.num_keys as usize / 2;
        left_page.truncate(mid_idx);

        // Determine which page to insert the new key into
        if new_key < median_key.as_slice() {
            // Insert into left page
            left_page.add_node_sorted_overflow_with_size(new_key, overflow_page_id, value_size)?;
        } else {
            // Insert into right page
            let right_page = txn.get_page_mut(right_page_id)?;
            right_page.add_node_sorted_overflow_with_size(new_key, overflow_page_id, value_size)?;
        }

        Ok(InsertResult::Split { median_key, right_page: right_page_id })
    }

    /// Delete a key from the B+Tree
    pub fn delete(
        txn: &mut Transaction<'_, Write>,
        root: &mut PageId,
        db_info: &mut DbInfo,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        // Start deletion from root
        let (new_root_id, result) = Self::delete_from_node(txn, *root, key)?;
        
        // Update root if it changed due to COW
        if new_root_id != *root {
            *root = new_root_id;
        }

        match result {
            DeleteResult::NotFound => Ok(None),
            DeleteResult::Deleted(old_value) => {
                db_info.entries = db_info.entries.saturating_sub(1);
                Ok(Some(old_value))
            }
            DeleteResult::Underflow { old_value } => {
                // Root underflowed, need to handle
                let root_page = txn.get_page(*root)?;

                if root_page.header.flags.contains(PageFlags::BRANCH)
                    && root_page.header.num_keys == 0
                {
                    // Root is empty branch, make its only child the new root
                    if let Ok(node) = root_page.node(0) {
                        *root = node.page_number()?;
                        db_info.depth = db_info.depth.saturating_sub(1);
                        db_info.branch_pages = db_info.branch_pages.saturating_sub(1);

                        // Free the old root
                        txn.free_page(PageId(root_page.header.pgno))?;
                    }
                }

                db_info.entries = db_info.entries.saturating_sub(1);
                Ok(Some(old_value))
            }
        }
    }

    /// Delete from a node
    fn delete_from_node(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
    ) -> Result<(PageId, DeleteResult)> {
        Self::delete_from_node_with_depth(txn, page_id, key, 0)
    }

    fn delete_from_node_with_depth(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
        depth: usize,
    ) -> Result<(PageId, DeleteResult)> {
        // Check depth limit to prevent stack exhaustion
        if depth > MAX_TREE_DEPTH {
            return Err(Error::Corruption {
                details: format!("Tree depth exceeds maximum allowed depth ({}) during delete", MAX_TREE_DEPTH),
                page_id: Some(page_id),
            });
        }

        let page = txn.get_page(page_id)?;

        if page.header.flags.contains(PageFlags::LEAF) {
            // Delete from leaf
            Self::delete_from_leaf(txn, page_id, key)
        } else {
            // Delete from branch
            Self::delete_from_branch_with_depth(txn, page_id, key, depth)
        }
    }

    /// Delete from a leaf page
    fn delete_from_leaf(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
    ) -> Result<(PageId, DeleteResult)> {
        // First, search for the key and get node info
        let (search_result, _num_keys, _pgno) = {
            let page = txn.get_page(page_id)?;
            (page.search_key_with_comparator::<C>(key)?, page.header.num_keys, page.header.pgno)
        };

        match search_result {
            SearchResult::Found { index } => {
                // Get the old value before deletion
                let (old_value, overflow_page_to_free) = {
                    let page = txn.get_page(page_id)?;
                    let node = page.node(index)?;

                    // Handle overflow values
                    if let Some(overflow_id) = node.overflow_page()? {
                        // Read value from overflow pages - get overflow count from page
                        let overflow_page = txn.get_page(overflow_id)?;
                        let overflow_count = overflow_page.header.overflow;
                        let value_size = node.header.value_size();
                        let value = crate::overflow::read_overflow_value_lmdb(txn, overflow_id, Some(overflow_count), Some(value_size))?;
                        (value, Some((overflow_id, overflow_count)))
                    } else {
                        // Regular value
                        (node.value()?.into_owned(), None)
                    }
                };

                // Remove the node using COW
                let new_page_id = {
                    let (new_id, page) = txn.get_page_cow(page_id)?;
                    page.remove_node(index)?;
                    new_id
                };

                // Free overflow pages if any (LMDB-style)
                if let Some((overflow_id, overflow_count)) = overflow_page_to_free {
                    crate::overflow::free_overflow_chain_lmdb(txn, overflow_id, overflow_count)?;
                }

                // Check if underflow occurred
                let underflowed = {
                    let page = txn.get_page(new_page_id)?;
                    is_underflowed(page)
                };

                if underflowed {
                    Ok((new_page_id, DeleteResult::Underflow { old_value }))
                } else {
                    Ok((new_page_id, DeleteResult::Deleted(old_value)))
                }
            }
            SearchResult::NotFound { .. } => Ok((page_id, DeleteResult::NotFound)),
        }
    }

    /// Delete from a branch page
    #[allow(dead_code)]
    fn delete_from_branch(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
    ) -> Result<(PageId, DeleteResult)> {
        Self::delete_from_branch_with_depth(txn, page_id, key, 0)
    }

    fn delete_from_branch_with_depth(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
        depth: usize,
    ) -> Result<(PageId, DeleteResult)> {
        let page = txn.get_page(page_id)?;

        // Find child to delete from using branch_v2 logic
        let child_page_id = crate::branch::BranchPage::find_child_with_comparator::<C>(page, key)?;

        // We need to track which child index we're using for rebalancing
        // This is a bit tricky with branch_v2's structure
        let child_index = match page.search_key_with_comparator::<C>(key)? {
            SearchResult::Found { index } => index,
            SearchResult::NotFound { insert_pos } => {
                // For branch_v2, if insert_pos is 0, we're going to leftmost child
                // We'll use usize::MAX to represent the leftmost child index
                if insert_pos == 0 {
                    usize::MAX
                } else {
                    insert_pos - 1
                }
            }
        };

        // Recursively delete from child
        let (new_child_id, child_result) = Self::delete_from_node_with_depth(txn, child_page_id, key, depth + 1)?;
        
        // If child page changed due to COW, update parent's reference
        let mut current_page_id = page_id;
        if new_child_id != child_page_id {
            let (new_parent_id, parent) = txn.get_page_cow(page_id)?;
            // Update the child pointer in parent
            crate::branch::BranchPage::update_child_pointer(
                parent,
                child_page_id,
                new_child_id,
            )?;
            current_page_id = new_parent_id;
        }

        match child_result {
            DeleteResult::NotFound => Ok((current_page_id, DeleteResult::NotFound)),
            DeleteResult::Deleted(old_value) => Ok((current_page_id, DeleteResult::Deleted(old_value))),
            DeleteResult::Underflow { old_value } => {
                // Child underflowed, need to rebalance
                Self::rebalance_child(txn, current_page_id, child_index)?;

                // Check if this page also underflowed
                let page = txn.get_page(current_page_id)?;
                if is_underflowed(page) {
                    Ok((current_page_id, DeleteResult::Underflow { old_value }))
                } else {
                    Ok((current_page_id, DeleteResult::Deleted(old_value)))
                }
            }
        }
    }

    /// Rebalance a child that has underflowed
    fn rebalance_child(
        txn: &mut Transaction<'_, Write>,
        parent_id: PageId,
        child_index: usize,
    ) -> Result<()> {
        // Handle branch_v2 structure
        let parent = txn.get_page(parent_id)?;
        if !parent.header.flags.contains(PageFlags::BRANCH) {
            return Err(Error::InvalidOperation("Parent must be a branch page"));
        }

        // Get necessary information from parent
        let (child_id, left_sibling_id, right_sibling_id, _parent_num_keys) =
            if child_index == usize::MAX {
                // Leftmost child
                let child_id = crate::branch::BranchPage::get_leftmost_child(parent)?;
                let right_sibling_id = if parent.header.num_keys > 0 {
                    Some(parent.node(0)?.page_number()?)
                } else {
                    None
                };
                (child_id, None, right_sibling_id, parent.header.num_keys)
            } else {
                // Regular child
                let child_id = parent.node(child_index)?.page_number()?;

                let left_sibling_id = if child_index == 0 {
                    // The left sibling of the first key's right child is the leftmost child
                    Some(crate::branch::BranchPage::get_leftmost_child(parent)?)
                } else {
                    Some(parent.node(child_index - 1)?.page_number()?)
                };

                let right_sibling_id = if child_index < parent.header.num_keys as usize - 1 {
                    Some(parent.node(child_index + 1)?.page_number()?)
                } else {
                    None
                };

                (child_id, left_sibling_id, right_sibling_id, parent.header.num_keys)
            };

        // Handle leftmost child rebalancing
        if child_index == usize::MAX {
            // Leftmost child can only borrow from or merge with its right sibling
            if let Some(right_sibling_id) = right_sibling_id {
                // Try to borrow from right sibling
                if Self::try_borrow_from_leftmost_to_right(
                    txn,
                    parent_id,
                    child_id,
                    right_sibling_id,
                )? {
                    return Ok(());
                }

                // If can't borrow, merge with right sibling
                Self::merge_leftmost_with_right(txn, parent_id, child_id, right_sibling_id)?;
            }
            return Ok(());
        }

        // Try to borrow from left sibling
        if let Some(left_sibling_id) = left_sibling_id {
            if Self::try_borrow_from_left(txn, parent_id, child_index, left_sibling_id, child_id)? {
                return Ok(());
            }
        }

        // Try to borrow from right sibling
        if let Some(right_sibling_id) = right_sibling_id {
            if Self::try_borrow_from_right(txn, parent_id, child_index, child_id, right_sibling_id)?
            {
                return Ok(());
            }
        }

        // Can't borrow, must merge
        if let Some(left_sibling_id) = left_sibling_id {
            // For child at index 0, merge leftmost child with it
            if child_index == 0 {
                Self::merge_leftmost_with_right(txn, parent_id, left_sibling_id, child_id)?;
                return Ok(());
            }
            // Merge with left sibling
            Self::merge_nodes(txn, parent_id, child_index - 1, left_sibling_id, child_id)?;
        } else if let Some(right_sibling_id) = right_sibling_id {
            // Merge with right sibling
            Self::merge_nodes(txn, parent_id, child_index, child_id, right_sibling_id)?;
        }

        Ok(())
    }

    /// Try to borrow a key from left sibling
    fn try_borrow_from_left(
        txn: &mut Transaction<'_, Write>,
        parent_id: PageId,
        child_index: usize,
        left_sibling_id: PageId,
        child_id: PageId,
    ) -> Result<bool> {
        // Check if left sibling can share without becoming underflowed
        let can_borrow = {
            let left_sibling = txn.get_page(left_sibling_id)?;
            // After removing one key, would the sibling still be above threshold?
            let is_branch = left_sibling.header.flags.contains(PageFlags::BRANCH);
            let min_keys = if is_branch { MIN_BRANCH_KEYS } else { MIN_LEAF_KEYS };
            
            // Check both key count and space requirements
            if left_sibling.header.num_keys as usize <= min_keys {
                false
            } else {
                // Estimate if sibling would still be above fill threshold after giving one key
                let used_space = PAGE_SIZE - left_sibling.header.free_space() - PageHeader::SIZE;
                used_space > FILL_THRESHOLD * 2  // Conservative check
            }
        };
        
        if !can_borrow {
            return Ok(false);
        }

        // Get the separator key from parent
        let separator_key = {
            let parent = txn.get_page(parent_id)?;
            parent.node(child_index - 1)?.key()?.to_vec()
        };

        // Get the rightmost node from left sibling
        let (borrowed_key, borrowed_value, borrowed_child, is_leaf) = {
            let left_sibling = txn.get_page(left_sibling_id)?;
            let last_idx = left_sibling.header.num_keys as usize - 1;
            let node = left_sibling.node(last_idx)?;
            let is_leaf = left_sibling.header.flags.contains(PageFlags::LEAF);
            if is_leaf {
                (node.key()?.to_vec(), get_node_value_data(left_sibling, &node)?, None, true)
            } else {
                (
                    node.key()?.to_vec(),
                    vec![], // For branch pages, we don't need the value
                    Some(node.page_number()?),
                    false,
                )
            }
        };

        // Remove the rightmost node from left sibling using COW
        let _new_left_sibling_id = {
            let (new_id, left_sibling) = txn.get_page_cow(left_sibling_id)?;
            left_sibling.remove_node(left_sibling.header.num_keys as usize - 1)?;
            new_id
        };

        // Insert into child using COW
        if is_leaf {
            // Check if child has space for the borrowed key/value
            let node_size = crate::page::NodeHeader::SIZE
                .checked_add(borrowed_key.len())
                .and_then(|s| s.checked_add(borrowed_value.len()))
                .ok_or_else(|| Error::Custom("Node size calculation overflow".into()))?;
            let required_space = node_size
                .checked_add(std::mem::size_of::<u16>())
                .ok_or_else(|| Error::Custom("Required space calculation overflow".into()))?;
            
            {
                let child_page = txn.get_page(child_id)?;
                if child_page.header.free_space() < required_space {
                    // Not enough space to borrow
                    return Ok(false);
                }
            }
            
            // For leaf nodes, the borrowed key goes directly to child
            let (_, child) = txn.get_page_cow(child_id)?;
            // Insert at the beginning
            child.add_node_sorted(&borrowed_key, &borrowed_value)?;

            // Update separator in parent to be the new first key of child
            let (_, parent) = txn.get_page_cow(parent_id)?;
            crate::branch::BranchPage::replace_key(parent, &separator_key, &borrowed_key)?
        } else {
            // For branch nodes, we need to handle the child pointers carefully
            let (_, child) = txn.get_page_cow(child_id)?;

            // The borrowed child becomes the new leftmost child of the right node
            let old_leftmost = crate::branch::BranchPage::get_leftmost_child(child)?;
            let borrowed_child_id = borrowed_child
                .ok_or_else(|| Error::Custom("Expected borrowed child for branch node".into()))?;
            crate::branch::BranchPage::update_leftmost_child(child, borrowed_child_id)?;

            // Insert separator with the old leftmost as its child
            child.add_node_sorted(&separator_key, &old_leftmost.0.to_le_bytes())?;

            // Update separator in parent to be the borrowed key
            let (_, parent) = txn.get_page_cow(parent_id)?;
            crate::branch::BranchPage::replace_key(parent, &separator_key, &borrowed_key)?
        }

        Ok(true)
    }

    /// Try to borrow a key from right sibling
    fn try_borrow_from_right(
        txn: &mut Transaction<'_, Write>,
        parent_id: PageId,
        child_index: usize,
        child_id: PageId,
        right_sibling_id: PageId,
    ) -> Result<bool> {
        // Check if right sibling can share without becoming underflowed
        let can_borrow = {
            let right_sibling = txn.get_page(right_sibling_id)?;
            // After removing one key, would the sibling still be above threshold?
            let is_branch = right_sibling.header.flags.contains(PageFlags::BRANCH);
            let min_keys = if is_branch { MIN_BRANCH_KEYS } else { MIN_LEAF_KEYS };
            
            // Check both key count and space requirements
            if right_sibling.header.num_keys as usize <= min_keys {
                false
            } else {
                // Estimate if sibling would still be above fill threshold after giving one key
                let used_space = PAGE_SIZE - right_sibling.header.free_space() - PageHeader::SIZE;
                used_space > FILL_THRESHOLD * 2  // Conservative check
            }
        };
        
        if !can_borrow {
            return Ok(false);
        }

        // Get the separator key from parent
        let separator_key = {
            let parent = txn.get_page(parent_id)?;
            parent.node(child_index)?.key()?.to_vec()
        };

        // Get the leftmost node from right sibling
        let (borrowed_key, borrowed_value, borrowed_child, right_new_leftmost, is_leaf) = {
            let right_sibling = txn.get_page(right_sibling_id)?;
            let node = right_sibling.node(0)?;
            let is_leaf = right_sibling.header.flags.contains(PageFlags::LEAF);
            if is_leaf {
                (node.key()?.to_vec(), get_node_value_data(right_sibling, &node)?, None, None, true)
            } else {
                // For branch pages, we need the leftmost child and the first node's child
                let leftmost = crate::branch::BranchPage::get_leftmost_child(right_sibling)?;
                let first_child = node.page_number()?;
                (
                    node.key()?.to_vec(),
                    vec![], // For branch pages, we don't need the value
                    Some(leftmost),
                    Some(first_child),
                    false,
                )
            }
        };

        // Remove the leftmost node from right sibling and update its leftmost child if branch using COW
        let new_right_sibling_id = {
            let (new_id, right_sibling) = txn.get_page_cow(right_sibling_id)?;
            right_sibling.remove_node(0)?;
            if let Some(new_leftmost) = right_new_leftmost {
                crate::branch::BranchPage::update_leftmost_child(right_sibling, new_leftmost)?;
            }
            new_id
        };

        // Insert into child using COW
        if is_leaf {
            // Check if child has space for the borrowed key/value
            let node_size = crate::page::NodeHeader::SIZE
                .checked_add(borrowed_key.len())
                .and_then(|s| s.checked_add(borrowed_value.len()))
                .ok_or_else(|| Error::Custom("Node size calculation overflow".into()))?;
            let required_space = node_size
                .checked_add(std::mem::size_of::<u16>())
                .ok_or_else(|| Error::Custom("Required space calculation overflow".into()))?;
            
            {
                let child_page = txn.get_page(child_id)?;
                if child_page.header.free_space() < required_space {
                    // Not enough space to borrow
                    return Ok(false);
                }
            }
            
            // For leaf nodes, the borrowed key goes directly to child
            let (_, child) = txn.get_page_cow(child_id)?;
            // Insert at the end
            child.add_node_sorted(&borrowed_key, &borrowed_value)?;

            // Update separator in parent to be the new first key of right sibling
            let new_separator = {
                let right_sibling = txn.get_page(new_right_sibling_id)?;
                right_sibling.node(0)?.key()?.to_vec()
            };

            // Update the separator key in parent using COW
            let (_, parent) = txn.get_page_cow(parent_id)?;
            crate::branch::BranchPage::replace_key(parent, &separator_key, &new_separator)?
        } else {
            // For branch nodes, separator goes down to child with borrowed leftmost as its child
            let (_, child) = txn.get_page_cow(child_id)?;
            // Insert separator with the borrowed leftmost child
            let borrowed_child_id = borrowed_child
                .ok_or_else(|| Error::Custom("Expected borrowed child for branch node".into()))?;
            child.add_node_sorted(&separator_key, &borrowed_child_id.0.to_le_bytes())?;

            // Update separator in parent to be the borrowed key using COW
            let (_, parent) = txn.get_page_cow(parent_id)?;
            crate::branch::BranchPage::replace_key(parent, &separator_key, &borrowed_key)?
        }

        Ok(true)
    }

    /// Merge two nodes
    fn merge_nodes(
        txn: &mut Transaction<'_, Write>,
        parent_id: PageId,
        left_index: usize,
        left_id: PageId,
        right_id: PageId,
    ) -> Result<()> {
        // Get separator key from parent
        let separator_key = {
            let parent = txn.get_page(parent_id)?;
            parent.node(left_index)?.key()?.to_vec()
        };

        // Check if we're dealing with branch or leaf pages
        let is_branch = {
            let left_page = txn.get_page(left_id)?;
            left_page.header.flags.contains(PageFlags::BRANCH)
        };

        // Calculate total size needed for merge
        let total_size_needed = {
            let left_page = txn.get_page(left_id)?;
            let right_page = txn.get_page(right_id)?;
            let left_used = PAGE_SIZE - left_page.header.free_space();
            let right_used = PAGE_SIZE - right_page.header.free_space();
            // Add some overhead for the separator key in branch pages
            if is_branch {
                left_used
                    .checked_add(right_used)
                    .and_then(|s| s.checked_add(separator_key.len()))
                    .and_then(|s| s.checked_add(16))
                    .ok_or_else(|| Error::Custom("Merge size calculation overflow".into()))?
            } else {
                left_used
                    .checked_add(right_used)
                    .ok_or_else(|| Error::Custom("Merge size calculation overflow".into()))?
            }
        };

        // Check if merge is possible
        if total_size_needed > PAGE_SIZE - PageHeader::SIZE - 100 { // Leave some safety margin
            // Can't merge - combined size would exceed page capacity
            // In this case, we'll just leave the pages as they are
            // This is similar to what LMDB does - it allows underflowed pages in some cases
            return Ok(());
        }

        if is_branch {
            // For branch pages, we need to handle the leftmost child pointer
            let left_leftmost =
                crate::branch::BranchPage::get_leftmost_child(txn.get_page(left_id)?)?;
            let right_leftmost =
                crate::branch::BranchPage::get_leftmost_child(txn.get_page(right_id)?)?;

            // Collect entries from both pages
            let mut all_entries = Vec::new();

            // Get entries from left page
            {
                let left_page = txn.get_page(left_id)?;
                for i in 0..left_page.header.num_keys as usize {
                    let node = left_page.node(i)?;
                    all_entries.push((node.key()?.to_vec(), node.page_number()?));
                }
            }

            // Add separator with right_leftmost as its child
            all_entries.push((separator_key.clone(), right_leftmost));

            // Get entries from right page
            {
                let right_page = txn.get_page(right_id)?;
                for i in 0..right_page.header.num_keys as usize {
                    let node = right_page.node(i)?;
                    all_entries.push((node.key()?.to_vec(), node.page_number()?));
                }
            }

            // Clear left page and rebuild as branch
            {
                let left_page = txn.get_page_mut(left_id)?;
                left_page.clear();
                left_page.header.flags = PageFlags::BRANCH;

                // Set leftmost child
                crate::branch::BranchPage::update_leftmost_child(left_page, left_leftmost)?;

                // Add all entries
                for (key, child) in all_entries {
                    left_page.add_node_sorted(&key, &child.0.to_le_bytes())?;
                }
            }
        } else {
            // For leaf pages, just collect and merge all entries
            let mut all_nodes = Vec::new();

            // Save the leaf chain pointers
            let (left_prev, right_next) = {
                let left_page = txn.get_page(left_id)?;
                let right_page = txn.get_page(right_id)?;
                (left_page.header.prev_pgno, right_page.header.next_pgno)
            };

            // Get nodes from left page
            {
                let left_page = txn.get_page(left_id)?;
                for i in 0..left_page.header.num_keys as usize {
                    let node = left_page.node(i)?;
                    all_nodes.push((node.key()?.to_vec(), get_node_value_data(left_page, &node)?));
                }
            }

            // Get nodes from right page
            {
                let right_page = txn.get_page(right_id)?;
                for i in 0..right_page.header.num_keys as usize {
                    let node = right_page.node(i)?;
                    all_nodes.push((node.key()?.to_vec(), get_node_value_data(right_page, &node)?));
                }
            }

            // Clear left page and add all nodes using COW
            let new_left_id = {
                let (new_id, left_page) = txn.get_page_cow(left_id)?;
                left_page.clear();
                for (key, value) in all_nodes {
                    left_page.add_node_sorted(&key, &value)?;
                }

                // Restore leaf chain pointers
                left_page.header.prev_pgno = left_prev;
                left_page.header.next_pgno = right_next;
                new_id
            };

            // Update next page's prev pointer if it exists
            if right_next != 0 {
                let (_, next_page) = txn.get_page_cow(PageId(right_next))?;
                next_page.header.prev_pgno = new_left_id.0;
            }
        }

        // Remove separator from parent using COW
        {
            let (_, parent) = txn.get_page_cow(parent_id)?;
            parent.remove_node(left_index)?;
        }

        // Free right page
        txn.free_page(right_id)?;

        Ok(())
    }

    /// Try to borrow from leftmost child to its right sibling
    fn try_borrow_from_leftmost_to_right(
        txn: &mut Transaction<'_, Write>,
        parent_id: PageId,
        leftmost_id: PageId,
        right_sibling_id: PageId,
    ) -> Result<bool> {
        // Check if leftmost child can share without becoming underflowed
        let can_borrow = {
            let leftmost = txn.get_page(leftmost_id)?;
            // After removing one key, would the leftmost still be above threshold?
            let is_branch = leftmost.header.flags.contains(PageFlags::BRANCH);
            let min_keys = if is_branch { MIN_BRANCH_KEYS } else { MIN_LEAF_KEYS };
            
            // Check both key count and space requirements
            if leftmost.header.num_keys as usize <= min_keys {
                false
            } else {
                // Estimate if leftmost would still be above fill threshold after giving one key
                let used_space = PAGE_SIZE - leftmost.header.free_space() - PageHeader::SIZE;
                used_space > FILL_THRESHOLD * 2  // Conservative check
            }
        };
        
        if !can_borrow {
            return Ok(false);
        }

        // Get the separator key (first key in parent)
        let separator_key = {
            let parent = txn.get_page(parent_id)?;
            parent.node(0)?.key()?.to_vec()
        };

        // Get the rightmost node from leftmost child
        let (borrowed_key, borrowed_value, borrowed_child, is_leaf) = {
            let leftmost = txn.get_page(leftmost_id)?;
            let last_idx = leftmost.header.num_keys as usize - 1;
            let node = leftmost.node(last_idx)?;
            let is_leaf = leftmost.header.flags.contains(PageFlags::LEAF);
            if is_leaf {
                (node.key()?.to_vec(), get_node_value_data(leftmost, &node)?, None, true)
            } else {
                (
                    node.key()?.to_vec(),
                    vec![], // For branch pages, we don't need the value
                    Some(node.page_number()?),
                    false,
                )
            }
        };

        // Remove the rightmost node from leftmost child
        {
            let leftmost = txn.get_page_mut(leftmost_id)?;
            leftmost.remove_node(leftmost.header.num_keys as usize - 1)?;
        }

        // Insert into right sibling
        if is_leaf {
            // For leaf nodes, the borrowed key goes directly to right sibling
            let right_sibling = txn.get_page_mut(right_sibling_id)?;
            // Insert at the beginning
            right_sibling.add_node_sorted(&borrowed_key, &borrowed_value)?;

            // Update separator in parent to be the new first key of right sibling
            let parent = txn.get_page_mut(parent_id)?;
            crate::branch::BranchPage::replace_key(parent, &separator_key, &borrowed_key)?
        } else {
            // For branch nodes, we need to handle the child pointers carefully
            let right_sibling = txn.get_page_mut(right_sibling_id)?;

            // The borrowed child becomes the new leftmost child of the right sibling
            let old_leftmost = crate::branch::BranchPage::get_leftmost_child(right_sibling)?;
            let borrowed_child_id = borrowed_child
                .ok_or_else(|| Error::Custom("Expected borrowed child for branch node".into()))?;
            crate::branch::BranchPage::update_leftmost_child(
                right_sibling,
                borrowed_child_id,
            )?;

            // Insert separator with the old leftmost as its child
            right_sibling.add_node_sorted(&separator_key, &old_leftmost.0.to_le_bytes())?;

            // Update separator in parent to be the borrowed key
            let parent = txn.get_page_mut(parent_id)?;
            crate::branch::BranchPage::replace_key(parent, &separator_key, &borrowed_key)?
        }

        Ok(true)
    }

    /// Merge leftmost child with its right sibling
    fn merge_leftmost_with_right(
        txn: &mut Transaction<'_, Write>,
        parent_id: PageId,
        leftmost_id: PageId,
        right_id: PageId,
    ) -> Result<()> {
        // Get separator key from parent (first key)
        let separator_key = {
            let parent = txn.get_page(parent_id)?;
            parent.node(0)?.key()?.to_vec()
        };

        // Check if we're dealing with branch or leaf pages
        let is_branch = {
            let leftmost_page = txn.get_page(leftmost_id)?;
            leftmost_page.header.flags.contains(PageFlags::BRANCH)
        };
        
        // Calculate total size needed for merge
        let total_size_needed = {
            let leftmost_page = txn.get_page(leftmost_id)?;
            let right_page = txn.get_page(right_id)?;
            let left_used = PAGE_SIZE - leftmost_page.header.free_space() - PageHeader::SIZE;
            let right_used = PAGE_SIZE - right_page.header.free_space() - PageHeader::SIZE;
            // Add some overhead for the separator key in branch pages
            if is_branch {
                left_used
                    .checked_add(right_used)
                    .and_then(|s| s.checked_add(separator_key.len()))
                    .and_then(|s| s.checked_add(64))
                    .ok_or_else(|| Error::Custom("Merge size calculation overflow".into()))?
            } else {
                left_used
                    .checked_add(right_used)
                    .and_then(|s| s.checked_add(32))
                    .ok_or_else(|| Error::Custom("Merge size calculation overflow".into()))?
            }
        };

        // Check if merge is possible
        if total_size_needed > PAGE_SIZE - PageHeader::SIZE - 100 { // Leave safety margin
            // Can't merge - combined size would exceed page capacity
            // This is not an error - we just leave the pages as they are
            // This matches LMDB behavior
            return Ok(());
        }

        if is_branch {
            // For branch pages, handle the leftmost child pointers
            let left_leftmost =
                crate::branch::BranchPage::get_leftmost_child(txn.get_page(leftmost_id)?)?;
            let right_leftmost =
                crate::branch::BranchPage::get_leftmost_child(txn.get_page(right_id)?)?;

            // Collect all entries
            let mut all_entries = Vec::new();

            // Get entries from leftmost page
            {
                let leftmost_page = txn.get_page(leftmost_id)?;
                for i in 0..leftmost_page.header.num_keys as usize {
                    let node = leftmost_page.node(i)?;
                    all_entries.push((node.key()?.to_vec(), node.page_number()?));
                }
            }

            // Add separator with right_leftmost as its child
            all_entries.push((separator_key.clone(), right_leftmost));

            // Get entries from right page
            {
                let right_page = txn.get_page(right_id)?;
                for i in 0..right_page.header.num_keys as usize {
                    let node = right_page.node(i)?;
                    all_entries.push((node.key()?.to_vec(), node.page_number()?));
                }
            }

            // Clear leftmost page and rebuild using COW
            {
                let (_, leftmost_page) = txn.get_page_cow(leftmost_id)?;
                leftmost_page.clear();
                leftmost_page.header.flags = PageFlags::BRANCH;

                // Set leftmost child
                crate::branch::BranchPage::update_leftmost_child(leftmost_page, left_leftmost)?;

                // Add all entries
                for (key, child) in all_entries {
                    leftmost_page.add_node_sorted(&key, &child.0.to_le_bytes())?;
                }
            }
        } else {
            // For leaf pages, just merge all entries
            let mut all_nodes = Vec::new();

            // Get nodes from leftmost page
            {
                let leftmost_page = txn.get_page(leftmost_id)?;
                for i in 0..leftmost_page.header.num_keys as usize {
                    let node = leftmost_page.node(i)?;
                    all_nodes.push((node.key()?.to_vec(), get_node_value_data(leftmost_page, &node)?));
                }
            }

            // Get nodes from right page
            {
                let right_page = txn.get_page(right_id)?;
                for i in 0..right_page.header.num_keys as usize {
                    let node = right_page.node(i)?;
                    all_nodes.push((node.key()?.to_vec(), get_node_value_data(right_page, &node)?));
                }
            }

            // Clear leftmost page and add all nodes using COW
            {
                let (_, leftmost_page) = txn.get_page_cow(leftmost_id)?;
                leftmost_page.clear();
                for (key, value) in all_nodes {
                    leftmost_page.add_node_sorted(&key, &value)?;
                }
            }
        }

        // Remove first key from parent and update leftmost child using COW
        {
            let (_, parent) = txn.get_page_cow(parent_id)?;

            // Get the new leftmost child (what was the first key's right child)
            let _new_leftmost = parent.node(0)?.page_number()?;

            // Remove the first key
            parent.remove_node(0)?;

            // Update leftmost child in parent
            crate::branch::BranchPage::update_leftmost_child(parent, leftmost_id)?;
        }

        // Free right page
        txn.free_page(right_id)?;

        Ok(())
    }

    /// Insert into leaf page with Copy-on-Write
    fn insert_into_leaf_cow(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<(PageId, InsertResult)> {
        // First check if key exists without modifying the page
        let search_result = {
            let page = txn.get_page(page_id)?;
            page.search_key_with_comparator::<C>(key)?
        };

        match search_result {
            SearchResult::Found { index } => {
                // Key exists - need to update with COW
                let (old_value, _old_overflow) = {
                    let page = txn.get_page(page_id)?;
                    let node = page.node(index)?;

                    if let Some(overflow_id) = node.overflow_page()? {
                        // Read value from overflow pages - get overflow count from page
                        let overflow_page = txn.get_page(overflow_id)?;
                        let overflow_count = overflow_page.header.overflow;
                        let value_size = node.header.value_size();
                        (
                            Some(crate::overflow::read_overflow_value_lmdb(txn, overflow_id, Some(overflow_count), Some(value_size))?),
                            Some((overflow_id, overflow_count)),
                        )
                    } else {
                        (node.value().ok().map(|v| v.into_owned()), None)
                    }
                };

                // Don't free old overflow pages yet - they're still referenced by the old page
                // The freelist will handle this when the old page is freed

                // Check if we need overflow for new value (do this before getting COW page)
                let needs_overflow = crate::overflow::needs_overflow(key.len(), value.len());
                let new_overflow_id = if needs_overflow {
                    let (overflow_id, _) = crate::overflow::write_overflow_value_lmdb(txn, value)?;
                    Some(overflow_id)
                } else {
                    None
                };

                // Now get COW page and perform modifications
                let (new_page_id, page) = txn.get_page_cow(page_id)?;

                // Remove old entry
                page.remove_node(index)?;

                // Insert the new value
                if let Some(overflow_id) = new_overflow_id {
                    match page.add_node_sorted_overflow(key, overflow_id) {
                        Ok(_) => Ok((new_page_id, InsertResult::Updated(old_value))),
                        Err(e) => Err(e),
                    }
                } else {
                    match page.add_node_sorted(key, value) {
                        Ok(_) => Ok((new_page_id, InsertResult::Updated(old_value))),
                        Err(e) => Err(e),
                    }
                }
            }
            SearchResult::NotFound { insert_pos: _ } => {
                // Key doesn't exist - check if we need to split (pre-emptively)
                let needs_split = {
                    let page = txn.get_page(page_id)?;
                    let needs_overflow = crate::overflow::needs_overflow(key.len(), value.len());
                    
                    // Calculate entry size for pre-emptive split check
                    let required_size = if needs_overflow {
                        crate::page::NodeHeader::SIZE
                            .checked_add(key.len())
                            .and_then(|s| s.checked_add(8))
                            .and_then(|s| s.checked_add(size_of::<u16>()))
                            .ok_or_else(|| Error::Custom("Node size calculation overflow".into()))?
                    } else {
                        crate::page::NodeHeader::SIZE
                            .checked_add(key.len())
                            .and_then(|s| s.checked_add(value.len()))
                            .and_then(|s| s.checked_add(size_of::<u16>()))
                            .ok_or_else(|| Error::Custom("Node size calculation overflow".into()))?
                    };
                    
                    // Use should_split for pre-emptive splitting at 85% utilization
                    page.should_split(Some(required_size))
                };

                if needs_split {
                    // Page will be full, handle split
                    let needs_overflow = crate::overflow::needs_overflow(key.len(), value.len());
                    if needs_overflow {
                        let (overflow_id, _) = crate::overflow::write_overflow_value_lmdb(txn, value)?;
                        Self::split_leaf_page_with_overflow(txn, page_id, key, overflow_id, value.len())
                            .map(|result| (page_id, result))
                    } else {
                        Self::split_leaf_page(txn, page_id, key, value)
                            .map(|result| (page_id, result))
                    }
                } else {
                    // Check if we need overflow (do this before getting COW page)
                    let needs_overflow = crate::overflow::needs_overflow(key.len(), value.len());
                    let overflow_id = if needs_overflow {
                        let (id, _) = crate::overflow::write_overflow_value_lmdb(txn, value)?;
                        Some(id)
                    } else {
                        None
                    };

                    // Get COW page and insert
                    let (new_page_id, page) = txn.get_page_cow(page_id)?;

                    if let Some(overflow_id) = overflow_id {
                        match page.add_node_sorted_overflow_with_size(key, overflow_id, value.len()) {
                            Ok(_) => Ok((new_page_id, InsertResult::Inserted)),
                            Err(e) => Err(e),
                        }
                    } else {
                        match page.add_node_sorted(key, value) {
                            Ok(_) => Ok((new_page_id, InsertResult::Inserted)),
                            Err(e) => Err(e),
                        }
                    }
                }
            }
        }
    }

    /// Insert into branch page with Copy-on-Write
    #[allow(dead_code)]
    fn insert_into_branch_cow(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<(PageId, InsertResult)> {
        Self::insert_into_branch_cow_with_depth(txn, page_id, key, value, 0)
    }

    fn insert_into_branch_cow_with_depth(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
        depth: usize,
    ) -> Result<(PageId, InsertResult)> {
        let child_page_id = {
            let page = txn.get_page(page_id)?;
            crate::branch::BranchPage::find_child_with_comparator::<C>(page, key)?
        };

        // Recursively insert into child with COW
        let (new_child_id, child_result) = Self::insert_cow_with_depth(txn, child_page_id, key, value, depth + 1)?;

        match child_result {
            InsertResult::Updated(old) => {
                if new_child_id != child_page_id {
                    // Child page changed due to COW, update parent
                    let (new_page_id, parent) = txn.get_page_cow(page_id)?;

                    // Find and update the child pointer
                    for i in 0..parent.header.num_keys as usize {
                        // First read the current value
                        let current_child = {
                            let node = parent.node(i)?;
                            node.page_number()?
                        };

                        if current_child == child_page_id {
                            // Now get mutable access and update
                            let mut node = parent.node_data_mut(i)?;
                            node.set_value(&new_child_id.0.to_le_bytes())?;
                            break;
                        }
                    }

                    Ok((new_page_id, InsertResult::Updated(old)))
                } else {
                    Ok((page_id, InsertResult::Updated(old)))
                }
            }
            InsertResult::Inserted => {
                if new_child_id != child_page_id {
                    // Child page changed due to COW, update parent
                    let (new_page_id, parent) = txn.get_page_cow(page_id)?;

                    // Update child pointer
                    crate::branch::BranchPage::update_child_pointer(
                        parent,
                        child_page_id,
                        new_child_id,
                    )?;

                    Ok((new_page_id, InsertResult::Inserted))
                } else {
                    Ok((page_id, InsertResult::Inserted))
                }
            }
            InsertResult::Split { median_key, right_page } => {
                // Child was split, need to add median key to this branch
                let (new_page_id, parent) = txn.get_page_cow(page_id)?;

                // Update the old child pointer if it changed
                if new_child_id != child_page_id {
                    crate::branch::BranchPage::update_child_pointer(
                        parent,
                        child_page_id,
                        new_child_id,
                    )?;
                }

                // Add the split child
                match crate::branch::BranchPage::add_split_child(parent, &median_key, right_page) {
                    Ok(()) => Ok((new_page_id, InsertResult::Inserted)),
                    Err(Error::Custom(msg)) if msg.contains("Page full") => {
                        // This branch is also full, split it
                        Self::split_branch_page(txn, new_page_id, median_key, right_page)
                            .map(|result| (new_page_id, result))
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }
}

/// Result of an insert operation
pub(crate) enum InsertResult {
    /// Key was updated, returns old value
    Updated(Option<Vec<u8>>),
    /// Key was inserted
    Inserted,
    /// Page was split, returns median key and new right page
    Split { median_key: Vec<u8>, right_page: PageId },
}

/// Result of a delete operation
enum DeleteResult {
    /// Key was not found
    NotFound,
    /// Key was deleted, returns old value
    Deleted(Vec<u8>),
    /// Key was deleted but page underflowed
    Underflow { old_value: Vec<u8> },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::EnvBuilder;
    use tempfile::TempDir;

    #[test]
    fn test_btree_search_empty() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path()).unwrap();

        let txn = env.read_txn().unwrap();
        let root = PageId(3); // Main DB root

        let result = BTree::<LexicographicComparator>::search(&txn, root, b"key").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_page_operations() {
        // Test that page operations work without hanging
        let mut page = crate::page::Page::new(PageId(1), PageFlags::LEAF);

        // Add a node
        page.add_node_sorted(b"test", b"value").unwrap();
        assert_eq!(page.header.num_keys, 1);

        // Search for it
        match page.search_key_with_comparator::<LexicographicComparator>(b"test").unwrap() {
            SearchResult::Found { index } => {
                assert_eq!(index, 0);
            }
            _ => panic!("Should have found key"),
        }
    }

    #[test]
    fn test_btree_insert_simple() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path()).unwrap();

        let mut txn = env.write_txn().unwrap();
        let mut root = PageId(3); // Main DB root
        let mut db_info = DbInfo {
            root,
            leaf_pages: 1,
            ..Default::default()
        };

        // Insert a key using B+Tree
        let old = BTree::<LexicographicComparator>::insert(
            &mut txn,
            &mut root,
            &mut db_info,
            b"key1",
            b"value1",
        )
        .unwrap();
        assert!(old.is_none());
        assert_eq!(db_info.entries, 1);

        // Check the page directly
        let page = txn.get_page(root).unwrap();
        assert_eq!(page.header.num_keys, 1);

        txn.commit().unwrap();

        // Search for the key
        let txn = env.read_txn().unwrap();
        let result = BTree::<LexicographicComparator>::search(&txn, root, b"key1").unwrap();
        assert_eq!(result.as_ref().map(|v| v.as_ref()), Some(&b"value1"[..]));
    }

    #[test]
    fn test_btree_insert_multiple() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path()).unwrap();

        let mut txn = env.write_txn().unwrap();
        let mut root = PageId(3);
        let mut db_info = DbInfo {
            root,
            leaf_pages: 1,
            ..Default::default()
        };

        // Insert multiple keys
        let keys = vec![
            (b"key3", b"value3"),
            (b"key1", b"value1"),
            (b"key5", b"value5"),
            (b"key2", b"value2"),
            (b"key4", b"value4"),
        ];

        for (key, value) in &keys {
            let old = BTree::<LexicographicComparator>::insert(
                &mut txn,
                &mut root,
                &mut db_info,
                *key,
                *value,
            )
            .unwrap();
            assert!(old.is_none());
        }

        assert_eq!(db_info.entries, 5);

        txn.commit().unwrap();

        // Search for all keys
        let txn = env.read_txn().unwrap();
        for (key, expected_value) in &keys {
            let result = BTree::<LexicographicComparator>::search(&txn, root, *key).unwrap();
            assert_eq!(result.as_ref().map(|v| v.as_ref()), Some(&expected_value[..]));
        }
    }

    #[test]
    fn test_btree_delete_simple() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path()).unwrap();

        let mut txn = env.write_txn().unwrap();
        let mut root = PageId(3);
        let mut db_info = DbInfo {
            root,
            leaf_pages: 1,
            ..Default::default()
        };

        // Insert some keys
        BTree::<LexicographicComparator>::insert(
            &mut txn,
            &mut root,
            &mut db_info,
            b"key1",
            b"value1",
        )
        .unwrap();
        BTree::<LexicographicComparator>::insert(
            &mut txn,
            &mut root,
            &mut db_info,
            b"key2",
            b"value2",
        )
        .unwrap();
        BTree::<LexicographicComparator>::insert(
            &mut txn,
            &mut root,
            &mut db_info,
            b"key3",
            b"value3",
        )
        .unwrap();
        assert_eq!(db_info.entries, 3);

        // Delete a key
        let deleted =
            BTree::<LexicographicComparator>::delete(&mut txn, &mut root, &mut db_info, b"key2")
                .unwrap();
        assert_eq!(deleted, Some(b"value2".to_vec()));
        assert_eq!(db_info.entries, 2);

        // Try to delete non-existent key
        let deleted =
            BTree::<LexicographicComparator>::delete(&mut txn, &mut root, &mut db_info, b"key4")
                .unwrap();
        assert_eq!(deleted, None);
        assert_eq!(db_info.entries, 2);

        txn.commit().unwrap();

        // Verify remaining keys
        let txn = env.read_txn().unwrap();
        assert!(BTree::<LexicographicComparator>::search(&txn, root, b"key1").unwrap().is_some());
        assert!(BTree::<LexicographicComparator>::search(&txn, root, b"key2").unwrap().is_none());
        assert!(BTree::<LexicographicComparator>::search(&txn, root, b"key3").unwrap().is_some());
    }

    #[test]
    fn test_btree_delete_with_rebalancing() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path()).unwrap();

        let mut txn = env.write_txn().unwrap();
        let mut root = PageId(3);
        let mut db_info = DbInfo {
            root,
            leaf_pages: 1,
            ..Default::default()
        };

        // Insert many keys to force page splits
        let num_keys = 50;
        for i in 0..num_keys {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}", i);
            BTree::<LexicographicComparator>::insert(
                &mut txn,
                &mut root,
                &mut db_info,
                key.as_bytes(),
                value.as_bytes(),
            )
            .unwrap();
        }
        assert_eq!(db_info.entries, num_keys);

        // Delete keys in a pattern that should trigger rebalancing
        for i in (0..num_keys).step_by(3) {
            let key = format!("key{:04}", i);
            let deleted = BTree::<LexicographicComparator>::delete(
                &mut txn,
                &mut root,
                &mut db_info,
                key.as_bytes(),
            )
            .unwrap();
            assert!(deleted.is_some());
        }

        // Verify the tree is still valid
        for i in 0..num_keys {
            let key = format!("key{:04}", i);
            let result =
                BTree::<LexicographicComparator>::search(&txn, root, key.as_bytes()).unwrap();
            if i % 3 == 0 {
                assert!(result.is_none(), "Key {} should be deleted", key);
            } else {
                assert!(result.is_some(), "Key {} should exist", key);
            }
        }

        txn.commit().unwrap();
    }

    #[test]
    fn test_btree_delete_all() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path()).unwrap();

        let mut txn = env.write_txn().unwrap();
        let mut root = PageId(3);
        let mut db_info = DbInfo {
            root,
            leaf_pages: 1,
            ..Default::default()
        };

        // Insert some keys
        let keys = vec![b"key1", b"key2", b"key3", b"key4", b"key5"];
        for key in &keys {
            BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, *key, *key)
                .unwrap();
        }
        assert_eq!(db_info.entries, keys.len() as u64);

        // Delete all keys
        for key in &keys {
            let deleted =
                BTree::<LexicographicComparator>::delete(&mut txn, &mut root, &mut db_info, *key)
                    .unwrap();
            assert!(deleted.is_some());
        }
        assert_eq!(db_info.entries, 0);

        // Tree should be empty
        for key in &keys {
            let result = BTree::<LexicographicComparator>::search(&txn, root, *key).unwrap();
            assert!(result.is_none());
        }

        txn.commit().unwrap();
    }

    #[test]
    fn test_btree_overflow_values() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new().map_size(10 * 1024 * 1024).open(dir.path()).unwrap();

        let mut txn = env.write_txn().unwrap();

        // Properly initialize the database
        let (root_id, root_page) = txn.alloc_page(PageFlags::LEAF).unwrap();
        root_page.header.num_keys = 0;

        let mut db_info = DbInfo {
            root: root_id,
            leaf_pages: 1,
            ..Default::default()
        };
        let mut root = root_id;

        // Save initial database info
        txn.update_db_info(None, db_info).unwrap();

        // Create a large value that needs overflow pages
        let large_value = vec![0xAB; 5000]; // 5KB

        // Insert with large value
        let old = BTree::<LexicographicComparator>::insert(
            &mut txn,
            &mut root,
            &mut db_info,
            b"large_key",
            &large_value,
        )
        .unwrap();
        assert!(old.is_none());
        assert_eq!(db_info.entries, 1);

        // Insert some normal values
        BTree::<LexicographicComparator>::insert(
            &mut txn,
            &mut root,
            &mut db_info,
            b"small1",
            b"value1",
        )
        .unwrap();
        BTree::<LexicographicComparator>::insert(
            &mut txn,
            &mut root,
            &mut db_info,
            b"small2",
            b"value2",
        )
        .unwrap();

        // Update the root in db_info after all inserts
        db_info.root = root;
        // Update transaction's database info
        txn.update_db_info(None, db_info).unwrap();

        txn.commit().unwrap();

        // Search for large value
        let txn = env.read_txn().unwrap();
        let db_info = txn.db_info(None).unwrap();
        let result =
            BTree::<LexicographicComparator>::search(&txn, db_info.root, b"large_key").unwrap();
        assert_eq!(result.as_ref().map(|v| v.as_ref()), Some(&large_value[..]));

        // Search for normal values
        let result =
            BTree::<LexicographicComparator>::search(&txn, db_info.root, b"small1").unwrap();
        assert_eq!(result.as_ref().map(|v| v.as_ref()), Some(&b"value1"[..]));

        drop(txn);

        // Delete large value
        let mut txn = env.write_txn().unwrap();
        let mut db_info = *txn.db_info(None).unwrap();
        let mut root = db_info.root;
        let deleted = BTree::<LexicographicComparator>::delete(
            &mut txn,
            &mut root,
            &mut db_info,
            b"large_key",
        )
        .unwrap();
        assert_eq!(deleted, Some(large_value));

        // Update db_info with new root
        db_info.root = root;
        txn.update_db_info(None, db_info).unwrap();

        // Verify it's deleted
        let result =
            BTree::<LexicographicComparator>::search(&txn, db_info.root, b"large_key").unwrap();
        assert!(result.is_none());

        txn.commit().unwrap();
    }
}
