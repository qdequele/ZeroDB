//! B+tree insert operations.
//!
//! This module handles inserting new key-value pairs into the B+tree,
//! including page splits when necessary.

#![allow(clippy::type_complexity)]

use crate::error::{Error, Result};
use crate::page::{PageNo, PAGE_HEADER_SIZE};

use super::P_INVALID;
use super::node::{Node, NodeRef};
use super::page_ops::{LeafPage, PageBuilder};

/// Size of a node pointer in the page index.
const NODE_PTR_SIZE: usize = 2;

/// Result of an insert operation.
#[derive(Debug)]
pub enum InsertResult {
    /// Insert completed without split.
    Done,
    /// Page was split; contains the separator key and new page number.
    Split {
        /// Separator key (first key of the new right page).
        separator: Vec<u8>,
        /// Page number of the new right page.
        right_pgno: PageNo,
    },
}

/// Calculates the split point for a page.
///
/// Returns the index at which to split (items 0..split go left, split.. go right).
#[allow(dead_code)]
pub fn calculate_split_point(
    num_keys: usize,
    _key_sizes: &[usize],
    _new_key_size: usize,
    _insert_index: usize,
    _page_size: usize,
) -> usize {
    // Simple strategy: split in the middle by count
    // A more sophisticated approach would balance by size
    let total = num_keys + 1;
    let mid = total / 2;

    // Ensure at least one key on each side
    mid.max(1).min(num_keys)
}

/// Inserts a node into a leaf page at the specified index.
///
/// This is an optimized implementation that:
/// 1. Attempts in-place insertion when there's enough space
/// 2. Uses zero-copy iteration when rebuilding is necessary
/// 3. Minimizes memory allocations
///
/// If the page is full, returns the nodes that should go into a new split page.
pub fn insert_into_leaf(
    page_data: &[u8],
    new_node: Node,
    insert_index: usize,
    page_no: PageNo,
    page_size: usize,
) -> Result<(Vec<u8>, Option<(Vec<u8>, Vec<u8>)>)> {
    let page = LeafPage::new(page_data, page_size)?;
    let num_keys = page.num_keys();
    let new_node_size = new_node.size();

    // Calculate if we can fit the new node
    let current_free_space = page.free_space();
    let space_needed = new_node_size + NODE_PTR_SIZE;

    // Fast path: in-place insertion when there's enough space
    if current_free_space >= space_needed {
        return insert_in_place(page_data, &new_node, insert_index, page_no, page_size);
    }

    // Need to rebuild (and possibly split)
    // Use zero-copy iteration to minimize allocations
    insert_with_rebuild(page_data, new_node, insert_index, page_no, page_size, num_keys)
}

/// Fast path: insert node in-place without full page rebuild.
///
/// This directly manipulates the page buffer to insert a new node,
/// shifting existing pointers as needed.
#[inline]
fn insert_in_place(
    page_data: &[u8],
    new_node: &Node,
    insert_index: usize,
    page_no: PageNo,
    page_size: usize,
) -> Result<(Vec<u8>, Option<(Vec<u8>, Vec<u8>)>)> {
    let page = LeafPage::new(page_data, page_size)?;
    let num_keys = page.num_keys();
    let new_node_size = new_node.size();

    // Copy page data to new buffer
    let mut data = page_data.to_vec();

    // Read current bounds from header
    let lower = u16::from_le_bytes([data[12], data[13]]) as usize;
    let upper = u16::from_le_bytes([data[14], data[15]]) as usize;

    // Calculate new bounds
    let new_upper = upper - new_node_size;
    let new_lower = lower + NODE_PTR_SIZE;

    // Write the new node data at new_upper
    new_node.write_leaf(&mut data[new_upper..])?;

    // Shift existing pointers after insert_index to make room
    if insert_index < num_keys {
        let shift_start = PAGE_HEADER_SIZE + insert_index * NODE_PTR_SIZE;
        let shift_end = lower;
        // Shift right by NODE_PTR_SIZE bytes
        data.copy_within(shift_start..shift_end, shift_start + NODE_PTR_SIZE);
    }

    // Write the new pointer at insert_index
    let ptr_offset = PAGE_HEADER_SIZE + insert_index * NODE_PTR_SIZE;
    data[ptr_offset..ptr_offset + 2].copy_from_slice(&(new_upper as u16).to_le_bytes());

    // Update header bounds
    data[12..14].copy_from_slice(&(new_lower as u16).to_le_bytes());
    data[14..16].copy_from_slice(&(new_upper as u16).to_le_bytes());

    // Update page number in header if different
    data[0..8].copy_from_slice(&page_no.to_le_bytes());

    Ok((data, None))
}

/// Slow path: rebuild the page (and possibly split) using zero-copy iteration.
fn insert_with_rebuild(
    page_data: &[u8],
    new_node: Node,
    insert_index: usize,
    page_no: PageNo,
    page_size: usize,
    num_keys: usize,
) -> Result<(Vec<u8>, Option<(Vec<u8>, Vec<u8>)>)> {
    let page = LeafPage::new(page_data, page_size)?;

    // Calculate total size to determine if split is needed
    let mut total_data_size = new_node.size() + NODE_PTR_SIZE;
    for i in 0..num_keys {
        let node_ref = page.node(i)?;
        total_data_size += node_ref.size() + NODE_PTR_SIZE;
    }

    let available_space = page_size - PAGE_HEADER_SIZE;
    let needs_split = total_data_size > available_space;

    if !needs_split {
        // Rebuild without split - use single-pass building
        let mut builder = PageBuilder::new_leaf(page_no, page_size);

        for i in 0..=num_keys {
            if i == insert_index {
                builder.add_leaf(&new_node)?;
            }
            if i < num_keys {
                let node_ref = page.node(i)?;
                add_node_ref_to_builder(&mut builder, &node_ref)?;
            }
        }

        return Ok((builder.finish(), None));
    }

    // Need to split - calculate split point
    let total_keys = num_keys + 1;
    let split_point = (total_keys / 2).max(1);

    // Build left page
    let mut left_builder = PageBuilder::new_leaf(page_no, page_size);
    // Build right page
    let mut right_builder = PageBuilder::new_leaf(P_INVALID, page_size);

    let mut separator_key: Option<Vec<u8>> = None;
    let mut current_index = 0;

    for i in 0..=num_keys {
        let is_new_node = i == insert_index;
        let has_existing = i < num_keys;

        // Process new node at insert position
        if is_new_node {
            if current_index < split_point {
                left_builder.add_leaf(&new_node)?;
            } else {
                if separator_key.is_none() {
                    separator_key = Some(new_node.key.clone());
                }
                right_builder.add_leaf(&new_node)?;
            }
            current_index += 1;
        }

        // Process existing node
        if has_existing {
            let node_ref = page.node(i)?;
            if current_index < split_point {
                add_node_ref_to_builder(&mut left_builder, &node_ref)?;
            } else {
                if separator_key.is_none() {
                    separator_key = Some(node_ref.key().to_vec());
                }
                add_node_ref_to_builder(&mut right_builder, &node_ref)?;
            }
            current_index += 1;
        }
    }

    let separator = separator_key.ok_or(Error::Corrupted)?;

    Ok((
        left_builder.finish(),
        Some((right_builder.finish(), separator)),
    ))
}

/// Helper to add a NodeRef to a PageBuilder without allocating a full Node.
#[inline]
fn add_node_ref_to_builder(builder: &mut PageBuilder, node_ref: &NodeRef) -> Result<()> {
    // Create a minimal Node from the reference
    if node_ref.is_overflow() {
        let node = Node::leaf_overflow(
            node_ref.key().to_vec(),
            node_ref.overflow_pgno().ok_or(Error::Corrupted)?,
        );
        builder.add_leaf(&node)
    } else {
        let node = Node::leaf(node_ref.key().to_vec(), node_ref.value().to_vec());
        builder.add_leaf(&node)
    }
}

/// Inserts a child pointer into a branch page.
pub fn insert_into_branch(
    page_data: &[u8],
    separator: Vec<u8>,
    right_pgno: PageNo,
    insert_index: usize,
    page_no: PageNo,
    page_size: usize,
) -> Result<(Vec<u8>, Option<(Vec<u8>, Vec<u8>)>)> {
    use super::page_ops::BranchPage;

    let page = BranchPage::new(page_data, page_size)?;
    let num_keys = page.num_keys();

    // Collect all nodes including the new one
    let mut nodes: Vec<Node> = Vec::with_capacity(num_keys + 1);

    for i in 0..num_keys {
        let node_ref = page.node(i)?;

        // Insert new node at the right position
        if i == insert_index + 1 {
            nodes.push(Node::branch(separator.clone(), right_pgno));
        }

        nodes.push(Node::branch(node_ref.key().to_vec(), node_ref.child_pgno()));
    }

    // Handle insert at end
    if insert_index + 1 >= num_keys {
        nodes.push(Node::branch(separator.clone(), right_pgno));
    }

    // Try to fit all nodes in one page
    let mut builder = PageBuilder::new_branch(page_no, page_size);
    let mut fit_count = 0;

    for node in &nodes {
        if builder.can_fit(node.size()) {
            builder.add_branch(node)?;
            fit_count += 1;
        } else {
            break;
        }
    }

    if fit_count == nodes.len() {
        return Ok((builder.finish(), None));
    }

    // Need to split
    let split_point = nodes.len() / 2;
    let split_point = split_point.max(1);

    let mut left_builder = PageBuilder::new_branch(page_no, page_size);
    for node in &nodes[..split_point] {
        left_builder.add_branch(node)?;
    }

    let mut right_builder = PageBuilder::new_branch(P_INVALID, page_size);
    for node in &nodes[split_point..] {
        right_builder.add_branch(node)?;
    }

    Ok((
        left_builder.finish(),
        Some((right_builder.finish(), nodes[split_point].key.clone())),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_into_empty_leaf() {
        let page_size = 4096;
        let builder = PageBuilder::new_leaf(1, page_size);
        let page_data = builder.finish();

        let node = Node::leaf(b"key".to_vec(), b"value".to_vec());
        let (new_data, split) = insert_into_leaf(&page_data, node, 0, 1, page_size).unwrap();

        assert!(split.is_none());

        let page = LeafPage::new(&new_data, page_size).unwrap();
        assert_eq!(page.num_keys(), 1);
        assert_eq!(page.key(0).unwrap(), b"key");
    }

    #[test]
    fn insert_maintains_order() {
        let page_size = 4096;
        let mut builder = PageBuilder::new_leaf(1, page_size);
        builder
            .add_leaf(&Node::leaf(b"a".to_vec(), b"1".to_vec()))
            .unwrap();
        builder
            .add_leaf(&Node::leaf(b"c".to_vec(), b"3".to_vec()))
            .unwrap();
        let page_data = builder.finish();

        // Insert "b" between "a" and "c"
        let node = Node::leaf(b"b".to_vec(), b"2".to_vec());
        let (new_data, split) = insert_into_leaf(&page_data, node, 1, 1, page_size).unwrap();

        assert!(split.is_none());

        let page = LeafPage::new(&new_data, page_size).unwrap();
        assert_eq!(page.num_keys(), 3);
        assert_eq!(page.key(0).unwrap(), b"a");
        assert_eq!(page.key(1).unwrap(), b"b");
        assert_eq!(page.key(2).unwrap(), b"c");
    }
}
