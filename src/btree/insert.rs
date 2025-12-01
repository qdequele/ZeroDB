//! B+tree insert operations.
//!
//! This module handles inserting new key-value pairs into the B+tree,
//! including page splits when necessary.

use crate::error::Result;
use crate::page::PageNo;

use super::node::Node;
use super::page_ops::{LeafPage, PageBuilder};
use super::P_INVALID;

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

    // Collect all nodes including the new one
    let mut nodes: Vec<Node> = Vec::with_capacity(num_keys + 1);

    for i in 0..num_keys {
        if i == insert_index {
            nodes.push(new_node.clone());
        }
        let node_ref = page.node(i)?;
        // Preserve overflow nodes properly
        if node_ref.is_overflow() {
            if let Some(overflow_pgno) = node_ref.overflow_pgno() {
                nodes.push(Node::leaf_overflow(
                    node_ref.key().to_vec(),
                    overflow_pgno,
                ));
            } else {
                return Err(crate::error::Error::Corrupted);
            }
        } else {
            nodes.push(Node::leaf(
                node_ref.key().to_vec(),
                node_ref.value().to_vec(),
            ));
        }
    }

    // Handle insert at end
    if insert_index >= num_keys {
        nodes.push(new_node.clone());
    }

    // Try to fit all nodes in one page
    let mut builder = PageBuilder::new_leaf(page_no, page_size);
    let mut fit_count = 0;

    for node in &nodes {
        if builder.can_fit(node.size()) {
            builder.add_leaf(node)?;
            fit_count += 1;
        } else {
            break;
        }
    }

    if fit_count == nodes.len() {
        // Everything fits
        return Ok((builder.finish(), None));
    }

    // Need to split - calculate split point
    let split_point = nodes.len() / 2;
    let split_point = split_point.max(1);

    // Build left page
    let mut left_builder = PageBuilder::new_leaf(page_no, page_size);
    for node in &nodes[..split_point] {
        left_builder.add_leaf(node)?;
    }

    // Build right page (page number will be set by caller)
    let mut right_builder = PageBuilder::new_leaf(P_INVALID, page_size);
    for node in &nodes[split_point..] {
        right_builder.add_leaf(node)?;
    }

    Ok((left_builder.finish(), Some((right_builder.finish(), nodes[split_point].key.clone()))))
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

        nodes.push(Node::branch(
            node_ref.key().to_vec(),
            node_ref.child_pgno(),
        ));
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

    Ok((left_builder.finish(), Some((right_builder.finish(), nodes[split_point].key.clone()))))
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
        builder.add_leaf(&Node::leaf(b"a".to_vec(), b"1".to_vec())).unwrap();
        builder.add_leaf(&Node::leaf(b"c".to_vec(), b"3".to_vec())).unwrap();
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
