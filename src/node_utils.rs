//! Utility functions for working with nodes and handling overflow values

use crate::error::{Error, Result};
use crate::page::{Node, Page};
use crate::txn::{mode::Mode, Transaction};
use std::borrow::Cow;

/// Read a node's value, handling overflow pages transparently
pub fn read_node_value<'txn, M: Mode>(
    txn: &'txn Transaction<'txn, M>,
    node: &Node<'txn>,
) -> Result<Cow<'txn, [u8]>> {
    if let Some(overflow_id) = node.overflow_page()? {
        // Value is in overflow pages
        let overflow_page = txn.get_page(overflow_id)?;
        let overflow_count = overflow_page.header.overflow;
        let value_size = node.header.value_size();
        
        let value = crate::overflow::read_overflow_value_lmdb(
            txn,
            overflow_id,
            Some(overflow_count),
            Some(value_size),
        )?;
        Ok(Cow::Owned(value))
    } else {
        // Regular inline value
        node.value()
    }
}

/// Read all key-value pairs from a leaf page, handling overflow values
pub fn read_leaf_entries<'txn, M: Mode>(
    txn: &'txn Transaction<'txn, M>,
    page: &'txn Page,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut entries = Vec::with_capacity(page.header.num_keys as usize);
    
    for i in 0..page.header.num_keys as usize {
        let node = page.node(i)?;
        let key = node.key()?.to_vec();
        let value = read_node_value(txn, &node)?.into_owned();
        entries.push((key, value));
    }
    
    Ok(entries)
}

/// Check if a node contains an overflow value
pub fn is_overflow_node(node: &Node) -> bool {
    node.header.flags.contains(crate::page::NodeFlags::BIGDATA)
}

/// Get the overflow page ID and metadata from a node
pub fn get_overflow_info(node: &Node) -> Result<Option<(crate::error::PageId, usize)>> {
    if !is_overflow_node(node) {
        return Ok(None);
    }
    
    if let Some(overflow_id) = node.overflow_page()? {
        let value_size = node.header.value_size();
        Ok(Some((overflow_id, value_size)))
    } else {
        Err(Error::Corruption {
            details: "Node marked as overflow but has no overflow page".into(),
            page_id: None,
        })
    }
}

/// Read a value from either inline or overflow storage, with size hint
pub fn read_value_with_hint<'txn, M: Mode>(
    txn: &'txn Transaction<'txn, M>,
    node: &Node<'txn>,
    expected_size: Option<usize>,
) -> Result<Vec<u8>> {
    if let Some(overflow_id) = node.overflow_page()? {
        // For overflow values, we can provide the size hint
        let overflow_page = txn.get_page(overflow_id)?;
        let overflow_count = overflow_page.header.overflow;
        let value_size = expected_size.unwrap_or_else(|| node.header.value_size());
        
        crate::overflow::read_overflow_value_lmdb(
            txn,
            overflow_id,
            Some(overflow_count),
            Some(value_size),
        )
    } else {
        // Regular inline value
        Ok(node.value()?.into_owned())
    }
}