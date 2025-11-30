//! B+tree implementation for ZeroDB.
//!
//! This module implements a B+tree data structure that stores key-value pairs
//! in sorted order, matching LMDB's format exactly.

mod cursor;
mod insert;
mod node;
mod page_ops;
mod search;

pub use cursor::{CursorLevel, CursorOps, CursorState};
pub use insert::{insert_into_branch, insert_into_leaf, InsertResult};
pub use node::{Node, NodeRef};
pub use page_ops::{BranchPage, LeafPage, PageBuilder};
pub use search::search_page;

use crate::page::PageNo;

/// Invalid/null page number marker.
pub const P_INVALID: PageNo = PageNo::MAX;

/// Result of a key comparison during search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchResult {
    /// Exact match found at the given index.
    Found(usize),
    /// Key not found, would be inserted at the given index.
    NotFound(usize),
}

impl SearchResult {
    /// Returns the index, whether found or not.
    pub fn index(&self) -> usize {
        match *self {
            SearchResult::Found(i) | SearchResult::NotFound(i) => i,
        }
    }

    /// Returns true if the key was found.
    pub fn is_found(&self) -> bool {
        matches!(self, SearchResult::Found(_))
    }
}

/// Comparison function type for keys.
pub type CompareFn = fn(&[u8], &[u8]) -> std::cmp::Ordering;

/// Default key comparison (lexicographic).
pub fn default_compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    a.cmp(b)
}

/// Reverse key comparison.
pub fn reverse_compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    b.cmp(a)
}

/// Integer key comparison (native byte order).
pub fn integer_compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    // Assumes keys are the same size
    match a.len() {
        4 => {
            let a_val = u32::from_ne_bytes([a[0], a[1], a[2], a[3]]);
            let b_val = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
            a_val.cmp(&b_val)
        }
        8 => {
            let a_val = u64::from_ne_bytes([a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7]]);
            let b_val = u64::from_ne_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
            a_val.cmp(&b_val)
        }
        _ => a.cmp(b), // Fall back to lexicographic
    }
}
