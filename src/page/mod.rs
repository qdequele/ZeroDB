//! Page structures for ZeroDB.
//!
//! This module contains all the on-disk page structures that match LMDB's format exactly.

mod header;
mod meta;
mod node;
mod overflow;

pub use header::{PAGE_HEADER_SIZE, PageFlags, PageHeader};
pub use meta::{DB_INFO_SIZE, DbInfo, MetaPage};
pub use node::{BranchNode, LeafNode, NodeFlags};
pub use overflow::{
    OVERFLOW_HEADER_SIZE, OverflowPage, node_max, overflow_pages, overflow_threshold,
    should_use_overflow,
};

/// Page number type (64-bit for large databases).
pub type PageNo = u64;

/// Transaction ID type.
pub type TxnId = u64;

/// Database handle index.
pub type Dbi = u32;
