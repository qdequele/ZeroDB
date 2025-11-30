//! Page structures for ZeroDB.
//!
//! This module contains all the on-disk page structures that match LMDB's format exactly.

mod header;
mod meta;
mod node;
mod overflow;

pub use header::{PageFlags, PageHeader, PAGE_HEADER_SIZE};
pub use meta::{DbInfo, MetaPage};
pub use node::{NodeFlags, LeafNode, BranchNode};
pub use overflow::OverflowPage;

/// Page number type (64-bit for large databases).
pub type PageNo = u64;

/// Transaction ID type.
pub type TxnId = u64;

/// Database handle index.
pub type Dbi = u32;
