//! Pure Rust implementation of LMDB with modern performance optimizations
//!
//! This crate provides a high-performance, type-safe embedded database
//! that is compatible with LMDB while leveraging modern Rust features
//! and performance technologies like SIMD.

#![warn(missing_docs)]
#![cfg_attr(not(test), warn(clippy::unwrap_used))]
#![deny(unsafe_op_in_unsafe_fn)]
// #![cfg_attr(feature = "simd", feature(portable_simd))]

pub mod adaptive_page;
pub mod auto_grow;
pub mod auto_grow_integration;
pub mod batch_commit;
pub mod bloom_filter;
pub mod branch;
pub mod branch_compressed;
pub mod btree;
pub mod cache_aligned;
pub mod catalog;
pub mod comparator;
pub mod copy;
pub mod cursor;
pub mod cursor_iter;
pub mod db;
pub mod dupsort;
pub mod env;
pub mod error;
pub mod fixed_size;
pub mod freelist;
pub mod io;
pub mod meta;
pub mod nested_txn;
pub mod node;
pub mod node_utils;
pub mod numa;
pub mod overflow;
pub mod overflow_consecutive;
pub mod overflow_stream;
pub mod page;
pub mod page_allocator;
pub mod page_capacity;
pub mod preflight_checks;
pub mod reader;
pub mod simd;
pub mod simd_advanced;
pub mod space_info;
pub mod tree_utils;
pub mod txn;

#[cfg(test)]
mod test_overflow_debug;

// Re-exports
pub use db::{Database, DatabaseFlags, Key, Value};
pub use env::{EnvBuilder, Environment};
pub use error::{Error, Result};
pub use txn::{ReadTransaction, Transaction, WriteTransaction};

// Type aliases for common use cases
/// A read-only transaction
pub type RoTxn<'env> = Transaction<'env, txn::Read>;
/// A read-write transaction
pub type RwTxn<'env> = Transaction<'env, txn::Write>;

/// The default page size (4KB)
pub const DEFAULT_PAGE_SIZE: usize = 4096;

/// Maximum key size (when not using longer-keys feature)


/// Library version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
