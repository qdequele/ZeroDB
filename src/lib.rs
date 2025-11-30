//! ZeroDB - A pure-Rust port of LMDB
//!
//! This library provides a memory-mapped key-value store with ACID transactions,
//! fully compatible with LMDB's file format.

#![forbid(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs)]

pub mod alloc;
pub mod btree;
pub mod db;
pub mod env;
pub mod error;
pub mod flags;
pub mod mmap;
pub mod page;
pub mod txn;
pub mod types;

pub use db::{Database, DbReader, DbWriter, RoCursor, RwCursor};
pub use types::{BytesDecode, BytesEncode, Bytes, OwnedBytes, Str, OwnedStr, U32, U64, I32, I64, Unit};
pub use error::{Error, Result};
pub use flags::{EnvFlags, DatabaseFlags, PutFlags};
pub use page::{PageFlags, PageHeader};
pub use env::{Env, EnvOpenOptions};
pub use txn::{RoTxn, RwTxn};

/// LMDB magic number: 0xBEEFC0DE
pub const MDB_MAGIC: u32 = 0xBEEF_C0DE;

/// LMDB format version
pub const MDB_VERSION: u32 = 1;

/// Default maximum key size in bytes
pub const MAX_KEY_SIZE: usize = 511;

/// Maximum number of named databases
pub const MAX_DBS: u32 = 32767;

/// Default page header size in bytes
pub const PAGE_HEADER_SIZE: usize = 16;
