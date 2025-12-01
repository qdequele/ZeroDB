//! ZeroDB - A pure-Rust port of LMDB
//!
//! This library provides a memory-mapped key-value store with ACID transactions,
//! fully compatible with LMDB's file format.
//!
//! # Heed-Compatible API
//!
//! ZeroDB provides a typed database API compatible with the Heed crate:
//!
//! ```ignore
//! use zerodb::{EnvOpenOptions, Database};
//! use zerodb::types::{Str, U32};
//!
//! let env = unsafe { EnvOpenOptions::new().open(path)? };
//! let mut wtxn = env.write_txn()?;
//!
//! // Create a database with string keys and u32 values
//! let db: Database<Str, U32> = env.create_database(&mut wtxn, None)?;
//!
//! db.put(&mut wtxn, "hello", &42)?;
//! wtxn.commit()?;
//!
//! let rtxn = env.read_txn()?;
//! assert_eq!(db.get(&rtxn, "hello")?, Some(42));
//! ```

#![forbid(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs)]

pub mod alloc;
pub mod btree;
pub mod database;
pub mod db;
pub mod env;
pub mod error;
pub mod flags;
pub mod mmap;
pub mod page;
pub mod txn;
pub mod types;

// Re-export the old API for backwards compatibility
#[doc(hidden)]
pub use db::Database as RawDatabase;
pub use db::{DbReader, DbWriter, RoCursor, RwCursor};

// Heed-compatible typed database API
pub use database::{
    Database,
    DatabaseOpenOptions,
    DatabaseStat,
    Dbi,
    LazyDecode,
    ReservedSpace,
    RoDuplicates,
    // Read-only iterators
    RoIter,
    RoPrefix,
    RoRange,
    RoRevIter,
    RoRevPrefix,
    RoRevRange,
    // Mutable iterators
    RwIter,
    RwPrefix,
    RwRange,
    RwRevIter,
    RwRevPrefix,
    RwRevRange,
    Unspecified,
};
pub use types::{
    BE,
    BEI128,
    BEU16,
    BEU32,
    BEU64,
    BEU128,
    // Re-export byteorder types
    BigEndian,
    ByteOrder,
    Bytes,
    BytesDecode,
    BytesEncode,
    // DecodeIgnore
    DecodeIgnore,
    I32,
    I64,
    I128,
    LE,
    LittleEndian,
    NE,
    NativeEndian,
    OwnedBytes,
    OwnedDecode,
    OwnedStr,
    Str,
    U8,
    // Endian-aware types (Heed compatible)
    U16,
    U32,
    U32BE,
    U64,
    U64BE,
    U128,
    Unit,
};

// Serde types (feature-gated)
#[cfg(feature = "serde")]
pub use types::{SerdeBincode, SerdeJson};

pub use error::{BoxedError, Error, Result};

/// MdbError is an alias for Error for Heed compatibility.
pub type MdbError = Error;
pub use env::{
    // Options
    CompactionOption,
    // Comparators
    Comparator,
    DbStat,
    DefaultComparator,
    Env,
    // Closing event
    EnvClosingEvent,
    EnvInfo,
    EnvOpenOptions,
    FlagSetMode,
    IntegerComparator,
    LexicographicComparator,
    env_closing_event,
};
pub use flags::{DatabaseFlags, EnvFlags, PutFlags};
pub use page::{PageFlags, PageHeader};
pub use txn::{AnyTls, RoTxn, RwTxn, TlsUsage, Txn, WithTls, WithoutTls};

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
