//! The public API of [heed](https://github.com/meilisearch/heed) 0.22.1,
//! implemented over ZeroDB instead of LMDB, so a heed consumer switches
//! engines without changing code.
//!
//! It mirrors heed's type paths (`Env<WithoutTls>`, `RoTxn`, `RwTxn`,
//! `Database<KC, DC>`, the flag types, `Error`, `ReservedSpace`, ...) and
//! re-exports `heed-traits` and `heed-types` unchanged, so codec traits stay
//! the very traits LMDB's heed uses.
//!
//! The package is named `heed`, at heed's version, so a consumer switches with
//! one `[patch.crates-io] heed = { git = "…ZeroDB" }` line: cargo's `[patch]`
//! matches by package name. Inside this repository it is the
//! `crates/heed-zerodb` directory, and other crates depend on it as
//! `heed_zerodb`.
//!
//! ## Meilisearch's heed fork (`unique-txns` feature, ADR-0027)
//!
//! From v1.55, Meilisearch builds against its own heed fork
//! (`meilisearch/heed@mutexify-open-dbi`), which serializes opening and
//! creating databases behind a per-environment mutex. The `unique-txns`
//! feature switches this crate to that fork's API: `UniqueRoTxn`,
//! `UniqueRwTxn`, `AsUniqueTxnRef` and the `Env::unique_*` constructors;
//! databases open through a unique transaction only; `RoTxn::commit` becomes
//! `UniqueRoTxn::commit`; `Env::non_free_pages_size` gives way to
//! `EnvStat::non_free_page_size` and `DatabaseStat::non_free_page_size`.
//! The feature changes signatures, so it is not additive: a build uses one
//! API or the other. A `[patch]` cannot enable a feature, so a consumer on the
//! fork adds `"unique-txns"` to the features of one of its own `heed`
//! dependencies (milli's, in Meilisearch).
//!
//! ## `unsafe`
//!
//! Only what heed's own signatures force: `Send` for read txns opened without
//! TLS (TXN-13), the TLS-marker retags (`txn.rs`), the lifetime-erased write
//! cursor (`iterator.rs`), the `ReservedSpace` uninitialized view
//! (`reserved_space.rs`), and one `sysconf` for the OS page size (`env.rs`).
//! Each site states its safety contract.

#![allow(clippy::result_large_err)]

pub use byteorder;
pub use heed_traits::{BoxedError, BytesDecode, BytesEncode, Comparator, LexicographicComparator};
pub use heed_types as types;
/// The page-validation policy (ADR-0014; ZeroDB extension).
pub use zerodb::FileTrust;

mod database;
mod env;
mod error;
mod flags;
pub mod iteration_method;
mod iterator;
mod reserved_space;
mod txn;
#[cfg(feature = "unique-txns")]
mod unique;

pub use self::database::{Database, DatabaseOpenOptions, DatabaseStat};
pub use self::env::{
    env_closing_event, CompactionOption, DefaultComparator, Env, EnvClosingEvent, EnvInfo,
    EnvOpenOptions, EnvStat, FlagSetMode, IntegerComparator, DATA_FILE_NAME,
};
pub use self::error::{Error, MdbError, Result};
pub use self::flags::{DatabaseFlags, EnvFlags, PutFlags};
pub use self::iterator::{
    RoIter, RoPrefix, RoRange, RoRevIter, RoRevPrefix, RoRevRange, RwIter, RwPrefix, RwRange,
    RwRevIter, RwRevPrefix, RwRevRange,
};
pub use self::reserved_space::ReservedSpace;
pub use self::txn::{AnyTls, RoTxn, RwTxn, TlsUsage, WithTls, WithoutTls};
#[cfg(feature = "unique-txns")]
pub use self::unique::{AsUniqueTxnRef, UniqueRoTxn, UniqueRwTxn};

/// An unspecified codec placeholder: used when opening a
/// database whose types are remapped per call. Does not implement the codec
/// traits.
pub enum Unspecified {}
