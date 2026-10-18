//! `heed-zerodb` — a 1:1 re-implementation of `heed 0.22.1`'s public surface
//! over ZeroDB's native API (ADR-0003 Option C/D).
//!
//! The crate presents the exact concrete type paths the five consumers name
//! (SPEC 00 §C): `Env<WithoutTls>`, `RoTxn<'a, WithoutTls>`,
//! `RoTxn<'static, WithoutTls>`, `RwTxn<'p>`, `Database<KC, DC>`, the
//! `EnvFlags`/`PutFlags`/`DatabaseFlags` types, `CompactionOption`,
//! `EnvClosingEvent`, `MdbError`, `Error`, `ReservedSpace`, `LazyDecode`,
//! `DecodeIgnore`, and `BytesEncode`/`BytesDecode`/`BoxedError`. It **re-exports
//! `heed-traits` and `heed-types` verbatim**, so codec and trait *identity*
//! (coherence) is preserved: milli's `impl heed::BytesEncode for FooCodec` is an
//! impl of the very same trait the LMDB `heed` uses.
//!
//! Consumers switch backends via `[patch.crates-io] heed = { path =
//! ".../crates/heed-shim" }` — a crate literally named `heed` that re-exports
//! this one (a `package =` rename inside `[patch]` is silently ignored by cargo;
//! see `crates/heed-shim/Cargo.toml` and `docs/CONSUMER-GATE.md`).
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
//! API or the other. Consumers reach it through `crates/heed-shim-unique-txns`.
//!
//! ## Adapter `unsafe` (see per-site `SAFETY:` comments)
//!
//! The adapter reproduces heed's inherently pointer-based model, so it needs the
//! same small `unsafe` heed itself carries: `unsafe impl Send for
//! RoTxn<WithoutTls>` (SPEC 04 TXN-13; every inner ZeroDB txn is itself `Send`,
//! the writer lock being thread-agnostic — `txn.rs`), the `repr(transparent)`
//! TLS-marker deref retags (`txn.rs`), the lifetime-erased write cursor
//! (`iterator.rs`, guarded by heed's documented "no live borrow across a
//! mutating call" contract), the `ReservedSpace` uninit view and its
//! `assume_written` (`reserved_space.rs`, heed's own signatures), and one
//! `sysconf` query for the OS-page-multiple `map_size` check (`env.rs`). Each
//! is documented at its site; this is the minimal adapter-shape `unsafe`,
//! wrapping no UB under the stated contracts.

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

/// An unspecified codec placeholder (SPEC 00 rows 12/51): used when opening a
/// database whose types are remapped per call. Does not implement the codec
/// traits.
pub enum Unspecified {}
