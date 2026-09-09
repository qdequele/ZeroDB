//! `heed-zerodb` — a 1:1 re-implementation of `heed 0.22.1`'s public surface
//! over ZeroDB's native API (ADR-0003 Option C/D; milestone 1.13).
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
//! ## Adapter `unsafe` (see per-site `SAFETY:` comments)
//!
//! The adapter reproduces heed's inherently pointer-based model, so it needs the
//! same small `unsafe` heed itself carries: `unsafe impl Send for
//! RoTxn<WithoutTls>` (SPEC 04 TXN-13; every inner ZeroDB txn is itself `Send`,
//! the writer lock being thread-agnostic — `txn.rs`), the `repr(transparent)` TLS-marker deref retags (`txn.rs`), the
//! lifetime-erased write cursor (`iterator.rs`, guarded by heed's documented
//! "no live borrow across a mutating call" contract), the `ReservedSpace`
//! uninit view and its `assume_written` (`reserved_space.rs`, heed's own
//! signatures), and one `sysconf` query for the D-006 boundary (`env.rs`). Each is documented at its site; this is
//! the vestigial adapter-shape `unsafe` PLAN 1.13 anticipates, wrapping no UB
//! under the stated contracts.

#![allow(clippy::result_large_err)]

pub use byteorder;
pub use heed_traits::{BoxedError, BytesDecode, BytesEncode, Comparator, LexicographicComparator};
pub use heed_types as types;

mod database;
mod env;
mod error;
mod flags;
pub mod iteration_method;
mod iterator;
mod reserved_space;
mod txn;

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

/// An unspecified codec placeholder (SPEC 00 rows 12/51): used when opening a
/// database whose types are remapped per call. Does not implement the codec
/// traits.
pub enum Unspecified {}
