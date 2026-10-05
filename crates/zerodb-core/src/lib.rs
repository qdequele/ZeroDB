//! zerodb-core — pages, B+tree, txns, GC, reader table. No I/O policy.
//!
//! See `docs/SPEC/` for the design.
//!
//! - The [`page`] module — on-disk page formats (SPEC 02).
//! - The [`env`] module — environment open/close, meta selection,
//!   and the same-process registry (SPEC 02 §3.2, SPEC 06 §1, SPEC 04 §7), plus
//!   the [`error`] taxonomy (SPEC 00 rows 54–58). All mmap `unsafe` lives in
//!   `zerodb-io` behind [`env::Backing`]; this crate's only `unsafe` is the
//!   `page::raw` unchecked field readers (see the lint note below).
//! - The [`btree`] read path and the [`rotxn`] read API.
//! - The write path — [`dirty`] (the stable-frame dirty store,
//!   SPEC 04 §6.3), [`rwtxn`] (single-writer txn, COW, split/rebalance, the
//!   commit pipeline with H0–H4 hooks, SPEC 04 §9), and [`check`] (the SPEC 03
//!   §11 invariant walker that `zerodb-tools check` wraps).
//! - The [`readers`] module — the lock-free MVCC reader table
//!   and the published-snapshot cell (SPEC 04 §3/§4, ADR-0006), model-checked
//!   under loom via the [`sync`] shim (`just loom`).

#![deny(missing_docs)]
// `deny`, not `forbid`, since the unchecked page-field readers landed
// (2026-07-22; docs/PERF-GAP-VS-LMDB.md, byte-copy field reads): the AGENTS.md
// unsafe policy sanctions `zerodb-core::page` for page-casting unsafe (the
// unchecked field readers in `page::raw`, allow on its `mod` declaration)
// and, since 2026-10-02 (ADR-0021), `zerodb-core::dirty` for the one call of
// the `unsafe` WRITE_MAP in-place map-slice broker (`dirty::map_mut`, allow
// on that fn, SAFETY contract stated there). The `Backing::map_dirty_page`
// trait method in `env` carries a declaration-only allow (an `unsafe fn`
// signature with a trivially safe default body — ADR-0021 B1 requires the
// broker to be `unsafe fn`). Everything else in this crate remains
// unsafe-free and the lint keeps it that way.
#![deny(unsafe_code)]

pub mod btree;
pub mod builder;
pub mod check;
pub mod cmp;
pub mod dirty;
pub mod env;
pub mod error;
pub mod nested;
pub mod page;
pub(crate) mod readers;
pub mod rotxn;
pub mod rwtxn;
pub(crate) mod stamps;
pub(crate) mod sync;

pub use env::{CommitHook, HookPoint, Snapshot};
pub use nested::NestedRoTxn;
pub use rotxn::{
    collect_entries_flagged, for_each_entry_flagged, named_databases, Database, RoRange, RoTxn,
    TxnRead,
};
pub use rwtxn::{PutFlags, RwCursor, RwTxn};

pub use error::{Error, MdbError, Result};
