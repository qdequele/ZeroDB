//! zerodb-core — pages, B+tree, txns, GC, reader table. No I/O policy.
//!
//! See `PLAN.md` and `docs/SPEC/` for the design.
//!
//! - Milestone 1.1: the [`page`] module — on-disk page formats (SPEC 02).
//! - Milestone 1.2: the [`env`] module — environment open/close, meta selection,
//!   and the same-process registry (SPEC 02 §3.2, SPEC 06 §1, SPEC 04 §7), plus
//!   the [`error`] taxonomy (SPEC 00 rows 54–58). All mmap `unsafe` lives in
//!   `zerodb-io` behind [`env::Backing`]; this crate contains none.
//! - Milestone 1.3: the [`btree`] read path and the [`rotxn`] read API.
//! - Milestone 1.4: the write path — [`dirty`] (the stable-frame dirty store,
//!   SPEC 04 §6.3), [`rwtxn`] (single-writer txn, COW, split/rebalance, the
//!   commit pipeline with H0–H4 hooks, SPEC 04 §9), and [`check`] (the SPEC 03
//!   §11 invariant walker the M1.12 tool will wrap).
//! - Milestone 1.8: the [`readers`] module — the lock-free MVCC reader table
//!   and the published-snapshot cell (SPEC 04 §3/§4, ADR-0006), model-checked
//!   under loom via the [`sync`] shim (`just loom`).

// `deny`, not `forbid`, since PERF-GAP A3 (2026-07-22): the CLAUDE.md unsafe
// policy sanctions `zerodb-core::page` for page-casting unsafe, and the A3
// unchecked field readers live in exactly one module there — `page::raw`,
// which carries the only `#[allow(unsafe_code)]` in the crate (on its `mod`
// declaration, with the safety contract in the module docs). Everything else
// in this crate remains unsafe-free and the lint keeps it that way.
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
pub(crate) mod sync;

pub use env::{CommitHook, HookPoint, Snapshot};
pub use nested::NestedRoTxn;
pub use rotxn::{
    collect_entries_flagged, for_each_entry_flagged, named_databases, Database, RoRange, RoTxn,
    TxnRead,
};
pub use rwtxn::{PutFlags, RwCursor, RwTxn};

pub use error::{Error, MdbError, Result};
