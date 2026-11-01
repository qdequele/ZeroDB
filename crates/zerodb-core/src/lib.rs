//! zerodb-core — pages, B+tree, txns, GC, reader table. No I/O policy.
//!
//! See `docs/SPEC/` for the design.
//!
//! - The [`page`] module — on-disk page formats (SPEC 02).
//! - The [`env`](mod@env) module — environment open/close, meta selection,
//!   and the same-process registry (SPEC 02 §3.2, SPEC 06 §1, SPEC 04 §7), plus
//!   the [`error`] taxonomy. All mmap `unsafe` lives in `zerodb-io` behind
//!   [`env::Backing`]; this crate's own `unsafe` is confined to the places
//!   listed in the lint note below.
//! - The [`btree`] read path and the [`rotxn`] read API.
//! - The write path — [`dirty`] (the stable-frame dirty store,
//!   SPEC 04 §6.3), [`rwtxn`] (single-writer txn, COW, split/rebalance, the
//!   commit pipeline with its test hook points, SPEC 04 §9), and [`check`]
//!   (the SPEC 03 §11 invariant walker that `zerodb-tools check` wraps).
//! - The `readers` module — the lock-free MVCC reader table
//!   and the published-snapshot cell (SPEC 04 §3/§4, ADR-0006), model-checked
//!   under loom via the `sync` shim (`just loom`).

#![deny(missing_docs)]
// `deny`, not `forbid`: the unsafe policy (CONTRIBUTING.md) sanctions these places,
// each allowed item by item with its SAFETY contract inline:
// - page casting: the unchecked field readers in `page::raw` (allow on the
//   `mod`), the unchecked cell reads of validated views in `page::tree`, and
//   the `unsafe fn FileTrust::trust_contents` constructor in `page::trust`;
// - the WRITE_MAP in-place map-slice broker (ADR-0021): `dirty::map_mut`, its
//   `&mut self` callers, and the declaration of `Backing::map_dirty_page`;
// - the aarch64 hardware CRC32C call in `page::crc32c` (ADR-0031).
//
// Small hot helpers in `btree` and `page` carry `#[inline(always)]`: LLVM's
// default threshold leaves them out of line on the descent and scan paths.
#![deny(unsafe_code)]

pub mod btree;
pub mod builder;
pub mod check;
pub mod cmp;
pub mod dirty;
pub mod env;
pub mod error;
pub(crate) mod handles;
pub mod nested;
pub mod page;
pub(crate) mod readers;
pub mod rotxn;
pub mod rwtxn;
pub mod shared;
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
