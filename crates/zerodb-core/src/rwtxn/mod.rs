//! The write transaction: single-writer `RwTxn`, COW mutation, and the commit
//! pipeline (SPEC 04 §2/§6/§8/§9, SPEC 03 §5–§10, ADR-0004).
//!
//! ## Shape (ADR-0004)
//!
//! [`Env::write_txn`] acquires the env's in-process write mutex (TXN-6) and
//! holds the guard for the txn's whole life. All mutation runs against the
//! txn-private [`DirtyStore`] (TXN-41 stable frames); reads on the txn resolve
//! dirty frames first, then the read-only map (TXN-38), through the same
//! [`Database`] read API as a `RoTxn` (via [`TxnRead`]). heed's borrow model is
//! the compile-time enforcement of the value-borrow contract: reads take
//! `&RwTxn`, mutations `&mut RwTxn`, so no `&'txn [u8]` can span a mutation
//! (TXN-39).
//!
//! ## COW, allocation, and GC (SPEC 03 §5/§8, SPEC 05; ADR-0005)
//!
//! First touch copies a committed page into a fresh frame under a **new** pgno
//! and rewrites the parent chain top-down (§5.1/§5.3). Allocation follows
//! GC-16: the loose-page list (GC-7/8) for single pages, then a GC-DB draw
//! (`RwTxn::gc_reclaim`, gated by the oldest live reader — the
//! lock-free reader-table scan, SPEC 04 TXN-20/21, cached per txn per
//! TXN-22), then file extend
//! (`next_pgno`, GC-15) with the GC-17 `MapFull` bound. Draws are recorded in
//! the in-memory drain map (GC-20) and applied to the GC tree at commit. At
//! C1, `RwTxn::freelist_save` rewrites drained entries, releases trailing
//! loose pages (GC-10), and writes this txn's freed set under
//! `BE(writer_txnid)` in a fixed-point loop (GC-11..13) during which
//! `allocate` is restricted to loose pages / extend only (GC-12,
//! `AllocMode::GcSave`). The GC tree is mutated by the **same** split/COW
//! code as the main tree, selected by `TreeId`.
//!
//! ## Commit (SPEC 04 §9, SPEC 06 §2)
//!
//! [`RwTxn::commit`] runs the C0–C6 steps in **one function**
//! (`RwTxn::commit_pipeline`) with the always-compiled H0–H4 crash hooks
//! between steps (ADR-0004). The single load-bearing ordering (REC-7):
//! dirty pages are written (C2) and fsynced (C3) strictly before the meta is
//! written (C4) to slot `txnid & 1` (TXN-63) and fsynced (C5); the in-memory
//! snapshot is published last (C6, TXN-18/19 order). A failed fsync poisons
//! the env (REC-13).
//!
//! ## txnid assignment note (TXN-2 vs TXN-63)
//!
//! `writer_txnid` is always `last_committed_txnid + 1`, so an aborted txn's id
//! is **reused** by the next writer. TXN-2 explicitly permits this (no page or
//! meta ever recorded the aborted id), and the slot-parity scheme **requires**
//! it: slot `N & 1` must be the older slot (TXN-63), which only holds when
//! commit ids are consecutive. Non-reuse would make a post-abort commit
//! overwrite the *live* snapshot's slot.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use crate::btree::{
    branch_view, leaf_view, node_view, node_view_classified, Cursor as BCursor, NodeView,
    SavedCursor, Source, Tree, ValidatedPages,
};
use crate::dirty::{DirtyCarry, DirtyStore, PgnoBuildHasher};
use crate::env::{Env, HookPoint, Snapshot};
use crate::error::{Error, MdbError, Result};
use crate::handles::{HandleTable, TxnHandles};
use crate::nested::{ChildCounter, NestedRoTxn};
use crate::page::geometry::{
    body_size, gc_key_decode, gc_key_encode, is_map_full, map_pages, overflow_page_count,
    pil_decode, pil_encode_into, pil_size, value_is_inline,
};
use crate::page::{
    write_overflow_head, BranchMut, BranchRef, DBRecord, LeafMut, LeafRef, LeafValue, MetaPage,
    PageError, PageRef, PageType, DBRECORD_LEN, FILL_THRESHOLD_PERMILLE, FIRST_DATA_PGNO,
    FORMAT_VERSION, F_SUBDATA, HEADER_SIZE, MAGIC, MAX_DATA_SIZE, MAX_DB_NAME, MAX_KEY_SIZE,
    MIN_KEYS_BRANCH, MIN_KEYS_LEAF, PGNO_INVALID,
};
use crate::rotxn::{map_page_err, resolve_named_record, Database, DbSel, TxnRead};

/// Round `n` up to the next even number (2-byte cell alignment, SPEC 02 §2.2).
#[inline]
fn even(n: usize) -> usize {
    (n + 1) & !1
}

/// Saturating in-place stat update. `DBRecord` stats (`entries`,
/// `leaf_pages`, `branch_pages`, `overflow_pages`, `depth`) are **on-disk
/// data**: a hostile file can carry `u64::MAX` or `0`, so the write path's
/// `+= 1` / `-= 1` bookkeeping must never overflow-panic (debug) or wrap
/// (release). Saturation is exact on every non-corrupt file (real counters
/// are nowhere near the bounds); on a corrupt one the drifted stat is caught
/// by the checker (INV-18), not by an arithmetic panic mid-mutation.
fn sat_add(v: &mut u64, n: u64) {
    *v = v.saturating_add(n);
}

/// See [`sat_add`].
fn sat_sub(v: &mut u64, n: u64) {
    *v = v.saturating_sub(n);
}

/// A root-to-leaf descent path: `(pgno, ki)` per level (SPEC 03 §1 cursor
/// shape). `ki` is the chosen child index on branches and the entry/insertion
/// slot on the leaf.
type Path = Vec<(u64, usize)>;

/// Which B+tree a mutation targets (ADR-0005). The GC (free) DB is an
/// ordinary tree of `P_LEAF`/`P_BRANCH` pages (SPEC 02 §7), so it is mutated
/// by exactly the same split/merge/COW code as the main tree — this selector
/// is the only difference, and it is the seam the named-DB catalog extends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TreeId {
    /// The main (unnamed) database — also the named-DB catalog (SPEC 02 §6).
    Main,
    /// The free (GC) database, `BE(txnid) → PIL` (SPEC 05 §1).
    Free,
    /// A named database, addressed by its env-level dbi index. Its
    /// working record lives in [`RwTxn::open`], loaded from the catalog on
    /// first touch and written back at commit (SPEC 04 TXN-10).
    Named(u32),
}

/// Most entries a carried `Vec` keeps of its capacity ([`WriterCarry`]): a
/// `Vec` is walked over its length only, so its capacity costs memory, not
/// time; this bounds what one large txn leaves allocated.
const CARRY_VEC_CAP: usize = 4096;

/// Empty `v`, keeping at most `cap` entries of capacity.
fn recycle_vec<T>(v: &mut Vec<T>, cap: usize) {
    v.clear();
    if v.capacity() > cap {
        v.shrink_to(cap);
    }
}

/// What a write txn leaves for the next write txn of its env: its
/// containers, emptied, with their allocations; the frame pool; the
/// named-database names it looked up; and the snapshot it ended with.
/// Carried on the writer slot (`crate::env::WriterGuard`), so taking and
/// returning it costs no lock of its own. LMDB likewise allocates its write
/// transaction once per environment (`me_txn0`) and reuses it.
///
/// Every container comes back empty. Capacity is kept up to a bound
/// ([`crate::dirty::CARRY_INDEX_CAP`] for hash tables, [`CARRY_VEC_CAP`]
/// for lists), so a large txn does not leave a large allocation behind.
///
/// A write txn uses these containers in place, through its `work` box
/// (`work!` / `work_mut!`), so beginning and ending a txn moves one pointer
/// rather than every container.
#[derive(Default)]
pub(crate) struct WriterCarry {
    /// The dirty store's indexes and spare frames, between txns; `None`
    /// while a txn's [`DirtyStore`] holds them.
    dirty: Option<DirtyCarry>,
    /// Every pgno handed out by a GC draw this txn (ADR-0005): feeds the
    /// generalized TXN-62 assert at C2 and the loose classification in
    /// [`RwTxn::free_page`]. Pgno-hashed: engine-authored keys, no HashDoS
    /// surface.
    reclaimed: HashSet<u64, PgnoBuildHasher>,
    /// Committed pages obsoleted by this txn (GC-6). Written to the GC DB
    /// under `BE(writer_txnid)` at commit step C1 (`freelist_save`).
    freed: Vec<u64>,
    /// Pages allocated *and* freed by this txn (GC-7): the reuse fast path
    /// (GC-8) and the trailing-shrink set (GC-10). A GC-reclaimed page freed
    /// again this txn also lands here (it is this-txn-private after the gate).
    loose: Vec<u64>,
    /// Merge buffer for the commit's annex settle (`freelist_save`).
    merged: Vec<u64>,
    /// The commit's sorted dirty-pgno list (step C2).
    pgnos: Vec<u64>,
    /// The order `flush_catalog` writes the dirty named records in.
    catalog: Vec<u32>,
    /// The open table's slots between txns, every entry `None`.
    open: Vec<Option<NamedTree>>,
    /// Named-database names carried from earlier write txns, by dbi, with
    /// the handle generation they belong to (`NamedTree::generation`).
    names: Vec<Option<(u32, Box<[u8]>)>>,
    /// Descent-path buffer reused by every put and delete: taken with
    /// `mem::take` for the op and put back after, so only the first op
    /// allocates, as LMDB allocates its cursor stack once.
    path_buf: Path,
    /// Byte buffer the delete rebalance moves cells through (ADR-0028,
    /// SPEC 03 §10 REB-2/REB-4): a cell or a whole page is copied out of
    /// one dirty frame, then written into another. Kept across ops, so only
    /// the first rebalance allocates; it avoids holding two dirty frames at
    /// once, which under `WRITE_MAP` would take a second live `&mut` into
    /// the map.
    cell_scratch: Vec<u8>,
    /// Rightmost-leaf fingers, at most one per tree (SPEC 03 §6.6): the
    /// cached right-spine descent a sequential/APPEND put reuses. Writer
    /// private. Dropped by every structural or ownership change of its pages
    /// (see [`RwTxn::finger_forget`]) and emptied at every txn end. A small
    /// `Vec` scanned linearly: a txn touches a handful of trees.
    fingers: Vec<Finger>,
    /// The handle-check memo allocation ([`TxnHandles::recycle`]).
    handles: Option<Box<crate::handles::Rare>>,
    /// The snapshot the last write txn published, or its base when it
    /// published nothing: the next write txn's base when it is still the
    /// latest ([`crate::env::EnvInner::writer_base`]).
    snapshot: Option<Arc<Snapshot>>,
}

impl std::fmt::Debug for WriterCarry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriterCarry")
            .field("dirty", &self.dirty)
            .field("snapshot", &self.snapshot.as_ref().map(|s| s.txnid))
            .finish_non_exhaustive()
    }
}

/// A write txn's [`WriterCarry`], shared, through its `work` field alone (so
/// the txn's other fields stay borrowable beside it).
macro_rules! work {
    ($txn:expr) => {
        $txn.work
            .as_deref()
            .expect("a write txn holds its work buffers until it drops")
    };
}

/// [`work!`], mutable.
macro_rules! work_mut {
    ($txn:expr) => {
        $txn.work
            .as_deref_mut()
            .expect("a write txn holds its work buffers until it drops")
    };
}

mod alloc;
mod api;
mod catalog;
mod commit;
mod cursor;
mod delete;
mod finger;
mod freelist;
mod insert;
mod spill;
#[cfg(test)]
mod tests;

pub(crate) use alloc::validate_pil_ids;
use alloc::*;
use catalog::*;
pub use cursor::RwCursor;
use cursor::*;
use delete::*;
use finger::*;
pub use insert::PutFlags;
use insert::*;

fn poisoned_error() -> Error {
    Error::Io(std::io::Error::other(
        "environment poisoned by a failed durability barrier (SPEC 06 REC-13)",
    ))
}

/// The `EACCES` a write txn on a read-only env returns (SPEC 01 Table 1, TXN-8).
/// The fork's `mdb_txn_begin` returns `EACCES`, which heed surfaces as
/// `Error::Io(PermissionDenied)` (os error 13); ZeroDB returns the identical
/// value so the oracle taxonomy agrees.
fn read_only_error() -> Error {
    Error::Io(std::io::Error::from_raw_os_error(13))
}

/// The `WRITE_MAP` arm of a write-txn begin (ADR-0023): grow the data file
/// to this process's logical map size. Out of line so the default write-txn
/// begin stays small enough for its own callees to be inlined.
#[cold]
#[inline(never)]
fn grow_file_to_map(inner: &crate::env::EnvInner) -> Result<()> {
    inner
        .backing_ref()
        .ensure_file_len(inner.map_size())
        .map_err(Error::Io)
}

// ---------------------------------------------------------------------------
// RwTxn
// ---------------------------------------------------------------------------

/// The single write transaction (SPEC 04 §2/§8). Holds the env's write-mutex
/// guard for its whole life (TXN-6); dropping it without [`RwTxn::commit`] is
/// an abort (TXN-59/60 — the dirty set vanishes, the disk is untouched).
pub struct RwTxn<'env> {
    env: &'env Env,
    /// TXN-6: released when the txn drops (commit or abort) — from whatever
    /// thread that happens on (`RwTxn` is `Send`; the guard is the
    /// thread-agnostic [`crate::env`] writer-lock guard, not a
    /// `std::sync::MutexGuard`).
    _guard: crate::env::WriterGuard<'env>,
    /// The mapped region (fallback source for untouched pages, TXN-38), heap
    /// modes only, re-derived at every spill (TXN-71). Under in-place
    /// `WRITE_MAP` (TXN-45b) it is the **empty slice**: a stale whole-map
    /// reference field would be retagged on every move of the txn, which is
    /// UB at the written locations, so [`RwTxn::whole_map`] borrows lazily
    /// instead (ADR-0021).
    bytes: &'env [u8],
    /// The snapshot this txn grew from (`writer_txnid = base.txnid + 1`).
    /// `Some` until the txn's drop hands it to the [`WriterCarry`]; read it
    /// through [`RwTxn::base`].
    base: Option<Arc<Snapshot>>,
    /// The snapshot this txn published at commit step C6, for the
    /// [`WriterCarry`]; `None` until then.
    published: Option<Arc<Snapshot>>,
    /// This txn's working lists and buffers ([`WriterCarry`]), used in
    /// place: they came from the writer slot with their allocations and go
    /// back to it, emptied, when the txn drops. `Some` until then; read
    /// them through `work!` / `work_mut!`.
    work: Option<Box<WriterCarry>>,
    txnid: u64,
    psize: u32,
    dirty: DirtyStore<'env>,
    /// Map-sourced pages fully validated this txn. Sound for a writer
    /// because a map page's bytes change during the txn only at pgnos this
    /// txn has dirtied (TXN-45a/45b), and the dirty store is probed before
    /// the memo; spills reset the memo (TXN-71). Probes, never publishes,
    /// the env-wide stamp cache (ADR-0018).
    validated: ValidatedPages<'env>,
    /// GC-20 drain bookkeeping (ADR-0005): for every GC entry `F` this txn
    /// drew from, the **remaining** (still-free, sorted-ascending) ids —
    /// [`Drain::live`], a consumed-prefix view so the GC-19 front pop is O(1).
    /// Loaded from the tree PIL on first touch; `freelist_save` step (a)
    /// rewrites the remainder (or deletes the entry when empty). `BTreeMap` so
    /// iteration is ascending-`F`, matching the GC-18 scan.
    drains: BTreeMap<u64, Drain>,
    /// The base meta's free-list annex pool (SPEC 05 §2a, ADR-0022): the
    /// pages freed by txn `base.txnid`, parsed (and `validate_pil_ids`-
    /// checked, GC-33) at begin when `base.free_annex > 0`. Drawn by
    /// [`RwTxn::annex_draw`] (allocate step 2b, gated per GC-31); whatever
    /// remains is folded into `freed` at the top of `freelist_save` (the
    /// GC-30 carry) — by then the pool is empty, so GcSave never sees it.
    annex: Drain,
    /// `freelist_save`'s GC-32 placement decision: `true` = this txn's own
    /// entry rides in the meta annex (C4 encodes `freed` as `fl_ids`; no
    /// GC-tree entry `BE(txnid)` exists), `false` = the v1 tree arm ran (or
    /// `freed` is empty) and the annex is empty.
    annex_out: bool,
    /// GC-12 allocation restriction (ADR-0005).
    alloc_mode: AllocMode,
    /// Entries of `drains` whose `remaining` shrank via an **in-save pool
    /// draw** (GC-12 — see `freelist_save`): each must be
    /// re-rewritten before the C1 fixed point is declared, so no rewritten
    /// entry ever lists a handed-out page.
    save_touched: std::collections::BTreeSet<u64>,
    /// Next never-allocated pgno (GC-15); persisted as `last_pg = next_pgno-1`.
    next_pgno: u64,
    /// `base.last_pg` — the committed high-water, used to classify freed pages
    /// as loose (`> committed_last_pg` = this-txn allocation) and to assert
    /// TXN-62 at C2.
    committed_last_pg: u64,
    /// Working roots/stats (TXN-56); written to the meta at commit.
    main_db: DBRecord,
    free_db: DBRecord,
    /// Per-txn named-DB working records (the dbi table's txn half),
    /// indexed by dbi. Loaded lazily from the catalog on first touch;
    /// `dirty` entries are written back to the main tree at commit before
    /// `freelist_save` ([`RwTxn::flush_catalog`]). Dropped-this-txn entries are
    /// removed here (their catalog entry is deleted eagerly at drop time).
    open: OpenTable,
    /// This txn's state in the env's slot table (ADR-0024).
    handles: TxnHandles,
    /// The main DB's sequential-writes setting (ADR-0015), resolved at txn
    /// begin; named DBs carry theirs in [`NamedTree::seq`].
    seq_main: bool,
    /// The env's dirty limit in pages (SPEC 04 TXN-68, ADR-0017): past it,
    /// the next mutating call spills pages to the file.
    dirty_limit: u64,
    /// Spills this txn has done (diagnostics; SPEC 04 TXN-69).
    spills: u64,
    /// The bound on this txn's map reads (SPEC 04 TXN-38/71): the committed
    /// high-water, raised past the highest page each spill writes. The spill
    /// wrote that page, so the file backs every page up to here (holes read
    /// as zeros), and a stray reference fails typed rather than faulting.
    read_high: u64,
    /// Fast-path hits this txn — a test signal that the finger actually
    /// fires (like `RwCursor::repaired_keeps`). Debug builds only.
    #[cfg(debug_assertions)]
    finger_hits: u64,
    /// LMDB `MDB_TXN_ERROR` parity: a mid-mutation failure (e.g. `MapFull`
    /// inside a split cascade) leaves the working tree partial, so every later
    /// mutation and `commit` returns `BadTxn`; only abort is valid.
    errored: bool,
    /// Per-txn cache of the GC reuse gate (SPEC 04 TXN-22, ADR-0006):
    /// computed by this txn's first `gc_reclaim` and reused for its other
    /// draws. Caching is only ever more conservative (a reader that releases
    /// mid-txn is just not reclaimed against). The debug shadow check
    /// re-scans at every draw.
    oldest_cache: Option<u64>,
    /// The smallest run length the cross-record run search
    /// ([`RwTxn::cross_record_run`]) has looked for and not found this txn,
    /// after reading every record the gate admits. During the txn's
    /// operations the reusable pages only ever shrink (draws remove ids;
    /// nothing adds any until `freelist_save`), so a later request for a run
    /// at least as long cannot succeed either and skips the search.
    cross_miss: Option<u64>,
    /// The cross-record run search's merged list of free ids, built on its
    /// first use in this txn ([`RwTxn::cross_record_run`]).
    cross_cache: Option<CrossRunCache>,
    /// Set by `freelist_save` around the one put that grows this txn's own
    /// GC entry after it has shrunk in the same save (SPEC 05 GC-13): the
    /// in-save allocator then skips the carried annex pool, whose ids are
    /// part of that entry, so the grow takes pages from outside it (the
    /// drain pool or the end of the file) and the save reaches its fixed
    /// point instead of trading pages between the entry and its own run.
    save_outside: bool,
    /// Live nested read children (SPEC 04 TXN-31, ADR-0007), checked by
    /// [`RwTxn::guard_ok`] (TXN-29) and at commit C0 (TXN-33).
    ///
    /// **`RwTxn` must stay `Sync`:** `NestedRoTxn` is `Send` only because
    /// `&RwTxn: Send`. No field may add interior mutability usable through
    /// `&self` (`Cell`, `RefCell`, …); use an atomic or keep it behind
    /// `&mut`. `rwtxn_sync_contract_and_child_send` in `crate::nested` makes
    /// a violation a build failure.
    children: ChildCounter,
}

impl Env {
    /// Begin the write transaction (SPEC 00 row 27, SPEC 04 TXN-6/56). Blocks
    /// until any current writer finishes (TXN-7); readers are unaffected
    /// (TXN-9).
    ///
    /// # Errors
    ///
    /// [`Error::Io`] if the env is poisoned by an earlier fsync failure
    /// (SPEC 06 REC-13). [`MdbError::MapResized`] if the newest snapshot lies
    /// beyond this process's logical map size: another process with a larger
    /// map committed past it (ADR-0023).
    pub fn write_txn(&self) -> Result<RwTxn<'_>> {
        let inner = self.inner();
        // TXN-8 / SPEC 01 Table 1: a write txn on a read-only env is
        // `EACCES` (the fork's `mdb_txn_begin` returns `EACCES` when
        // `me_flags & MDB_RDONLY`). Checked before the write mutex is taken —
        // an RDONLY env never has a writer.
        if inner.is_read_only() {
            return Err(read_only_error());
        }
        if inner.is_poisoned() {
            return Err(poisoned_error());
        }
        let mut guard = inner.lock_writer();
        // In a lock-file environment, also the cross-process writer lock
        // (ADR-0023 §2); a no-op otherwise.
        inner.lock_writer_shared(&mut guard)?;
        // Recheck under the lock: a concurrent commit may have failed while we
        // blocked on the mutex.
        if inner.is_poisoned() {
            return Err(poisoned_error());
        }
        // Under WRITE_MAP the data file grows to this process's logical map
        // size here, with both writer locks held (and after any dead-owner
        // takeover reap), so every page this txn can allocate below the map
        // size is inside the file before anything is written through the
        // map (ADR-0023). A joiner's open never grows the file.
        let write_map = inner.durability().write_map;
        if write_map {
            grow_file_to_map(inner)?;
        }
        // The latest published snapshot, which in a lock-file env may be
        // another process's commit (ADR-0023 §4). The previous write txn's
        // snapshot, carried on the writer slot, serves when it is still the
        // latest.
        let cached = guard.carry().snapshot.take();
        let base = inner.writer_base(cached)?;
        // The map view, loaded once for the whole begin: the base check
        // below and the txn's read view (`bytes`) both come from it.
        let backing = inner.backing_ref();
        let view = backing.view();
        // The base must lie inside this process's logical map size, even
        // when it came from the cache; otherwise the begin fails with
        // `MapResized` (ADR-0023). Either error drops `guard`, releasing both
        // writer locks.
        inner.check_base_in(view, &base)?;
        // ADR-0021 (SPEC 04 §6.4 TXN-45b): under WRITE_MAP, with a backing
        // that brokers map slices, dirty pages are realized **in the map** at
        // their freshly-COW'd pgnos — no heap staging, no commit write-back.
        // Every other combination keeps the heap-staged store (TXN-45a):
        // the default mode, the miri test backing, the fault-injection
        // backend (whose crash journal only sees fd-path writes).
        let in_map = write_map && backing.dirty_in_map();
        // TXN-14 runtime half of the open-time bound (SPEC 06 REC-1a): the
        // next txnid must stay 2^32 below the reader-table sentinel band, so
        // an accepted file cannot commit its way into RDR_CLAIMED/RDR_FREE.
        // Unreachable by any honest store (2^64 - 2^32 commits); a hit means a
        // corrupt or hostile meta that slipped past open.
        if base.txnid >= crate::readers::MAX_COMMITTED_TXNID {
            return Err(Error::Mdb(MdbError::Invalid));
        }
        // The base meta's free-list annex pool (SPEC 05 §2a, ADR-0022). The
        // ids are pinned in the snapshot (the slot is overwritten two
        // commits later, TXN-63) and are never trusted (GC-33): they must
        // pass the same ascending/range validation as a tree PIL before any
        // can be handed out — open-time slots only had the count bound and
        // the CRC checked.
        let annex = if base.free_annex.is_empty() {
            Drain::new(Vec::new())
        } else {
            validate_pil_ids(&base.free_annex, base.last_pg)?;
            Drain::shared(Arc::clone(&base.free_annex))
        };
        // The previous write txn's containers, and the frames recycled from
        // earlier write txns (LMDB's `me_dpages`) as the dirty store's spare
        // list. They came with the writer slot, so this takes no lock.
        let mut work = guard.take_carry();
        let dirty = DirtyStore::from_carry(
            inner.page_size(),
            work.dirty.take().unwrap_or_default(),
            in_map.then_some(backing),
        );
        Ok(RwTxn {
            txnid: base.txnid + 1, // TXN-2 (+ the reuse note in the module docs)
            next_pgno: base.last_pg + 1,
            committed_last_pg: base.last_pg,
            main_db: base.main_db,
            free_db: base.free_db,
            open: OpenTable {
                slots: std::mem::take(&mut work.open),
            },
            handles: inner.handles().begin_reusing(work.handles.take()),
            seq_main: inner.sequential_writes_for(None),
            dirty_limit: inner.dirty_limit(),
            spills: 0,
            read_high: base.last_pg,
            #[cfg(debug_assertions)]
            finger_hits: 0,
            psize: inner.page_size(),
            dirty,
            // ADR-0018: the writer probes the env
            // cache (the previous commit seeded it with the very pages this
            // txn's first descent walks) but never publishes from a miss.
            validated: ValidatedPages::for_writer(inner.file_trust(), Some(inner.stamp_cache())),
            drains: BTreeMap::new(),
            annex,
            annex_out: false,
            alloc_mode: AllocMode::Normal,
            save_touched: std::collections::BTreeSet::new(),
            errored: false,
            oldest_cache: None,
            cross_miss: None,
            cross_cache: None,
            save_outside: false,
            children: ChildCounter::new(),
            // In-place WRITE_MAP keeps no whole-map reference in the struct
            // (see the field docs; ADR-0021) — `whole_map` borrows lazily.
            bytes: if in_map { &[] } else { view.bytes },
            base: Some(base),
            published: None,
            work: Some(work),
            _guard: guard,
            env: self,
        })
    }
}

impl TxnRead for RwTxn<'_> {
    fn source(&self) -> Source<'_> {
        Source::Writer {
            dirty: &self.dirty,
            bytes: self.whole_map(),
            // Bounds the map fallback only: pages this txn allocated beyond
            // the base snapshot live in `dirty` and resolve before the bound,
            // or were spilled below `read_high` (SPEC 04 TXN-71).
            last_pg: self.read_high,
        }
    }
    fn main_record(&self) -> &DBRecord {
        &self.main_db
    }
    fn free_record(&self) -> &DBRecord {
        &self.free_db
    }
    fn free_annex_count(&self) -> u64 {
        // Working-state view (like `free_record`): the base annex pool's
        // live remainder — drawn ids are no longer free.
        self.annex.len() as u64
    }
    fn page_size(&self) -> u32 {
        self.psize
    }
    fn handle_state(&self) -> (&HandleTable, &TxnHandles, Option<&TxnHandles>) {
        (self.env.inner().handles(), &self.handles, None)
    }
    fn record_for(&self, db: &Database) -> DBRecord {
        match db.sel() {
            DbSel::Main => self.main_db,
            DbSel::Named(dbi) => {
                // The writer's own working record if the DB was touched this
                // txn (uncommitted state, TXN-38); otherwise resolve from the
                // working catalog (which itself reflects uncommitted catalog
                // inserts through the dirty-frame source).
                if let Some(t) = self.open.get(dbi) {
                    return t.rec;
                }
                match self.env.inner().named_name(dbi) {
                    Some(name) => {
                        resolve_named_record(self.source(), self.psize, &self.main_db, &name)
                    }
                    None => DBRecord::empty(),
                }
            }
        }
    }
    fn comparator_for(&self, sel: DbSel) -> crate::cmp::KeyCmp<'_> {
        self.env.inner().comparator_for(sel)
    }
    fn validated_pages(&self) -> Option<&ValidatedPages<'_>> {
        Some(&self.validated)
    }
}

impl<'env> RwTxn<'env> {
    /// The identity of the environment this transaction belongs to
    /// ([`Env::ident`]).
    #[must_use]
    pub fn env_ident(&self) -> usize {
        self.env.ident()
    }

    /// The env this txn writes (for `crate::nested`).
    pub(crate) fn env_inner(&self) -> &crate::env::EnvInner {
        self.env.inner()
    }

    /// This writer's txnid (`last committed + 1`, SPEC 04 TXN-2/5).
    #[must_use]
    pub fn txnid(&self) -> u64 {
        self.txnid
    }

    /// Abort: drop the dirty set and freed lists, change nothing on disk,
    /// release the write mutex (SPEC 04 TXN-59). Equivalent to dropping.
    ///
    /// Nested read children must be gone first (TXN-33) — enforced at
    /// compile time: `abort(self)` is by-value, and a live child holds a
    /// shared borrow of this txn. (There is deliberately no drop-time
    /// quiescence assert: a nonzero counter at drop is reachable *only* via
    /// the sound `mem::forget(child)` degradation — see `crate::nested`.)
    pub fn abort(self) {
        drop(self);
    }

    /// Open a **nested read transaction**: a read-only child of this write
    /// txn that sees its *uncommitted, in-progress* state (SPEC 00 row 16,
    /// SPEC 04 §5 TXN-26..28 — the fork's ITS#10395 semantics; heed:
    /// `RwTxn::nested_read_txn`). Reads go through this txn's dirty frames
    /// first, then the map (TXN-27); no reader-table slot is claimed
    /// (TXN-25/32).
    ///
    /// Arbitrarily many children may be open concurrently (TXN-28); each is
    /// `Send`, so the milli/hannoy pattern — open `N` children, move one to
    /// each rayon worker, read in parallel while the writer is paused, join,
    /// resume — works as with the fork. While any child is alive this txn
    /// cannot mutate, commit, or abort: the child's shared borrow enforces it
    /// at compile time (TXN-30), and every mutating op additionally refuses
    /// with [`MdbError::BadTxn`] at runtime (TXN-29). The writer
    /// resumes implicitly when the last child drops.
    ///
    /// # Errors
    ///
    /// [`MdbError::BadTxn`] if this txn is errored (fork parity:
    /// `mdb_txn_begin` on an `MDB_TXN_ERROR` parent).
    pub fn nested_read_txn(&self) -> Result<NestedRoTxn<'_>> {
        if self.errored {
            return Err(Error::Mdb(MdbError::BadTxn));
        }
        Ok(NestedRoTxn::open(self))
    }

    /// The live-child counter (ADR-0007) — for `crate::nested` only.
    pub(crate) fn children(&self) -> &ChildCounter {
        &self.children
    }

    fn guard_ok(&self) -> Result<()> {
        if self.errored {
            return Err(Error::Mdb(MdbError::BadTxn));
        }
        // TXN-29 (ADR-0007): while any nested read child is live,
        // the dirty set is frozen — every mutating entry funnels through here
        // and refuses with BadTxn (LMDB `MDB_BAD_TXN` = "transaction … has a
        // child") in BOTH debug and release. Normally unreachable (the child
        // holds a shared `&RwTxn` borrow, so no `&mut` op can even be
        // called); it backstops `unsafe`/FFI lifetime erasure (e.g. the
        // oracle). The `Acquire` inside `live()` pairs with each child-drop's
        // `Release` (see `crate::nested`).
        if self.children.live() != 0 {
            return Err(Error::Mdb(MdbError::BadTxn));
        }
        Ok(())
    }

    /// The key ordering of `tree` (SPEC 03 §2.0).
    ///
    /// `Main` and `Free` are memcmp **unconditionally**: `Main` doubles as the
    /// named-DB catalog and `Free` is keyed by big-endian txnids whose memcmp
    /// order *is* their numeric order (SPEC 05 §1), which the GC gate depends
    /// on. Only a named DB can carry a caller's comparator.
    fn tree_comparator(&self, tree: TreeId) -> crate::cmp::KeyCmp<'_> {
        match tree {
            TreeId::Main | TreeId::Free => crate::cmp::KeyCmp::Default,
            TreeId::Named(dbi) => self.env.inner().comparator_for(DbSel::Named(dbi)),
        }
    }

    /// The working `DBRecord` of `tree` (ADR-0005 selector). A named tree's
    /// record must have been loaded by [`RwTxn::ensure_open`] first.
    fn record(&self, tree: TreeId) -> &DBRecord {
        match tree {
            TreeId::Main => &self.main_db,
            TreeId::Free => &self.free_db,
            TreeId::Named(dbi) => {
                &self
                    .open
                    .get(dbi)
                    .expect("named record loaded before use")
                    .rec
            }
        }
    }

    /// Mutable working `DBRecord` of `tree`. Touching a named tree's record
    /// marks it dirty (its catalog entry is rewritten at commit, SPEC 02 §6).
    fn record_mut(&mut self, tree: TreeId) -> &mut DBRecord {
        match tree {
            TreeId::Main => &mut self.main_db,
            TreeId::Free => &mut self.free_db,
            TreeId::Named(dbi) => {
                let e = self
                    .open
                    .get_mut(dbi)
                    .expect("named record loaded before use");
                e.dirty = true;
                &mut e.rec
            }
        }
    }

    /// Ensure the named DB `dbi`'s working record is loaded into [`RwTxn::open`]
    /// (from the catalog view, or empty if the entry does not yet exist), and
    /// return its tree selector. Idempotent. The main DB needs no loading.
    fn ensure_open(&mut self, db: &Database) -> Result<TreeId> {
        // TXN-29 airtightness: `ensure_open` mutates the open table, which
        // live children read through `record_for` — so the child guard fires
        // here too, *before* the table is touched (the page-mutation entries
        // it precedes re-check via their own `guard_ok`).
        self.guard_ok()?;
        // SPEC 04 TXN-68: every page-mutating `Database`/`RwCursor` entry
        // passes here first, with no path, view or frame borrow live.
        self.maybe_spill()?;
        let dbi = match db.sel() {
            DbSel::Main => return Ok(TreeId::Main),
            DbSel::Named(dbi) => dbi,
        };
        // ADR-0024: every write and every write-cursor step on a named
        // database checks its handle first.
        self.check_db(db)?;
        if !self.open.contains(dbi) {
            // The name the previous write txn carried for this slot and
            // generation, if any (`NamedTree::generation`): the same name
            // the table would return, without its lock or an allocation.
            let name = match work_mut!(self)
                .names
                .get_mut(dbi as usize)
                .and_then(Option::take)
            {
                Some((g, name)) if g == db.generation() => name,
                _ => self
                    .env
                    .inner()
                    .named_name(dbi)
                    .expect("named dbi is assigned before a handle exists"),
            };
            let rec = resolve_named_record(self.source(), self.psize, &self.main_db, &name);
            let t = self.named_tree(dbi, name, Some(db.generation()), rec);
            self.open.insert(dbi, t);
        }
        Ok(TreeId::Named(dbi))
    }

    /// The whole-map read view this txn's resolutions go through (TXN-38).
    ///
    /// Under in-place `WRITE_MAP` (ADR-0021, TXN-45b) the writer mutates map
    /// bytes **mid-txn** (through the brokered slices), so a view cached at
    /// txn begin would predate those writes: under Stacked/Tree Borrows,
    /// copying such a stale `&[u8]` — let alone reading a spilled page
    /// through it — is undefined behavior at the written locations
    /// (ADR-0021). The in-map arm
    /// therefore borrows the view **lazily per access**, exactly as `RoTxn`
    /// does: a fresh borrow at a `&self` boundary postdates every in-place
    /// write, and none can happen while it lives (TXN-39).
    ///
    /// Heap modes keep the cached view (`self.bytes`, re-derived per spill —
    /// TXN-71, ADR-0021): their mid-txn file writes go through the fd,
    /// and the only Rust-visible map mutation is the commit/spill-time
    /// `memcpy` of the heap-staged WRITE_MAP backing, which the spill-time
    /// re-derivation postdates.
    #[inline]
    fn whole_map(&self) -> &'env [u8] {
        if self.dirty.in_map_mode() {
            self.env.inner().backing_bytes()
        } else {
            self.bytes
        }
    }

    fn load(&self, pgno: u64) -> Result<PageRef<'_>> {
        // Trusted-psize load: psize is env-validated at open.
        PageRef::new_trusted_psize(
            self.source()
                .bytes_from(self.psize, pgno)
                .map_err(map_page_err)?,
            self.psize,
        )
        .map_err(map_page_err)
    }

    /// COW first-touch (SPEC 03 §5.1/§5.2): an already-dirty page is edited in
    /// place; a committed page is copied into a fresh frame under a **new**
    /// pgno, stamped with this txn's id, and its old pgno freed. Returns the
    /// (possibly new) pgno; the caller rewrites the parent pointer (§5.3).
    fn touch(&mut self, pgno: u64) -> Result<u64> {
        if self.dirty.contains(pgno) {
            return Ok(pgno);
        }
        if self.dirty.has_spills() && self.dirty.spilled_pages(pgno).is_some() {
            return self.unspill(pgno);
        }
        // Not dirty, so the page must be committed: a committed reference
        // never exceeds the base snapshot's high-water (SPEC 06 REC-14). The
        // map may extend past the real file end (ADR-0004), so an
        // unbounded read here could fault on a corrupt reference.
        if pgno > self.committed_last_pg {
            return Err(Error::Mdb(MdbError::Invalid));
        }
        let ps = self.psize as usize;
        let base = (pgno as usize)
            .checked_mul(ps)
            .ok_or(Error::Mdb(MdbError::Invalid))?;
        // Take the `&'env [u8]` map view out of `self` (lifetime 'env, so the
        // source borrow does not alias the `&mut self.dirty` op below —
        // letting `insert_copy` copy straight into a pooled frame). Under
        // in-place WRITE_MAP this is a fresh per-access borrow postdating
        // every brokered write (see `RwTxn::whole_map`).
        let map: &[u8] = self.whole_map();
        let src = map
            .get(base..base + ps)
            .ok_or(Error::Mdb(MdbError::Invalid))?;
        let np = self.allocate(1)?;
        // Copy the committed page into a fresh (or recycled) frame — under
        // in-place WRITE_MAP (ADR-0021) straight into the map at `np`, which
        // is safe to store to mid-txn because `allocate` only returns
        // TXN-62 pages (no live snapshot references `np`) — then restamp
        // the copy's identity in place (SPEC 02 §2: pgno + writer txnid).
        let frame = self.dirty.insert_copy(np, src);
        let mut hdr = crate::page::CommonHeader::read(frame);
        hdr.pgno = np;
        hdr.txnid = self.txnid;
        hdr.write(frame);
        self.free_page(pgno);
        Ok(np)
    }

    /// Read-only root-to-key descent (SPEC 03 §2), returning the path of
    /// `(pgno, ki)` frames and whether the key is present at `path.last()`.
    /// No page is touched — COW happens only when a mutation is decided
    /// (`touch_path`), so a `NO_OVERWRITE` miss or a `del` of an absent key
    /// dirties nothing (LMDB parity).
    fn search_path(&self, tree: TreeId, key: &[u8]) -> Result<(Path, bool)> {
        let mut path = Path::new();
        let found = self.search_path_into(tree, key, &mut path)?;
        Ok((path, found))
    }

    /// [`RwTxn::search_path`] into a caller-owned buffer (cleared first), so
    /// the put and delete paths can reuse the carried `path_buf` ([`WriterCarry`]).
    fn search_path_into(&self, tree: TreeId, key: &[u8], path: &mut Path) -> Result<bool> {
        path.clear();
        let cmp = self.tree_comparator(tree);
        let rec = *self.record(tree);
        if rec.root == PGNO_INVALID {
            return Ok(false);
        }
        // Hostile-depth guard: `rec.depth` is on-disk data. Deeper than the
        // read path's CURSOR_STACK bound is corruption (a legal tree of
        // minimum fanout 2 addresses 2^31 leaves at depth 32); without the
        // cap the descent loop below can build an unbounded `path` (a page
        // cycle plus a huge depth) and `rebalance` recurses `path.len()`
        // frames — a stack overflow, not a typed error.
        if rec.depth as usize > crate::btree::CURSOR_STACK {
            return Err(Error::Mdb(MdbError::Invalid));
        }
        let mut pgno = rec.root;
        let valid = Some(&self.validated);
        for _ in 0..=rec.depth {
            // One source resolution per level; a non-tree page type maps
            // through `map_page_err` to `Invalid`.
            match node_view(self.source(), self.psize, pgno, valid).map_err(map_page_err)? {
                NodeView::Leaf(leaf) => {
                    let (ki, found) = match leaf.lookup_with(key, cmp) {
                        Ok(i) => (i, true),
                        Err(i) => (i, false),
                    };
                    path.push((pgno, ki));
                    return Ok(found);
                }
                NodeView::Branch(br) => {
                    let i = br.child_index_with(key, cmp);
                    path.push((pgno, i));
                    pgno = br.child_pgno(i);
                }
            }
        }
        Err(Error::Mdb(MdbError::Invalid)) // deeper than depth: corrupt
    }

    /// Read-only descent to the rightmost entry (APPEND's last-key check,
    /// §6.3) into a caller-owned buffer (cleared first): fills the path (leaf
    /// `ki = num_keys - 1`) and returns whether `key` sorts strictly after the
    /// last key under the tree's ordering, or `None` if not. The key is
    /// compared in place in the leaf, as LMDB's APPEND check does, not copied
    /// out. The tree must be non-empty.
    ///
    /// `Some(owned)` also says whether every page on the path is already a
    /// frame of this txn, in which case [`touch_path`](Self::touch_path)
    /// would change nothing: an append load stays on one dirty right spine
    /// between splits, and LMDB's `mdb_page_touch` skips such pages on their
    /// `P_DIRTY` flag the same way.
    fn rightmost_path_into(
        &self,
        tree: TreeId,
        key: &[u8],
        path: &mut Path,
    ) -> Result<Option<bool>> {
        path.clear();
        let rec = *self.record(tree);
        // Same hostile-depth guard as `search_path`.
        if rec.depth as usize > crate::btree::CURSOR_STACK {
            return Err(Error::Mdb(MdbError::Invalid));
        }
        let mut pgno = rec.root;
        let valid = Some(&self.validated);
        let mut owned = true;
        for _ in 0..=rec.depth {
            let (view, from_map) = node_view_classified(self.source(), self.psize, pgno, valid)
                .map_err(map_page_err)?;
            owned &= !from_map;
            match view {
                NodeView::Leaf(leaf) => {
                    let n = leaf.num_keys();
                    if n == 0 {
                        return Err(Error::Mdb(MdbError::Invalid));
                    }
                    path.push((pgno, n - 1));
                    // Custom comparators: "strictly greater than the last key" is decided by
                    // the target tree's ordering, not memcmp — otherwise
                    // APPEND on a custom-comparator DB would reject exactly
                    // the keys it should accept.
                    let after = self.tree_comparator(tree).compare(key, leaf.key(n - 1))
                        == Ordering::Greater;
                    return Ok(after.then_some(owned));
                }
                NodeView::Branch(br) => {
                    // A zero-child branch is rejected at decode
                    // (PageError::EmptyBranch); keep the subtraction checked
                    // anyway so this path can never underflow.
                    let last = br
                        .num_keys()
                        .checked_sub(1)
                        .ok_or(Error::Mdb(MdbError::Invalid))?;
                    path.push((pgno, last));
                    pgno = br.child_pgno(last);
                }
            }
        }
        Err(Error::Mdb(MdbError::Invalid))
    }

    /// COW every page on `path` top-down (SPEC 03 §5.3): copying a child
    /// rewrites the (already dirty) parent's child pointer; copying the root
    /// updates the working `DBRecord.root`. `ki` values stay valid — the copy
    /// is byte-identical apart from its header identity.
    fn touch_path(&mut self, tree: TreeId, path: &mut [(u64, usize)]) -> Result<()> {
        for level in 0..path.len() {
            let (pgno, _) = path[level];
            let np = self.touch(pgno)?;
            if np != pgno {
                path[level].0 = np;
                if level == 0 {
                    self.record_mut(tree).root = np;
                } else {
                    let (ppg, pki) = path[level - 1];
                    let frame = self.dirty.bytes_mut(ppg).expect("parent already touched");
                    BranchMut::from_valid(frame, self.psize)
                        .map_err(map_page_err)?
                        .set_child_pgno(pki, np);
                }
            }
        }
        Ok(())
    }

    /// The snapshot this txn grew from.
    #[inline]
    fn base(&self) -> &Snapshot {
        self.base
            .as_deref()
            .expect("the base is held until the txn drops")
    }

    /// Whether this txn changed nothing observable (no dirty pages, no frees,
    /// no allocation, identical records): commit is then a pure mutex release,
    /// no meta write, no txnid consumption (LMDB parity).
    fn is_unchanged(&self) -> bool {
        self.dirty.is_empty()
            && !self.dirty.has_spills()
            && work!(self).freed.is_empty()
            && work!(self).loose.is_empty()
            && self.drains.is_empty()
            && work!(self).reclaimed.is_empty()
            && self.next_pgno == self.base().last_pg + 1
            && self.main_db == self.base().main_db
            && self.free_db == self.base().free_db
            // A dirty named-DB working record still needs its catalog write-back
            // (defensive: any named mutation also dirties a page, so this is
            // implied — but keep the commit honest, SPEC 02 §6).
            && !self.open.values().any(|t| t.dirty)
    }
}

impl Drop for RwTxn<'_> {
    fn drop(&mut self) {
        // Hand the dirty store's frames back for the next write txn (LMDB's
        // `me_dpages` reuse; `DirtyStore::take_carry` says why this is safe
        // on every end-of-txn path). They ride with the txn's other
        // containers in its `WriterCarry`, which the writer guard stores in
        // the slot when this txn's fields drop right after this method.
        let dirty = self.dirty.take_carry();
        // ADR-0024: an end without a commit (abort, failed commit) frees what
        // this txn opened; after a commit there is nothing left to end.
        self.end_handles(false);
        let Some(mut work) = self.work.take() else {
            return;
        };
        work.dirty = Some(dirty);
        work.handles = self.handles.recycle();
        work.reclaimed.clear();
        if crate::dirty::index_oversized(work.reclaimed.capacity()) {
            work.reclaimed.shrink_to(crate::dirty::CARRY_INDEX_CAP);
        }
        recycle_vec(&mut work.freed, CARRY_VEC_CAP);
        recycle_vec(&mut work.loose, CARRY_VEC_CAP);
        recycle_vec(&mut work.merged, CARRY_VEC_CAP);
        recycle_vec(&mut work.pgnos, CARRY_VEC_CAP);
        recycle_vec(&mut work.catalog, CARRY_VEC_CAP);
        recycle_vec(&mut work.path_buf, CARRY_VEC_CAP);
        // Byte buffer: up to the largest page (64 KiB), its usual content.
        recycle_vec(&mut work.cell_scratch, 64 * 1024);
        recycle_vec(&mut work.fingers, CARRY_VEC_CAP);
        // Names looked up through a checked handle stay with their
        // generation for the next txn; the slots stay, empty. Checked
        // through a reference: `Option::take` would copy every slot's whole
        // `NamedTree` out, empty or not.
        let names = &mut work.names;
        for (dbi, slot) in self.open.slots.iter_mut().enumerate() {
            let Some(t) = slot else { continue };
            if let Some(g) = t.generation {
                if names.len() <= dbi {
                    names.resize_with(dbi + 1, || None);
                }
                names[dbi] = Some((g, std::mem::take(&mut t.name)));
            }
            *slot = None;
        }
        // Both lists are as long as the highest dbi a txn touched: bounded
        // like the others. A name past the cap is looked up again
        // (`named_name`), which changes no result.
        work.names.truncate(CARRY_VEC_CAP);
        work.names.shrink_to(CARRY_VEC_CAP);
        self.open.slots.truncate(CARRY_VEC_CAP);
        self.open.slots.shrink_to(CARRY_VEC_CAP);
        work.open = std::mem::take(&mut self.open.slots);
        // The snapshot published at C6, else the unchanged base: either is
        // the latest this process knows of.
        work.snapshot = self.published.take().or_else(|| self.base.take());
        self._guard.give_back_carry(work);
    }
}

// ---------------------------------------------------------------------------
// Database write API (heed-shaped wrappers over RwTxn internals)
// ---------------------------------------------------------------------------
