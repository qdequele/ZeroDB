//! Environment open/close, meta selection, and the same-process registry
//! (SPEC 02 §3.2, SPEC 06 §1, SPEC 04 §7).
//!
//! This module owns the *behavioral* half of env lifecycle and is deliberately
//! I/O-free so it stays `miri`-clean: the mapped file bytes reach it through the
//! [`Backing`] trait, which the mmap layer (`zerodb-io`) implements for real
//! envs and which tests implement over a plain `Vec<u8>`. All `unsafe` (the
//! mmap) lives behind that trait in `zerodb-io`; this module contains none.
//!
//! What is implemented here: reading both meta slots, validating
//! each via the page-layer predicate ([`crate::page::MetaPage::validate`]), selecting
//! the live snapshot (normal / `PREV_SNAPSHOT`), mapping the SPEC 06 REC error
//! taxonomy onto [`Error`], the process registry with `EnvAlreadyOpened`, the
//! refcounted `EnvInner` behind [`Env`] (`Clone`), and deferred close with
//! [`EnvClosingEvent`] (SPEC 04 TXN-52/53). The inner also owns the
//! MVCC reader table and the published-snapshot cell (`crate::readers`,
//! SPEC 04 §3/§4, ADR-0006); the write path lives in `crate::rwtxn`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::Duration;

use crate::cmp::{Comparator, ComparatorError, ComparatorRegistry, KeyCmp};
use crate::error::{Error, MdbError};
use crate::page::{
    select_meta, DBRecord, FileTrust, MetaChoice, MetaPage, MetaValidity, META_A_PGNO, META_B_PGNO,
};
use crate::readers::{ReaderTable, SnapshotCell};
use crate::stamps::StampCache;

mod backing;
mod info;
mod open;
mod writer_lock;

pub use backing::{Backing, MapView};
pub use info::{EnvClosingEvent, EnvInfo, EnvStat, ReaderEntry};
pub(crate) use open::read_snapshot_at;
pub use open::{
    check_open_limits, dirty_limit_pages, open_with_backing, open_with_backing_policy,
    open_with_backing_shared, MAX_READERS_LIMIT,
};
use open::{bytes_needed, check_snapshot_backed, META_RETRY_LIMIT};
pub(crate) use writer_lock::WriterGuard;
use writer_lock::{SignalEvent, WriterLock};

#[doc(hidden)]
pub mod testutil;

// ---------------------------------------------------------------------------
// Published snapshot + commit hooks (SPEC 04 TXN-18/19, §9; ADR-0004 D3)
// ---------------------------------------------------------------------------

/// An immutable committed snapshot: the roots and geometry of one committed
/// state (SPEC 04 TXN-18). Readers `Arc`-clone the env's published snapshot at
/// begin and never re-read a durable meta page (the slot a pinned txnid lived
/// in is overwritten two commits later, TXN-63).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// The commit point of this snapshot.
    pub txnid: u64,
    /// File high-water as of this commit (`next_pgno - 1`, SPEC 05 GC-15).
    pub last_pg: u64,
    /// The map size this snapshot's meta records (ADR-0023). A
    /// commit persists `max(base.map_size, the committing process's logical
    /// map size)`, so it never lowers the recorded size.
    pub map_size: u64,
    /// Root/stats of the main/catalog DB.
    pub main_db: DBRecord,
    /// Root/stats of the free (GC) DB.
    pub free_db: DBRecord,
    /// This snapshot's meta free-list annex (SPEC 02 §3 format v2, ADR-0022;
    /// SPEC 05 §2a): the pages freed by txn `txnid`, exactly the `fl_ids` of
    /// its meta. Pinned **in the snapshot** — the meta slot `txnid & 1` is
    /// overwritten by txn `txnid + 2` even while this snapshot stays pinned
    /// (TXN-63), so holders (`copy`, the next writer) must never re-read the
    /// slot. `Arc<[u64]>` keeps `Snapshot` cheap to clone.
    pub free_annex: std::sync::Arc<[u64]>,
}

impl Snapshot {
    /// The snapshot a validated meta page describes, with `annex` as the
    /// meta's free-list annex ids (read via [`MetaPage::read_annex`] from the
    /// same validated slot buffer; `annex.len()` must equal `meta.fl_count`).
    #[must_use]
    pub fn from_meta(meta: &MetaPage, annex: Vec<u64>) -> Snapshot {
        debug_assert_eq!(annex.len(), meta.fl_count as usize);
        Snapshot {
            txnid: meta.txnid,
            last_pg: meta.last_pg,
            map_size: meta.map_size,
            main_db: meta.main_db,
            free_db: meta.free_db,
            free_annex: annex.into(),
        }
    }
}

/// A crash-injection point between commit-pipeline steps (SPEC 04 §9 H0–H4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookPoint {
    /// After C1 (`freelist_save`): nothing on disk yet.
    H0,
    /// After C2 (dirty pages written, **not** fsynced).
    H1,
    /// After C3 (`fsync(data)`).
    H2,
    /// After C4 (meta written to slot `N & 1`, **not** fsynced).
    H3,
    /// After C5 (`fsync(meta)`): txn `N` durable.
    H4,
}

/// A commit-pipeline observer (ADR-0004 D3/OQ5: **always compiled**, default
/// absent, so the crash-tested pipeline is byte-for-byte the shipped one).
/// The crash-consistency harnesses install hooks that kill/tear at a chosen
/// [`HookPoint`]; the commit smoke test aborts the process at each point in turn.
pub trait CommitHook: Send + Sync {
    /// Called between commit steps, at `point`. May abort/kill the process.
    fn at(&self, point: HookPoint);
}

/// Env-level durability / write-mode flags (SPEC 01 Table 1, §S6/§S7).
/// Selected once at open and immutable for the env's life. The commit pipeline
/// reads them to decide which fsync/msync barriers run (SPEC 06 REC-9/REC-12);
/// the backing implementation chooses the *primitive* (`fdatasync` vs `msync`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DurabilityFlags {
    /// `MDB_RDONLY` — env-level read-only: `write_txn`/`force_sync` → `EACCES`
    /// (SPEC 01 Table 1, TXN-8).
    pub read_only: bool,
    /// `MDB_NOSYNC` — skip **both** data (C3) and meta (C5) fsync on commit
    /// (SPEC 01 §S6). Durability restored by `force_sync`.
    pub no_sync: bool,
    /// `MDB_NOMETASYNC` — fsync data (C3) but skip the meta fsync (C5) this
    /// commit (SPEC 01 §S6, REC-10).
    pub no_meta_sync: bool,
    /// `MDB_MAPASYNC` — with `WRITE_MAP`, use `msync(MS_ASYNC)` for the commit
    /// flushes (SPEC 01 §S6, REC-9). No effect without `write_map`.
    pub map_async: bool,
    /// `MDB_WRITEMAP` — writes go through a writable mmap (SPEC 01 §S7,
    /// SPEC 04 §6.4). Recorded here for introspection; the actual writable map
    /// lives in the `zerodb-io` backing.
    pub write_map: bool,
}

// ---------------------------------------------------------------------------
// Same-process registry (SPEC 04 TXN-51)
// ---------------------------------------------------------------------------

/// Registry value: the owning env's unique id plus a `Weak` handle. The id lets
/// an [`EnvInner`]'s `Drop` remove only *its own* slot even if a newer env has
/// meanwhile re-registered the same path (the classic registry race).
type RegEntry = (u64, Weak<EnvInner>);

fn registry() -> &'static Mutex<HashMap<PathBuf, RegEntry>> {
    static REG: OnceLock<Mutex<HashMap<PathBuf, RegEntry>>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(HashMap::new()))
}

fn next_env_id() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    // Relaxed: the counter only needs uniqueness, it publishes/acquires no other
    // memory (each fetch_add yields a distinct value on every architecture).
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// EnvInner / Env
// ---------------------------------------------------------------------------

/// The shared, refcounted heart of an environment (SPEC 04 TXN-50). Owns the
/// backing map, the selected snapshot metadata, the registry back-reference,
/// and the close signal. Torn down (map unmapped, fd closed, registry slot
/// cleared, close event fired) only when the last [`Env`] / txn reference drops
/// (TXN-52/53).
pub(crate) struct EnvInner {
    /// Unique id for the registry-race guard.
    id: u64,
    /// Canonical directory path — the registry key and [`Env::path`] value.
    path: PathBuf,
    /// The mapped file. `Option` so `Drop` can release it *before* firing the
    /// close event, guaranteeing waiters observe a fully-closed env (TXN-53).
    ///
    /// No `Mutex` is needed: the only mutation is the release in [`EnvInner`]'s
    /// `Drop`, which receives `&mut self` and runs only when the last `Arc`
    /// reference is gone (so no reader can be touching it concurrently). Shared
    /// `&self` readers ([`EnvInner::backing_bytes`], `real_disk_size`, …) take
    /// only immutable references, which is why a `RoTxn` can borrow the mapped
    /// `&[u8]` for its whole life (SPEC 04 TXN-37) with no `unsafe` in this
    /// crate. `Backing: Send + Sync` keeps `EnvInner: Sync`.
    backing: Option<Box<dyn Backing>>,
    /// The DB page size (from the live meta; authoritative — SPEC 02 §3.2).
    /// The runtime map size is not stored here: it lives in the backing's
    /// published view with the mapping it fits in (ADR-0023).
    page_size: u32,

    /// Whether this env was opened with `PREV_SNAPSHOT` (SPEC 01 §S5).
    prev_snapshot: bool,
    /// Close signal, shared with any outstanding [`EnvClosingEvent`].
    closing: Arc<SignalEvent>,
    /// The single-writer lock (SPEC 04 TXN-6). Guards no data — the write
    /// txn's state lives in the `RwTxn` — it only serializes writers. A
    /// thread-agnostic flag+condvar lock (see [`WriterLock`]) so the guard —
    /// and with it `RwTxn` — is `Send`.
    write_mutex: WriterLock,
    /// The page buffer a commit encodes its meta into (commit step C4),
    /// reused across commits. Every byte is zero while it sits here; empty
    /// until the first commit. Only the writer touches it, so its lock is
    /// never contended; it is taken only by commits that write a meta, which
    /// keeps it off the begin path and out of the `RwTxn` (an empty write
    /// txn pays nothing for it).
    meta_buf: Mutex<Vec<u8>>,
    /// The published-snapshot cell (SPEC 04 TXN-18; ADR-0006 Option B): the
    /// immutable `Arc<Snapshot>` behind a
    /// bounded-O(1)-critical-section mutex, plus the mirroring SeqCst
    /// `commit_point` atomic that carries the whole lock-free pin protocol
    /// (TXN-17/19/20). Published in the TXN-19 order (swap the object, then
    /// store the commit point).
    snap_cell: SnapshotCell,
    /// Commit-pipeline crash hooks (ADR-0004 D3; default `None` = no-op).
    commit_hook: Mutex<Option<Arc<dyn CommitHook>>>,
    /// REC-13 fsync-gate: set (Release) when a commit fsync fails; checked
    /// (Acquire) at every write-txn begin and commit. A poisoned env still
    /// serves read txns from their pinned snapshots.
    poisoned: AtomicBool,
    /// The MVCC reader table (SPEC 04 §4; ADR-0006): `max_readers` 128-byte
    /// slots, in this process's memory or, in a lock-file environment, in
    /// the shared lock file (ADR-0023 §3). Readers claim/pin/release
    /// slots lock-free; the writer's GC gate scans it (`oldest_live_reader`,
    /// TXN-21).
    reader_table: ReaderTable,
    /// The named-database slot table (SPEC 02 §6.2, ADR-0024): slot ↔ name
    /// and each slot's lifetime. Records resolve from the catalog (TXN-10).
    handles: crate::handles::HandleTable,
    /// Env-level durability / write-mode flags (SPEC 01 §S6/§S7).
    /// Immutable after open; read by the commit pipeline and by `write_txn` /
    /// `force_sync`.
    durability: DurabilityFlags,
    /// Per-named-database key comparators (SPEC 03 §2.0). Empty
    /// unless a caller opened a database through one of the
    /// `*_with_comparator` entry points; the main/catalog tree and the GC tree
    /// are never represented here and are always memcmp.
    ///
    /// **Not persisted** — see `crate::cmp` for the reopen hazard
    /// (docs/DIVERGENCES.md, comparator persistence).
    comparators: ComparatorRegistry,
    /// Catalog capacity (`max_dbs`), immutable after open. Duplicated out of
    /// the `named` registry so read txns can size their dbi-indexed record
    /// table without taking the registry lock.
    max_dbs: u32,
    /// Page-validation policy (ADR-0014), fixed at open. Every txn copies it
    /// into its validated-pages memo at begin.
    file_trust: FileTrust,
    /// Env-wide cache of validated page versions (ADR-0018), borrowed by
    /// every plain read txn. Never consulted under a trusting policy.
    stamp_cache: StampCache,
    /// A write txn's dirty limit in pages (SPEC 04 TXN-68, ADR-0017), fixed
    /// at open: past it the txn spills pages to the file.
    dirty_limit: u64,
    /// Sequential-writes default (ADR-0015), fixed at open: whether a write
    /// txn keeps a rightmost-leaf finger for a tree with no override.
    sequential_writes: bool,
    /// Per-database sequential-writes overrides (ADR-0015): slot 0 is the
    /// main DB, slot `1 + dbi` a named DB. [`SEQ_FOLLOW`] = the env default,
    /// [`SEQ_OFF`] / [`SEQ_ON`] = forced. Runtime state, never persisted.
    sequential_overrides: Box<[AtomicU8]>,
    /// The cross-process region when opened with a lock file (ADR-0023);
    /// `None` keeps the single-process path unchanged.
    shared: Option<Arc<dyn crate::shared::SharedRegion>>,
    /// Whether this process still holds the first-opener election past its
    /// open (ADR-0023): set at open for a `PREV_SNAPSHOT` open
    /// that initialized the lock file, cleared by its first commit, which
    /// finishes the election instead of publishing. Read and written only
    /// at open and by the write-lock holder.
    holds_election: AtomicBool,
    /// A durable commit newer than the published snapshot (ADR-0023):
    /// the commit that finished a held election (never published, its
    /// txnid reused by the next commit), or, in a joiner, the newest meta
    /// on disk when it was newer than `T` at open. Reported by
    /// [`Env::info`], [`Env::stat`], [`Env::txnid`] and [`Env::resize`] while
    /// it is newer than the published snapshot, as LMDB reports the newest
    /// meta on disk; cleared once a publish reaches it.
    unpublished: Mutex<Option<Unpublished>>,
    /// Whether [`EnvInner::unpublished`] may hold a value: lets the common
    /// case skip its mutex.
    has_unpublished: AtomicBool,
}

/// A durable commit newer than the published snapshot (ADR-0023), kept in
/// [`EnvInner`] for `info`, `stat` and `resize`.
struct Unpublished {
    snap: Arc<Snapshot>,
    /// This process's own unpublished first `PREV_SNAPSHOT` commit, which
    /// [`Env::txnid`] also reports; `false` for a newer meta a joiner found
    /// on disk at open.
    own_commit: bool,
}

/// [`EnvInner`] sequential-writes override: follow the env default.
const SEQ_FOLLOW: u8 = 0;
/// [`EnvInner`] sequential-writes override: forced off.
const SEQ_OFF: u8 = 1;
/// [`EnvInner`] sequential-writes override: forced on.
const SEQ_ON: u8 = 2;

impl std::fmt::Debug for EnvInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvInner")
            .field("id", &self.id)
            .field("path", &self.path)
            .field("page_size", &self.page_size)
            .field("map_size", &self.map_size())
            .field("txnid", &self.snapshot().txnid)
            .field("prev_snapshot", &self.prev_snapshot)
            .finish_non_exhaustive()
    }
}

impl EnvInner {
    /// The DB page size recorded in the live meta.
    #[must_use]
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// The runtime map size (SPEC 00 row 20, `Env::info().map_size`): the
    /// logical size of the backing's current view (ADR-0023).
    #[must_use]
    pub fn map_size(&self) -> u64 {
        self.backing_ref().view().map_size
    }

    /// The txnid of the current live snapshot (the commit point; advances on
    /// every commit, SPEC 04 TXN-19). SeqCst load inside the cell — see
    /// `crate::readers` for the TXN-17/19/20 StoreLoad pairing argument. In a
    /// lock-file environment it is the shared published txnid `T`, so it
    /// includes other processes' commits (ADR-0023 §4).
    ///
    /// One exception (ADR-0023, SPEC 04 TXN-67): in a
    /// `PREV_SNAPSHOT` opener with a lock file, between its first and second
    /// commit, it is the first commit's txnid. That commit is durable but not
    /// published, so no read txn can pin it: read txns still begin at `T`. In
    /// a process that joined, it stays `T` even when [`Env::info`] reports a
    /// newer meta found on disk at open.
    #[must_use]
    pub fn txnid(&self) -> u64 {
        let t = match &self.shared {
            // SeqCst: the same total order as the T stores (TXN-17/19).
            Some(region) => region.published_txnid().load(Ordering::SeqCst),
            None => self.snap_cell.commit_point(),
        };
        // ADR-0023: a `PREV_SNAPSHOT` opener's own unpublished
        // first commit, while it is newer than `T`. A joiner's newer meta on
        // disk is left out: `txnid` stays the published txnid there.
        self.newer_unpublished(t, true).map_or(t, |u| u.txnid)
    }

    /// Whether this process still holds the first-opener election past its
    /// open (ADR-0023).
    pub(crate) fn holds_election(&self) -> bool {
        // Relaxed: set before the env is shared, and afterwards read and
        // written only by the holder of the writer locks, whose
        // acquire/release orders every access.
        self.holds_election.load(Ordering::Relaxed)
    }

    /// The unpublished first commit of a `PREV_SNAPSHOT` opener
    /// (ADR-0023), if its txnid is above `txnid`.
    /// With `own_only`, only this process's own unpublished commit counts,
    /// not a newer meta a joiner found on disk at open.
    fn newer_unpublished(&self, txnid: u64, own_only: bool) -> Option<Arc<Snapshot>> {
        // Acquire: pairs with the Release store made after the cell is
        // filled. The flag only lets this call skip the mutex; the value
        // itself is read under the mutex, which orders it. A stale `false`
        // answers as a call made just before the cell was filled (or just
        // after it was cleared) would, which is a valid order on any memory
        // model, ARM's included.
        if !self.has_unpublished.load(Ordering::Acquire) {
            return None;
        }
        let mut cell = self.unpublished.lock().expect("unpublished cell poisoned");
        match cell.as_ref() {
            Some(u) if u.snap.txnid > txnid => {
                (u.own_commit || !own_only).then(|| Arc::clone(&u.snap))
            }
            Some(_) => {
                // A published snapshot reached it (`T` never decreases), so
                // it can never be newer again.
                *cell = None;
                // Release: under the mutex, after the cell is emptied.
                self.has_unpublished.store(false, Ordering::Release);
                None
            }
            None => None,
        }
    }

    /// `snap`, or the unpublished first commit of a `PREV_SNAPSHOT` opener
    /// when that one is newer (ADR-0023): the newest meta on
    /// disk, which LMDB's `mdb_env_info`, `mdb_env_stat` and
    /// `mdb_env_set_mapsize` read.
    fn or_newer_unpublished(&self, snap: Arc<Snapshot>) -> Arc<Snapshot> {
        self.newer_unpublished(snap.txnid, false).unwrap_or(snap)
    }

    /// Finish a commit while this process holds the election
    /// (ADR-0023). The commit is durable; it is not published,
    /// as LMDB's first `MDB_PREVSNAPSHOT` commit leaves the shared txnid on
    /// the older meta (`OBS_PREV_SNAPSHOT_COMMIT_TXNIDS`): `T` and the
    /// published-snapshot cell keep the older snapshot, so the next writer
    /// begins from it and reuses this txnid (TXN-2). Then the election is
    /// finished, letting the processes that wait in it join.
    ///
    /// # Errors
    ///
    /// The I/O error of [`SharedRegion::finish_open`]. The election stays
    /// held and the next commit tries again.
    ///
    /// [`SharedRegion::finish_open`]: crate::shared::SharedRegion::finish_open
    pub(crate) fn finish_held_election(&self, committed: Arc<Snapshot>) -> Result<(), Error> {
        {
            let mut cell = self.unpublished.lock().expect("unpublished cell poisoned");
            *cell = Some(Unpublished {
                snap: committed,
                own_commit: true,
            });
            // Release: after the cell is filled; see `newer_unpublished`.
            self.has_unpublished.store(true, Ordering::Release);
        }
        let region = self
            .shared
            .as_ref()
            .expect("the election is held only with a lock file");
        region.finish_open().map_err(Error::Io)?;
        // Relaxed: see `holds_election`.
        self.holds_election.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// `Arc`-clone the current published snapshot (SPEC 04 TXN-18). The
    /// clone keeps the `(txnid, roots)` alive for the caller's
    /// life regardless of later commits. Critical section: one refcount bump
    /// (ADR-0006 Option B).
    #[must_use]
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.snap_cell.clone_snapshot()
    }

    /// Publish a freshly committed snapshot (commit step C6) in the TXN-19
    /// order: (1) swap the `Arc<Snapshot>` into the cell, then (2) store the
    /// commit point `SeqCst`. Object-before-counter guarantees a reader that
    /// sees the new counter can load the matching (or newer) roots. See
    /// `crate::readers::SnapshotCell::publish` for the ordering comments.
    ///
    /// In a lock-file environment the shared published txnid `T` is stored
    /// last, which is the moment other processes begin to see the commit
    /// (ADR-0023 §4).
    pub(crate) fn publish_snapshot(&self, snap: Arc<Snapshot>) {
        let txnid = snap.txnid;
        self.snap_cell.publish(snap);
        if let Some(region) = &self.shared {
            // ADR-0023 §4: T is stored after the meta write (and its fsync in
            // the durable modes, C4/C5), and after the cache swap (TXN-19
            // object-before-counter). SeqCst pairs with every reader's
            // publish-and-verify and with the writer's SeqCst slot scan,
            // across processes. Only the cross-process writer-lock holder
            // advances T.
            region.published_txnid().store(txnid, Ordering::SeqCst);
        }
    }

    /// The commit's reusable meta-page buffer (commit step C4), locked for
    /// the meta write. The buffer is all zero on entry, or empty before the
    /// first commit; the commit sizes it to the page, encodes into it, writes
    /// it, and clears the prefix it encoded before the guard drops.
    pub(crate) fn meta_buf(&self) -> std::sync::MutexGuard<'_, Vec<u8>> {
        // Poison recovery: a writer that panicked mid-encode may have left
        // bytes behind, so a recovered buffer is discarded (the commit
        // reallocates a zeroed one).
        self.meta_buf.lock().unwrap_or_else(|p| {
            let mut g = p.into_inner();
            g.clear();
            g
        })
    }

    /// Acquire the single-writer lock (SPEC 04 TXN-6/7): blocks until the
    /// current writer finishes; never errors. The returned guard releases
    /// from whatever thread drops it ([`WriterLock`]), so `RwTxn` is `Send`.
    /// The writer-panic poison-recovery policy lives in
    /// [`WriterLock::acquire`].
    pub(crate) fn lock_writer(&self) -> WriterGuard<'_> {
        self.write_mutex.acquire()
    }

    /// Take the cross-process writer lock on top of the in-process one
    /// (ADR-0023 §2); a no-op without a lock file. The guard releases it on
    /// drop, before the in-process slot. A writer that took the lock over
    /// from a dead owner then frees dead processes' reader slots (see
    /// [`EnvInner::lock_writer_region`]).
    ///
    /// # Errors
    ///
    /// The I/O error of the lock call, or of a liveness probe during the
    /// takeover reap. After a failed lock call the guard holds only the
    /// in-process lock. After a failed takeover reap it holds both locks, and
    /// dropping it (as the failing write-txn begin does) releases both, as
    /// LMDB's lock recovery releases the writer mutex when its reader check
    /// fails.
    #[inline]
    pub(crate) fn lock_writer_shared<'a>(
        &'a self,
        guard: &mut WriterGuard<'a>,
    ) -> Result<(), Error> {
        match &self.shared {
            None => Ok(()),
            Some(region) => self.lock_writer_region(region.as_ref(), guard),
        }
    }

    /// The lock-file arm of [`EnvInner::lock_writer_shared`], kept out of
    /// line so the write-txn path without a lock file stays small.
    ///
    /// A writer that took the lock over from a dead owner then frees dead
    /// processes' reader slots, as LMDB's writer does when it recovers the
    /// lock from a dead owner on Linux (ADR-0023).
    ///
    /// # Errors
    ///
    /// The I/O error of the lock call, or of a liveness probe during the
    /// takeover reap. In the second case the guard already records the
    /// cross-process lock, so dropping it (as the failing write txn does)
    /// releases the lock, as LMDB's lock recovery releases the writer mutex
    /// when its reader check fails.
    #[inline(never)]
    fn lock_writer_region<'a>(
        &'a self,
        region: &'a dyn crate::shared::SharedRegion,
        guard: &mut WriterGuard<'a>,
    ) -> Result<(), Error> {
        let took_over = region.lock_writer().map_err(Error::Io)?;
        guard.shared = Some(region);
        if took_over {
            self.clear_stale_readers()?;
        }
        Ok(())
    }

    /// Free the reader slots of dead processes (`mdb_reader_check`,
    /// ADR-0023 §3). Returns the number of slots freed (the fork's
    /// `mdb_reader_check` also counts slots, ADR-0023); always 0
    /// without a lock file, where every slot belongs to this process.
    ///
    /// # Errors
    ///
    /// The I/O error of a liveness probe. Nothing has been freed then: every
    /// probe runs before any slot is touched.
    pub(crate) fn clear_stale_readers(&self) -> Result<usize, Error> {
        #[cfg(not(loom))]
        if let Some(region) = &self.shared {
            let n = self
                .reader_table
                .reap_dead(region.pid(), |p| region.is_alive(p))
                .map_err(Error::Io)?;
            return Ok(n as usize);
        }
        Ok(0)
    }

    /// Whether a failed commit fsync poisoned the env (SPEC 06 REC-13).
    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        // Acquire: pairs with the Release in `poison` so a writer that
        // observes the flag also observes everything before the failure.
        self.poisoned.load(Ordering::Acquire)
    }

    /// Poison the env after a failed durability barrier (SPEC 06 REC-13).
    pub(crate) fn poison(&self) {
        // Release: see `is_poisoned`.
        self.poisoned.store(true, Ordering::Release);
    }

    /// Install (or clear) the commit-pipeline crash hook (ADR-0004 D3). Test
    /// infrastructure for the commit smoke test and the crash harness; the
    /// default (`None`) makes every hook site a no-op.
    pub fn set_commit_hook(&self, hook: Option<Arc<dyn CommitHook>>) {
        *self.commit_hook.lock().expect("hook cell poisoned") = hook;
    }

    /// Fire the crash hook at `point`, if one is installed. The `Arc` is
    /// cloned out of the cell before the call so a hook that re-enters the env
    /// cannot deadlock on the cell.
    pub(crate) fn run_hook(&self, point: HookPoint) {
        let hook = self.commit_hook.lock().expect("hook cell poisoned").clone();
        if let Some(h) = hook {
            h.at(point);
        }
    }

    /// The backing for commit I/O (write pages / fsync). Panics only if called
    /// after close — impossible while any `Env`/txn borrow exists.
    pub(crate) fn backing_ref(&self) -> &dyn Backing {
        self.backing
            .as_deref()
            .expect("backing present while the env is open")
    }

    /// Pin a snapshot for a new read txn (SPEC 04 TXN-10 steps 1–3):
    /// claim a reader-table slot, run the TXN-17 SeqCst publish-and-verify
    /// loop against the commit point, and clone the published snapshot
    /// (adopting a newer one if a commit raced the clone — the TXN-17 tail).
    /// Returns the pinned snapshot and the owned slot index; the caller
    /// (`RoTxn`) releases the slot at drop via [`EnvInner::release_reader`].
    ///
    /// In a lock-file environment the commit point is the shared published
    /// txnid `T` and the snapshot comes from [`EnvInner::snapshot_at`]
    /// (ADR-0023 §4).
    ///
    /// # Errors
    ///
    /// [`MdbError::ReadersFull`] when every slot is occupied (TXN-16). In a
    /// lock-file environment also [`MdbError::MapResized`] when the snapshot
    /// at `T` lies beyond this process's logical map size, and
    /// [`MdbError::Invalid`] when it cannot be decoded or lies beyond the
    /// file.
    pub(crate) fn pin_reader(&self) -> Result<(Arc<Snapshot>, u32), Error> {
        // loom builds have no shared reader table (`ReaderTable::new_shared`
        // is not compiled), and the open path refuses a region there.
        #[cfg(not(loom))]
        if let Some(region) = &self.shared {
            return self.pin_shared(region.as_ref());
        }
        self.snap_cell
            .pin(&self.reader_table)
            .ok_or(Error::Mdb(MdbError::ReadersFull))
    }

    /// The lock-file arm of [`EnvInner::pin_reader`]: claim a shared slot,
    /// run TXN-17 against `T`, then resolve the snapshot for the verified
    /// txnid. The slot keeps the verified pin while the meta is read, so a
    /// retry after the slot moved on is conservative (GC holds back, TXN-20).
    #[cfg(not(loom))]
    fn pin_shared(
        &self,
        region: &dyn crate::shared::SharedRegion,
    ) -> Result<(Arc<Snapshot>, u32), Error> {
        let slot = self
            .reader_table
            .claim()
            .ok_or(Error::Mdb(MdbError::ReadersFull))?;
        // TXN-17 publish-and-verify against the shared T, then the snapshot
        // for the verified txnid; `None` means meta slot `t & 1` moved on.
        match self
            .reader_table
            .pin_against(slot, region.published_txnid(), META_RETRY_LIMIT, |t| {
                self.snapshot_at(t)
            }) {
            Ok(Some(snap)) => Ok((snap, slot)),
            Ok(None) => {
                self.reader_table.release(slot);
                Err(Error::Mdb(MdbError::Invalid))
            }
            Err(e) => {
                self.reader_table.release(slot);
                Err(e)
            }
        }
    }

    /// The snapshot for published txnid `t`: the cached one when it matches
    /// (the single-process fast path), else decoded from the data file and
    /// cached. `None` when meta slot `t & 1` has moved on or was torn.
    ///
    /// The decoded snapshot is checked against this process's map and the
    /// current file length before it is used or cached (SPEC 06 REC-1a). The
    /// cache hit skips both checks: a cached snapshot passed them already.
    fn snapshot_at(&self, t: u64) -> Result<Option<Arc<Snapshot>>, Error> {
        let cached = self.snap_cell.clone_snapshot();
        if cached.txnid == t {
            return Ok(Some(cached));
        }
        let backing = self.backing_ref();
        let Some(snap) = read_snapshot_at(backing, self.page_size, t)? else {
            return Ok(None);
        };
        check_snapshot_backed(backing, self.page_size, &snap)?;
        let snap = Arc::new(snap);
        self.snap_cell.install_if_newer(Arc::clone(&snap));
        Ok(Some(snap))
    }

    /// The latest published snapshot. In a lock-file environment it follows
    /// the shared `T`, so it includes other processes' commits (ADR-0023 §4);
    /// otherwise it is [`EnvInner::snapshot`]. The writer's base.
    ///
    /// # Errors
    ///
    /// In a lock-file environment, [`MdbError::MapResized`] when the snapshot
    /// at `T` lies beyond this process's logical map size, and
    /// [`MdbError::Invalid`] when it cannot be decoded or lies beyond the
    /// file.
    #[inline]
    pub(crate) fn current_snapshot(&self) -> Result<Arc<Snapshot>, Error> {
        self.writer_base(None)
    }

    /// [`EnvInner::current_snapshot`] for a writer holding the writer lock,
    /// given the snapshot the previous write txn of this process ended with
    /// (its published one, or its unchanged base; [`crate::rwtxn::WriterCarry`]).
    /// When that is still the latest it is returned as is: no lock of the
    /// snapshot cell and no reference-count change, which a write txn
    /// otherwise pays at every begin and end.
    ///
    /// Published txnids only grow — a commit publishes a txnid once, and a
    /// commit that fails or finishes a held election publishes nothing — so
    /// a cached snapshot whose txnid is the latest published one is the
    /// latest snapshot.
    ///
    /// # Errors
    ///
    /// As [`EnvInner::current_snapshot`].
    #[inline]
    pub(crate) fn writer_base(
        &self,
        cached: Option<Arc<Snapshot>>,
    ) -> Result<Arc<Snapshot>, Error> {
        match &self.shared {
            None => match cached {
                // The commit point is stored by the publishing writer, whose
                // writer-lock release happens-before this writer's acquire,
                // so this load sees the latest publish.
                Some(s) if s.txnid == self.snap_cell.commit_point() => Ok(s),
                _ => Ok(self.snapshot()),
            },
            Some(region) => self.current_snapshot_shared(region.as_ref(), cached),
        }
    }

    /// The lock-file arm of [`EnvInner::writer_base`]: the snapshot at the
    /// shared `T`, `cached` when that is it. Out of line so the default
    /// write-txn path stays small.
    #[inline(never)]
    fn current_snapshot_shared(
        &self,
        region: &dyn crate::shared::SharedRegion,
        cached: Option<Arc<Snapshot>>,
    ) -> Result<Arc<Snapshot>, Error> {
        for _ in 0..META_RETRY_LIMIT {
            // SeqCst: pairs with the committing writer's SeqCst store of T
            // (the same total order as TXN-17/19).
            let t = region.published_txnid().load(Ordering::SeqCst);
            if let Some(s) = cached.as_ref() {
                if s.txnid == t {
                    return Ok(cached.expect("matched above"));
                }
            }
            if let Some(s) = self.snapshot_at(t)? {
                return Ok(s);
            }
            std::thread::yield_now();
        }
        Err(Error::Mdb(MdbError::Invalid))
    }

    /// The newest published snapshot, without the map check: in a lock-file
    /// environment the snapshot committed as `T`, decoded from a `pread`
    /// copy of its meta slot (no map access); otherwise the process-local
    /// cell. For `Env::resize`, which holds both writer locks, so `T` cannot
    /// move while it runs (ADR-0023).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] when the published slot never decodes, or its
    /// meta is corrupt; the I/O error of the meta read.
    fn newest_snapshot_unchecked(&self) -> Result<Arc<Snapshot>, Error> {
        let Some(region) = &self.shared else {
            return Ok(self.snapshot());
        };
        for _ in 0..META_RETRY_LIMIT {
            // SeqCst: pairs with the committing writer's SeqCst store of T.
            let t = region.published_txnid().load(Ordering::SeqCst);
            if let Some(s) = read_snapshot_at(self.backing_ref(), self.page_size, t)? {
                return Ok(Arc::new(s));
            }
            std::thread::yield_now();
        }
        Err(Error::Mdb(MdbError::Invalid))
    }

    /// Release a reader-table slot (SPEC 04 TXN-18a: `Release` store of
    /// `RDR_FREE`). Called from `RoTxn::drop`, from whatever thread the
    /// `Send` txn ended up on (TXN-13).
    pub(crate) fn release_reader(&self, slot: u32) {
        self.reader_table.release(slot);
    }

    /// The reader table's fixed slot count (`MDB_envinfo::me_maxreaders`):
    /// `max_readers` as configured at open, or, in a lock-file environment,
    /// the count recorded in the lock file, which a joining process adopts
    /// (ADR-0023 §1 as amended).
    #[must_use]
    pub fn max_readers(&self) -> u32 {
        self.reader_table.capacity()
    }

    /// `MDB_envinfo::me_numreaders` parity: the **high-water** reader-slot
    /// count, which never decreases when a read txn ends, over every
    /// process's readers in a lock-file environment. See
    /// [`crate::readers::ReaderTable::num_readers`] for the fork evidence and
    /// docs/DIVERGENCES.md (`me_numreaders` semantics).
    #[must_use]
    pub fn num_readers(&self) -> u32 {
        self.reader_table.num_readers()
    }

    /// Reader slots **currently** occupied — a ZeroDB extension, and the
    /// number [`EnvInner::num_readers`] misleadingly looks like.
    /// A concurrently-sampled diagnostic count, not a synchronization point.
    #[must_use]
    pub fn live_readers(&self) -> u32 {
        self.reader_table.in_use()
    }

    /// The ordering in force for the database `sel` addresses
    /// (SPEC 03 §2.0).
    ///
    /// [`DbSel::Main`] is **always** [`KeyCmp::Default`]: the main tree is
    /// also the named-DB catalog, whose keys are DB names and whose values are
    /// engine-internal `DBRecord` bytes. A named DB is whatever was registered
    /// for its dbi, defaulting to memcmp.
    #[must_use]
    pub fn comparator_for(&self, sel: crate::rotxn::DbSel) -> KeyCmp<'_> {
        match sel {
            crate::rotxn::DbSel::Main => KeyCmp::Default,
            crate::rotxn::DbSel::Named(dbi) => self.comparators.get(dbi),
        }
    }

    /// Register a comparator for named database `dbi`. Called from
    /// the `*_with_comparator` open paths, never directly by users.
    pub(crate) fn register_comparator(
        &self,
        dbi: u32,
        cmp: Box<dyn Comparator>,
    ) -> Result<(), ComparatorError> {
        self.comparators.register(dbi, cmp)
    }

    /// Whether any custom comparator is registered on this env.
    /// Gates the memcmp-only compacting-copy path (SPEC 03 §2.0).
    #[must_use]
    pub fn has_custom_comparator(&self) -> bool {
        self.comparators.any_custom()
    }

    /// Per-slot reader introspection — `mdb_reader_list`. See
    /// [`Env::reader_list`] for the full contract; this is the raw form.
    #[must_use]
    pub fn reader_list(&self) -> Vec<ReaderEntry> {
        // Sample the commit point *once* so every age in the returned vector
        // is relative to the same reference point. Sampling per entry would
        // make the ages mutually inconsistent for no benefit.
        let now = self.txnid();
        self.reader_table
            .list()
            .into_iter()
            .map(|(slot, txnid, pid)| ReaderEntry {
                slot,
                txnid,
                pid,
                // `saturating_sub`, not a subtraction: a reader can publish a
                // pin *newer* than the commit point we sampled a moment ago
                // (it read a commit that landed in between), which would
                // otherwise underflow. Reporting age 0 for "as new as or newer
                // than the reference point" is the truthful answer.
                age: txnid.map(|t| now.saturating_sub(t)),
            })
            .collect()
    }

    /// The smallest snapshot txnid any live reader has pinned, if any: the
    /// SeqCst full-table scan of SPEC 04 TXN-20 (two-case proof quoted at the
    /// scan site, `crate::readers::ReaderTable::oldest`). The GC gate folds it
    /// with `writer_txnid − 1` (TXN-20/21) and caches it per write txn
    /// (TXN-22; ADR-0006 decision 6).
    #[must_use]
    pub(crate) fn oldest_live_reader(&self) -> Option<u64> {
        self.reader_table.oldest()
    }

    /// The named-database slot table (ADR-0024).
    pub(crate) fn handles(&self) -> &crate::handles::HandleTable {
        &self.handles
    }

    /// Whether a free slot may take another database's name: not when a
    /// custom comparator is registered for it, since that registration is
    /// per slot and permanent (ADR-0024).
    pub(crate) fn slot_reusable(&self, slot: u32) -> bool {
        matches!(self.comparators.get(slot), KeyCmp::Default)
    }

    /// The catalog capacity this env was opened with (`max_dbs`). Every dbi
    /// the registry hands out is below it. Lock-free.
    #[must_use]
    pub(crate) fn max_dbs(&self) -> u32 {
        self.max_dbs
    }

    /// The name for a named-DB dbi index (`DbSel::Named`), if the index is
    /// assigned. Cloned out so no registry lock is held by the caller.
    #[must_use]
    pub(crate) fn named_name(&self, dbi: u32) -> Option<Box<[u8]>> {
        self.handles.name(dbi)
    }

    /// Whether this env was opened on the previous (older) snapshot.
    #[must_use]
    pub fn is_prev_snapshot(&self) -> bool {
        self.prev_snapshot
    }

    /// The env-level durability / write-mode flags (SPEC 01 §S6/§S7).
    #[must_use]
    pub fn durability(&self) -> DurabilityFlags {
        self.durability
    }

    /// The page-validation policy this env was opened with (ADR-0014).
    #[must_use]
    pub fn file_trust(&self) -> FileTrust {
        self.file_trust
    }

    /// The env-wide cache of validated page versions (ADR-0018).
    pub(crate) fn stamp_cache(&self) -> &StampCache {
        &self.stamp_cache
    }

    /// Test/diagnostics hook (ADR-0018; like
    /// [`crate::rwtxn::RwTxn::spills`]): whether the env-wide
    /// validated-pages cache currently holds exactly the page version
    /// `(pgno, kind, stamp)` — `branch` selects the kind half of the key.
    /// Observes only; never part of the stable API.
    #[must_use]
    pub fn stamp_cache_has(&self, pgno: u64, branch: bool, stamp: u64) -> bool {
        let kind = if branch {
            crate::stamps::StampKind::Branch
        } else {
            crate::stamps::StampKind::Leaf
        };
        self.stamp_cache.contains(pgno, kind, stamp)
    }

    /// A write txn's dirty limit in pages (SPEC 04 TXN-68, ADR-0017).
    #[must_use]
    pub fn dirty_limit(&self) -> u64 {
        self.dirty_limit
    }

    /// Whether the main DB (`None`) or named DB `dbi` uses the sequential-
    /// writes fast path: its override if set, else the env default
    /// (ADR-0015).
    #[must_use]
    pub fn sequential_writes_for(&self, dbi: Option<u32>) -> bool {
        let slot = dbi.map_or(0, |d| d as usize + 1);
        // Ordering: `Relaxed` — the value is a performance hint that selects
        // between two paths with identical results, it publishes no other
        // data, and any value a racing read sees is a valid choice.
        match self
            .sequential_overrides
            .get(slot)
            .map_or(SEQ_FOLLOW, |a| a.load(Ordering::Relaxed))
        {
            SEQ_ON => true,
            SEQ_OFF => false,
            _ => self.sequential_writes,
        }
    }

    /// Set or clear (`None`) the sequential-writes override of the main DB
    /// (`dbi = None`) or named DB `dbi` (ADR-0015). Write txns that start
    /// after the call use it; an out-of-range dbi (impossible for a handle
    /// the engine produced) is ignored.
    pub fn set_sequential_writes(&self, dbi: Option<u32>, on: Option<bool>) {
        let slot = dbi.map_or(0, |d| d as usize + 1);
        let v = match on {
            None => SEQ_FOLLOW,
            Some(false) => SEQ_OFF,
            Some(true) => SEQ_ON,
        };
        if let Some(a) = self.sequential_overrides.get(slot) {
            // Ordering: `Relaxed` — see `sequential_writes_for`.
            a.store(v, Ordering::Relaxed);
        }
    }

    /// Whether the env is read-only (`MDB_RDONLY`, SPEC 01 Table 1).
    #[must_use]
    pub fn is_read_only(&self) -> bool {
        self.durability.read_only
    }

    /// [`EnvInner::sync`]`(true)`, the only form heed exposes.
    pub fn force_sync(&self) -> Result<(), Error> {
        self.sync(true)
    }

    /// Explicit environment sync — full `mdb_env_sync(env, force)` parity
    /// (SPEC 01 §S6). heed exposes only the `force = true` form
    /// ([`EnvInner::force_sync`]); `force = false` is a ZeroDB extension.
    ///
    /// The fork's `mdb_env_sync0` decides three things, reproduced exactly:
    ///
    /// 1. **`MDB_RDONLY` → `EACCES`**, before anything else, for either value
    ///    of `force`.
    /// 2. **Whether to flush at all**: only if `force || !NO_SYNC`. So
    ///    `sync(false)` on a `NO_SYNC` env is a **silent no-op returning
    ///    `Ok(())`** — it does *not* make prior commits durable. This is the
    ///    one behavioral difference between the two `force` values, and the
    ///    reason `force_sync` is the durability-restoring call.
    /// 3. **Which primitive**: under `WRITE_MAP`, `msync(MS_ASYNC)` iff
    ///    `MAP_ASYNC && !force`, else `MS_SYNC`; without `WRITE_MAP`,
    ///    `fdatasync`. I.e. `force` also downgrades `MAP_ASYNC` to a real
    ///    barrier. Expressed here as the `async_flush` argument to
    ///    [`Backing::sync`], which owns the primitive choice.
    ///
    /// A call that does flush always issues the barrier even when nothing is
    /// pending, matching LMDB (which likewise always calls through).
    ///
    /// # Errors
    ///
    /// - [`Error::Io`] (`EACCES`) on a read-only env (SPEC 01 Table 1).
    /// - [`Error::Io`] if the env is already poisoned (SPEC 06 REC-13).
    /// - [`Error::Io`] on an `msync`/`fsync` failure — which also **poisons**
    ///   the env (REC-13), exactly like a failed commit barrier.
    pub fn sync(&self, force: bool) -> Result<(), Error> {
        // (1) RDONLY is rejected before the NO_SYNC test, as in mdb_env_sync0.
        if self.durability.read_only {
            return Err(crate::error::eacces());
        }
        if self.is_poisoned() {
            return Err(Error::Io(std::io::Error::other(
                "environment poisoned by a failed durability barrier (SPEC 06 REC-13)",
            )));
        }
        // (2) `force || !NO_SYNC` — otherwise no flush is issued at all.
        if !force && self.durability.no_sync {
            return Ok(());
        }
        // (3) MS_ASYNC only when MAP_ASYNC is in effect and this is not a
        // forced sync. `Backing::sync` ignores `async_flush` unless the
        // backing is the writable map, matching the `MDB_WRITEMAP` guard.
        let async_flush = !force && self.durability.map_async && self.durability.write_map;
        // Serialize with the writer: a concurrent commit must not interleave its
        // pipeline with an explicit sync. The guard is released on return.
        let _guard = self.lock_writer();
        match self.backing_ref().sync(async_flush) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.poison(); // REC-13
                Err(e.into())
            }
        }
    }

    /// The canonical directory path (SPEC 00 row 21).
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The actual on-disk file length (SPEC 00 row 18).
    pub fn real_disk_size(&self) -> Result<u64, Error> {
        match self.backing.as_ref() {
            Some(b) => Ok(b.real_disk_size()?),
            None => Err(Error::Mdb(MdbError::Invalid)),
        }
    }

    /// The whole mapped env file as bytes, borrowed for as long as this
    /// `EnvInner` is borrowed (SPEC 04 TXN-37). A `RoTxn` holds `&'env Env`,
    /// which keeps the owning `Arc<EnvInner>` (and hence this map) alive for the
    /// txn's life, so the returned slice is valid `'env`. Returns an empty slice
    /// only after close has released the map (never observed by a live txn).
    #[must_use]
    pub fn backing_bytes(&self) -> &[u8] {
        match self.backing.as_ref() {
            Some(b) => b.bytes(),
            None => &[],
        }
    }

    /// The path the data file was opened at (SPEC 00 row 17: a raw copy
    /// clones the file it names, after checking it is still the same file).
    #[must_use]
    pub fn data_path(&self) -> Option<&Path> {
        self.backing.as_ref().and_then(|b| b.data_path())
    }

    /// `dup()` the data-file fd for raw snapshot streaming (SPEC 00 row 22).
    pub fn try_clone_inner_file(&self) -> Result<std::fs::File, Error> {
        match self.backing.as_ref() {
            Some(b) => Ok(b.try_clone_file()?),
            None => Err(Error::Mdb(MdbError::Invalid)),
        }
    }

    /// Whether allocating an `n`-page run at `next_pgno` would exceed the map
    /// (SPEC 02 §8, `MdbError::MapFull`). Exposed so the geometry ceiling is
    /// testable at the env level; the write path checks `is_map_full` itself.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn would_map_full(&self, next_pgno: u64, n: u64) -> bool {
        use crate::page::geometry::{is_map_full, map_pages};
        is_map_full(next_pgno, n, map_pages(self.map_size(), self.page_size))
    }
}

impl Drop for EnvInner {
    fn drop(&mut self) {
        // Remove our own registry slot (guarded by id against a re-registered
        // path — see `RegEntry`).
        if let Ok(mut reg) = registry().lock() {
            if let Some((rid, _)) = reg.get(&self.path) {
                if *rid == self.id {
                    reg.remove(&self.path);
                }
            }
        }
        // Release the map (and, inside `MmapBacking`, close the fd) *before*
        // firing the close event, so `EnvClosingEvent::wait` returns only once
        // teardown has actually run (SPEC 04 TXN-53). `Drop` holds `&mut self`
        // (the last `Arc` reference is gone), so taking the backing here races
        // with no reader.
        drop(self.backing.take());
        // Release the cross-process region (ADR-0023) before the close event
        // too: dropping the last reference closes the lock file, which
        // releases its record locks and this process's claim on its path. A
        // waiter woken by the event may reopen at once, and would otherwise
        // find the lock file still open (`EnvAlreadyOpened`). The reader table
        // holds the only other reference; write txns borrow the region and
        // cannot outlive this `EnvInner`.
        self.reader_table.release_region();
        let shared = self.shared.take();
        // With the reader table's reference released, this is the last one,
        // so the drop below really closes the lock file before the close
        // event fires. (Unit tests keep their own reference to the in-memory
        // test region they open with, so the check is for every other build.)
        #[cfg(not(test))]
        if let Some(region) = &shared {
            debug_assert_eq!(
                Arc::strong_count(region),
                1,
                "the lock-file region outlives its environment"
            );
        }
        drop(shared);
        self.closing.signal();
    }
}

/// A cheap, cloneable environment handle (SPEC 00 row 25, SPEC 04 TXN-50).
///
/// `Clone` bumps the refcount; it never reopens the file. The underlying
/// `EnvInner` is torn down only when the last clone (and every outstanding
/// txn) drops.
#[derive(Clone)]
pub struct Env {
    inner: Arc<EnvInner>,
}

impl std::fmt::Debug for Env {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Env").field(&self.inner).finish()
    }
}

impl Env {
    /// Access the shared inner state.
    #[must_use]
    pub(crate) fn inner(&self) -> &Arc<EnvInner> {
        &self.inner
    }

    /// The process-local published snapshot (test and harness hook).
    #[doc(hidden)]
    #[must_use]
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.inner.snapshot()
    }

    /// The whole mapped data file (test and harness hook).
    #[doc(hidden)]
    #[must_use]
    pub fn backing_bytes(&self) -> &[u8] {
        self.inner.backing_bytes()
    }

    /// Whether the validated-page cache holds `pgno` at `stamp` (test hook).
    #[doc(hidden)]
    #[must_use]
    pub fn stamp_cache_has(&self, pgno: u64, branch: bool, stamp: u64) -> bool {
        self.inner.stamp_cache_has(pgno, branch, stamp)
    }

    /// An identity for this environment, equal across every clone of the same
    /// `Env` and distinct from every other open environment (the address of the
    /// shared inner state). Consumers use it to assert that a database handle
    /// and a transaction belong to the same environment, as heed does with its
    /// `MDB_env` pointer.
    #[must_use]
    pub fn ident(&self) -> usize {
        Arc::as_ptr(self.inner()) as *const () as usize
    }

    /// The DB page size (SPEC 02 §3.2, authoritative from the live meta).
    #[must_use]
    pub fn page_size(&self) -> u32 {
        self.inner.page_size()
    }

    /// The runtime map size (SPEC 00 row 20).
    #[must_use]
    pub fn map_size(&self) -> u64 {
        self.inner.map_size()
    }

    /// Whether this process opened the environment alone: it won the lock
    /// file's first-opener election, so no other process had the environment
    /// open at that moment (ADR-0023). Always `true` without a
    /// lock file, where the environment is single-process.
    ///
    /// The answer describes the open only: another process can join later.
    /// `zerodb-tools load` uses it to refuse a destination that another
    /// process has open.
    #[must_use]
    pub fn opened_exclusively(&self) -> bool {
        self.inner
            .shared
            .as_ref()
            .is_none_or(|r| r.is_initializer())
    }

    /// Whether this environment runs with a lock file shared with other
    /// processes (ADR-0023). `false` when it was opened without
    /// one, and for a read-only environment on read-only media, where no
    /// lock file can be created and the open proceeds without one, as LMDB
    /// does.
    #[must_use]
    pub fn has_lock_file(&self) -> bool {
        self.inner.shared.is_some()
    }

    /// Change the map size, as `mdb_env_set_mapsize` on an open env
    /// (ADR-0023).
    ///
    /// - `0` adopts the map size recorded in the newest committed meta
    ///   (`OBS_SET_MAPSIZE_ZERO_ADOPTS`), the answer to a
    ///   [`MdbError::MapResized`] begin.
    /// - A size below the newest snapshot's data rounds up to exactly
    ///   `(last_pg + 1) * page_size`, silently, as LMDB does.
    /// - A smaller size than the current mapping lowers only the logical
    ///   size: nothing is ever unmapped before the env closes.
    ///
    /// Live read txns do not block it, and every value they borrowed stays
    /// valid (grow-only mappings). In a lock-file environment it takes the
    /// cross-process writer lock, waiting like a write begin, so no commit
    /// lands while it rounds up. LMDB takes no lock here: its
    /// `mdb_env_set_mapsize` returns while another process holds a write txn
    /// (`OBS_SET_MAPSIZE_OTHER_PROCESS_WRITER`; docs/DIVERGENCES.md D-023).
    /// Under `WRITE_MAP` it grows the data file to the new size, never
    /// shrinking it.
    ///
    /// # Errors
    ///
    /// - [`Error::Io`] with raw OS error `EINVAL` (22) while a write txn of
    ///   this env is live (`OBS_SET_MAPSIZE_WITH_WRITE_TXN`).
    /// - [`Error::Io`] (`InvalidInput`) for a requested size above
    ///   [`MAX_MAP_SIZE`](crate::page::geometry::MAX_MAP_SIZE);
    ///   [`MdbError::Invalid`] when `0` adopts a meta size above it (a corrupt
    ///   meta), or the newest meta cannot be read.
    /// - The I/O error of the cross-process lock, of the `mmap`, or of the
    ///   file growth.
    pub fn resize(&self, new_size: u64) -> Result<(), Error> {
        let inner = self.inner();
        // OBS_SET_MAPSIZE_WITH_WRITE_TXN: EINVAL while a write txn is live.
        let mut guard = inner
            .write_mutex
            .try_acquire()
            .ok_or_else(crate::error::einval)?;
        // Step 2: the cross-process writer lock (a no-op without a lock
        // file), through the write begin's path, which reaps after a
        // dead-owner takeover. Every error below drops `guard`, releasing
        // both locks.
        inner.lock_writer_shared(&mut guard)?;
        // ADR-0023: the newest meta on disk, as LMDB's
        // `mdb_env_set_mapsize`, including an unpublished first commit.
        let snap = inner.or_newer_unpublished(inner.newest_snapshot_unchecked()?);
        let min = bytes_needed(snap.last_pg, inner.page_size())?;
        // OBS_SET_MAPSIZE_ZERO_ADOPTS: 0 adopts the newest meta's size.
        let (size, from_meta) = if new_size == 0 {
            (snap.map_size, true)
        } else {
            (new_size, false)
        };
        // LMDB rounds up silently.
        let size = size.max(min);
        if size > crate::page::geometry::MAX_MAP_SIZE {
            return Err(if from_meta {
                Error::Mdb(MdbError::Invalid)
            } else {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "map size ({size}) exceeds the supported maximum {}",
                        crate::page::geometry::MAX_MAP_SIZE
                    ),
                ))
            });
        }
        inner.backing_ref().grow_map(size).map_err(Error::Io)?;
        // The guard's drop releases both writer locks.
        drop(guard);
        Ok(())
    }

    /// Environment info (SPEC 00 rows 20/60; `mdb_env_info`). The struct is
    /// complete — see [`EnvInfo`] for the field-by-field
    /// `MDB_envinfo` mapping and which values are format-specific.
    ///
    /// All fields are read from the **live published snapshot** plus the reader
    /// table; no transaction is opened, so this never blocks a writer and never
    /// consumes a reader slot. In a lock-file environment the snapshot follows
    /// the shared published txnid `T` (`EnvInner::current_snapshot`), so
    /// `last_pgno`/`last_txnid` reflect another process's commit as soon as it
    /// publishes (`OBS_INFO_SEES_OTHER_COMMIT`, ADR-0023 §4). This call stays
    /// infallible: if the snapshot at `T` cannot be read right now (for
    /// example [`MdbError::MapResized`], when `T` has moved past a commit that
    /// grew the map beyond this process's current mapping), it falls back to
    /// this process's own cached snapshot (`EnvInner::snapshot`) rather than
    /// surfacing the error, which can under-report a page/txnid that another
    /// process has already published. That fallback has no dedicated test.
    ///
    /// It can also report a commit that no read txn sees yet, as LMDB's
    /// `mdb_env_info` reports the newest meta on disk (ADR-0023,
    /// SPEC 04 TXN-67): a `PREV_SNAPSHOT` opener's unpublished first commit,
    /// and, in a process that joined, a meta newer than `T` that it found on
    /// disk at open (such an opener's first commit, or another process's
    /// commit between its meta write and its `T` store). Either is reported
    /// until `T` reaches it, and `last_txnid` can then exceed
    /// [`Env::txnid`]. The metas are not re-read per call, so a newer meta
    /// written after this process opened and never published (a writer that
    /// died before storing `T`) is not reported; LMDB's would report it.
    #[must_use]
    pub fn info(&self) -> EnvInfo {
        let snap = self
            .inner
            .current_snapshot()
            .unwrap_or_else(|_| self.inner.snapshot());
        // ADR-0023: an unpublished first `PREV_SNAPSHOT` commit
        // is the newest meta on disk, which LMDB's `mdb_env_info` reports.
        let snap = self.inner.or_newer_unpublished(snap);
        EnvInfo {
            map_size: self.inner.map_size(),
            last_pgno: snap.last_pg,
            last_txnid: snap.txnid,
            max_readers: self.inner.max_readers(),
            num_readers: self.inner.num_readers(),
            live_readers: self.inner.live_readers(),
        }
    }

    /// Environment-level statistics (`mdb_env_stat`): the [`EnvStat`] of the
    /// **main** DB of the live snapshot.
    ///
    /// Like [`Env::info`] this reads the published snapshot directly rather
    /// than opening a read txn, so it neither blocks nor occupies a reader
    /// slot. See [`EnvStat`] for the `MDB_stat` field mapping; the page counts
    /// are ZeroDB-format values (own on-disk format) and must not be compared to LMDB's.
    /// In a lock-file environment this follows the shared published txnid `T`
    /// the same way [`Env::info`] does, so another process's commit is visible
    /// as soon as it publishes (`OBS_STAT_SEES_OTHER_COMMIT`), with the same
    /// fall back to the cached snapshot if the read at `T` fails.
    ///
    /// Like [`Env::info`], it reports an unpublished `PREV_SNAPSHOT` first
    /// commit, or a joiner's newer meta found on disk at open, until `T`
    /// reaches it, so its counts can describe a commit no read txn sees yet
    /// (ADR-0023, SPEC 04 TXN-67).
    #[must_use]
    pub fn stat(&self) -> EnvStat {
        let snap = self
            .inner
            .current_snapshot()
            .unwrap_or_else(|_| self.inner.snapshot());
        // As in `info` (ADR-0023).
        let snap = self.inner.or_newer_unpublished(snap);
        let rec = snap.main_db;
        EnvStat {
            page_size: self.inner.page_size(),
            depth: rec.depth,
            branch_pages: rec.branch_pages,
            leaf_pages: rec.leaf_pages,
            overflow_pages: rec.overflow_pages,
            entries: rec.entries,
        }
    }

    /// List the environment's **occupied** reader slots —
    /// ZeroDB's answer to `mdb_reader_list`, which heed does not expose at all.
    ///
    /// One [`ReaderEntry`] per slot that is currently owned by a read
    /// transaction, in slot order. Free slots are omitted, exactly as
    /// `mdb_reader_list` skips slots with `mr_pid == 0`: presence in the
    /// returned vector *is* the liveness answer, and `max_readers() -
    /// reader_list().len()` is the number of free slots. Each entry gives the
    /// slot index, the pinned snapshot txnid, its age, and, in an environment
    /// opened with a lock file, the owner's PID (LMDB's `pid` column). There
    /// is no thread column: a `RoTxn` is `Send`, so its thread is not a
    /// stable property.
    ///
    /// # This is a sample, not a snapshot
    ///
    /// The reader table is lock-free (SPEC 04 §4): slots are read one at a
    /// time, so **entries can already be stale by the time this returns**, and
    /// the vector may correspond to no single instant — a read txn can end (or
    /// begin) between two slot loads. This is inherent to lock-free
    /// introspection, not an implementation shortcut: the only way to get a
    /// linearizable answer would be to stop the world, which would make the
    /// diagnostic itself a correctness hazard. Treat the result as a
    /// monitoring sample. Nothing in the engine consults it; the GC gate uses
    /// a different scan (TXN-20) whose staleness is provably one-directional.
    ///
    /// Ages are all computed against a single sampled commit point
    /// ([`Env::txnid`]); see [`ReaderEntry::age`].
    #[must_use]
    pub fn reader_list(&self) -> Vec<ReaderEntry> {
        self.inner.reader_list()
    }

    /// Reclaim reader slots abandoned by dead processes —
    /// `mdb_reader_check`. Returns the number of slots freed, counting
    /// slots, not processes.
    ///
    /// With a lock file (the default, ADR-0023 §3) the reader table is shared
    /// across processes, and one that dies inside a read txn leaves its slot
    /// pinned, holding GC back; this probes the owner of every occupied slot
    /// and frees those of processes that no longer have the environment
    /// open. As in LMDB, the only automatic reaping is by a writer that takes
    /// the writer lock over from a dead owner (Linux); a full reader table
    /// fails with `ReadersFull` without reaping. Without a lock file the
    /// table is process memory, every slot is released by its `RoTxn`'s
    /// `Drop`, and this always returns `0`.
    ///
    /// # Errors
    ///
    /// The I/O error of a liveness probe. Nothing has been freed then: every
    /// probe runs before any slot is touched.
    pub fn clear_stale_readers(&self) -> Result<usize, Error> {
        self.inner.clear_stale_readers()
    }

    /// Whether any custom key comparator is registered on this environment
    /// Gates the memcmp-only compacting-copy path
    /// (SPEC 03 §2.0).
    #[must_use]
    pub fn has_custom_comparator(&self) -> bool {
        self.inner.has_custom_comparator()
    }

    /// The canonical directory path (SPEC 00 row 21).
    #[must_use]
    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    /// The txnid of the live snapshot: in a lock-file environment, the
    /// shared published txnid `T`, which includes other processes' commits.
    ///
    /// In a `PREV_SNAPSHOT` opener with a lock file, between its first and
    /// second commit, it is the first commit's txnid, which is durable but not
    /// published: read txns still pin the older snapshot (ADR-0023, SPEC 04
    /// TXN-67). In a process that joined, it stays at `T` even while
    /// [`Env::info`] and [`Env::stat`] report a newer meta.
    #[must_use]
    pub fn txnid(&self) -> u64 {
        self.inner.txnid()
    }

    /// Whether opened on the previous snapshot (`PREV_SNAPSHOT`).
    #[must_use]
    pub fn is_prev_snapshot(&self) -> bool {
        self.inner.is_prev_snapshot()
    }

    /// Whether the env is read-only (`MDB_RDONLY`, SPEC 01 Table 1).
    #[must_use]
    pub fn is_read_only(&self) -> bool {
        self.inner.is_read_only()
    }

    /// The env-level durability / write-mode flags (SPEC 01 §S6/§S7).
    #[must_use]
    pub fn durability(&self) -> DurabilityFlags {
        self.inner.durability()
    }

    /// The page-validation policy this env was opened with (ADR-0014).
    #[must_use]
    pub fn file_trust(&self) -> FileTrust {
        self.inner.file_trust()
    }

    /// Force durability of all prior commits — `mdb_env_sync(env, 1)`
    /// (SPEC 01 §S6). Restores durability under `NO_SYNC` / `NO_META_SYNC` /
    /// `MAP_ASYNC`. The form heed exposes; equivalent to [`Env::sync`]`(true)`.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] (`EACCES`) on a read-only env; [`Error::Io`] on an
    /// `msync`/`fsync` failure (which poisons the env, REC-13).
    pub fn force_sync(&self) -> Result<(), Error> {
        self.inner.force_sync()
    }

    /// Explicit environment sync with full `mdb_env_sync(env, force)` parity
    /// `force = false` is a ZeroDB extension — heed exposes
    /// only [`Env::force_sync`]. See `EnvInner::sync` for the exact
    /// three-way semantics (`EACCES`, the `force || !NO_SYNC` gate, and the
    /// `MAP_ASYNC` downgrade).
    ///
    /// # Errors
    ///
    /// See `EnvInner::sync`.
    pub fn sync(&self, force: bool) -> Result<(), Error> {
        self.inner.sync(force)
    }

    /// Actual on-disk file size (SPEC 00 row 18).
    ///
    /// # Errors
    ///
    /// Propagates the `fstat` I/O error.
    pub fn real_disk_size(&self) -> Result<u64, Error> {
        self.inner.real_disk_size()
    }

    /// `dup()` the data-file fd (SPEC 00 row 22).
    ///
    /// # Errors
    ///
    /// Propagates the `dup` I/O error.
    pub fn try_clone_inner_file(&self) -> Result<std::fs::File, Error> {
        self.inner.try_clone_inner_file()
    }

    /// The path the data file was opened at, or `None` for a backing that
    /// is not a file opened by path. The file at that path may since have
    /// been renamed or replaced: compare it with
    /// [`Env::try_clone_inner_file`] before relying on it.
    #[must_use]
    pub fn data_path(&self) -> Option<&Path> {
        self.inner.data_path()
    }

    /// Number of strong references to the shared inner (this handle included).
    /// Diagnostic; the deferred-close contract does not depend on it.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn handle_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    /// Install (or clear) the commit-pipeline crash hook (ADR-0004 D3). See
    /// `EnvInner::set_commit_hook`; test infrastructure (commit smoke test,
    /// crash harness).
    pub fn set_commit_hook(&self, hook: Option<Arc<dyn CommitHook>>) {
        self.inner.set_commit_hook(hook);
    }

    /// Consume this handle and return an [`EnvClosingEvent`] that fires when the
    /// **last** reference to the env drops (SPEC 04 TXN-52). Dropping this
    /// handle is part of the close: if it was the last reference, the event has
    /// already fired by the time this returns.
    #[must_use]
    pub fn prepare_for_closing(self) -> EnvClosingEvent {
        let event = EnvClosingEvent {
            signal: Arc::clone(&self.inner.closing),
        };
        // Drop this handle's strong ref; if it was the last, `EnvInner::drop`
        // fires `signal` now.
        drop(self);
        event
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod shared_meta_tests;

// `HeapRegion` exists only in non-loom test builds (loom models the
// in-process protocol, not the shared region).
#[cfg(all(test, not(loom)))]
mod shared_env_tests;
