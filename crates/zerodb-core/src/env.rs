//! Environment open/close, meta selection, and the same-process registry
//! (SPEC 02 §3.2, SPEC 06 §1, SPEC 04 §7). Milestone 1.2.
//!
//! This module owns the *behavioral* half of env lifecycle and is deliberately
//! I/O-free so it stays `miri`-clean: the mapped file bytes reach it through the
//! [`Backing`] trait, which the mmap layer (`zerodb-io`) implements for real
//! envs and which tests implement over a plain `Vec<u8>`. All `unsafe` (the
//! mmap) lives behind that trait in `zerodb-io`; this module contains none.
//!
//! What is implemented here (M1.2 scope): reading both meta slots, validating
//! each via the M1.1 predicate ([`crate::page::MetaPage::validate`]), selecting
//! the live snapshot (normal / `PREV_SNAPSHOT`), mapping the SPEC 06 REC error
//! taxonomy onto [`Error`], the process registry with `EnvAlreadyOpened`, the
//! refcounted [`EnvInner`] behind [`Env`] (`Clone`), and deferred close with
//! [`EnvClosingEvent`] (SPEC 04 TXN-52/53). Write txns, the reader table, and
//! the B-tree read path are later milestones and are *not* built here.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};
use std::time::Duration;

use crate::error::{Error, MdbError};
use crate::page::geometry::{is_map_full, map_pages};
use crate::page::{
    select_meta, DBRecord, MetaChoice, MetaPage, MetaValidity, META_A_PGNO, META_B_PGNO,
};

/// Read (and, for the write path, page-granular write) access to a
/// memory-mapped (or, in tests, heap) env file.
///
/// The concrete real implementation is `zerodb_io::MmapBacking`, which confines
/// the mmap `unsafe` and guarantees the map is unmapped before the file fd is
/// closed (SPEC 04 TXN-53). Tests supply a `Vec<u8>`-backed implementation so
/// the selection logic runs under `miri` (that implementation is read-only —
/// the write methods keep their erroring defaults, so miri tests exercise
/// mutation and in-txn reads, never commit I/O).
///
/// Implementors must be `Send + Sync`: an [`EnvInner`] is shared across threads
/// through an `Arc` and read txns hand out `Send` borrows.
pub trait Backing: Send + Sync {
    /// The whole mapped region as bytes. Slot 0 is `[0, page_size)`, slot 1 is
    /// `[page_size, 2*page_size)`; data pages follow. The region may extend
    /// past the current file length (ADR-0004 D4: the map covers the full
    /// `map_size`); callers only dereference pages a committed snapshot
    /// references, which are always within the file (SPEC 06 REC-14).
    fn bytes(&self) -> &[u8];

    /// The actual on-disk length of the backing file (`fstat`), for
    /// [`Env::real_disk_size`] (SPEC 00 row 18).
    fn real_disk_size(&self) -> std::io::Result<u64>;

    /// `dup()` the backing data-file descriptor (SPEC 00 row 22,
    /// [`Env::try_clone_inner_file`]). Requires the env to be a single regular
    /// data file (SPEC 02 §8).
    fn try_clone_file(&self) -> std::io::Result<std::fs::File>;

    /// Positioned write of `data` starting at page `pgno` (commit step C2/C4,
    /// SPEC 04 §9). `data.len()` is a multiple of `psize` (one page, or a whole
    /// overflow run). Writing past EOF extends the file. Not durable until
    /// [`Backing::sync_data`].
    ///
    /// The default errors with `Unsupported` — read-only backings (the miri
    /// test backing) never commit.
    ///
    /// # Errors
    ///
    /// Propagates the positioned-write I/O error.
    fn write_at_page(&self, pgno: u64, psize: u32, data: &[u8]) -> std::io::Result<()> {
        let _ = (pgno, psize, data);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "this backing is read-only",
        ))
    }

    /// Durability barrier for previously-written pages (commit steps C3/C5;
    /// `File::sync_data` — ADR-0004 D3 as amended by OQ3: std semantics as-is,
    /// `fdatasync` on Linux).
    ///
    /// # Errors
    ///
    /// Propagates the fsync I/O error (the caller poisons the env, REC-13).
    fn sync_data(&self) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "this backing is read-only",
        ))
    }
}

// ---------------------------------------------------------------------------
// Published snapshot + commit hooks (SPEC 04 TXN-18/19, §9; ADR-0004 D3)
// ---------------------------------------------------------------------------

/// An immutable committed snapshot: the roots and geometry of one committed
/// state (SPEC 04 TXN-18). Readers `Arc`-clone the env's published snapshot at
/// begin and never re-read a durable meta page (the slot a pinned txnid lived
/// in is overwritten two commits later, TXN-63).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    /// The commit point of this snapshot.
    pub txnid: u64,
    /// File high-water as of this commit (`next_pgno - 1`, SPEC 05 GC-15).
    pub last_pg: u64,
    /// Root/stats of the main/catalog DB.
    pub main_db: DBRecord,
    /// Root/stats of the free (GC) DB.
    pub free_db: DBRecord,
}

impl Snapshot {
    /// The snapshot a validated meta page describes.
    #[must_use]
    pub fn from_meta(meta: &MetaPage) -> Snapshot {
        Snapshot {
            txnid: meta.txnid,
            last_pg: meta.last_pg,
            main_db: meta.main_db,
            free_db: meta.free_db,
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
/// M1.11's harnesses install hooks that kill/tear at a chosen [`HookPoint`];
/// the M1.4 smoke test aborts the process at each point in turn.
pub trait CommitHook: Send + Sync {
    /// Called between commit steps, at `point`. May abort/kill the process.
    fn at(&self, point: HookPoint);
}

/// A cross-thread one-shot signal: fires once, when the last [`EnvInner`]
/// reference is dropped (SPEC 04 TXN-53). Waiters block until then.
#[derive(Debug)]
struct SignalEvent {
    fired: Mutex<bool>,
    cv: Condvar,
}

impl SignalEvent {
    fn new() -> SignalEvent {
        SignalEvent {
            fired: Mutex::new(false),
            cv: Condvar::new(),
        }
    }

    fn signal(&self) {
        let mut g = self.fired.lock().expect("signal mutex poisoned");
        *g = true;
        self.cv.notify_all();
    }

    fn wait(&self) {
        let mut g = self.fired.lock().expect("signal mutex poisoned");
        while !*g {
            g = self.cv.wait(g).expect("signal mutex poisoned");
        }
    }

    /// Wait up to `dur`. Returns `true` if the event has fired.
    fn wait_timeout(&self, dur: Duration) -> bool {
        let g = self.fired.lock().expect("signal mutex poisoned");
        if *g {
            return true;
        }
        let (g2, _res) = self.cv.wait_timeout(g, dur).expect("signal mutex poisoned");
        *g2
    }
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
// Named-DB registry (the dbi table, SPEC 02 §6, SPEC 04 TXN-10; M1.6)
// ---------------------------------------------------------------------------

/// The env-level named-database registry — ZeroDB's analogue of LMDB's
/// `me_dbxs` array (the stable dbi ↔ name mapping). A named `Database` handle
/// carries a small integer *dbi index* into `names`; the record itself always
/// resolves lazily from the transaction's catalog view (the main tree; SPEC 04
/// TXN-10 step 3), so this table maps **only** dbi → name, never dbi → root.
///
/// **Assignment is append-only within a process** (an interim simplification,
/// like the M1.5 reader registry). LMDB frees a dbi when the txn that opened it
/// aborts; ZeroDB keeps the slot and re-uses it on a later open of the same
/// name (`by_name`). This is **unobservable** through the heed/SPEC-00 surface:
/// resolution is always catalog-driven, so a handle whose creation was aborted
/// resolves to *absent* (its catalog entry was discarded with the dirty set),
/// and re-creating the name re-uses the same dbi. The only theoretical effect
/// is that `max_dbs` counts distinct names ever seen (incl. aborted) rather
/// than currently-live ones, so `DbsFull` could fire one creation early after
/// `max_dbs` *distinct* aborted-and-never-reused names — a case no consumer and
/// no oracle sequence produces (names are a bounded reused set). The full dbi
/// lifecycle (abort-frees-slot) lands with the M1.8 reader/handle rework.
#[derive(Debug)]
struct NamedRegistry {
    /// dbi index → name. Append-only; index is the `DbSel::Named` payload.
    names: Vec<Box<[u8]>>,
    /// name → dbi index, for `open`/`create` lookup.
    by_name: HashMap<Box<[u8]>, u32>,
    /// Catalog capacity (number of **named** DBs; the main DB is not counted,
    /// matching LMDB's `mdb_env_set_maxdbs` semantics).
    max_dbs: u32,
}

impl NamedRegistry {
    fn new(max_dbs: u32) -> NamedRegistry {
        NamedRegistry {
            names: Vec::new(),
            by_name: HashMap::new(),
            max_dbs,
        }
    }
}

// ---------------------------------------------------------------------------
// EnvInner / Env
// ---------------------------------------------------------------------------

/// The shared, refcounted heart of an environment (SPEC 04 TXN-50). Owns the
/// backing map, the selected snapshot metadata, the registry back-reference,
/// and the close signal. Torn down (map unmapped, fd closed, registry slot
/// cleared, close event fired) only when the last [`Env`] / txn reference drops
/// (TXN-52/53).
pub struct EnvInner {
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
    page_size: u32,
    /// The runtime map size (SPEC 02 §8): the caller's `map_size` if given, else
    /// the live meta's. Returned by [`Env::info`].
    map_size: u64,
    /// The meta selected **at open** (higher-txnid, or older under
    /// `PREV_SNAPSHOT`). Read once to seed the published snapshot (TXN-18);
    /// steady-state reads use [`EnvInner::snapshot`], never this field, because
    /// commits advance past it.
    meta: MetaPage,
    /// Whether this env was opened with `PREV_SNAPSHOT` (SPEC 01 §S5).
    prev_snapshot: bool,
    /// Close signal, shared with any outstanding [`EnvClosingEvent`].
    closing: Arc<SignalEvent>,
    /// The single-writer mutex (SPEC 04 TXN-6). Guards no data — the write
    /// txn's state lives in the `RwTxn` — it only serializes writers.
    write_mutex: Mutex<()>,
    /// The published-snapshot cell (SPEC 04 TXN-18). M1.4 placeholder per
    /// ADR-0004 OQ1: a `Mutex<Arc<Snapshot>>` with the TXN-19 publish order
    /// (swap the object, then store `commit_point`); M1.8 replaces the cell's
    /// implementation with the lock-free ArcSwap-style cell + reader table.
    snapshot: Mutex<Arc<Snapshot>>,
    /// Mirrors the published snapshot's txnid (SPEC 04 TXN-17/19). Stored
    /// `SeqCst` *after* the snapshot swap so a reader that observes the new
    /// commit point can always load matching-or-newer roots.
    commit_point: AtomicU64,
    /// Commit-pipeline crash hooks (ADR-0004 D3; default `None` = no-op).
    commit_hook: Mutex<Option<Arc<dyn CommitHook>>>,
    /// REC-13 fsync-gate: set (Release) when a commit fsync fails; checked
    /// (Acquire) at every write-txn begin and commit. A poisoned env still
    /// serves read txns from their pinned snapshots.
    poisoned: AtomicBool,
    /// **Interim reader registry** (M1.5, ADR-0005 OQ1; SPEC 04 TXN-21 as
    /// amended): refcounts of live `RoTxn` snapshot txnids, so the GC gate
    /// (`oldest_reader()`, SPEC 05 GC-18) never reclaims a page a live reader
    /// can still reach. A plain mutexed map — deliberately boring; it is
    /// replaced wholesale by the M1.8 lock-free reader table (TXN-20), whose
    /// only consumer is the same gate expression.
    readers: Mutex<std::collections::BTreeMap<u64, usize>>,
    /// The named-DB registry (the dbi table, SPEC 02 §6; M1.6). Guards the
    /// dbi ↔ name mapping only — records resolve from the catalog (TXN-10).
    named: Mutex<NamedRegistry>,
}

impl std::fmt::Debug for EnvInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvInner")
            .field("id", &self.id)
            .field("path", &self.path)
            .field("page_size", &self.page_size)
            .field("map_size", &self.map_size)
            .field("txnid", &self.meta.txnid)
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

    /// The runtime map size (SPEC 00 row 20, `Env::info().map_size`).
    #[must_use]
    pub fn map_size(&self) -> u64 {
        self.map_size
    }

    /// The txnid of the current live snapshot (the commit point; advances on
    /// every commit, SPEC 04 TXN-19).
    #[must_use]
    pub fn txnid(&self) -> u64 {
        // SeqCst: pairs with the publish store in `publish_snapshot` (TXN-19)
        // and the reader pin protocol (TXN-17) — see SPEC 04 §4.3 for why the
        // commit-point loads/stores are all SeqCst (StoreLoad on ARM).
        self.commit_point.load(Ordering::SeqCst)
    }

    /// The meta selected **at open** (creation-time geometry). Live roots must
    /// come from [`EnvInner::snapshot`] — this field goes stale after the first
    /// commit (TXN-18: the durable meta page is read only once, at open).
    #[must_use]
    pub fn meta(&self) -> &MetaPage {
        &self.meta
    }

    /// `Arc`-clone the current published snapshot (SPEC 04 TXN-18). The clone
    /// keeps the `(txnid, roots)` alive for the caller's life regardless of
    /// later commits. M1.4 cell placeholder: a brief mutex lock (ADR-0004 OQ1;
    /// lock-free in M1.8).
    #[must_use]
    pub fn snapshot(&self) -> Arc<Snapshot> {
        Arc::clone(&self.snapshot.lock().expect("snapshot cell poisoned"))
    }

    /// Publish a freshly committed snapshot (commit step C6) in the TXN-19
    /// order: (1) swap the `Arc<Snapshot>` into the cell, then (2) store the
    /// commit point `SeqCst`. Object-before-counter guarantees a reader that
    /// sees the new counter can load the matching (or newer) roots.
    pub(crate) fn publish_snapshot(&self, snap: Arc<Snapshot>) {
        let txnid = snap.txnid;
        *self.snapshot.lock().expect("snapshot cell poisoned") = snap;
        // SeqCst: the writer half of the TXN-17/TXN-19 StoreLoad pairing (ARM
        // weak memory) — the reader's publish-and-verify loop and this store
        // must share the single SeqCst total order.
        self.commit_point.store(txnid, Ordering::SeqCst);
    }

    /// Acquire the single-writer mutex (SPEC 04 TXN-6/7): blocks until the
    /// current writer finishes; never errors.
    pub(crate) fn lock_writer(&self) -> MutexGuard<'_, ()> {
        // A panicked writer poisons the std mutex, but the mutex guards no
        // data (the dirty set lived in the RwTxn and was dropped during
        // unwind — TXN-60 implicit abort), so clearing the poison is sound and
        // keeps the env usable after a writer panic.
        self.write_mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
    /// infrastructure for M1.4's smoke test and M1.11's crash harness; the
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

    /// Atomically clone the published snapshot **and** pin its txnid in the
    /// interim reader registry (ADR-0005 OQ1). The clone and the pin happen
    /// under the registry mutex, and the GC gate ([`EnvInner::oldest_live_reader`])
    /// also reads under that mutex, so a gate computation either runs before
    /// this section (in which case the writer it belongs to is `W` with
    /// published snapshot `W − 1`; the clone below then returns `≥ W − 1`,
    /// which `W`'s reclamation — capped at `F ≤ W − 1` — can never invalidate)
    /// or after it (and sees the pin). No register-then-verify retry loop is
    /// needed. Paired with [`EnvInner::deregister_reader`] in `RoTxn::drop`.
    /// Lock order: registry → snapshot cell; nothing takes them in the other
    /// order.
    pub(crate) fn pin_reader(&self) -> Arc<Snapshot> {
        let mut m = self.readers.lock().expect("reader registry poisoned");
        let snap = self.snapshot();
        *m.entry(snap.txnid).or_insert(0) += 1;
        snap
    }

    /// Deregister one reader pinned at `txnid` (interim registry).
    pub(crate) fn deregister_reader(&self, txnid: u64) {
        let mut m = self.readers.lock().expect("reader registry poisoned");
        match m.get_mut(&txnid) {
            Some(n) if *n > 1 => *n -= 1,
            Some(_) => {
                m.remove(&txnid);
            }
            None => debug_assert!(false, "deregistering an unregistered reader"),
        }
    }

    /// The smallest snapshot txnid any live reader has pinned, if any (interim
    /// registry; the GC gate folds it with `writer_txnid − 1` per SPEC 04
    /// TXN-20/21).
    #[must_use]
    pub(crate) fn oldest_live_reader(&self) -> Option<u64> {
        self.readers
            .lock()
            .expect("reader registry poisoned")
            .keys()
            .next()
            .copied()
    }

    /// The dbi index for `name`, assigning a fresh one if absent (SPEC 02 §6).
    /// Returns `None` when the catalog is full (`DbsFull`): the number of
    /// distinct named DBs has reached `max_dbs`.
    #[must_use]
    pub(crate) fn named_dbi_assign(&self, name: &[u8]) -> Option<u32> {
        let mut r = self.named.lock().expect("named registry poisoned");
        if let Some(&dbi) = r.by_name.get(name) {
            return Some(dbi);
        }
        if r.names.len() as u64 >= u64::from(r.max_dbs) {
            return None;
        }
        let dbi = r.names.len() as u32;
        let boxed: Box<[u8]> = name.into();
        r.names.push(boxed.clone());
        r.by_name.insert(boxed, dbi);
        Some(dbi)
    }

    /// The name for a named-DB dbi index (`DbSel::Named`), if the index is
    /// assigned. Cloned out so no registry lock is held by the caller.
    #[must_use]
    pub(crate) fn named_name(&self, dbi: u32) -> Option<Box<[u8]>> {
        self.named
            .lock()
            .expect("named registry poisoned")
            .names
            .get(dbi as usize)
            .cloned()
    }

    /// Whether this env was opened on the previous (older) snapshot.
    #[must_use]
    pub fn is_prev_snapshot(&self) -> bool {
        self.prev_snapshot
    }

    /// The canonical directory path (SPEC 00 row 21).
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The actual on-disk file length (SPEC 00 row 18).
    ///
    /// # Errors
    ///
    /// Propagates the `fstat` I/O error.
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

    /// `dup()` the data-file fd for raw snapshot streaming (SPEC 00 row 22).
    ///
    /// # Errors
    ///
    /// Propagates the `dup` I/O error.
    pub fn try_clone_inner_file(&self) -> Result<std::fs::File, Error> {
        match self.backing.as_ref() {
            Some(b) => Ok(b.try_clone_file()?),
            None => Err(Error::Mdb(MdbError::Invalid)),
        }
    }

    /// Whether allocating an `n`-page run at `next_pgno` would exceed the map
    /// (SPEC 02 §8, `MdbError::MapFull`). No consumer of the write path exists
    /// yet (M1.4); exposed now so the geometry ceiling is testable at open.
    #[must_use]
    pub fn would_map_full(&self, next_pgno: u64, n: u64) -> bool {
        is_map_full(next_pgno, n, map_pages(self.map_size, self.page_size))
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
        self.closing.signal();
    }
}

/// A cheap, cloneable environment handle (SPEC 00 row 25, SPEC 04 TXN-50).
///
/// `Clone` bumps the refcount; it never reopens the file. The underlying
/// [`EnvInner`] is torn down only when the last clone (and every outstanding
/// txn, in later milestones) drops.
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
    pub fn inner(&self) -> &Arc<EnvInner> {
        &self.inner
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

    /// Environment info (SPEC 00 row 20/60). Only `map_size` is populated in
    /// Phase 1 — the sole field any consumer reads (`Env::info().map_size`).
    #[must_use]
    pub fn info(&self) -> EnvInfo {
        EnvInfo {
            map_size: self.inner.map_size(),
        }
    }

    /// The canonical directory path (SPEC 00 row 21).
    #[must_use]
    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    /// The txnid of the live snapshot.
    #[must_use]
    pub fn txnid(&self) -> u64 {
        self.inner.txnid()
    }

    /// Whether opened on the previous snapshot (`PREV_SNAPSHOT`).
    #[must_use]
    pub fn is_prev_snapshot(&self) -> bool {
        self.inner.is_prev_snapshot()
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

    /// Number of strong references to the shared inner (this handle included).
    /// Diagnostic; the deferred-close contract does not depend on it.
    #[must_use]
    pub fn handle_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    /// Install (or clear) the commit-pipeline crash hook (ADR-0004 D3). See
    /// [`EnvInner::set_commit_hook`]; test infrastructure (M1.4 smoke, M1.11).
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

/// Environment info (SPEC 00 rows 20/60). Mirrors the single field any consumer
/// reads from `mdb_env_info`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvInfo {
    /// The configured/runtime map size, in bytes.
    pub map_size: u64,
}

/// A signal fired once the environment is fully closed (SPEC 04 TXN-53,
/// SPEC 00 rows 23/24). Obtained from [`Env::prepare_for_closing`].
#[derive(Clone)]
pub struct EnvClosingEvent {
    signal: Arc<SignalEvent>,
}

impl std::fmt::Debug for EnvClosingEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvClosingEvent").finish_non_exhaustive()
    }
}

impl EnvClosingEvent {
    /// Block until the environment is fully closed.
    pub fn wait(&self) {
        self.signal.wait();
    }

    /// Block until closed or `dur` elapses. Returns `true` if the env closed.
    #[must_use]
    pub fn wait_timeout(&self, dur: Duration) -> bool {
        self.signal.wait_timeout(dur)
    }
}

// ---------------------------------------------------------------------------
// Open
// ---------------------------------------------------------------------------

/// Open an env over an already-constructed [`Backing`] (SPEC 02 §3.2, SPEC 06
/// §1). This is the I/O-free core of open: the caller (`zerodb-io` / the public
/// `zerodb` crate) has already opened, created-if-new, and mapped the file, and
/// determined `page_size` (from the meta, or the creation option) and the
/// runtime `map_size`.
///
/// Steps: registry dedup (`EnvAlreadyOpened`) → validate both slots → select →
/// map REC taxonomy to [`Error`] → build & register [`EnvInner`].
///
/// `page_size` is the size the caller mapped/validated with; it must be the
/// authoritative page size for the file (the meta's own `page_size` field is
/// re-checked by [`MetaPage::validate`]). The registry key is `canonical_path`;
/// this function does **not** touch the filesystem.
///
/// # Errors
///
/// - [`Error::EnvAlreadyOpened`] if a live handle for `canonical_path` exists.
/// - [`Error::Mdb`]`(`[`MdbError::Invalid`]`)` if neither slot validates, or
///   under the one-valid + `PREV_SNAPSHOT` combination (SPEC 06 REC-2†).
pub fn open_with_backing(
    canonical_path: PathBuf,
    backing: Box<dyn Backing>,
    page_size: u32,
    map_size: u64,
    prev_snapshot: bool,
    max_dbs: u32,
) -> Result<Env, Error> {
    // Validate the two meta slots from the mapped bytes (SPEC 02 §3.2). A
    // decode error here (bad page size / truncated buffer) means the file is not
    // a usable env → Invalid.
    let bytes = backing.bytes();
    let ps = page_size as usize;
    let slot0 = read_slot(bytes, META_A_PGNO, ps, page_size)?;
    let slot1 = read_slot(bytes, META_B_PGNO, ps, page_size)?;

    // Select the live snapshot (SPEC 02 §3.2 / SPEC 06 REC-2..5).
    let meta = match select_meta(&slot0, &slot1, prev_snapshot) {
        MetaChoice::Both { meta, .. } => meta,
        MetaChoice::OnlyOne { meta, .. } => {
            if prev_snapshot {
                // REC-2† (ratified 2026-07-16): one valid slot + PREV_SNAPSHOT is
                // a hard error — there are not two committed snapshots to pick an
                // older from.
                return Err(Error::Mdb(MdbError::Invalid));
            }
            meta
        }
        // REC-3: both invalid → unrecoverable.
        MetaChoice::None => return Err(Error::Mdb(MdbError::Invalid)),
    };

    // Register under the process registry (SPEC 04 TXN-51). Hold the lock across
    // check + insert so two concurrent opens of one path cannot both succeed.
    let id = next_env_id();
    let closing = Arc::new(SignalEvent::new());
    let inner = Arc::new(EnvInner {
        id,
        path: canonical_path.clone(),
        backing: Some(backing),
        page_size,
        map_size,
        // Seed the published-snapshot cell from the durable meta — the one
        // and only time a meta *page* is read for roots (SPEC 04 TXN-18).
        snapshot: Mutex::new(Arc::new(Snapshot::from_meta(&meta))),
        commit_point: AtomicU64::new(meta.txnid),
        write_mutex: Mutex::new(()),
        commit_hook: Mutex::new(None),
        poisoned: AtomicBool::new(false),
        readers: Mutex::new(std::collections::BTreeMap::new()),
        named: Mutex::new(NamedRegistry::new(max_dbs)),
        meta,
        prev_snapshot,
        closing,
    });

    {
        let mut reg = registry().lock().expect("registry mutex poisoned");
        if let Some((_, w)) = reg.get(&canonical_path) {
            if w.upgrade().is_some() {
                // A live handle already exists (SPEC 04 TXN-51). `inner` is
                // dropped here, releasing its (freshly built) backing.
                return Err(Error::EnvAlreadyOpened);
            }
        }
        reg.insert(canonical_path, (id, Arc::downgrade(&inner)));
    }

    Ok(Env { inner })
}

/// Validate one meta slot at `pgno` from the mapped bytes.
fn read_slot(
    bytes: &[u8],
    pgno: u64,
    page_size_usize: usize,
    page_size: u32,
) -> Result<MetaValidity, Error> {
    let base = pgno as usize * page_size_usize;
    let end = base
        .checked_add(page_size_usize)
        .ok_or(Error::Mdb(MdbError::Invalid))?;
    if end > bytes.len() {
        // The mapped region does not even cover both meta slots → not an env.
        return Err(Error::Mdb(MdbError::Invalid));
    }
    // `validate` never panics on bad content; a decode error (bad reader psize)
    // is impossible here because `page_size` is validated by the caller, but map
    // any such error to Invalid defensively.
    MetaPage::validate(&bytes[base..end], page_size).map_err(|_| Error::Mdb(MdbError::Invalid))
}

/// Test-only helpers: a heap-backed [`Backing`] and an in-memory env
/// constructor, so txn/mutation logic runs under `miri` (no mmap, no file
/// I/O). **Not part of the stable API** — used by this crate's tests, the
/// crate's integration tests, and nothing else. The backing is read-only
/// (commit I/O keeps the erroring `Backing` defaults), so in-memory envs
/// exercise mutation and in-txn reads but never the commit pipeline — commits
/// are tested against real files in `crates/zerodb`.
#[doc(hidden)]
pub mod testutil {
    use super::{next_env_id, open_with_backing, Backing, Env};
    use crate::error::Error;
    use crate::page::MetaPage;
    use std::path::PathBuf;

    /// A heap-backed read-only [`Backing`].
    pub struct VecBacking(pub Vec<u8>);

    impl Backing for VecBacking {
        fn bytes(&self) -> &[u8] {
            &self.0
        }
        fn real_disk_size(&self) -> std::io::Result<u64> {
            Ok(self.0.len() as u64)
        }
        fn try_clone_file(&self) -> std::io::Result<std::fs::File> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "vec backing has no fd",
            ))
        }
    }

    /// A fresh in-memory env: both meta slots valid at txnid 0 (SPEC 02 §3.4),
    /// the backing vector sized to `map_size` (as a real map would be,
    /// ADR-0004 D4), registered under a unique virtual path.
    ///
    /// # Panics
    ///
    /// On an invalid `page_size` (test helper).
    #[must_use]
    pub fn mem_env(page_size: u32, map_size: u64) -> Env {
        let ps = page_size as usize;
        let mut buf = vec![0u8; map_size as usize];
        for slot in [0u64, 1] {
            let meta = MetaPage::create(slot, page_size, map_size);
            let base = slot as usize * ps;
            meta.encode(&mut buf[base..base + ps]).expect("valid meta");
        }
        let path = PathBuf::from(format!("/virtual/mem-env-{}", next_env_id()));
        // A generous named-DB capacity for tests (real envs pass the caller's
        // `max_dbs`; SPEC 02 §6 / M1.6).
        match open_with_backing(
            path,
            Box::new(VecBacking(buf)),
            page_size,
            map_size,
            false,
            128,
        ) {
            Ok(env) => env,
            Err(Error::Io(e)) => panic!("mem_env open failed: {e}"),
            Err(e) => panic!("mem_env open failed: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testutil::VecBacking;
    use super::*;
    use crate::page::MetaPage;

    const PS: u32 = 4096;
    const MAP: u64 = 1 << 20;

    /// Build a two-slot file image; each slot carries the given txnid (or is
    /// left as raw zeros when `None`, i.e. never written = invalid).
    fn image(slot0: Option<u64>, slot1: Option<u64>) -> Vec<u8> {
        let ps = PS as usize;
        let mut buf = vec![0u8; 2 * ps];
        if let Some(t) = slot0 {
            let mut m = MetaPage::create(0, PS, MAP);
            m.txnid = t;
            m.encode(&mut buf[0..ps]).unwrap();
        }
        if let Some(t) = slot1 {
            let mut m = MetaPage::create(1, PS, MAP);
            m.txnid = t;
            m.encode(&mut buf[ps..2 * ps]).unwrap();
        }
        buf
    }

    fn unique_path(tag: &str) -> PathBuf {
        let n = next_env_id();
        PathBuf::from(format!("/virtual/{tag}-{n}"))
    }

    fn open(buf: Vec<u8>, prev: bool, path: PathBuf) -> Result<Env, Error> {
        open_with_backing(path, Box::new(VecBacking(buf)), PS, MAP, prev, 128)
    }

    #[test]
    fn selects_higher_txnid() {
        let env = open(image(Some(3), Some(7)), false, unique_path("hi")).unwrap();
        assert_eq!(env.txnid(), 7);
        assert_eq!(env.page_size(), PS);
        assert_eq!(env.map_size(), MAP);
    }

    #[test]
    fn prev_snapshot_selects_lower_txnid() {
        let env = open(image(Some(3), Some(7)), true, unique_path("prev")).unwrap();
        assert_eq!(env.txnid(), 3);
        assert!(env.is_prev_snapshot());
    }

    #[test]
    fn one_valid_slot_wins_without_prev_snapshot() {
        // Slot 1 never written (all zeros) → invalid; slot 0 wins (REC-2).
        let env = open(image(Some(5), None), false, unique_path("one")).unwrap();
        assert_eq!(env.txnid(), 5);
    }

    #[test]
    fn one_valid_slot_with_prev_snapshot_is_invalid() {
        // REC-2†: one valid slot + PREV_SNAPSHOT → Invalid.
        let e = open(image(Some(5), None), true, unique_path("one-prev")).unwrap_err();
        assert!(matches!(e, Error::Mdb(MdbError::Invalid)));
    }

    #[test]
    fn both_invalid_is_invalid() {
        let e = open(image(None, None), false, unique_path("none")).unwrap_err();
        assert!(matches!(e, Error::Mdb(MdbError::Invalid)));
    }

    #[test]
    fn torn_higher_slot_falls_back_to_older() {
        // Slot 1 has the higher txnid but a corrupted CRC region → invalid;
        // open must fall back to the older, intact slot 0 (REC-2 one-valid).
        let mut buf = image(Some(4), Some(9));
        let ps = PS as usize;
        // Corrupt a byte inside slot 1's CRC-covered region [ps, ps+168).
        buf[ps + 100] ^= 0xFF;
        let env = open(buf, false, unique_path("torn")).unwrap();
        assert_eq!(env.txnid(), 4, "must fall back to the older intact slot");
    }

    #[test]
    fn registry_rejects_second_open() {
        let path = unique_path("dup");
        let env = open(image(Some(1), Some(1)), false, path.clone()).unwrap();
        let e = open(image(Some(1), Some(1)), false, path.clone()).unwrap_err();
        assert!(matches!(e, Error::EnvAlreadyOpened));
        drop(env);
        // After the first env drops, the path is free again.
        let _env2 = open(image(Some(1), Some(1)), false, path).unwrap();
    }

    #[test]
    fn clone_shares_inner_and_close_defers() {
        let env = open(image(Some(2), Some(2)), false, unique_path("clone")).unwrap();
        let c = env.clone();
        assert_eq!(env.handle_count(), 2);
        // prepare_for_closing on one handle: the other keeps the env alive, so
        // the event must NOT have fired yet.
        let ev = env.prepare_for_closing();
        assert!(
            !ev.wait_timeout(Duration::from_millis(0)),
            "close must wait for the surviving clone"
        );
        drop(c);
        // Now the last reference is gone; the event fires.
        ev.wait();
        assert!(ev.wait_timeout(Duration::from_millis(0)));
    }

    #[test]
    fn map_full_boundary_via_env() {
        let env = open(image(Some(1), Some(1)), false, unique_path("mapfull")).unwrap();
        // map_size 1 MiB, psize 4096 → 256 pages, valid pgnos 0..255.
        assert!(!env.inner().would_map_full(255, 1));
        assert!(env.inner().would_map_full(256, 1));
    }
}
