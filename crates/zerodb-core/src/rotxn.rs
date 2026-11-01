//! Read transactions and the heed-shaped read API over the [`btree`](crate::btree) cursor
//! (SPEC 04 §3, SPEC 00 read-op rows).
//!
//! A [`RoTxn`] pins the env's **published snapshot** at open: it claims a
//! reader-table slot, runs the SeqCst publish-and-verify pin (SPEC 04
//! TXN-10/15/17; ADR-0006), and `Arc`-clones the current [`Snapshot`]
//! object (TXN-18 — never re-reading a durable meta page, whose slot a later
//! commit overwrites). The pinned slot is what stops the writer's GC from
//! reclaiming any page this snapshot can reach (TXN-20/21); it is released
//! with a `Release` store at drop (TXN-18a), from whatever thread the `Send`
//! txn ended up on (TXN-13, WithoutTls). Tree bytes are borrowed from the
//! mapped file for the txn's life (TXN-37).
//!
//! The read API is generic over [`TxnRead`], so the same `Database` methods
//! serve a `RoTxn` (mapped bytes) **and** a write txn (`RwTxn`: dirty frames
//! first, map fallback — SPEC 04 TXN-38, ADR-0004 D2). heed's borrow model is
//! reproduced: reads take `&Txn` and return `&'txn [u8]`; mutations take
//! `&mut RwTxn`, so no read borrow can span a mutation (TXN-39).
//!
//! Named databases: a [`Database`] handle carries a [`DbSel`] — the
//! main/unnamed DB or a named DB addressed by its env-level *dbi index*
//! ([`Env::open_database`] / [`Env::create_database`]). A named DB's
//! `DBRecord` (root/depth/stats) is **not** stored in the [`RoTxn`] snapshot;
//! it resolves lazily from the transaction's catalog view — the main tree,
//! keyed by name, value = 48-byte `F_SUBDATA` record (SPEC 02 §6, SPEC 04
//! TXN-10 step 3). This module owns that resolution (`resolve_named_record`)
//! and the read API; the write side (create/clear/drop, catalog write-back)
//! lives in [`crate::rwtxn`].

use std::cell::Cell;
use std::ops::Bound;
use std::sync::{Arc, Mutex, OnceLock};

use crate::btree::{prefix_successor, Cursor, Source, Tree, ValidatedPages};
use crate::builder::StreamBuildError;
use crate::cmp::KeyCmp;
use crate::env::{Env, Snapshot};
use crate::error::{Error, MdbError, Result};
use crate::handles::{HandleTable, MemoCell, TxnHandles, DENSE_DBI_LIMIT};
use crate::page::{DBRecord, PageError, F_SUBDATA};
use std::cmp::Ordering;

/// Map a structural tree-decode error to the public taxonomy. A corrupt page
/// reached during a read means the store is not a valid zerodb file
/// (SPEC 00 row 56); for a builder/write-path-produced tree this never fires.
pub(crate) fn map_page_err(_e: PageError) -> Error {
    Error::Mdb(MdbError::Invalid)
}

/// Anything the read API can read through: a [`RoTxn`] (mapped bytes of a
/// committed snapshot) or a [`crate::rwtxn::RwTxn`] (the writer's in-progress
/// view). The three accessors are exactly what [`Tree`] needs (ADR-0004 D2).
pub trait TxnRead {
    /// Where this txn's pages come from (SPEC 04 TXN-37/38).
    fn source(&self) -> Source<'_>;
    /// The main DB's root/stats as this txn observes them.
    fn main_record(&self) -> &DBRecord;
    /// The free (GC) DB's root/stats as this txn observes them (SPEC 05 §7 —
    /// the `non_free_pages_size` walk reads the GC DB under a snapshot).
    fn free_record(&self) -> &DBRecord;
    /// The env's page size.
    fn page_size(&self) -> u32;
    /// The `DBRecord` (root/depth/stats) of the database `sel` addresses, as
    /// this txn observes it (SPEC 04 TXN-10 step 3). For the main DB this is
    /// [`TxnRead::main_record`]; for a named DB it resolves from the catalog
    /// (or the write txn's working record). An absent/unresolvable named DB
    /// yields [`DBRecord::empty`] (a lenient read view — the strict
    /// `Incompatible` check lives in `open`/`create`). Callers check the
    /// handle first ([`TxnRead::check_db`]).
    fn record_for(&self, db: &Database) -> DBRecord;
    /// The env's slot table, this transaction's state in it, and its parent's
    /// for a nested read child (ADR-0024). Engine-internal.
    #[doc(hidden)]
    fn handle_state(&self) -> (&HandleTable, &TxnHandles, Option<&TxnHandles>);
    /// Whether `db`'s handle may be used in this transaction (ADR-0024; SPEC
    /// 02 §6.2). The main database's handle always may.
    ///
    /// # Errors
    ///
    /// `Error::Io` with raw OS error `EINVAL` (22) for a handle that is not
    /// valid here; [`MdbError::BadDbi`] for one that was valid when this
    /// transaction began and whose database was removed since.
    #[inline]
    fn check_db(&self, db: &Database) -> Result<()> {
        match db.sel {
            DbSel::Main => Ok(()),
            DbSel::Named(slot) => {
                let (table, txn, parent) = self.handle_state();
                table.check(txn, parent, slot, db.generation)
            }
        }
    }
    /// The key ordering in force for the database `sel` addresses
    /// (SPEC 03 §2.0). [`DbSel::Main`] is always memcmp — it is the named-DB
    /// catalog. Every tree this module builds for a *user* database routes
    /// through here.
    fn comparator_for(&self, sel: DbSel) -> KeyCmp<'_>;
    /// The txn's validated-pages memo, if it keeps one: pages
    /// whose cells were fully validated earlier this txn and may be re-wrapped
    /// without the O(`num_keys`) cell walk. Default `None` = always fully
    /// validate.
    fn validated_pages(&self) -> Option<&ValidatedPages<'_>> {
        None
    }
    /// The meta free-list annex id count this txn observes (SPEC 05 §2a,
    /// ADR-0022; feeds [`free_page_count`]). A reader reports its snapshot's
    /// count; the writer reports its pool's live remainder (a working-state
    /// view, like [`TxnRead::free_record`]).
    fn free_annex_count(&self) -> u64 {
        0
    }
}

/// Which database a [`Database`] handle addresses (SPEC 02 §6). `Copy` so the
/// handle stays a cheap value like `heed::Database`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbSel {
    /// The main (unnamed/default) database — the meta's `main_db` and the
    /// named-DB catalog itself.
    Main,
    /// A named database, addressed by its env-level dbi index (the position in
    /// the env's named registry; SPEC 02 §6).
    Named(u32),
}

/// Resolve a named DB's `DBRecord` from a catalog view (SPEC 02 §6, SPEC 04
/// TXN-10 step 3): search the main tree for `name`; if the entry is a 48-byte
/// `F_SUBDATA` sub-DB record, decode it; otherwise (absent, or a plain user-key
/// collision) return [`DBRecord::empty`]. The strict collision → `Incompatible`
/// error is enforced only at `open`/`create` time, where a `create` intent
/// exists; a bare read is lenient.
pub(crate) fn resolve_named_record(
    src: Source<'_>,
    psize: u32,
    main: &DBRecord,
    name: &[u8],
) -> DBRecord {
    let tree = Tree::new(src, psize, main.root, main.depth);
    match tree.get_catalog_entry(name) {
        Ok(Some((flags, val))) if flags & F_SUBDATA != 0 => {
            DBRecord::from_bytes(val).unwrap_or_else(DBRecord::empty)
        }
        _ => DBRecord::empty(),
    }
}

/// How a [`RoTxn`] holds its environment: borrowed for a plain
/// `Env::read_txn` (TXN-23), or owned for a `'static`, env-owning
/// `Env::static_read_txn` (TXN-24 — the owned clone keeps the env, and hence
/// the mapped file, alive and blocks close until the txn drops, TXN-52).
enum EnvHandle<'e> {
    /// Plain `RoTxn<'env>`: borrows the env by lifetime; cannot outlive it.
    Borrowed(&'e Env),
    /// `RoTxn<'static>`: owns an `Env` clone (a strong `Arc<EnvInner>` ref).
    Owned(Env),
}

/// A read-only transaction: a consistent view of one committed snapshot
/// (SPEC 04 §3). It holds the [`Env`] (borrowed or owned, `EnvHandle`),
/// which keeps the mapped file alive so every `&'txn [u8]` it lends stays
/// valid (TXN-37); an `Arc` to the pinned [`Snapshot`] so its roots survive
/// later commits (TXN-18); and its reader-table slot, which gates the
/// writer's GC (TXN-20/21) and is released at drop (TXN-18a).
///
/// `RoTxn` is **`Send`** (WithoutTls, TXN-13): the slot is owned by the txn
/// *object* (`slot` is a plain field), never a thread, so the txn — and any
/// `&[u8]` it lends — may move to another thread (rayon/async). Asserted at
/// compile time by the `send_assertions` test below.
///
/// **Leaked readers (documented stall, ADR-0006 R5):** `mem::forget(ro_txn)`
/// is safe Rust and cannot be prevented; the slot then pins its txnid
/// forever, the GC gate stops advancing past it, and the file grows. Same
/// failure mode as a stale LMDB reader, except that no reap frees it while
/// this process lives: the slot's owner is this process, which is alive. In a
/// lock-file environment (ADR-0023) the slot is freed once this process has
/// exited, by `Env::clear_stale_readers` in another process or by the next
/// first opener; without a lock file it dies with the process. Reader
/// introspection (`Env::reader_list`) lists such slots.
pub struct RoTxn<'env> {
    env: EnvHandle<'env>,
    psize: u32,
    snap: Arc<Snapshot>,
    /// The owned reader-table slot (claimed in `EnvInner::pin_reader`).
    slot: u32,
    /// Per-txn memo of resolved named-DB records, keyed by dbi.
    ///
    /// Without it every read op on a named DB would redo the registry lock +
    /// name clone + a full catalog descent (`resolve_named_record`) — roughly
    /// doubling the tree work per `get` (docs/PERF-GAP-VS-LMDB.md, named-DB
    /// record re-resolved on every read).
    ///
    /// Soundness: this txn pins an immutable [`Snapshot`] (its catalog cannot
    /// change while pinned, TXN-18/20), and a slot keeps its name for as long
    /// as it keeps its generation (ADR-0024), so a (dbi, generation) →
    /// record resolution is constant for the txn's life. Entries carry the
    /// generation; another generation's lookup resolves past the memo.
    ///
    /// Two tiers. `named_dense` is the hot path: a dbi-indexed table of
    /// write-once slots, the shape of LMDB's per-txn `mt_dbs[dbi]` array.
    /// A hit is an index plus one `Acquire` load, with no lock, so rayon
    /// workers sharing one `RoTxn` (milli) do not contend. It is allocated on
    /// the first named access and sized `min(max_dbs, DENSE_DBI_LIMIT)`.
    /// `named_memo` (a locked `Vec` scan) serves only dbis past that cap, so a
    /// huge `max_dbs` never costs a huge per-txn allocation. Both keep
    /// `RoTxn` auto-`Sync`.
    named_dense: OnceLock<Box<[NamedEntry]>>,
    named_memo: Mutex<Vec<(u32, u32, DBRecord)>>,
    /// This transaction's state in the env's slot table (ADR-0024).
    handles: TxnHandles,
    /// Pages fully validated this txn (see
    /// [`ValidatedPages`]). Sound here because every page this snapshot can
    /// reach is immutable while its reader slot is held (TXN-20/21).
    validated: ValidatedPages<'env>,
}

impl RoTxn<'_> {
    /// The snapshot txnid this read txn observes (SPEC 04 TXN-4).
    #[must_use]
    pub fn txnid(&self) -> u64 {
        self.snap.txnid
    }

    /// The committed [`Snapshot`] this txn pins (roots + `last_pg`, SPEC 04
    /// TXN-18). `Env::copy_to_file` reads `last_pg`/`main_db`/`free_db`
    /// from here to bound a raw range copy and to synthesize the copy's meta
    /// pages.
    #[must_use]
    pub fn snapshot(&self) -> &Snapshot {
        &self.snap
    }

    /// The whole mapped env file as bytes, borrowed for the txn's life
    /// (SPEC 04 TXN-37). The non-compact `copy_to_file` copies the pinned
    /// snapshot's pages directly from here; pages this snapshot references are
    /// immutable while the txn's reader slot is held (TXN-20/21), so the copy
    /// is torn-free for reachable pages.
    #[must_use]
    pub fn map_bytes(&self) -> &[u8] {
        self.env_ref().inner().backing_bytes()
    }

    /// The env this txn reads (borrowed or owned).
    fn env_ref(&self) -> &Env {
        match &self.env {
            EnvHandle::Borrowed(e) => e,
            EnvHandle::Owned(e) => e,
        }
    }

    /// The identity of the environment this transaction belongs to
    /// ([`Env::ident`]).
    #[must_use]
    pub fn env_ident(&self) -> usize {
        self.env_ref().ident()
    }

    /// The dense memo entry for named DB `dbi`, allocating the table on the
    /// first named access; `None` past [`DENSE_DBI_LIMIT`].
    #[inline]
    fn dense_entry(&self, dbi: u32) -> Option<&NamedEntry> {
        if dbi >= DENSE_DBI_LIMIT {
            return None;
        }
        let table = self.named_dense.get_or_init(|| {
            let n = self.env_ref().inner().max_dbs().min(DENSE_DBI_LIMIT) as usize;
            // `try_with` fails only during thread-local teardown. A parked
            // table of another length (another env's `max_dbs`) is freed.
            match SPARE_NAMED.try_with(Cell::take).ok().flatten() {
                Some(spare) if spare.len() == n => spare,
                _ => (0..n).map(|_| NamedEntry::default()).collect(),
            }
        });
        table.get(dbi as usize)
    }

    /// End this txn in the slot table (ADR-0024): publish what it opened on
    /// a commit, free it otherwise. A txn that opened nothing returns at once.
    fn end_handles(&mut self, committed: bool) {
        if self.handles.is_idle() {
            return;
        }
        let inner = Arc::clone(self.env_ref().inner());
        inner.handles().end(&mut self.handles, committed);
    }

    /// `commit` (heed's `RoTxn::commit`; `mdb_txn_commit` on a read txn): end
    /// the txn, keeping the databases it opened for transactions that begin
    /// after it (ADR-0024). Dropping a read txn instead frees them.
    ///
    /// # Errors
    ///
    /// Infallible; returns [`Result`] for heed's API shape.
    pub fn commit(mut self) -> Result<()> {
        self.end_handles(true);
        Ok(())
    }

    /// Resolve `dbi`'s record against this txn's pinned catalog (uncached).
    fn resolve_named(&self, dbi: u32) -> DBRecord {
        match self.env_ref().inner().named_name(dbi) {
            Some(name) => {
                resolve_named_record(self.source(), self.psize, &self.snap.main_db, &name)
            }
            None => DBRecord::empty(),
        }
    }
}

/// A memoized named-DB record and the slot generation it was resolved for.
type GenRecord = (u32, DBRecord);

/// One slot of [`RoTxn`]'s dense memo: the resolved record, and the handle
/// check's memo cell (ADR-0024, [`HandleTable::check_with`]), kept together so
/// a read touches one table.
#[derive(Default)]
struct NamedEntry {
    rec: OnceLock<GenRecord>,
    checked: MemoCell,
}

thread_local! {
    /// One cleared named-database table, left by the last read txn dropped on
    /// this thread that had one.
    ///
    /// A read txn that touches a named database allocates its dbi-indexed
    /// table (`named_dense`), and frees it at drop; for a txn that reads one
    /// key that was a large share of its cost. LMDB keeps its per-txn
    /// `mt_dbs` array with the transaction object, which a caller reuses
    /// through `mdb_txn_reset` / `mdb_txn_renew`; heed 0.22.1 does not expose
    /// those, so the reuse happens here. Every entry is reset to its default
    /// before the table is parked, so a later txn starts with an empty table,
    /// exactly as with a fresh allocation: records belong to one snapshot and
    /// are never carried over.
    static SPARE_NAMED: Cell<Option<Box<[NamedEntry]>>> = const { Cell::new(None) };
}

impl Drop for RoTxn<'_> {
    fn drop(&mut self) {
        // TXN-18a: release the slot (Release store of RDR_FREE) so the GC
        // gate can advance past this snapshot. Ordering vs teardown: this
        // body runs *before* the struct's fields drop, so for an env-owning
        // txn (TXN-24) the owned `Env` — and with it the reader table — is
        // still alive here; the handle's strong ref drops after, which is
        // what un-blocks a pending close (TXN-52/53).
        self.env_ref().inner().release_reader(self.slot);
        // ADR-0024: an end without a commit frees what this txn opened.
        self.end_handles(false);
        // Park the named-DB table, cleared, for the next txn on this thread.
        if let Some(mut table) = self.named_dense.take() {
            for entry in table.iter_mut() {
                *entry = NamedEntry::default();
            }
            // Replaces (and frees) a table parked first. Fails only during
            // thread-local teardown, which frees `table`.
            let _ = SPARE_NAMED.try_with(|spare| spare.set(Some(table)));
        }
    }
}

impl TxnRead for RoTxn<'_> {
    fn source(&self) -> Source<'_> {
        // Borrowed lazily from the env on each access (rather than cached at
        // open) so the same struct supports the env-owning 'static shape
        // without self-reference; the map's address is stable for the txn's
        // life either way (TXN-37: the env cannot close while this txn holds
        // it, borrowed or owned).
        Source::Map {
            bytes: self.env_ref().inner().backing_bytes(),
            // The pinned snapshot's committed high-water bounds every map
            // read (SPEC 06 REC-14: no committed reference exceeds it, so any
            // pgno above it is corruption, refused typed).
            last_pg: self.snap.last_pg,
        }
    }
    fn main_record(&self) -> &DBRecord {
        &self.snap.main_db
    }
    fn free_record(&self) -> &DBRecord {
        &self.snap.free_db
    }
    fn free_annex_count(&self) -> u64 {
        self.snap.free_annex.len() as u64
    }
    fn page_size(&self) -> u32 {
        self.psize
    }
    fn record_for(&self, db: &Database) -> DBRecord {
        let generation = db.generation;
        match db.sel {
            DbSel::Main => self.snap.main_db,
            DbSel::Named(dbi) => {
                // The resolution is constant for this txn's life (see the
                // `named_dense` field docs).
                if let Some(entry) = self.dense_entry(dbi) {
                    match entry.rec.get() {
                        Some(&(g, rec)) if g == generation => return rec,
                        // Another generation of the slot: rare (a remove and
                        // a reuse during this txn), resolve uncached.
                        Some(_) => return self.resolve_named(dbi),
                        None => {}
                    }
                    let rec = self.resolve_named(dbi);
                    // A racing thread may have filled the slot first; it
                    // resolved the same constant record, so either wins.
                    let _ = entry.rec.set((generation, rec));
                    return rec;
                }
                let mut memo = self.named_memo.lock().expect("named memo poisoned");
                if let Some(&(_, _, rec)) =
                    memo.iter().find(|&&(d, g, _)| d == dbi && g == generation)
                {
                    return rec;
                }
                let rec = self.resolve_named(dbi);
                memo.push((dbi, generation, rec));
                rec
            }
        }
    }
    fn handle_state(&self) -> (&HandleTable, &TxnHandles, Option<&TxnHandles>) {
        (self.env_ref().inner().handles(), &self.handles, None)
    }
    #[inline]
    fn check_db(&self, db: &Database) -> Result<()> {
        let DbSel::Named(slot) = db.sel else {
            return Ok(());
        };
        let table = self.env_ref().inner().handles();
        match self.dense_entry(slot) {
            Some(e) => table.check_with(&self.handles, slot, db.generation, &e.checked),
            None => table.check(&self.handles, None, slot, db.generation),
        }
    }
    fn comparator_for(&self, sel: DbSel) -> KeyCmp<'_> {
        self.env_ref().inner().comparator_for(sel)
    }
    fn validated_pages(&self) -> Option<&ValidatedPages<'_>> {
        Some(&self.validated)
    }
}

impl Env {
    /// Open a read transaction over the live published snapshot (SPEC 00
    /// row 13, SPEC 04 TXN-10/23): claim a reader-table slot, pin the
    /// snapshot with the SeqCst publish-and-verify protocol (TXN-17), and
    /// clone the published roots. The pin guarantees GC never reclaims a page
    /// this reader can reach (TXN-20/21); the slot is released at drop.
    ///
    /// # Errors
    ///
    /// [`MdbError::ReadersFull`] when every reader-table slot is occupied
    /// (TXN-16; the table holds `max_readers` slots, default 126). In a
    /// lock-file environment, [`MdbError::MapResized`] when another process
    /// committed beyond this process's logical map size (ADR-0023).
    pub fn read_txn(&self) -> Result<RoTxn<'_>> {
        let (snap, slot) = self.inner().pin_reader()?;
        Ok(RoTxn {
            psize: self.page_size(),
            snap,
            slot,
            env: EnvHandle::Borrowed(self),
            named_dense: OnceLock::new(),
            named_memo: Mutex::new(Vec::new()),
            handles: self.inner().handles().begin(),
            // ADR-0018: borrows the env's cache; no refcount per txn.
            validated: ValidatedPages::for_reader(
                self.inner().file_trust(),
                Some(self.inner().stamp_cache()),
            ),
        })
    }

    /// Open a `'static`, env-owning read transaction (SPEC 00 row 15, SPEC 04
    /// TXN-24; heed's `Env::static_read_txn`): consumes this handle (clone the
    /// `Env` first to keep one), pins a slot exactly like [`Env::read_txn`],
    /// and owns the env for the txn's life — keeping the env open (blocking
    /// [`Env::prepare_for_closing`]'s event, TXN-52) until the txn drops.
    /// `Send`, for handing to async handlers.
    ///
    /// # Errors
    ///
    /// As [`Env::read_txn`].
    pub fn static_read_txn(self) -> Result<RoTxn<'static>> {
        let (snap, slot) = self.inner().pin_reader()?;
        // ADR-0018: an env-owning txn cannot borrow the env's cache and does
        // not use it (Meilisearch opens these only off its search path).
        let validated = ValidatedPages::for_policy(self.inner().file_trust());
        Ok(RoTxn {
            psize: self.page_size(),
            snap,
            slot,
            handles: self.inner().handles().begin(),
            env: EnvHandle::Owned(self),
            named_dense: OnceLock::new(),
            named_memo: Mutex::new(Vec::new()),
            validated,
        })
    }

    /// Set (`Some`) or clear (`None`, follow the env default) `db`'s
    /// sequential-writes override (**ADR-0015**; ZeroDB extension). With the
    /// setting on, a write txn remembers the path to the tree's rightmost
    /// leaf and appends there without descending when a key sorts after
    /// every key already in it: faster ascending and APPEND loads, slightly
    /// slower random-key writes. Results are identical either way. Runtime
    /// state, not persisted; write txns that start after the call use it.
    pub fn set_sequential_writes(&self, db: &Database, on: Option<bool>) {
        let dbi = match db.sel() {
            DbSel::Main => None,
            DbSel::Named(dbi) => Some(dbi),
        };
        self.inner().set_sequential_writes(dbi, on);
    }

    /// Whether write txns use the sequential-writes fast path for `db`: its
    /// override if set, else the env default (ADR-0015).
    #[must_use]
    pub fn sequential_writes(&self, db: &Database) -> bool {
        let dbi = match db.sel() {
            DbSel::Main => None,
            DbSel::Named(dbi) => Some(dbi),
        };
        self.inner().sequential_writes_for(dbi)
    }

    /// A handle to the main (unnamed) database (SPEC 00 row 10, `None` name).
    /// Always present — it is the meta's `main_db`.
    #[must_use]
    pub fn main_database(&self) -> Database {
        Database::main()
    }

    /// Open `name`'s slot for `txn` (ADR-0024) and make its handle. A slot
    /// that changes name drops the previous database's sequential-writes
    /// override, which is per slot.
    ///
    /// # Errors
    ///
    /// [`MdbError::DbsFull`] when every slot is in use.
    pub(crate) fn open_slot(&self, txn: &TxnHandles, name: &[u8]) -> Result<Database> {
        let inner = self.inner();
        let (slot, generation, renamed) = inner
            .handles()
            .open(txn, name, |i| inner.slot_reusable(i))
            .ok_or(Error::Mdb(MdbError::DbsFull))?;
        if renamed {
            inner.set_sequential_writes(Some(slot), None);
        }
        Ok(Database::named(slot, generation))
    }

    /// `open_database(txn, name)` (SPEC 00 rows 10/12, `mdb_dbi_open` without
    /// `MDB_CREATE`): open an **existing** database by name. `None` name → the
    /// main DB (always present). `Some(name)` resolves the name in the txn's
    /// catalog view (SPEC 04 TXN-10 step 3): present as an `F_SUBDATA` sub-DB
    /// record → `Some(handle)`; absent → `Ok(None)`. Works over any readable
    /// txn ([`TxnRead`]): a `RoTxn` (committed catalog) or an `RwTxn` (its
    /// working catalog, so a database created earlier in the same write txn is
    /// visible).
    ///
    /// # Errors
    ///
    /// - [`MdbError::BadValSize`] if `name` is empty or `> MAX_DB_NAME`.
    /// - [`MdbError::Incompatible`] if the name exists in the main tree as a
    ///   plain **user key** (not a sub-DB record) — SPEC 02 §6.
    pub fn open_database<T: TxnRead>(
        &self,
        txn: &T,
        name: Option<&[u8]>,
    ) -> Result<Option<Database>> {
        let name = match name {
            None => return Ok(Some(self.main_database())),
            Some(n) => n,
        };
        if name.is_empty() || name.len() > crate::page::MAX_DB_NAME {
            return Err(Error::Mdb(MdbError::BadValSize));
        }
        let main = txn.main_record();
        let tree = Tree::new(txn.source(), txn.page_size(), main.root, main.depth);
        match tree.get_catalog_entry(name).map_err(map_page_err)? {
            Some((flags, _val)) if flags & F_SUBDATA != 0 => {
                let (_, handles, _) = txn.handle_state();
                Ok(Some(self.open_slot(handles, name)?))
            }
            // Present but a plain user key (no F_SUBDATA): a name collision.
            Some(_) => Err(Error::Mdb(MdbError::Incompatible)),
            None => Ok(None),
        }
    }

    /// `open_database` with a **custom key comparator**
    /// (`mdb_dbi_open` + `mdb_set_compare`). See
    /// [`Env::create_database_with_comparator`] for the full contract — in
    /// particular that the comparator is **not stored in the file**, so this
    /// call is where you take responsibility for passing the same ordering the
    /// database was built under. Passing a different one is undetectable and
    /// silently returns wrong results (see docs/DIVERGENCES.md, comparator
    /// persistence).
    ///
    /// The comparator is registered even though the database already exists:
    /// registration is an environment-level fact about a dbi, not a
    /// creation-time one.
    ///
    /// # Errors
    ///
    /// As [`Env::open_database`], plus `Io(InvalidInput)` for a `None` name
    /// (the main DB is always memcmp) or an in-process comparator conflict.
    pub fn open_database_with_comparator<T: TxnRead>(
        &self,
        txn: &T,
        name: Option<&[u8]>,
        cmp: Box<dyn crate::cmp::Comparator>,
    ) -> Result<Option<Database>> {
        let Some(db) = self.open_database(txn, name)? else {
            return Ok(None);
        };
        self.register_comparator_on(db, cmp)?;
        Ok(Some(db))
    }

    /// `non_free_pages_size()` (SPEC 00 row 19 — MUST; SPEC 05 GC-23/GC-24):
    /// the bytes held by the env's databases — the main
    /// DB's branch, leaf and overflow pages plus those of every named DB,
    /// times the page size — under a fresh read snapshot. This is heed's
    /// definition (heed 0.22.1 `Env::non_free_pages_size` sums `mdb_stat`
    /// over the unnamed DB and every named DB), computed from the records
    /// the catalog already keeps: one pass over the main DB's entries, no
    /// free-list walk. It does not count the two meta pages or the GC tree's
    /// own pages, and it does not depend on the file length (which under
    /// `WRITE_MAP` is the whole map). Meilisearch reads it on every task
    /// registration and after every indexing batch.
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt main tree or catalog record.
    pub fn non_free_pages_size(&self) -> Result<u64> {
        let rtxn = self.read_txn()?;
        let pages = |r: &DBRecord| r.branch_pages + r.leaf_pages + r.overflow_pages;
        let mut total = pages(rtxn.main_record());
        let main = *rtxn.main_record();
        let tree = Tree::new(rtxn.source(), rtxn.page_size(), main.root, main.depth);
        let mut cursor = tree.cursor();
        let mut entry = cursor.first().map_err(map_page_err)?;
        while let Some((_key, val)) = entry {
            let flags = cursor.current_flags().map_err(map_page_err)?.unwrap_or(0);
            if flags & F_SUBDATA != 0 {
                let rec = DBRecord::from_bytes(val).ok_or(Error::Mdb(MdbError::Invalid))?;
                total += pages(&rec);
            }
            entry = cursor.next().map_err(map_page_err)?;
        }
        Ok(total * u64::from(self.page_size()))
    }
}

/// Sum of every GC entry's PIL `count` prefix reachable from the txn's
/// `free_db` root (SPEC 05 GC-23). Overflow-spilled PILs are read through
/// their run like any large value (the cursor already resolves them).
///
/// # Errors
///
/// [`MdbError::Invalid`] on a corrupt GC tree or a malformed PIL.
pub fn free_page_count<T: TxnRead>(txn: &T) -> Result<u64> {
    let rec = txn.free_record();
    let source = txn.source();
    let tree = Tree::new(source, txn.page_size(), rec.root, rec.depth);
    let mut cursor = tree.cursor();
    // Format v2 (SPEC 05 §2a/GC-23): the snapshot's meta annex ids are free
    // pages too; the snapshot carries their count, so no meta re-read.
    let mut total = txn.free_annex_count();
    let mut entry = cursor.first().map_err(map_page_err)?;
    while let Some((_key, val)) = entry {
        // GC-23: sum only each PIL's `count` prefix — no `Vec<u64>` decode and
        // no id-range walk. The length/count shape check still fires (a torn
        // PIL errors with `MdbError::Invalid` rather than mis-counting —
        // INV-26's length half); the ids' range and order (INV-25/GC-4) are
        // validated where they are drawn for reuse (`gc_reclaim`), not here.
        let count = crate::page::geometry::pil_count(val).ok_or(Error::Mdb(MdbError::Invalid))?;
        total += count;
        entry = cursor.next().map_err(map_page_err)?;
    }
    Ok(total)
}

/// One owned, flagged entry: `(key, leaf-node flags, value)` (tools and copy).
pub type FlaggedEntry = (Vec<u8>, u16, Vec<u8>);

/// Collect every entry of the database `db` addresses, in key order, as owned
/// bytes together with each entry's leaf **node flags** (tools and copy).
///
/// The flags let a caller separate `F_SUBDATA` named-DB catalog records (which
/// live inline on the **main** tree, SPEC 02 §6) from plain user data:
/// `dump`/`copy_to_file` dump only the user entries of the main DB and follow
/// the catalog entries into their own sub-DB sections. For any non-main DB (and
/// for user keys) the flags are `0`.
///
/// Owned (not zero-copy) because the tool then re-encodes the bytes and the
/// values may span overflow runs — a `Vec<u8>` per value is the natural shape.
///
/// # Errors
///
/// [`MdbError::Invalid`] on a structurally-corrupt tree.
pub fn collect_entries_flagged<T: TxnRead>(db: &Database, txn: &T) -> Result<Vec<FlaggedEntry>> {
    let tree = db.tree(txn);
    let mut c = tree.cursor();
    let mut out = Vec::new();
    let mut e = c.first().map_err(map_page_err)?;
    while let Some((k, v)) = e {
        let flags = c.current_flags().map_err(map_page_err)?.unwrap_or(0);
        out.push((k.to_vec(), flags, v.to_vec()));
        e = c.next().map_err(map_page_err)?;
    }
    Ok(out)
}

/// Walk `db`'s entries in key order, handing each `(key, node_flags, value)`
/// to `f` as **borrows** of the snapshot — the streaming-compaction feed
/// (bounded-memory compaction; the owned sibling is [`collect_entries_flagged`]). Errors
/// share [`StreamBuildError`] with the streaming builder so a
/// [`crate::builder::TreeStream::push`] call inside `f` needs no conversion;
/// walk-side page errors surface as `StreamBuildError::Page`.
///
/// # Errors
///
/// `StreamBuildError::Page` on a structurally-corrupt tree, or whatever `f`
/// returns.
pub fn for_each_entry_flagged<T: TxnRead>(
    db: &Database,
    txn: &T,
    mut f: impl FnMut(&[u8], u16, &[u8]) -> std::result::Result<(), StreamBuildError>,
) -> std::result::Result<(), StreamBuildError> {
    let tree = db.tree(txn);
    let mut c = tree.cursor();
    let mut e = c.first().map_err(StreamBuildError::Page)?;
    while let Some((k, v)) = e {
        let flags = c
            .current_flags()
            .map_err(StreamBuildError::Page)?
            .unwrap_or(0);
        f(k, flags, v)?;
        e = c.next().map_err(StreamBuildError::Page)?;
    }
    Ok(())
}

/// The names of every named database, in key (name) order (tools and copy):
/// the `F_SUBDATA` catalog entries on the main tree (SPEC 02 §6). An env with
/// no named DBs yields an empty list. Works over any readable txn: a committed
/// `RoTxn`, or an `RwTxn`'s working catalog view.
///
/// # Errors
///
/// [`MdbError::Invalid`] on a structurally-corrupt main tree.
pub fn named_databases<T: TxnRead>(txn: &T) -> Result<Vec<Vec<u8>>> {
    let main = Database::main();
    Ok(collect_entries_flagged(&main, txn)?
        .into_iter()
        .filter(|(_, flags, _)| flags & F_SUBDATA != 0)
        .map(|(k, _, _)| k)
        .collect())
}

/// A database handle: the main/unnamed DB or a named DB (a [`DbSel`]). Its
/// root/stats come from the transaction passed to each method (resolved lazily
/// from the catalog for named DBs, SPEC 04 TXN-10). `Copy`, like
/// `heed::Database`.
///
/// **Key-size validation is intentionally lenient on the read side.** Read
/// methods are pure tree searches: an empty or oversized key is not rejected,
/// it simply matches nothing. LMDB's heed-observed read-key error taxonomy
/// (SPEC 03 §2.1) is a heed-API-boundary concern applied by the caller (the
/// oracle adapter and `heed-zerodb`). **Write** methods do
/// validate (SPEC 03 §6: `put*` rejects an empty or `> 511`-byte key and an
/// oversized value with `BadValSize`) — the split machinery's termination
/// proof relies on the key bound.
#[derive(Debug, Clone, Copy)]
pub struct Database {
    sel: DbSel,
    /// The slot's generation when this handle was made (ADR-0024); 0 for the
    /// main database.
    generation: u32,
}

/// Per-database statistics (`Database::stat`, SPEC 00 row 49; `mdb_stat`).
/// Mirrors `heed::DatabaseStat`. Page counts are ZeroDB-format specific;
/// `entries` and `depth` carry the same meaning as LMDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseStat {
    /// Tree height (0 = empty, 1 = root-is-leaf).
    pub depth: u16,
    /// Number of branch (internal) pages.
    pub branch_pages: u64,
    /// Number of leaf pages.
    pub leaf_pages: u64,
    /// Number of overflow pages (sum of all runs).
    pub overflow_pages: u64,
    /// Number of key/value entries.
    pub entries: u64,
}

/// The main DB's tree view over any readable txn (a test helper; the sel-aware
/// form is [`Database::tree`]).
#[cfg(test)]
pub(crate) fn tree_of<T: TxnRead + ?Sized>(txn: &T) -> Tree<'_> {
    let rec = txn.main_record();
    Tree::new(txn.source(), txn.page_size(), rec.root, rec.depth)
}

impl Database {
    /// The main database's handle.
    pub(crate) fn main() -> Database {
        Database {
            sel: DbSel::Main,
            generation: 0,
        }
    }

    /// A named database's handle: its slot and the slot's generation.
    pub(crate) fn named(slot: u32, generation: u32) -> Database {
        Database {
            sel: DbSel::Named(slot),
            generation,
        }
    }

    /// The slot's generation this handle carries (ADR-0024).
    pub(crate) fn generation(&self) -> u32 {
        self.generation
    }

    /// This handle's selector (used by the write path to pick the target tree).
    pub(crate) fn sel(&self) -> DbSel {
        self.sel
    }

    /// This database's tree in `txn`, after checking the handle there
    /// (ADR-0024). Every read entry point goes through it.
    ///
    /// # Errors
    ///
    /// As [`TxnRead::check_db`].
    #[inline]
    pub(crate) fn checked_tree<'txn, T: TxnRead + ?Sized>(
        &self,
        txn: &'txn T,
    ) -> Result<Tree<'txn>> {
        txn.check_db(self)?;
        Ok(self.tree(txn))
    }

    /// This database's tree in `txn`, unchecked: for callers that checked the
    /// handle already.
    pub(crate) fn tree<'txn, T: TxnRead + ?Sized>(&self, txn: &'txn T) -> Tree<'txn> {
        let rec = txn.record_for(self);
        // The tree carries its database's ordering, so every descent,
        // seek, range bound and hit-test below uses it (SPEC 03 §2.0).
        Tree::with_comparator(
            txn.source(),
            txn.page_size(),
            rec.root,
            rec.depth,
            txn.comparator_for(self.sel),
        )
        .with_validation_memo(txn.validated_pages())
    }

    /// `stat(txn)` (SPEC 00 row 49): depth, page counts, and entry count of
    /// this database, read from its `DBRecord` (maintained by the write path
    /// and verified against a full walk by [`crate::check`]).
    ///
    /// # Errors
    ///
    /// Infallible; returns [`Result`] for API shape and future fallibility.
    pub fn stat<T: TxnRead>(&self, txn: &T) -> Result<DatabaseStat> {
        txn.check_db(self)?;
        let rec = txn.record_for(self);
        Ok(DatabaseStat {
            depth: rec.depth,
            branch_pages: rec.branch_pages,
            leaf_pages: rec.leaf_pages,
            overflow_pages: rec.overflow_pages,
            entries: rec.entries,
        })
    }

    /// `get(txn, key)` (SPEC 00 row 30): the value for `key`, or `Ok(None)` if
    /// absent — never an error for a missing key.
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] only on a structurally-corrupt tree.
    pub fn get<'txn, T: TxnRead>(&self, txn: &'txn T, key: &[u8]) -> Result<Option<&'txn [u8]>> {
        self.checked_tree(txn)?.get(key).map_err(map_page_err)
    }

    /// `len(txn)` — entry count (SPEC 00 row 39), read from the snapshot's
    /// `DBRecord`.
    ///
    /// # Errors
    ///
    /// Infallible; returns [`Result`] for API shape.
    pub fn len<T: TxnRead>(&self, txn: &T) -> Result<u64> {
        txn.check_db(self)?;
        Ok(txn.record_for(self).entries)
    }

    /// `is_empty(txn)` (SPEC 00 row 40).
    ///
    /// # Errors
    ///
    /// Infallible; returns [`Result`] for API shape.
    pub fn is_empty<T: TxnRead>(&self, txn: &T) -> Result<bool> {
        txn.check_db(self)?;
        Ok(txn.record_for(self).entries == 0)
    }

    /// `first(txn)` — the minimum entry (SPEC 00 row 41).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn first<'txn, T: TxnRead>(
        &self,
        txn: &'txn T,
    ) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        self.checked_tree(txn)?
            .cursor()
            .first()
            .map_err(map_page_err)
    }

    /// `last(txn)` — the maximum entry (SPEC 00 row 42).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn last<'txn, T: TxnRead>(&self, txn: &'txn T) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        self.checked_tree(txn)?
            .cursor()
            .last()
            .map_err(map_page_err)
    }

    /// `get_greater_than_or_equal_to(txn, key)` — first entry `≥ key`
    /// (SPEC 00 SHOULD; the `set_range` primitive).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn get_greater_than_or_equal_to<'txn, T: TxnRead>(
        &self,
        txn: &'txn T,
        key: &[u8],
    ) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        self.checked_tree(txn)?
            .cursor()
            .set_range(key)
            .map_err(map_page_err)
    }

    /// `get_greater_than(txn, key)` — first entry `> key` (SPEC 00 row 47).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn get_greater_than<'txn, T: TxnRead>(
        &self,
        txn: &'txn T,
        key: &[u8],
    ) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        self.checked_tree(txn)?
            .cursor()
            .get_greater_than(key)
            .map_err(map_page_err)
    }

    /// `get_lower_than_or_equal_to(txn, key)` — last entry `≤ key`
    /// (SPEC 00 row 48).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn get_lower_than_or_equal_to<'txn, T: TxnRead>(
        &self,
        txn: &'txn T,
        key: &[u8],
    ) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        self.checked_tree(txn)?
            .cursor()
            .get_lower_than_or_equal_to(key)
            .map_err(map_page_err)
    }

    /// `get_lower_than(txn, key)` — last entry `< key` (SPEC 00 SHOULD).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn get_lower_than<'txn, T: TxnRead>(
        &self,
        txn: &'txn T,
        key: &[u8],
    ) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        self.checked_tree(txn)?
            .cursor()
            .get_lower_than(key)
            .map_err(map_page_err)
    }

    /// `iter(txn)` — forward full scan (SPEC 00 row 43).
    #[must_use]
    pub fn iter<'txn, T: TxnRead>(&self, txn: &'txn T) -> RoRange<'txn> {
        self.range_impl(txn, Dir::Fwd, Bound::Unbounded, Bound::Unbounded)
    }

    /// `rev_iter(txn)` — reverse full scan (SPEC 00 SHOULD).
    #[must_use]
    pub fn rev_iter<'txn, T: TxnRead>(&self, txn: &'txn T) -> RoRange<'txn> {
        self.range_impl(txn, Dir::Rev, Bound::Unbounded, Bound::Unbounded)
    }

    /// `range(txn, lower, upper)` — forward scan over the bound pair
    /// (SPEC 00 row 44). Any `Bound` combination is supported.
    #[must_use]
    pub fn range<'txn, T: TxnRead>(
        &self,
        txn: &'txn T,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> RoRange<'txn> {
        self.range_impl(txn, Dir::Fwd, own(lower), own(upper))
    }

    /// `rev_range(txn, lower, upper)` — reverse scan over the bound pair
    /// (SPEC 00 row 44).
    #[must_use]
    pub fn rev_range<'txn, T: TxnRead>(
        &self,
        txn: &'txn T,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> RoRange<'txn> {
        self.range_impl(txn, Dir::Rev, own(lower), own(upper))
    }

    /// `prefix_iter(txn, prefix)` — forward scan of all keys with `prefix`
    /// (SPEC 00 row 45), realized as the range `[prefix, prefix_successor)`
    /// (SPEC 03 §4; the all-`0xFF` edge yields an unbounded upper).
    #[must_use]
    pub fn prefix_iter<'txn, T: TxnRead>(&self, txn: &'txn T, prefix: &[u8]) -> RoRange<'txn> {
        let (lo, hi) = prefix_bounds(prefix);
        self.range_impl(txn, Dir::Fwd, lo, hi)
    }

    /// `rev_prefix_iter(txn, prefix)` — reverse prefix scan (SPEC 00 row 46).
    #[must_use]
    pub fn rev_prefix_iter<'txn, T: TxnRead>(&self, txn: &'txn T, prefix: &[u8]) -> RoRange<'txn> {
        let (lo, hi) = prefix_bounds(prefix);
        self.range_impl(txn, Dir::Rev, lo, hi)
    }

    fn range_impl<'txn, T: TxnRead>(
        &self,
        txn: &'txn T,
        dir: Dir,
        lo: Bound<Vec<u8>>,
        hi: Bound<Vec<u8>>,
    ) -> RoRange<'txn> {
        // A handle that is not valid here (ADR-0024) yields its error as the
        // range's first item; heed-zerodb checks first, so heed's callers
        // get it from `iter`/`range` themselves, as from LMDB.
        let failed = txn.check_db(self).err();
        let tree = self.tree(txn);
        RoRange {
            cmp: tree.comparator(),
            cursor: tree.cursor(),
            dir,
            lo,
            hi,
            started: false,
            done: false,
            failed,
        }
    }
}

fn own(b: Bound<&[u8]>) -> Bound<Vec<u8>> {
    match b {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(s) => Bound::Included(s.to_vec()),
        Bound::Excluded(s) => Bound::Excluded(s.to_vec()),
    }
}

/// The `[prefix, prefix_successor(prefix))` bounds for a prefix scan (SPEC 03
/// §4). An all-`0xFF` (or empty) prefix has no successor → unbounded upper.
fn prefix_bounds(prefix: &[u8]) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let lo = Bound::Included(prefix.to_vec());
    let hi = match prefix_successor(prefix) {
        Some(s) => Bound::Excluded(s),
        None => Bound::Unbounded,
    };
    (lo, hi)
}

#[derive(Debug, Clone, Copy)]
enum Dir {
    Fwd,
    Rev,
}

/// A lazy, zero-copy range/prefix iterator (SPEC 00 rows 43–46). Yields
/// `Result<(&'txn [u8], &'txn [u8])>`; the borrowed slices live for the txn
/// borrow, not for the `&mut` `next` call, so they can outlive iteration.
/// Works over both txn kinds ([`TxnRead`]); on a write txn the shared `&RwTxn`
/// borrow it holds forbids any mutation while it is alive (SPEC 04 TXN-39).
pub struct RoRange<'txn> {
    cursor: Cursor<'txn>,
    /// The database's ordering, copied from the tree so the
    /// termination test below agrees with the seek the cursor performed. A
    /// memcmp bound test over a comparator-ordered cursor would stop the scan
    /// at an arbitrary point.
    cmp: KeyCmp<'txn>,
    dir: Dir,
    lo: Bound<Vec<u8>>,
    hi: Bound<Vec<u8>>,
    started: bool,
    done: bool,
    /// The handle check's error, yielded as the first item (ADR-0024).
    failed: Option<Error>,
}

impl<'txn> RoRange<'txn> {
    fn seek_start(&mut self) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let r = match self.dir {
            Dir::Fwd => match &self.lo {
                Bound::Unbounded => self.cursor.first(),
                Bound::Included(l) => self.cursor.set_range(l),
                Bound::Excluded(l) => self.cursor.get_greater_than(l),
            },
            Dir::Rev => match &self.hi {
                Bound::Unbounded => self.cursor.last(),
                Bound::Included(h) => self.cursor.get_lower_than_or_equal_to(h),
                Bound::Excluded(h) => self.cursor.get_lower_than(h),
            },
        };
        r.map_err(map_page_err)
    }

    /// Whether `key` is within the bound that *terminates* iteration in this
    /// direction (the upper bound going forward, the lower bound going reverse).
    ///
    /// `SKIP` only separates the copy `nth` uses (`true`) from the one `next`
    /// uses (`false`). Each copy then has a single caller, which is what lets
    /// LLVM inline this, key compare included, into `next`; with `nth` as a
    /// second caller of one shared copy it stopped doing so and every plain
    /// scan paid a call per entry (scan/full measured 1.4x slower).
    fn in_bounds<const SKIP: bool>(&self, key: &[u8]) -> bool {
        match self.dir {
            Dir::Fwd => match &self.hi {
                Bound::Unbounded => true,
                Bound::Included(h) => self.cmp.compare(key, h) != Ordering::Greater,
                Bound::Excluded(h) => self.cmp.compare(key, h) == Ordering::Less,
            },
            Dir::Rev => match &self.lo {
                Bound::Unbounded => true,
                Bound::Included(l) => self.cmp.compare(key, l) != Ordering::Less,
                Bound::Excluded(l) => self.cmp.compare(key, l) == Ordering::Greater,
            },
        }
    }
}

impl<'txn> Iterator for RoRange<'txn> {
    type Item = Result<(&'txn [u8], &'txn [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if let Some(e) = self.failed.take() {
            self.done = true;
            return Some(Err(e));
        }
        let step = if !self.started {
            self.started = true;
            self.seek_start()
        } else {
            match self.dir {
                Dir::Fwd => self.cursor.next().map_err(map_page_err),
                Dir::Rev => self.cursor.prev().map_err(map_page_err),
            }
        };
        match step {
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
            Ok(None) => {
                self.done = true;
                None
            }
            Ok(Some((k, v))) => {
                if self.in_bounds::<false>(k) {
                    Some(Ok((k, v)))
                } else {
                    self.done = true;
                    None
                }
            }
        }
    }

    /// Skip `n` entries leaf by leaf ([`Cursor::advance_by`] /
    /// [`Cursor::retreat_by`]) instead of stepping, bound-testing and
    /// resolving each one — what `Iterator::skip` calls. The result equals
    /// `n` calls to `next` followed by one more: keys are sorted, so the
    /// landing entry being in bounds means every skipped one was. The one
    /// difference is under corruption (SPEC 03 §4): a skipped entry's value is
    /// never read, and a page error met while crossing leaves is yielded
    /// rather than consumed as a skipped item.
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if n == 0 {
            return self.next();
        }
        if self.done {
            return None;
        }
        // The first item is the seek (or the handle check's error): it counts
        // as one skipped entry and leaves the cursor on a yielded entry.
        if !self.started || self.failed.is_some() {
            let _ = self.next()?;
            return self.nth(n - 1);
        }
        // The walk moves a copy of the cursor so the bound test can borrow
        // `self`; the copy is written back below. (Splitting the bound test
        // out of `self` instead changed how LLVM inlines `next`, costing every
        // plain scan.)
        let mut cursor = self.cursor.clone();
        let past_end = |k: &[u8]| !self.in_bounds::<true>(k);
        // The cursor sits on the last yielded entry, so the `n`th next one is
        // `n + 1` steps away.
        let steps = n.saturating_add(1);
        let step = match self.dir {
            Dir::Fwd => cursor.advance_by(steps, past_end),
            Dir::Rev => cursor.retreat_by(steps, past_end),
        };
        self.cursor = cursor;
        match step {
            Ok(Some((k, v))) if self.in_bounds::<true>(k) => Some(Ok((k, v))),
            Ok(_) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(map_page_err(e)))
            }
        }
    }
}

#[cfg(test)]
mod send_assertions {
    use super::RoTxn;

    fn assert_send<T: Send>() {}

    /// SPEC 04 TXN-13 (WithoutTls): a reader-table slot is owned by the txn
    /// object, not a thread, so `RoTxn` — both the borrowed and the `'static`
    /// env-owning shape — is `Send`. Compile-time assertion; a regression
    /// (e.g. a non-`Send` field sneaking into `RoTxn`) fails to build.
    #[test]
    fn rotxn_is_send_in_both_shapes() {
        assert_send::<RoTxn<'_>>();
        assert_send::<RoTxn<'static>>();
    }
}
