//! Read transactions and the heed-shaped read API over the [`btree`] cursor
//! (SPEC 04 §3, SPEC 00 read-op rows). Milestones 1.3/1.4.
//!
//! A [`RoTxn`] pins the env's **published snapshot** at open: it claims a
//! reader-table slot, runs the SeqCst publish-and-verify pin (SPEC 04
//! TXN-10/15/17; M1.8, ADR-0006), and `Arc`-clones the current [`Snapshot`]
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
//! Named databases (M1.6): a [`Database`] handle carries a [`DbSel`] — the
//! main/unnamed DB or a named DB addressed by its env-level *dbi index*
//! ([`Env::open_database`] / [`Env::create_database`]). A named DB's
//! `DBRecord` (root/depth/stats) is **not** stored in the [`RoTxn`] snapshot;
//! it resolves lazily from the transaction's catalog view — the main tree,
//! keyed by name, value = 48-byte `F_SUBDATA` record (SPEC 02 §6, SPEC 04
//! TXN-10 step 3). This module owns that resolution ([`resolve_named_record`])
//! and the read API; the write side (create/clear/drop, catalog write-back)
//! lives in [`crate::rwtxn`].

use std::ops::Bound;
use std::sync::{Arc, Mutex};

use crate::btree::{prefix_successor, Cursor, Source, Tree, ValidatedPages};
use crate::builder::StreamBuildError;
use crate::cmp::KeyCmp;
use crate::env::{Env, Snapshot};
use crate::error::{Error, MdbError, Result};
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
    /// `Incompatible` check lives in `open`/`create`).
    fn record_for(&self, sel: DbSel) -> DBRecord;
    /// The key ordering in force for the database `sel` addresses (**M2.4**,
    /// SPEC 03 §2.0). [`DbSel::Main`] is always memcmp — it is the named-DB
    /// catalog. Every tree this module builds for a *user* database routes
    /// through here.
    fn comparator_for(&self, sel: DbSel) -> KeyCmp<'_>;
    /// The txn's validated-pages memo, if it keeps one (PERF-GAP A2): pages
    /// whose cells were fully validated earlier this txn and may be re-wrapped
    /// without the O(`num_keys`) cell walk. Default `None` = always fully
    /// validate.
    fn validated_pages(&self) -> Option<&ValidatedPages> {
        None
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
/// (SPEC 04 §3). It holds the [`Env`] (borrowed or owned, [`EnvHandle`]),
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
/// failure mode as a stale LMDB reader, minus the cross-process reap (under
/// D-001 there is nothing to reap — the owner provably is this process).
/// Reader introspection is Phase 2.2; no reaping path exists.
pub struct RoTxn<'env> {
    env: EnvHandle<'env>,
    psize: u32,
    snap: Arc<Snapshot>,
    /// The owned reader-table slot (claimed in `EnvInner::pin_reader`).
    slot: u32,
    /// Per-txn memo of resolved named-DB records, keyed by dbi.
    ///
    /// Without it every read op on a named DB re-did the registry lock + name
    /// clone + a full catalog descent (`resolve_named_record`) — roughly
    /// doubling the tree work per `get` (docs/PERF-GAP-VS-LMDB.md A1).
    ///
    /// Soundness: this txn pins an immutable [`Snapshot`] (its catalog cannot
    /// change while pinned, TXN-18/20) and the dbi→name registry is
    /// append-only for the process (M1.6), so a (dbi → record) resolution is
    /// constant for the txn's life. A linear `Vec` scan beats a map: the set
    /// is bounded by `max_dbs` and typically small. `Mutex` (not `RefCell`)
    /// keeps `RoTxn` auto-`Sync`; the lock is uncontended and held only for
    /// the lookup/insert.
    named_memo: Mutex<Vec<(u32, DBRecord)>>,
    /// Pages fully validated this txn (PERF-GAP A2; see
    /// [`ValidatedPages`]). Sound here because every page this snapshot can
    /// reach is immutable while its reader slot is held (TXN-20/21).
    validated: ValidatedPages,
}

impl RoTxn<'_> {
    /// The snapshot txnid this read txn observes (SPEC 04 TXN-4).
    #[must_use]
    pub fn txnid(&self) -> u64 {
        self.snap.txnid
    }

    /// The committed [`Snapshot`] this txn pins (roots + `last_pg`, SPEC 04
    /// TXN-18). M1.12's `Env::copy_to_file` reads `last_pg`/`main_db`/`free_db`
    /// from here to bound a raw range copy and to synthesize the copy's meta
    /// pages.
    #[must_use]
    pub fn snapshot(&self) -> &Snapshot {
        &self.snap
    }

    /// The whole mapped env file as bytes, borrowed for the txn's life
    /// (SPEC 04 TXN-37). M1.12's non-compact `copy_to_file` copies the pinned
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
        }
    }
    fn main_record(&self) -> &DBRecord {
        &self.snap.main_db
    }
    fn free_record(&self) -> &DBRecord {
        &self.snap.free_db
    }
    fn page_size(&self) -> u32 {
        self.psize
    }
    fn record_for(&self, sel: DbSel) -> DBRecord {
        match sel {
            DbSel::Main => self.snap.main_db,
            DbSel::Named(dbi) => {
                // Memo hit: the resolution is constant for this txn's life
                // (see the `named_memo` field docs).
                let mut memo = self.named_memo.lock().expect("named memo poisoned");
                if let Some(&(_, rec)) = memo.iter().find(|&&(d, _)| d == dbi) {
                    return rec;
                }
                let rec = match self.env_ref().inner().named_name(dbi) {
                    Some(name) => {
                        resolve_named_record(self.source(), self.psize, &self.snap.main_db, &name)
                    }
                    None => DBRecord::empty(),
                };
                memo.push((dbi, rec));
                rec
            }
        }
    }
    fn comparator_for(&self, sel: DbSel) -> KeyCmp<'_> {
        self.env_ref().inner().comparator_for(sel)
    }
    fn validated_pages(&self) -> Option<&ValidatedPages> {
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
    /// (TXN-16; the table holds `max_readers` slots, default 126).
    pub fn read_txn(&self) -> Result<RoTxn<'_>> {
        let (snap, slot) = self.inner().pin_reader()?;
        Ok(RoTxn {
            psize: self.page_size(),
            snap,
            slot,
            env: EnvHandle::Borrowed(self),
            named_memo: Mutex::new(Vec::new()),
            validated: ValidatedPages::new(),
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
    /// [`MdbError::ReadersFull`] when every reader-table slot is occupied
    /// (TXN-16).
    pub fn static_read_txn(self) -> Result<RoTxn<'static>> {
        let (snap, slot) = self.inner().pin_reader()?;
        Ok(RoTxn {
            psize: self.page_size(),
            snap,
            slot,
            env: EnvHandle::Owned(self),
            named_memo: Mutex::new(Vec::new()),
            validated: ValidatedPages::new(),
        })
    }

    /// A handle to the main (unnamed) database (SPEC 00 row 10, `None` name).
    /// Always present — it is the meta's `main_db`.
    #[must_use]
    pub fn main_database(&self) -> Database {
        Database { sel: DbSel::Main }
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
                let dbi = self
                    .inner()
                    .named_dbi_assign(name)
                    .ok_or(Error::Mdb(MdbError::DbsFull))?;
                Ok(Some(Database {
                    sel: DbSel::Named(dbi),
                }))
            }
            // Present but a plain user key (no F_SUBDATA): a name collision.
            Some(_) => Err(Error::Mdb(MdbError::Incompatible)),
            None => Ok(None),
        }
    }

    /// `open_database` with a **custom key comparator** (**milestone 2.4**;
    /// `mdb_dbi_open` + `mdb_set_compare`). See
    /// [`Env::create_database_with_comparator`] for the full contract — in
    /// particular that the comparator is **not stored in the file**, so this
    /// call is where you take responsibility for passing the same ordering the
    /// database was built under. Passing a different one is undetectable and
    /// silently returns wrong results (D-014).
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
    /// `real_disk_size() − free_page_count() * psize`, where the free-page
    /// count is the exact sum of every GC entry's PIL count under a fresh read
    /// snapshot. This is the native replacement for milli reading LMDB's
    /// freelist; it drives the `> 0.75 * map_size` auto-resize trigger.
    ///
    /// **TOCTOU note (GC-24):** the free count is exact *for the snapshot*,
    /// but the `fstat` length is sampled independently and can only be
    /// **larger** (a concurrent writer may extend the file; nothing ever
    /// truncates it in Phase 1). The result may therefore over-report
    /// non-free bytes by at most the concurrent growth — monotone-conservative
    /// for milli's resize trigger (it can only fire *earlier* than the exact
    /// value would, never later), and exact whenever no writer commits during
    /// the call. GC-24's precision claim is per-snapshot and holds.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] from `fstat`; [`MdbError::Invalid`] on a corrupt GC DB.
    pub fn non_free_pages_size(&self) -> Result<u64> {
        let rtxn = self.read_txn()?;
        let free_pages = free_page_count(&rtxn)?;
        let disk = self.inner().real_disk_size()?;
        Ok(disk.saturating_sub(free_pages * u64::from(self.page_size())))
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
    let tree = Tree::new(txn.source(), txn.page_size(), rec.root, rec.depth);
    let mut cursor = tree.cursor();
    let mut total = 0u64;
    let mut entry = cursor.first().map_err(map_page_err)?;
    while let Some((_key, val)) = entry {
        // GC-3 shape validation via the shared PIL codec (a torn PIL errors
        // rather than silently mis-counting — INV-26's runtime cousin).
        let ids = crate::page::geometry::pil_decode(val).ok_or(Error::Mdb(MdbError::Invalid))?;
        total += ids.len() as u64;
        entry = cursor.next().map_err(map_page_err)?;
    }
    Ok(total)
}

/// One owned, flagged entry: `(key, leaf-node flags, value)` (M1.12 tools/copy).
pub type FlaggedEntry = (Vec<u8>, u16, Vec<u8>);

/// Collect every entry of the database `db` addresses, in key order, as owned
/// bytes together with each entry's leaf **node flags** (M1.12 tools/copy).
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
/// (PERF-GAP C1; the owned sibling is [`collect_entries_flagged`]). Errors
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

/// The names of every named database, in key (name) order (M1.12 tools/copy):
/// the `F_SUBDATA` catalog entries on the main tree (SPEC 02 §6). An env with
/// no named DBs yields an empty list. Works over any readable txn: a committed
/// `RoTxn`, or an `RwTxn`'s working catalog view.
///
/// # Errors
///
/// [`MdbError::Invalid`] on a structurally-corrupt main tree.
pub fn named_databases<T: TxnRead>(txn: &T) -> Result<Vec<Vec<u8>>> {
    let main = Database::from_sel(DbSel::Main);
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
/// oracle adapter today; `heed-zerodb` at M1.13). **Write** methods do
/// validate (SPEC 03 §6: `put*` rejects an empty or `> 511`-byte key and an
/// oversized value with `BadValSize`) — the split machinery's termination
/// proof relies on the key bound.
#[derive(Debug, Clone, Copy)]
pub struct Database {
    sel: DbSel,
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
    /// Construct a handle from a raw selector (used by the write path).
    pub(crate) fn from_sel(sel: DbSel) -> Database {
        Database { sel }
    }

    /// This handle's selector (used by the write path to pick the target tree).
    pub(crate) fn sel(&self) -> DbSel {
        self.sel
    }

    pub(crate) fn tree<'txn, T: TxnRead + ?Sized>(&self, txn: &'txn T) -> Tree<'txn> {
        let rec = txn.record_for(self.sel);
        // M2.4: the tree carries its database's ordering, so every descent,
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
        let rec = txn.record_for(self.sel);
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
        self.tree(txn).get(key).map_err(map_page_err)
    }

    /// `len(txn)` — entry count (SPEC 00 row 39), read from the snapshot's
    /// `DBRecord`.
    ///
    /// # Errors
    ///
    /// Infallible; returns [`Result`] for API shape.
    pub fn len<T: TxnRead>(&self, txn: &T) -> Result<u64> {
        Ok(txn.record_for(self.sel).entries)
    }

    /// `is_empty(txn)` (SPEC 00 row 40).
    ///
    /// # Errors
    ///
    /// Infallible; returns [`Result`] for API shape.
    pub fn is_empty<T: TxnRead>(&self, txn: &T) -> Result<bool> {
        Ok(txn.record_for(self.sel).entries == 0)
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
        self.tree(txn).cursor().first().map_err(map_page_err)
    }

    /// `last(txn)` — the maximum entry (SPEC 00 row 42).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn last<'txn, T: TxnRead>(&self, txn: &'txn T) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        self.tree(txn).cursor().last().map_err(map_page_err)
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
        self.tree(txn).cursor().set_range(key).map_err(map_page_err)
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
        self.tree(txn)
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
        self.tree(txn)
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
        self.tree(txn)
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
        let tree = self.tree(txn);
        RoRange {
            cmp: tree.comparator(),
            cursor: tree.cursor(),
            dir,
            lo,
            hi,
            started: false,
            done: false,
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
    /// The database's ordering (**M2.4**), copied from the tree so the
    /// termination test below agrees with the seek the cursor performed. A
    /// memcmp bound test over a comparator-ordered cursor would stop the scan
    /// at an arbitrary point.
    cmp: KeyCmp<'txn>,
    dir: Dir,
    lo: Bound<Vec<u8>>,
    hi: Bound<Vec<u8>>,
    started: bool,
    done: bool,
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
    fn in_bounds(&self, key: &[u8]) -> bool {
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
                if self.in_bounds(k) {
                    Some(Ok((k, v)))
                } else {
                    self.done = true;
                    None
                }
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
