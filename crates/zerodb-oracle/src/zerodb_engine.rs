//! The native [`Engine`] backed by the `zerodb` crate.
//!
//! ## Milestone scope (M1.6 — named databases and the catalog)
//!
//! Named DBs and `DropDb` are now differential (previously gated out): the
//! engine holds real [`zerodb::Database`] handles created via
//! [`zerodb::Env::create_database`], resolved from the shared `dbs` table by
//! index exactly as [`crate::LmdbEngine`] does, so the two engines exercise
//! identical multi-DB workloads. Reads/writes target the resolved handle;
//! `VerifyGet` opens the DB in a fresh read txn via
//! [`zerodb::Env::open_database`].
//!
//! ## Nested read txns (M1.9, SPEC 04 §5) — now differential
//!
//! `BeginNestedRo`/`EndNestedRo` mirror [`crate::LmdbEngine`]'s
//! `Active::RwNested`: while a nested child is open, every read is served
//! **through the child** (uncommitted state, TXN-26/27) and every write op is
//! `Skip::WriteBlockedByNested` — the shared driver classifies that skip
//! *before either engine runs*, so the fork's technical allowance of
//! writes-under-a-child (D-005: it does not enforce quiescence; zerodb does)
//! stays unobservable and the two engines cannot drift. The op model holds
//! **one** child at a time (like its one-txn limitation); multiple concurrent
//! children + real thread fan-out are covered by
//! `crates/zerodb/tests/nested_fanout.rs`.
//!
//! Unlike heed's child (a raw `MDB_txn` pointer, no Rust reference into the
//! parent), `zerodb::NestedRoTxn` holds a **real `&RwTxn`** — so the parent
//! must sit at a stable heap address while children exist. `Active::Rw`
//! already boxes the txn (ADR-0007 D5); the nested child borrows the Box's
//! target and both move together between `Active` variants.
//!
//! Earlier scope carries over: every write op runs zerodb's real COW write
//! path, `Commit` runs the real C0–C6 pipeline (now incl. the C1a catalog
//! write-back, SPEC 02 §6), and in debug builds every successful commit re-reads
//! `zerodb.dat` and runs the SPEC 03 §11 + M1.6 catalog invariant walk
//! ([`zerodb::check::check_image`]).
//!
//! ## Transaction storage (the sanctioned unsafe, as in [`crate::LmdbEngine`])
//!
//! zerodb's `RoTxn<'env>` / `RwTxn<'env>` borrow the `Env`. To hold an open
//! transaction across [`Engine::apply`] calls they live next to the `Env` in
//! one struct — a self-referential shape safe Rust cannot express. The txn
//! lifetimes are extended to `'static` with `mem::transmute`; the invariants
//! are stated at each site and mirror the `LmdbEngine` construction exactly
//! (CLAUDE.md unsafe policy: confined to this test-only oracle crate).
//!
//! **Key-size taxonomy split** (SPEC 03 §2.1): zerodb-core's *write* path
//! validates keys itself (`put*` → `BadValSize`); the *read/del* leniency
//! differences are LMDB API-boundary behaviors applied here.

use zerodb::{
    check, Database, Env, EnvFlags, EnvOpenOptions, Error, MdbError, NestedRoTxn, PutFlags, RoTxn,
    RwTxn,
};

use crate::result::{OpResult, OracleError, Skip};
use crate::tempdir::TempDir;
use crate::{DbName, Engine, EngineMode, Op, PutFlag};

/// Base map size (see [`crate::DIFF_MAP_SIZE`]); matches [`crate::LmdbEngine`].
const BASE_MAP_SIZE: usize = crate::DIFF_MAP_SIZE;
/// DB page size (Phase 1 exposes no heed selector; 4 KiB).
const PAGE_SIZE: u32 = 4096;
/// Catalog capacity: unnamed + `db0`..`db3` plus slack (matches `LmdbEngine`).
const MAX_DBS: u32 = 16;

/// The zerodb env flags for a mode (M1.10, SPEC 01 Table 1) — the durability /
/// write-mode bits mirrored from `EngineMode`.
fn env_flags(mode: EngineMode) -> EnvFlags {
    let mut f = EnvFlags::EMPTY;
    if mode.write_map {
        f |= EnvFlags::WRITE_MAP;
    }
    if mode.no_sync {
        f |= EnvFlags::NO_SYNC;
    }
    if mode.no_meta_sync {
        f |= EnvFlags::NO_META_SYNC;
    }
    if mode.map_async {
        f |= EnvFlags::MAP_ASYNC;
    }
    f
}

/// Run a read expression `$body` (with the serving txn bound to `$t`) against
/// whichever transaction serves reads, or skip. A macro (not a `&dyn` helper)
/// so the generic `Database` read methods monomorphize over the concrete
/// `&RwTxn` / `&RoTxn` types (`TxnRead` is used generically, not as an object).
macro_rules! with_read {
    ($active:expr, |$t:ident| $body:expr) => {
        match &$active {
            Active::Rw(w) => {
                let $t = &**w;
                $body
            }
            // A live nested child serves the reads (uncommitted view,
            // TXN-26/27) — mirrors `LmdbEngine::read_source`.
            Active::RwNested { nested, .. } => {
                let $t = nested;
                $body
            }
            Active::Ro(r) => {
                let $t = &**r;
                $body
            }
            Active::None => OpResult::Skipped(Skip::NoTxn),
        }
    };
}

/// The currently-open transaction, if any.
///
/// Declared **before** `env` in [`ZerodbEngine`] so transactions drop before
/// the env (field declaration order = drop order).
enum Active {
    None,
    Rw(Box<RwTxn<'static>>),
    // Boxed: `RoTxn` carries the inline lock-free validated-pages memo
    // since PERF-GAP A8 (~180 B), tripping `clippy::large_enum_variant`.
    Ro(Box<RoTxn<'static>>),
    /// A nested read child over the paused write txn (SPEC 04 §5, M1.9).
    ///
    /// Field order is load-bearing: `nested` is declared **before** `wtxn`,
    /// so the child (which borrows the boxed txn) drops before its parent —
    /// same construction as `LmdbEngine::Active::RwNested`.
    RwNested {
        nested: NestedRoTxn<'static>,
        wtxn: Box<RwTxn<'static>>,
    },
}

/// A database handle plus the catalog name it was opened under (mirrors
/// `LmdbEngine::DbEntry`).
struct DbEntry {
    name: Option<String>,
    db: Database,
    /// Whether the transaction that opened this handle has **committed**.
    ///
    /// LMDB (`lmdb.h`): "The database handle will be private to the current
    /// transaction until the transaction is successfully committed. If the
    /// transaction is aborted the handle will be closed automatically."
    /// `mdb.c`'s `mdb_dbis_update(txn, keep=0)` implements that close on the
    /// abort path. So a handle whose creating txn aborted is DEAD, and using it
    /// afterwards is an API-contract violation that LMDB reports as `EINVAL`
    /// from the `TXN_DBI_EXIST` gate in `mdb_cursor_open` / `mdb_put`.
    ///
    /// This flag replaces the old positional `committed_dbs` watermark, which
    /// was only correct while entries were append-only: `drop_db`'s
    /// `dbs.remove(idx)` removes from the middle, after which
    /// `truncate(committed_dbs)` retained the WRONG set — keeping an
    /// uncommitted (dead) handle while discarding a committed one. That made
    /// the harness drive both engines through a use-after-close and report the
    /// resulting LMDB `EINVAL` as an engine divergence.
    committed: bool,
}

/// The native engine under differential test.
///
/// Field order is load-bearing for `Drop`: `active` (transactions) before
/// `env` before `dir`.
pub struct ZerodbEngine {
    active: Active,
    /// Open databases in creation order (index = the op's `db` selector, taken
    /// modulo the length — identical addressing to `LmdbEngine`).
    dbs: Vec<DbEntry>,
    /// `Box` gives the `Env` a stable heap address that survives moves of the
    /// engine struct — what makes the `'static` txn borrows sound. Always
    /// `Some` between operations.
    env: Option<Box<Env>>,
    map_size: usize,
    /// FORK-1 guard fact (see `driver::classify`): set by `ClearDb`, reset at
    /// every txn boundary.
    cleared_in_txn: bool,
    /// The env open mode (M1.10): `WRITE_MAP` / durability flags. Preserved
    /// across `reopen` so a reopened env keeps the same write mode.
    mode: EngineMode,
    dir: TempDir,
}

impl ZerodbEngine {
    fn open(dir: &std::path::Path, map_size: usize, mode: EngineMode) -> Result<Env, Error> {
        let mut opts = EnvOpenOptions::new();
        opts.map_size(map_size);
        opts.max_dbs(MAX_DBS);
        opts.page_size(PAGE_SIZE);
        opts.flags(env_flags(mode));
        opts.open(dir)
    }

    fn env(&self) -> &Env {
        self.env.as_ref().expect("env present between ops")
    }

    fn desired_map_size(&self, kib: u16) -> usize {
        let want = BASE_MAP_SIZE + (kib as usize) * 4096;
        crate::round_map_size(want.max(self.map_size))
    }

    fn txn_state(&self) -> crate::driver::TxnState {
        use crate::driver::TxnState;
        match self.active {
            Active::None => TxnState::None,
            Active::Rw(_) => TxnState::Rw,
            Active::Ro(_) => TxnState::Ro,
            Active::RwNested { .. } => TxnState::RwNested,
        }
    }

    /// The active write txn, or the structured skip (unreachable after the
    /// driver gate; kept as a handle resolver like `LmdbEngine`).
    fn write_txn(&mut self) -> Result<&mut RwTxn<'static>, OpResult> {
        match &mut self.active {
            Active::Rw(w) => Ok(w),
            // Writer paused while a nested child lives (D-005 / TXN-29):
            // symmetric with `LmdbEngine::write_txn`.
            Active::RwNested { .. } => Err(OpResult::Skipped(Skip::WriteBlockedByNested)),
            Active::Ro(_) | Active::None => Err(OpResult::Skipped(Skip::NoWriteTxn)),
        }
    }

    /// Mark every open handle as surviving the txn boundary: a successful
    /// commit "exports" the dbis opened in this txn into the shared env
    /// (`mdb_dbis_update(txn, keep=1)`), after which they stay valid.
    fn mark_dbs_committed(&mut self) {
        for e in &mut self.dbs {
            e.committed = true;
        }
    }

    /// Drop the handles LMDB closes when a write txn ends without committing
    /// (`mdb_dbis_update(txn, keep=0)`): exactly those opened in that txn.
    /// Committed handles survive. Keeping a dead handle here would make the
    /// harness issue a use-after-close, which LMDB rejects with `EINVAL` while
    /// ZeroDB — whose handles are plain values, not env-level dbi slots —
    /// happily serves it. That is a harness defect, not an engine divergence.
    fn close_dbs_opened_in_aborted_txn(&mut self) {
        self.dbs.retain(|e| e.committed);
    }

    /// Resolve a db index (modulo the number of open dbs) to a handle.
    fn db_at(&self, db: u8) -> Result<Database, OpResult> {
        if self.dbs.is_empty() {
            return Err(OpResult::Skipped(Skip::NoDb));
        }
        Ok(self.dbs[db as usize % self.dbs.len()].db)
    }

    /// Resolve a db index to the catalog name it was opened under.
    fn db_name_at(&self, db: u8) -> Option<Option<String>> {
        if self.dbs.is_empty() {
            None
        } else {
            Some(self.dbs[db as usize % self.dbs.len()].name.clone())
        }
    }

    /// In debug builds, verify the committed on-disk image against the
    /// SPEC 03 §11 + M1.6 catalog invariant walk.
    fn debug_check_image(&self) {
        #[cfg(debug_assertions)]
        {
            let path = self.dir.path().join(zerodb::DATA_FILE_NAME);
            let bytes = std::fs::read(&path).expect("read committed image");
            let violations = check::check_image(&bytes, PAGE_SIZE);
            assert!(
                violations.is_empty(),
                "invariant violations after commit: {violations:#?}"
            );
        }
        #[cfg(not(debug_assertions))]
        {
            let _ = &check::check_image; // keep the import meaningful
        }
    }

    // -- environment ---------------------------------------------------------

    fn reopen(&mut self, kib: u16) -> OpResult {
        // Reopen drops the active txn (a txn boundary: reset the FORK-1 fact)
        // and all db handles; committed data persists in the real file now.
        self.active = Active::None;
        self.cleared_in_txn = false;
        self.dbs.clear();
        self.env = None;

        let new_size = self.desired_map_size(kib);
        match Self::open(self.dir.path(), new_size, self.mode) {
            Ok(env) => {
                self.env = Some(Box::new(env));
                self.map_size = new_size;
                OpResult::Ok
            }
            Err(e) => {
                if let Ok(env) = Self::open(self.dir.path(), self.map_size, self.mode) {
                    self.env = Some(Box::new(env));
                }
                OpResult::Err(to_oracle(e))
            }
        }
    }

    // -- transactions ---------------------------------------------------------

    fn begin_rw(&mut self) -> OpResult {
        self.cleared_in_txn = false;
        // SAFETY: extend the txn borrow to 'static. Invariants (mirrors
        // LmdbEngine::begin_rw):
        //  * the `Env` lives in a `Box` (stable address) owned by `self`, and
        //    its `EnvInner` (mutex, mmap) lives behind an `Arc` (stable); the
        //    txn's borrows (`&Env`, `MutexGuard`, mapped `&[u8]`) all target
        //    those stable allocations;
        //  * the txn is a *prior* field of `self`, so it drops before the env,
        //    and is cleared on commit/abort/reopen before `env` is ever taken;
        //  * the txn never escapes `self`.
        let outcome: Result<RwTxn<'static>, Error> = match self.env().write_txn() {
            Ok(txn) => Ok(unsafe { std::mem::transmute::<RwTxn<'_>, RwTxn<'static>>(txn) }),
            Err(e) => Err(e),
        };
        match outcome {
            Ok(txn) => {
                self.active = Active::Rw(Box::new(txn));
                OpResult::Ok
            }
            Err(e) => OpResult::Err(to_oracle(e)),
        }
    }

    fn begin_ro(&mut self) -> OpResult {
        // SAFETY: as in `begin_rw`.
        let outcome: Result<RoTxn<'static>, Error> = match self.env().read_txn() {
            Ok(txn) => Ok(unsafe { std::mem::transmute::<RoTxn<'_>, RoTxn<'static>>(txn) }),
            Err(e) => Err(e),
        };
        match outcome {
            Ok(txn) => {
                self.active = Active::Ro(Box::new(txn));
                OpResult::Ok
            }
            Err(e) => OpResult::Err(to_oracle(e)),
        }
    }

    fn commit(&mut self) -> OpResult {
        self.cleared_in_txn = false;
        match std::mem::replace(&mut self.active, Active::None) {
            Active::Rw(w) => match w.commit() {
                Ok(()) => {
                    self.mark_dbs_committed();
                    self.debug_check_image();
                    OpResult::Ok
                }
                Err(e) => {
                    // Failed commit = abort: roll back txn-created dbs
                    // (matches LmdbEngine).
                    self.close_dbs_opened_in_aborted_txn();
                    OpResult::Err(to_oracle(e))
                }
            },
            // Commit with a live child first drops the child (mirrors
            // `LmdbEngine`; the driver only reaches this via `Commit`, which
            // is legal in `RwNested`), then commits the parent — TXN-33 is
            // satisfied because the child is gone before `commit()` runs.
            Active::RwNested { nested, wtxn } => {
                drop(nested);
                match wtxn.commit() {
                    Ok(()) => {
                        self.mark_dbs_committed();
                        self.debug_check_image();
                        OpResult::Ok
                    }
                    Err(e) => {
                        self.close_dbs_opened_in_aborted_txn();
                        OpResult::Err(to_oracle(e))
                    }
                }
            }
            Active::Ro(r) => {
                drop(r);
                OpResult::Ok
            }
            Active::None => OpResult::Skipped(Skip::NoTxn),
        }
    }

    fn abort(&mut self) -> OpResult {
        self.cleared_in_txn = false;
        match std::mem::replace(&mut self.active, Active::None) {
            Active::None => OpResult::Skipped(Skip::NoTxn),
            Active::Rw(w) => {
                w.abort();
                self.close_dbs_opened_in_aborted_txn();
                OpResult::Ok
            }
            // Child dropped before the parent aborts (field order also
            // guarantees this if dropped as a unit; mirrors `LmdbEngine`).
            Active::RwNested { nested, wtxn } => {
                drop(nested);
                wtxn.abort();
                self.close_dbs_opened_in_aborted_txn();
                OpResult::Ok
            }
            Active::Ro(r) => {
                drop(r);
                OpResult::Ok
            }
        }
    }

    fn begin_nested_ro(&mut self) -> OpResult {
        match std::mem::replace(&mut self.active, Active::None) {
            Active::Rw(wtxn) => {
                // Open the nested child and erase its borrow of the boxed
                // parent to 'static so both can be stored side by side.
                //
                // SAFETY (ADR-0007 D5): the child's one reference is
                // `&RwTxn`, pointing at the **Box's heap target** — a stable
                // address that survives moves of the `Box` itself and of
                // `self.active` between variants. Invariants:
                //  * `wtxn` stays boxed and is never dropped or moved-out
                //    while `nested` exists (`RwNested` holds both; `nested`
                //    is a *prior* field, so it drops first, and
                //    `end_nested_ro`/`commit`/`abort` all drop the child
                //    before touching the parent);
                //  * no `&mut RwTxn` is created while `nested` exists — the
                //    `Active::RwNested` state routes every write op to
                //    `Skip::WriteBlockedByNested` (and zerodb's own TXN-29
                //    counter guard backstops even that);
                //  * the child never escapes `self` and both drop before the
                //    boxed `Env` (field order of `ZerodbEngine`).
                let outcome: Result<NestedRoTxn<'static>, Error> = match wtxn.nested_read_txn() {
                    Ok(n) => Ok(unsafe {
                        std::mem::transmute::<NestedRoTxn<'_>, NestedRoTxn<'static>>(n)
                    }),
                    Err(e) => Err(e),
                };
                match outcome {
                    Ok(nested) => {
                        self.active = Active::RwNested { nested, wtxn };
                        OpResult::Ok
                    }
                    Err(e) => {
                        self.active = Active::Rw(wtxn);
                        OpResult::Err(to_oracle(e))
                    }
                }
            }
            other => {
                self.active = other;
                OpResult::Skipped(Skip::NoWriteTxnForNested)
            }
        }
    }

    fn end_nested_ro(&mut self) -> OpResult {
        match std::mem::replace(&mut self.active, Active::None) {
            Active::RwNested { nested, wtxn } => {
                // Child first (its Drop releases the parent's counter —
                // TXN-31), then the parent resumes as plain Rw.
                drop(nested);
                self.active = Active::Rw(wtxn);
                OpResult::Ok
            }
            other => {
                self.active = other;
                OpResult::Skipped(Skip::NoNestedToEnd)
            }
        }
    }

    // -- databases -------------------------------------------------------------

    fn create_db(&mut self, name: &DbName) -> OpResult {
        let resolved = name.resolve();
        // Reuse an already-open handle for this name (idempotent open).
        if self.dbs.iter().any(|e| e.name == resolved) {
            return match self.write_txn() {
                Ok(_) => OpResult::Ok,
                Err(skip) => skip,
            };
        }
        if let Err(skip) = self.write_txn() {
            return skip;
        }
        // Reborrow env + txn together (disjoint fields).
        let created = {
            let env = self.env.as_ref().expect("env present").as_ref();
            let wtxn = match &mut self.active {
                Active::Rw(w) => w,
                _ => unreachable!("write_txn() checked above"),
            };
            env.create_database(wtxn, resolved.as_deref().map(str::as_bytes))
        };
        match created {
            Ok(db) => {
                self.dbs.push(DbEntry {
                    name: resolved,
                    db,
                    committed: false,
                });
                OpResult::Ok
            }
            Err(e) => OpResult::Err(to_oracle(e)),
        }
    }

    fn clear_db(&mut self, db: u8) -> OpResult {
        self.cleared_in_txn = true; // FORK-1 guard fact
        let dbh = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let wtxn = match self.write_txn() {
            Ok(w) => w,
            Err(r) => return r,
        };
        match dbh.clear(wtxn) {
            Ok(()) => OpResult::Ok,
            Err(e) => OpResult::Err(to_oracle(e)),
        }
    }

    fn drop_db(&mut self, db: u8) -> OpResult {
        if self.dbs.is_empty() {
            return OpResult::Skipped(Skip::NoDb);
        }
        let idx = db as usize % self.dbs.len();
        let handle = self.dbs[idx].db;
        let wtxn = match self.write_txn() {
            Ok(w) => w,
            Err(r) => return r,
        };
        match handle.drop_db(wtxn) {
            Ok(()) => {
                // `mdb_drop(.., del=1)` closes the dbi at ENV level
                // (mdb.c `mdb_dbi_close`), and that close is NOT undone by a
                // later abort — the entry is gone for good, never restored on
                // rollback.
                self.dbs.remove(idx);
                OpResult::Ok
            }
            Err(e) => OpResult::Err(to_oracle(e)),
        }
    }

    // -- writes ------------------------------------------------------------------

    fn put(&mut self, db: u8, key: &[u8], val: &[u8]) -> OpResult {
        let dbh = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let wtxn = match self.write_txn() {
            Ok(w) => w,
            Err(r) => return r,
        };
        match dbh.put(wtxn, key, val) {
            Ok(()) => OpResult::Ok,
            Err(e) => OpResult::Err(to_oracle(e)),
        }
    }

    fn put_flagged(&mut self, db: u8, key: &[u8], val: &[u8], flag: PutFlag) -> OpResult {
        let flags = match flag {
            PutFlag::Append => PutFlags::APPEND,
            PutFlag::NoOverwrite => PutFlags::NO_OVERWRITE,
        };
        let dbh = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let wtxn = match self.write_txn() {
            Ok(w) => w,
            Err(r) => return r,
        };
        match dbh.put_with_flags(wtxn, flags, key, val) {
            Ok(()) => OpResult::Ok,
            Err(e) => OpResult::Err(to_oracle(e)),
        }
    }

    fn put_reserved(&mut self, db: u8, key: &[u8], val: &[u8]) -> OpResult {
        let dbh = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let wtxn = match self.write_txn() {
            Ok(w) => w,
            Err(r) => return r,
        };
        match dbh.put_reserved(wtxn, key, val.len(), |buf| buf.copy_from_slice(val)) {
            Ok(()) => OpResult::Ok,
            Err(e) => OpResult::Err(to_oracle(e)),
        }
    }

    fn del(&mut self, db: u8, key: &[u8]) -> OpResult {
        // LMDB API boundary (§2.1): `del` rejects only the empty key up front;
        // an oversized key finds nothing → Ok(false).
        if let Some(e) = bad_read_key(key) {
            return e;
        }
        let dbh = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let wtxn = match self.write_txn() {
            Ok(w) => w,
            Err(r) => return r,
        };
        match dbh.delete(wtxn, key) {
            Ok(existed) => OpResult::Bool(existed),
            Err(e) => OpResult::Err(to_oracle(e)),
        }
    }

    // -- in-place cursor mutation (iter_mut, SPEC 03 §7) -------------------------

    fn iter_mut_put(&mut self, db: u8, nth: u8, val: &[u8]) -> OpResult {
        let dbh = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let wtxn = match self.write_txn() {
            Ok(w) => w,
            Err(r) => return r,
        };
        let mut cur = dbh.rw_cursor(wtxn);
        for _ in 0..=nth {
            match cur.move_next() {
                Ok(Some(_)) => {}
                Ok(None) => return OpResult::Bool(false),
                Err(e) => return OpResult::Err(to_oracle(e)),
            }
        }
        match cur.put_current(val) {
            Ok(b) => OpResult::Bool(b),
            Err(e) => OpResult::Err(to_oracle(e)),
        }
    }

    fn iter_mut_del(&mut self, db: u8, nth: u8) -> OpResult {
        let dbh = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let wtxn = match self.write_txn() {
            Ok(w) => w,
            Err(r) => return r,
        };
        let mut cur = dbh.rw_cursor(wtxn);
        for _ in 0..=nth {
            match cur.move_next() {
                Ok(Some(_)) => {}
                Ok(None) => return OpResult::Bool(false),
                Err(e) => return OpResult::Err(to_oracle(e)),
            }
        }
        match cur.del_current() {
            Ok(b) => OpResult::Bool(b),
            Err(e) => OpResult::Err(to_oracle(e)),
        }
    }

    // -- reads --------------------------------------------------------------------

    fn get(&self, db: u8, key: &[u8]) -> OpResult {
        if let Some(e) = bad_read_key(key) {
            return e;
        }
        let dbh = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        with_read!(self.active, |t| match dbh.get(t, key) {
            Ok(v) => OpResult::MaybeVal(v.map(<[u8]>::to_vec)),
            Err(e) => OpResult::Err(to_oracle(e)),
        })
    }

    fn len(&self, db: u8) -> OpResult {
        let dbh = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        with_read!(self.active, |t| match dbh.len(t) {
            Ok(n) => OpResult::Count(n),
            Err(e) => OpResult::Err(to_oracle(e)),
        })
    }

    fn is_empty(&self, db: u8) -> OpResult {
        let dbh = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        with_read!(self.active, |t| match dbh.is_empty(t) {
            Ok(b) => OpResult::Bool(b),
            Err(e) => OpResult::Err(to_oracle(e)),
        })
    }

    fn first_last(&self, db: u8, last: bool) -> OpResult {
        let dbh = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        with_read!(self.active, |t| {
            let r = if last { dbh.last(t) } else { dbh.first(t) };
            match r {
                Ok(e) => OpResult::MaybeEntry(e.map(|(k, v)| (k.to_vec(), v.to_vec()))),
                Err(e) => OpResult::Err(to_oracle(e)),
            }
        })
    }

    fn seek(&self, db: u8, key: &[u8], kind: Seek) -> OpResult {
        if let Some(e) = bad_seek_key(key) {
            return e;
        }
        let dbh = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        with_read!(self.active, |t| {
            let r = match kind {
                Seek::Ge => dbh.get_greater_than_or_equal_to(t, key),
                Seek::Gt => dbh.get_greater_than(t, key),
                Seek::Le => dbh.get_lower_than_or_equal_to(t, key),
            };
            match r {
                Ok(e) => OpResult::MaybeEntry(e.map(|(k, v)| (k.to_vec(), v.to_vec()))),
                Err(e) => OpResult::Err(to_oracle(e)),
            }
        })
    }

    fn iter(&self, db: u8, rev: bool) -> OpResult {
        let dbh = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        with_read!(self.active, |t| {
            let it = if rev { dbh.rev_iter(t) } else { dbh.iter(t) };
            collect_iter(it)
        })
    }

    fn prefix_iter(&self, db: u8, prefix: &[u8], rev: bool) -> OpResult {
        if let Some(e) = bad_prefix_key(prefix, rev) {
            return e;
        }
        let dbh = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        with_read!(self.active, |t| {
            let it = if rev {
                dbh.rev_prefix_iter(t, prefix)
            } else {
                dbh.prefix_iter(t, prefix)
            };
            collect_iter(it)
        })
    }

    fn verify_get(&self, db: u8, key: &[u8]) -> OpResult {
        // A fresh independent read txn: sees only committed state, regardless
        // of any active txn (readers never block on the writer, TXN-9). The DB
        // is resolved by name through `open_database` — a committed catalog
        // lookup, matching `LmdbEngine::verify_get`.
        //
        // Ordering matters (found by the M1.6 differential): LMDB resolves and
        // **opens** the DB before any key-size check, so an uncommitted /
        // dropped name returns `None` regardless of the key. The empty-read-key
        // boundary shim (§2.1) must therefore be applied only once the DB
        // opens — at the `get`, exactly where LMDB's `mdb_get` would hit it.
        let name = match self.db_name_at(db) {
            Some(n) => n,
            None => return OpResult::Skipped(Skip::NoDb),
        };
        let env = self.env();
        let rtxn = match env.read_txn() {
            Ok(t) => t,
            Err(e) => return OpResult::Err(to_oracle(e)),
        };
        let dbh = match env.open_database(&rtxn, name.as_deref().map(str::as_bytes)) {
            Ok(Some(d)) => d,
            // Not yet committed (or dropped): nothing visible, key irrelevant.
            Ok(None) => return OpResult::MaybeVal(None),
            Err(e) => return OpResult::Err(to_oracle(e)),
        };
        if let Some(e) = bad_read_key(key) {
            return e;
        }
        match dbh.get(&rtxn, key) {
            Ok(v) => OpResult::MaybeVal(v.map(<[u8]>::to_vec)),
            Err(e) => OpResult::Err(to_oracle(e)),
        }
    }
}

fn collect_iter(it: zerodb::RoRange<'_>) -> OpResult {
    let mut out = Vec::new();
    for item in it {
        match item {
            Ok((k, v)) => out.push((k.to_vec(), v.to_vec())),
            Err(e) => return OpResult::Err(to_oracle(e)),
        }
    }
    OpResult::Entries(out)
}

/// Neighbor-seek variants.
enum Seek {
    Ge,
    Gt,
    Le,
}

const BAD: Option<OpResult> = Some(OpResult::Err(OracleError::BadValSize));

/// Exact **read**-key size validation (observed via the oracle, SPEC 03 §2.1):
/// an **empty** key → `BadValSize`; an **oversized** key is *not* rejected —
/// the search finds nothing. Used by `get`/`set`/`del`/`verify_get`.
fn bad_read_key(key: &[u8]) -> Option<OpResult> {
    if key.is_empty() {
        BAD
    } else {
        None
    }
}

/// Prefix-key validation (SPEC 03 §2.1): forward empty prefix → `BadValSize`
/// (realized as an empty set-range); reverse empty prefix seeks via `last`
/// and works.
fn bad_prefix_key(key: &[u8], rev: bool) -> Option<OpResult> {
    if key.is_empty() && !rev {
        BAD
    } else {
        None
    }
}

/// Seek-key validation (SPEC 03 §2.1): only the empty key errors.
fn bad_seek_key(key: &[u8]) -> Option<OpResult> {
    if key.is_empty() {
        BAD
    } else {
        None
    }
}

/// Map a `zerodb::Error` into the oracle's normalized taxonomy (matching
/// [`crate::LmdbEngine`]'s mapping so error kinds compare equal). `DbsFull` and
/// `Incompatible` fall through to `Other("mdb:DbsFull"/"mdb:Incompatible")`,
/// the same string `heed`'s mapping produces for its identical variants.
fn to_oracle(e: Error) -> OracleError {
    match e {
        Error::Mdb(m) => match m {
            MdbError::NotFound => OracleError::NotFound,
            MdbError::KeyExist => OracleError::KeyExist,
            MdbError::MapFull => OracleError::MapFull,
            MdbError::BadValSize => OracleError::BadValSize,
            MdbError::Invalid => OracleError::Invalid,
            other => OracleError::Other(format!("mdb:{other:?}")),
        },
        Error::Io(io) => OracleError::Other(format!("io:{}", io.kind())),
        Error::EnvAlreadyOpened => OracleError::Other("env-already-opened".into()),
        other => OracleError::Other(format!("{other:?}")),
    }
}

impl Engine for ZerodbEngine {
    fn new() -> Self {
        Self::new_in_mode(EngineMode::DEFAULT)
    }

    fn new_in_mode(mode: EngineMode) -> Self {
        let dir = TempDir::new().expect("create temp dir");
        let env = ZerodbEngine::open(dir.path(), BASE_MAP_SIZE, mode).expect("open zerodb env");
        ZerodbEngine {
            active: Active::None,
            dbs: Vec::new(),
            env: Some(Box::new(env)),
            map_size: BASE_MAP_SIZE,
            cleared_in_txn: false,
            mode,
            dir,
        }
    }

    fn name(&self) -> &'static str {
        "zerodb"
    }

    fn real_disk_size(&self) -> Option<u64> {
        self.env.as_ref().and_then(|e| e.real_disk_size().ok())
    }

    fn implements(&self, _op: &Op) -> bool {
        // Every modeled op is differential as of M1.9 (nested read txns were
        // the last gated pair).
        true
    }

    fn apply(&mut self, op: &Op) -> OpResult {
        // The shared driver is the single authority on op-validity / Skip.
        if let Some(skip) = crate::driver::classify(
            op,
            self.txn_state(),
            self.dbs.is_empty(),
            self.cleared_in_txn,
        ) {
            return OpResult::Skipped(skip);
        }
        match op {
            Op::Reopen { map_size_kib } => self.reopen(*map_size_kib),

            Op::BeginRw => self.begin_rw(),
            Op::BeginRo => self.begin_ro(),
            Op::Commit => self.commit(),
            Op::Abort => self.abort(),

            Op::CreateDb { name } => self.create_db(name),
            Op::ClearDb { db } => self.clear_db(*db),
            Op::DropDb { db } => self.drop_db(*db),

            Op::Get { db, key } => self.get(*db, &key.0),
            Op::Put { db, key, val } => self.put(*db, &key.0, &val.0),
            Op::PutFlagged { db, key, val, flag } => self.put_flagged(*db, &key.0, &val.0, *flag),
            Op::PutReserved { db, key, val } => self.put_reserved(*db, &key.0, &val.0),
            Op::Del { db, key } => self.del(*db, &key.0),
            Op::Len { db } => self.len(*db),
            Op::IsEmpty { db } => self.is_empty(*db),

            Op::First { db } => self.first_last(*db, false),
            Op::Last { db } => self.first_last(*db, true),
            Op::SetExact { db, key } => self.get(*db, &key.0),
            Op::SetRange { db, key } => self.seek(*db, &key.0, Seek::Ge),
            Op::GetGreaterThan { db, key } => self.seek(*db, &key.0, Seek::Gt),
            Op::GetLowerThanOrEqualTo { db, key } => self.seek(*db, &key.0, Seek::Le),

            Op::Iter { db } => self.iter(*db, false),
            Op::RevIter { db } => self.iter(*db, true),
            Op::PrefixIter { db, prefix } => self.prefix_iter(*db, &prefix.0, false),
            Op::RevPrefixIter { db, prefix } => self.prefix_iter(*db, &prefix.0, true),

            Op::IterMutPutCurrent { db, nth, val } => self.iter_mut_put(*db, *nth, &val.0),
            Op::IterMutDelCurrent { db, nth } => self.iter_mut_del(*db, *nth),

            Op::VerifyGet { db, key } => self.verify_get(*db, &key.0),

            Op::BeginNestedRo => self.begin_nested_ro(),
            Op::EndNestedRo => self.end_nested_ro(),
        }
    }
}
