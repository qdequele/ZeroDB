//! The [`Engine`] driven **through the `heed-zerodb` adapter** (milestone 1.13,
//! ADR-0003 accept-criterion 6: "the oracle re-run *through* the adapter shows
//! zero divergences"). This is a near-verbatim copy of [`crate::lmdb`] with the
//! backend import swapped `heed` → `heed_zerodb`, so the *same op driver* runs
//! over the ZeroDB engine behind the heed surface. Paired against
//! [`crate::LmdbEngine`] (real LMDB) in
//! `tests/heed_adapter_differential.rs`, it validates that the adapter boundary
//! introduces no observable divergence.
//!
//! ## Self-referential transaction state (the one unsafe spot)
//!
//! `heed_zerodb`'s `RwTxn<'p>` / `RoTxn<'e>` borrow the `Env`, and a nested read
//! txn borrows its parent `RwTxn`. To hold an *open* transaction across separate
//! [`Engine::apply`] calls, this engine stores those transactions next to the
//! `Env` in one struct — a self-referential shape safe Rust cannot express
//! without a helper crate (none are on the allowlist). We therefore extend the
//! transaction lifetimes to `'static` with `mem::transmute` and uphold the
//! borrows manually. The invariants are stated at each `unsafe` site; the whole
//! construction is confined to this test-only oracle crate, exactly where the
//! CLAUDE.md unsafe policy permits FFI-adjacent unsafe.

use std::ops::Deref;

use heed_zerodb::types::Bytes;
use heed_zerodb::{Database, Env, EnvOpenOptions, RoTxn, RwTxn, WithoutTls};

use crate::result::{OpResult, OracleError, Skip};
use crate::tempdir::TempDir;
use crate::{DbName, Engine, EngineMode, Op, PutFlag};

/// Base map size; growth is layered on top in 4 KiB units. Sized so a full
/// op sequence cannot exhaust the map (see [`crate::DIFF_MAP_SIZE`]).
const BASE_MAP_SIZE: usize = crate::DIFF_MAP_SIZE;
/// Catalog capacity: unnamed + `db0`..`db3` plus slack.
const MAX_DBS: u32 = 16;

type Db = Database<Bytes, Bytes>;

/// A database handle plus the catalog name it was opened under.
struct DbEntry {
    name: Option<String>,
    db: Db,
    /// Whether the transaction that opened this handle has **committed**.
    ///
    /// LMDB (`lmdb.h`): "The database handle will be private to the current
    /// transaction until the transaction is successfully committed. If the
    /// transaction is aborted the handle will be closed automatically."
    /// `mdb.c`'s `mdb_dbis_update(txn, keep=0)` implements that close on the
    /// abort path; a dead handle then fails the `TXN_DBI_EXIST` gate with
    /// `EINVAL`. (Replaces the old positional `committed_dbs` watermark,
    /// which `drop_db`'s middle-removal broke — the 2026-07-20 harness fix.)
    committed: bool,
    /// Whether the handle is still open. **Dead handles are kept in the vec
    /// and stay addressable** (M2.9 / ADR-0013): resolving a `db` index to a
    /// dead entry issues the op against the stale handle on BOTH engines,
    /// which is now a differential state — LMDB answers `EINVAL` from
    /// `TXN_DBI_EXIST`, ZeroDB `BadDbi` from the TXN-68 generation gate, and
    /// both normalize to `OracleError::BadDbi`. Death events: end of the
    /// creating txn without commit, and a successful `DropDb`. Dead entries
    /// are purged at the next executed `CreateDb` (see `create_db`) so the
    /// harness never drives a stale handle after LMDB may have *reused* its
    /// freed dbi slot for a new name — the resurrection corner ZeroDB
    /// deliberately does not replicate (D-009 hazard class; DIVERGENCES
    /// D-015 PROPOSED).
    alive: bool,
}

/// The currently-open transaction, if any.
///
/// Variant field order matters for `Drop`: fields drop in declaration order, so
/// in [`Active::RwNested`] the child `nested` is aborted before the parent
/// `wtxn`, and in every case the transaction is dropped before the `Env` (which
/// is a later field of [`HeedZerodbEngine`]).
// The write txn is **boxed** for a stable heap address. This is the one
// structural difference from [`crate::lmdb`]: heed's nested reader holds a raw
// C `MDB_txn` pointer (heap-stable in LMDB regardless of where the Rust
// `RwTxn` moves), but `heed_zerodb`'s nested reader holds a genuine Rust borrow
// into the parent `RwTxn`. When `begin_nested_ro` `mem::transmute`s that borrow
// to `'static` and moves the wtxn into `Active::RwNested`, an unboxed wtxn would
// relocate the borrowed `zerodb::RwTxn` and dangle the nested reader (observed:
// SIGSEGV). Boxing keeps the pointee put — the same discipline this engine
// already applies to `Env`.
enum Active {
    None,
    Rw(Box<RwTxn<'static>>),
    Ro(RoTxn<'static, WithoutTls>),
    RwNested {
        nested: RoTxn<'static, WithoutTls>,
        wtxn: Box<RwTxn<'static>>,
    },
}

/// The engine driven through the heed-zerodb adapter.
///
/// Field order is load-bearing for `Drop`: `active` (transactions) before `env`
/// before `dir`, so transactions close, then the env closes, then the backing
/// directory is removed.
pub struct HeedZerodbEngine {
    active: Active,
    dbs: Vec<DbEntry>,
    /// `Box` gives the `Env` a stable heap address that survives moves of the
    /// engine struct, which is what makes the `'static` transaction borrows
    /// sound. Always `Some` between operations.
    env: Option<Box<Env<WithoutTls>>>,
    map_size: usize,
    /// Set when the env was irrecoverably lost (double reopen failure); every
    /// subsequent op returns a comparable error instead of panicking.
    poisoned: Option<String>,
    /// Whether the current write txn has executed a `ClearDb` — the FORK-1
    /// guard fact (see `driver::classify` and `docs/UPSTREAM-BUGS.md`). Set in
    /// `clear_db`, reset at every txn boundary.
    cleared_in_txn: bool,
    /// The env open mode (M1.10): `WRITE_MAP` / durability flags. Preserved
    /// across `reopen` so a reopened env keeps the same write mode.
    mode: EngineMode,
    dir: TempDir,
}

impl HeedZerodbEngine {
    fn env(&self) -> &Env<WithoutTls> {
        self.env
            .as_ref()
            .expect("env is always present between ops")
    }

    /// The current read source: the nested reader if one is open, otherwise the
    /// active read or write txn.
    fn read_source(&self) -> Result<&RoTxn<'static, heed_zerodb::AnyTls>, OpResult> {
        match &self.active {
            Active::RwNested { nested, .. } => Ok(nested.deref()),
            Active::Ro(r) => Ok(r.deref()),
            // Box<RwTxn> -> &RwTxn -> &RoTxn<WithoutTls> -> &RoTxn<AnyTls>.
            Active::Rw(w) => Ok(w.deref().deref().deref()),
            Active::None => Err(OpResult::Skipped(Skip::NoTxn)),
        }
    }

    /// The active write txn, or a structured skip explaining why there isn't one.
    fn write_txn(&mut self) -> Result<&mut RwTxn<'static>, OpResult> {
        match &mut self.active {
            Active::Rw(w) => Ok(w.as_mut()),
            Active::RwNested { .. } => Err(OpResult::Skipped(Skip::WriteBlockedByNested)),
            Active::Ro(_) | Active::None => Err(OpResult::Skipped(Skip::NoWriteTxn)),
        }
    }

    /// Mark every live handle as surviving the txn boundary: a successful
    /// commit "exports" the dbis opened in this txn into the shared env
    /// (`mdb_dbis_update(txn, keep=1)`), after which they stay valid.
    fn mark_dbs_committed(&mut self) {
        for e in &mut self.dbs {
            if e.alive {
                e.committed = true;
            }
        }
    }

    /// Mark dead the handles LMDB closes when a write txn ends without
    /// committing (`mdb_dbis_update(txn, keep=0)`): exactly those opened in
    /// that txn. Committed handles survive. Since M2.9 (ADR-0013) the dead
    /// entries are KEPT: a later op resolving to one is a real differential
    /// stale-handle use, which both engines must refuse identically
    /// (`OracleError::BadDbi`).
    fn close_dbs_opened_in_aborted_txn(&mut self) {
        for e in &mut self.dbs {
            if e.alive && !e.committed {
                e.alive = false;
            }
        }
    }

    /// Resolve a db index (modulo the number of TRACKED dbs, dead included —
    /// M2.9) to a handle. A dead entry's stale handle is returned on purpose:
    /// issuing the op against it is the differential use-after-close state
    /// both engines must refuse identically (`OracleError::BadDbi`).
    fn db_at(&self, db: u8) -> Result<Db, OpResult> {
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

    /// Map this engine's concrete transaction state onto the abstract
    /// [`TxnState`](crate::driver::TxnState) the shared classifier reasons over.
    fn txn_state(&self) -> crate::driver::TxnState {
        use crate::driver::TxnState;
        match &self.active {
            Active::None => TxnState::None,
            Active::Rw(_) => TxnState::Rw,
            Active::Ro(_) => TxnState::Ro,
            Active::RwNested { .. } => TxnState::RwNested,
        }
    }

    fn desired_map_size(&self, kib: u16) -> usize {
        let want = BASE_MAP_SIZE + (kib as usize) * 4096;
        // Monotonic: never shrink below the current size, so a reopen can never
        // fail by cutting below the live data (models "reopen larger on MapFull").
        // Rounded to a 64 KiB multiple so heed accepts it and both engines agree
        // on the effective size (DIVERGENCES D-006).
        crate::round_map_size(want.max(self.map_size))
    }
}

/// The heed env flags for a mode (M1.10, SPEC 01 Table 1). `WithoutTls` is set
/// separately via `read_txn_without_tls()` (SPEC 00 rows 2/29); these are the
/// durability / write-mode bits only.
fn heed_flags(mode: EngineMode) -> heed_zerodb::EnvFlags {
    let mut f = heed_zerodb::EnvFlags::empty();
    if mode.write_map {
        f |= heed_zerodb::EnvFlags::WRITE_MAP;
    }
    if mode.no_sync {
        f |= heed_zerodb::EnvFlags::NO_SYNC;
    }
    if mode.no_meta_sync {
        f |= heed_zerodb::EnvFlags::NO_META_SYNC;
    }
    if mode.map_async {
        f |= heed_zerodb::EnvFlags::MAP_ASYNC;
    }
    f
}

fn open_env(
    dir: &std::path::Path,
    map_size: usize,
    mode: EngineMode,
) -> heed_zerodb::Result<Env<WithoutTls>> {
    let mut opts = EnvOpenOptions::new().read_txn_without_tls();
    opts.map_size(map_size);
    opts.max_dbs(MAX_DBS);
    let flags = heed_flags(mode);
    // SAFETY: `open`/`flags` are `unsafe` only because LMDB env flags can enable
    // cross-process behaviors; the flags we set (`WRITE_MAP` + durability, M1.10)
    // are single-process-safe, and the path is a private temp dir used
    // single-threaded by this engine instance.
    unsafe {
        if !flags.is_empty() {
            opts.flags(flags);
        }
        opts.open(dir)
    }
}

/// Map a heed error into the oracle's normalized taxonomy.
fn to_oracle(e: heed_zerodb::Error) -> OracleError {
    match e {
        heed_zerodb::Error::Mdb(m) => match m {
            heed_zerodb::MdbError::NotFound => OracleError::NotFound,
            heed_zerodb::MdbError::KeyExist => OracleError::KeyExist,
            heed_zerodb::MdbError::MapFull => OracleError::MapFull,
            heed_zerodb::MdbError::BadValSize => OracleError::BadValSize,
            heed_zerodb::MdbError::Invalid => OracleError::Invalid,
            // Defensive namesake mapping; the adapter's boundary re-imposes
            // the fork's `Io(EINVAL)` shape, matched below.
            heed_zerodb::MdbError::BadDbi => OracleError::BadDbi,
            other => OracleError::Other(format!("mdb:{other:?}")),
        },
        // The adapter maps zerodb's `BadDbi` to the fork's exact observable —
        // raw `EINVAL` (M2.9, ADR-0013); normalize like the LMDB engine.
        heed_zerodb::Error::Io(io) if io.kind() == std::io::ErrorKind::InvalidInput => {
            OracleError::BadDbi
        }
        heed_zerodb::Error::Io(io) => OracleError::Other(format!("io:{}", io.kind())),
        heed_zerodb::Error::Encoding(_) => OracleError::Other("encoding".into()),
        heed_zerodb::Error::Decoding(_) => OracleError::Other("decoding".into()),
        heed_zerodb::Error::EnvAlreadyOpened => OracleError::Other("env-already-opened".into()),
    }
}

fn err(e: heed_zerodb::Error) -> OpResult {
    OpResult::Err(to_oracle(e))
}

impl Engine for HeedZerodbEngine {
    fn new() -> Self {
        Self::new_in_mode(EngineMode::DEFAULT)
    }

    fn new_in_mode(mode: EngineMode) -> Self {
        let dir = TempDir::new().expect("create temp dir");
        let env = open_env(dir.path(), BASE_MAP_SIZE, mode).expect("open env");
        HeedZerodbEngine {
            active: Active::None,
            dbs: Vec::new(),
            env: Some(Box::new(env)),
            map_size: BASE_MAP_SIZE,
            poisoned: None,
            cleared_in_txn: false,
            mode,
            dir,
        }
    }

    fn name(&self) -> &'static str {
        "heed-zerodb"
    }

    fn real_disk_size(&self) -> Option<u64> {
        self.env.as_ref().and_then(|e| e.real_disk_size().ok())
    }

    fn apply(&mut self, op: &Op) -> OpResult {
        if let Some(reason) = &self.poisoned {
            return OpResult::Err(crate::result::OracleError::Other(format!(
                "poisoned: {reason}"
            )));
        }
        // The shared driver is the single authority on op-validity/Skip
        // (M1.2 hoist). If it says skip, do so without touching the backend; the
        // per-method `db_at`/`write_txn`/`read_source` helpers below only resolve
        // handles from here on (their skip arms are unreachable after this gate).
        if let Some(skip) = crate::driver::classify(
            op,
            self.txn_state(),
            self.dbs.is_empty(),
            self.cleared_in_txn,
        ) {
            return OpResult::Skipped(skip);
        }
        match op {
            // ---------------- environment ----------------
            Op::Reopen { map_size_kib } => self.reopen(*map_size_kib),

            // ---------------- transactions ----------------
            Op::BeginRw => self.begin_rw(),
            Op::BeginRo => self.begin_ro(),
            Op::Commit => self.commit(),
            Op::Abort => self.abort(),
            Op::BeginNestedRo => self.begin_nested_ro(),
            Op::EndNestedRo => self.end_nested_ro(),

            // ---------------- databases ----------------
            Op::CreateDb { name } => self.create_db(name),
            Op::ClearDb { db } => self.clear_db(*db),
            Op::DropDb { db } => self.drop_db(*db),

            // ---------------- key/value ----------------
            Op::Get { db, key } => self.get(*db, &key.0),
            Op::Put { db, key, val } => self.put(*db, &key.0, &val.0),
            Op::PutFlagged { db, key, val, flag } => self.put_flagged(*db, &key.0, &val.0, *flag),
            Op::PutReserved { db, key, val } => self.put_reserved(*db, &key.0, &val.0),
            Op::Del { db, key } => self.del(*db, &key.0),
            Op::Len { db } => self.len(*db),
            Op::IsEmpty { db } => self.is_empty(*db),

            // ---------------- positioning ----------------
            Op::First { db } => self.first(*db),
            Op::Last { db } => self.last(*db),
            Op::SetExact { db, key } => self.get(*db, &key.0),
            Op::SetRange { db, key } => self.seek(*db, &key.0, Seek::Ge),
            Op::GetGreaterThan { db, key } => self.seek(*db, &key.0, Seek::Gt),
            Op::GetLowerThanOrEqualTo { db, key } => self.seek(*db, &key.0, Seek::Le),

            // ---------------- iteration ----------------
            Op::Iter { db } => self.iter(*db, false),
            Op::RevIter { db } => self.iter(*db, true),
            Op::PrefixIter { db, prefix } => self.prefix_iter(*db, &prefix.0, false),
            Op::RevPrefixIter { db, prefix } => self.prefix_iter(*db, &prefix.0, true),

            // ---------------- in-place mutation ----------------
            Op::IterMutPutCurrent { db, nth, val } => self.iter_mut_put(*db, *nth, &val.0),
            Op::IterMutDelCurrent { db, nth } => self.iter_mut_del(*db, *nth),

            // ---------------- verification ----------------
            Op::VerifyGet { db, key } => self.verify_get(*db, &key.0),
        }
    }
}

/// Neighbor-seek variants.
enum Seek {
    Ge,
    Gt,
    Le,
}

impl HeedZerodbEngine {
    fn reopen(&mut self, kib: u16) -> OpResult {
        // Close all transactions and forget handles before dropping the env
        // (heed's same-process registry forbids two open handles to one path).
        // Reopen drops the active txn, so it is a txn boundary: reset the FORK-1
        // guard fact too, exactly as `commit`/`abort`/`begin_rw` do (and as
        // `ZerodbEngine::reopen` does). Without this the two engines' tracked
        // `cleared_in_txn` drift after a reopen-while-cleared, making the shared
        // `classify` FORK-1 guard fire asymmetrically (found by the M1.3
        // differential fuzz).
        self.active = Active::None;
        self.cleared_in_txn = false;
        self.dbs.clear();
        drop(self.env.take());

        let new_size = self.desired_map_size(kib);
        match open_env(self.dir.path(), new_size, self.mode) {
            Ok(env) => {
                self.env = Some(Box::new(env));
                self.map_size = new_size;
                OpResult::Ok
            }
            Err(e) => {
                // Reopening at a non-shrinking size should not fail; if it
                // somehow does, restore the env at the previous size (which was
                // open moments ago) so the engine stays usable, and report.
                match open_env(self.dir.path(), self.map_size, self.mode) {
                    Ok(env) => self.env = Some(Box::new(env)),
                    Err(restore) => {
                        // Both opens failed (e.g. transient EMFILE): the env is
                        // gone. Poison the engine so every later op reports a
                        // comparable error instead of panicking on `env()`.
                        self.poisoned = Some(format!("env lost on reopen: {restore}"));
                    }
                }
                err(e)
            }
        }
    }

    fn begin_rw(&mut self) -> OpResult {
        // Txn boundary: reset the FORK-1 guard fact.
        self.cleared_in_txn = false;
        if !matches!(self.active, Active::None) {
            return OpResult::Skipped(Skip::TxnAlreadyOpen);
        }
        // SAFETY: extend the txn borrow to 'static. Invariants:
        //  * the `Env` lives in a `Box` (stable address) owned by `self`, and
        //    outlives the txn — the txn is a *prior* field, so it is dropped
        //    before the env, and is cleared on commit/abort/reopen before the
        //    env is ever taken;
        //  * the txn never escapes `self`.
        // The `outcome` binding erases the borrow of `self` before we assign
        // `self.active`.
        let outcome: heed_zerodb::Result<RwTxn<'static>> = match self.env().write_txn() {
            Ok(txn) => Ok(unsafe { std::mem::transmute::<RwTxn<'_>, RwTxn<'static>>(txn) }),
            Err(e) => Err(e),
        };
        match outcome {
            Ok(txn) => {
                self.active = Active::Rw(Box::new(txn));
                OpResult::Ok
            }
            Err(e) => err(e),
        }
    }

    fn begin_ro(&mut self) -> OpResult {
        if !matches!(self.active, Active::None) {
            return OpResult::Skipped(Skip::TxnAlreadyOpen);
        }
        // SAFETY: as in `begin_rw`.
        let outcome: heed_zerodb::Result<RoTxn<'static, WithoutTls>> = match self.env().read_txn() {
            Ok(txn) => Ok(unsafe {
                std::mem::transmute::<RoTxn<'_, WithoutTls>, RoTxn<'static, WithoutTls>>(txn)
            }),
            Err(e) => Err(e),
        };
        match outcome {
            Ok(txn) => {
                self.active = Active::Ro(txn);
                OpResult::Ok
            }
            Err(e) => err(e),
        }
    }

    fn commit(&mut self) -> OpResult {
        // Txn boundary: reset the FORK-1 guard fact.
        self.cleared_in_txn = false;
        match std::mem::replace(&mut self.active, Active::None) {
            Active::Rw(w) => match (*w).commit() {
                Ok(()) => {
                    self.mark_dbs_committed();
                    OpResult::Ok
                }
                Err(e) => {
                    // A failed commit aborts the txn, closing dbis opened within
                    // it — same rollback as abort().
                    self.close_dbs_opened_in_aborted_txn();
                    err(e)
                }
            },
            Active::RwNested { nested, wtxn } => {
                drop(nested);
                match (*wtxn).commit() {
                    Ok(()) => {
                        self.mark_dbs_committed();
                        OpResult::Ok
                    }
                    Err(e) => {
                        self.close_dbs_opened_in_aborted_txn();
                        err(e)
                    }
                }
            }
            Active::Ro(r) => match r.commit() {
                Ok(()) => OpResult::Ok,
                Err(e) => err(e),
            },
            Active::None => OpResult::Skipped(Skip::NoTxn),
        }
    }

    fn abort(&mut self) -> OpResult {
        // Txn boundary: reset the FORK-1 guard fact.
        self.cleared_in_txn = false;
        match std::mem::replace(&mut self.active, Active::None) {
            Active::None => OpResult::Skipped(Skip::NoTxn),
            Active::Rw(w) => {
                (*w).abort();
                // Databases created during this txn are rolled back.
                self.close_dbs_opened_in_aborted_txn();
                OpResult::Ok
            }
            Active::RwNested { nested, wtxn } => {
                drop(nested);
                (*wtxn).abort();
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
                // Open the nested reader and immediately erase its borrow of
                // `wtxn` (extend to 'static) so `wtxn` can be moved next to it.
                //
                // SAFETY: unlike heed (whose nested reader is a heap-stable C
                // `MDB_txn` pointer), `heed_zerodb`'s nested reader holds a real
                // Rust borrow into the parent `RwTxn`. That borrow stays valid
                // across the move **because `wtxn` is boxed**: moving the `Box`
                // relocates only the pointer, not the pointee the nested reader
                // borrows. `nested` is dropped before `wtxn` (field order), and
                // both before the env.
                let outcome: heed_zerodb::Result<RoTxn<'static, WithoutTls>> = match wtxn
                    .nested_read_txn()
                {
                    Ok(n) => Ok(unsafe {
                        std::mem::transmute::<RoTxn<'_, WithoutTls>, RoTxn<'static, WithoutTls>>(n)
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
                        err(e)
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

    fn create_db(&mut self, name: &DbName) -> OpResult {
        // M2.9 (ADR-0013): purge dead entries BEFORE opening/creating. LMDB
        // frees a closed handle's dbi slot and `mdb_dbi_open` may REUSE it for
        // this (or a later) name, silently resurrecting a stale handle —
        // ZeroDB's append-only generation registry deliberately refuses that
        // (D-009 hazard class; DIVERGENCES D-015 PROPOSED). Purging here keeps
        // stale handles addressable only in the window where both engines
        // agree they are dead: between the close event and the next create.
        self.dbs.retain(|e| e.alive);
        let name = name.resolve();
        // Reuse an already-open handle for this name.
        if self.dbs.iter().any(|e| e.name == name) {
            return match self.write_txn() {
                Ok(_) => OpResult::Ok,
                Err(skip) => skip,
            };
        }
        // Need a write txn; look it up without holding a borrow across create.
        if let Err(skip) = self.write_txn() {
            return skip;
        }
        // Reborrow env + txn together. `env()` and `active` are disjoint fields.
        let created = {
            let env = self.env.as_ref().expect("env present").as_ref();
            let wtxn = match &mut self.active {
                Active::Rw(w) => w.as_mut(),
                _ => unreachable!("write_txn() checked above"),
            };
            env.create_database::<Bytes, Bytes>(wtxn, name.as_deref())
        };
        match created {
            Ok(db) => {
                self.dbs.push(DbEntry {
                    name,
                    db,
                    committed: false,
                    alive: true,
                });
                OpResult::Ok
            }
            Err(e) => err(e),
        }
    }

    fn clear_db(&mut self, db: u8) -> OpResult {
        // FORK-1 guard fact: this write txn has cleared a db (see driver::classify).
        self.cleared_in_txn = true;
        let db = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let wtxn = match self.write_txn() {
            Ok(w) => w,
            Err(r) => return r,
        };
        match db.clear(wtxn) {
            Ok(()) => OpResult::Ok,
            Err(e) => err(e),
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
        // SAFETY: `remove` invalidates the dbi for any other handle to the
        // same database; this engine keeps exactly one handle per catalog
        // entry. The entry is marked dead below and only ever used again as a
        // deliberate stale-handle probe (M2.9), which the adapter refuses at
        // the TXN-68 generation gate before touching any state.
        let res = unsafe { handle.remove(wtxn) };
        match res {
            Ok(()) => {
                // `mdb_drop(.., del=1)` closes the dbi at ENV level (zerodb:
                // the TXN-68 generation bump), NOT undone by a later abort.
                // The entry is KEPT, dead (M2.9): later uses are differential
                // stale-handle probes.
                self.dbs[idx].alive = false;
                OpResult::Ok
            }
            Err(e) => err(e),
        }
    }

    fn get(&mut self, db: u8, key: &[u8]) -> OpResult {
        let db = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let rtxn = match self.read_source() {
            Ok(t) => t,
            Err(r) => return r,
        };
        match db.get(rtxn, key) {
            Ok(v) => OpResult::MaybeVal(v.map(|v| v.to_vec())),
            Err(e) => err(e),
        }
    }

    fn put(&mut self, db: u8, key: &[u8], val: &[u8]) -> OpResult {
        let db = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let wtxn = match self.write_txn() {
            Ok(w) => w,
            Err(r) => return r,
        };
        match db.put(wtxn, key, val) {
            Ok(()) => OpResult::Ok,
            Err(e) => err(e),
        }
    }

    fn put_flagged(&mut self, db: u8, key: &[u8], val: &[u8], flag: PutFlag) -> OpResult {
        let db = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let flags = match flag {
            PutFlag::Append => heed_zerodb::PutFlags::APPEND,
            PutFlag::NoOverwrite => heed_zerodb::PutFlags::NO_OVERWRITE,
        };
        let wtxn = match self.write_txn() {
            Ok(w) => w,
            Err(r) => return r,
        };
        match db.put_with_flags(wtxn, flags, key, val) {
            Ok(()) => OpResult::Ok,
            Err(e) => err(e),
        }
    }

    fn put_reserved(&mut self, db: u8, key: &[u8], val: &[u8]) -> OpResult {
        use std::io::Write as _;
        let db = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let wtxn = match self.write_txn() {
            Ok(w) => w,
            Err(r) => return r,
        };
        let res = db.put_reserved(wtxn, key, val.len(), |space| space.write_all(val));
        match res {
            Ok(()) => OpResult::Ok,
            Err(e) => err(e),
        }
    }

    fn del(&mut self, db: u8, key: &[u8]) -> OpResult {
        let db = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let wtxn = match self.write_txn() {
            Ok(w) => w,
            Err(r) => return r,
        };
        match db.delete(wtxn, key) {
            Ok(existed) => OpResult::Bool(existed),
            Err(e) => err(e),
        }
    }

    fn len(&mut self, db: u8) -> OpResult {
        let db = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let rtxn = match self.read_source() {
            Ok(t) => t,
            Err(r) => return r,
        };
        match db.len(rtxn) {
            Ok(n) => OpResult::Count(n),
            Err(e) => err(e),
        }
    }

    fn is_empty(&mut self, db: u8) -> OpResult {
        let db = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let rtxn = match self.read_source() {
            Ok(t) => t,
            Err(r) => return r,
        };
        match db.is_empty(rtxn) {
            Ok(b) => OpResult::Bool(b),
            Err(e) => err(e),
        }
    }

    fn first(&mut self, db: u8) -> OpResult {
        let db = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let rtxn = match self.read_source() {
            Ok(t) => t,
            Err(r) => return r,
        };
        match db.first(rtxn) {
            Ok(e) => OpResult::MaybeEntry(e.map(|(k, v)| (k.to_vec(), v.to_vec()))),
            Err(e) => err(e),
        }
    }

    fn last(&mut self, db: u8) -> OpResult {
        let db = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let rtxn = match self.read_source() {
            Ok(t) => t,
            Err(r) => return r,
        };
        match db.last(rtxn) {
            Ok(e) => OpResult::MaybeEntry(e.map(|(k, v)| (k.to_vec(), v.to_vec()))),
            Err(e) => err(e),
        }
    }

    fn seek(&mut self, db: u8, key: &[u8], kind: Seek) -> OpResult {
        let db = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let rtxn = match self.read_source() {
            Ok(t) => t,
            Err(r) => return r,
        };
        let res = match kind {
            Seek::Ge => db.get_greater_than_or_equal_to(rtxn, key),
            Seek::Gt => db.get_greater_than(rtxn, key),
            Seek::Le => db.get_lower_than_or_equal_to(rtxn, key),
        };
        match res {
            Ok(e) => OpResult::MaybeEntry(e.map(|(k, v)| (k.to_vec(), v.to_vec()))),
            Err(e) => err(e),
        }
    }

    fn iter(&mut self, db: u8, rev: bool) -> OpResult {
        let db = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let rtxn = match self.read_source() {
            Ok(t) => t,
            Err(r) => return r,
        };
        let mut out = Vec::new();
        if rev {
            let it = match db.rev_iter(rtxn) {
                Ok(it) => it,
                Err(e) => return err(e),
            };
            for item in it {
                match item {
                    Ok((k, v)) => out.push((k.to_vec(), v.to_vec())),
                    Err(e) => return err(e),
                }
            }
        } else {
            let it = match db.iter(rtxn) {
                Ok(it) => it,
                Err(e) => return err(e),
            };
            for item in it {
                match item {
                    Ok((k, v)) => out.push((k.to_vec(), v.to_vec())),
                    Err(e) => return err(e),
                }
            }
        }
        OpResult::Entries(out)
    }

    fn prefix_iter(&mut self, db: u8, prefix: &[u8], rev: bool) -> OpResult {
        let db = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let rtxn = match self.read_source() {
            Ok(t) => t,
            Err(r) => return r,
        };
        let mut out = Vec::new();
        if rev {
            let it = match db.rev_prefix_iter(rtxn, prefix) {
                Ok(it) => it,
                Err(e) => return err(e),
            };
            for item in it {
                match item {
                    Ok((k, v)) => out.push((k.to_vec(), v.to_vec())),
                    Err(e) => return err(e),
                }
            }
        } else {
            let it = match db.prefix_iter(rtxn, prefix) {
                Ok(it) => it,
                Err(e) => return err(e),
            };
            for item in it {
                match item {
                    Ok((k, v)) => out.push((k.to_vec(), v.to_vec())),
                    Err(e) => return err(e),
                }
            }
        }
        OpResult::Entries(out)
    }

    fn iter_mut_put(&mut self, db: u8, nth: u8, val: &[u8]) -> OpResult {
        let db = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let wtxn = match self.write_txn() {
            Ok(w) => w,
            Err(r) => return r,
        };
        let mut it = match db.iter_mut(wtxn) {
            Ok(it) => it,
            Err(e) => return err(e),
        };
        let target = nth as usize;
        let mut i = 0usize;
        // Collect the target key as owned bytes so no borrow into the entry
        // spans the unsafe `put_current` (SPEC 01 §S3).
        let mut key_owned: Option<Vec<u8>> = None;
        loop {
            match it.next() {
                Some(Ok((k, _v))) => {
                    if i == target {
                        key_owned = Some(k.to_vec());
                        break;
                    }
                    i += 1;
                }
                Some(Err(e)) => return err(e),
                None => break,
            }
        }
        match key_owned {
            Some(k) => {
                // SAFETY: we hold no live borrow into the current entry (the
                // key was copied out and the borrowing `next()` result dropped);
                // `k` is owned and equal to the current key, as `put_current`
                // requires.
                match unsafe { it.put_current(k.as_slice(), val) } {
                    Ok(b) => OpResult::Bool(b),
                    Err(e) => err(e),
                }
            }
            None => OpResult::Bool(false),
        }
    }

    fn iter_mut_del(&mut self, db: u8, nth: u8) -> OpResult {
        let db = match self.db_at(db) {
            Ok(d) => d,
            Err(r) => return r,
        };
        let wtxn = match self.write_txn() {
            Ok(w) => w,
            Err(r) => return r,
        };
        let mut it = match db.iter_mut(wtxn) {
            Ok(it) => it,
            Err(e) => return err(e),
        };
        let target = nth as usize;
        let mut i = 0usize;
        let mut positioned = false;
        loop {
            match it.next() {
                Some(Ok(_)) => {
                    if i == target {
                        positioned = true;
                        break;
                    }
                    i += 1;
                }
                Some(Err(e)) => return err(e),
                None => break,
            }
        }
        if !positioned {
            return OpResult::Bool(false);
        }
        // SAFETY: no live borrow into the current entry spans this call — the
        // last `next()` result was dropped before we reached here.
        match unsafe { it.del_current() } {
            Ok(b) => OpResult::Bool(b),
            Err(e) => err(e),
        }
    }

    fn verify_get(&mut self, db: u8, key: &[u8]) -> OpResult {
        let name = match self.db_name_at(db) {
            Some(n) => n,
            None => return OpResult::Skipped(Skip::NoDb),
        };
        let env = self.env();
        let rtxn = match env.read_txn() {
            Ok(t) => t,
            Err(e) => return err(e),
        };
        let dbh = match env.open_database::<Bytes, Bytes>(&rtxn, name.as_deref()) {
            Ok(Some(d)) => d,
            // Not yet committed (or dropped): nothing visible.
            Ok(None) => return OpResult::MaybeVal(None),
            Err(e) => return err(e),
        };
        match dbh.get(&rtxn, key) {
            Ok(v) => OpResult::MaybeVal(v.map(|v| v.to_vec())),
            Err(e) => err(e),
        }
    }
}
