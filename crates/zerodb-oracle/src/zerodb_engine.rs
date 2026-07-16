//! The native [`Engine`] backed by the `zerodb` crate.
//!
//! ## Milestone scope (M1.4 — real write path)
//!
//! The M1.3 rebuild-world-on-commit shadow is **gone**: every write op runs
//! zerodb's real COW write path (`RwTxn`), `Commit` runs the real C0–C6 commit
//! pipeline, and reads inside a write txn go through the dirty-frame source
//! (SPEC 04 TXN-38). The `iter_mut` cursor-mutation ops (`put_current` /
//! `del_current`) are differential from this milestone on.
//!
//! In debug builds (which includes `cargo fuzz`'s default), every successful
//! commit re-reads `zerodb.dat` and runs the SPEC 03 §11 invariant walk
//! ([`zerodb::check::check_image`]); a violation panics with the INV ids —
//! PLAN §1.4's "tree invariant checker passes after every fuzz run".
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
//! Scoped **out** (gated via [`Engine::implements`], symmetric on both
//! engines): named databases + `DropDb` (M1.6), nested read txns (M1.9).
//!
//! **Key-size taxonomy split** (SPEC 03 §2.1): zerodb-core's *write* path
//! validates keys itself (`put*` → `BadValSize`); the *read/del* leniency
//! differences are LMDB API-boundary behaviors applied here (empty read key →
//! `BadValSize`, etc.), exactly as in M1.3.

use zerodb::{check, Env, EnvOpenOptions, Error, MdbError, PutFlags, RoTxn, RwTxn};

use crate::result::{OpResult, OracleError, Skip};
use crate::tempdir::TempDir;
use crate::{DbName, Engine, Op, PutFlag};

/// Base map size (see [`crate::DIFF_MAP_SIZE`]); matches [`crate::LmdbEngine`].
const BASE_MAP_SIZE: usize = crate::DIFF_MAP_SIZE;
/// DB page size (Phase 1 exposes no heed selector; 4 KiB).
const PAGE_SIZE: u32 = 4096;
/// Catalog capacity (stored; consumed when named DBs land in M1.6).
const MAX_DBS: u32 = 16;

/// The currently-open transaction, if any.
///
/// Declared **before** `env` in [`ZerodbEngine`] so transactions drop before
/// the env (field declaration order = drop order).
enum Active {
    None,
    Rw(Box<RwTxn<'static>>),
    Ro(RoTxn<'static>),
}

/// The native engine under differential test.
///
/// Field order is load-bearing for `Drop`: `active` (transactions) before
/// `env` before `dir`.
pub struct ZerodbEngine {
    active: Active,
    /// Open database names in creation order (only the unnamed DB until M1.6).
    dbs: Vec<Option<String>>,
    /// Databases known committed (survive an abort).
    committed_dbs: usize,
    /// `Box` gives the `Env` a stable heap address that survives moves of the
    /// engine struct — what makes the `'static` txn borrows sound. Always
    /// `Some` between operations.
    env: Option<Box<Env>>,
    map_size: usize,
    /// FORK-1 guard fact (see `driver::classify`): set by `ClearDb`, reset at
    /// every txn boundary.
    cleared_in_txn: bool,
    dir: TempDir,
}

/// Run a read closure against whichever txn serves reads, or skip.
macro_rules! with_read {
    ($self:ident, |$t:ident, $db:ident| $body:expr) => {{
        let $db = $self.env().main_database();
        match &$self.active {
            Active::Rw(w) => {
                let $t = &**w;
                $body
            }
            Active::Ro(r) => {
                let $t = r;
                $body
            }
            Active::None => OpResult::Skipped(Skip::NoTxn),
        }
    }};
}

impl ZerodbEngine {
    fn open(dir: &std::path::Path, map_size: usize) -> Result<Env, Error> {
        let mut opts = EnvOpenOptions::new();
        opts.map_size(map_size);
        opts.max_dbs(MAX_DBS);
        opts.page_size(PAGE_SIZE);
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
        }
    }

    /// The active write txn, or the structured skip (unreachable after the
    /// driver gate; kept as a handle resolver like `LmdbEngine`).
    fn write_txn(&mut self) -> Result<&mut RwTxn<'static>, OpResult> {
        match &mut self.active {
            Active::Rw(w) => Ok(w),
            Active::Ro(_) | Active::None => Err(OpResult::Skipped(Skip::NoWriteTxn)),
        }
    }

    fn db_name_at(&self, db: u8) -> Option<String> {
        let n = self.dbs.len();
        self.dbs[db as usize % n].clone()
    }

    /// In debug builds, verify the committed on-disk image against the
    /// SPEC 03 §11 invariant walk (minus INV-10, the sanctioned M1.4 leak
    /// window — ADR-0004 OQ4).
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
        self.committed_dbs = 0;
        self.env = None;

        let new_size = self.desired_map_size(kib);
        match Self::open(self.dir.path(), new_size) {
            Ok(env) => {
                self.env = Some(Box::new(env));
                self.map_size = new_size;
                OpResult::Ok
            }
            Err(e) => {
                if let Ok(env) = Self::open(self.dir.path(), self.map_size) {
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
                self.active = Active::Ro(txn);
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
                    self.committed_dbs = self.dbs.len();
                    self.debug_check_image();
                    OpResult::Ok
                }
                Err(e) => {
                    // Failed commit = abort: roll back txn-created dbs
                    // (matches LmdbEngine).
                    self.dbs.truncate(self.committed_dbs);
                    OpResult::Err(to_oracle(e))
                }
            },
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
                self.dbs.truncate(self.committed_dbs);
                OpResult::Ok
            }
            Active::Ro(r) => {
                drop(r);
                OpResult::Ok
            }
        }
    }

    // -- databases -------------------------------------------------------------

    fn create_db(&mut self, name: &DbName) -> OpResult {
        // Only the unnamed DB is in scope (named gated out): the main DB
        // always exists in zerodb, so this only tracks the handle.
        if let Err(skip) = self.write_txn() {
            return skip;
        }
        let resolved = name.resolve();
        if !self.dbs.iter().any(|n| n == &resolved) {
            self.dbs.push(resolved);
        }
        OpResult::Ok
    }

    fn clear_db(&mut self, db: u8) -> OpResult {
        self.cleared_in_txn = true; // FORK-1 guard fact
        let _name = self.db_name_at(db);
        let dbh = self.env().main_database();
        let wtxn = match self.write_txn() {
            Ok(w) => w,
            Err(r) => return r,
        };
        match dbh.clear(wtxn) {
            Ok(()) => OpResult::Ok,
            Err(e) => OpResult::Err(to_oracle(e)),
        }
    }

    // -- writes ------------------------------------------------------------------

    fn put(&mut self, db: u8, key: &[u8], val: &[u8]) -> OpResult {
        let _name = self.db_name_at(db);
        let dbh = self.env().main_database();
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
        let _name = self.db_name_at(db);
        let flags = match flag {
            PutFlag::Append => PutFlags::APPEND,
            PutFlag::NoOverwrite => PutFlags::NO_OVERWRITE,
        };
        let dbh = self.env().main_database();
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
        let _name = self.db_name_at(db);
        let dbh = self.env().main_database();
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
        let _name = self.db_name_at(db);
        let dbh = self.env().main_database();
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
        let _name = self.db_name_at(db);
        let dbh = self.env().main_database();
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
        let _name = self.db_name_at(db);
        let dbh = self.env().main_database();
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
        let _name = self.db_name_at(db);
        with_read!(self, |t, dbh| match dbh.get(t, key) {
            Ok(v) => OpResult::MaybeVal(v.map(<[u8]>::to_vec)),
            Err(e) => OpResult::Err(to_oracle(e)),
        })
    }

    fn len(&self, db: u8) -> OpResult {
        let _name = self.db_name_at(db);
        with_read!(self, |t, dbh| match dbh.len(t) {
            Ok(n) => OpResult::Count(n),
            Err(e) => OpResult::Err(to_oracle(e)),
        })
    }

    fn is_empty(&self, db: u8) -> OpResult {
        let _name = self.db_name_at(db);
        with_read!(self, |t, dbh| match dbh.is_empty(t) {
            Ok(b) => OpResult::Bool(b),
            Err(e) => OpResult::Err(to_oracle(e)),
        })
    }

    fn first_last(&self, db: u8, last: bool) -> OpResult {
        let _name = self.db_name_at(db);
        with_read!(self, |t, dbh| {
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
        let _name = self.db_name_at(db);
        with_read!(self, |t, dbh| {
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
        let _name = self.db_name_at(db);
        with_read!(self, |t, dbh| {
            let it = if rev { dbh.rev_iter(t) } else { dbh.iter(t) };
            collect_iter(it)
        })
    }

    fn prefix_iter(&self, db: u8, prefix: &[u8], rev: bool) -> OpResult {
        if let Some(e) = bad_prefix_key(prefix, rev) {
            return e;
        }
        let _name = self.db_name_at(db);
        with_read!(self, |t, dbh| {
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
        // of any active txn (readers never block on the writer, TXN-9).
        if let Some(e) = bad_read_key(key) {
            return e;
        }
        let _name = self.db_name_at(db);
        let env = self.env();
        let rtxn = match env.read_txn() {
            Ok(t) => t,
            Err(e) => return OpResult::Err(to_oracle(e)),
        };
        match env.main_database().get(&rtxn, key) {
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
/// [`crate::LmdbEngine`]'s mapping so error kinds compare equal).
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
        let dir = TempDir::new().expect("create temp dir");
        let env = ZerodbEngine::open(dir.path(), BASE_MAP_SIZE).expect("open zerodb env");
        ZerodbEngine {
            active: Active::None,
            dbs: Vec::new(),
            committed_dbs: 0,
            env: Some(Box::new(env)),
            map_size: BASE_MAP_SIZE,
            cleared_in_txn: false,
            dir,
        }
    }

    fn name(&self) -> &'static str {
        "zerodb"
    }

    fn implements(&self, op: &Op) -> bool {
        match op {
            // Out of scope, gated symmetrically: named DBs + DropDb (M1.6),
            // nested read txns (M1.9).
            Op::CreateDb { name } => matches!(name, DbName::Unnamed),
            Op::BeginNestedRo | Op::EndNestedRo | Op::DropDb { .. } => false,
            _ => true,
        }
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

            // Gated out by `implements`; never reached in a differential run.
            Op::BeginNestedRo | Op::EndNestedRo | Op::DropDb { .. } => {
                OpResult::Skipped(Skip::NotImplemented)
            }
        }
    }
}
