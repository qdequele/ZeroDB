//! The reference [`Engine`] backed by heed =0.22.1 (the Meilisearch LMDB fork).
//!
//! See ADR-0001 for why this crate — and only this crate — links LMDB.
//!
//! ## Self-referential transaction state (the one unsafe spot)
//!
//! heed's `RwTxn<'p>` / `RoTxn<'e>` borrow the `Env`, and a nested read txn
//! borrows its parent `RwTxn`. To hold an *open* transaction across separate
//! [`Engine::apply`] calls, this engine stores those transactions next to the
//! `Env` in one struct — a self-referential shape safe Rust cannot express
//! without a helper crate (none are on the allowlist). We therefore extend the
//! transaction lifetimes to `'static` with `mem::transmute` and uphold the
//! borrows manually. The invariants are stated at each `unsafe` site; the whole
//! construction is confined to this test-only oracle crate, exactly where the
//! CLAUDE.md unsafe policy permits FFI-adjacent unsafe.

use std::ops::Deref;

use heed::types::Bytes;
use heed::{Database, Env, EnvOpenOptions, RoTxn, RwTxn, WithoutTls};

use crate::result::{OpResult, OracleError, Skip};
use crate::tempdir::TempDir;
use crate::{DbName, Engine, Op, PutFlag};

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
}

/// The currently-open transaction, if any.
///
/// Variant field order matters for `Drop`: fields drop in declaration order, so
/// in [`Active::RwNested`] the child `nested` is aborted before the parent
/// `wtxn`, and in every case the transaction is dropped before the `Env` (which
/// is a later field of [`LmdbEngine`]).
enum Active {
    None,
    Rw(RwTxn<'static>),
    Ro(RoTxn<'static, WithoutTls>),
    RwNested {
        nested: RoTxn<'static, WithoutTls>,
        wtxn: RwTxn<'static>,
    },
}

/// The reference engine.
///
/// Field order is load-bearing for `Drop`: `active` (transactions) before `env`
/// before `dir`, so transactions close, then the env closes, then the backing
/// directory is removed.
pub struct LmdbEngine {
    active: Active,
    dbs: Vec<DbEntry>,
    /// Number of databases known to be committed (survive an abort).
    committed_dbs: usize,
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
    dir: TempDir,
}

impl LmdbEngine {
    fn env(&self) -> &Env<WithoutTls> {
        self.env
            .as_ref()
            .expect("env is always present between ops")
    }

    /// The current read source: the nested reader if one is open, otherwise the
    /// active read or write txn.
    fn read_source(&self) -> Result<&RoTxn<'static, heed::AnyTls>, OpResult> {
        match &self.active {
            Active::RwNested { nested, .. } => Ok(nested.deref()),
            Active::Ro(r) => Ok(r.deref()),
            Active::Rw(w) => Ok(w.deref().deref()),
            Active::None => Err(OpResult::Skipped(Skip::NoTxn)),
        }
    }

    /// The active write txn, or a structured skip explaining why there isn't one.
    fn write_txn(&mut self) -> Result<&mut RwTxn<'static>, OpResult> {
        match &mut self.active {
            Active::Rw(w) => Ok(w),
            Active::RwNested { .. } => Err(OpResult::Skipped(Skip::WriteBlockedByNested)),
            Active::Ro(_) | Active::None => Err(OpResult::Skipped(Skip::NoWriteTxn)),
        }
    }

    /// Resolve a db index (modulo the number of open dbs) to a handle.
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

fn open_env(dir: &std::path::Path, map_size: usize) -> heed::Result<Env<WithoutTls>> {
    let mut opts = EnvOpenOptions::new().read_txn_without_tls();
    opts.map_size(map_size);
    opts.max_dbs(MAX_DBS);
    // SAFETY: `open` is `unsafe` only because LMDB env flags can enable
    // cross-process behaviors; we pass none, and the path is a private temp dir
    // used single-threaded by this engine instance.
    unsafe { opts.open(dir) }
}

/// Map a heed error into the oracle's normalized taxonomy.
fn to_oracle(e: heed::Error) -> OracleError {
    match e {
        heed::Error::Mdb(m) => match m {
            heed::MdbError::NotFound => OracleError::NotFound,
            heed::MdbError::KeyExist => OracleError::KeyExist,
            heed::MdbError::MapFull => OracleError::MapFull,
            heed::MdbError::BadValSize => OracleError::BadValSize,
            heed::MdbError::Invalid => OracleError::Invalid,
            other => OracleError::Other(format!("mdb:{other:?}")),
        },
        heed::Error::Io(io) => OracleError::Other(format!("io:{}", io.kind())),
        heed::Error::Encoding(_) => OracleError::Other("encoding".into()),
        heed::Error::Decoding(_) => OracleError::Other("decoding".into()),
        heed::Error::EnvAlreadyOpened => OracleError::Other("env-already-opened".into()),
    }
}

fn err(e: heed::Error) -> OpResult {
    OpResult::Err(to_oracle(e))
}

impl Engine for LmdbEngine {
    fn new() -> Self {
        let dir = TempDir::new().expect("create temp dir");
        let env = open_env(dir.path(), BASE_MAP_SIZE).expect("open env");
        LmdbEngine {
            active: Active::None,
            dbs: Vec::new(),
            committed_dbs: 0,
            env: Some(Box::new(env)),
            map_size: BASE_MAP_SIZE,
            poisoned: None,
            cleared_in_txn: false,
            dir,
        }
    }

    fn name(&self) -> &'static str {
        "lmdb"
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

impl LmdbEngine {
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
        self.committed_dbs = 0;
        drop(self.env.take());

        let new_size = self.desired_map_size(kib);
        match open_env(self.dir.path(), new_size) {
            Ok(env) => {
                self.env = Some(Box::new(env));
                self.map_size = new_size;
                OpResult::Ok
            }
            Err(e) => {
                // Reopening at a non-shrinking size should not fail; if it
                // somehow does, restore the env at the previous size (which was
                // open moments ago) so the engine stays usable, and report.
                match open_env(self.dir.path(), self.map_size) {
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
        let outcome: heed::Result<RwTxn<'static>> = match self.env().write_txn() {
            Ok(txn) => Ok(unsafe { std::mem::transmute::<RwTxn<'_>, RwTxn<'static>>(txn) }),
            Err(e) => Err(e),
        };
        match outcome {
            Ok(txn) => {
                self.active = Active::Rw(txn);
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
        let outcome: heed::Result<RoTxn<'static, WithoutTls>> = match self.env().read_txn() {
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
            Active::Rw(w) => match w.commit() {
                Ok(()) => {
                    self.committed_dbs = self.dbs.len();
                    OpResult::Ok
                }
                Err(e) => {
                    // A failed mdb_txn_commit aborts the txn on the C side,
                    // closing dbis opened within it — same rollback as abort().
                    self.dbs.truncate(self.committed_dbs);
                    err(e)
                }
            },
            Active::RwNested { nested, wtxn } => {
                drop(nested);
                match wtxn.commit() {
                    Ok(()) => {
                        self.committed_dbs = self.dbs.len();
                        OpResult::Ok
                    }
                    Err(e) => {
                        self.dbs.truncate(self.committed_dbs);
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
                w.abort();
                // Databases created during this txn are rolled back.
                self.dbs.truncate(self.committed_dbs);
                OpResult::Ok
            }
            Active::RwNested { nested, wtxn } => {
                drop(nested);
                wtxn.abort();
                self.dbs.truncate(self.committed_dbs);
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
                // SAFETY: the nested reader's real pointers are the child
                // `MDB_txn` (owned by C, stable) and `Env::inner` (stable in the
                // Box); it stores no Rust pointer into `wtxn`, so extending the
                // lifetime and moving `wtxn` alongside it does not invalidate the
                // C parent link. `nested` is dropped before `wtxn` (field order),
                // and both before the env.
                let outcome: heed::Result<RoTxn<'static, WithoutTls>> = match wtxn.nested_read_txn()
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
                Active::Rw(w) => w,
                _ => unreachable!("write_txn() checked above"),
            };
            env.create_database::<Bytes, Bytes>(wtxn, name.as_deref())
        };
        match created {
            Ok(db) => {
                self.dbs.push(DbEntry { name, db });
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
        // SAFETY: `remove` invalidates the dbi for any other handle to the same
        // database; this engine keeps exactly one handle per catalog entry, and
        // we drop that entry immediately below, so no stale handle survives.
        let res = unsafe { handle.remove(wtxn) };
        match res {
            Ok(()) => {
                self.dbs.remove(idx);
                if self.committed_dbs > self.dbs.len() {
                    self.committed_dbs = self.dbs.len();
                }
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
            PutFlag::Append => heed::PutFlags::APPEND,
            PutFlag::NoOverwrite => heed::PutFlags::NO_OVERWRITE,
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
