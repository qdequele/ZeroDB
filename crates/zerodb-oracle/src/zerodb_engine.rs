//! The native [`Engine`] backed by the `zerodb` crate.
//!
//! ## Milestone scope (M1.3 — read path, differential)
//!
//! zerodb has a real **read** path (M1.3) but no **write** path yet (M1.4). This
//! engine bridges the gap with a **rebuild-world-on-commit** harness that makes
//! nearly the whole SPEC 00 op surface differential *now*:
//!
//! - **Writes** (`Put`/`PutFlagged`/`PutReserved`/`Del`/`ClearDb`) buffer into an
//!   in-memory `BTreeMap` **shadow** (`working`), a clone of the committed world
//!   taken at `BeginRw`. `APPEND`/`NoOverwrite` semantics are applied on the
//!   shadow per SPEC 01 §S1/§S2; key-size bounds per §S4.
//! - **Commit** promotes the shadow to `committed` and **materializes** it
//!   through the [`build_single_db_image`] loader into a fresh `zerodb.dat`,
//!   which is reopened so subsequent read txns exercise the **real B-tree read
//!   path** (M1.3). This is a legitimate read-path harness: it graduates to the
//!   real COW write path in M1.4, which will differential-test *against* this
//!   loader (PLAN §1.3).
//! - **Reads** in a read txn (and `VerifyGet`) go through the real zerodb tree
//!   over the committed file — the point of M1.3. Reads inside an open **write**
//!   txn are served from the shadow (they must see the txn's uncommitted state,
//!   which the committed file does not yet reflect).
//!
//! Scoped **out** of M1.3 (gated via [`Engine::implements`], so [`crate::run`]
//! skips them symmetrically on both engines): named databases and their catalog
//! (M1.6) — only the unnamed/main DB is differential; nested read txns (M1.9);
//! write-cursor `iter_mut` ops and `DropDb` (M1.4/M1.6). Because both sides skip
//! `CreateDb { Named }`, no named DB is ever created, so every `db` index
//! resolves to the one unnamed DB on both engines.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;

use zerodb_core::btree::prefix_successor;
use zerodb_core::builder::{build_single_db_image, DEFAULT_FILL_PERMILLE};

use crate::result::{OpResult, OracleError, Skip};
use crate::tempdir::TempDir;
use crate::{DbName, Engine, Op, PutFlag};

use zerodb::{Env, EnvOpenOptions, Error, MdbError};

/// Base map size (see [`crate::DIFF_MAP_SIZE`]); matches [`LmdbEngine`].
const BASE_MAP_SIZE: usize = crate::DIFF_MAP_SIZE;
/// DB page size for materialized files (Phase 1 exposes no selector; 4 KiB).
const PAGE_SIZE: u32 = 4096;
/// Catalog capacity (stored; consumed when named DBs land in M1.6).
const MAX_DBS: u32 = 16;
/// Fill factor for the bulk-load materializer.
const FILL: u32 = DEFAULT_FILL_PERMILLE;

/// One database's committed/working key→value state.
type DbMap = BTreeMap<Vec<u8>, Vec<u8>>;
/// The whole world: db-name (`None` = unnamed) → its map. M1.3 only ever holds
/// the unnamed DB.
type World = HashMap<Option<String>, DbMap>;

/// Abstract transaction state (mirrors [`crate::driver::TxnState`]). Nested
/// readers never occur (gated out), so there is no `RwNested`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Txn {
    None,
    Rw,
    Ro,
}

/// The native engine under differential test.
pub struct ZerodbEngine {
    dir: TempDir,
    /// Open over the materialized committed world. `None` only briefly during a
    /// reopen/materialize.
    env: Option<Env>,
    map_size: usize,
    /// Logical committed state (mirror of the materialized file).
    committed: World,
    /// A clone of `committed` taken at `BeginRw`; mutated by writes, promoted on
    /// commit, dropped on abort.
    working: Option<World>,
    /// Open database names in creation order (mirrors [`LmdbEngine`]); tracks
    /// uncommitted creations for `db_at` resolution and abort rollback.
    dbs: Vec<Option<String>>,
    /// Databases known committed (survive an abort / are re-openable after a
    /// reopen).
    committed_dbs: usize,
    /// Whether the current write txn has executed a `ClearDb` (FORK-1 guard —
    /// see `driver::classify`). Reset at every txn boundary.
    cleared_in_txn: bool,
    /// Monotonic txnid stamped into each materialized image.
    commit_txnid: u64,
    txn: Txn,
}

impl ZerodbEngine {
    fn open(dir: &std::path::Path, map_size: usize) -> Result<Env, Error> {
        let mut opts = EnvOpenOptions::new();
        opts.map_size(map_size);
        opts.max_dbs(MAX_DBS);
        opts.page_size(PAGE_SIZE);
        opts.open(dir)
    }

    fn desired_map_size(&self, kib: u16) -> usize {
        let want = BASE_MAP_SIZE + (kib as usize) * 4096;
        crate::round_map_size(want.max(self.map_size))
    }

    fn txn_state(&self) -> crate::driver::TxnState {
        use crate::driver::TxnState;
        match self.txn {
            Txn::None => TxnState::None,
            Txn::Rw => TxnState::Rw,
            Txn::Ro => TxnState::Ro,
        }
    }

    /// Resolve a `db` index (modulo the number of open dbs) to its catalog name.
    fn db_name_at(&self, db: u8) -> Option<String> {
        // The driver gates `dbs.is_empty()`; only the unnamed DB is ever open.
        let n = self.dbs.len();
        self.dbs[db as usize % n].clone()
    }

    /// The committed data map for `name`.
    fn committed_map(&self, name: &Option<String>) -> Option<&DbMap> {
        self.committed.get(name)
    }

    /// The working (uncommitted) data map for `name`.
    fn working_map(&self, name: &Option<String>) -> Option<&DbMap> {
        self.working.as_ref().and_then(|w| w.get(name))
    }

    /// The mutable working map for `name`, creating it if the db exists in the
    /// working set.
    fn working_map_mut(&mut self, name: &Option<String>) -> &mut DbMap {
        self.working
            .as_mut()
            .expect("write op requires an active write txn")
            .entry(name.clone())
            .or_default()
    }

    // -- reopen / materialize ---------------------------------------------

    fn reopen(&mut self, kib: u16) -> OpResult {
        // Reopen drops all txns and db handles (LMDB parity); committed *data*
        // persists in the file. Uncommitted work (an open write txn) is lost.
        self.txn = Txn::None;
        self.working = None;
        self.dbs.clear();
        self.committed_dbs = 0;
        self.cleared_in_txn = false;
        self.env = None;

        let new_size = self.desired_map_size(kib);
        match Self::open(self.dir.path(), new_size) {
            Ok(env) => {
                self.env = Some(env);
                self.map_size = new_size;
                OpResult::Ok
            }
            Err(e) => {
                if let Ok(env) = Self::open(self.dir.path(), self.map_size) {
                    self.env = Some(env);
                }
                OpResult::Err(to_oracle(e))
            }
        }
    }

    /// Rebuild `zerodb.dat` from the committed world's unnamed DB and reopen it,
    /// so later read txns exercise the real B-tree read path.
    fn materialize(&mut self) {
        self.env = None; // free the path (registry) and unmap before rewriting
        let entries: Vec<(Vec<u8>, Vec<u8>)> = self
            .committed
            .get(&None)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        let image = build_single_db_image(
            PAGE_SIZE,
            self.map_size as u64,
            self.commit_txnid,
            &entries,
            FILL,
        )
        .expect("materialize: builder never fails for validated key/value bounds");
        self.commit_txnid += 1;
        let data_path = self.dir.path().join(zerodb::DATA_FILE_NAME);
        std::fs::write(&data_path, &image).expect("materialize: write zerodb.dat");
        let env = Self::open(self.dir.path(), self.map_size).expect("materialize: reopen env");
        self.env = Some(env);
    }

    // -- transactions ------------------------------------------------------

    fn begin_rw(&mut self) -> OpResult {
        self.cleared_in_txn = false;
        self.working = Some(self.committed.clone());
        self.txn = Txn::Rw;
        OpResult::Ok
    }

    fn begin_ro(&mut self) -> OpResult {
        self.txn = Txn::Ro;
        OpResult::Ok
    }

    fn commit(&mut self) -> OpResult {
        self.cleared_in_txn = false;
        match self.txn {
            Txn::Rw => {
                let working = self.working.take().expect("rw has a working set");
                self.committed = working;
                self.committed_dbs = self.dbs.len();
                self.txn = Txn::None;
                self.materialize();
                OpResult::Ok
            }
            Txn::Ro => {
                self.txn = Txn::None;
                OpResult::Ok
            }
            Txn::None => OpResult::Skipped(Skip::NoTxn),
        }
    }

    fn abort(&mut self) -> OpResult {
        self.cleared_in_txn = false;
        match self.txn {
            Txn::Rw => {
                self.working = None;
                // Roll back databases created during this txn.
                self.dbs.truncate(self.committed_dbs);
                self.txn = Txn::None;
                OpResult::Ok
            }
            Txn::Ro => {
                self.txn = Txn::None;
                OpResult::Ok
            }
            Txn::None => OpResult::Skipped(Skip::NoTxn),
        }
    }

    fn create_db(&mut self, name: &DbName) -> OpResult {
        // Only the unnamed DB is implemented (named gated out); `name` is always
        // `Unnamed` here.
        let resolved = name.resolve();
        if !self.dbs.iter().any(|n| n == &resolved) {
            self.dbs.push(resolved.clone());
        }
        // Ensure the working set has the db (created empty within this txn).
        self.working
            .as_mut()
            .expect("create_db requires a write txn (driver-gated)")
            .entry(resolved)
            .or_default();
        OpResult::Ok
    }

    fn clear_db(&mut self, db: u8) -> OpResult {
        self.cleared_in_txn = true; // FORK-1 guard fact
        let name = self.db_name_at(db);
        self.working_map_mut(&name).clear();
        OpResult::Ok
    }

    // -- writes (shadow) ---------------------------------------------------

    fn put(&mut self, db: u8, key: &[u8], val: &[u8]) -> OpResult {
        if let Some(e) = bad_write_key(key) {
            return e;
        }
        let name = self.db_name_at(db);
        self.working_map_mut(&name)
            .insert(key.to_vec(), val.to_vec());
        OpResult::Ok
    }

    fn put_flagged(&mut self, db: u8, key: &[u8], val: &[u8], flag: PutFlag) -> OpResult {
        if let Some(e) = bad_write_key(key) {
            return e;
        }
        let name = self.db_name_at(db);
        let map = self.working_map_mut(&name);
        match flag {
            PutFlag::Append => {
                // SPEC 01 §S1: key must be strictly greater than the current
                // last key, else KeyExist (equal included).
                if let Some((last, _)) = map.iter().next_back() {
                    if key <= last.as_slice() {
                        return OpResult::Err(OracleError::KeyExist);
                    }
                }
                map.insert(key.to_vec(), val.to_vec());
                OpResult::Ok
            }
            PutFlag::NoOverwrite => {
                // SPEC 01 §S2: insert only if absent, else KeyExist.
                if map.contains_key(key) {
                    return OpResult::Err(OracleError::KeyExist);
                }
                map.insert(key.to_vec(), val.to_vec());
                OpResult::Ok
            }
        }
    }

    fn put_reserved(&mut self, db: u8, key: &[u8], val: &[u8]) -> OpResult {
        // `put_reserved` with no flags == plain overwrite (the value is written
        // into the reserved space).
        self.put(db, key, val)
    }

    fn del(&mut self, db: u8, key: &[u8]) -> OpResult {
        // `del` does not validate maxkey up front (unlike `put`): it searches
        // like `get`, so an empty key → `BadValSize` but an **oversized** key
        // simply finds nothing → `Ok(false)` (observed via the oracle).
        if let Some(e) = bad_read_key(key) {
            return e;
        }
        let name = self.db_name_at(db);
        let existed = self.working_map_mut(&name).remove(key).is_some();
        OpResult::Bool(existed)
    }

    // -- reads -------------------------------------------------------------

    /// Whether reads should be served from the working shadow (open write txn)
    /// rather than the committed real tree.
    fn read_from_shadow(&self) -> bool {
        self.txn == Txn::Rw
    }

    fn len(&self, db: u8) -> OpResult {
        let name = self.db_name_at(db);
        let n = if self.read_from_shadow() {
            self.working_map(&name).map_or(0, BTreeMap::len)
        } else {
            self.committed_map(&name).map_or(0, BTreeMap::len)
        };
        OpResult::Count(n as u64)
    }

    fn is_empty(&self, db: u8) -> OpResult {
        match self.len(db) {
            OpResult::Count(n) => OpResult::Bool(n == 0),
            other => other,
        }
    }

    fn get(&self, db: u8, key: &[u8]) -> OpResult {
        if let Some(e) = bad_read_key(key) {
            return e;
        }
        let name = self.db_name_at(db);
        if self.read_from_shadow() {
            let v = self.working_map(&name).and_then(|m| m.get(key)).cloned();
            OpResult::MaybeVal(v)
        } else {
            self.real_get(&name, key)
        }
    }

    fn first(&self, db: u8) -> OpResult {
        let name = self.db_name_at(db);
        if self.read_from_shadow() {
            let e = self
                .working_map(&name)
                .and_then(|m| m.iter().next())
                .map(clone_kv);
            OpResult::MaybeEntry(e)
        } else {
            self.real_first_last(&name, false)
        }
    }

    fn last(&self, db: u8) -> OpResult {
        let name = self.db_name_at(db);
        if self.read_from_shadow() {
            let e = self
                .working_map(&name)
                .and_then(|m| m.iter().next_back())
                .map(clone_kv);
            OpResult::MaybeEntry(e)
        } else {
            self.real_first_last(&name, true)
        }
    }

    fn seek(&self, db: u8, key: &[u8], kind: Seek) -> OpResult {
        if let Some(e) = bad_seek_key(key) {
            return e;
        }
        let name = self.db_name_at(db);
        if self.read_from_shadow() {
            let map = match self.working_map(&name) {
                Some(m) => m,
                None => return OpResult::MaybeEntry(None),
            };
            let e = match kind {
                Seek::Ge => map
                    .range::<[u8], _>((Bound::Included(key), Bound::Unbounded))
                    .next()
                    .map(clone_kv),
                Seek::Gt => map
                    .range::<[u8], _>((Bound::Excluded(key), Bound::Unbounded))
                    .next()
                    .map(clone_kv),
                Seek::Le => map
                    .range::<[u8], _>((Bound::Unbounded, Bound::Included(key)))
                    .next_back()
                    .map(clone_kv),
            };
            OpResult::MaybeEntry(e)
        } else {
            self.real_seek(&name, key, kind)
        }
    }

    fn iter(&self, db: u8, rev: bool) -> OpResult {
        let name = self.db_name_at(db);
        if self.read_from_shadow() {
            let mut out: Vec<(Vec<u8>, Vec<u8>)> = self
                .working_map(&name)
                .map(|m| m.iter().map(clone_kv).collect())
                .unwrap_or_default();
            if rev {
                out.reverse();
            }
            OpResult::Entries(out)
        } else {
            self.real_iter(&name, rev)
        }
    }

    fn prefix_iter(&self, db: u8, prefix: &[u8], rev: bool) -> OpResult {
        if let Some(e) = bad_prefix_key(prefix, rev) {
            return e;
        }
        let name = self.db_name_at(db);
        if self.read_from_shadow() {
            let map = match self.working_map(&name) {
                Some(m) => m,
                None => return OpResult::Entries(Vec::new()),
            };
            let hi: Bound<Vec<u8>> = match prefix_successor(prefix) {
                Some(s) => Bound::Excluded(s),
                None => Bound::Unbounded,
            };
            let lo: Bound<&[u8]> = Bound::Included(prefix);
            let hi_ref: Bound<&[u8]> = match &hi {
                Bound::Unbounded => Bound::Unbounded,
                Bound::Excluded(s) => Bound::Excluded(s.as_slice()),
                Bound::Included(s) => Bound::Included(s.as_slice()),
            };
            let mut out: Vec<(Vec<u8>, Vec<u8>)> =
                map.range::<[u8], _>((lo, hi_ref)).map(clone_kv).collect();
            if rev {
                out.reverse();
            }
            OpResult::Entries(out)
        } else {
            self.real_prefix_iter(&name, prefix, rev)
        }
    }

    fn verify_get(&self, db: u8, key: &[u8]) -> OpResult {
        // A fresh independent read: always the committed real tree, regardless
        // of any active txn.
        if let Some(e) = bad_read_key(key) {
            return e;
        }
        let name = self.db_name_at(db);
        self.real_get(&name, key)
    }

    // -- real read path (committed file) ----------------------------------

    fn real_get(&self, name: &Option<String>, key: &[u8]) -> OpResult {
        debug_assert!(name.is_none(), "M1.3 only reads the unnamed DB");
        let env = self.env.as_ref().expect("env present");
        let rtxn = match env.read_txn() {
            Ok(t) => t,
            Err(e) => return OpResult::Err(to_oracle(e)),
        };
        match env.main_database().get(&rtxn, key) {
            Ok(v) => OpResult::MaybeVal(v.map(<[u8]>::to_vec)),
            Err(e) => OpResult::Err(to_oracle(e)),
        }
    }

    fn real_first_last(&self, name: &Option<String>, last: bool) -> OpResult {
        debug_assert!(name.is_none());
        let env = self.env.as_ref().expect("env present");
        let rtxn = match env.read_txn() {
            Ok(t) => t,
            Err(e) => return OpResult::Err(to_oracle(e)),
        };
        let db = env.main_database();
        let r = if last {
            db.last(&rtxn)
        } else {
            db.first(&rtxn)
        };
        match r {
            Ok(e) => OpResult::MaybeEntry(e.map(|(k, v)| (k.to_vec(), v.to_vec()))),
            Err(e) => OpResult::Err(to_oracle(e)),
        }
    }

    fn real_seek(&self, name: &Option<String>, key: &[u8], kind: Seek) -> OpResult {
        debug_assert!(name.is_none());
        let env = self.env.as_ref().expect("env present");
        let rtxn = match env.read_txn() {
            Ok(t) => t,
            Err(e) => return OpResult::Err(to_oracle(e)),
        };
        let db = env.main_database();
        let r = match kind {
            Seek::Ge => db.get_greater_than_or_equal_to(&rtxn, key),
            Seek::Gt => db.get_greater_than(&rtxn, key),
            Seek::Le => db.get_lower_than_or_equal_to(&rtxn, key),
        };
        match r {
            Ok(e) => OpResult::MaybeEntry(e.map(|(k, v)| (k.to_vec(), v.to_vec()))),
            Err(e) => OpResult::Err(to_oracle(e)),
        }
    }

    fn real_iter(&self, name: &Option<String>, rev: bool) -> OpResult {
        debug_assert!(name.is_none());
        let env = self.env.as_ref().expect("env present");
        let rtxn = match env.read_txn() {
            Ok(t) => t,
            Err(e) => return OpResult::Err(to_oracle(e)),
        };
        let db = env.main_database();
        let mut out = Vec::new();
        if rev {
            for item in db.rev_iter(&rtxn) {
                match item {
                    Ok((k, v)) => out.push((k.to_vec(), v.to_vec())),
                    Err(e) => return OpResult::Err(to_oracle(e)),
                }
            }
        } else {
            for item in db.iter(&rtxn) {
                match item {
                    Ok((k, v)) => out.push((k.to_vec(), v.to_vec())),
                    Err(e) => return OpResult::Err(to_oracle(e)),
                }
            }
        }
        OpResult::Entries(out)
    }

    fn real_prefix_iter(&self, name: &Option<String>, prefix: &[u8], rev: bool) -> OpResult {
        debug_assert!(name.is_none());
        let env = self.env.as_ref().expect("env present");
        let rtxn = match env.read_txn() {
            Ok(t) => t,
            Err(e) => return OpResult::Err(to_oracle(e)),
        };
        let db = env.main_database();
        let mut out = Vec::new();
        if rev {
            for item in db.rev_prefix_iter(&rtxn, prefix) {
                match item {
                    Ok((k, v)) => out.push((k.to_vec(), v.to_vec())),
                    Err(e) => return OpResult::Err(to_oracle(e)),
                }
            }
        } else {
            for item in db.prefix_iter(&rtxn, prefix) {
                match item {
                    Ok((k, v)) => out.push((k.to_vec(), v.to_vec())),
                    Err(e) => return OpResult::Err(to_oracle(e)),
                }
            }
        }
        OpResult::Entries(out)
    }
}

/// Neighbor-seek variants.
enum Seek {
    Ge,
    Gt,
    Le,
}

fn clone_kv((k, v): (&Vec<u8>, &Vec<u8>)) -> (Vec<u8>, Vec<u8>) {
    (k.clone(), v.clone())
}

const BAD: Option<OpResult> = Some(OpResult::Err(OracleError::BadValSize));

/// Write-key size validation (SPEC 01 §S4, confirmed by `key_bounds.rs`): empty
/// **or** `> 511` bytes → `BadValSize`. Used by `put`/`del`/…
fn bad_write_key(key: &[u8]) -> Option<OpResult> {
    if key.is_empty() || key.len() > 511 {
        BAD
    } else {
        None
    }
}

/// Exact **read**-key size validation (observed via the oracle,
/// `read_differential.rs`): an **empty** key → `BadValSize`, but an
/// **oversized** key is *not* rejected — `mdb_get` simply finds nothing and
/// returns `Ok(None)`. Used by `get`/`set`/`verify_get`.
fn bad_read_key(key: &[u8]) -> Option<OpResult> {
    if key.is_empty() {
        BAD
    } else {
        None
    }
}

/// Prefix-key size validation (observed via the oracle). Direction-dependent,
/// because heed realizes the two directions with different seeks:
///  * **forward** `prefix_iter` does `MDB_SET_RANGE(prefix)`, so an **empty**
///    prefix → `BadValSize` (like any empty-key set-range);
///  * **reverse** `rev_prefix_iter` of an empty prefix seeks via `last` (its
///    successor is unbounded), so it works — full reverse iteration.
///
/// An **oversized** prefix is *not* rejected: `MDB_SET_RANGE` with a `> 511`
/// key returns nothing (no key can be `≥` it), so the scan is simply empty —
/// the same as any oversized set-range (see [`bad_seek_key`]).
fn bad_prefix_key(key: &[u8], rev: bool) -> Option<OpResult> {
    if key.is_empty() && !rev {
        BAD
    } else {
        None
    }
}

/// Seek-key size validation (`MDB_SET_RANGE`, observed via the oracle): only an
/// **empty** key → `BadValSize` (an explicit set-range with a zero-size key is
/// rejected). An **oversized** key is *not* an error — the search simply finds
/// nothing `≥` it and returns `None` (mirroring `get`). Used by `set_range` /
/// `get_greater_than` / `get_lower_than_or_equal_to`.
fn bad_seek_key(key: &[u8]) -> Option<OpResult> {
    if key.is_empty() {
        BAD
    } else {
        None
    }
}

/// Map a `zerodb::Error` into the oracle's normalized taxonomy (matching
/// [`LmdbEngine`]'s mapping so error kinds compare equal across engines).
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
            dir,
            env: Some(env),
            map_size: BASE_MAP_SIZE,
            committed: World::new(),
            working: None,
            dbs: Vec::new(),
            committed_dbs: 0,
            cleared_in_txn: false,
            commit_txnid: 1,
            txn: Txn::None,
        }
    }

    fn name(&self) -> &'static str {
        "zerodb"
    }

    fn implements(&self, op: &Op) -> bool {
        match op {
            // Out of M1.3 scope, gated symmetrically (see the module docs):
            //  * named DBs + their catalog (M1.6) — no named DB is ever created,
            //    so every `db` index resolves to the unnamed DB on both engines;
            //  * nested read txns (M1.9);
            //  * write-cursor `iter_mut` ops + `DropDb` (M1.4 / M1.6).
            Op::CreateDb { name } => matches!(name, DbName::Unnamed),
            Op::BeginNestedRo
            | Op::EndNestedRo
            | Op::DropDb { .. }
            | Op::IterMutPutCurrent { .. }
            | Op::IterMutDelCurrent { .. } => false,
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

            Op::First { db } => self.first(*db),
            Op::Last { db } => self.last(*db),
            Op::SetExact { db, key } => self.get(*db, &key.0),
            Op::SetRange { db, key } => self.seek(*db, &key.0, Seek::Ge),
            Op::GetGreaterThan { db, key } => self.seek(*db, &key.0, Seek::Gt),
            Op::GetLowerThanOrEqualTo { db, key } => self.seek(*db, &key.0, Seek::Le),

            Op::Iter { db } => self.iter(*db, false),
            Op::RevIter { db } => self.iter(*db, true),
            Op::PrefixIter { db, prefix } => self.prefix_iter(*db, &prefix.0, false),
            Op::RevPrefixIter { db, prefix } => self.prefix_iter(*db, &prefix.0, true),

            Op::VerifyGet { db, key } => self.verify_get(*db, &key.0),

            // Gated out by `implements`; never reached in a differential run.
            Op::BeginNestedRo
            | Op::EndNestedRo
            | Op::DropDb { .. }
            | Op::IterMutPutCurrent { .. }
            | Op::IterMutDelCurrent { .. } => OpResult::Skipped(Skip::NotImplemented),
        }
    }
}
