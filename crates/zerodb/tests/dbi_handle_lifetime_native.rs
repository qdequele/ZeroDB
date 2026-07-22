//! Native-API strictness tests for the dbi-handle lifetime (M2.9; SPEC 04
//! TXN-68, ADR-0013, D-013).
//!
//! A named [`zerodb::Database`] handle captures its dbi slot's generation and
//! dies exactly when LMDB would close the dbi: when the write txn that
//! **created** the DB ends without committing (TXN-59/60 — explicit abort,
//! plain drop, panic, failed commit), or immediately when `drop_db`
//! (`mdb_drop(_, 1)`) deletes it. A dead handle fails every operation with
//! [`MdbError::BadDbi`] — in write txns, read txns, and nested read txns —
//! without poisoning the transaction. `clear`, committed creates, and the
//! main/unnamed handle never invalidate. The heed-boundary observable
//! (`Io(EINVAL)`, fork parity) is pinned in
//! `zerodb-oracle/tests/dbi_handle_lifetime.rs`; these tests pin the richer
//! native taxonomy.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{Database, Env, EnvOpenOptions, Error, MdbError, RwTxn};

const MAP_SIZE: usize = 1024 * 1024;

// --- tiny self-cleaning temp dir (no tempfile dep on the allowlist) ---

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        // Relaxed: uniqueness only; the counter publishes no other memory.
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("zerodb-dbi-{pid}-{nanos}-{seq}"));
        std::fs::create_dir_all(&path).expect("create temp dir");
        TempDir { path }
    }
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn open_env(dir: &Path) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP_SIZE);
    opts.max_dbs(8);
    opts.open(dir).expect("open env")
}

#[track_caller]
fn assert_bad_dbi<T>(label: &str, r: Result<T, Error>) {
    match r {
        Err(Error::Mdb(MdbError::BadDbi)) => {}
        Err(other) => panic!("{label}: expected BadDbi, got {other:?}"),
        Ok(_) => panic!("{label}: expected BadDbi, got Ok"),
    }
}

/// Assert every op class refuses the stale handle `db` in fresh txns, and
/// that the errors are non-poisoning (each probing txn stays usable).
fn assert_dead_everywhere(env: &Env, db: Database, tag: &str) {
    // Read-txn classes (the dbi table is env-level: a read txn must refuse
    // a stale handle too).
    {
        let rtxn = env.read_txn().unwrap();
        assert_bad_dbi(&format!("{tag}/ro get"), db.get(&rtxn, b"k"));
        assert_bad_dbi(&format!("{tag}/ro len"), db.len(&rtxn));
        assert_bad_dbi(&format!("{tag}/ro is_empty"), db.is_empty(&rtxn));
        assert_bad_dbi(&format!("{tag}/ro stat"), db.stat(&rtxn));
        assert_bad_dbi(&format!("{tag}/ro first"), db.first(&rtxn));
        assert_bad_dbi(&format!("{tag}/ro last"), db.last(&rtxn));
        assert_bad_dbi(
            &format!("{tag}/ro set_range"),
            db.get_greater_than_or_equal_to(&rtxn, b"k"),
        );
        assert_bad_dbi(&format!("{tag}/ro gt"), db.get_greater_than(&rtxn, b"k"));
        assert_bad_dbi(
            &format!("{tag}/ro le"),
            db.get_lower_than_or_equal_to(&rtxn, b"k"),
        );
        assert_bad_dbi(&format!("{tag}/ro lt"), db.get_lower_than(&rtxn, b"k"));
        // Iterator constructors are infallible by signature: the BadDbi is
        // deferred to the first `next()` (SPEC 04 TXN-68 note; callers that
        // need error-at-open pre-check with `Database::validate`).
        assert_bad_dbi(
            &format!("{tag}/ro iter first item"),
            db.iter(&rtxn).next().expect("deferred error item"),
        );
        assert_bad_dbi(
            &format!("{tag}/ro rev_iter first item"),
            db.rev_iter(&rtxn).next().expect("deferred error item"),
        );
        assert_bad_dbi(&format!("{tag}/ro validate"), db.validate(&rtxn));
    }
    // Write-txn classes, then prove the txn is NOT poisoned.
    {
        let mut wtxn = env.write_txn().unwrap();
        assert_bad_dbi(&format!("{tag}/rw put"), db.put(&mut wtxn, b"k", b"v"));
        assert_bad_dbi(
            &format!("{tag}/rw put_reserved"),
            db.put_reserved(&mut wtxn, b"k", 3, |_| {}),
        );
        assert_bad_dbi(&format!("{tag}/rw del"), db.delete(&mut wtxn, b"k"));
        assert_bad_dbi(&format!("{tag}/rw clear"), db.clear(&mut wtxn));
        assert_bad_dbi(&format!("{tag}/rw drop"), db.drop_db(&mut wtxn));
        assert_bad_dbi(&format!("{tag}/rw get"), db.get(&wtxn, b"k"));
        assert_bad_dbi(&format!("{tag}/rw stat"), db.stat(&wtxn));
        assert_bad_dbi(
            &format!("{tag}/rw cursor first move"),
            db.rw_cursor(&mut wtxn).move_next(),
        );
        // Non-poisoning (probed fork parity: EINVAL leaves the txn
        // committable): unrelated work still lands.
        let main = env.main_database();
        main.put(&mut wtxn, b"alive", b"yes").unwrap();
        wtxn.commit().expect("BadDbi must not poison the txn");
    }
    let rtxn = env.read_txn().unwrap();
    let main = env.main_database();
    assert_eq!(main.get(&rtxn, b"alive").unwrap(), Some(&b"yes"[..]));
}

/// TXN-68 event 1: the creating txn ends without committing → handle dead.
#[test]
fn create_then_abort_kills_the_handle() {
    let dir = TempDir::new();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db = env.create_database(&mut wtxn, Some(b"a")).unwrap();
    wtxn.abort();
    assert_dead_everywhere(&env, db, "abort");
}

/// TXN-68 event 1, plain-drop shape (TXN-59: drop == abort).
#[test]
fn create_then_drop_kills_the_handle() {
    let dir = TempDir::new();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db = env.create_database(&mut wtxn, Some(b"a")).unwrap();
    drop(wtxn);
    assert_bad_dbi("plain drop", db.len(&env.read_txn().unwrap()));
}

/// TXN-68 event 1, panic shape (TXN-60: unwind == implicit abort).
#[test]
fn panic_while_txn_live_kills_created_handle() {
    let dir = TempDir::new();
    let env = open_env(dir.path());
    let db = {
        let db_slot = std::sync::Mutex::new(None);
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut wtxn = env.write_txn().unwrap();
            let db = env.create_database(&mut wtxn, Some(b"p")).unwrap();
            *db_slot.lock().unwrap() = Some(db);
            panic!("simulated consumer panic");
        }));
        assert!(r.is_err());
        db_slot.into_inner().unwrap().unwrap()
    };
    assert_bad_dbi("after panic", db.len(&env.read_txn().unwrap()));
}

/// Commit keeps a created handle valid — and it stays valid across LATER
/// unrelated aborts (only the creating txn's fate matters).
#[test]
fn commit_keeps_handle_valid_across_later_aborts() {
    let dir = TempDir::new();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db = env.create_database(&mut wtxn, Some(b"a")).unwrap();
    db.put(&mut wtxn, b"k", b"v").unwrap();
    wtxn.commit().unwrap();

    // An unrelated aborting txn that USES (but did not create) the handle.
    let mut wtxn = env.write_txn().unwrap();
    db.put(&mut wtxn, b"rolled-back", b"x").unwrap();
    wtxn.abort();

    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&rtxn, b"k").unwrap(), Some(&b"v"[..]));
    assert_eq!(db.get(&rtxn, b"rolled-back").unwrap(), None);
}

/// A create that merely opens an existing committed DB whose slot is already
/// **exported** does not make the txn the slot's opener: aborting it must NOT
/// kill the handle (TXN-68 — the `DB_NEW` analog tracks the opener of an
/// *unexported slot*, not catalog-entry creation).
#[test]
fn reopening_create_is_not_a_create_for_abort_purposes() {
    let dir = TempDir::new();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    env.create_database(&mut wtxn, Some(b"a")).unwrap();
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    let again = env.create_database(&mut wtxn, Some(b"a")).unwrap();
    wtxn.abort();
    let rtxn = env.read_txn().unwrap();
    assert_eq!(again.len(&rtxn).unwrap(), 0); // valid, not BadDbi
}

/// TXN-68 event 2: `drop_db(delete)` closes env-wide IMMEDIATELY — the same
/// txn can no longer use the handle, yet still commits (the successful drop
/// lands; the stale-use refusals are non-poisoning).
#[test]
fn drop_db_closes_immediately_in_the_same_txn() {
    let dir = TempDir::new();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db = env.create_database(&mut wtxn, Some(b"b")).unwrap();
    db.put(&mut wtxn, b"k", b"v").unwrap();
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    db.drop_db(&mut wtxn).unwrap();
    assert_bad_dbi("same-txn get", db.get(&wtxn, b"k"));
    assert_bad_dbi("same-txn put", db.put(&mut wtxn, b"k2", b"v2"));
    assert_bad_dbi("same-txn drop again", db.drop_db(&mut wtxn));
    wtxn.commit().expect("the successful drop still commits");

    let rtxn = env.read_txn().unwrap();
    assert!(env.open_database(&rtxn, Some(b"b")).unwrap().is_none());
}

/// The (c) asymmetry: drop + abort brings the DATA back (fresh open sees it)
/// but does NOT resurrect the handle; a fresh open yields a new valid handle
/// while the old one stays dead.
#[test]
fn drop_then_abort_data_back_handle_dead() {
    let dir = TempDir::new();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db = env.create_database(&mut wtxn, Some(b"c")).unwrap();
    db.put(&mut wtxn, b"k", b"v").unwrap();
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    db.drop_db(&mut wtxn).unwrap();
    wtxn.abort();

    assert_dead_everywhere(&env, db, "drop+abort");

    let rtxn = env.read_txn().unwrap();
    let fresh = env
        .open_database(&rtxn, Some(b"c"))
        .unwrap()
        .expect("abort restored the database");
    assert_eq!(fresh.get(&rtxn, b"k").unwrap(), Some(&b"v"[..]));
    // Old and fresh address the same dbi with different generations: the old
    // one is still refused in the very txn the fresh one works in.
    assert_bad_dbi("old handle, same txn as fresh", db.get(&rtxn, b"k"));
}

/// `clear` (`mdb_drop(_, 0)`) empties the DB but keeps the handle valid.
#[test]
fn clear_does_not_invalidate() {
    let dir = TempDir::new();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db = env.create_database(&mut wtxn, Some(b"a")).unwrap();
    db.put(&mut wtxn, b"k", b"v").unwrap();
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    db.clear(&mut wtxn).unwrap();
    db.put(&mut wtxn, b"k2", b"v2").unwrap(); // same txn, same handle: fine
    wtxn.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&rtxn, b"k").unwrap(), None);
    assert_eq!(db.get(&rtxn, b"k2").unwrap(), Some(&b"v2"[..]));
}

/// The main/unnamed handle never dies — `drop_db` on it is a clear (LMDB's
/// `MAIN_DBI` is a core dbi, always valid).
#[test]
fn main_database_handle_never_dies() {
    let dir = TempDir::new();
    let env = open_env(dir.path());
    let main = env.main_database();
    let mut wtxn = env.write_txn().unwrap();
    main.put(&mut wtxn, b"k", b"v").unwrap();
    main.drop_db(&mut wtxn).unwrap(); // = clear for the main DB
    main.put(&mut wtxn, b"k2", b"v2").unwrap();
    wtxn.abort(); // aborting never invalidates main either

    let rtxn = env.read_txn().unwrap();
    assert_eq!(main.get(&rtxn, b"k").unwrap(), None); // nothing committed
    let mut wtxn = env.write_txn().unwrap();
    main.put(&mut wtxn, b"still-works", b"1").unwrap();
    wtxn.commit().unwrap();
}

/// Re-creating a name after an aborted create yields a FRESH valid handle;
/// the old handle stays dead even though both carry the same dbi index (the
/// generation differs). This is where ZeroDB is deliberately stricter than
/// LMDB's slot-reuse resurrection (DIVERGENCES D-015, PROPOSED).
#[test]
fn recreate_yields_fresh_handle_old_stays_dead() {
    let dir = TempDir::new();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let old = env.create_database(&mut wtxn, Some(b"a")).unwrap();
    wtxn.abort();

    let mut wtxn = env.write_txn().unwrap();
    let fresh = env.create_database(&mut wtxn, Some(b"a")).unwrap();
    fresh.put(&mut wtxn, b"k", b"v").unwrap();
    // The dead handle is refused inside the very txn the fresh one works in.
    assert_bad_dbi("old in create txn", old.get(&wtxn, b"k"));
    wtxn.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    assert_eq!(fresh.get(&rtxn, b"k").unwrap(), Some(&b"v"[..]));
    assert_bad_dbi("old after commit", old.get(&rtxn, b"k"));
}

/// Same-txn drop + recreate: the handle from before the drop is dead, the
/// re-created one works — within one write txn.
#[test]
fn same_txn_drop_and_recreate() {
    let dir = TempDir::new();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let first = env.create_database(&mut wtxn, Some(b"x")).unwrap();
    first.put(&mut wtxn, b"k", b"v1").unwrap();
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    first.drop_db(&mut wtxn).unwrap();
    let second = env.create_database(&mut wtxn, Some(b"x")).unwrap();
    second.put(&mut wtxn, b"k", b"v2").unwrap();
    assert_bad_dbi("pre-drop handle", first.get(&wtxn, b"k"));
    wtxn.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    assert_eq!(second.get(&rtxn, b"k").unwrap(), Some(&b"v2"[..]));
    assert_bad_dbi("pre-drop handle after commit", first.get(&rtxn, b"k"));
}

/// A nested read txn shares the parent's dbi view (TXN-68 through §5): a
/// handle the parent txn dropped is refused through the child too.
#[test]
fn nested_reader_refuses_stale_handle() {
    let dir = TempDir::new();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db = env.create_database(&mut wtxn, Some(b"n")).unwrap();
    db.put(&mut wtxn, b"k", b"v").unwrap();
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    db.drop_db(&mut wtxn).unwrap();
    let child = wtxn.nested_read_txn().unwrap();
    assert_bad_dbi("nested get", db.get(&child, b"k"));
    assert_bad_dbi("nested validate", db.validate(&child));
    drop(child);
    wtxn.abort();
}

/// A long-lived READ txn binds a handle once and keeps that bind for its
/// whole life (per-txn-bind granularity, ADR-0013 Q2): an invalidation that
/// lands mid-read-txn is not observed by that txn, exactly like LMDB's
/// txn-local `mt_dbflags` copy; a txn opened after the bump refuses.
#[test]
fn read_txn_bind_is_stable_for_its_life() {
    let dir = TempDir::new();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db = env.create_database(&mut wtxn, Some(b"s")).unwrap();
    db.put(&mut wtxn, b"k", b"v").unwrap();
    wtxn.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&rtxn, b"k").unwrap(), Some(&b"v"[..])); // bind now

    // Writer drops the DB (bumps the generation) while rtxn is live.
    let mut wtxn = env.write_txn().unwrap();
    db.drop_db(&mut wtxn).unwrap();
    wtxn.commit().unwrap();

    // The already-bound read txn keeps serving its pinned snapshot ...
    assert_eq!(db.get(&rtxn, b"k").unwrap(), Some(&b"v"[..]));
    drop(rtxn);
    // ... but a txn opened after the bump refuses the handle.
    assert_bad_dbi(
        "fresh txn after bump",
        db.get(&env.read_txn().unwrap(), b"k"),
    );
}

/// The write-txn value of `fn(&mut RwTxn)` helpers still compiles/behaves for
/// a generic caller (regression guard for the `ensure_open(Database)`
/// signature change: consumer-visible types are unchanged).
#[test]
fn write_helpers_generic_shape_unchanged() {
    fn touch(env: &Env, db: Database, wtxn: &mut RwTxn<'_>) -> zerodb::Result<()> {
        let _ = env;
        db.put(wtxn, b"g", b"1")?;
        db.delete(wtxn, b"g")?;
        Ok(())
    }
    let dir = TempDir::new();
    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db = env.create_database(&mut wtxn, Some(b"g")).unwrap();
    touch(&env, db, &mut wtxn).unwrap();
    wtxn.commit().unwrap();
}

/// The `DB_NEW` analog is per **slot open**, not per catalog create: after
/// an env close+reopen every registry slot is fresh, so the first
/// `create_database` of a name already existing on disk makes that txn the
/// slot's opener — abort kills the handle; a committing open **exports** the
/// slot, after which re-opens are not openers and later aborts keep every
/// handle valid (SPEC 04 TXN-68; found by self-review during M2.9).
#[test]
fn reopen_env_open_existing_then_abort_kills_handle() {
    let dir = TempDir::new();
    {
        let env = open_env(dir.path());
        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database(&mut wtxn, Some(b"x")).unwrap();
        db.put(&mut wtxn, b"k", b"v").unwrap();
        wtxn.commit().unwrap();
    } // env closed: the registry (and every handle) dies with the process env

    let env = open_env(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db = env.create_database(&mut wtxn, Some(b"x")).unwrap();
    assert_eq!(db.get(&wtxn, b"k").unwrap(), Some(&b"v"[..]));
    wtxn.abort(); // DB_NEW slot: the handle dies with the abort
    assert_bad_dbi("after abort", db.get(&env.read_txn().unwrap(), b"k"));

    // A committing open exports the slot ...
    let mut wtxn = env.write_txn().unwrap();
    let exported = env.create_database(&mut wtxn, Some(b"x")).unwrap();
    wtxn.commit().unwrap();
    // ... so a later re-open is NOT an opener and an abort keeps both alive.
    let mut wtxn = env.write_txn().unwrap();
    let reopened = env.create_database(&mut wtxn, Some(b"x")).unwrap();
    wtxn.abort();
    let rtxn = env.read_txn().unwrap();
    assert_eq!(exported.get(&rtxn, b"k").unwrap(), Some(&b"v"[..]));
    assert_eq!(reopened.get(&rtxn, b"k").unwrap(), Some(&b"v"[..]));
}
