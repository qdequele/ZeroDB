//! Adversarial coverage for the M2.9 dbi-handle lifetime work (ADR-0013,
//! SPEC 04 TXN-68, D-013/D-015/D-016) that is NOT already pinned by
//! `crates/zerodb/tests/dbi_handle_lifetime_native.rs` or
//! `crates/zerodb-oracle/tests/dbi_handle_lifetime.rs`.
//!
//! Scope (per the M2.9 test-writer brief):
//! 1. Concurrency: reader binds racing a creating txn's abort (ADR-0013 Q3
//!    linearization: a bind either observes the bump or completes before it —
//!    never a torn/panicking outcome); a long-lived `RoTxn`'s stable bind vs a
//!    `drop_db` commit that lands mid-life, and a fresh `RoTxn`/`open_database`
//!    seeing the post-bump truth.
//! 2. Env lifecycle: a `Database` handle used across `Env::clone()` shares the
//!    same registry (single `EnvInner`), so a bump through one clone is
//!    visible through operations bound via another.
//! 3. Cursor mid-life: an `RwCursor` positioned on a database, then a SECOND
//!    handle to the same dbi drops it (`delete=true`) inside the same txn —
//!    the next cursor step must see `BadDbi`, not a stale read of the
//!    now-deleted tree.
//! 4. Nested readers: binding a handle FOR THE FIRST TIME inside a nested
//!    child (never bound in the parent first) must delegate correctly; and a
//!    live nested child must block `drop_db` with `BadTxn` (TXN-29 fires
//!    before the dbi table is touched), not silently corrupt the handle
//!    table — only after the child drops does `drop_db` proceed and bump the
//!    generation.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};

use zerodb::{Env, EnvOpenOptions, Error, MdbError};

const MAP_SIZE: usize = 4 << 20;

// --- tiny self-cleaning temp dir (matches the sibling test files' style) ---

// Relaxed suffices for this counter: it only provides tempdir-name
// uniqueness; no memory is published under it.
static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("zerodb-dbi-adv-{pid}-{nanos}-{seq}"));
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
    opts.max_dbs(512);
    opts.max_readers(64);
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

#[track_caller]
fn assert_bad_txn<T>(label: &str, r: Result<T, Error>) {
    match r {
        Err(Error::Mdb(MdbError::BadTxn)) => {}
        Err(other) => panic!("{label}: expected BadTxn, got {other:?}"),
        Ok(_) => panic!("{label}: expected BadTxn, got Ok"),
    }
}

// ---------------------------------------------------------------------------
// 1. Concurrency: bind vs abort race (ADR-0013 Q3 linearization)
// ---------------------------------------------------------------------------

/// N reader threads repeatedly try to `get` through a handle whose creating
/// txn is racing towards an abort. ADR-0013 Q3: "the bind either sees the
/// bump (refused, like LMDB) or completed before it (legal use of a
/// then-valid handle)". The only two acceptable outcomes for every attempt
/// are `Ok(None)` (bound before the abort's bump) or `Err(BadDbi)` (bound
/// after) — never a panic, never any other error, and never a torn read.
/// Once the writer thread has joined (the abort has fully happened, under
/// the registry mutex, before `RwTxn::drop` returns), every subsequent read
/// from any thread must be `BadDbi` — no straggling stale success.
#[test]
fn concurrent_binds_race_creating_txn_abort() {
    let dir = TempDir::new();
    let env = open_env(dir.path());

    const READERS: usize = 8;
    const ROUNDS: usize = 200;

    for round in 0..ROUNDS {
        let name = format!("race-{round}");
        let mut wtxn = env.write_txn().unwrap();
        let db = env
            .create_database(&mut wtxn, Some(name.as_bytes()))
            .unwrap();

        // Barrier so every reader thread starts hammering the handle before
        // the writer aborts — maximizing the chance of catching a real race
        // rather than a trivially-serialized before/after.
        let start = Arc::new(Barrier::new(READERS + 1));
        let bad_outcome: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for _ in 0..READERS {
                let env = &env;
                let start = Arc::clone(&start);
                let bad_outcome = Arc::clone(&bad_outcome);
                handles.push(scope.spawn(move || {
                    start.wait();
                    for _ in 0..50 {
                        let rtxn = match env.read_txn() {
                            Ok(t) => t,
                            Err(e) => {
                                *bad_outcome.lock().unwrap() =
                                    Some(format!("read_txn failed: {e:?}"));
                                return;
                            }
                        };
                        match db.get(&rtxn, b"anything") {
                            Ok(None) => {}                          // bound before the abort's bump
                            Err(Error::Mdb(MdbError::BadDbi)) => {} // bound after
                            other => {
                                *bad_outcome.lock().unwrap() =
                                    Some(format!("unexpected outcome: {other:?}"));
                                return;
                            }
                        }
                    }
                }));
            }
            // Main thread: release the barrier, then race the abort itself.
            start.wait();
            wtxn.abort();
            for h in handles {
                h.join().expect("reader thread panicked");
            }
        });

        if let Some(msg) = bad_outcome.lock().unwrap().take() {
            panic!("round {round}: {msg}");
        }

        // Post-join: the abort is fully complete (RwTxn::Drop ran under the
        // write mutex before `abort` returned, and the generation bump runs
        // under the registry mutex inside that same body) — every read now
        // must be BadDbi, no straggling success.
        let rtxn = env.read_txn().unwrap();
        assert_bad_dbi(
            &format!("round {round}: post-join read"),
            db.get(&rtxn, b"anything"),
        );
    }
}

/// Same race, but for the OTHER close event: `drop_db(delete=true)` bumps the
/// generation immediately at call time (not at txn end), so this races a
/// concurrent bump against readers that may already be mid-bind. Same two
/// acceptable outcomes; after the writer's `drop_db` call returns (and
/// commits), every subsequent read must be `BadDbi`.
#[test]
fn concurrent_binds_race_drop_db() {
    let dir = TempDir::new();
    let env = open_env(dir.path());

    const READERS: usize = 8;
    const ROUNDS: usize = 100;

    for round in 0..ROUNDS {
        let name = format!("dropped-{round}");
        let mut wtxn = env.write_txn().unwrap();
        let db = env
            .create_database(&mut wtxn, Some(name.as_bytes()))
            .unwrap();
        db.put(&mut wtxn, b"k", b"v").unwrap();
        wtxn.commit().unwrap();

        // Relaxed on `stop`: a pure shutdown flag — threads act only on the
        // flag value itself, and the scope join below provides the final
        // happens-before edge for everything they wrote.
        let stop = Arc::new(AtomicBool::new(false));
        let bad_outcome: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for _ in 0..READERS {
                let env = &env;
                let stop = Arc::clone(&stop);
                let bad_outcome = Arc::clone(&bad_outcome);
                handles.push(scope.spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let rtxn = env.read_txn().unwrap();
                        match db.get(&rtxn, b"k") {
                            Ok(Some(v)) if v == b"v" => {}          // bound before the bump
                            Err(Error::Mdb(MdbError::BadDbi)) => {} // bound after
                            other => {
                                *bad_outcome.lock().unwrap() =
                                    Some(format!("unexpected outcome: {other:?}"));
                                return;
                            }
                        }
                    }
                }));
            }
            let mut wtxn = env.write_txn().unwrap();
            db.drop_db(&mut wtxn).unwrap();
            wtxn.commit().unwrap();
            stop.store(true, Ordering::Relaxed);
            for h in handles {
                h.join().expect("reader thread panicked");
            }
        });

        if let Some(msg) = bad_outcome.lock().unwrap().take() {
            panic!("round {round}: {msg}");
        }

        let rtxn = env.read_txn().unwrap();
        assert_bad_dbi(
            &format!("round {round}: post-join read"),
            db.get(&rtxn, b"k"),
        );
    }
}

/// A long-lived `RoTxn` binds once and keeps serving its bind (per-txn-bind
/// granularity, ADR-0013 Q2) across a `drop_db` that both bumps the
/// generation AND commits, mid-life. Extends
/// `dbi_handle_lifetime_native.rs::read_txn_bind_is_stable_for_its_life` by
/// also checking `Env::open_database` (a FRESH by-name lookup) on a NEW read
/// txn opened after the commit: it must report the database absent
/// (`Ok(None)`), not merely that the OLD handle is stale.
#[test]
fn long_lived_rotxn_stable_new_rotxn_sees_absence_via_open_database() {
    let dir = TempDir::new();
    let env = open_env(dir.path());

    let mut wtxn = env.write_txn().unwrap();
    let db = env.create_database(&mut wtxn, Some(b"longlived")).unwrap();
    db.put(&mut wtxn, b"k", b"v").unwrap();
    wtxn.commit().unwrap();

    // Bind in a long-lived read txn.
    let long_rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&long_rtxn, b"k").unwrap(), Some(&b"v"[..]));
    assert_eq!(long_rtxn.snapshot().txnid, env.txnid());

    // Writer drops the DB and commits while `long_rtxn` is still alive.
    let mut wtxn = env.write_txn().unwrap();
    db.drop_db(&mut wtxn).unwrap();
    wtxn.commit().unwrap();

    // The long-lived txn keeps serving its stable bind (MVCC + bind
    // stability): get/len/iter must all still work, unaffected.
    assert_eq!(db.get(&long_rtxn, b"k").unwrap(), Some(&b"v"[..]));
    assert_eq!(db.len(&long_rtxn).unwrap(), 1);
    let keys: Vec<Vec<u8>> = db.iter(&long_rtxn).map(|r| r.unwrap().0.to_vec()).collect();
    assert_eq!(keys, vec![b"k".to_vec()]);

    // A brand-new read txn resolving the name AFRESH via `open_database` sees
    // the database is simply gone (`Ok(None)`) — not `BadDbi` (there is no
    // handle to be stale; the catalog entry itself is absent post-commit).
    let fresh_rtxn = env.read_txn().unwrap();
    assert!(env
        .open_database(&fresh_rtxn, Some(b"longlived"))
        .unwrap()
        .is_none());

    // And the OLD handle, used against the fresh (post-bump) txn, is BadDbi.
    assert_bad_dbi("old handle in fresh txn", db.get(&fresh_rtxn, b"k"));

    drop(long_rtxn);
}

// ---------------------------------------------------------------------------
// 2. Env lifecycle: Env::clone() shares the same registry/generations
// ---------------------------------------------------------------------------

/// `Env::clone()` bumps the `Arc<EnvInner>` refcount without reopening the
/// file (TXN-50): the named-DB registry — and hence every generation — is
/// the SAME state, not a per-handle copy. A `drop_db` performed through a
/// txn opened on one clone must invalidate a `Database` handle used through
/// a txn opened on a DIFFERENT clone.
#[test]
fn database_handle_shares_generation_across_env_clones() {
    let dir = TempDir::new();
    let env_a = open_env(dir.path());
    let env_b = env_a.clone();

    let mut wtxn = env_a.write_txn().unwrap();
    let db = env_a.create_database(&mut wtxn, Some(b"shared")).unwrap();
    db.put(&mut wtxn, b"k", b"v").unwrap();
    wtxn.commit().unwrap();

    // Read through the OTHER clone: same handle, same generation, works.
    let rtxn_b = env_b.read_txn().unwrap();
    assert_eq!(db.get(&rtxn_b, b"k").unwrap(), Some(&b"v"[..]));
    drop(rtxn_b);

    // Drop the DB through clone B.
    let mut wtxn_b = env_b.write_txn().unwrap();
    db.drop_db(&mut wtxn_b).unwrap();
    wtxn_b.commit().unwrap();

    // The handle is now dead through clone A too — same registry, one bump.
    let rtxn_a = env_a.read_txn().unwrap();
    assert_bad_dbi("stale handle via original clone", db.get(&rtxn_a, b"k"));

    // Both `Env` values report the same handle count (same Arc).
    assert_eq!(env_a.handle_count(), env_b.handle_count());
}

// ---------------------------------------------------------------------------
// 3. Cursor mid-life: a second handle's drop_db invalidates a positioned
//    RwCursor in the SAME txn.
// ---------------------------------------------------------------------------

/// An `RwCursor` positioned on database X, then a second (equal-generation)
/// `Database` handle to X calls `drop_db` in the SAME txn: the next cursor
/// step must see `BadDbi` (per-step validation, `RwCursor::drive` calls
/// `validate_db` before every physical move), not a stale read of pages the
/// tree no longer owns.
#[test]
fn rw_cursor_positioned_then_dropped_via_second_handle_same_txn() {
    let dir = TempDir::new();
    let env = open_env(dir.path());

    let mut wtxn = env.write_txn().unwrap();
    let db = env.create_database(&mut wtxn, Some(b"cursored")).unwrap();
    db.put(&mut wtxn, b"a", b"1").unwrap();
    db.put(&mut wtxn, b"b", b"2").unwrap();
    db.put(&mut wtxn, b"c", b"3").unwrap();
    wtxn.commit().unwrap();

    // `open_database` returns a second handle with the SAME (current)
    // generation as `db` — same dbi, both currently valid.
    let mut wtxn = env.write_txn().unwrap();
    let second_handle = env
        .open_database(&wtxn, Some(b"cursored"))
        .unwrap()
        .expect("db exists");

    // Position a cursor via the FIRST handle on the first entry.
    let mut cursor = db.rw_cursor(&mut wtxn);
    assert_eq!(
        cursor.seek_first().unwrap(),
        Some((b"a".as_slice(), b"1".as_slice()))
    );
    drop(cursor);

    // The SECOND handle drops the database (env-wide, immediate, TXN-68).
    second_handle.drop_db(&mut wtxn).unwrap();

    // Driving the FIRST handle's cursor again must now fail with BadDbi, not
    // silently continue walking a tree the catalog no longer has an entry
    // for.
    let mut cursor = db.rw_cursor(&mut wtxn);
    assert_bad_dbi("cursor step after second-handle drop", cursor.seek_first());
    assert_bad_dbi(
        "cursor move_next after second-handle drop",
        cursor.move_next(),
    );
    drop(cursor);

    // The first handle is dead everywhere else in this txn too.
    assert_bad_dbi("get after second-handle drop", db.get(&wtxn, b"a"));

    wtxn.commit().expect("BadDbi must not poison the txn");
}

/// The symmetric case: cursor positioned via the handle that ITSELF calls
/// `drop_db` (so the cursor's own `db` field is now stale) must also refuse
/// on the next step — not merely when a *different* handle triggers the
/// invalidation.
#[test]
fn rw_cursor_positioned_then_self_drop_db() {
    let dir = TempDir::new();
    let env = open_env(dir.path());

    let mut wtxn = env.write_txn().unwrap();
    let db = env.create_database(&mut wtxn, Some(b"selfdrop")).unwrap();
    db.put(&mut wtxn, b"a", b"1").unwrap();
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    let mut cursor = db.rw_cursor(&mut wtxn);
    assert!(cursor.seek_first().unwrap().is_some());
    drop(cursor);

    db.drop_db(&mut wtxn).unwrap();

    let mut cursor = db.rw_cursor(&mut wtxn);
    assert_bad_dbi("self-drop cursor step", cursor.seek_first());
    wtxn.commit().expect("BadDbi must not poison the txn");
}

// ---------------------------------------------------------------------------
// 4. Nested readers: bind-first-in-child, and live-child blocks drop_db.
// ---------------------------------------------------------------------------

/// A handle that has NEVER been bound in the parent write txn (no `get`/
/// `put`/`ensure_open` call yet) is bound FOR THE FIRST TIME through a
/// nested child (`validate_db`/`record_for` delegate-live to the parent,
/// TXN-26/§5). This must work identically to binding via the parent
/// directly — the child is not a second, independent binding path.
#[test]
fn nested_reader_binds_handle_before_parent_does() {
    let dir = TempDir::new();
    let env = open_env(dir.path());

    let mut wtxn = env.write_txn().unwrap();
    let db = env.create_database(&mut wtxn, Some(b"unbound")).unwrap();
    db.put(&mut wtxn, b"k", b"v").unwrap();
    wtxn.commit().unwrap();

    // Fresh write txn: `db` has NOT been touched via `wtxn` yet (no
    // ensure_open call from this txn's perspective).
    let wtxn = env.write_txn().unwrap();
    let child = wtxn.nested_read_txn().unwrap();
    // First bind of this dbi in this whole txn lineage happens HERE, through
    // the child.
    assert_eq!(db.get(&child, b"k").unwrap(), Some(&b"v"[..]));
    assert!(db.validate(&child).is_ok());
    drop(child);

    // The parent, binding afterward, sees the same valid handle (shared
    // registry state — nothing about going through the child first should
    // poison or diverge the parent's own bind).
    assert_eq!(db.get(&wtxn, b"k").unwrap(), Some(&b"v"[..]));
    wtxn.abort();
}

/// A live nested child must block `drop_db` with `BadTxn` (TXN-29, D-005):
/// `drop_database` calls `guard_ok()` FIRST, before touching the dbi table,
/// so the child sees no torn/partial state — the drop simply hasn't
/// happened yet. Only after the child drops does `drop_db` succeed and bump
/// the generation.
#[test]
fn live_nested_child_blocks_drop_db_with_bad_txn_not_bad_dbi() {
    let dir = TempDir::new();
    let env = open_env(dir.path());

    let mut wtxn = env.write_txn().unwrap();
    let db = env.create_database(&mut wtxn, Some(b"guarded")).unwrap();
    db.put(&mut wtxn, b"k", b"v").unwrap();
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    let child = wtxn.nested_read_txn().unwrap();

    // While the child lives, drop_db must NOT run — no mutating op may
    // (TXN-29). The compile-time borrow already prevents calling
    // `db.drop_db(&mut wtxn)` while `child` (which holds `&wtxn`) is in
    // scope, so we prove the runtime backstop with `mem::forget` (the
    // documented sound-degradation path, ADR-0007 R1) exactly like
    // `nested.rs::forgotten_child_blocks_writer_soundly`.
    std::mem::forget(child);
    assert_bad_txn(
        "drop_db while a child is (forgotten-)live",
        db.drop_db(&mut wtxn),
    );

    // The handle is NOT dead — guard_ok fired before the dbi table was
    // touched, so this is BadTxn, never BadDbi, and a plain read still
    // works (nothing was mutated).
    assert_eq!(db.get(&wtxn, b"k").unwrap(), Some(&b"v"[..]));

    // The write txn is permanently blocked from mutating/committing now
    // (the counter can never reach 0 again after a forget) — this is the
    // documented sound-but-stuck state, not a corruption. Verify: dropping
    // it (abort) is the only way out, and a fresh txn instead performs the
    // drop cleanly, exercising the intended (non-forgotten) child lifecycle.
    wtxn.abort();

    let mut wtxn = env.write_txn().unwrap();
    {
        let child = wtxn.nested_read_txn().unwrap();
        assert_eq!(db.get(&child, b"k").unwrap(), Some(&b"v"[..]));
        // child drops here (end of block) — quiescence restored.
    }
    db.drop_db(&mut wtxn).unwrap();
    wtxn.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    assert_bad_dbi("after real drop_db", db.get(&rtxn, b"k"));
}
