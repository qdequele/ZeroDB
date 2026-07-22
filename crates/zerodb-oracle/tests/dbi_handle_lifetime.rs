//! **Differential** corpus for the dbi-handle lifetime rule (D-013 →
//! ADR-0013, milestone 2.9, SPEC 04 TXN-68). Originally a harness-regression
//! corpus (2026-07-20, `ZERODB_FUZZ_PAIR=heed` fuzz artifacts); graduated to
//! a differential now that the engine implements the rule.
//!
//! ## The rule
//!
//! LMDB, `lmdb.h` on `mdb_dbi_open`:
//!
//! > "The database handle will be private to the current transaction until the
//! > transaction is successfully committed. If the transaction is aborted the
//! > handle will be closed automatically."
//!
//! `mdb.c` implements it in `mdb_dbis_update(txn, keep)`, called from
//! `mdb_txn_end`: on the non-commit path (`keep == 0`) every dbi flagged
//! `DB_NEW` — i.e. opened in this txn — has `me_dbflags[i]` cleared and
//! `me_dbiseqs[i]` bumped. Any later use of that handle then fails the
//! `TXN_DBI_EXIST(txn, dbi, DB_VALID)` gate at the top of `mdb_cursor_open`
//! (and `TXN_DBI_EXIST(.., DB_USRVALID)` in `mdb_put`) and returns **`EINVAL`**.
//!
//! Separately, `mdb_drop(txn, dbi, del=1)` calls `mdb_dbi_close(env, dbi)`
//! directly — an **env-level** close that a subsequent abort does *not* undo.
//!
//! ## What each side answers (probed + implemented, M2.9)
//!
//! The fork reports **every** stale-handle op class as raw `EINVAL`
//! (`Io(InvalidInput)` through heed) — never `MDB_BAD_DBI` — and the error
//! does not poison the txn (`fork_taxonomy_*` pins below). ZeroDB implements
//! the same lifetime with a per-dbi generation registry (SPEC 04 TXN-68):
//! natively the error is `MdbError::BadDbi`; through `heed-zerodb` the
//! adapter re-imposes the fork's exact `Io(EINVAL)` observable. The harness
//! keeps dead handles addressable (until the next `CreateDb` purges them —
//! see `op.rs`), so these sequences drive real use-after-close differentials;
//! all three answers normalize to `OracleError::BadDbi`.

use heed::types::Bytes;
use zerodb_oracle::{run, DbName, HeedZerodbEngine, Key, LmdbEngine, Op, Value, ZerodbEngine};

/// Assert both engine pairs agree on `ops` — the native pair and the adapter
/// pair, since the harness bookkeeping is shared by all three engines.
fn diff_both_pairs(ops: &[Op]) {
    if let Err(d) = run::<LmdbEngine, ZerodbEngine>(ops) {
        panic!("native divergence:\n{d}");
    }
    if let Err(d) = run::<LmdbEngine, HeedZerodbEngine>(ops) {
        panic!("adapter divergence:\n{d}");
    }
}

fn k(s: &str) -> Key {
    Key(s.as_bytes().to_vec())
}
fn v(s: &str) -> Value {
    Value(s.as_bytes().to_vec())
}

/// Artifact `crash-c48149179fae…`, minimized: commit a named DB, open a second
/// DB in a txn that is never committed, drop the **committed** one, then abort.
/// The abort closes the uncommitted handle (`mdb_dbis_update` keep=0) *and* the
/// dropped handle stays closed (`mdb_drop` → `mdb_dbi_close`), so no usable
/// handle survives. The cursor path (`iter_mut` → `mdb_cursor_open`) is what
/// returned `EINVAL`.
#[test]
fn cursor_after_abort_of_txn_that_opened_the_handle() {
    let ops = vec![
        // Committed named DB.
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(40),
        },
        Op::Commit,
        // A second DB opened in a txn that never commits.
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        // Drop the committed one from the middle of the harness's list.
        Op::DropDb { db: 0 },
        // Rollback: closes the uncommitted handle; the dropped dbi stays closed.
        Op::Abort,
        // Fresh txn — the write-cursor path that produced EINVAL.
        Op::BeginRw,
        Op::IterMutPutCurrent {
            db: 45,
            nth: 1,
            val: v("after-abort"),
        },
        Op::Commit,
    ];
    diff_both_pairs(&ops);
}

/// Artifact `crash-580edc9bd18e…`, minimized: the same stale-handle situation
/// reached through `mdb_put` instead of a cursor. The reported key was an
/// ordinary 14-byte key — the size was a red herring; `mdb_put`'s very first
/// check is the same `TXN_DBI_EXIST` gate, so a dead dbi yields `EINVAL`
/// regardless of key or value.
#[test]
fn put_after_abort_of_txn_that_opened_the_handle() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(255),
        },
        Op::Commit,
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        Op::DropDb { db: 0 },
        Op::Abort,
        Op::BeginRw,
        Op::Put {
            db: 226,
            key: k("an-ordinary-key"),
            val: v("an-ordinary-value"),
        },
        Op::Commit,
    ];
    diff_both_pairs(&ops);
}

/// The positive control the watermark used to get wrong in the other direction:
/// a handle whose creating txn **committed** must stay usable across a later
/// unrelated abort.
#[test]
fn committed_handle_survives_a_later_abort() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        Op::Put {
            db: 0,
            key: k("k"),
            val: v("v"),
        },
        Op::Commit,
        // An unrelated txn that rolls back.
        Op::BeginRw,
        Op::Put {
            db: 0,
            key: k("rolled-back"),
            val: v("x"),
        },
        Op::Abort,
        // The committed handle is still good.
        Op::BeginRw,
        Op::Get { db: 0, key: k("k") },
        Op::Put {
            db: 0,
            key: k("k2"),
            val: v("v2"),
        },
        Op::Len { db: 0 },
        Op::Commit,
    ];
    diff_both_pairs(&ops);
}

/// `mdb_drop(.., del=1)` closes the dbi at env level, and an abort does **not**
/// resurrect it — even though the abort *does* roll back the deletion of the
/// database itself. Pins that asymmetry.
#[test]
fn dropped_handle_is_not_resurrected_by_abort() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(7),
        },
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        Op::Commit,
        Op::BeginRw,
        Op::DropDb { db: 0 },
        Op::Abort,
        // Both engines must agree about what is reachable now.
        Op::BeginRw,
        Op::Len { db: 0 },
        Op::Len { db: 1 },
        Op::Iter { db: 0 },
        Op::Commit,
    ];
    diff_both_pairs(&ops);
}

/// Drop + abort + re-create under the same name: the re-created handle is a
/// fresh dbi and must behave identically on both engines.
#[test]
fn recreate_after_drop_and_abort() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(3),
        },
        Op::Commit,
        Op::BeginRw,
        Op::DropDb { db: 0 },
        Op::Abort,
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(3),
        },
        Op::Put {
            db: 0,
            key: k("fresh"),
            val: v("entry"),
        },
        Op::Commit,
        Op::BeginRo,
        Op::Get {
            db: 0,
            key: k("fresh"),
        },
        Op::Commit,
    ];
    diff_both_pairs(&ops);
}

// ---------------------------------------------------------------------------
// M2.9 differential scenarios (ADR-0013 accept: (a) create+abort+use,
// (b) drop-then-use-same-txn, (c) drop+abort+use) — every modeled op class.
// ---------------------------------------------------------------------------

/// (a) A handle whose creating txn **aborted** is dead in a later write txn:
/// every op class errors identically (`EINVAL` / `BadDbi`), and the error
/// does NOT poison the txn (the trailing commit succeeds on both engines —
/// probed fork behavior).
#[test]
fn scenario_a_use_after_abort_every_op_class() {
    let mut ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(1),
        },
        Op::Abort, // closes the handle: mdb_dbis_update(keep=0) / TXN-68 bump
        Op::BeginRw,
    ];
    ops.extend(stale_use_ops(0));
    ops.push(Op::Commit);
    diff_both_pairs(&ops);
}

/// (a') The same dead handle must also fail in a **read** txn — the dbi table
/// is env-level, not writer-only.
#[test]
fn scenario_a_use_after_abort_in_read_txn() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(2),
        },
        Op::Abort,
        Op::BeginRo,
        Op::Get { db: 0, key: k("k") },
        Op::Len { db: 0 },
        Op::IsEmpty { db: 0 },
        Op::First { db: 0 },
        Op::Last { db: 0 },
        Op::SetRange { db: 0, key: k("k") },
        Op::Iter { db: 0 },
        Op::RevIter { db: 0 },
        Op::PrefixIter {
            db: 0,
            prefix: k("k"),
        },
        Op::Commit,
    ];
    diff_both_pairs(&ops);
}

/// (b) `mdb_drop(.., del=1)` closes the dbi **immediately**: using the handle
/// later in the SAME txn errors on every op class, and the txn still commits
/// (the successful drop lands; the stale-use errors are non-poisoning).
#[test]
fn scenario_b_use_in_same_txn_after_drop() {
    let mut ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(3),
        },
        Op::Put {
            db: 0,
            key: k("k"),
            val: v("v"),
        },
        Op::Commit,
        Op::BeginRw,
        Op::DropDb { db: 0 }, // env-level close, effective NOW
    ];
    ops.extend(stale_use_ops(0));
    ops.push(Op::DropDb { db: 0 }); // drop again: also EINVAL / BadDbi
    ops.push(Op::Commit);
    diff_both_pairs(&ops);
}

/// (c) drop, abort, use: the abort rolls the *deletion* back (the data is
/// visible again through a fresh by-name open) but does NOT resurrect the
/// handle — the asymmetry LMDB implements by pairing an env-level
/// `mdb_dbi_close` with a txn-level data rollback.
#[test]
fn scenario_c_drop_abort_then_use() {
    let mut ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(0),
        },
        Op::Put {
            db: 0,
            key: k("k"),
            val: v("v"),
        },
        Op::Commit,
        Op::BeginRw,
        Op::DropDb { db: 0 },
        Op::Abort,
        // The handle stays dead in a fresh write txn...
        Op::BeginRw,
    ];
    ops.extend(stale_use_ops(0));
    ops.push(Op::Commit);
    // ...and in a fresh read txn...
    ops.extend([
        Op::BeginRo,
        Op::Get { db: 0, key: k("k") },
        Op::Iter { db: 0 },
        Op::Commit,
        // ...while the DATA is back for a fresh by-name open (VerifyGet
        // resolves by name in its own read txn): Some("v") on both engines.
        Op::VerifyGet { db: 0, key: k("k") },
    ]);
    diff_both_pairs(&ops);
}

/// Every write-txn-shaped op class against a stale handle at index `db`.
/// (`ClearDb` is a *use* too — LMDB's `mdb_drop(del=0)` hits the same
/// `TXN_DBI_EXIST` gate, even though a clear through a LIVE handle never
/// invalidates anything.)
fn stale_use_ops(db: u8) -> Vec<Op> {
    vec![
        Op::Put {
            db,
            key: k("k"),
            val: v("v"),
        },
        Op::PutReserved {
            db,
            key: k("kr"),
            val: v("vr"),
        },
        Op::Del { db, key: k("k") },
        Op::Get { db, key: k("k") },
        Op::Len { db },
        Op::IsEmpty { db },
        Op::First { db },
        Op::Last { db },
        Op::SetExact { db, key: k("k") },
        Op::SetRange { db, key: k("k") },
        Op::GetGreaterThan { db, key: k("k") },
        Op::GetLowerThanOrEqualTo { db, key: k("k") },
        Op::Iter { db },
        Op::RevIter { db },
        Op::PrefixIter { db, prefix: k("k") },
        Op::RevPrefixIter { db, prefix: k("k") },
        Op::IterMutPutCurrent {
            db,
            nth: 0,
            val: v("x"),
        },
        Op::IterMutDelCurrent { db, nth: 0 },
        Op::ClearDb { db },
    ]
}

/// (a'') The `DB_NEW` analog is per **slot open**, not per catalog create:
/// after an env REOPEN every dbi slot is fresh, so a `CreateDb` that merely
/// opens a name already existing on disk is `DB_NEW` again — an abort kills
/// that handle exactly like an aborted create (found by self-review during
/// M2.9: the fork sets `DB_NEW` in `mdb_dbi_open` for every newly-allocated
/// slot, existing-on-disk or not).
#[test]
fn scenario_a_open_existing_after_reopen_then_abort() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(1),
        },
        Op::Put {
            db: 0,
            key: k("k"),
            val: v("v"),
        },
        Op::Commit,
        Op::Reopen { map_size_kib: 0 },
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(1),
        },
        Op::Abort, // DB_NEW slot freed: the handle is dead again
        Op::BeginRw,
        Op::Get { db: 0, key: k("k") },
        Op::Put {
            db: 0,
            key: k("k2"),
            val: v("x"),
        },
        Op::Iter { db: 0 },
        Op::Commit,
        // A committing re-open exports the slot: handle usable afterwards.
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(1),
        },
        Op::Commit,
        Op::BeginRo,
        Op::Get { db: 0, key: k("k") },
        Op::Commit,
    ];
    diff_both_pairs(&ops);
}

// ---------------------------------------------------------------------------
// Pinned fork taxonomy (the M2.9 probe, kept as a regression pin): the
// vendored fork answers EVERY stale-handle op class with raw `EINVAL` —
// heed's `Io(kind=InvalidInput, raw_os=EINVAL)`, never `MDB_BAD_DBI` — and
// the error does not poison the txn. The `heed-zerodb` adapter must keep
// reproducing this exact shape (`einval_pins` below runs the same probes
// through it). If a fork upgrade ever changes these answers, the adapter
// mapping in `heed-zerodb/src/error.rs` must be re-probed, not guessed.
// ---------------------------------------------------------------------------

mod fork_taxonomy {
    use super::*;
    use heed::{Env, EnvOpenOptions, WithoutTls};

    type Db = heed::Database<Bytes, Bytes>;

    fn open_env(dir: &std::path::Path) -> Env<WithoutTls> {
        let mut opts = EnvOpenOptions::new().read_txn_without_tls();
        opts.map_size(1 << 20);
        opts.max_dbs(16);
        // SAFETY: private temp dir, single-threaded test, no cross-process
        // flags.
        unsafe { opts.open(dir) }.expect("open env")
    }

    #[track_caller]
    fn assert_einval<T>(label: &str, r: Result<T, heed::Error>) {
        match r {
            Err(heed::Error::Io(io)) => {
                assert_eq!(
                    io.raw_os_error(),
                    Some(libc::EINVAL),
                    "{label}: expected raw EINVAL, got {io:?}"
                );
                assert_eq!(
                    io.kind(),
                    std::io::ErrorKind::InvalidInput,
                    "{label}: EINVAL must surface as InvalidInput"
                );
            }
            Err(other) => panic!("{label}: expected Io(EINVAL), got {other:?}"),
            Ok(_) => panic!("{label}: expected Io(EINVAL), got Ok"),
        }
    }

    /// Probe every op class against `db` in fresh txns; each must be EINVAL.
    fn assert_all_classes_einval(env: &Env<WithoutTls>, db: Db, tag: &str) {
        {
            let rtxn = env.read_txn().unwrap();
            assert_einval(&format!("{tag}/ro get"), db.get(&rtxn, b"k"));
            assert_einval(&format!("{tag}/ro len"), db.len(&rtxn));
            assert_einval(&format!("{tag}/ro stat"), db.stat(&rtxn));
            assert_einval(&format!("{tag}/ro first"), db.first(&rtxn));
            assert_einval(&format!("{tag}/ro iter open"), db.iter(&rtxn).map(|_| ()));
            assert_einval(
                &format!("{tag}/ro get_ge"),
                db.get_greater_than_or_equal_to(&rtxn, b"k"),
            );
        }
        {
            let mut wtxn = env.write_txn().unwrap();
            assert_einval(&format!("{tag}/rw put"), db.put(&mut wtxn, b"k", b"v"));
            assert_einval(&format!("{tag}/rw del"), db.delete(&mut wtxn, b"k"));
            assert_einval(&format!("{tag}/rw clear"), db.clear(&mut wtxn));
            // SAFETY (heed contract): only handle to this db in this probe.
            assert_einval(&format!("{tag}/rw drop"), unsafe { db.remove(&mut wtxn) });
            assert_einval(
                &format!("{tag}/rw iter_mut open"),
                db.iter_mut(&mut wtxn).map(|_| ()),
            );
            // Non-poisoning: after all those EINVALs the txn still commits.
            wtxn.commit()
                .expect("stale-handle EINVAL must not poison the txn");
        }
    }

    /// Scenario (a): create + abort.
    #[test]
    fn fork_taxonomy_after_aborted_create() {
        let dir = tempdir();
        let env = open_env(dir.path());
        let mut wtxn = env.write_txn().unwrap();
        let db: Db = env.create_database(&mut wtxn, Some("a")).unwrap();
        wtxn.abort();
        assert_all_classes_einval(&env, db, "a");
    }

    /// Scenario (b): drop(del=1), same txn.
    #[test]
    fn fork_taxonomy_same_txn_after_drop() {
        let dir = tempdir();
        let env = open_env(dir.path());
        let mut wtxn = env.write_txn().unwrap();
        let db: Db = env.create_database(&mut wtxn, Some("b")).unwrap();
        db.put(&mut wtxn, b"k", b"v").unwrap();
        wtxn.commit().unwrap();

        let mut wtxn = env.write_txn().unwrap();
        // SAFETY: only handle to this db.
        unsafe { db.remove(&mut wtxn) }.unwrap();
        assert_einval("b/same-txn get", db.get(&wtxn, b"k"));
        assert_einval("b/same-txn put", db.put(&mut wtxn, b"k2", b"v2"));
        assert_einval("b/same-txn stat", db.stat(&wtxn));
        // SAFETY: as above.
        assert_einval("b/same-txn drop again", unsafe { db.remove(&mut wtxn) });
        wtxn.commit().expect("the successful drop still commits");
    }

    /// Scenario (c): drop + abort — handle dead, data back.
    #[test]
    fn fork_taxonomy_after_drop_then_abort() {
        let dir = tempdir();
        let env = open_env(dir.path());
        let mut wtxn = env.write_txn().unwrap();
        let db: Db = env.create_database(&mut wtxn, Some("c")).unwrap();
        db.put(&mut wtxn, b"k", b"v").unwrap();
        wtxn.commit().unwrap();

        let mut wtxn = env.write_txn().unwrap();
        // SAFETY: only handle to this db.
        unsafe { db.remove(&mut wtxn) }.unwrap();
        wtxn.abort();
        assert_all_classes_einval(&env, db, "c");

        // The abort restored the DATA; only the handle is dead.
        let rtxn = env.read_txn().unwrap();
        let fresh: Db = env
            .open_database(&rtxn, Some("c"))
            .unwrap()
            .expect("db restored by abort");
        assert_eq!(fresh.get(&rtxn, b"k").unwrap(), Some(&b"v"[..]));
    }

    pub(super) struct TmpDir(std::path::PathBuf);
    impl TmpDir {
        pub(super) fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    pub(super) fn tempdir() -> TmpDir {
        // Uniqueness needs the counter: parallel test threads can observe the
        // same wall-clock instant (coarse clock granularity), and two heed
        // envs on one path would corrupt the probe.
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        // Relaxed: the counter only needs uniqueness; it publishes no other
        // memory (each fetch_add yields a distinct value on any architecture,
        // ARM included).
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let base = std::env::temp_dir().join(format!(
            "zerodb-dbi-lifetime-{}-{seq}-{:x}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&base).unwrap();
        TmpDir(base)
    }
}

// ---------------------------------------------------------------------------
// Adapter boundary pin: heed-zerodb must reproduce the fork's exact
// observable — `Io(kind=InvalidInput, raw_os=EINVAL)` — for a stale handle,
// including at iterator OPEN (the fork errors in `mdb_cursor_open`, so the
// adapter pre-validates instead of deferring to the first `next()`).
// ---------------------------------------------------------------------------

mod adapter_boundary {
    use heed_zerodb::types::Bytes;

    type Db = heed_zerodb::Database<Bytes, Bytes>;

    #[track_caller]
    fn assert_einval<T>(label: &str, r: Result<T, heed_zerodb::Error>) {
        match r {
            Err(heed_zerodb::Error::Io(io)) => {
                assert_eq!(
                    io.raw_os_error(),
                    Some(libc::EINVAL),
                    "{label}: expected raw EINVAL, got {io:?}"
                );
            }
            Err(other) => panic!("{label}: expected Io(EINVAL), got {other:?}"),
            Ok(_) => panic!("{label}: expected Io(EINVAL), got Ok"),
        }
    }

    #[test]
    fn adapter_reproduces_fork_einval_shape() {
        let dir = super::fork_taxonomy::tempdir();
        // SAFETY: same contract as `fork_taxonomy::open_env` — a fresh private
        // tempdir, single env per path in this process (heed open contract).
        let env = unsafe {
            heed_zerodb::EnvOpenOptions::new()
                .map_size(1 << 20)
                .max_dbs(16)
                .open(dir.path())
        }
        .expect("open adapter env");

        let mut wtxn = env.write_txn().unwrap();
        let db: Db = env.create_database(&mut wtxn, Some("a")).unwrap();
        wtxn.abort();

        let rtxn = env.read_txn().unwrap();
        assert_einval("ro get", db.get(&rtxn, b"k"));
        assert_einval("ro len", db.len(&rtxn));
        assert_einval("ro stat", db.stat(&rtxn));
        assert_einval("ro iter OPEN", db.iter(&rtxn).map(|_| ()));
        drop(rtxn);

        let mut wtxn = env.write_txn().unwrap();
        assert_einval("rw put", db.put(&mut wtxn, b"k", b"v"));
        assert_einval("rw del", db.delete(&mut wtxn, b"k"));
        assert_einval("rw clear", db.clear(&mut wtxn));
        assert_einval("rw iter_mut OPEN", db.iter_mut(&mut wtxn).map(|_| ()));
        // SAFETY: mirrors heed's `Database::remove` contract; only handle.
        assert_einval("rw drop", unsafe { db.remove(&mut wtxn) });
        // Non-poisoning, like the fork.
        wtxn.commit()
            .expect("stale-handle error must not poison the txn");
    }
}
