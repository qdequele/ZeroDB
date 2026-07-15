//! Pins SPEC 01 §S1 / §S2 / §S5 as executable facts observed directly against
//! the real Meilisearch LMDB fork (heed =0.22.1 / lmdb-master-sys 0.2.6,
//! `mdb.master.nested-rtxns` @ cd767228). These tests are ground-truth pins,
//! not zerodb tests: they establish what the oracle says, the way
//! `key_bounds.rs` already does for §S4. Do NOT weaken these assertions to
//! make them pass (CLAUDE.md rule 2) — a mismatch with SPEC 01 prose is a
//! spec bug for a human to reconcile, not a test bug.
//!
//! Test names follow the SPEC 01 differential-test slug column where a slug
//! exists (`flag_append_out_of_order`, `flag_append_ascending_ok`,
//! `flag_append_equal_key_keyexist`, `flag_no_overwrite_returns_existing`,
//! `env_prevsnapshot_opens_older_meta`).
//!
//! ## Coverage gaps (reported, not worked around)
//!
//! * §S1 (`APPEND`) and part of §S2 (the plain `KeyExist` error path) are
//!   reachable through the `Op`/`Engine` model (`Op::PutFlagged`), so those
//!   run through `LmdbEngine` like the rest of the oracle suite.
//! * §S2's *returned-existing-value* contract is **not** reachable through
//!   the `Op` model: `Op::PutFlagged` drives `Database::put_with_flags`,
//!   which returns `Result<()>` and cannot surface the old value. heed
//!   0.22.1 *does* expose the right primitive — `Database::get_or_put(_with_flags)`,
//!   which returns `Result<Option<DItem>>` (`None` = fresh insert, `Some(existing)`
//!   = collision, existing value left untouched) — but nothing in `op.rs` emits
//!   it. Per the task instructions this gap is reported rather than
//!   worked around with new dependencies/unsafe: the test below drives heed
//!   directly (same pattern as `key_bounds.rs`), not through `LmdbEngine`.
//! * §S5 (`PREV_SNAPSHOT`) is not reachable through the `Op` model either:
//!   `Op::Reopen` has no flags parameter, and `LmdbEngine` always opens with
//!   `EnvFlags::empty()`. Driven directly via heed's `EnvOpenOptions::flags`,
//!   again outside the `Op`/`Engine` seam.

use heed::types::Bytes;
use heed::{Database, EnvFlags, EnvOpenOptions, PutFlags};
use zerodb_oracle::tempdir::TempDir;
use zerodb_oracle::{DbName, Engine, LmdbEngine, Op, OpResult, OracleError, PutFlag};

fn k(bytes: &[u8]) -> zerodb_oracle::Key {
    zerodb_oracle::Key(bytes.to_vec())
}
fn v(bytes: &[u8]) -> zerodb_oracle::Value {
    zerodb_oracle::Value(bytes.to_vec())
}

// ---------------------------------------------------------------------
// §S1 — MDB_APPEND: last-key compare, not full-order validation
// ---------------------------------------------------------------------

/// APPEND into an empty database always succeeds (root is `P_INVALID`, the
/// `MDB_NO_ROOT` path — SPEC 01 §S1 point 1).
#[test]
fn flag_append_into_empty_db_ok() {
    let mut e = LmdbEngine::new();
    assert_eq!(e.apply(&Op::BeginRw), OpResult::Ok);
    assert_eq!(
        e.apply(&Op::CreateDb {
            name: DbName::Unnamed
        }),
        OpResult::Ok
    );
    assert_eq!(
        e.apply(&Op::PutFlagged {
            db: 0,
            key: k(b"a"),
            val: v(b"1"),
            flag: PutFlag::Append,
        }),
        OpResult::Ok,
        "APPEND into an empty db must succeed"
    );
    assert_eq!(e.apply(&Op::Commit), OpResult::Ok);
}

/// Strictly-ascending APPENDs succeed, one after another (each only compared
/// against the current last key).
#[test]
fn flag_append_ascending_ok() {
    let mut e = LmdbEngine::new();
    assert_eq!(e.apply(&Op::BeginRw), OpResult::Ok);
    assert_eq!(
        e.apply(&Op::CreateDb {
            name: DbName::Unnamed
        }),
        OpResult::Ok
    );
    for (key, val) in [
        (b"a".as_slice(), b"1".as_slice()),
        (b"b", b"2"),
        (b"c", b"3"),
    ] {
        assert_eq!(
            e.apply(&Op::PutFlagged {
                db: 0,
                key: k(key),
                val: v(val),
                flag: PutFlag::Append,
            }),
            OpResult::Ok,
            "ascending APPEND of {key:?} must succeed"
        );
    }
    assert_eq!(e.apply(&Op::Commit), OpResult::Ok);
}

/// An out-of-order (byte-descending relative to the last key) APPEND errors
/// `KeyExist`, per §S1: only the *last* key is compared, and `new <= last`
/// (equal included) is rejected, not silently sorted in.
#[test]
fn flag_append_out_of_order() {
    let mut e = LmdbEngine::new();
    assert_eq!(e.apply(&Op::BeginRw), OpResult::Ok);
    assert_eq!(
        e.apply(&Op::CreateDb {
            name: DbName::Unnamed
        }),
        OpResult::Ok
    );
    assert_eq!(
        e.apply(&Op::PutFlagged {
            db: 0,
            key: k(b"a"),
            val: v(b"1"),
            flag: PutFlag::Append,
        }),
        OpResult::Ok
    );
    assert_eq!(
        e.apply(&Op::PutFlagged {
            db: 0,
            key: k(b"b"),
            val: v(b"2"),
            flag: PutFlag::Append,
        }),
        OpResult::Ok
    );
    // "a" < "b" (the current last key) -> out-of-order APPEND -> KeyExist.
    assert_eq!(
        e.apply(&Op::PutFlagged {
            db: 0,
            key: k(b"a"),
            val: v(b"x"),
            flag: PutFlag::Append,
        }),
        OpResult::Err(OracleError::KeyExist),
        "out-of-order APPEND must error KeyExist, not silently insert"
    );
    assert_eq!(e.apply(&Op::Commit), OpResult::Ok);
}

/// Equal-to-last-key APPEND is *also* `KeyExist` — different from a plain
/// `put`, which would overwrite (SPEC 01 §S1 point 3).
#[test]
fn flag_append_equal_key_keyexist() {
    let mut e = LmdbEngine::new();
    assert_eq!(e.apply(&Op::BeginRw), OpResult::Ok);
    assert_eq!(
        e.apply(&Op::CreateDb {
            name: DbName::Unnamed
        }),
        OpResult::Ok
    );
    assert_eq!(
        e.apply(&Op::PutFlagged {
            db: 0,
            key: k(b"b"),
            val: v(b"2"),
            flag: PutFlag::Append,
        }),
        OpResult::Ok
    );
    assert_eq!(
        e.apply(&Op::PutFlagged {
            db: 0,
            key: k(b"b"),
            val: v(b"different"),
            flag: PutFlag::Append,
        }),
        OpResult::Err(OracleError::KeyExist),
        "APPEND with a key equal to the current last key must error KeyExist"
    );
    // Confirm it did NOT overwrite (unlike a plain put would have).
    assert_eq!(
        e.apply(&Op::Get {
            db: 0,
            key: k(b"b")
        }),
        OpResult::MaybeVal(Some(b"2".to_vec()))
    );
    assert_eq!(e.apply(&Op::Commit), OpResult::Ok);
}

/// Plain `KeyExist` path for `NO_OVERWRITE` reachable through the `Op` model:
/// putting over an existing key with `NoOverwrite` errors and does not
/// modify the stored value.
#[test]
fn flag_no_overwrite_errors_and_does_not_modify() {
    let mut e = LmdbEngine::new();
    assert_eq!(e.apply(&Op::BeginRw), OpResult::Ok);
    assert_eq!(
        e.apply(&Op::CreateDb {
            name: DbName::Unnamed
        }),
        OpResult::Ok
    );
    assert_eq!(
        e.apply(&Op::Put {
            db: 0,
            key: k(b"k"),
            val: v(b"original"),
        }),
        OpResult::Ok
    );
    assert_eq!(
        e.apply(&Op::PutFlagged {
            db: 0,
            key: k(b"k"),
            val: v(b"clobber"),
            flag: PutFlag::NoOverwrite,
        }),
        OpResult::Err(OracleError::KeyExist)
    );
    assert_eq!(
        e.apply(&Op::Get {
            db: 0,
            key: k(b"k")
        }),
        OpResult::MaybeVal(Some(b"original".to_vec())),
        "NO_OVERWRITE on an existing key must not modify the stored value"
    );
    assert_eq!(e.apply(&Op::Commit), OpResult::Ok);
}

// ---------------------------------------------------------------------
// §S1 / SPEC 03 §7 — cursor put_current_with_options(APPEND): the write-cursor
// APPEND path (milli facet bulk). heed's `put_current_with_options` passes the
// caller's `PutFlags` straight to `mdb_cursor_put` (verified: cursor.rs
// `put_current_with_flags` -> `flags.bits()`), with NO forced MDB_CURRENT. So
// APPEND here behaves exactly like a plain `MDB_APPEND` put: mdb_cursor_put runs
// its own `mdb_cursor_last` + last-key compare, IGNORING where the iterator is
// currently positioned. These tests pin that observed behavior; SPEC 03 §7 is
// annotated "confirmed via oracle self-test 2026-07-15".
//
// NOT reachable through the Op/Engine model (no write-cursor op in op.rs);
// driven directly via heed like the §S2/§S5 tests.
// ---------------------------------------------------------------------

/// APPEND through a write cursor positioned at the FIRST (non-last) entry, with
/// a key strictly greater than the current last key, SUCCEEDS — proving the
/// cursor position is irrelevant to MDB_APPEND (it compares against the DB's
/// last key, not the cursor's entry).
#[test]
fn cursor_put_current_append_ignores_position_when_greater() {
    let dir = TempDir::new().unwrap();
    let env = open_raw(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, None).unwrap();
    for (kk, vv) in [
        (b"a".as_slice(), b"1".as_slice()),
        (b"b", b"2"),
        (b"c", b"3"),
    ] {
        db.put(&mut wtxn, kk, vv).unwrap();
    }

    let mut it = db.iter_mut(&mut wtxn).unwrap();
    {
        // Position at the FIRST entry ("a"), which is NOT the last key.
        let first = it.next().unwrap().unwrap();
        assert_eq!(first.0, b"a".as_slice(), "cursor sits at first entry");
    }
    // Append "z" (> last key "c") while the cursor is at "a": must succeed.
    // SAFETY: no cursor-borrowed value is held across this call (the `first`
    // borrow above is dropped); owned literals are passed.
    let res =
        unsafe { it.put_current_with_options::<Bytes>(PutFlags::APPEND, b"z".as_slice(), b"Z") };
    assert!(
        res.is_ok(),
        "cursor-APPEND of a key > last must succeed regardless of cursor position, got {res:?}"
    );
    drop(it);

    assert_eq!(
        db.get(&wtxn, b"z".as_slice()).unwrap(),
        Some(b"Z".as_slice()),
        "the appended key must be present"
    );
    wtxn.commit().unwrap();
}

/// APPEND through a write cursor with a key that is NOT strictly greater than
/// the current last key errors `KeyExist`, even when the cursor is parked at
/// the last entry — the last-key compare rejects `new <= last` (SPEC 01 §S1),
/// and the cursor position does not exempt it.
#[test]
fn cursor_put_current_append_not_greater_is_keyexist() {
    let dir = TempDir::new().unwrap();
    let env = open_raw(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, None).unwrap();
    for (kk, vv) in [(b"a".as_slice(), b"1".as_slice()), (b"b", b"2")] {
        db.put(&mut wtxn, kk, vv).unwrap();
    }

    let mut it = db.iter_mut(&mut wtxn).unwrap();
    {
        // Walk to the LAST entry ("b").
        let a = it.next().unwrap().unwrap();
        assert_eq!(a.0, b"a".as_slice());
        let b = it.next().unwrap().unwrap();
        assert_eq!(b.0, b"b".as_slice(), "cursor now at last entry");
    }
    // Append "a" (< last "b"): out-of-order -> KeyExist even at the last entry.
    // SAFETY: no cursor-borrowed value held across the call.
    let res_lt =
        unsafe { it.put_current_with_options::<Bytes>(PutFlags::APPEND, b"a".as_slice(), b"x") };
    assert!(
        matches!(res_lt, Err(heed::Error::Mdb(heed::MdbError::KeyExist))),
        "cursor-APPEND of key < last must be KeyExist, got {res_lt:?}"
    );
    // Append "b" (== last "b"): equal-to-last is also KeyExist (not overwrite).
    // SAFETY: as above.
    let res_eq =
        unsafe { it.put_current_with_options::<Bytes>(PutFlags::APPEND, b"b".as_slice(), b"y") };
    assert!(
        matches!(res_eq, Err(heed::Error::Mdb(heed::MdbError::KeyExist))),
        "cursor-APPEND of key == last must be KeyExist, got {res_eq:?}"
    );
    drop(it);

    // The equal-key APPEND must NOT have overwritten "b"'s value.
    assert_eq!(
        db.get(&wtxn, b"b".as_slice()).unwrap(),
        Some(b"2".as_slice()),
        "failed cursor-APPEND must not modify the stored value"
    );
    wtxn.commit().unwrap();
}

// ---------------------------------------------------------------------
// §S2 — MDB_NOOVERWRITE: the returned-existing-value contract
//
// NOT reachable through the Op/Engine model (see module doc gap note).
// Driven directly via heed's `get_or_put`/`get_or_put_with_flags`, which
// heed documents as "insert, or if a value already exists for the key,
// return the previous value" — exactly the §S2 contract, with NO_OVERWRITE
// applied internally.
// ---------------------------------------------------------------------

fn open_raw(dir: &std::path::Path) -> heed::Env<heed::WithoutTls> {
    let mut opts = EnvOpenOptions::new().read_txn_without_tls();
    opts.map_size(1 << 20);
    opts.max_dbs(4);
    // SAFETY: no cross-process flags passed, private temp dir, single-threaded.
    unsafe { opts.open(dir) }.expect("open env")
}

/// §S2: on key collision, `get_or_put` returns the *existing* value (not the
/// caller's new one) and leaves it unmodified in the store.
#[test]
fn flag_no_overwrite_returns_existing() {
    let dir = TempDir::new().unwrap();
    let env = open_raw(dir.path());
    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, None).unwrap();

    // Fresh insert -> None (no prior value).
    let first = db
        .get_or_put(&mut wtxn, b"k".as_slice(), b"original".as_slice())
        .unwrap();
    assert_eq!(first, None, "fresh insert must report no previous value");

    // Collision -> Some(existing), and the stored value is unchanged.
    let second = db
        .get_or_put(&mut wtxn, b"k".as_slice(), b"clobber".as_slice())
        .unwrap();
    assert_eq!(
        second,
        Some(b"original".as_slice()),
        "collision must return the pre-existing value, not the caller's new one"
    );

    let stored = db.get(&wtxn, b"k".as_slice()).unwrap();
    assert_eq!(
        stored,
        Some(b"original".as_slice()),
        "the existing value must not be overwritten by the failed insert"
    );

    wtxn.commit().unwrap();
}

// ---------------------------------------------------------------------
// §S5 — MDB_PREVSNAPSHOT open protocol
//
// NOT reachable through the Op/Engine model (see module doc gap note).
// Driven directly via heed's `EnvOpenOptions::flags(EnvFlags::PREV_SNAPSHOT)`,
// following the same open/close/reopen protocol heed's own
// `examples/prev-snapshot.rs` documents.
// ---------------------------------------------------------------------

fn open_with_flags(dir: &std::path::Path, flags: EnvFlags) -> heed::Env<heed::WithTls> {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(10 << 20);
    opts.max_dbs(4);
    // SAFETY: PREV_SNAPSHOT / no flags only; private temp dir, single process,
    // exclusive access (D-001) satisfied since only this handle is open at a
    // time (each open is paired with `prepare_for_closing().wait()` first).
    unsafe {
        opts.flags(flags);
    }
    unsafe { opts.open(dir) }.expect("open env")
}

/// §S5: write txn A commits, write txn B commits; reopening with
/// `PREV_SNAPSHOT` must show A's state, not B's (the older of the two meta
/// pages, per `mdb_env_pick_meta`'s XOR-with-flag selection).
///
/// Then: commit a write txn while `PREV_SNAPSHOT` is active, and reopen
/// *without* the flag — the newly-committed state must be visible, proving
/// the flag does not permanently pin the env to the old meta (SPEC 01 §S5
/// "auto-clear after first commit").
#[test]
fn env_prevsnapshot_opens_older_meta() {
    let dir = TempDir::new().unwrap();

    // --- txn A: writes "a" ---
    {
        let env = open_with_flags(dir.path(), EnvFlags::empty());
        let mut wtxn = env.write_txn().unwrap();
        let db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, None).unwrap();
        db.put(&mut wtxn, b"a".as_slice(), b"from-A".as_slice())
            .unwrap();
        wtxn.commit().unwrap();
        env.prepare_for_closing().wait();
    }

    // --- txn B: writes "b" (a second, newer committed snapshot) ---
    {
        let env = open_with_flags(dir.path(), EnvFlags::empty());
        let mut wtxn = env.write_txn().unwrap();
        let db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, None).unwrap();
        db.put(&mut wtxn, b"b".as_slice(), b"from-B".as_slice())
            .unwrap();
        wtxn.commit().unwrap();
        env.prepare_for_closing().wait();
    }

    // --- reopen with PREV_SNAPSHOT: must see A's state, not B's ---
    {
        let env = open_with_flags(dir.path(), EnvFlags::PREV_SNAPSHOT);
        let rtxn = env.read_txn().unwrap();
        let db: Database<Bytes, Bytes> = env.open_database(&rtxn, None).unwrap().unwrap();
        assert_eq!(
            db.get(&rtxn, b"a".as_slice()).unwrap(),
            Some(b"from-A".as_slice()),
            "PREV_SNAPSHOT must see txn A's committed key"
        );
        assert_eq!(
            db.get(&rtxn, b"b".as_slice()).unwrap(),
            None,
            "PREV_SNAPSHOT must NOT see txn B's (newer) committed key"
        );
        drop(rtxn);

        // Commit a write txn while PREV_SNAPSHOT is active: SPEC 01 §S5 says
        // this makes the older meta the new live root and auto-clears the
        // flag ("one committed txn rewrites history, then normal").
        let mut wtxn = env.write_txn().unwrap();
        let db: Database<Bytes, Bytes> = env.open_database(&wtxn, None).unwrap().unwrap();
        db.put(&mut wtxn, b"c".as_slice(), b"from-prevsnapshot".as_slice())
            .unwrap();
        wtxn.commit().unwrap();
        env.prepare_for_closing().wait();
    }

    // --- reopen normally (no PREV_SNAPSHOT): the post-commit state from the
    // previous block must be visible, NOT a permanent pin to the old meta,
    // and "b" (from txn B) must be gone -- that meta was overwritten. ---
    {
        let env = open_with_flags(dir.path(), EnvFlags::empty());
        let rtxn = env.read_txn().unwrap();
        let db: Database<Bytes, Bytes> = env.open_database(&rtxn, None).unwrap().unwrap();
        assert_eq!(
            db.get(&rtxn, b"a".as_slice()).unwrap(),
            Some(b"from-A".as_slice()),
            "A's key survives (it was the base the prev-snapshot commit built on)"
        );
        assert_eq!(
            db.get(&rtxn, b"c".as_slice()).unwrap(),
            Some(b"from-prevsnapshot".as_slice()),
            "the commit made while PREV_SNAPSHOT was active must be visible \
             on a normal (non-PREV_SNAPSHOT) reopen -- the flag did not pin \
             state permanently"
        );
        assert_eq!(
            db.get(&rtxn, b"b".as_slice()).unwrap(),
            None,
            "txn B's meta was overwritten by the prev-snapshot commit"
        );
    }
}
