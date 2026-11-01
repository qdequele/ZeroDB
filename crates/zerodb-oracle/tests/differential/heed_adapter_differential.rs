//! heed adapter acceptance (ADR-0003 #6): re-run the oracle op model **through
//! the `heed-zerodb` adapter** and show zero divergences against the LMDB fork.
//!
//! Identical to the native `LmdbEngine`-vs-`ZerodbEngine` suites, but the
//! candidate is [`HeedZerodbEngine`] — ZeroDB driven *behind heed's surface*.
//! Any divergence here is an adapter-boundary bug (the native engine already
//! passes the same corpus). Contains a deterministic corpus exercising every op
//! category plus a 200-case `decode_ops` proptest.

use proptest::prelude::*;
use zerodb_oracle::{
    assert_matches_lmdb, decode_ops, run, DbName, HeedZerodbEngine, LmdbEngine, Op, PutFlag,
};

fn open_unnamed() -> Vec<Op> {
    vec![Op::BeginRw, Op::create_db(DbName::Unnamed), Op::Commit]
}

#[test]
fn put_get_commit_reopen() {
    let mut ops = open_unnamed();
    ops.push(Op::BeginRw);
    for i in 0..50u32 {
        ops.push(Op::put(0, format!("key-{i:03}"), format!("val-{i}")));
    }
    ops.push(Op::Commit);
    // Reads over a fresh read txn.
    ops.push(Op::BeginRo);
    for i in 0..50u32 {
        ops.push(Op::get(0, format!("key-{i:03}")));
    }
    ops.push(Op::Len { db: 0 });
    ops.push(Op::First { db: 0 });
    ops.push(Op::Last { db: 0 });
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::RevIter { db: 0 });
    ops.push(Op::Commit);
    // Reopen durability.
    ops.push(Op::reopen(0));
    ops.push(Op::BeginRo);
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Commit);
    assert_matches_lmdb::<HeedZerodbEngine>(&ops);
}

#[test]
fn flags_append_no_overwrite_reserved() {
    let mut ops = open_unnamed();
    ops.push(Op::BeginRw);
    // APPEND in ascending order, then a misfire.
    for i in 0..20u32 {
        ops.push(Op::put_flagged(
            0,
            format!("a-{i:03}"),
            "x",
            PutFlag::Append,
        ));
    }
    ops.push(Op::put_flagged(0, "a-000", "x", PutFlag::Append));
    // NO_OVERWRITE on an existing and a fresh key.
    ops.push(Op::put_flagged(0, "a-000", "y", PutFlag::NoOverwrite));
    ops.push(Op::put_flagged(0, "z-new", "y", PutFlag::NoOverwrite));
    // RESERVE.
    ops.push(Op::put_reserved(0, "r-key", "reserved-value"));
    ops.push(Op::get(0, "r-key"));
    ops.push(Op::Commit);
    assert_matches_lmdb::<HeedZerodbEngine>(&ops);
}

#[test]
fn delete_clear_and_cursor_mutation() {
    let mut ops = open_unnamed();
    ops.push(Op::BeginRw);
    for i in 0..40u32 {
        ops.push(Op::put(0, format!("m-{i:03}"), "v"));
    }
    // Cursor put/del at positions.
    ops.push(Op::iter_mut_put_current(0, 5, "rewritten"));
    ops.push(Op::iter_mut_del_current(0, 10));
    ops.push(Op::iter_mut_del_current(0, 0));
    ops.push(Op::del(0, "m-020"));
    ops.push(Op::get(0, "m-005"));
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    // Clear.
    ops.push(Op::BeginRw);
    ops.push(Op::ClearDb { db: 0 });
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Commit);
    assert_matches_lmdb::<HeedZerodbEngine>(&ops);
}

#[test]
fn seeks_prefix_and_range() {
    let mut ops = open_unnamed();
    ops.push(Op::BeginRw);
    for p in ["aa", "ab", "ba", "bb", "bc", "ca"] {
        for i in 0..5u32 {
            ops.push(Op::put(0, format!("{p}-{i}"), "v"));
        }
    }
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::prefix_iter(0, "b"));
    ops.push(Op::rev_prefix_iter(0, "b"));
    ops.push(Op::set_range(0, "ba-2"));
    ops.push(Op::get_greater_than(0, "ba-2"));
    ops.push(Op::get_lower_than_or_equal_to(0, "bb-0"));
    ops.push(Op::Commit);
    assert_matches_lmdb::<HeedZerodbEngine>(&ops);
}

#[test]
fn named_databases_and_abort() {
    let mut ops = vec![
        Op::BeginRw,
        Op::create_db(DbName::Named(0)),
        Op::create_db(DbName::Named(1)),
        Op::Commit,
        Op::BeginRw,
    ];
    for i in 0..15u32 {
        ops.push(Op::put(0, format!("d0-{i}"), "a"));
        ops.push(Op::put(1, format!("d1-{i}"), "b"));
    }
    ops.push(Op::Commit);
    // Create-in-txn then abort: must vanish.
    ops.push(Op::BeginRw);
    ops.push(Op::create_db(DbName::Named(2)));
    ops.push(Op::put(2, "ephemeral", "x"));
    ops.push(Op::Abort);
    ops.push(Op::BeginRo);
    ops.push(Op::verify_get(0, "d0-3"));
    ops.push(Op::Len { db: 1 });
    ops.push(Op::Commit);
    assert_matches_lmdb::<HeedZerodbEngine>(&ops);
}

#[test]
fn nested_read_over_write_txn() {
    let mut ops = open_unnamed();
    ops.push(Op::BeginRw);
    for i in 0..30u32 {
        ops.push(Op::put(0, format!("n-{i:02}"), format!("uncommitted-{i}")));
    }
    // Nested reader sees uncommitted state.
    ops.push(Op::BeginNestedRo);
    ops.push(Op::get(0, "n-05"));
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Len { db: 0 });
    ops.push(Op::EndNestedRo);
    ops.push(Op::Commit);
    assert_matches_lmdb::<HeedZerodbEngine>(&ops);
}

#[test]
fn large_overflow_values() {
    let mut ops = open_unnamed();
    ops.push(Op::BeginRw);
    ops.push(Op::put(0, "small", vec![1u8; 10]));
    ops.push(Op::put(0, "big", vec![7u8; 40_000]));
    ops.push(Op::put(0, "mid", vec![3u8; 4096]));
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::get(0, "big"));
    ops.push(Op::get(0, "mid"));
    ops.push(Op::Commit);
    assert_matches_lmdb::<HeedZerodbEngine>(&ops);
}

proptest! {
    // 200-case randomized differential through the adapter. Each case
    // materializes real files for both backends and runs up to 48 ops.
    #![proptest_config(ProptestConfig { cases: 200, ..ProptestConfig::default() })]

    #[test]
    fn random_ops_through_adapter(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let ops = decode_ops(&bytes, 48);
        if let Err(d) = run::<LmdbEngine, HeedZerodbEngine>(&ops) {
            prop_assert!(false, "adapter divergence:\n{d}");
        }
    }
}

/// Reserved-put pin (adapter `ReservedSpace`, docs/PERFORMANCE.md): the
/// fork's `put_reserved` semantics when the caller's closure FAILS. LMDB
/// reserves the slot inside the page via `MDB_RESERVE` *before* the closure
/// runs, so a closure error cannot un-put the entry. Pinned side by side:
/// (a) whether the call errors, (b) whether the key is present afterwards,
/// (c) the stored length. Value BYTES are
/// deliberately not compared — LMDB's unwritten reserve tail is whatever the
/// page held (uninitialized from the API's point of view; zerodb zero-fills
/// its tail, which this pin cannot and does not observe).
///
/// Regression: an adapter that fills a heap buffer before inserting leaves NO
/// entry on a closure error, diverging from the fork.
#[test]
fn put_reserved_failing_closure_leaves_entry_parity() {
    use std::io::Write as _;
    use zerodb_oracle::tempdir::TempDir;

    const LEN: usize = 64;

    let fork = {
        let dir = TempDir::new().unwrap();
        let mut opts = heed::EnvOpenOptions::new().read_txn_without_tls();
        opts.map_size(1 << 20);
        opts.max_dbs(4);
        // SAFETY: no cross-process flags; private temp dir, single-threaded.
        let env = unsafe { opts.open(dir.path()).unwrap() };
        let mut w = env.write_txn().unwrap();
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> =
            env.create_database(&mut w, None).unwrap();
        let r = db.put_reserved(&mut w, b"key", LEN, |sp| {
            sp.write_all(b"partial")?;
            Err(std::io::Error::other("closure failure"))
        });
        let errored = r.is_err();
        w.commit().unwrap();
        let rt = env.read_txn().unwrap();
        let len = db.get(&rt, b"key").unwrap().map(<[u8]>::len);
        (errored, len)
    };

    let adapter = {
        let dir = TempDir::new().unwrap();
        let mut opts = heed_zerodb::EnvOpenOptions::new().read_txn_without_tls();
        opts.map_size(1 << 20);
        opts.max_dbs(4);
        // SAFETY: as above.
        let env = unsafe { opts.open(dir.path()).unwrap() };
        let mut w = env.write_txn().unwrap();
        let db: heed_zerodb::Database<heed_zerodb::types::Bytes, heed_zerodb::types::Bytes> =
            env.create_database(&mut w, None).unwrap();
        let r = db.put_reserved(&mut w, b"key", LEN, |sp| {
            sp.write_all(b"partial")?;
            Err(std::io::Error::other("closure failure"))
        });
        let errored = r.is_err();
        w.commit().unwrap();
        let rt = env.read_txn().unwrap();
        let len = db.get(&rt, b"key").unwrap().map(<[u8]>::len);
        (errored, len)
    };

    assert_eq!(
        fork.0, adapter.0,
        "closure-error propagation parity (fork errored: {}, adapter errored: {})",
        fork.0, adapter.0
    );
    assert_eq!(
        fork.1, adapter.1,
        "post-error presence/length parity (fork: {:?}, adapter: {:?})",
        fork.1, adapter.1
    );
}
