//! Read-path differential tests: `LmdbEngine` vs the native
//! `ZerodbEngine`.
//!
//! Both engines are populated by the same `Put`/`PutFlagged`/`Del`/`Clear` op
//! stream; every read / cursor / seek / iteration op is then compared verbatim
//! against the LMDB fork.

use zerodb_oracle::{assert_matches_lmdb, decode_ops, DbName, Op, PutFlag, ZerodbEngine};

/// Create the unnamed DB, put some entries, commit, then read them back through
/// the real tree.
fn seed(pairs: &[(&[u8], &[u8])]) -> Vec<Op> {
    let mut ops = vec![Op::BeginRw, Op::create_db(DbName::Unnamed)];
    for (key, val) in pairs {
        ops.push(Op::put(0, key, val));
    }
    ops.push(Op::Commit);
    ops
}

#[test]
fn get_iter_first_last_after_commit() {
    let mut ops = seed(&[
        (b"apple", b"1"),
        (b"banana", b"2"),
        (b"cherry", b"3"),
        (b"date", b"4"),
    ]);
    ops.push(Op::BeginRo);
    ops.push(Op::get(0, b"banana"));
    ops.push(Op::get(0, b"missing"));
    ops.push(Op::First { db: 0 });
    ops.push(Op::Last { db: 0 });
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::RevIter { db: 0 });
    ops.push(Op::Len { db: 0 });
    ops.push(Op::IsEmpty { db: 0 });
    ops.push(Op::Commit);
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

#[test]
fn neighbor_seeks_after_commit() {
    let mut ops = seed(&[(b"10", b"a"), (b"20", b"b"), (b"30", b"c"), (b"40", b"d")]);
    ops.push(Op::BeginRo);
    for key in [b"05".as_slice(), b"20", b"25", b"40", b"99"] {
        ops.push(Op::set_range(0, key));
        ops.push(Op::get_greater_than(0, key));
        ops.push(Op::get_lower_than_or_equal_to(0, key));
    }
    ops.push(Op::Commit);
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

#[test]
fn prefix_iter_after_commit() {
    let mut ops = seed(&[
        (b"aa", b"1"),
        (b"ab", b"2"),
        (b"ac", b"3"),
        (b"b", b"4"),
        (b"ba", b"5"),
    ]);
    ops.push(Op::BeginRo);
    for p in [b"a".as_slice(), b"ab", b"b", b"z", b""] {
        ops.push(Op::prefix_iter(0, p));
        ops.push(Op::rev_prefix_iter(0, p));
    }
    ops.push(Op::Commit);
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

#[test]
fn all_ff_prefix_edge() {
    let mut ops = seed(&[
        (b"\xff", b"1"),
        (b"\xff\x00", b"2"),
        (b"\xff\xff", b"3"),
        (b"a", b"4"),
    ]);
    ops.push(Op::BeginRo);
    ops.push(Op::prefix_iter(0, b"\xff"));
    ops.push(Op::rev_prefix_iter(0, b"\xff"));
    ops.push(Op::prefix_iter(0, b"\xff\xff"));
    ops.push(Op::Commit);
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

#[test]
fn overflow_values_readback() {
    let big = vec![0x41u8; 9_000];
    let bigger = vec![0x5au8; 50_000];
    let ops = vec![
        Op::BeginRw,
        Op::create_db(DbName::Unnamed),
        Op::put(0, b"a", b"tiny"),
        Op::put(0, b"b", big.clone()),
        Op::put(0, b"c", bigger.clone()),
        Op::Commit,
        Op::BeginRo,
        Op::get(0, b"b"),
        Op::get(0, b"c"),
        Op::Iter { db: 0 },
        Op::Commit,
    ];
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

#[test]
fn empty_and_oversized_keys_match_lmdb() {
    // Exact-key ops with empty / oversized keys, on both a committed read and an
    // uncommitted write-txn read, must return whatever LMDB returns.
    let over = vec![b'z'; 600];
    let mut ops = seed(&[(b"k1", b"v1"), (b"k2", b"v2")]);
    ops.push(Op::BeginRo);
    ops.push(Op::get(0, b""));
    ops.push(Op::get(0, over.clone()));
    ops.push(Op::set_range(0, b""));
    ops.push(Op::set_range(0, over.clone()));
    ops.push(Op::get_greater_than(0, b""));
    ops.push(Op::get_lower_than_or_equal_to(0, over.clone()));
    ops.push(Op::prefix_iter(0, over.clone()));
    ops.push(Op::Commit);
    // `del` (a write op) with empty / oversized keys inside a write txn: empty
    // → BadValSize, oversized → Ok(false) (del searches, it does not validate
    // maxkey up front — unlike `put`, which rejects oversized).
    ops.push(Op::BeginRw);
    ops.push(Op::del(0, b""));
    ops.push(Op::del(0, over.clone()));
    ops.push(Op::put(0, b"", b"x")); // put empty → BadValSize
    ops.push(Op::put_reserved(0, over.clone(), b"x")); // put_reserved oversized → BadValSize
    ops.push(Op::Commit);
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

#[test]
fn writes_visible_within_txn_then_committed() {
    // Reads inside the write txn see uncommitted state; after commit the same
    // reads go through a committed snapshot.
    let ops = vec![
        Op::BeginRw,
        Op::create_db(DbName::Unnamed),
        Op::put(0, b"x", b"1"),
        Op::get(0, b"x"), // in-txn read (uncommitted)
        Op::put(0, b"y", b"2"),
        Op::First { db: 0 },
        Op::Last { db: 0 },
        Op::Iter { db: 0 },
        Op::Len { db: 0 },
        Op::Commit,
        Op::verify_get(0, b"x"),
        Op::BeginRo,
        Op::get(0, b"x"), // committed read
        Op::Iter { db: 0 },
        Op::Commit,
    ];
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

#[test]
fn append_and_no_overwrite_semantics() {
    let ops = vec![
        Op::BeginRw,
        Op::create_db(DbName::Unnamed),
        // APPEND in ascending order succeeds.
        Op::put_flagged(0, b"a", b"1", PutFlag::Append),
        Op::put_flagged(0, b"b", b"2", PutFlag::Append),
        // APPEND equal-to-last / less-than-last → KeyExist.
        Op::put_flagged(0, b"b", b"x", PutFlag::Append),
        Op::put_flagged(0, b"a", b"x", PutFlag::Append),
        // NO_OVERWRITE on absent → ok; on present → KeyExist.
        Op::put_flagged(0, b"c", b"3", PutFlag::NoOverwrite),
        Op::put_flagged(0, b"a", b"y", PutFlag::NoOverwrite),
        Op::Iter { db: 0 },
        Op::Commit,
        Op::BeginRo,
        Op::Iter { db: 0 },
        Op::Commit,
    ];
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

#[test]
fn clear_then_read() {
    let mut ops = seed(&[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]);
    ops.push(Op::BeginRw);
    ops.push(Op::ClearDb { db: 0 });
    ops.push(Op::Iter { db: 0 }); // in-txn: empty
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 }); // committed: empty
    ops.push(Op::IsEmpty { db: 0 });
    ops.push(Op::Commit);
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

/// Regression for a harness drift: a `Reopen` is a txn boundary, so
/// both engines must reset their `cleared_in_txn` FORK-1-guard fact there. If
/// only one does, a later `PutFlagged { Append }` makes the shared `classify`
/// guard fire asymmetrically (`KnownForkBug` vs `NoDb`).
#[test]
fn reopen_resets_cleared_in_txn_guard() {
    let ops = vec![
        Op::BeginRw,
        Op::create_db(DbName::Unnamed),
        Op::ClearDb { db: 0 }, // sets cleared_in_txn on both engines
        Op::reopen(0),         // must reset it on both
        // dbs are now empty on both → NoDb, not KnownForkBug.
        Op::put_flagged(0, b"a", b"1", PutFlag::Append),
    ];
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

/// Corpus-shaped smoke: decode a few fixed byte blobs into op sequences and run
/// the differential (a cheap always-on regression net; the real fuzzing is the
/// `diff_ops` target).
#[test]
fn decoded_blobs_do_not_diverge() {
    for seed in 0u64..2000 {
        let bytes = seed.to_le_bytes().repeat(8);
        let ops = decode_ops(&bytes, 32);
        assert_matches_lmdb::<ZerodbEngine>(&ops);
    }
}
