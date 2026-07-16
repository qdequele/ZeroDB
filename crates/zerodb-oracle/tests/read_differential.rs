//! Milestone 1.3 read-path differential tests: `LmdbEngine` vs the native
//! `ZerodbEngine`.
//!
//! The zerodb side populates via the same `Put`/`PutFlagged`/`Del`/`Clear` op
//! stream (buffered in a shadow, materialized through the bulk-load builder on
//! commit) and then serves reads from the real B-tree read path. Every read /
//! cursor / seek / iteration op is compared verbatim against the LMDB fork,
//! which is populated identically via its own API (PLAN §1.3 acceptance).

use zerodb_oracle::{decode_ops, run, DbName, Key, LmdbEngine, Op, PutFlag, Value, ZerodbEngine};

fn k(bytes: &[u8]) -> Key {
    Key(bytes.to_vec())
}
fn v(bytes: &[u8]) -> Value {
    Value(bytes.to_vec())
}

fn diff(ops: &[Op]) {
    if let Err(d) = run::<LmdbEngine, ZerodbEngine>(ops) {
        panic!("read-path divergence:\n{d}");
    }
}

/// Create the unnamed DB, put some entries, commit, then read them back through
/// the real tree.
fn seed(pairs: &[(&[u8], &[u8])]) -> Vec<Op> {
    let mut ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
    ];
    for (key, val) in pairs {
        ops.push(Op::Put {
            db: 0,
            key: k(key),
            val: v(val),
        });
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
    ops.push(Op::Get {
        db: 0,
        key: k(b"banana"),
    });
    ops.push(Op::Get {
        db: 0,
        key: k(b"missing"),
    });
    ops.push(Op::First { db: 0 });
    ops.push(Op::Last { db: 0 });
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::RevIter { db: 0 });
    ops.push(Op::Len { db: 0 });
    ops.push(Op::IsEmpty { db: 0 });
    ops.push(Op::Commit);
    diff(&ops);
}

#[test]
fn neighbor_seeks_after_commit() {
    let mut ops = seed(&[(b"10", b"a"), (b"20", b"b"), (b"30", b"c"), (b"40", b"d")]);
    ops.push(Op::BeginRo);
    for key in [b"05".as_slice(), b"20", b"25", b"40", b"99"] {
        ops.push(Op::SetRange { db: 0, key: k(key) });
        ops.push(Op::GetGreaterThan { db: 0, key: k(key) });
        ops.push(Op::GetLowerThanOrEqualTo { db: 0, key: k(key) });
    }
    ops.push(Op::Commit);
    diff(&ops);
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
        ops.push(Op::PrefixIter {
            db: 0,
            prefix: k(p),
        });
        ops.push(Op::RevPrefixIter {
            db: 0,
            prefix: k(p),
        });
    }
    ops.push(Op::Commit);
    diff(&ops);
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
    ops.push(Op::PrefixIter {
        db: 0,
        prefix: k(b"\xff"),
    });
    ops.push(Op::RevPrefixIter {
        db: 0,
        prefix: k(b"\xff"),
    });
    ops.push(Op::PrefixIter {
        db: 0,
        prefix: k(b"\xff\xff"),
    });
    ops.push(Op::Commit);
    diff(&ops);
}

#[test]
fn overflow_values_readback() {
    let big = vec![0x41u8; 9_000];
    let bigger = vec![0x5au8; 50_000];
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        Op::Put {
            db: 0,
            key: k(b"a"),
            val: v(b"tiny"),
        },
        Op::Put {
            db: 0,
            key: k(b"b"),
            val: Value(big.clone()),
        },
        Op::Put {
            db: 0,
            key: k(b"c"),
            val: Value(bigger.clone()),
        },
        Op::Commit,
        Op::BeginRo,
        Op::Get {
            db: 0,
            key: k(b"b"),
        },
        Op::Get {
            db: 0,
            key: k(b"c"),
        },
        Op::Iter { db: 0 },
        Op::Commit,
    ];
    diff(&ops);
}

#[test]
fn empty_and_oversized_keys_match_lmdb() {
    // Exact-key ops with empty / oversized keys, on both a committed read and an
    // uncommitted write-txn read, must return whatever LMDB returns.
    let over = vec![b'z'; 600];
    let mut ops = seed(&[(b"k1", b"v1"), (b"k2", b"v2")]);
    ops.push(Op::BeginRo);
    ops.push(Op::Get { db: 0, key: k(b"") });
    ops.push(Op::Get {
        db: 0,
        key: Key(over.clone()),
    });
    ops.push(Op::SetRange { db: 0, key: k(b"") });
    ops.push(Op::SetRange {
        db: 0,
        key: Key(over.clone()),
    });
    ops.push(Op::GetGreaterThan { db: 0, key: k(b"") });
    ops.push(Op::GetLowerThanOrEqualTo {
        db: 0,
        key: Key(over.clone()),
    });
    ops.push(Op::PrefixIter {
        db: 0,
        prefix: Key(over.clone()),
    });
    ops.push(Op::Commit);
    // `del` (a write op) with empty / oversized keys inside a write txn: empty
    // → BadValSize, oversized → Ok(false) (del searches, it does not validate
    // maxkey up front — unlike `put`, which rejects oversized). Regression for
    // the fuzz-found divergence at `Del { oversized }`.
    ops.push(Op::BeginRw);
    ops.push(Op::Del { db: 0, key: k(b"") });
    ops.push(Op::Del {
        db: 0,
        key: Key(over.clone()),
    });
    ops.push(Op::Put {
        db: 0,
        key: k(b""),
        val: v(b"x"),
    }); // put empty → BadValSize
    ops.push(Op::PutReserved {
        db: 0,
        key: Key(over.clone()),
        val: v(b"x"),
    }); // put_reserved oversized → BadValSize
    ops.push(Op::Commit);
    diff(&ops);
}

#[test]
fn writes_visible_within_txn_then_committed() {
    // Reads inside the write txn see uncommitted state (served from the shadow);
    // after commit the same reads go through the real tree.
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        Op::Put {
            db: 0,
            key: k(b"x"),
            val: v(b"1"),
        },
        Op::Get {
            db: 0,
            key: k(b"x"),
        }, // in-txn read (shadow)
        Op::Put {
            db: 0,
            key: k(b"y"),
            val: v(b"2"),
        },
        Op::First { db: 0 },
        Op::Last { db: 0 },
        Op::Iter { db: 0 },
        Op::Len { db: 0 },
        Op::Commit,
        Op::VerifyGet {
            db: 0,
            key: k(b"x"),
        },
        Op::BeginRo,
        Op::Get {
            db: 0,
            key: k(b"x"),
        }, // committed read (real tree)
        Op::Iter { db: 0 },
        Op::Commit,
    ];
    diff(&ops);
}

#[test]
fn append_and_no_overwrite_semantics() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        // APPEND in ascending order succeeds.
        Op::PutFlagged {
            db: 0,
            key: k(b"a"),
            val: v(b"1"),
            flag: PutFlag::Append,
        },
        Op::PutFlagged {
            db: 0,
            key: k(b"b"),
            val: v(b"2"),
            flag: PutFlag::Append,
        },
        // APPEND equal-to-last / less-than-last → KeyExist.
        Op::PutFlagged {
            db: 0,
            key: k(b"b"),
            val: v(b"x"),
            flag: PutFlag::Append,
        },
        Op::PutFlagged {
            db: 0,
            key: k(b"a"),
            val: v(b"x"),
            flag: PutFlag::Append,
        },
        // NO_OVERWRITE on absent → ok; on present → KeyExist.
        Op::PutFlagged {
            db: 0,
            key: k(b"c"),
            val: v(b"3"),
            flag: PutFlag::NoOverwrite,
        },
        Op::PutFlagged {
            db: 0,
            key: k(b"a"),
            val: v(b"y"),
            flag: PutFlag::NoOverwrite,
        },
        Op::Iter { db: 0 },
        Op::Commit,
        Op::BeginRo,
        Op::Iter { db: 0 },
        Op::Commit,
    ];
    diff(&ops);
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
    diff(&ops);
}

/// Regression for a fuzz-found harness drift: a `Reopen` is a txn boundary, so
/// both engines must reset their `cleared_in_txn` FORK-1-guard fact there. If
/// only one does, a later `PutFlagged { Append }` makes the shared `classify`
/// guard fire asymmetrically (`KnownForkBug` vs `NoDb`).
#[test]
fn reopen_resets_cleared_in_txn_guard() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        Op::ClearDb { db: 0 },          // sets cleared_in_txn on both engines
        Op::Reopen { map_size_kib: 0 }, // must reset it on both
        // dbs are now empty on both → NoDb, not KnownForkBug.
        Op::PutFlagged {
            db: 0,
            key: k(b"a"),
            val: v(b"1"),
            flag: PutFlag::Append,
        },
    ];
    diff(&ops);
}

/// Corpus-shaped smoke: decode a few fixed byte blobs into op sequences and run
/// the differential (a cheap always-on regression net; the real fuzzing is the
/// `diff_ops` target).
#[test]
fn decoded_blobs_do_not_diverge() {
    for seed in 0u64..2000 {
        let bytes = seed.to_le_bytes().repeat(8);
        let ops = decode_ops(&bytes, 32);
        diff(&ops);
    }
}
