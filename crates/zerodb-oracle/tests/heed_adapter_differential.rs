//! Milestone 1.13 acceptance (ADR-0003 #6): re-run the oracle op model **through
//! the `heed-zerodb` adapter** and show zero divergences against the LMDB fork.
//!
//! Identical to the native `LmdbEngine`-vs-`ZerodbEngine` suites, but the
//! candidate is [`HeedZerodbEngine`] — ZeroDB driven *behind heed's surface*.
//! Any divergence here is an adapter-boundary bug (the native engine already
//! passes the same corpus). Contains a deterministic corpus exercising every op
//! category plus a 200-case `decode_ops` proptest.

use proptest::prelude::*;
use zerodb_oracle::{
    decode_ops, run, DbName, HeedZerodbEngine, Key, LmdbEngine, Op, PutFlag, Value,
};

fn diff(ops: &[Op]) {
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

fn open_unnamed() -> Vec<Op> {
    vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        Op::Commit,
    ]
}

#[test]
fn put_get_commit_reopen() {
    let mut ops = open_unnamed();
    ops.push(Op::BeginRw);
    for i in 0..50u32 {
        ops.push(Op::Put {
            db: 0,
            key: k(&format!("key-{i:03}")),
            val: v(&format!("val-{i}")),
        });
    }
    ops.push(Op::Commit);
    // Reads over a fresh read txn.
    ops.push(Op::BeginRo);
    for i in 0..50u32 {
        ops.push(Op::Get {
            db: 0,
            key: k(&format!("key-{i:03}")),
        });
    }
    ops.push(Op::Len { db: 0 });
    ops.push(Op::First { db: 0 });
    ops.push(Op::Last { db: 0 });
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::RevIter { db: 0 });
    ops.push(Op::Commit);
    // Reopen durability.
    ops.push(Op::Reopen { map_size_kib: 0 });
    ops.push(Op::BeginRo);
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Commit);
    diff(&ops);
}

#[test]
fn flags_append_no_overwrite_reserved() {
    let mut ops = open_unnamed();
    ops.push(Op::BeginRw);
    // APPEND in ascending order, then a misfire.
    for i in 0..20u32 {
        ops.push(Op::PutFlagged {
            db: 0,
            key: k(&format!("a-{i:03}")),
            val: v("x"),
            flag: PutFlag::Append,
        });
    }
    ops.push(Op::PutFlagged {
        db: 0,
        key: k("a-000"),
        val: v("x"),
        flag: PutFlag::Append,
    });
    // NO_OVERWRITE on an existing and a fresh key.
    ops.push(Op::PutFlagged {
        db: 0,
        key: k("a-000"),
        val: v("y"),
        flag: PutFlag::NoOverwrite,
    });
    ops.push(Op::PutFlagged {
        db: 0,
        key: k("z-new"),
        val: v("y"),
        flag: PutFlag::NoOverwrite,
    });
    // RESERVE.
    ops.push(Op::PutReserved {
        db: 0,
        key: k("r-key"),
        val: v("reserved-value"),
    });
    ops.push(Op::Get {
        db: 0,
        key: k("r-key"),
    });
    ops.push(Op::Commit);
    diff(&ops);
}

#[test]
fn delete_clear_and_cursor_mutation() {
    let mut ops = open_unnamed();
    ops.push(Op::BeginRw);
    for i in 0..40u32 {
        ops.push(Op::Put {
            db: 0,
            key: k(&format!("m-{i:03}")),
            val: v("v"),
        });
    }
    // Cursor put/del at positions.
    ops.push(Op::IterMutPutCurrent {
        db: 0,
        nth: 5,
        val: v("rewritten"),
    });
    ops.push(Op::IterMutDelCurrent { db: 0, nth: 10 });
    ops.push(Op::IterMutDelCurrent { db: 0, nth: 0 });
    ops.push(Op::Del {
        db: 0,
        key: k("m-020"),
    });
    ops.push(Op::Get {
        db: 0,
        key: k("m-005"),
    });
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    // Clear.
    ops.push(Op::BeginRw);
    ops.push(Op::ClearDb { db: 0 });
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Commit);
    diff(&ops);
}

#[test]
fn seeks_prefix_and_range() {
    let mut ops = open_unnamed();
    ops.push(Op::BeginRw);
    for p in ["aa", "ab", "ba", "bb", "bc", "ca"] {
        for i in 0..5u32 {
            ops.push(Op::Put {
                db: 0,
                key: k(&format!("{p}-{i}")),
                val: v("v"),
            });
        }
    }
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::PrefixIter {
        db: 0,
        prefix: k("b"),
    });
    ops.push(Op::RevPrefixIter {
        db: 0,
        prefix: k("b"),
    });
    ops.push(Op::SetRange {
        db: 0,
        key: k("ba-2"),
    });
    ops.push(Op::GetGreaterThan {
        db: 0,
        key: k("ba-2"),
    });
    ops.push(Op::GetLowerThanOrEqualTo {
        db: 0,
        key: k("bb-0"),
    });
    ops.push(Op::Commit);
    diff(&ops);
}

#[test]
fn named_databases_and_abort() {
    let mut ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(0),
        },
        Op::CreateDb {
            name: DbName::Named(1),
        },
        Op::Commit,
        Op::BeginRw,
    ];
    for i in 0..15u32 {
        ops.push(Op::Put {
            db: 0,
            key: k(&format!("d0-{i}")),
            val: v("a"),
        });
        ops.push(Op::Put {
            db: 1,
            key: k(&format!("d1-{i}")),
            val: v("b"),
        });
    }
    ops.push(Op::Commit);
    // Create-in-txn then abort: must vanish.
    ops.push(Op::BeginRw);
    ops.push(Op::CreateDb {
        name: DbName::Named(2),
    });
    ops.push(Op::Put {
        db: 2,
        key: k("ephemeral"),
        val: v("x"),
    });
    ops.push(Op::Abort);
    ops.push(Op::BeginRo);
    ops.push(Op::VerifyGet {
        db: 0,
        key: k("d0-3"),
    });
    ops.push(Op::Len { db: 1 });
    ops.push(Op::Commit);
    diff(&ops);
}

#[test]
fn nested_read_over_write_txn() {
    let mut ops = open_unnamed();
    ops.push(Op::BeginRw);
    for i in 0..30u32 {
        ops.push(Op::Put {
            db: 0,
            key: k(&format!("n-{i:02}")),
            val: v(&format!("uncommitted-{i}")),
        });
    }
    // Nested reader sees uncommitted state.
    ops.push(Op::BeginNestedRo);
    ops.push(Op::Get {
        db: 0,
        key: k("n-05"),
    });
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Len { db: 0 });
    ops.push(Op::EndNestedRo);
    ops.push(Op::Commit);
    diff(&ops);
}

#[test]
fn large_overflow_values() {
    let mut ops = open_unnamed();
    ops.push(Op::BeginRw);
    ops.push(Op::Put {
        db: 0,
        key: k("small"),
        val: Value(vec![1u8; 10]),
    });
    ops.push(Op::Put {
        db: 0,
        key: k("big"),
        val: Value(vec![7u8; 40_000]),
    });
    ops.push(Op::Put {
        db: 0,
        key: k("mid"),
        val: Value(vec![3u8; 4096]),
    });
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Get {
        db: 0,
        key: k("big"),
    });
    ops.push(Op::Get {
        db: 0,
        key: k("mid"),
    });
    ops.push(Op::Commit);
    diff(&ops);
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
