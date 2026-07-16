//! M1.4 fix follow-up: differential twins of
//! `crates/zerodb/tests/deep_tree_rebalance_symmetric.rs` — the symmetric
//! branch-rebalance directions (borrow/merge from the LEFT sibling, driven by
//! descending deletes; interior-band deletes for both merge directions) run
//! against the LMDB fork. The fixed borrow-from-right shifted-index bug
//! (repeated `remove(0)`) had these as its audit siblings; this pins them
//! differentially. `ZerodbEngine` runs the invariant walk after every commit.

use zerodb_oracle::{run, DbName, Key, LmdbEngine, Op, Value, ZerodbEngine};

fn diff(ops: Vec<Op>) {
    if let Err(d) = run::<LmdbEngine, ZerodbEngine>(&ops) {
        panic!("divergence:\n{d}");
    }
}

fn setup() -> Vec<Op> {
    vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
    ]
}

/// Same wide-key geometry as `write_rebalance_differential.rs`.
fn wide_key(i: u32) -> Key {
    let mut k = format!("wk{i:06}").into_bytes();
    k.resize(500, b'.');
    Key(k)
}

fn small_val(i: u32) -> Value {
    Value(format!("v{i}").into_bytes())
}

#[test]
fn descending_delete_to_empty_differential() {
    let n = 400u32;
    let mut ops = setup();
    for i in 0..n {
        ops.push(Op::Put {
            db: 0,
            key: wide_key(i),
            val: small_val(i),
        });
    }
    ops.push(Op::Commit);

    // Strictly descending cascade: branch rebalance always finds its sibling
    // on the LEFT (borrow-from-left while it is full, merge-from-left once it
    // drains), down to the empty tree.
    ops.push(Op::BeginRw);
    for i in (0..n).rev() {
        ops.push(Op::Del {
            db: 0,
            key: wide_key(i),
        });
    }
    ops.push(Op::Len { db: 0 });
    ops.push(Op::IsEmpty { db: 0 });
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);

    // Regrow after collapse.
    ops.push(Op::BeginRw);
    ops.push(Op::Put {
        db: 0,
        key: wide_key(7),
        val: small_val(7),
    });
    ops.push(Op::Commit);
    diff(ops);
}

#[test]
fn interior_band_delete_differential() {
    let n = 400u32;
    let mut ops = setup();
    for i in 0..n {
        ops.push(Op::Put {
            db: 0,
            key: wide_key(i),
            val: small_val(i),
        });
    }
    ops.push(Op::Commit);

    // Hollow out the interior [100, 300) from both ends of the band toward
    // the middle: underful pages meet populated neighbors on either side, so
    // merges run with the survivor on the left AND on the right.
    ops.push(Op::BeginRw);
    let (mut lo, mut hi) = (100u32, 299u32);
    while lo <= hi {
        ops.push(Op::Del {
            db: 0,
            key: wide_key(lo),
        });
        if hi != lo {
            ops.push(Op::Del {
                db: 0,
                key: wide_key(hi),
            });
        }
        lo += 1;
        hi -= 1;
    }
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::RevIter { db: 0 });
    ops.push(Op::Commit);
    diff(ops);
}
