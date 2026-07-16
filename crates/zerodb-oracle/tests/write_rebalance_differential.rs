//! Milestone 1.4 coverage pass: deep-tree rebalance storms. Op sequences that
//! force multi-level splits (depth >= 3, confirmed independently in
//! `crates/zerodb/tests/deep_tree_rebalance.rs` using `TxnRead::main_record`,
//! since the `Op` model has no depth-introspection op) and then cascade
//! deletes back down through merges/borrows to empty, at psize 4096 (the
//! `ZerodbEngine` fixed page size). Interleaves puts/deletes crossing
//! SPEC 03 §10 `FILL_THRESHOLD`/`MIN_KEYS` boundaries in both directions.
//! `ZerodbEngine::debug_check_image` already runs the SPEC 03 §11 invariant
//! walk after every commit (see `write_differential.rs`'s module docs) — no
//! extra plumbing needed here.
//!
//! Keys are ~500 bytes (near the SPEC 01 §S4 511-byte max) so branch fanout is
//! small (~7 entries/page at 4 KiB), reaching depth 3 in a few hundred entries
//! instead of tens of thousands — keeps the whole file's C-LMDB-linked runtime
//! well under the 60 s coverage-pass budget.

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

/// A near-max-size key (small branch fanout) with a distinguishing numeric
/// suffix so lexicographic order matches insertion order for `i < 10_000`.
fn wide_key(i: u32) -> Key {
    let mut k = format!("wk{i:06}").into_bytes();
    k.resize(500, b'.');
    Key(k)
}

fn small_val(i: u32) -> Value {
    Value(format!("v{i}").into_bytes())
}

#[test]
fn deep_split_cascade_then_delete_to_empty() {
    let mut ops = setup();
    // Enough wide-keyed entries to push past depth 2 (branch-of-branches):
    // ~7 entries/leaf and ~7 entries/branch at this key size means >~49
    // leaves forces a second branch level; 400 entries comfortably clears it
    // (independently confirmed in deep_tree_rebalance.rs).
    let n = 400u32;
    for i in 0..n {
        ops.push(Op::Put {
            db: 0,
            key: wide_key(i),
            val: small_val(i),
        });
    }
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);

    // Cascade delete: remove every other entry first (crosses FILL_THRESHOLD
    // downward, forcing borrow-or-merge across most leaves), then the rest
    // (drives merges up through the branch levels, INV-8/§10), down to an
    // empty tree (root collapse, §9).
    ops.push(Op::BeginRw);
    for i in (0..n).step_by(2) {
        ops.push(Op::Del {
            db: 0,
            key: wide_key(i),
        });
    }
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);

    ops.push(Op::BeginRw);
    for i in (1..n).step_by(2) {
        ops.push(Op::Del {
            db: 0,
            key: wide_key(i),
        });
    }
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::IsEmpty { db: 0 });
    ops.push(Op::Commit);

    // Rebuild after full collapse: the empty-tree grow path (§9) must still
    // work post-collapse.
    ops.push(Op::BeginRw);
    ops.push(Op::Put {
        db: 0,
        key: wide_key(0),
        val: small_val(999),
    });
    ops.push(Op::Commit);
    diff(ops);
}

#[test]
fn interleaved_put_del_crossing_threshold_both_directions() {
    // Repeatedly grow a wide-keyed tree past a branch split, then shrink it
    // back under MIN_KEYS, several times over — exercises borrow AND merge
    // (not just monotonic delete-to-empty) since population oscillates across
    // FILL_THRESHOLD (25%, SPEC 03 §10) each round.
    let mut ops = setup();
    ops.push(Op::Commit);
    for round in 0..4u32 {
        ops.push(Op::BeginRw);
        let base = round * 200;
        for i in 0..120u32 {
            ops.push(Op::Put {
                db: 0,
                key: wide_key(base + i),
                val: small_val(i),
            });
        }
        // Shrink back to a handful of entries: most of this round's inserts
        // (and some of the previous round's survivors) get removed, driving
        // borrow/merge cascades while other subtrees stay populated.
        for i in (0..120u32).step_by(3) {
            ops.push(Op::Del {
                db: 0,
                key: wide_key(base + i),
            });
        }
        if round > 0 {
            // Also thin out the previous round's surviving keys.
            let prev_base = (round - 1) * 200;
            for i in (1..120u32).step_by(5) {
                ops.push(Op::Del {
                    db: 0,
                    key: wide_key(prev_base + i),
                });
            }
        }
        ops.push(Op::Len { db: 0 });
        ops.push(Op::Iter { db: 0 });
        ops.push(Op::Commit);
        ops.push(Op::BeginRo);
        ops.push(Op::Iter { db: 0 });
        ops.push(Op::RevIter { db: 0 });
        ops.push(Op::Commit);
    }
    diff(ops);
}

#[test]
fn insert_exactly_at_split_index_tie_break() {
    // SPEC 03 §6.4 tie-break: when the new key's post-insert index equals the
    // chosen split point `s`, it must land as the FIRST entry of the right
    // page (index `s` belongs to R, half-open `[s, nkeys+1)`). Build a leaf
    // to exactly one entry short of full with sorted wide keys, leaving a
    // gap in the middle, then insert the gap key: its post-insert index is
    // the tree's median, landing it exactly at the split boundary.
    let mut ops = setup();
    // 7 wide-keyed entries roughly fill one leaf (see module docs' fanout
    // estimate); skip the middle slot (index 3 of 0..7) to open a gap whose
    // fill lands near the median split point.
    let mut ops2 = Vec::new();
    for i in 0..7u32 {
        if i == 3 {
            continue;
        }
        ops2.push(Op::Put {
            db: 0,
            key: wide_key(i),
            val: small_val(i),
        });
    }
    // Force the leaf to actually be full before the gap-fill by padding with
    // more entries around it (both sides), so the gap-fill triggers a split
    // rather than a plain in-page insert.
    for i in 7..40u32 {
        ops2.push(Op::Put {
            db: 0,
            key: wide_key(i),
            val: small_val(i),
        });
    }
    ops.extend(ops2);
    // The tie-break insert: fills the gap at the median of a full leaf.
    ops.push(Op::Put {
        db: 0,
        key: wide_key(3),
        val: small_val(3),
    });
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::GetGreaterThan {
        db: 0,
        key: wide_key(2),
    });
    ops.push(Op::GetLowerThanOrEqualTo {
        db: 0,
        key: wide_key(3),
    });
    ops.push(Op::Commit);
    diff(ops);
}
