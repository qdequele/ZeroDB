//! Milestone 1.4 write-path differential tests: deterministic sequences that
//! pin every write op (put / put flags / put_reserved / del / clear /
//! iter_mut put_current / del_current) plus commit/abort visibility and reopen
//! durability, against the LMDB fork. Any divergence is a zerodb bug
//! (CLAUDE.md rule 1). The `ZerodbEngine` re-checks the on-disk image against
//! the SPEC 03 §11 invariant walk after every commit (debug builds).

use zerodb_oracle::{run, DbName, Key, LmdbEngine, Op, PutFlag, Value, ZerodbEngine};

fn diff(ops: Vec<Op>) {
    if let Err(d) = run::<LmdbEngine, ZerodbEngine>(&ops) {
        panic!("divergence:\n{d}");
    }
}

fn k(s: &str) -> Key {
    Key(s.as_bytes().to_vec())
}

fn v(s: &str) -> Value {
    Value(s.as_bytes().to_vec())
}

fn vbytes(b: u8, n: usize) -> Value {
    Value(vec![b; n])
}

fn setup() -> Vec<Op> {
    vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
    ]
}

#[test]
fn put_commit_verify_roundtrip() {
    let mut ops = setup();
    for i in 0..60u32 {
        ops.push(Op::Put {
            db: 0,
            key: k(&format!("key{i:03}")),
            val: v(&format!("val{i}")),
        });
    }
    // In-txn reads see the uncommitted state.
    ops.push(Op::Get {
        db: 0,
        key: k("key007"),
    });
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Iter { db: 0 });
    // VerifyGet reads the *committed* view — still empty.
    ops.push(Op::VerifyGet {
        db: 0,
        key: k("key007"),
    });
    ops.push(Op::Commit);
    ops.push(Op::VerifyGet {
        db: 0,
        key: k("key007"),
    });
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::RevIter { db: 0 });
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Commit);
    diff(ops);
}

#[test]
fn abort_discards_uncommitted_writes() {
    let mut ops = setup();
    ops.push(Op::Put {
        db: 0,
        key: k("committed"),
        val: v("1"),
    });
    ops.push(Op::Commit);
    ops.push(Op::BeginRw);
    ops.push(Op::Put {
        db: 0,
        key: k("doomed"),
        val: v("2"),
    });
    ops.push(Op::Del {
        db: 0,
        key: k("committed"),
    });
    ops.push(Op::Abort);
    ops.push(Op::VerifyGet {
        db: 0,
        key: k("doomed"),
    });
    ops.push(Op::VerifyGet {
        db: 0,
        key: k("committed"),
    });
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    diff(ops);
}

#[test]
fn append_flag_success_and_misuse() {
    let mut ops = setup();
    for key in ["a", "b", "c"] {
        ops.push(Op::PutFlagged {
            db: 0,
            key: k(key),
            val: v("x"),
            flag: PutFlag::Append,
        });
    }
    // Equal-to-last and less-than-last: KeyExist, value untouched.
    ops.push(Op::PutFlagged {
        db: 0,
        key: k("c"),
        val: v("overwrite-attempt"),
        flag: PutFlag::Append,
    });
    ops.push(Op::PutFlagged {
        db: 0,
        key: k("a"),
        val: v("overwrite-attempt"),
        flag: PutFlag::Append,
    });
    ops.push(Op::Get { db: 0, key: k("c") });
    ops.push(Op::Get { db: 0, key: k("a") });
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    diff(ops);
}

#[test]
fn no_overwrite_flag() {
    let mut ops = setup();
    ops.push(Op::Put {
        db: 0,
        key: k("dup"),
        val: v("original"),
    });
    ops.push(Op::PutFlagged {
        db: 0,
        key: k("dup"),
        val: v("blocked"),
        flag: PutFlag::NoOverwrite,
    });
    ops.push(Op::PutFlagged {
        db: 0,
        key: k("fresh"),
        val: v("ok"),
        flag: PutFlag::NoOverwrite,
    });
    ops.push(Op::Get {
        db: 0,
        key: k("dup"),
    });
    ops.push(Op::Commit);
    diff(ops);
}

#[test]
fn put_reserved_matches_put() {
    let mut ops = setup();
    ops.push(Op::PutReserved {
        db: 0,
        key: k("reserved-inline"),
        val: vbytes(0x21, 700),
    });
    // Overflow-sized reserved value.
    ops.push(Op::PutReserved {
        db: 0,
        key: k("reserved-big"),
        val: vbytes(0x42, 20_000),
    });
    // Overwrite an existing key through reserve.
    ops.push(Op::PutReserved {
        db: 0,
        key: k("reserved-inline"),
        val: vbytes(0x33, 700),
    });
    ops.push(Op::Get {
        db: 0,
        key: k("reserved-inline"),
    });
    ops.push(Op::Get {
        db: 0,
        key: k("reserved-big"),
    });
    ops.push(Op::Commit);
    ops.push(Op::VerifyGet {
        db: 0,
        key: k("reserved-big"),
    });
    diff(ops);
}

#[test]
fn delete_and_shape_after() {
    let mut ops = setup();
    for i in 0..120u32 {
        ops.push(Op::Put {
            db: 0,
            key: k(&format!("d{i:03}")),
            val: vbytes((i % 250) as u8, (i as usize % 500) + 1),
        });
    }
    for i in (0..120u32).step_by(2) {
        ops.push(Op::Del {
            db: 0,
            key: k(&format!("d{i:03}")),
        });
    }
    // Delete a missing key.
    ops.push(Op::Del {
        db: 0,
        key: k("d000"),
    });
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::PrefixIter {
        db: 0,
        prefix: k("d0"),
    });
    ops.push(Op::RevPrefixIter {
        db: 0,
        prefix: k("d1"),
    });
    ops.push(Op::Commit);
    diff(ops);
}

#[test]
fn clear_then_rebuild() {
    let mut ops = setup();
    for i in 0..40u32 {
        ops.push(Op::Put {
            db: 0,
            key: k(&format!("pre{i:02}")),
            val: v("x"),
        });
    }
    ops.push(Op::Commit);
    ops.push(Op::BeginRw);
    ops.push(Op::ClearDb { db: 0 });
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Put {
        db: 0,
        key: k("post-clear"),
        val: v("y"),
    });
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    diff(ops);
}

#[test]
fn iter_mut_put_current_positions() {
    let mut ops = setup();
    for i in 0..25u32 {
        ops.push(Op::Put {
            db: 0,
            key: k(&format!("m{i:02}")),
            val: v("orig"),
        });
    }
    // Rewrite at several positions: first, middle, last, and out of range.
    for nth in [0u8, 12, 24, 25, 200] {
        ops.push(Op::IterMutPutCurrent {
            db: 0,
            nth,
            val: v(&format!("rewritten-at-{nth}")),
        });
    }
    // Size-changing rewrite (forces delete+reinsert, possibly split).
    ops.push(Op::IterMutPutCurrent {
        db: 0,
        nth: 5,
        val: vbytes(0x66, 1500),
    });
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    diff(ops);
}

#[test]
fn iter_mut_del_current_positions() {
    let mut ops = setup();
    for i in 0..25u32 {
        ops.push(Op::Put {
            db: 0,
            key: k(&format!("m{i:02}")),
            val: v("orig"),
        });
    }
    for nth in [0u8, 10, 22, 30] {
        ops.push(Op::IterMutDelCurrent { db: 0, nth });
    }
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    diff(ops);
}

#[test]
fn overflow_values_differential() {
    let mut ops = setup();
    ops.push(Op::Put {
        db: 0,
        key: k("huge"),
        val: vbytes(0xAB, 60_000),
    });
    ops.push(Op::Put {
        db: 0,
        key: k("mid"),
        val: vbytes(0xCD, 5_000),
    });
    // Shrink an overflow value to inline, grow an inline to overflow.
    ops.push(Op::Put {
        db: 0,
        key: k("huge"),
        val: v("tiny-now"),
    });
    ops.push(Op::Put {
        db: 0,
        key: k("mid"),
        val: vbytes(0xEF, 50_000),
    });
    ops.push(Op::Get {
        db: 0,
        key: k("huge"),
    });
    ops.push(Op::Get {
        db: 0,
        key: k("mid"),
    });
    ops.push(Op::Del {
        db: 0,
        key: k("mid"),
    });
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    diff(ops);
}

#[test]
fn key_size_edges_on_write_ops() {
    let mut ops = setup();
    // Empty and oversized write keys → BadValSize on both engines.
    ops.push(Op::Put {
        db: 0,
        key: Key(vec![]),
        val: v("x"),
    });
    ops.push(Op::Put {
        db: 0,
        key: Key(vec![1u8; 512]),
        val: v("x"),
    });
    ops.push(Op::PutReserved {
        db: 0,
        key: Key(vec![]),
        val: v("x"),
    });
    // Max-size key works.
    ops.push(Op::Put {
        db: 0,
        key: Key(vec![7u8; 511]),
        val: v("max"),
    });
    // del: empty rejected, oversized finds nothing.
    ops.push(Op::Del {
        db: 0,
        key: Key(vec![]),
    });
    ops.push(Op::Del {
        db: 0,
        key: Key(vec![1u8; 600]),
    });
    ops.push(Op::Commit);
    diff(ops);
}

#[test]
fn reopen_persists_committed_state() {
    let mut ops = setup();
    for i in 0..30u32 {
        ops.push(Op::Put {
            db: 0,
            key: k(&format!("p{i:02}")),
            val: v(&format!("v{i}")),
        });
    }
    ops.push(Op::Commit);
    ops.push(Op::Reopen { map_size_kib: 64 });
    ops.push(Op::BeginRw);
    ops.push(Op::CreateDb {
        name: DbName::Unnamed,
    });
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Put {
        db: 0,
        key: k("post-reopen"),
        val: v("new"),
    });
    ops.push(Op::Commit);
    ops.push(Op::VerifyGet {
        db: 0,
        key: k("p07"),
    });
    ops.push(Op::VerifyGet {
        db: 0,
        key: k("post-reopen"),
    });
    diff(ops);
}

#[test]
fn multi_txn_churn_with_seeks() {
    let mut ops = setup();
    ops.push(Op::Commit);
    for round in 0..5u32 {
        ops.push(Op::BeginRw);
        for i in 0..40u32 {
            ops.push(Op::Put {
                db: 0,
                key: k(&format!("r{}k{i:02}", round % 2)),
                val: vbytes(round as u8, (i as usize * 13) % 900),
            });
        }
        for i in (0..40u32).step_by(3) {
            ops.push(Op::Del {
                db: 0,
                key: k(&format!("r{}k{i:02}", round % 2)),
            });
        }
        ops.push(Op::Commit);
        ops.push(Op::BeginRo);
        ops.push(Op::SetRange {
            db: 0,
            key: k("r0k1"),
        });
        ops.push(Op::GetGreaterThan {
            db: 0,
            key: k("r1k2"),
        });
        ops.push(Op::GetLowerThanOrEqualTo {
            db: 0,
            key: k("r1k99"),
        });
        ops.push(Op::First { db: 0 });
        ops.push(Op::Last { db: 0 });
        ops.push(Op::Len { db: 0 });
        ops.push(Op::Commit);
    }
    diff(ops);
}
