//! Write-path differential tests: deterministic sequences that
//! pin every write op (put / put flags / put_reserved / del / clear /
//! iter_mut put_current / del_current) plus commit/abort visibility and reopen
//! durability, against the LMDB fork. Any divergence is a zerodb bug. The
//! `ZerodbEngine` re-checks the on-disk image against
//! the SPEC 03 §11 invariant walk after every commit (debug builds).

use zerodb_oracle::{assert_matches_lmdb, DbName, Op, PutFlag, Value, ZerodbEngine};

fn vbytes(b: u8, n: usize) -> Value {
    Value(vec![b; n])
}

fn setup() -> Vec<Op> {
    vec![Op::BeginRw, Op::create_db(DbName::Unnamed)]
}

#[test]
fn put_commit_verify_roundtrip() {
    let mut ops = setup();
    for i in 0..60u32 {
        ops.push(Op::put(0, format!("key{i:03}"), format!("val{i}")));
    }
    // In-txn reads see the uncommitted state.
    ops.push(Op::get(0, "key007"));
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Iter { db: 0 });
    // VerifyGet reads the *committed* view — still empty.
    ops.push(Op::verify_get(0, "key007"));
    ops.push(Op::Commit);
    ops.push(Op::verify_get(0, "key007"));
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::RevIter { db: 0 });
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Commit);
    assert_matches_lmdb::<ZerodbEngine>(ops);
}

#[test]
fn abort_discards_uncommitted_writes() {
    let mut ops = setup();
    ops.push(Op::put(0, "committed", "1"));
    ops.push(Op::Commit);
    ops.push(Op::BeginRw);
    ops.push(Op::put(0, "doomed", "2"));
    ops.push(Op::del(0, "committed"));
    ops.push(Op::Abort);
    ops.push(Op::verify_get(0, "doomed"));
    ops.push(Op::verify_get(0, "committed"));
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    assert_matches_lmdb::<ZerodbEngine>(ops);
}

#[test]
fn append_flag_success_and_misuse() {
    let mut ops = setup();
    for key in ["a", "b", "c"] {
        ops.push(Op::put_flagged(0, key, "x", PutFlag::Append));
    }
    // Equal-to-last and less-than-last: KeyExist, value untouched.
    ops.push(Op::put_flagged(
        0,
        "c",
        "overwrite-attempt",
        PutFlag::Append,
    ));
    ops.push(Op::put_flagged(
        0,
        "a",
        "overwrite-attempt",
        PutFlag::Append,
    ));
    ops.push(Op::get(0, "c"));
    ops.push(Op::get(0, "a"));
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    assert_matches_lmdb::<ZerodbEngine>(ops);
}

#[test]
fn no_overwrite_flag() {
    let mut ops = setup();
    ops.push(Op::put(0, "dup", "original"));
    ops.push(Op::put_flagged(0, "dup", "blocked", PutFlag::NoOverwrite));
    ops.push(Op::put_flagged(0, "fresh", "ok", PutFlag::NoOverwrite));
    ops.push(Op::get(0, "dup"));
    ops.push(Op::Commit);
    assert_matches_lmdb::<ZerodbEngine>(ops);
}

#[test]
fn put_reserved_matches_put() {
    let mut ops = setup();
    ops.push(Op::put_reserved(0, "reserved-inline", vbytes(0x21, 700)));
    // Overflow-sized reserved value.
    ops.push(Op::put_reserved(0, "reserved-big", vbytes(0x42, 20_000)));
    // Overwrite an existing key through reserve.
    ops.push(Op::put_reserved(0, "reserved-inline", vbytes(0x33, 700)));
    ops.push(Op::get(0, "reserved-inline"));
    ops.push(Op::get(0, "reserved-big"));
    ops.push(Op::Commit);
    ops.push(Op::verify_get(0, "reserved-big"));
    assert_matches_lmdb::<ZerodbEngine>(ops);
}

#[test]
fn delete_and_shape_after() {
    let mut ops = setup();
    for i in 0..120u32 {
        ops.push(Op::put(
            0,
            format!("d{i:03}"),
            vbytes((i % 250) as u8, (i as usize % 500) + 1),
        ));
    }
    for i in (0..120u32).step_by(2) {
        ops.push(Op::del(0, format!("d{i:03}")));
    }
    // Delete a missing key.
    ops.push(Op::del(0, "d000"));
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::prefix_iter(0, "d0"));
    ops.push(Op::rev_prefix_iter(0, "d1"));
    ops.push(Op::Commit);
    assert_matches_lmdb::<ZerodbEngine>(ops);
}

#[test]
fn clear_then_rebuild() {
    let mut ops = setup();
    for i in 0..40u32 {
        ops.push(Op::put(0, format!("pre{i:02}"), "x"));
    }
    ops.push(Op::Commit);
    ops.push(Op::BeginRw);
    ops.push(Op::ClearDb { db: 0 });
    ops.push(Op::Len { db: 0 });
    ops.push(Op::put(0, "post-clear", "y"));
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    assert_matches_lmdb::<ZerodbEngine>(ops);
}

#[test]
fn iter_mut_put_current_positions() {
    let mut ops = setup();
    for i in 0..25u32 {
        ops.push(Op::put(0, format!("m{i:02}"), "orig"));
    }
    // Rewrite at several positions: first, middle, last, and out of range.
    for nth in [0u8, 12, 24, 25, 200] {
        ops.push(Op::iter_mut_put_current(
            0,
            nth,
            format!("rewritten-at-{nth}"),
        ));
    }
    // Size-changing rewrite (forces delete+reinsert, possibly split).
    ops.push(Op::iter_mut_put_current(0, 5, vbytes(0x66, 1500)));
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    assert_matches_lmdb::<ZerodbEngine>(ops);
}

#[test]
fn iter_mut_del_current_positions() {
    let mut ops = setup();
    for i in 0..25u32 {
        ops.push(Op::put(0, format!("m{i:02}"), "orig"));
    }
    for nth in [0u8, 10, 22, 30] {
        ops.push(Op::iter_mut_del_current(0, nth));
    }
    ops.push(Op::Len { db: 0 });
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    assert_matches_lmdb::<ZerodbEngine>(ops);
}

#[test]
fn overflow_values_differential() {
    let mut ops = setup();
    ops.push(Op::put(0, "huge", vbytes(0xAB, 60_000)));
    ops.push(Op::put(0, "mid", vbytes(0xCD, 5_000)));
    // Shrink an overflow value to inline, grow an inline to overflow.
    ops.push(Op::put(0, "huge", "tiny-now"));
    ops.push(Op::put(0, "mid", vbytes(0xEF, 50_000)));
    ops.push(Op::get(0, "huge"));
    ops.push(Op::get(0, "mid"));
    ops.push(Op::del(0, "mid"));
    ops.push(Op::Commit);
    ops.push(Op::BeginRo);
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::Commit);
    assert_matches_lmdb::<ZerodbEngine>(ops);
}

#[test]
fn key_size_edges_on_write_ops() {
    let mut ops = setup();
    // Empty and oversized write keys → BadValSize on both engines.
    ops.push(Op::put(0, vec![], "x"));
    ops.push(Op::put(0, vec![1u8; 512], "x"));
    ops.push(Op::put_reserved(0, vec![], "x"));
    // Max-size key works.
    ops.push(Op::put(0, vec![7u8; 511], "max"));
    // del: empty rejected, oversized finds nothing.
    ops.push(Op::del(0, vec![]));
    ops.push(Op::del(0, vec![1u8; 600]));
    ops.push(Op::Commit);
    assert_matches_lmdb::<ZerodbEngine>(ops);
}

#[test]
fn reopen_persists_committed_state() {
    let mut ops = setup();
    for i in 0..30u32 {
        ops.push(Op::put(0, format!("p{i:02}"), format!("v{i}")));
    }
    ops.push(Op::Commit);
    ops.push(Op::reopen(64));
    ops.push(Op::BeginRw);
    ops.push(Op::create_db(DbName::Unnamed));
    ops.push(Op::Iter { db: 0 });
    ops.push(Op::put(0, "post-reopen", "new"));
    ops.push(Op::Commit);
    ops.push(Op::verify_get(0, "p07"));
    ops.push(Op::verify_get(0, "post-reopen"));
    assert_matches_lmdb::<ZerodbEngine>(ops);
}

#[test]
fn multi_txn_churn_with_seeks() {
    let mut ops = setup();
    ops.push(Op::Commit);
    for round in 0..5u32 {
        ops.push(Op::BeginRw);
        for i in 0..40u32 {
            ops.push(Op::put(
                0,
                format!("r{}k{i:02}", round % 2),
                vbytes(round as u8, (i as usize * 13) % 900),
            ));
        }
        for i in (0..40u32).step_by(3) {
            ops.push(Op::del(0, format!("r{}k{i:02}", round % 2)));
        }
        ops.push(Op::Commit);
        ops.push(Op::BeginRo);
        ops.push(Op::set_range(0, "r0k1"));
        ops.push(Op::get_greater_than(0, "r1k2"));
        ops.push(Op::get_lower_than_or_equal_to(0, "r1k99"));
        ops.push(Op::First { db: 0 });
        ops.push(Op::Last { db: 0 });
        ops.push(Op::Len { db: 0 });
        ops.push(Op::Commit);
    }
    assert_matches_lmdb::<ZerodbEngine>(ops);
}
