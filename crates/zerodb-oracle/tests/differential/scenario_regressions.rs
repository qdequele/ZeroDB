//! Minimized scenarios ([`zerodb_oracle::scenario`]) that once diverged from
//! LMDB. Each one is a regression test: the scenario must run identically on
//! LMDB and ZeroDB.

use std::ops::Bound;

use zerodb_oracle::scenario::{
    run_scenario, CmpKind, OpenFlags, OrderedCase, OrderedOp, ReadOp, ReopenCase, ReopenOp, SKey,
    SVal, Scenario, SnapshotCase, SnapshotOp, WriteOp,
};

fn k(b: &[u8]) -> SKey {
    SKey(b.to_vec())
}

fn v(b: &[u8]) -> SVal {
    SVal(b.to_vec())
}

#[track_caller]
fn same(s: Scenario) {
    if let Err(d) = run_scenario(&s) {
        panic!("{d}\n{s:#?}");
    }
}

const PLAIN: OpenFlags = OpenFlags {
    read_only: false,
    prev_snapshot: false,
    no_lock: false,
    write_map: false,
};

/// heed's `get_greater_than` seeks with the database's comparator, then
/// keeps the entry only if it is greater than the key under memcmp, else
/// steps to the next one. With a custom comparator the result is not the
/// least entry strictly greater, and the adapter must return heed's.
#[test]
fn get_greater_than_with_custom_comparators() {
    for cmp in [
        CmpKind::Reverse,
        CmpKind::LenThenBytes,
        CmpKind::Integer { wide: false },
        CmpKind::Integer { wide: true },
    ] {
        let shape = zerodb_oracle::scenario::KeyShape::for_cmp(cmp);
        let mut ops = vec![OrderedOp::Write(WriteOp::Fill {
            start: 0,
            n: 40,
            vlen: 3,
        })];
        for i in [0u32, 5, 17, 39, 40, 300] {
            ops.push(OrderedOp::Read(ReadOp::Gt(shape.index_key(i))));
        }
        ops.push(OrderedOp::Read(ReadOp::Gt(k(
            &[0x41, 0x41, 0x01][..usize::from(!matches!(cmp, CmpKind::Integer { .. })) * 3]
        ))));
        ops.retain(|op| !matches!(op, OrderedOp::Read(ReadOp::Gt(SKey(b))) if b.is_empty()));
        same(Scenario::Ordered(OrderedCase { cmp, ops }));
    }
}

/// An empty key as the bound heed seeks to (the start of a forward range,
/// the end of a reverse one) fails with `BadValSize`; as the other bound it
/// is only compared. `delete_range`, and heed's loop written out over
/// `range_mut` or `rev_range_mut`, then fail with `EINVAL` from the
/// `del_current` on the unpositioned cursor.
#[test]
fn empty_range_bounds() {
    let e = || k(b"");
    let ops = vec![
        OrderedOp::Write(WriteOp::Put(k(b"a"), v(b"1"))),
        OrderedOp::Write(WriteOp::Put(k(b"b"), v(b"2"))),
        OrderedOp::Read(ReadOp::Range(Bound::Included(e()), Bound::Unbounded)),
        OrderedOp::Read(ReadOp::Range(
            Bound::Excluded(e()),
            Bound::Included(k(b"a")),
        )),
        OrderedOp::Read(ReadOp::Range(Bound::Unbounded, Bound::Excluded(e()))),
        OrderedOp::Read(ReadOp::RevRange(Bound::Unbounded, Bound::Excluded(e()))),
        OrderedOp::Read(ReadOp::RevRange(Bound::Included(e()), Bound::Unbounded)),
        OrderedOp::Write(WriteOp::DeleteRange(Bound::Included(e()), Bound::Unbounded)),
        OrderedOp::Write(WriteOp::DeleteRange(Bound::Unbounded, Bound::Excluded(e()))),
        OrderedOp::Write(WriteOp::CursorDeleteRange {
            lo: Bound::Included(e()),
            hi: Bound::Unbounded,
            rev: false,
        }),
        OrderedOp::Write(WriteOp::CursorDeleteRange {
            lo: Bound::Unbounded,
            hi: Bound::Excluded(e()),
            rev: true,
        }),
        OrderedOp::Read(ReadOp::Iter),
        OrderedOp::Commit,
        OrderedOp::Write(WriteOp::DeleteRange(Bound::Excluded(e()), Bound::Unbounded)),
        OrderedOp::Read(ReadOp::Iter),
    ];
    for cmp in [CmpKind::Default, CmpKind::Reverse, CmpKind::LenThenBytes] {
        same(Scenario::Ordered(OrderedCase {
            cmp,
            ops: ops.clone(),
        }));
    }
    same(Scenario::Snapshots(SnapshotCase {
        ops: vec![
            SnapshotOp::Write(WriteOp::Put(k(b"a"), v(b"1"))),
            SnapshotOp::Commit,
            SnapshotOp::OpenReader(0),
            SnapshotOp::Read(0, ReadOp::Range(Bound::Included(e()), Bound::Unbounded)),
        ],
    }));
}

/// A commit whose writes change nothing still commits a new transaction on
/// LMDB when it ran `clear`.
#[test]
fn noop_clear_commit_advances_txn_id() {
    same(Scenario::Reopen(ReopenCase {
        ops: vec![ReopenOp::Commit(vec![WriteOp::Clear]), ReopenOp::Info],
    }));
    same(Scenario::Reopen(ReopenCase {
        ops: vec![
            ReopenOp::Commit(vec![WriteOp::DeleteRange(
                Bound::Unbounded,
                Bound::Unbounded,
            )]),
            ReopenOp::Info,
        ],
    }));
    same(Scenario::Reopen(ReopenCase {
        ops: vec![
            ReopenOp::Commit(vec![WriteOp::Del(k(b"x"))]),
            ReopenOp::Info,
        ],
    }));
    same(Scenario::Reopen(ReopenCase {
        ops: vec![ReopenOp::Commit(vec![]), ReopenOp::Info],
    }));
}

/// `NO_LOCK` gives LMDB no reader table: `number_of_readers` stays 0.
#[test]
fn no_lock_reader_count() {
    same(Scenario::Reopen(ReopenCase {
        ops: vec![
            ReopenOp::Reopen {
                flags: OpenFlags {
                    no_lock: true,
                    ..PLAIN
                },
                map_units: None,
            },
            ReopenOp::Read(ReadOp::Len),
            ReopenOp::Info,
        ],
    }));
}

/// Opening with no map size adopts the persisted one.
#[test]
fn persisted_map_size() {
    same(Scenario::Reopen(ReopenCase {
        ops: vec![
            ReopenOp::Reopen {
                flags: PLAIN,
                map_units: Some(131),
            },
            ReopenOp::Commit(vec![WriteOp::Put(k(b"a"), v(b"1"))]),
            ReopenOp::Reopen {
                flags: PLAIN,
                map_units: None,
            },
            ReopenOp::Info,
        ],
    }));
}

/// Opening with a map smaller than the data.
#[test]
fn map_smaller_than_data() {
    same(Scenario::Reopen(ReopenCase {
        ops: vec![
            ReopenOp::Commit(vec![WriteOp::Fill {
                start: 0,
                n: 200,
                vlen: 30,
            }]),
            ReopenOp::Reopen {
                flags: PLAIN,
                map_units: Some(1),
            },
            ReopenOp::Info,
            ReopenOp::Read(ReadOp::Len),
        ],
    }));
}
