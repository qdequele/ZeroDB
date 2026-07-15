//! Milestone 0.3 acceptance: a trivial op sequence through `run_self_test`
//! (LmdbEngine vs a second independent LmdbEngine), plus a few structured
//! sequences that exercise the harness surface.

use zerodb_oracle::{run_self_test, DbName, Op, PutFlag};

fn k(bytes: &[u8]) -> zerodb_oracle::Key {
    zerodb_oracle::Key(bytes.to_vec())
}
fn v(bytes: &[u8]) -> zerodb_oracle::Value {
    zerodb_oracle::Value(bytes.to_vec())
}

/// The milestone acceptance test: open, put, get, commit, reopen, get.
#[test]
fn trivial_sequence_self_test() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        Op::Put {
            db: 0,
            key: k(b"hello"),
            val: v(b"world"),
        },
        Op::Get {
            db: 0,
            key: k(b"hello"),
        },
        Op::Commit,
        // A fresh independent read sees the committed value.
        Op::VerifyGet {
            db: 0,
            key: k(b"hello"),
        },
        // Reopen the env (drop + reopen larger) and read again.
        Op::Reopen { map_size_kib: 256 },
        Op::VerifyGet {
            db: 0,
            key: k(b"hello"),
        },
    ];
    if let Err(d) = run_self_test(&ops) {
        panic!("self-test diverged:\n{d}");
    }
}

/// Exercises named + unnamed databases, iteration order, and neighbor seeks.
#[test]
fn multi_db_iteration_and_seeks() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        Op::CreateDb {
            name: DbName::Named(1),
        },
        Op::Put {
            db: 0,
            key: k(b"b"),
            val: v(b"2"),
        },
        Op::Put {
            db: 0,
            key: k(b"a"),
            val: v(b"1"),
        },
        Op::Put {
            db: 0,
            key: k(b"c"),
            val: v(b"3"),
        },
        Op::Iter { db: 0 },
        Op::RevIter { db: 0 },
        Op::First { db: 0 },
        Op::Last { db: 0 },
        Op::SetRange {
            db: 0,
            key: k(b"aa"),
        },
        Op::GetGreaterThan {
            db: 0,
            key: k(b"b"),
        },
        Op::GetLowerThanOrEqualTo {
            db: 0,
            key: k(b"b"),
        },
        Op::PrefixIter {
            db: 0,
            prefix: k(b""),
        },
        Op::Len { db: 0 },
        Op::IsEmpty { db: 1 },
        Op::Commit,
    ];
    assert!(run_self_test(&ops).is_ok());
}

/// Exercises write flags, reserved puts, delete, clear, and in-place mutation.
#[test]
fn write_flags_and_cursor_mutation() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        // APPEND ascending, then an out-of-order APPEND (must error KeyExist).
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
        Op::PutFlagged {
            db: 0,
            key: k(b"a"),
            val: v(b"x"),
            flag: PutFlag::Append,
        },
        // NO_OVERWRITE on an existing key (must error KeyExist).
        Op::PutFlagged {
            db: 0,
            key: k(b"b"),
            val: v(b"z"),
            flag: PutFlag::NoOverwrite,
        },
        Op::PutReserved {
            db: 0,
            key: k(b"c"),
            val: v(b"reserved"),
        },
        Op::IterMutPutCurrent {
            db: 0,
            nth: 1,
            val: v(b"rewritten"),
        },
        Op::IterMutDelCurrent { db: 0, nth: 0 },
        Op::Iter { db: 0 },
        Op::Del {
            db: 0,
            key: k(b"c"),
        },
        Op::ClearDb { db: 0 },
        Op::Len { db: 0 },
        Op::Commit,
    ];
    assert!(run_self_test(&ops).is_ok());
}

/// Exercises nested read txns over a write txn (fork-only) reading uncommitted
/// state (SPEC 00 row 16, SPEC 01 §S9).
#[test]
fn nested_read_over_write_txn() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        Op::Put {
            db: 0,
            key: k(b"k"),
            val: v(b"uncommitted"),
        },
        // Nested reader sees the write txn's uncommitted put.
        Op::BeginNestedRo,
        Op::Get {
            db: 0,
            key: k(b"k"),
        },
        Op::Iter { db: 0 },
        Op::EndNestedRo,
        // Back to the write txn.
        Op::Put {
            db: 0,
            key: k(b"k2"),
            val: v(b"more"),
        },
        Op::Commit,
        Op::VerifyGet {
            db: 0,
            key: k(b"k"),
        },
    ];
    assert!(run_self_test(&ops).is_ok());
}

/// Empty engine: ops that reference a db when none are open are structured
/// skips, and the two engines agree.
#[test]
fn empty_engine_skips_agree() {
    let ops = vec![
        Op::Get {
            db: 0,
            key: k(b"x"),
        },
        Op::Iter { db: 0 },
        Op::Len { db: 3 },
        Op::Commit, // no txn
        Op::EndNestedRo,
        Op::BeginRw,
        Op::Put {
            db: 0,
            key: k(b"x"),
            val: v(b"y"),
        }, // no db yet
        Op::Abort,
    ];
    assert!(run_self_test(&ops).is_ok());
}
