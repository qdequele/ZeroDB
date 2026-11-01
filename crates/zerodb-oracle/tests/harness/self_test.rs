//! Harness acceptance: a trivial op sequence through `run_self_test`
//! (LmdbEngine vs a second independent LmdbEngine), plus a few structured
//! sequences that exercise the harness surface.

use zerodb_oracle::{run_self_test, DbName, Op, PutFlag};

/// The harness acceptance test: open, put, get, commit, reopen, get.
#[test]
fn trivial_sequence_self_test() {
    let ops = vec![
        Op::BeginRw,
        Op::create_db(DbName::Unnamed),
        Op::put(0, b"hello", b"world"),
        Op::get(0, b"hello"),
        Op::Commit,
        // A fresh independent read sees the committed value.
        Op::verify_get(0, b"hello"),
        // Reopen the env (drop + reopen larger) and read again.
        Op::reopen(256),
        Op::verify_get(0, b"hello"),
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
        Op::create_db(DbName::Unnamed),
        Op::create_db(DbName::Named(1)),
        Op::put(0, b"b", b"2"),
        Op::put(0, b"a", b"1"),
        Op::put(0, b"c", b"3"),
        Op::Iter { db: 0 },
        Op::RevIter { db: 0 },
        Op::First { db: 0 },
        Op::Last { db: 0 },
        Op::set_range(0, b"aa"),
        Op::get_greater_than(0, b"b"),
        Op::get_lower_than_or_equal_to(0, b"b"),
        Op::prefix_iter(0, b""),
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
        Op::create_db(DbName::Unnamed),
        // APPEND ascending, then an out-of-order APPEND (must error KeyExist).
        Op::put_flagged(0, b"a", b"1", PutFlag::Append),
        Op::put_flagged(0, b"b", b"2", PutFlag::Append),
        Op::put_flagged(0, b"a", b"x", PutFlag::Append),
        // NO_OVERWRITE on an existing key (must error KeyExist).
        Op::put_flagged(0, b"b", b"z", PutFlag::NoOverwrite),
        Op::put_reserved(0, b"c", b"reserved"),
        Op::iter_mut_put_current(0, 1, b"rewritten"),
        Op::iter_mut_del_current(0, 0),
        Op::Iter { db: 0 },
        Op::del(0, b"c"),
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
        Op::create_db(DbName::Unnamed),
        Op::put(0, b"k", b"uncommitted"),
        // Nested reader sees the write txn's uncommitted put.
        Op::BeginNestedRo,
        Op::get(0, b"k"),
        Op::Iter { db: 0 },
        Op::EndNestedRo,
        // Back to the write txn.
        Op::put(0, b"k2", b"more"),
        Op::Commit,
        Op::verify_get(0, b"k"),
    ];
    assert!(run_self_test(&ops).is_ok());
}

/// Empty engine: ops that reference a db when none are open are structured
/// skips, and the two engines agree.
#[test]
fn empty_engine_skips_agree() {
    let ops = vec![
        Op::get(0, b"x"),
        Op::Iter { db: 0 },
        Op::Len { db: 3 },
        Op::Commit, // no txn
        Op::EndNestedRo,
        Op::BeginRw,
        Op::put(0, b"x", b"y"), // no db yet
        Op::Abort,
    ];
    assert!(run_self_test(&ops).is_ok());
}
