//! The FORK-1 sequences (`docs/UPSTREAM-BUGS.md`) as the oracle expresses
//! them, run on both engines with nothing skipped.
//!
//! LMDB crashes with `SIGSEGV` in `_mdb_cursor_put` when the true root
//! (unnamed) database is cleared or dropped and an empty named database then
//! gets two puts. The oracle's `DbName::Unnamed` is milli's named `"main"`
//! database, never the root, so no oracle op stream can empty the root: these
//! sequences clear a named database and LMDB answers them normally. Each test
//! would kill the test process if that stopped being true. The root-database
//! crash itself is reproduced by `examples/fork_segv_repro.rs`.

use zerodb_oracle::{assert_matches_lmdb, run_self_test, DbName, Op, PutFlag, ZerodbEngine};

/// Txn A creates `db0` (left empty); txn B opens `"main"`, clears it, runs
/// `puts` on `db0` (index 0) and commits.
fn after_clear(puts: &[Op]) -> Vec<Op> {
    let mut ops = vec![
        Op::BeginRw,
        Op::create_db(DbName::Named(40)),
        Op::Commit,
        Op::BeginRw,
        Op::create_db(DbName::Unnamed),
        Op::ClearDb { db: 1 },
    ];
    ops.extend_from_slice(puts);
    ops.push(Op::Commit);
    ops
}

fn check(ops: &[Op]) {
    run_self_test(ops).expect("LMDB must answer the sequence the same way twice");
    assert_matches_lmdb::<ZerodbEngine>(ops);
}

/// The sequence the fuzzer minimized when FORK-1 was found: the second
/// `APPEND` must fail with `KeyExist`.
#[test]
fn minimized_fuzz_sequence_runs() {
    check(&after_clear(&[
        Op::put_flagged(0, vec![0xbe, 0x68], vec![], PutFlag::Append),
        Op::put_flagged(0, vec![0u8; 500], vec![0u8; 1_000_000], PutFlag::Append),
    ]));
}

/// Every put kind after the clear, which on the root database would crash.
#[test]
fn every_put_kind_after_a_clear_runs() {
    check(&after_clear(&[
        Op::put(0, b"a", b"1"),
        Op::put_flagged(0, b"b", b"2", PutFlag::NoOverwrite),
        Op::put_reserved(0, b"c", b"3"),
        Op::put_flagged(0, b"d", b"4", PutFlag::Append),
        Op::put(0, b"e", b"5"),
    ]));
}

/// A drop instead of a clear, and puts in the txn after a committed clear.
#[test]
fn drop_and_committed_clear_run() {
    let mut ops = after_clear(&[]);
    ops.extend([
        Op::BeginRw,
        Op::put(0, b"a", b"1"),
        Op::put(0, b"b", b"2"),
        Op::DropDb { db: 1 },
        Op::put(0, b"c", b"3"),
        Op::put_flagged(0, b"d", b"4", PutFlag::Append),
        Op::Commit,
    ]);
    check(&ops);
}
