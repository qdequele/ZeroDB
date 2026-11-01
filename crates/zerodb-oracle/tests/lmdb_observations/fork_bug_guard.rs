//! Regression tests for the FORK-1 guard (`docs/UPSTREAM-BUGS.md`).
//!
//! The vendored LMDB fork SEGVs in `_mdb_cursor_put` when an `APPEND` put that
//! should return `KeyExist` runs in a write txn that has also `clear`ed another
//! db (see `examples/fork_segv_repro.rs` for the live crash, run per-variant in
//! a child process). The oracle cannot assert on UB, so `driver::classify`
//! skips the combination symmetrically. These tests pin the guard itself.
//!
//! Remove ONLY when the fork fix lands upstream and the repro example no
//! longer crashes.

use zerodb_oracle::{run_self_test, DbName, LmdbEngine, Op, PutFlag};

/// The exact minimized crash sequence must complete without a SEGV —
/// the APPEND puts after the clear classify as `Skip::KnownForkBug`.
#[test]
fn fork1_crash_sequence_is_guarded() {
    let ops = vec![
        Op::BeginRw,
        Op::create_db(DbName::Named(40)),
        Op::Commit,
        Op::BeginRw,
        Op::create_db(DbName::Unnamed),
        Op::ClearDb { db: 165 },
        Op::put_flagged(104, vec![0xbe, 0x68], vec![], PutFlag::Append),
        Op::put_flagged(104, vec![0u8; 500], vec![0u8; 1_000_000], PutFlag::Append),
        Op::Commit,
    ];
    // Would SIGSEGV the whole test process without the guard.
    run_self_test(&ops).expect("guarded sequence must not diverge");
}

/// APPEND with no prior clear in the txn is NOT guarded — coverage stays.
#[test]
fn append_without_clear_still_executes() {
    use zerodb_oracle::Engine;
    let mut e = LmdbEngine::new();
    assert!(matches!(e.apply(&Op::BeginRw), zerodb_oracle::OpResult::Ok));
    e.apply(&Op::create_db(DbName::Unnamed));
    let r = e.apply(&Op::put_flagged(0, vec![1], vec![2], PutFlag::Append));
    assert!(matches!(r, zerodb_oracle::OpResult::Ok), "got {r:?}");
}

/// The guard resets at txn boundaries: clear, commit, then APPEND in the NEXT
/// txn executes normally.
#[test]
fn guard_resets_after_commit() {
    use zerodb_oracle::Engine;
    let mut e = LmdbEngine::new();
    e.apply(&Op::BeginRw);
    e.apply(&Op::create_db(DbName::Unnamed));
    e.apply(&Op::ClearDb { db: 0 });
    e.apply(&Op::Commit);
    e.apply(&Op::BeginRw);
    let r = e.apply(&Op::put_flagged(0, vec![1], vec![2], PutFlag::Append));
    assert!(matches!(r, zerodb_oracle::OpResult::Ok), "got {r:?}");
}
