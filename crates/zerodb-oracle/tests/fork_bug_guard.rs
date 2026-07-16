//! Regression tests for the FORK-1 guard (`docs/UPSTREAM-BUGS.md`).
//!
//! The vendored LMDB fork SEGVs in `_mdb_cursor_put` when an `APPEND` put that
//! should return `KeyExist` runs in a write txn that has also `clear`ed another
//! db (see `examples/fork_segv_repro.rs` for the live crash, run per-variant in
//! a child process). The oracle cannot assert on UB, so `driver::classify`
//! skips the combination symmetrically. These tests pin the guard itself.
//!
//! Do not weaken or remove (CLAUDE.md rule 2). Remove ONLY when the fork fix
//! lands upstream and the repro example no longer crashes.

use zerodb_oracle::{run_self_test, DbName, Key, LmdbEngine, Op, PutFlag, Value};

/// The exact minimized crash sequence must now complete without a SEGV —
/// the APPEND puts after the clear classify as `Skip::KnownForkBug`.
#[test]
fn fork1_crash_sequence_is_guarded() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Named(40),
        },
        Op::Commit,
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        Op::ClearDb { db: 165 },
        Op::PutFlagged {
            db: 104,
            key: Key(vec![0xbe, 0x68]),
            val: Value(vec![]),
            flag: PutFlag::Append,
        },
        Op::PutFlagged {
            db: 104,
            key: Key(vec![0u8; 500]),
            val: Value(vec![0u8; 1_000_000]),
            flag: PutFlag::Append,
        },
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
    e.apply(&Op::CreateDb {
        name: DbName::Unnamed,
    });
    let r = e.apply(&Op::PutFlagged {
        db: 0,
        key: Key(vec![1]),
        val: Value(vec![2]),
        flag: PutFlag::Append,
    });
    assert!(matches!(r, zerodb_oracle::OpResult::Ok), "got {r:?}");
}

/// The guard resets at txn boundaries: clear, commit, then APPEND in the NEXT
/// txn executes normally.
#[test]
fn guard_resets_after_commit() {
    use zerodb_oracle::Engine;
    let mut e = LmdbEngine::new();
    e.apply(&Op::BeginRw);
    e.apply(&Op::CreateDb {
        name: DbName::Unnamed,
    });
    e.apply(&Op::ClearDb { db: 0 });
    e.apply(&Op::Commit);
    e.apply(&Op::BeginRw);
    let r = e.apply(&Op::PutFlagged {
        db: 0,
        key: Key(vec![1]),
        val: Value(vec![2]),
        flag: PutFlag::Append,
    });
    assert!(matches!(r, zerodb_oracle::OpResult::Ok), "got {r:?}");
}
