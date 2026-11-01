//! Differential tests: nested read transactions over a write
//! txn (`BeginNestedRo`/`EndNestedRo`), zerodb vs the Meilisearch LMDB fork
//! (the only LMDB with this feature — ITS#10395, SPEC 04 §5).
//!
//! Acceptance: parity on write-then-nested-read sequences including
//! reads of **uncommitted** state. The op model drives one child at a time
//! (`TxnState::RwNested`); while the child lives, every read is served
//! through it on both engines and every write op is classified
//! `Skip::WriteBlockedByNested` by the shared driver **before either engine
//! runs** — the fork technically allows a write under a live child, zerodb
//! forbids it, and the symmetric skip keeps that divergence unobservable
//! (writer quiescence, docs/LMDB-COMPATIBILITY.md). Multi-child + real-thread fan-out
//! parity is covered by `crates/zerodb/tests/nested_fanout.rs`.

use zerodb_oracle::{assert_matches_lmdb, DbName, Op, PutFlag, ZerodbEngine};

/// Begin a write txn on the unnamed DB and stage some uncommitted entries.
fn staged(pairs: &[(&[u8], &[u8])]) -> Vec<Op> {
    let mut ops = vec![Op::BeginRw, Op::create_db(DbName::Unnamed)];
    for (key, val) in pairs {
        ops.push(Op::put(0, key, val));
    }
    ops
}

/// TXN-26: a nested child sees the writer's uncommitted puts — gets, seeks,
/// iteration, len — none of it visible to any committed snapshot yet.
#[test]
fn nested_child_sees_uncommitted_state() {
    let mut ops = staged(&[
        (b"apple", b"1"),
        (b"banana", b"2"),
        (b"cherry", b"3"),
        (b"date", b"4"),
    ]);
    ops.extend([
        Op::BeginNestedRo,
        Op::get(0, b"banana"),
        Op::get(0, b"missing"),
        Op::Len { db: 0 },
        Op::IsEmpty { db: 0 },
        Op::First { db: 0 },
        Op::Last { db: 0 },
        Op::set_range(0, b"b"),
        Op::get_greater_than(0, b"banana"),
        Op::get_lower_than_or_equal_to(0, b"cc"),
        Op::Iter { db: 0 },
        Op::RevIter { db: 0 },
        Op::prefix_iter(0, b"c"),
        Op::EndNestedRo,
        Op::Commit,
    ]);
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

/// The paused-writer window: writes attempted while the child lives are the
/// symmetric `WriteBlockedByNested` skip (writer quiescence); after `EndNestedRo` the
/// writer resumes for real, and a second child sees both generations.
#[test]
fn write_blocked_while_nested_then_resume() {
    let mut ops = staged(&[(b"gen1", b"a")]);
    ops.extend([
        Op::BeginNestedRo,
        // All classified Skip::WriteBlockedByNested by the shared driver —
        // neither engine executes them.
        Op::put(0, b"blocked", b"x"),
        Op::put_flagged(0, b"blocked", b"x", PutFlag::NoOverwrite),
        Op::del(0, b"gen1"),
        Op::ClearDb { db: 0 },
        Op::get(0, b"gen1"),
        Op::EndNestedRo,
        // Writer resumed: this put must succeed on both engines.
        Op::put(0, b"gen2", b"b"),
        Op::BeginNestedRo,
        Op::get(0, b"gen1"),
        Op::get(0, b"gen2"),
        Op::Iter { db: 0 },
        Op::EndNestedRo,
        Op::Commit,
        Op::BeginRo,
        Op::Iter { db: 0 },
        Op::Commit,
    ]);
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

/// Uncommitted state a child must see through *dirty overflow runs*: a value
/// far above the inline threshold, staged but not committed.
#[test]
fn nested_child_reads_dirty_overflow_values() {
    let big1 = vec![0x5A_u8; 20 * 1024]; // multi-page overflow, dirty
    let big2 = vec![0xC3_u8; 5 * 1024];
    let mut ops = vec![
        Op::BeginRw,
        Op::create_db(DbName::Unnamed),
        Op::put(0, b"big1", big1),
        Op::put(0, b"small", b"s"),
        Op::put(0, b"big2", big2),
    ];
    ops.extend([
        Op::BeginNestedRo,
        Op::get(0, b"big1"),
        Op::get(0, b"big2"),
        Op::Iter { db: 0 },
        Op::EndNestedRo,
        Op::Commit,
        Op::BeginRo,
        Op::get(0, b"big1"),
        Op::Commit,
    ]);
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

/// Mixed committed + uncommitted view (TXN-27 both arms): the child reads
/// pages the writer copied (dirty) and pages it never touched (mapped).
#[test]
fn nested_child_merged_committed_and_dirty_view() {
    let mut ops = staged(&[(b"committed-a", b"1"), (b"committed-b", b"2")]);
    ops.push(Op::Commit);
    ops.extend([
        Op::BeginRw,
        Op::put(0, b"dirty-c", b"3"),
        Op::del(0, b"committed-a"),
        Op::BeginNestedRo,
        // deleted in the uncommitted view
        Op::get(0, b"committed-a"),
        // untouched, via the map
        Op::get(0, b"committed-b"),
        // uncommitted, via a dirty frame
        Op::get(0, b"dirty-c"),
        Op::Iter { db: 0 },
        Op::Len { db: 0 },
        Op::EndNestedRo,
        Op::Commit,
    ]);
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

/// Named DBs through a child: a DB created in this very txn (uncommitted
/// catalog entry) is readable through the nested child on both engines.
#[test]
fn nested_child_reads_uncommitted_named_db() {
    let ops = vec![
        Op::BeginRw,
        Op::create_db(DbName::Named(1)),
        Op::put(0, b"in-named", b"v"),
        Op::BeginNestedRo,
        Op::get(0, b"in-named"),
        Op::Len { db: 0 },
        Op::Iter { db: 0 },
        Op::EndNestedRo,
        Op::Commit,
        Op::BeginRo,
        Op::get(0, b"in-named"),
        Op::Commit,
    ];
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

/// Lifecycle edges, all driver-classified symmetric skips: nesting without a
/// write txn, nesting twice, ending with nothing to end — plus Commit and
/// Abort while a child is live (legal: the engines drop the child first,
/// TXN-33 order).
#[test]
fn nested_lifecycle_edges() {
    // BeginNestedRo with no txn / with a read txn: NoWriteTxnForNested.
    assert_matches_lmdb::<ZerodbEngine>(&[
        Op::BeginNestedRo,
        Op::BeginRo,
        Op::BeginNestedRo,
        Op::Commit,
    ]);
    // EndNestedRo with nothing to end: NoNestedToEnd.
    assert_matches_lmdb::<ZerodbEngine>(&[
        Op::EndNestedRo,
        Op::BeginRw,
        Op::EndNestedRo,
        Op::Commit,
    ]);
    // Double BeginNestedRo: the second is classified in RwNested state →
    // NoWriteTxnForNested (the op model holds one child at a time).
    let mut ops = staged(&[(b"k", b"v")]);
    ops.extend([
        Op::BeginNestedRo,
        Op::BeginNestedRo,
        Op::EndNestedRo,
        Op::Commit,
    ]);
    assert_matches_lmdb::<ZerodbEngine>(&ops);
    // Commit while nested: child dropped, then parent commits; data lands.
    let mut ops = staged(&[(b"k", b"v")]);
    ops.extend([
        Op::BeginNestedRo,
        Op::Commit,
        Op::BeginRo,
        Op::get(0, b"k"),
        Op::Commit,
    ]);
    assert_matches_lmdb::<ZerodbEngine>(&ops);
    // Abort while nested: child dropped, parent rolls back; data gone.
    let mut ops = staged(&[(b"k", b"v")]);
    ops.extend([
        Op::BeginNestedRo,
        Op::Abort,
        Op::BeginRo,
        Op::get(0, b"k"),
        Op::Commit,
    ]);
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}

/// A reopen while nested is a hard txn boundary on both engines (the child
/// and parent are torn down together, drop order child-first).
#[test]
fn reopen_while_nested_is_symmetric() {
    let mut ops = staged(&[(b"k", b"v")]);
    ops.extend([
        Op::BeginNestedRo,
        Op::reopen(0),
        Op::BeginRo,
        // uncommitted at reopen → gone on both engines
        Op::get(0, b"k"),
        Op::Commit,
    ]);
    assert_matches_lmdb::<ZerodbEngine>(&ops);
}
