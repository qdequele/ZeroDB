//! Focused proptest for the M2.9 dbi-handle lifetime rule (ADR-0013, SPEC 04
//! TXN-68): random INTERLEAVINGS of create/open/abort/drop/use across a
//! small, fixed set of 3 database names, differential against the vendored
//! LMDB fork through BOTH the native pair and the `heed-zerodb` adapter pair.
//!
//! This complements (does not replace) the hand-picked scenarios in
//! `dbi_handle_lifetime.rs` and the general-purpose `Arbitrary`-driven fuzz
//! corpus (`crates/zerodb-oracle/fuzz`, `just fuzz-quick`): those two already
//! cover the WHOLE op model, including dbi-lifetime, at low density per
//! sequence over a huge input space. This file instead concentrates every
//! generated op on the lifecycle-relevant subset (`CreateDb`/`DropDb`/
//! `ClearDb`/`Commit`/`Abort`/`Get`/`Put`/`BeginRw`/`BeginRo`) over 40-op
//! sequences at proptest's higher per-run case count, which shrinks failures
//! to a minimal repro far better than a byte-blob `Arbitrary` corpus does —
//! valuable specifically because the state machine here (registry
//! generations, exported/unexported slots, per-txn binds) is small and
//! deterministic, exactly what shrinking exploits well.
//!
//! `db` indices are plain `u8`s uses modulo the engine's current *tracked*
//! handle count (harness convention, `op.rs`) — including DEAD handles
//! (M2.9: a handle whose creating txn aborted, or whose database was
//! `DropDb`'d, stays addressable until the next `CreateDb` purges it), so
//! these sequences keep driving real use-after-close differentials, not just
//! well-behaved ones.

use proptest::prelude::*;
use zerodb_oracle::{run, DbName, HeedZerodbEngine, Key, LmdbEngine, Op, Value, ZerodbEngine};

/// One lifecycle-relevant action. Kept separate from [`Op`] so the strategy
/// stays a flat, easily-shrinkable enum instead of fighting `Op`'s full
/// `Arbitrary` derive (which would spend most of its entropy on op classes
/// this file does not care about, e.g. cursor iteration or put flags).
#[derive(Debug, Clone)]
enum Action {
    BeginRw,
    BeginRo,
    Commit,
    Abort,
    BeginNestedRo,
    EndNestedRo,
    CreateDb(u8), // resolves to DbName::Named(n % 3)
    ClearDb(u8),
    DropDb(u8),
    Get(u8),
    Put(u8),
}

fn action_strategy() -> impl Strategy<Value = Action> {
    prop_oneof![
        3 => Just(Action::BeginRw),
        1 => Just(Action::BeginRo),
        3 => Just(Action::Commit),
        3 => Just(Action::Abort),
        1 => Just(Action::BeginNestedRo),
        1 => Just(Action::EndNestedRo),
        4 => (0u8..3).prop_map(Action::CreateDb),
        2 => any::<u8>().prop_map(Action::ClearDb),
        3 => any::<u8>().prop_map(Action::DropDb), // the TXN-68 event-2 driver
        3 => any::<u8>().prop_map(Action::Get),
        3 => any::<u8>().prop_map(Action::Put),
    ]
}

/// A short, fixed key/value pair — the byte content is irrelevant to this
/// rule (dbi-lifetime, not tree correctness), so keeping it constant lets
/// proptest's entropy budget go entirely towards the ACTION sequence, which
/// is what actually needs varying and shrinking here.
fn k() -> Key {
    Key(b"k".to_vec())
}
fn v() -> Value {
    Value(b"v".to_vec())
}

fn action_to_op(a: &Action) -> Op {
    match a {
        Action::BeginRw => Op::BeginRw,
        Action::BeginRo => Op::BeginRo,
        Action::Commit => Op::Commit,
        Action::Abort => Op::Abort,
        Action::BeginNestedRo => Op::BeginNestedRo,
        Action::EndNestedRo => Op::EndNestedRo,
        Action::CreateDb(n) => Op::CreateDb {
            name: DbName::Named(n % 3),
        },
        Action::ClearDb(db) => Op::ClearDb { db: *db },
        Action::DropDb(db) => Op::DropDb { db: *db },
        Action::Get(db) => Op::Get { db: *db, key: k() },
        Action::Put(db) => Op::Put {
            db: *db,
            key: k(),
            val: v(),
        },
    }
}

fn to_ops(actions: &[Action]) -> Vec<Op> {
    let mut ops: Vec<Op> = actions.iter().map(action_to_op).collect();
    // Always end with a clean shutdown so the last active txn (if any) is
    // resolved deterministically rather than dropped implicitly — matches
    // the hand-written scenarios' style and avoids the harness having to
    // guess an implicit-abort's shape at end-of-input.
    ops.push(Op::Commit);
    ops
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 300, ..ProptestConfig::default() })]

    /// Native pair: `LmdbEngine` vs `ZerodbEngine`.
    #[test]
    fn dbi_lifecycle_interleavings_native_pair(
        actions in proptest::collection::vec(action_strategy(), 1..40)
    ) {
        let ops = to_ops(&actions);
        if let Err(d) = run::<LmdbEngine, ZerodbEngine>(&ops) {
            prop_assert!(false, "native divergence:\n{d}");
        }
    }
}

proptest! {
    // Smaller case count: each case here additionally spins up a real
    // `heed-zerodb` adapter env on top of the LMDB one.
    #![proptest_config(ProptestConfig { cases: 120, ..ProptestConfig::default() })]

    /// Adapter pair: `LmdbEngine` vs `HeedZerodbEngine` (heed-zerodb) — the
    /// consumer-visible surface.
    #[test]
    fn dbi_lifecycle_interleavings_adapter_pair(
        actions in proptest::collection::vec(action_strategy(), 1..40)
    ) {
        let ops = to_ops(&actions);
        if let Err(d) = run::<LmdbEngine, HeedZerodbEngine>(&ops) {
            prop_assert!(false, "adapter divergence:\n{d}");
        }
    }
}
