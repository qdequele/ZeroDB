//! Milestone 1.4 randomized **write-workload** differential (PLAN §1.4
//! acceptance: randomized single-threaded put/del/commit/abort workloads).
//!
//! Unlike `read_proptest` (raw `decode_ops` bytes, whose op density is
//! whatever `Arbitrary` yields), this generator is **write-biased and
//! collision-heavy**: keys come from a 24-slot pool so puts overwrite, deletes
//! hit, APPEND misfires, and `iter_mut` positions land on real entries; ops
//! are wrapped in write-txn blocks ending in Commit or Abort. The
//! `ZerodbEngine` runs the SPEC 03 §11 invariant walk on the committed image
//! after every commit (debug builds), so every proptest case is also an
//! invariant-checker run.

use proptest::prelude::*;
use zerodb_oracle::{run, DbName, Key, LmdbEngine, Op, PutFlag, Value, ZerodbEngine};

/// One simplified mutation/read step inside a write txn.
#[derive(Debug, Clone)]
enum Step {
    Put { slot: u8, val: Value },
    PutAppend { slot: u8, val: Value },
    PutNoOverwrite { slot: u8, val: Value },
    PutReserved { slot: u8, val: Value },
    Del { slot: u8 },
    Clear,
    IterMutPut { nth: u8, val: Value },
    IterMutDel { nth: u8 },
    Get { slot: u8 },
    Iter,
    RevIter,
    Prefix { slot: u8 },
    Len,
    First,
    Last,
    SeekGe { slot: u8 },
    SeekGt { slot: u8 },
    SeekLe { slot: u8 },
}

fn pool_key(slot: u8) -> Key {
    Key(format!("pool-key-{:02}", slot % 24).into_bytes())
}

fn arb_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        8 => proptest::collection::vec(any::<u8>(), 0..80).prop_map(Value),
        1 => (1_000usize..20_000, any::<u8>()).prop_map(|(n, b)| Value(vec![b; n])),
    ]
}

fn arb_step() -> impl Strategy<Value = Step> {
    prop_oneof![
        6 => (any::<u8>(), arb_value()).prop_map(|(slot, val)| Step::Put { slot, val }),
        2 => (any::<u8>(), arb_value()).prop_map(|(slot, val)| Step::PutAppend { slot, val }),
        2 => (any::<u8>(), arb_value()).prop_map(|(slot, val)| Step::PutNoOverwrite { slot, val }),
        2 => (any::<u8>(), arb_value()).prop_map(|(slot, val)| Step::PutReserved { slot, val }),
        4 => any::<u8>().prop_map(|slot| Step::Del { slot }),
        1 => Just(Step::Clear),
        2 => (any::<u8>(), arb_value()).prop_map(|(nth, val)| Step::IterMutPut { nth: nth % 40, val }),
        2 => any::<u8>().prop_map(|nth| Step::IterMutDel { nth: nth % 40 }),
        2 => any::<u8>().prop_map(|slot| Step::Get { slot }),
        1 => Just(Step::Iter),
        1 => Just(Step::RevIter),
        1 => any::<u8>().prop_map(|slot| Step::Prefix { slot }),
        1 => Just(Step::Len),
        1 => Just(Step::First),
        1 => Just(Step::Last),
        1 => any::<u8>().prop_map(|slot| Step::SeekGe { slot }),
        1 => any::<u8>().prop_map(|slot| Step::SeekGt { slot }),
        1 => any::<u8>().prop_map(|slot| Step::SeekLe { slot }),
    ]
}

/// A whole scenario: txn blocks of steps, each committed or aborted, with
/// post-txn verification reads.
fn arb_scenario() -> impl Strategy<Value = Vec<Op>> {
    proptest::collection::vec(
        (
            proptest::collection::vec(arb_step(), 1..24),
            any::<bool>(), // commit or abort
            any::<u8>(),   // post-txn verify slot
        ),
        1..6,
    )
    .prop_map(|blocks| {
        let mut ops = vec![
            Op::BeginRw,
            Op::CreateDb {
                name: DbName::Unnamed,
            },
            Op::Commit,
        ];
        for (steps, commit, verify_slot) in blocks {
            ops.push(Op::BeginRw);
            for s in steps {
                ops.push(step_to_op(s));
            }
            ops.push(if commit { Op::Commit } else { Op::Abort });
            ops.push(Op::VerifyGet {
                db: 0,
                key: pool_key(verify_slot),
            });
            ops.push(Op::BeginRo);
            ops.push(Op::Iter { db: 0 });
            ops.push(Op::Len { db: 0 });
            ops.push(Op::Commit);
        }
        ops
    })
}

fn step_to_op(s: Step) -> Op {
    match s {
        Step::Put { slot, val } => Op::Put {
            db: 0,
            key: pool_key(slot),
            val,
        },
        Step::PutAppend { slot, val } => Op::PutFlagged {
            db: 0,
            key: pool_key(slot),
            val,
            flag: PutFlag::Append,
        },
        Step::PutNoOverwrite { slot, val } => Op::PutFlagged {
            db: 0,
            key: pool_key(slot),
            val,
            flag: PutFlag::NoOverwrite,
        },
        Step::PutReserved { slot, val } => Op::PutReserved {
            db: 0,
            key: pool_key(slot),
            val,
        },
        Step::Del { slot } => Op::Del {
            db: 0,
            key: pool_key(slot),
        },
        Step::Clear => Op::ClearDb { db: 0 },
        Step::IterMutPut { nth, val } => Op::IterMutPutCurrent { db: 0, nth, val },
        Step::IterMutDel { nth } => Op::IterMutDelCurrent { db: 0, nth },
        Step::Get { slot } => Op::Get {
            db: 0,
            key: pool_key(slot),
        },
        Step::Iter => Op::Iter { db: 0 },
        Step::RevIter => Op::RevIter { db: 0 },
        Step::Prefix { slot } => Op::PrefixIter {
            db: 0,
            prefix: Key(format!("pool-key-{}", slot % 3).into_bytes()),
        },
        Step::Len => Op::Len { db: 0 },
        Step::First => Op::First { db: 0 },
        Step::Last => Op::Last { db: 0 },
        Step::SeekGe { slot } => Op::SetRange {
            db: 0,
            key: pool_key(slot),
        },
        Step::SeekGt { slot } => Op::GetGreaterThan {
            db: 0,
            key: pool_key(slot),
        },
        Step::SeekLe { slot } => Op::GetLowerThanOrEqualTo {
            db: 0,
            key: pool_key(slot),
        },
    }
}

proptest! {
    // Each case spins up two real engines (one C LMDB env) and runs up to
    // ~150 ops with real commits; keep the count moderate.
    #![proptest_config(ProptestConfig { cases: 200, ..ProptestConfig::default() })]

    /// Write-biased random workloads must not diverge from the fork.
    #[test]
    fn random_write_workloads_no_divergence(ops in arb_scenario()) {
        if let Err(d) = run::<LmdbEngine, ZerodbEngine>(&ops) {
            prop_assert!(false, "divergence:\n{d}");
        }
    }
}
