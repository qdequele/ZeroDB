//! Proves the harness itself works: `run()` must catch a lying engine, not
//! just accept two engines that happen to agree.
//!
//! [`FaultyEngine<F>`] wraps the real [`LmdbEngine`] and, at exactly one
//! pre-chosen op index, corrupts the result the inner engine actually
//! produced. Comparing it against a clean `LmdbEngine` via [`run`] must
//! report a [`Divergence`] whose `index` is that exact op index — proving the
//! comparison is precise, not just "something differs somewhere".
//!
//! One negative control (`NoFault`) proves the harness does not cry wolf: a
//! `FaultyEngine` with its fault index unreachable, run over a nontrivial
//! multi-op sequence, must diverge from nothing.
//!
//! No `unsafe` is used here: `FaultyEngine` only ever calls the safe
//! `Engine`/`OpResult` surface `LmdbEngine` already exposes.

use std::marker::PhantomData;

use zerodb_oracle::{run, DbName, Engine, LmdbEngine, Op, OpResult, OracleError, PutFlag};

fn k(bytes: &[u8]) -> zerodb_oracle::Key {
    zerodb_oracle::Key(bytes.to_vec())
}
fn v(bytes: &[u8]) -> zerodb_oracle::Value {
    zerodb_oracle::Value(bytes.to_vec())
}

/// A single-shot, targeted result corruption applied at op index `TARGET`.
///
/// Each impl only touches the `OpResult` shape it claims to corrupt; every
/// other shape (and every other op index) passes through untouched, so a
/// fault can never accidentally fire on the wrong op.
trait Fault: 'static {
    /// The (fixed) op index this fault fires at. `usize::MAX` for "never" —
    /// used by the negative control.
    const TARGET: usize;

    /// Corrupt `result`, the value the wrapped `LmdbEngine` actually produced
    /// for `op`. Called only when the running op index equals `TARGET`.
    fn corrupt(op: &Op, result: OpResult) -> OpResult;
}

/// (a) Flips a byte in one `get()`-shaped result.
struct FlipByte<const N: usize>;
impl<const N: usize> Fault for FlipByte<N> {
    const TARGET: usize = N;
    fn corrupt(_op: &Op, result: OpResult) -> OpResult {
        match result {
            OpResult::MaybeVal(Some(mut bytes)) => {
                match bytes.first_mut() {
                    Some(b) => *b ^= 0xFF,
                    None => bytes.push(0xFF),
                }
                OpResult::MaybeVal(Some(bytes))
            }
            other => other,
        }
    }
}

/// (b) Reports `NotFound` for a key that actually exists (a `get()` that
/// found `Some(_)` is rewritten into an error).
struct FalseNotFound<const N: usize>;
impl<const N: usize> Fault for FalseNotFound<N> {
    const TARGET: usize = N;
    fn corrupt(_op: &Op, result: OpResult) -> OpResult {
        match result {
            OpResult::MaybeVal(Some(_)) => OpResult::Err(OracleError::NotFound),
            other => other,
        }
    }
}

/// (c) Drops the last entry from an iteration/range snapshot.
struct DropLastEntry<const N: usize>;
impl<const N: usize> Fault for DropLastEntry<N> {
    const TARGET: usize = N;
    fn corrupt(_op: &Op, result: OpResult) -> OpResult {
        match result {
            OpResult::Entries(mut entries) if !entries.is_empty() => {
                entries.pop();
                OpResult::Entries(entries)
            }
            other => other,
        }
    }
}

/// (d) Swaps the order of the first two entries in an iteration snapshot.
struct SwapEntryOrder<const N: usize>;
impl<const N: usize> Fault for SwapEntryOrder<N> {
    const TARGET: usize = N;
    fn corrupt(_op: &Op, result: OpResult) -> OpResult {
        match result {
            OpResult::Entries(mut entries) if entries.len() >= 2 => {
                entries.swap(0, 1);
                OpResult::Entries(entries)
            }
            other => other,
        }
    }
}

/// (e) Returns `Ok` where the inner engine actually errored (suppresses a
/// `KeyExist`).
struct SuppressKeyExist<const N: usize>;
impl<const N: usize> Fault for SuppressKeyExist<N> {
    const TARGET: usize = N;
    fn corrupt(_op: &Op, result: OpResult) -> OpResult {
        match result {
            OpResult::Err(OracleError::KeyExist) => OpResult::Ok,
            other => other,
        }
    }
}

/// (f) Misreports a `len()` count by 1.
struct MiscountLen<const N: usize>;
impl<const N: usize> Fault for MiscountLen<N> {
    const TARGET: usize = N;
    fn corrupt(_op: &Op, result: OpResult) -> OpResult {
        match result {
            OpResult::Count(n) => OpResult::Count(n + 1),
            other => other,
        }
    }
}

/// The negative control: never corrupts anything (`TARGET` is unreachable for
/// any op sequence used in this file).
struct NoFault;
impl Fault for NoFault {
    const TARGET: usize = usize::MAX;
    fn corrupt(_op: &Op, result: OpResult) -> OpResult {
        result
    }
}

/// Wraps a real [`LmdbEngine`] and applies `F::corrupt` to the result of the
/// op at index `F::TARGET` (0-based, counted across every `apply()` call).
struct FaultyEngine<F: Fault> {
    inner: LmdbEngine,
    op_index: usize,
    _fault: PhantomData<F>,
}

impl<F: Fault> Engine for FaultyEngine<F> {
    fn new() -> Self {
        FaultyEngine {
            inner: LmdbEngine::new(),
            op_index: 0,
            _fault: PhantomData,
        }
    }

    fn name(&self) -> &'static str {
        "faulty-lmdb"
    }

    fn apply(&mut self, op: &Op) -> OpResult {
        let idx = self.op_index;
        self.op_index += 1;
        let result = self.inner.apply(op);
        if idx == F::TARGET {
            F::corrupt(op, result)
        } else {
            result
        }
    }
}

/// (a) A `get()` on an existing key has its result byte-flipped -> the
/// harness must catch the mismatch at the `Get` op's exact index.
#[test]
fn detects_flipped_byte_in_get_result() {
    let ops = vec![
        Op::BeginRw, // 0
        Op::CreateDb {
            name: DbName::Unnamed,
        }, // 1
        Op::Put {
            db: 0,
            key: k(b"k"),
            val: v(b"v"),
        }, // 2
        Op::Get {
            db: 0,
            key: k(b"k"),
        }, // 3 <- target
        Op::Commit,  // 4
    ];
    const TARGET: usize = 3;
    let err = run::<LmdbEngine, FaultyEngine<FlipByte<TARGET>>>(&ops)
        .expect_err("flipped byte must be detected as a divergence");
    assert_eq!(
        err.index, TARGET,
        "divergence must point at the corrupted op"
    );
    assert_eq!(
        err.op,
        Op::Get {
            db: 0,
            key: k(b"k")
        }
    );
}

/// (b) A `get()` on an existing key is rewritten to report `NotFound`.
#[test]
fn detects_false_not_found_for_existing_key() {
    let ops = vec![
        Op::BeginRw, // 0
        Op::CreateDb {
            name: DbName::Unnamed,
        }, // 1
        Op::Put {
            db: 0,
            key: k(b"k"),
            val: v(b"v"),
        }, // 2
        Op::Get {
            db: 0,
            key: k(b"k"),
        }, // 3 <- target
        Op::Commit,  // 4
    ];
    const TARGET: usize = 3;
    let err = run::<LmdbEngine, FaultyEngine<FalseNotFound<TARGET>>>(&ops)
        .expect_err("misreported NotFound must be detected");
    assert_eq!(err.index, TARGET);
    assert_eq!(
        err.op,
        Op::Get {
            db: 0,
            key: k(b"k")
        }
    );
}

/// (c) The last entry of a 3-entry forward iteration is silently dropped.
#[test]
fn detects_dropped_last_entry_in_iteration() {
    let ops = vec![
        Op::BeginRw, // 0
        Op::CreateDb {
            name: DbName::Unnamed,
        }, // 1
        Op::Put {
            db: 0,
            key: k(b"a"),
            val: v(b"1"),
        }, // 2
        Op::Put {
            db: 0,
            key: k(b"b"),
            val: v(b"2"),
        }, // 3
        Op::Put {
            db: 0,
            key: k(b"c"),
            val: v(b"3"),
        }, // 4
        Op::Iter { db: 0 }, // 5 <- target
        Op::Commit,  // 6
    ];
    const TARGET: usize = 5;
    let err = run::<LmdbEngine, FaultyEngine<DropLastEntry<TARGET>>>(&ops)
        .expect_err("a dropped trailing entry must be detected");
    assert_eq!(err.index, TARGET);
    assert_eq!(err.op, Op::Iter { db: 0 });
}

/// (d) The order of the first two entries of a 3-entry forward iteration is
/// swapped.
#[test]
fn detects_swapped_entry_order_in_iteration() {
    let ops = vec![
        Op::BeginRw, // 0
        Op::CreateDb {
            name: DbName::Unnamed,
        }, // 1
        Op::Put {
            db: 0,
            key: k(b"a"),
            val: v(b"1"),
        }, // 2
        Op::Put {
            db: 0,
            key: k(b"b"),
            val: v(b"2"),
        }, // 3
        Op::Put {
            db: 0,
            key: k(b"c"),
            val: v(b"3"),
        }, // 4
        Op::Iter { db: 0 }, // 5 <- target
        Op::Commit,  // 6
    ];
    const TARGET: usize = 5;
    let err = run::<LmdbEngine, FaultyEngine<SwapEntryOrder<TARGET>>>(&ops)
        .expect_err("swapped iteration order must be detected");
    assert_eq!(err.index, TARGET);
    assert_eq!(err.op, Op::Iter { db: 0 });
}

/// (e) `NO_OVERWRITE` on an already-existing key really does error
/// `KeyExist` on the inner engine (SPEC 01 §S2); the faulty engine swallows
/// that error and reports success instead.
#[test]
fn detects_suppressed_key_exist_error() {
    let ops = vec![
        Op::BeginRw, // 0
        Op::CreateDb {
            name: DbName::Unnamed,
        }, // 1
        Op::Put {
            db: 0,
            key: k(b"a"),
            val: v(b"1"),
        }, // 2
        Op::PutFlagged {
            db: 0,
            key: k(b"a"),
            val: v(b"2"),
            flag: PutFlag::NoOverwrite,
        }, // 3 <- target, must be KeyExist on the real engine
        Op::Commit,  // 4
    ];
    const TARGET: usize = 3;
    let err = run::<LmdbEngine, FaultyEngine<SuppressKeyExist<TARGET>>>(&ops)
        .expect_err("a suppressed KeyExist error must be detected");
    assert_eq!(err.index, TARGET);
    assert_eq!(
        err.op,
        Op::PutFlagged {
            db: 0,
            key: k(b"a"),
            val: v(b"2"),
            flag: PutFlag::NoOverwrite,
        }
    );
    // Sanity: confirm the reference engine really did error KeyExist here (so
    // the fault had something real to suppress), not e.g. Ok on both sides.
    assert_eq!(err.a, OpResult::Err(OracleError::KeyExist));
    assert_eq!(err.b, OpResult::Ok);
}

/// (f) `len()` after two puts is misreported as one too many.
#[test]
fn detects_miscounted_len() {
    let ops = vec![
        Op::BeginRw, // 0
        Op::CreateDb {
            name: DbName::Unnamed,
        }, // 1
        Op::Put {
            db: 0,
            key: k(b"a"),
            val: v(b"1"),
        }, // 2
        Op::Put {
            db: 0,
            key: k(b"b"),
            val: v(b"2"),
        }, // 3
        Op::Len { db: 0 }, // 4 <- target
        Op::Commit,  // 5
    ];
    const TARGET: usize = 4;
    let err = run::<LmdbEngine, FaultyEngine<MiscountLen<TARGET>>>(&ops)
        .expect_err("a miscounted len() must be detected");
    assert_eq!(err.index, TARGET);
    assert_eq!(err.op, Op::Len { db: 0 });
    assert_eq!(err.a, OpResult::Count(2));
    assert_eq!(err.b, OpResult::Count(3));
}

/// Negative control: a `FaultyEngine` with an unreachable fault index, run
/// over a nontrivial multi-op sequence (multiple DBs, puts, deletes, clears,
/// nested read txn, iteration, seeks, reserved puts, cursor mutation,
/// verify-after-commit), must diverge from nothing. This proves the harness
/// does not cry wolf on a merely-differently-implemented-but-behaviorally-
/// identical engine.
#[test]
fn no_fault_produces_no_divergence() {
    let ops = vec![
        Op::BeginRw,
        Op::CreateDb {
            name: DbName::Unnamed,
        },
        Op::CreateDb {
            name: DbName::Named(2),
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
        Op::PutReserved {
            db: 0,
            key: k(b"c"),
            val: v(b"reserved"),
        },
        Op::BeginNestedRo,
        Op::Get {
            db: 0,
            key: k(b"a"),
        },
        Op::Iter { db: 0 },
        Op::EndNestedRo,
        Op::Put {
            db: 1,
            key: k(b"x"),
            val: v(b"y"),
        },
        Op::SetRange {
            db: 0,
            key: k(b"aa"),
        },
        Op::GetGreaterThan {
            db: 0,
            key: k(b"a"),
        },
        Op::IterMutPutCurrent {
            db: 0,
            nth: 0,
            val: v(b"rewritten"),
        },
        Op::Del {
            db: 0,
            key: k(b"c"),
        },
        Op::Len { db: 0 },
        Op::ClearDb { db: 1 },
        Op::Commit,
        Op::VerifyGet {
            db: 0,
            key: k(b"a"),
        },
    ];
    assert!(
        run::<LmdbEngine, FaultyEngine<NoFault>>(&ops).is_ok(),
        "a non-corrupting FaultyEngine must never diverge from LmdbEngine"
    );
}
