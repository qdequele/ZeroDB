//! # zerodb-oracle — differential test harness
//!
//! Drives an identical sequence of [`Op`]s against two [`Engine`]s and compares
//! every result op-by-op: return values, error codes, iteration order, and
//! post-txn reads. This is the machinery ground rule 1 (CLAUDE.md, PLAN.md
//! §0.3) mandates: LMDB behavior is *observed*, never guessed.
//!
//! * The reference engine is [`LmdbEngine`], backed by `heed =0.22.1` — the
//!   Meilisearch LMDB fork (`mdb.master.nested-rtxns`), the exact C Meilisearch
//!   runs. See ADR-0001.
//! * The native [`ZerodbEngine`] is the second [`Engine`] implementor. At M1.2
//!   it covers only the environment-lifecycle op ([`Op::Reopen`]); every other
//!   op is gated out symmetrically by the driver ([`Engine::implements`]), so a
//!   `run::<LmdbEngine, ZerodbEngine>` differential run restricts itself to the
//!   ops both engines support and grows as later milestones fill ops in.
//! * [`run_self_test`] (`LmdbEngine` vs a second, independent `LmdbEngine`)
//!   remains the harness's determinism/order-stability check and backs the
//!   `diff_ops` fuzz target.
//!
//! ```
//! use zerodb_oracle::{run_self_test, Op, DbName};
//!
//! let ops = vec![
//!     Op::BeginRw,
//!     Op::CreateDb { name: DbName::Unnamed },
//!     Op::Commit,
//! ];
//! assert!(run_self_test(&ops).is_ok());
//! ```
//!
//! This crate is the sole place in the workspace permitted to link C LMDB
//! (CLAUDE.md unsafe/dependency policy; ADR-0001).

#![forbid(unsafe_op_in_unsafe_fn)]

pub mod driver;
mod engine;
mod lmdb;
mod op;
mod result;
pub mod tempdir;
mod zerodb_engine;

pub use driver::{classify, TxnState};
pub use engine::Engine;
pub use lmdb::LmdbEngine;
pub use op::{DbName, Key, Op, PutFlag, Value};
pub use result::{OpResult, OracleError, Skip};
pub use zerodb_engine::ZerodbEngine;

/// A point where two engines produced different results for the same op.
#[derive(Clone, PartialEq, Eq)]
pub struct Divergence {
    /// Zero-based index of the diverging op within the sequence.
    pub index: usize,
    /// The op that diverged.
    pub op: Op,
    /// Name of the first (reference) engine.
    pub a_name: &'static str,
    /// Result from the first engine.
    pub a: OpResult,
    /// Name of the second engine.
    pub b_name: &'static str,
    /// Result from the second engine.
    pub b: OpResult,
}

impl std::fmt::Debug for Divergence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Divergence at op #{}:", self.index)?;
        writeln!(f, "  op:  {:?}", self.op)?;
        writeln!(f, "  {:>5}: {:?}", self.a_name, self.a)?;
        writeln!(f, "  {:>5}: {:?}", self.b_name, self.b)
    }
}

impl std::fmt::Display for Divergence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for Divergence {}

/// Run `ops` against two fresh engines and return the first [`Divergence`], if
/// any.
///
/// Each engine gets its own private storage via [`Engine::new`]. Comparison is
/// exact ([`OpResult`] equality), so iteration snapshots must match verbatim.
///
/// The native differential mode is `run::<LmdbEngine, ZerodbEngine>(ops)` once
/// the zerodb `Engine` impl lands in M1.2+.
pub fn run<A: Engine, B: Engine>(ops: &[Op]) -> Result<(), Box<Divergence>> {
    let mut a = A::new();
    let mut b = B::new();
    for (index, op) in ops.iter().enumerate() {
        // Symmetric milestone gate: if either engine does not yet implement this
        // op, skip it on both sides so a partially-built engine restricts the
        // differential to its supported ops without spurious divergences
        // (see `Engine::implements`). Neither engine's state advances.
        if !a.implements(op) || !b.implements(op) {
            continue;
        }
        let ra = a.apply(op);
        let rb = b.apply(op);
        if ra != rb {
            return Err(Box::new(Divergence {
                index,
                op: op.clone(),
                a_name: a.name(),
                a: ra,
                b_name: b.name(),
                b: rb,
            }));
        }
    }
    Ok(())
}

/// Decode up to `max` [`Op`]s from an arbitrary byte slice.
///
/// Shared by the proptest generator and the `diff_ops` fuzz target so both drive
/// structurally identical inputs. Decoding stops at `max`, when the input is
/// exhausted, or on the first `Arbitrary` error.
pub fn decode_ops(data: &[u8], max: usize) -> Vec<Op> {
    let mut u = arbitrary::Unstructured::new(data);
    let mut ops = Vec::with_capacity(max.min(64));
    while ops.len() < max && !u.is_empty() {
        match <Op as arbitrary::Arbitrary>::arbitrary(&mut u) {
            Ok(op) => ops.push(op),
            Err(_) => break,
        }
    }
    ops
}

/// Phase 0 acceptance mode: run `ops` against two independent [`LmdbEngine`]
/// instances. Any divergence indicates non-determinism in the harness itself
/// (LMDB is deterministic), never an engine bug — there is only one engine here.
pub fn run_self_test(ops: &[Op]) -> Result<(), Box<Divergence>> {
    run::<LmdbEngine, LmdbEngine>(ops)
}

// The native differential is `run::<LmdbEngine, ZerodbEngine>(&ops)`. At M1.2
// only `Op::Reopen` is implemented on the zerodb side (see `ZerodbEngine`), so
// its dedicated env-lifecycle tests live in
// `tests/env_lifecycle_differential.rs`. The `diff_ops` fuzz target stays on
// `run_self_test` until enough ops are implemented to make a differential fuzz
// worthwhile (it will also need `Op::Reopen` map sizes normalized to the OS page
// size — DIVERGENCES D-006 — before graduating).
