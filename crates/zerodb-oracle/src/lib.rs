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

pub mod crash;
pub mod driver;
mod engine;
mod lmdb;
mod op;
mod result;
pub mod tempdir;
mod zerodb_engine;

pub use driver::{classify, TxnState};
pub use engine::Engine;
// `EngineMode` / `run_in_mode` are defined in this module; re-listed here for
// discoverability alongside the other public items.
pub use lmdb::LmdbEngine;
pub use op::{DbName, Key, Op, PutFlag, Value};
pub use result::{OpResult, OracleError, Skip};
pub use zerodb_engine::ZerodbEngine;

/// The env open **mode** both differential engines are opened in (M1.10,
/// SPEC 01 Table 1). Both engines open with the identical flag combination so a
/// `run_in_mode` differential exercises `WRITE_MAP` and the durability lattice
/// at parity. `READ_ONLY` is not here — it is exercised by dedicated tests
/// (a write-rejection is not expressible as an op sequence).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EngineMode {
    /// `MDB_WRITEMAP`: writes go through a writable mmap (SPEC 01 §S7).
    pub write_map: bool,
    /// `MDB_NOSYNC`: skip both fsync barriers (SPEC 01 §S6).
    pub no_sync: bool,
    /// `MDB_NOMETASYNC`: skip the meta fsync (SPEC 01 §S6).
    pub no_meta_sync: bool,
    /// `MDB_MAPASYNC` (with `WRITE_MAP`): async msync (SPEC 01 §S6).
    pub map_async: bool,
}

impl EngineMode {
    /// The default (all-durable, heap+pwrite) mode.
    pub const DEFAULT: EngineMode = EngineMode {
        write_map: false,
        no_sync: false,
        no_meta_sync: false,
        map_async: false,
    };
    /// Plain `WRITE_MAP` (durable msync).
    pub const WRITE_MAP: EngineMode = EngineMode {
        write_map: true,
        no_sync: false,
        no_meta_sync: false,
        map_async: false,
    };
    /// `WRITE_MAP | MAP_ASYNC` (relaxed msync).
    pub const WRITE_MAP_ASYNC: EngineMode = EngineMode {
        write_map: true,
        no_sync: false,
        no_meta_sync: false,
        map_async: true,
    };

    /// A fuzz dimension: derive a mode from one seed byte so ~25% of fuzz cases
    /// run both engines under `WRITE_MAP` (PLAN 1.10 gate), a slice of those
    /// also with a relaxed durability flag. The durability flags never change
    /// the *observable* result (only crash windows, M1.11), so they broaden
    /// commit-path coverage without risking spurious divergences.
    #[must_use]
    pub fn from_fuzz_byte(b: u8) -> EngineMode {
        // Low two bits == 0 → WRITE_MAP (~25%). Higher bits pick a durability
        // relaxation for a fraction of those (and of the default cases).
        let write_map = (b & 0b11) == 0;
        EngineMode {
            write_map,
            no_sync: !write_map && (b & 0b0001_0000) != 0,
            no_meta_sync: !write_map && (b & 0b0010_0000) != 0,
            map_async: write_map && (b & 0b0100_0000) != 0,
        }
    }
}

/// Base map size for the differential engines: 64 MiB. Large enough that no
/// 64-op sequence of `≤ 64 KiB` values (see [`Value`]) can exceed it, so the
/// reference LMDB env never returns `MdbError::MapFull` — a boundary the
/// rebuild-on-commit [`ZerodbEngine`] does not model (its shadow is in-memory).
pub const DIFF_MAP_SIZE: usize = 64 << 20;

/// Round a map size up to a 64 KiB multiple — a multiple of every target OS
/// page size (4 / 16 / 64 KiB), so heed never rejects it for not being an
/// OS-page multiple (DIVERGENCES D-006) and both engines agree on the effective
/// size after an [`Op::Reopen`]. Both differential engines round identically.
#[must_use]
pub fn round_map_size(size: usize) -> usize {
    let q = 64 * 1024;
    size.div_ceil(q) * q
}

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
    run_in_mode::<A, B>(ops, EngineMode::DEFAULT)
}

/// Run `ops` against two fresh engines **opened in `mode`** (M1.10): both
/// engines get the identical `WRITE_MAP` / durability flag combination, so a
/// differential run exercises the second write mode at parity. Returns the first
/// [`Divergence`], if any.
///
/// The M1.5 file-size tripwire is **disabled under `WRITE_MAP`**: a writemap env
/// `set_len`s its file to the full `map_size` at open (SPEC 04 §6.4, matching the
/// fork's `ftruncate`-to-mapsize), so both engines report `map_size` and the
/// size is dominated by that constant, not by GC growth (which the heap-mode
/// tripwire already guards on every default-mode run).
pub fn run_in_mode<A: Engine, B: Engine>(
    ops: &[Op],
    mode: EngineMode,
) -> Result<(), Box<Divergence>> {
    let mut a = A::new_in_mode(mode);
    let mut b = B::new_in_mode(mode);
    let size_tripwire = !mode.write_map;
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
        // M1.5 file-size tripwire (ADR-0005 D5, approved bands): after every
        // commit, the second engine's on-disk size must stay within
        // `BASE + 1.5x` of the reference's — a cheap unbounded-GC-growth
        // detector on every fuzz case / proptest sequence. `BASE` = 16 pages
        // (16 x 4096 = 64 KiB).
        //
        // BASE was tightened from 72 to 16 pages when the SPEC 03 §6.4
        // end-of-page insert-point rule was ratified (ADR-0005 D5 addendum,
        // 2026-07-16). The old 72-page BASE existed almost entirely to absorb
        // the sequential-plain-put fill-factor divergence: zerodb's median
        // split left ~50%-full leaves on ascending inserts where the fork's
        // `mdb_page_split` splits at the insert point, a ~2x leaf-page ratio
        // (old `deep_split_cascade_then_delete_to_empty` peak: zerodb 263 vs
        // fork 132 pages at 4 KiB → the old 65-page deterministic minimum).
        // The amendment eliminates that divergence — measured post-amendment:
        // zerodb@4K peak on the same workload is 154 pages (vs fork ~132),
        // overage `sb - 1.5*sa = 154 - 198 = -44` pages (negative); at MATCHED
        // page size small DBs are byte-identical (fixed overhead ~0); and the
        // whole differential+proptest suite shows NO positive-overage commit
        // (max observed -4 pages on this dev machine's fork@16K vs zerodb@4K).
        // 16 pages is thus the ~0-page deterministic minimum plus a margin for
        // GC-21 within-PIL-only overflow-fragmentation transients in fuzz
        // overflow sequences, still a tight unbounded-growth tripwire (a real
        // leak grows without bound and clears BASE + 1.5x within a few
        // commits). See ADR-0005 D5 addendum for the full measurement.
        if size_tripwire && matches!(op, Op::Commit) {
            if let (Some(sa), Some(sb)) = (a.real_disk_size(), b.real_disk_size()) {
                const BASE: u64 = 16 * 4096;
                assert!(
                    sb <= BASE + sa.saturating_mul(3) / 2,
                    "file-size tripwire at op #{index}: {} = {sb} bytes vs {} = {sa} bytes \
                     (band BASE=288KiB + 1.5x, ADR-0005 D5)",
                    b.name(),
                    a.name(),
                );
            }
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
