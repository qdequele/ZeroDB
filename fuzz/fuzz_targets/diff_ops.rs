#![no_main]
//! Differential fuzz target: decode an arbitrary `Vec<Op>` and run it through
//! the oracle as **LmdbEngine vs the native ZerodbEngine** (M1.3).
//!
//! The zerodb side buffers writes in a shadow, materializes them through the
//! bulk-load builder on commit, and serves reads from the real B-tree read
//! path; every read / cursor / seek / iteration result is compared against the
//! LMDB fork. Ops zerodb does not yet implement (named DBs, nested read txns,
//! write-cursor mutation, drop) are gated out symmetrically by
//! `Engine::implements`, so the differential restricts itself to the M1.3
//! surface without spurious divergences.

use libfuzzer_sys::fuzz_target;
use zerodb_oracle::{decode_ops, run, LmdbEngine, ZerodbEngine};

fuzz_target!(|data: &[u8]| {
    let ops = decode_ops(data, 64);
    if let Err(divergence) = run::<LmdbEngine, ZerodbEngine>(&ops) {
        panic!("oracle divergence:\n{divergence}");
    }
});
