#![no_main]
//! Differential fuzz target: decode an arbitrary `Vec<Op>` and run it through
//! the oracle as **LmdbEngine vs the native ZerodbEngine**.
//!
//! Every read / cursor / seek / iteration result is compared op-by-op against
//! the LMDB fork. Ops zerodb does not yet implement are gated out symmetrically
//! by `Engine::implements`, so the differential restricts itself to the
//! supported surface without spurious divergences.
//!
//! **M1.10 mode dimension.** The first input byte seeds an [`EngineMode`]
//! ([`EngineMode::from_fuzz_byte`]): ~25% of cases open **both** engines under
//! `EnvFlags::WRITE_MAP` (the PLAN 1.10 gate — exercise the writable-mmap write
//! path differentially), and a slice of cases add a relaxed durability flag
//! (`NO_SYNC` / `NO_META_SYNC` / `MAP_ASYNC`). Durability flags never change the
//! observable result (only crash windows, M1.11), so they broaden commit-path
//! coverage without risking spurious divergences. The remaining bytes decode the
//! op sequence exactly as before.

use libfuzzer_sys::fuzz_target;
use zerodb_oracle::{decode_ops, run_in_mode, EngineMode, LmdbEngine, ZerodbEngine};

fuzz_target!(|data: &[u8]| {
    let (mode, rest) = match data.split_first() {
        Some((seed, rest)) => (EngineMode::from_fuzz_byte(*seed), rest),
        None => (EngineMode::DEFAULT, data),
    };
    let ops = decode_ops(rest, 64);
    if let Err(divergence) = run_in_mode::<LmdbEngine, ZerodbEngine>(&ops, mode) {
        panic!("oracle divergence (mode {mode:?}):\n{divergence}");
    }
});
