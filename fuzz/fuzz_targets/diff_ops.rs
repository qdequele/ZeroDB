#![no_main]
//! Differential fuzz target: decode an arbitrary `Vec<Op>` and run it through
//! the oracle, comparing the LMDB fork against a ZeroDB engine op-by-op.
//!
//! **Engine-pair selection (M1.13).** The candidate engine is chosen once, from
//! the `ZERODB_FUZZ_PAIR` env var, so a single target covers both the native and
//! the adapter paths:
//!
//! - unset / any other value (**default, unchanged**): `LmdbEngine` vs the
//!   **native** `ZerodbEngine`.
//! - `ZERODB_FUZZ_PAIR=heed`: `LmdbEngine` vs `HeedZerodbEngine` — the whole op
//!   model re-run **through the `heed-zerodb` adapter** (ADR-0003 accept #6).
//!   For manual runs: `ZERODB_FUZZ_PAIR=heed cargo +nightly fuzz run diff_ops`.
//!
//! **M1.10 mode dimension.** The first input byte seeds an [`EngineMode`]
//! ([`EngineMode::from_fuzz_byte`]): ~25% of cases open **both** engines under
//! `EnvFlags::WRITE_MAP`, and a slice add a relaxed durability flag. Durability
//! flags never change the observable result, so they broaden commit-path
//! coverage without spurious divergences. The remaining bytes decode the ops.

use std::sync::OnceLock;

use libfuzzer_sys::fuzz_target;
use zerodb_oracle::{
    decode_ops, run_in_mode, EngineMode, HeedZerodbEngine, LmdbEngine, ZerodbEngine,
};

/// Which candidate engine the run uses (decided once from the environment).
#[derive(Clone, Copy)]
enum Pair {
    Native,
    HeedAdapter,
}

fn pair() -> Pair {
    static PAIR: OnceLock<Pair> = OnceLock::new();
    *PAIR.get_or_init(|| match std::env::var("ZERODB_FUZZ_PAIR").as_deref() {
        Ok("heed") => Pair::HeedAdapter,
        _ => Pair::Native,
    })
}

fuzz_target!(|data: &[u8]| {
    let (mode, rest) = match data.split_first() {
        Some((seed, rest)) => (EngineMode::from_fuzz_byte(*seed), rest),
        None => (EngineMode::DEFAULT, data),
    };
    let ops = decode_ops(rest, 64);
    let result = match pair() {
        Pair::Native => run_in_mode::<LmdbEngine, ZerodbEngine>(&ops, mode),
        Pair::HeedAdapter => run_in_mode::<LmdbEngine, HeedZerodbEngine>(&ops, mode),
    };
    if let Err(divergence) = result {
        panic!("oracle divergence (mode {mode:?}):\n{divergence}");
    }
});
