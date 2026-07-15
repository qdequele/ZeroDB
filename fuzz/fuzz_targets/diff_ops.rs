#![no_main]
//! Differential fuzz target: decode an arbitrary `Vec<Op>` and run it through
//! the oracle.
//!
//! Today it runs `run_self_test` (LmdbEngine vs a second independent
//! LmdbEngine), which fuzzes the harness for determinism. When the native
//! zerodb `Engine` lands (M1.2+), switch the body to
//! `zerodb_oracle::run::<LmdbEngine, ZerodbEngine>(&ops)` to fuzz true
//! LMDB-vs-zerodb differential parity (see the seam note in `lib.rs`).

use libfuzzer_sys::fuzz_target;
use zerodb_oracle::{decode_ops, run_self_test};

fuzz_target!(|data: &[u8]| {
    let ops = decode_ops(data, 64);
    if let Err(divergence) = run_self_test(&ops) {
        panic!("oracle divergence:\n{divergence}");
    }
});
