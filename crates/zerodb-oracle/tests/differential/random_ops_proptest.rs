//! Randomized op sequences (proptest): arbitrary bytes decoded into bounded
//! `Vec<Op>`, the same path the `diff_ops` fuzz target uses.
//!
//! * LMDB against ZeroDB: any divergence is a ZeroDB bug.
//! * LMDB against a second LMDB: any divergence means the harness itself is
//!   non-deterministic.

use proptest::prelude::*;
use zerodb_oracle::{decode_ops, run, run_self_test, LmdbEngine, ZerodbEngine};

proptest! {
    // Modest case count: each case spins up two real engines (one a C LMDB env),
    // materializes files, and runs up to 48 ops.
    #![proptest_config(ProptestConfig { cases: 400, ..ProptestConfig::default() })]

    /// Random byte blobs → op sequences → differential. Mirrors `diff_ops`.
    #[test]
    fn random_ops_no_divergence(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let ops = decode_ops(&bytes, 48);
        if let Err(d) = run::<LmdbEngine, ZerodbEngine>(&ops) {
            prop_assert!(false, "{d}");
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    /// The same op sequences against two LMDB engines: the harness's
    /// determinism check.
    #[test]
    fn random_sequences_do_not_diverge(data in prop::collection::vec(any::<u8>(), 0..512)) {
        let ops = decode_ops(&data, 48);
        if let Err(d) = run_self_test(&ops) {
            panic!("self-test diverged on random input:\n{d}");
        }
    }
}
