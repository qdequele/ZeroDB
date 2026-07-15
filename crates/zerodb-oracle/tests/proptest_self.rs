//! Randomized self-test: proptest generates arbitrary byte strings, decodes
//! them into bounded `Vec<Op>` (the same path the `diff_ops` fuzz target uses),
//! and runs them through `run_self_test`. Since both sides are LMDB, any
//! divergence means the harness is non-deterministic.

use proptest::prelude::*;
use zerodb_oracle::{decode_ops, run_self_test};

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    #[test]
    fn random_sequences_do_not_diverge(data in prop::collection::vec(any::<u8>(), 0..512)) {
        let ops = decode_ops(&data, 48);
        if let Err(d) = run_self_test(&ops) {
            panic!("self-test diverged on random input:\n{d}");
        }
    }
}
