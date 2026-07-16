//! Milestone 1.3 randomized read-workload differential (proptest): populate both
//! engines via identical op streams (decoded from arbitrary bytes, the same path
//! the `diff_ops` fuzz target uses) and compare every read / cursor / seek /
//! iteration result. Any divergence is a zerodb bug (CLAUDE.md rule 1).

use proptest::prelude::*;
use zerodb_oracle::{decode_ops, run, LmdbEngine, Op, ZerodbEngine};

fn run_diff(ops: &[Op]) -> Result<(), String> {
    run::<LmdbEngine, ZerodbEngine>(ops).map_err(|d| d.to_string())
}

proptest! {
    // Modest case count: each case spins up two real engines (one a C LMDB env),
    // materializes files, and runs up to 48 ops.
    #![proptest_config(ProptestConfig { cases: 400, ..ProptestConfig::default() })]

    /// Random byte blobs → op sequences → differential. Mirrors `diff_ops`.
    #[test]
    fn random_ops_no_divergence(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let ops = decode_ops(&bytes, 48);
        prop_assert!(run_diff(&ops).is_ok(), "{}", run_diff(&ops).unwrap_err());
    }
}
