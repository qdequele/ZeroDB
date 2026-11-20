//! Generated scenarios ([`zerodb_oracle::scenario`]): comparators and
//! `delete_range`, live readers at several snapshots, read-only and reopen
//! sequences, and the `MapFull` property. Mirrors the `diff_scenarios` fuzz
//! target.
//!
//! * LMDB against ZeroDB: any divergence is a ZeroDB bug unless listed in
//!   docs/LMDB-COMPATIBILITY.md.
//! * LMDB against LMDB: any divergence means the harness is
//!   non-deterministic.

use proptest::prelude::*;
use zerodb_oracle::scenario::{decode_scenario, minimize, run_scenario, run_scenario_self_test};

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]

    #[test]
    fn random_scenarios_match_lmdb(bytes in proptest::collection::vec(any::<u8>(), 0..1200)) {
        if let Ok(s) = decode_scenario(&bytes) {
            if run_scenario(&s).is_err() {
                let m = minimize(&s);
                let d = run_scenario(&m).unwrap_err();
                prop_assert!(false, "{d}\nminimized scenario: {m:?}");
            }
        }
    }

    #[test]
    fn random_scenarios_are_deterministic(bytes in proptest::collection::vec(any::<u8>(), 0..1200)) {
        if let Ok(s) = decode_scenario(&bytes) {
            if let Err(d) = run_scenario_self_test(&s) {
                prop_assert!(false, "LMDB self-test diverged:\n{d}\n{s:?}");
            }
        }
    }
}
