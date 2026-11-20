#![no_main]
//! Differential fuzz target for the generated scenarios the linear op model
//! cannot express (`zerodb_oracle::scenario`): key comparators and
//! `delete_range`, several live read transactions at different snapshots
//! while a writer commits, read-only and reopen sequences, and the `MapFull`
//! property. Each input decodes to one scenario, run on LMDB and on ZeroDB
//! through the heed adapter; any difference panics with the minimized
//! scenario.

use libfuzzer_sys::fuzz_target;
use zerodb_oracle::scenario::{decode_scenario, minimize, run_scenario};

fuzz_target!(|data: &[u8]| {
    let Ok(scenario) = decode_scenario(data) else {
        return;
    };
    if let Err(divergence) = run_scenario(&scenario) {
        let small = minimize(&scenario);
        panic!("scenario divergence:\n{divergence}\nminimized: {small:?}");
    }
});
