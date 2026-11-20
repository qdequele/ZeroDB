//! Generated multi-process sequences ([`zerodb_oracle::mp`]): two or three
//! worker processes on one environment, with reader slots, writes, growth
//! past another process's map, `resize`, `MapResized`, `SIGKILL` while
//! holding readers, stale-reader cleanup and respawns, each run on LMDB and on
//! ZeroDB and compared. Any difference is a ZeroDB bug unless listed in
//! docs/LMDB-COMPATIBILITY.md.
//!
//! `ZERODB_MP_CASES` sets the number of generated cases (default 16); the PR
//! fuzz gate runs more.

use std::path::Path;

use proptest::prelude::*;
use zerodb_oracle::mp::{decode_mp_case, minimize_mp, run_mp};

fn exe() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_mp-worker"))
}

fn cases() -> u32 {
    std::env::var("ZERODB_MP_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(), ..ProptestConfig::default() })]

    #[test]
    fn generated_multi_process_sequences_match_lmdb(
        bytes in proptest::collection::vec(any::<u8>(), 0..400)
    ) {
        if let Ok(case) = decode_mp_case(&bytes) {
            if run_mp(&case, exe()).is_err() {
                let small = minimize_mp(&case, exe());
                let d = run_mp(&small, exe()).unwrap_err();
                prop_assert!(false, "{d}\nminimized case:\n{small}");
            }
        }
    }
}

/// A worker reopened with a map smaller than the data: both engines raise it
/// to the pages in use, a size that another process's next commit crosses or
/// not depending on page reuse. The worker grows a raised map to the big one.
#[test]
fn raised_map_after_reopen() {
    use zerodb_oracle::mp::{Cmd, MapChoice, MpCase, MpOp};
    let case = MpCase {
        workers: vec![MapChoice::Big, MapChoice::Big],
        ops: vec![
            MpOp::Run(0, Cmd::BeginRead(0)),
            MpOp::Run(0, Cmd::Bulk { id: 1 }),
            MpOp::Run(
                0,
                Cmd::Open {
                    map: MapChoice::Small,
                    read_only: false,
                },
            ),
            MpOp::Run(
                0,
                Cmd::Write {
                    start: 21,
                    n: 1,
                    vlen: 19,
                    tag: 0,
                },
            ),
            MpOp::Run(0, Cmd::BeginRead(0)),
            MpOp::Run(0, Cmd::Delete { start: 126, n: 32 }),
            MpOp::Run(
                1,
                Cmd::Write {
                    start: 94,
                    n: 5,
                    vlen: 185,
                    tag: 180,
                },
            ),
            MpOp::Run(
                0,
                Cmd::Write {
                    start: 0,
                    n: 1,
                    vlen: 0,
                    tag: 0,
                },
            ),
            MpOp::Run(0, Cmd::Info),
        ],
    };
    if let Err(d) = run_mp(&case, exe()) {
        panic!("{d}\n{case}");
    }
}
