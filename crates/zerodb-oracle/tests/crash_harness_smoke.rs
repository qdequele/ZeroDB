//! Crash-harness smoke coverage for the normal test suite (M1.11): a small
//! batch of image cuts across all durability modes, one deterministic and one
//! asynchronous SIGKILL cycle, and determinism of a cut's verification. The
//! volume runs live in `just crash-test-quick` / `crash-test-full`
//! (ADR-0008 D3); this keeps `cargo test` self-sufficient.

use std::path::PathBuf;

use zerodb_oracle::crash::{
    gen_spec,
    image::{run_image_cut, ImageOpts},
    sigkill::{run_sigkill_cycle, SigkillOpts},
    Mode,
};

fn opts(worker: usize) -> ImageOpts {
    ImageOpts {
        variants: 12,
        broken_barriers: false,
        env_path: PathBuf::from(format!("/crash-smoke/w{worker}/env")),
        verify_path: PathBuf::from(format!("/crash-smoke/w{worker}/verify")),
        repro_dir: None,
    }
}

/// First seeds hitting each durability mode, so the smoke run always covers
/// the whole REC-9 lattice regardless of the mode distribution.
fn seeds_covering_all_modes() -> Vec<u64> {
    let mut want = vec![
        Mode::Default,
        Mode::WriteMap,
        Mode::NoMetaSync,
        Mode::NoSync,
        Mode::MapAsync,
    ];
    let mut seeds = Vec::new();
    for s in 1u64..10_000 {
        if let Some(pos) = want.iter().position(|m| *m == gen_spec(s).mode) {
            want.remove(pos);
            seeds.push(s);
            if want.is_empty() {
                break;
            }
        }
    }
    assert!(want.is_empty(), "mode coverage seeds not found");
    seeds
}

#[test]
fn image_cuts_all_modes_clean() {
    let mut verified = 0;
    for seed in seeds_covering_all_modes() {
        let r = run_image_cut(seed, &opts(0));
        assert!(
            r.violation.is_none(),
            "seed {seed} ({:?}): {:?}",
            r.mode,
            r.violation
        );
        assert!(!r.abandoned, "seed {seed} abandoned");
        verified += r.verified;
    }
    assert!(
        verified >= 5 * 12,
        "expected ≥60 verified cycles, got {verified}"
    );
}

#[test]
fn image_cut_reports_are_deterministic() {
    // Same seed ⇒ same spec, plans, and verdict (ratified OQ4 end-to-end).
    let a = run_image_cut(4242, &opts(1));
    let b = run_image_cut(4242, &opts(1));
    assert_eq!(a.verified, b.verified);
    assert_eq!(a.violation.is_none(), b.violation.is_none());
    assert_eq!(a.adv_probes, b.adv_probes);
    assert!(a.violation.is_none(), "{:?}", a.violation);
}

/// Regression: acked-at-cut vs acked-at-workload-end (found by the harness's
/// own shakedown, 2026-07-16). Under `NO_META_SYNC`, this seed's cut is a
/// commit-hook capture early in the run; the pre-fix verifier compared the
/// cut's `ceil` against the **workload-end** acked txnid and spuriously
/// reported "acked txnid 1 not even issued (ceil 0) — REC-10 window broken".
/// REC-10 quantifies over commits acked **at the crash instant** (C4 is
/// issued before `commit()` returns; only C5's fsync is relaxed), so the
/// obligation is `ceil(cut) ≥ acked(cut)`, never `ceil(cut) ≥ acked(end)` —
/// with capture-and-continue those differ. The engine's ordering was and is
/// correct; the verifier now carries acked-at-cut with every capture.
#[test]
fn regression_nometasync_acked_at_cut_not_at_end() {
    const SEED: u64 = 8_467_876_453_780_440_666;
    // The pin is only meaningful while this seed maps to NO_META_SYNC; if
    // gen_spec's distribution ever changes, fail loudly so the pin is redone.
    assert_eq!(
        gen_spec(SEED).mode,
        Mode::NoMetaSync,
        "seed no longer maps to NO_META_SYNC — re-pin this regression"
    );
    let r = run_image_cut(SEED, &opts(2));
    assert!(r.violation.is_none(), "{:?}", r.violation);
    assert!(!r.abandoned);
    assert!(r.verified > 0);
}

/// Regression + characterization pin: the `NO_META_SYNC` reclaim-clobber
/// window (REC-10 as amended M1.11; SPEC 06 conflict block item 4, human
/// ratification pending). This seed's cut has `{meta k (un-fsynced), data of
/// txn k+1}` pending; the `meta-subsector-torn` quota plan persists txn
/// k+1's data while rejecting meta k, so recovery falls to snapshot k−1
/// whose pages txn k+1 legally reclaimed (GC-18) — a REAL corruption window
/// shared with LMDB's `MDB_NOMETASYNC` (libmdbx's steady/weak metas close
/// it; Phase 3 candidate). The scoped verifier must classify those images as
/// stale fallbacks (window/taxonomy verified, walk/data waived), not as
/// violations — and must still fully verify every image that recovers to the
/// newest issued meta.
#[test]
fn regression_nometasync_reclaim_clobber_window_is_characterized() {
    const SEED: u64 = 15_797_139_550_980_166_469;
    assert_eq!(
        gen_spec(SEED).mode,
        Mode::NoMetaSync,
        "seed no longer maps to NO_META_SYNC — re-pin this regression"
    );
    let r = run_image_cut(SEED, &opts(3));
    assert!(r.violation.is_none(), "{:?}", r.violation);
    assert!(!r.abandoned);
    assert!(
        r.stale_fallback >= 1,
        "the clobber-window image must be exercised and classified (got {} stale fallbacks)",
        r.stale_fallback
    );
    assert!(
        r.verified > r.stale_fallback,
        "most images must still be FULLY verified ({} verified, {} stale)",
        r.verified,
        r.stale_fallback
    );
}

fn harness_exe() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_crash-harness"))
}

#[test]
fn sigkill_deterministic_hook_cycle_clean() {
    // Seed chosen so the seeded kill style is the deterministic hook kill
    // (7-in-10 branch); scan a few seeds and require at least one verified
    // hook-kill cycle.
    let opts = SigkillOpts {
        exe: harness_exe(),
        repro_dir: None,
    };
    let mut verified = 0;
    for seed in 1u64..=6 {
        let r = run_sigkill_cycle(seed, &opts);
        assert!(r.violation.is_none(), "seed {seed}: {:?}", r.violation);
        verified += r.verified;
    }
    assert!(
        verified >= 4,
        "expected most sigkill cycles verified, got {verified}"
    );
}
