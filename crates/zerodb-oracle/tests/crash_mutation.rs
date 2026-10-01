//! ADR-0008 D6.1 — the harness mutation self-test.
//!
//! A crash harness that cannot catch the bug class it exists for is
//! untrustworthy. This test breaks the *fault model's barrier accounting*
//! (never the shipped pipeline): with `broken_data_barriers`, `sync` folds
//! only meta-page writes and silently drops data durability — modeling a
//! pipeline whose data fsync is ineffective or misordered after the meta
//! write (the one corruption REC-7 forbids). The harness MUST report a
//! violation within a bounded cycle budget: the durable-only (`floor`) image
//! then contains a meta referencing never-persisted pages, which REC-18.3's
//! walk / REC-18.4's window rejects.
//!
//! The negative control runs the same seeds unbroken and must stay clean —
//! proving the failure comes from the injected misordering, not the budget.

use std::path::PathBuf;

use zerodb_oracle::crash::{gen_spec, image::run_image_cut, image::ImageOpts, Mode};

fn opts(worker: usize, broken: bool) -> ImageOpts {
    ImageOpts {
        variants: 12,
        broken_barriers: broken,
        broken_dsync: false,
        env_path: PathBuf::from(format!("/crash-mutation/w{worker}/env")),
        verify_path: PathBuf::from(format!("/crash-mutation/w{worker}/verify")),
        repro_dir: None,
    }
}

fn opts_broken_dsync(worker: usize) -> ImageOpts {
    ImageOpts {
        broken_dsync: true,
        ..opts(worker, false)
    }
}

/// Seeds whose spec is default-mode (the mode with the strongest obligations
/// — the misordered pipeline must be caught THERE, not merely under relaxed
/// modes where windows are wider).
fn default_mode_seeds(n: usize) -> Vec<u64> {
    (1u64..)
        .filter(|s| gen_spec(*s).mode == Mode::Default)
        .take(n)
        .collect()
}

const BUDGET: usize = 20;

#[test]
fn broken_data_barriers_are_caught_within_budget() {
    let mut caught = None;
    for (i, seed) in default_mode_seeds(BUDGET).into_iter().enumerate() {
        let report = run_image_cut(seed, &opts(0, true));
        if let Some(v) = report.violation {
            caught = Some((i, v));
            break;
        }
    }
    let (i, v) = caught.unwrap_or_else(|| {
        panic!("harness failed to catch a data-fsync-less pipeline within {BUDGET} default-mode cuts — the fault model is too kind (ADR-0008 D6.1)")
    });
    // Visible with --nocapture / in CI logs: what the tripwire actually fired.
    eprintln!("mutation self-test caught (cut {i}): {v}");
    // The violation must be about durability/consistency, not an incidental
    // harness error: it fires either as a walk failure (meta referencing
    // never-persisted pages) or as a broken durability window/floor.
    assert!(
        v.contains("check_image")
            || v.contains("window")
            || v.contains("floor")
            || v.contains("legal window")
            || v.contains("no valid meta"),
        "unexpected violation class for the mutation self-test (cut {i}): {v}"
    );
}

/// ADR-0019 mutation self-test (`broken_dsync`): demote the durable meta
/// write to a plain journaled write — modeling an O_DSYNC descriptor that
/// lies about durability (the exact failure the fused C4+C5 relies on never
/// happening). In default mode a commit then returns `Ok` while its meta is
/// still losable, so a cut after that ack must trip REC-18.4 (acked txnid
/// above the durable floor) or the window-shape assertion — the tripwire
/// guarding ADR-0019's "durable on return" claim.
#[test]
fn broken_dsync_is_caught_within_budget() {
    let mut caught = None;
    for (i, seed) in default_mode_seeds(BUDGET).into_iter().enumerate() {
        let report = run_image_cut(seed, &opts_broken_dsync(2));
        if let Some(v) = report.violation {
            caught = Some((i, v));
            break;
        }
    }
    let (i, v) = caught.unwrap_or_else(|| {
        panic!("harness failed to catch a lying dsync fd within {BUDGET} default-mode cuts — the durable-write model is too kind (ADR-0019)")
    });
    eprintln!("broken_dsync self-test caught (cut {i}): {v}");
    // The violation must be about durability, not an incidental harness
    // error: the demoted write surfaces as a broken monotonic-durability
    // floor, a broken window shape, or (when the un-durable meta is the only
    // valid one in a floor image) a selection/walk failure.
    assert!(
        v.contains("monotonic durability")
            || v.contains("window")
            || v.contains("floor")
            || v.contains("legal window")
            || v.contains("check_image")
            || v.contains("no valid meta"),
        "unexpected violation class for the broken_dsync self-test (cut {i}): {v}"
    );
}

#[test]
fn negative_control_same_seeds_clean_when_unbroken() {
    for seed in default_mode_seeds(BUDGET) {
        let report = run_image_cut(seed, &opts(1, false));
        assert!(
            report.violation.is_none(),
            "negative control violated on seed {seed}: {:?}",
            report.violation
        );
        assert!(
            !report.abandoned,
            "negative control abandoned on seed {seed}"
        );
        assert!(
            report.verified > 0,
            "negative control verified nothing on seed {seed}"
        );
    }
}
