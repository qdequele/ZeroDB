//! Mechanism A — in-process image-based crash cycles (REC-19/REC-20,
//! ADR-0008 D2): workload on a [`FaultBacking`], simulated power cut at a
//! commit hook (H0..H4) or an op boundary, N fault-plan images materialized,
//! each reopened and verified per REC-18.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use zerodb::{CommitHook, HookPoint};
use zerodb_core::env::open_with_backing;
use zerodb_io::fault::{
    splitmix64, CapturedDisk, FaultBacking, FaultHandle, FaultPlan, Rng, WriteFate,
};

use super::model::{Exec, ExecErr, StepOutcome};
use super::verify::{
    compare_env_world, image_txnid, probe_image_adversarial, verify_image_full, FullChecks,
    FullOutcome,
};
use super::workload::gen_spec;
use super::CutReport;
use crate::tempdir::TempDir;

/// A [`CommitHook`] that freezes the fault journal at the `commit`-th
/// pipeline entry, at `point` — capture, don't kill (ADR-0008 D2): the cut
/// state is cloned and the workload continues, so the model also records the
/// in-flight commit's final state.
///
/// The `commit`-th pipeline entry carries txnid exactly `commit`: only
/// effective commits enter the pipeline, and each takes `last + 1` (aborted
/// txnids are reused, SPEC 04 TXN-2).
struct CaptureHook {
    handle: FaultHandle,
    target_commit: u64,
    target_point: HookPoint,
    /// Pipeline entries seen (H0 marks entry; every effective commit passes
    /// H0 first). Relaxed suffices: the hook runs on the single writer
    /// thread; the harness reads `captured` only after the workload joined.
    entries: AtomicU64,
    captured: Mutex<Option<CapturedDisk>>,
}

impl CommitHook for CaptureHook {
    fn at(&self, point: HookPoint) {
        let entry = if point == HookPoint::H0 {
            self.entries.fetch_add(1, Ordering::Relaxed) + 1
        } else {
            self.entries.load(Ordering::Relaxed)
        };
        if entry == self.target_commit && point == self.target_point {
            let mut slot = self.captured.lock().expect("capture slot");
            if slot.is_none() {
                *slot = Some(self.handle.capture());
            }
        }
    }
}

/// Options for one image cut.
#[derive(Debug, Clone)]
pub struct ImageOpts {
    /// Fault-plan images per cut (each = one counted cycle).
    pub variants: usize,
    /// ADR-0008 D6.1 mutation self-test: break the data barrier accounting.
    /// The harness MUST then report a violation — see `crash_mutation.rs`.
    pub broken_barriers: bool,
    /// Registry path for the workload env (reuse per worker arms the
    /// deregistration tripwire, ADR-0008 D6.2).
    pub env_path: PathBuf,
    /// Registry path for verification reopens (same reuse rationale).
    pub verify_path: PathBuf,
    /// Where to dump repro artifacts on a violation (`None` = don't dump).
    pub repro_dir: Option<PathBuf>,
}

impl ImageOpts {
    /// Defaults for worker `w` of a run.
    #[must_use]
    pub fn for_worker(w: usize, repro_dir: Option<PathBuf>) -> ImageOpts {
        ImageOpts {
            variants: 12,
            broken_barriers: false,
            env_path: PathBuf::from(format!("/crash-harness/w{w}/env")),
            verify_path: PathBuf::from(format!("/crash-harness/w{w}/verify")),
            repro_dir,
        }
    }
}

enum CutFail {
    Abandon(String),
    Violation {
        detail: String,
        /// The offending image + plan dump, when one exists.
        artifact: Option<(Vec<u8>, String)>,
    },
}

/// Run one image cut: workload → cut → materialize → verify each image.
/// `report.verified` counts the successfully verified images (cycles).
#[must_use]
pub fn run_image_cut(seed: u64, opts: &ImageOpts) -> CutReport {
    let mut report = CutReport {
        mode: Some(gen_spec(seed).mode),
        ..CutReport::default()
    };
    match run_inner(seed, opts, &mut report) {
        Ok(()) => {}
        Err(CutFail::Abandon(why)) => {
            report.abandoned = true;
            report.violation = None;
            // Abandonment is not a violation but is visible in the summary.
            let _ = why;
        }
        Err(CutFail::Violation { detail, artifact }) => {
            let full = dump_repro(seed, opts, &detail, artifact);
            report.violation = Some(full);
        }
    }
    report
}

fn dump_repro(
    seed: u64,
    opts: &ImageOpts,
    detail: &str,
    artifact: Option<(Vec<u8>, String)>,
) -> String {
    let spec = gen_spec(seed);
    let mut msg = format!(
        "[image] seed={seed} mode={} psize={} : {detail}",
        spec.mode.name(),
        spec.page_size
    );
    if let Some(base) = &opts.repro_dir {
        let dir = base.join(format!("image-{seed}"));
        if std::fs::create_dir_all(&dir).is_ok() {
            if let Some((img, plan)) = artifact {
                let _ = std::fs::write(dir.join("image.bin"), img);
                let _ = std::fs::write(dir.join("plan.txt"), plan);
            }
            let _ = std::fs::write(dir.join("ops.txt"), format!("{:#?}", spec.ops_for_round(0)));
            let _ = std::fs::write(
                dir.join("info.txt"),
                format!("seed={seed}\nmode={}\npage_size={}\nmap_size={}\nviolation={detail}\nrepro: crash-harness --repro image:{seed}\n",
                    spec.mode.name(), spec.page_size, spec.map_size),
            );
            msg.push_str(&format!(" [repro artifacts: {}]", dir.display()));
        }
    }
    msg
}

#[allow(clippy::too_many_lines)] // linear cycle script: setup → run → cut → verify
fn run_inner(seed: u64, opts: &ImageOpts, report: &mut CutReport) -> Result<(), CutFail> {
    let spec = gen_spec(seed);
    let mut rng = Rng::new(splitmix64(seed ^ 0x0000_1A9E_0000_1A9E));
    let abandon = |why: String| CutFail::Abandon(why);

    // Real temp file + real mmap backing under the fault wrapper (ADR-0008 D1
    // Option B): the live view is production-identical; only durability is
    // simulated.
    let tmp = TempDir::new().map_err(|e| abandon(format!("tempdir: {e}")))?;
    let data_path = tmp.path().join("zerodb.dat");
    let opened = zerodb_io::open_or_create(
        &data_path,
        spec.page_size,
        Some(spec.map_size),
        spec.map_size,
        false,
        false,
    )
    .map_err(|e| abandon(format!("open_or_create: {e}")))?;
    let (fault, handle) = FaultBacking::wrap(opened.backing, spec.page_size)
        .map_err(|e| abandon(format!("fault wrap: {e}")))?;
    if opts.broken_barriers {
        handle.set_broken_data_barriers(true);
    }
    let env = open_with_backing(
        opts.env_path.clone(),
        Box::new(fault),
        spec.page_size,
        spec.map_size,
        false,
        16,
        126,
        spec.mode.durability(),
    )
    .map_err(|e| abandon(format!("env open: {e}")))?;

    // Cut point: 80% at a commit hook (REC-17's H0..H4 grid drives the REC-6
    // rows), 20% at an op boundary (between-commit windows — interesting
    // under relaxed durability where pending spans commits).
    let ops = spec.ops_for_round(0);
    let hook_cut = rng.ratio(4, 5);
    let hook = if hook_cut {
        let points = [
            HookPoint::H0,
            HookPoint::H1,
            HookPoint::H2,
            HookPoint::H3,
            HookPoint::H4,
        ];
        let h = Arc::new(CaptureHook {
            handle: handle.clone(),
            target_commit: 1 + rng.below(6) as u64,
            target_point: points[rng.below(5)],
            entries: AtomicU64::new(0),
            captured: Mutex::new(None),
        });
        env.set_commit_hook(Some(h.clone()));
        Some(h)
    } else {
        None
    };
    let boundary_at = if hook_cut {
        usize::MAX
    } else {
        rng.below(ops.len().max(1))
    };

    // Run the workload, cross-checking every op against the model
    // (ADR-0008 D6.3: drift fails loudly BEFORE the crash is injected).
    // `boundary_cap` carries the acked txnid AT the cut (capture-and-continue
    // means workload-end acks are later than the cut's — the durability
    // obligations compare against the cut-time value).
    let mut boundary_cap: Option<(CapturedDisk, u64)> = None;
    let (states, acked_end) = {
        let mut exec = Exec::new(Some(&env));
        for (i, op) in ops.iter().enumerate() {
            match exec.step(op) {
                Ok(StepOutcome::Executed | StepOutcome::Skipped | StepOutcome::Committed(_)) => {}
                Err(ExecErr::Drift(d)) => {
                    return Err(CutFail::Violation {
                        detail: format!("pre-crash model/engine drift at op {i}: {d}"),
                        artifact: None,
                    });
                }
                Err(ExecErr::Abandon(a)) => return Err(abandon(format!("op {i}: {a}"))),
            }
            if i == boundary_at {
                boundary_cap = Some((handle.capture(), exec.acked()));
            }
        }
        exec.finish();
        // Final pre-crash cross-check: live env == model, completely.
        if env.txnid() != exec.txnid {
            return Err(CutFail::Violation {
                detail: format!(
                    "pre-crash txnid drift: engine {} vs model {}",
                    env.txnid(),
                    exec.txnid
                ),
                artifact: None,
            });
        }
        if let Err(d) = compare_env_world(&env, &exec.world) {
            return Err(CutFail::Violation {
                detail: format!("pre-crash state drift: {d}"),
                artifact: None,
            });
        }
        (std::mem::take(&mut exec.states), exec.acked())
    };
    env.set_commit_hook(None);
    drop(env); // release the registry entry; the fault journal lives on

    // The cut: hook capture, else op-boundary capture, else end-of-run. Each
    // carries the acked txnid at cut time: a hook capture fires inside the
    // `target_commit`-th pipeline entry, so exactly `target_commit − 1`
    // commits had returned `Ok` (the k-th pipeline entry carries txnid k).
    let (cut, acked) = match hook {
        Some(h) => {
            let captured = h.captured.lock().expect("capture slot").take();
            match captured {
                Some(c) => (c, h.target_commit - 1),
                // Workload had fewer commits than the target: cut at the end.
                None => (handle.capture(), acked_end),
            }
        }
        None => match boundary_cap {
            Some((c, a)) => (c, a),
            None => (handle.capture(), acked_end),
        },
    };

    // Legal recovery window: floor = durable-only selection, ceil =
    // all-applied selection (self-adapting encoding of the REC-6 rows).
    let floor_img = cut.floor_image();
    let ceil_img = cut.ceil_image();
    let floor = image_txnid(&floor_img, spec.page_size).ok_or_else(|| CutFail::Violation {
        detail: "durable-only image has no valid meta (REC-3: a crash must never lose both slots)"
            .to_string(),
        artifact: Some((floor_img.clone(), "floor (all pending dropped)".to_string())),
    })?;
    let ceil = image_txnid(&ceil_img, spec.page_size).ok_or_else(|| CutFail::Violation {
        detail: "all-applied image has no valid meta".to_string(),
        artifact: Some((ceil_img.clone(), "ceil (all pending applied)".to_string())),
    })?;

    // Pipeline-shape assertions per mode (ADR-0008 D4).
    if spec.mode.bounded_window() && ceil.saturating_sub(floor) > 1 {
        return Err(CutFail::Violation {
            detail: format!(
                "window shape: ceil {ceil} − floor {floor} > 1 under {} (a barrier failed to fold)",
                spec.mode.name()
            ),
            artifact: None,
        });
    }
    if spec.mode.strict_ack() && floor < acked {
        return Err(CutFail::Violation {
            detail: format!(
                "REC-18.4 monotonic durability: acked txnid {acked} above durable floor {floor}"
            ),
            artifact: None,
        });
    }
    if spec.mode == super::Mode::NoMetaSync && ceil < acked {
        return Err(CutFail::Violation {
            detail: format!(
                "NO_META_SYNC: acked txnid {acked} not even issued (ceil {ceil}) — REC-10 window broken"
            ),
            artifact: None,
        });
    }

    // Materialize + verify (ADR-0008 D4): bounded-window modes run the full
    // adversarial fault model WITH full REC-18 checks; NO_SYNC/MAP_ASYNC
    // split into the ordered sub-model (full checks) and the adversarial
    // sub-model (taxonomy-only probes, characterization logged).
    let (full_plans, adv_plans): (Vec<FaultPlan>, Vec<FaultPlan>) = if spec.mode.bounded_window() {
        (cut.plans_adversarial(seed, opts.variants), Vec::new())
    } else {
        (
            cut.plans_ordered(seed, opts.variants / 2),
            cut.plans_adversarial(seed, opts.variants - opts.variants / 2),
        )
    };
    for plan in &full_plans {
        // REC-10 as amended (M1.11 find, seed 15797139550980166469): under
        // NO_META_SYNC a plan that persists any in-flight **data** write while
        // the (single) pending meta is lost can legally clobber the fallback
        // snapshot — walk/data are waived for exactly those images (see
        // `FullChecks::stale_data_exempt`). Requires a pending meta to exist:
        // without one, recovery cannot fall below the ceiling anyway.
        let stale_data_exempt = spec.mode == super::Mode::NoMetaSync
            && cut.last_meta_write().is_some()
            && plan
                .fates
                .iter()
                .enumerate()
                .any(|(i, f)| !cut.is_meta_write(i) && !matches!(f, WriteFate::Dropped));
        let full = FullChecks {
            floor,
            ceil,
            states: &states,
            psize: spec.page_size,
            map_size: spec.map_size,
            verify_path: &opts.verify_path,
            stale_data_exempt,
        };
        let img = cut.materialize(plan);
        match verify_image_full(img, &full) {
            Ok(FullOutcome::Verified) => {}
            Ok(FullOutcome::StaleFallback) => report.stale_fallback += 1,
            Err(detail) => {
                return Err(CutFail::Violation {
                    detail: format!("plan '{}': {detail}", plan.label),
                    artifact: Some((cut.materialize(plan), format!("{plan:#?}"))),
                });
            }
        }
        report.verified += 1;
    }
    for plan in &adv_plans {
        let img = cut.materialize(plan);
        report.adv_probes += 1;
        match probe_image_adversarial(img, spec.page_size, spec.map_size, &opts.verify_path) {
            Ok(out) => {
                if out.opened.is_some() {
                    report.adv_opened += 1;
                }
                if out.invalid {
                    report.adv_invalid += 1;
                }
                if out.walk_clean {
                    report.adv_walk_clean += 1;
                }
            }
            Err(detail) => {
                return Err(CutFail::Violation {
                    detail: format!("adversarial plan '{}': {detail}", plan.label),
                    artifact: Some((cut.materialize(plan), format!("{plan:#?}"))),
                });
            }
        }
        report.verified += 1;
    }
    Ok(())
}
