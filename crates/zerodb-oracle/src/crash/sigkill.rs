//! Mechanism B — child-process SIGKILL crash cycles (REC-17, ADR-0008 D2):
//! the real OS path (real file, real mmap/pwrite or writable map). A child
//! runs the seeded workload and dies — deterministically (`abort()` at a
//! seeded `(commit, hook)` via the M1.4 [`CommitHook`]) or asynchronously
//! (parent SIGKILL after a seeded wall-clock delay, which can land *mid*-C2,
//! a state the hooks cannot produce; ratified OQ2 keeps this in CI because
//! verification tolerates any legal recovery point).
//!
//! SIGKILL does not tear writes (the OS page cache survives, REC-17), so
//! every write *issued* before death persists; the legal recovered set is
//! spec-scoped (the REC-6 rows / the acked window), and the sidecar of
//! acknowledged commits gives the monotonic-durability floor.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use zerodb::check::check_image;
use zerodb::{CommitHook, EnvOpenOptions, HookPoint};
use zerodb_io::fault::{splitmix64, Rng};

use super::model::{Exec, ExecErr, StepOutcome};
use super::verify::compare_env_world;
use super::workload::gen_spec;
use super::CutReport;
use crate::tempdir::TempDir;

/// Workload rounds a child loops through (async kills need runway; the
/// parent replays the same rounds).
pub const MAX_ROUNDS: u32 = 8;

/// How a child dies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kill {
    /// `abort()` at the `commit`-th pipeline entry, at hook `point` (0..=4).
    Hook { commit: u64, point: u8 },
    /// Parent sends SIGKILL after this many milliseconds.
    AsyncMs(u64),
}

fn kill_arg(k: Kill) -> String {
    match k {
        Kill::Hook { commit, point } => format!("hook:{commit}:{point}"),
        Kill::AsyncMs(_) => "none".to_string(),
    }
}

fn hookpoint(idx: u8) -> HookPoint {
    match idx {
        0 => HookPoint::H0,
        1 => HookPoint::H1,
        2 => HookPoint::H2,
        3 => HookPoint::H3,
        _ => HookPoint::H4,
    }
}

/// A [`CommitHook`] that aborts the process at the `commit`-th pipeline
/// entry, at `point` (the M1.4 crash_smoke mechanism, seeded).
struct AbortAtHook {
    target_commit: u64,
    target_point: HookPoint,
    entries: AtomicU64,
}

impl CommitHook for AbortAtHook {
    fn at(&self, point: HookPoint) {
        let entry = if point == HookPoint::H0 {
            self.entries.fetch_add(1, Ordering::Relaxed) + 1
        } else {
            self.entries.load(Ordering::Relaxed)
        };
        if entry == self.target_commit && point == self.target_point {
            // Die without unwinding — nothing after the hook may run.
            std::process::abort();
        }
    }
}

/// Options for one SIGKILL cycle.
#[derive(Debug, Clone)]
pub struct SigkillOpts {
    /// Path of the `crash-harness` binary (the child re-invokes it).
    pub exe: PathBuf,
    /// Where to dump repro artifacts on a violation.
    pub repro_dir: Option<PathBuf>,
}

/// Sidecar file: one `C <txnid>` line per acknowledged commit, then a
/// `DONE` / `ABANDON <why>` / `DRIFT <why>` terminal marker (absent when
/// killed). Written after `commit` returns and flushed — SIGKILL preserves
/// the page cache, so the parent reads it verbatim; it can lag the true
/// acked count by at most the in-flight append.
const SIDECAR: &str = "acked.log";

// ---------------------------------------------------------------------------
// Child
// ---------------------------------------------------------------------------

/// The child body: open a real env at `dir`, run the seeded workload with the
/// per-mode real flags, ack commits to the sidecar, die per `kill`. Exits the
/// process; never returns. Invoked by the binary as
/// `crash-harness --crash-child <seed> <dir> <kill>`.
pub fn child_run(seed: u64, dir: &Path, kill: &str) -> ! {
    let spec = gen_spec(seed);
    let mut sidecar = match std::fs::File::create(dir.join(SIDECAR)) {
        Ok(f) => f,
        Err(_) => std::process::exit(2),
    };
    let mut mark = |line: String| {
        let _ = writeln!(sidecar, "{line}");
        let _ = sidecar.flush();
    };
    let mut opts = EnvOpenOptions::new();
    opts.map_size(spec.map_size as usize)
        .page_size(spec.page_size)
        .max_dbs(16)
        .flags(spec.mode.env_flags());
    let env = match opts.open(dir) {
        Ok(e) => e,
        Err(e) => {
            mark(format!("ABANDON open: {e}"));
            std::process::exit(0);
        }
    };
    if let Some(rest) = kill.strip_prefix("hook:") {
        let mut it = rest.split(':');
        let commit: u64 = it.next().and_then(|s| s.parse().ok()).unwrap_or(1);
        let point: u8 = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        env.set_commit_hook(Some(Arc::new(AbortAtHook {
            target_commit: commit,
            target_point: hookpoint(point),
            entries: AtomicU64::new(0),
        })));
    }
    let mut exec = Exec::new(Some(&env));
    for round in 0..MAX_ROUNDS {
        for op in spec.ops_for_round(round) {
            match exec.step(&op) {
                Ok(StepOutcome::Committed(t)) => mark(format!("C {t}")),
                Ok(_) => {}
                Err(ExecErr::Drift(d)) => {
                    mark(format!("DRIFT {d}"));
                    std::process::exit(0);
                }
                Err(ExecErr::Abandon(a)) => {
                    mark(format!("ABANDON {a}"));
                    std::process::exit(0);
                }
            }
        }
    }
    exec.finish();
    mark("DONE".to_string());
    std::process::exit(0);
}

// ---------------------------------------------------------------------------
// Parent
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Sidecar {
    commits: Vec<u64>,
    done: bool,
    abandon: Option<String>,
    drift: Option<String>,
}

fn read_sidecar(dir: &Path) -> Sidecar {
    let mut s = Sidecar {
        commits: Vec::new(),
        done: false,
        abandon: None,
        drift: None,
    };
    let Ok(text) = std::fs::read_to_string(dir.join(SIDECAR)) else {
        return s;
    };
    for line in text.lines() {
        // Complete lines only: an async kill can tear the in-flight append.
        if let Some(t) = line.strip_prefix("C ") {
            if let Ok(t) = t.parse() {
                s.commits.push(t);
            }
        } else if line == "DONE" {
            s.done = true;
        } else if let Some(w) = line.strip_prefix("ABANDON") {
            s.abandon = Some(w.trim().to_string());
        } else if let Some(w) = line.strip_prefix("DRIFT") {
            s.drift = Some(w.trim().to_string());
        }
    }
    s
}

/// Run one SIGKILL cycle: spawn, kill, reopen, verify. `report.verified` is 1
/// on a clean verification.
#[must_use]
#[allow(clippy::too_many_lines)] // linear cycle script: spawn → kill → replay → verify
pub fn run_sigkill_cycle(seed: u64, opts: &SigkillOpts) -> CutReport {
    let spec = gen_spec(seed);
    let mut report = CutReport {
        mode: Some(spec.mode),
        ..CutReport::default()
    };
    let violation = |detail: String| {
        Some(format!(
            "[sigkill] seed={seed} mode={} psize={} : {detail} [repro: crash-harness --repro sigkill:{seed}]",
            spec.mode.name(),
            spec.page_size
        ))
    };
    let mut rng = Rng::new(splitmix64(seed ^ 0x0000_516B_0000_516B));
    let kill = if rng.ratio(7, 10) {
        Kill::Hook {
            commit: 1 + rng.below(10) as u64,
            point: rng.below(5) as u8,
        }
    } else {
        Kill::AsyncMs(1 + rng.below(400) as u64)
    };

    let Ok(tmp) = TempDir::new() else {
        report.abandoned = true;
        return report;
    };
    let mut child = match Command::new(&opts.exe)
        .arg("--crash-child")
        .arg(seed.to_string())
        .arg(tmp.path())
        .arg(kill_arg(kill))
        .spawn()
    {
        Ok(c) => c,
        Err(_) => {
            report.abandoned = true;
            return report;
        }
    };
    if let Kill::AsyncMs(ms) = kill {
        std::thread::sleep(std::time::Duration::from_millis(ms));
        let _ = child.kill(); // SIGKILL; ignore "already exited"
    }
    let Ok(status) = child.wait() else {
        report.abandoned = true;
        return report;
    };

    let side = read_sidecar(tmp.path());
    if let Some(d) = side.drift {
        report.violation = violation(format!("child model/engine drift: {d}"));
        return report;
    }
    if side.abandon.is_some() {
        report.abandoned = true;
        return report;
    }

    // Parent replay: same seed ⇒ same ops ⇒ the standalone model rebuilds
    // the committed state for every txnid the child can have reached.
    let mut exec = Exec::new(None);
    for round in 0..MAX_ROUNDS {
        for op in spec.ops_for_round(round) {
            // Standalone replay is infallible (no env to disagree with).
            let _ = exec.step(&op);
        }
    }
    exec.finish();
    // Sidecar acks must be a prefix of the replayed commit log — the
    // mechanism-B leg of the ADR-0008 D6.3 drift guard.
    if side.commits.as_slice() != &exec.commit_log[..side.commits.len().min(exec.commit_log.len())]
        || side.commits.len() > exec.commit_log.len()
    {
        report.violation = violation(format!(
            "sidecar/model txnid drift: child acked {:?}…, model predicts {:?}…",
            &side.commits[..side.commits.len().min(8)],
            &exec.commit_log[..exec.commit_log.len().min(8)]
        ));
        return report;
    }
    let acked = side.commits.last().copied().unwrap_or(0);

    // Reopen the real file (REC-18.1). One designed exception: a kill inside
    // env *creation* (empty sidecar, no commits) may leave a partially
    // created store → the designed `Invalid`/IO error is a legal outcome of
    // the pre-transactional window (SPEC 02 §3.4), not a recovery failure.
    let mut ropts = EnvOpenOptions::new();
    ropts.max_dbs(16);
    let env = match ropts.open(tmp.path()) {
        Ok(e) => e,
        Err(e) => {
            if side.commits.is_empty() && !side.done {
                report.verified = 1; // creation-window crash, designed outcome
                return report;
            }
            report.violation = violation(format!("recovery open failed: {e}"));
            return report;
        }
    };

    let r = env.txnid();
    // Legal recovered set. Page cache survives SIGKILL, so nothing issued is
    // lost; what is bounded is how far past the last *ack* the child got:
    // at most one commit whose ack append was cut plus one in flight.
    let legal = match (side.done, kill) {
        (true, _) => r == acked,
        (false, Kill::Hook { commit, point }) => {
            // The k-th pipeline entry carries txnid k (TXN-2 reuse). The
            // sidecar then acked exactly k−1 — cross-checked here — and the
            // REC-6 row gives the legal set.
            if acked != commit - 1 {
                report.violation = violation(format!(
                    "hook-kill bookkeeping: died at commit {commit} but acked {acked}"
                ));
                return report;
            }
            match point {
                0..=2 => r == commit - 1,            // H0/H1/H2 → N−1
                3 => r == commit - 1 || r == commit, // H3 → {N−1, N}
                _ => r == commit,                    // H4 → N (REC-18.4)
            }
        }
        (false, Kill::AsyncMs(_)) => r >= acked && r <= acked + 2,
    };
    if !legal {
        report.violation = violation(format!(
            "recovered txnid {r} outside the legal set (acked {acked}, kill {kill:?}, done {})",
            side.done
        ));
        return report;
    }
    let Some(world) = exec.states.get(&r) else {
        report.violation = violation(format!("recovered txnid {r} has no modeled state"));
        return report;
    };

    // Full walk on the real recovered file (REC-18.3) — except inside the
    // env-*creation* window (no commit ever acked, recovered txnid 0, child
    // killed mid-`create_env_file`): a one-slot half-created file legally
    // opens via REC-4 but is shorter than two meta pages, which the walker
    // rightly flags for any post-creation store. Never taken once a single
    // commit was acknowledged (creation completes both slots before txn 1).
    let creation_window = side.commits.is_empty() && !side.done && r == 0;
    if !creation_window {
        match std::fs::read(tmp.path().join(zerodb::DATA_FILE_NAME)) {
            Ok(bytes) => {
                let v = check_image(&bytes, spec.page_size);
                if !v.is_empty() {
                    report.violation = violation(format!("check_image failed: {v:?}"));
                    return report;
                }
            }
            Err(e) => {
                report.violation = violation(format!("reading recovered file: {e}"));
                return report;
            }
        }
    }
    // Exact data (REC-18.2/.4): all-or-nothing per txn.
    if let Err(d) = compare_env_world(&env, world) {
        report.violation = violation(format!("recovered state mismatch at txnid {r}: {d}"));
        return report;
    }
    drop(env);

    // A deterministically killed child must have died abnormally.
    if !side.done && matches!(kill, Kill::Hook { .. }) && status.success() {
        report.violation = violation("hook-kill child exited cleanly without DONE".to_string());
        return report;
    }
    report.verified = 1;
    report
}
