//! Multi-process crash cycles (ADR-0023): a writer process is killed
//! mid-commit while a reader process keeps reading the same lock-file env.
//!
//! One cycle:
//! 1. a writer child (`crash-harness --crash-child-lock`) opens the env with a
//!    lock file, writes `writer-open`, and waits for a reader;
//! 2. a reader child (`crash-harness --crash-reader`) opens the env as a
//!    second process, writes `reader-up`, and walks every database in a loop
//!    until told to stop;
//! 3. the writer dies like a mechanism-B child: an `abort()` at a seeded
//!    commit-pipeline hook, or a SIGKILL after a seeded delay that starts
//!    once the reader is up;
//! 4. with the reader still alive, the parent joins the env through the
//!    shared lock file, checks the recovered txnid against the legal set and
//!    the contents against the replayed model;
//! 5. the parent commits one marker write txn, which takes over the dead
//!    writer's lock, then stops the reader (which must exit cleanly);
//! 6. the parent closes, checks the file image, and reopens without a lock
//!    file to confirm the marker commit is the latest one.
//!
//! The legal recovered set differs from single-process recovery: a joiner
//! takes its snapshot from the txnid the last writer *published* to the lock
//! file, and a committing writer publishes it only after hook H4. A writer
//! killed at any hook of commit `N` therefore leaves `N − 1` published, even
//! when its meta page for `N` reached the file; the next write txn reuses
//! txnid `N` and overwrites that orphaned meta.

use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use zerodb::check::check_image;
use zerodb::{named_databases, Database, EnvOpenOptions, RoTxn};
use zerodb_io::fault::{splitmix64, Rng};

use super::model::Exec;
use super::sigkill::{kill_arg, read_sidecar, Kill, SigkillOpts, MAX_ROUNDS};
use super::verify::compare_env_world;
use super::workload::gen_spec;
use super::CutReport;
use crate::tempdir::TempDir;

/// Marker file the writer child creates once its lock-file env is open.
pub(crate) const WRITER_OPEN: &str = "writer-open";
/// Marker file the reader child creates once its lock-file env is open.
pub(crate) const READER_UP: &str = "reader-up";
/// Marker file the parent creates to stop the reader loop.
const READER_STOP: &str = "reader-stop";
/// File the reader writes its error to before exiting with status 1.
const READER_FAIL: &str = "reader-fail";
/// Key of the parent's marker write in the main database.
const MARKER_KEY: &[u8] = b"__mp_marker";
/// Value of the parent's marker write.
const MARKER_VAL: &[u8] = b"mp";
/// How long the parent waits for a child's marker file.
const MARKER_WAIT: Duration = Duration::from_secs(120);
/// A reader that is never told to stop (its parent died) gives up after this.
const READER_MAX_LIFETIME: Duration = Duration::from_secs(600);

// ---------------------------------------------------------------------------
// Reader child
// ---------------------------------------------------------------------------

/// Walk one database with a cursor: iteration must succeed and its keys must
/// be strictly ascending.
fn walk_db(db: &Database, rtxn: &RoTxn<'_>, what: &str) -> Result<(), String> {
    let mut prev: Option<Vec<u8>> = None;
    for pair in db.iter(rtxn) {
        let (k, _) = pair.map_err(|e| format!("iter({what}): {e}"))?;
        if let Some(p) = &prev {
            if k <= p.as_slice() {
                return Err(format!(
                    "keys not strictly ascending in {what}: {:02x?}… after {:02x?}…",
                    &k[..k.len().min(12)],
                    &p[..p.len().min(12)]
                ));
            }
        }
        prev = Some(k.to_vec());
    }
    Ok(())
}

/// One reader pass: a read txn over the main database and every named
/// database its catalog lists.
fn reader_pass(env: &zerodb::Env) -> Result<(), String> {
    let rtxn = env.read_txn().map_err(|e| format!("read_txn: {e}"))?;
    walk_db(&env.main_database(), &rtxn, "main database")?;
    let names = named_databases(&rtxn).map_err(|e| format!("named_databases: {e}"))?;
    for name in names {
        let what = String::from_utf8_lossy(&name).into_owned();
        match env.open_database(&rtxn, Some(&name)) {
            Ok(Some(db)) => walk_db(&db, &rtxn, &what)?,
            Ok(None) => return Err(format!("catalog lists {what:?} but it does not open")),
            Err(e) => return Err(format!("open_database({what:?}): {e}")),
        }
    }
    Ok(())
}

/// The reader child body: open the lock-file env at `dir` as a second
/// process, write `reader-up`, and walk every database in a fresh read txn
/// until `reader-stop` exists. On any error it writes `reader-fail` with the
/// message and exits 1; when stopped it exits 0. Never returns. Invoked by
/// the binary as `crash-harness --crash-reader <dir> <page_size> <map_size>`.
pub fn reader_run(dir: &Path, page_size: u32, map_size: usize) -> ! {
    let fail = |msg: String| -> ! {
        let _ = std::fs::write(dir.join(READER_FAIL), msg.as_bytes());
        std::process::exit(1);
    };
    let mut opts = EnvOpenOptions::new();
    opts.map_size(map_size)
        .page_size(page_size)
        .max_dbs(16)
        .lock_file(true);
    let env = match opts.open(dir) {
        Ok(e) => e,
        Err(e) => fail(format!("open: {e}")),
    };
    if let Err(e) = std::fs::write(dir.join(READER_UP), b"") {
        fail(format!("writing {READER_UP}: {e}"));
    }
    let started = Instant::now();
    while !dir.join(READER_STOP).exists() {
        if started.elapsed() > READER_MAX_LIFETIME {
            fail("never told to stop".to_string());
        }
        if let Err(e) = reader_pass(&env) {
            fail(e);
        }
    }
    drop(env);
    std::process::exit(0);
}

// ---------------------------------------------------------------------------
// Parent
// ---------------------------------------------------------------------------

/// A child process killed and reaped on drop, so an early return never
/// leaves a writer or reader running.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// How waiting for a child's marker file ended.
enum MarkerWait {
    /// The marker exists.
    Present,
    /// The child exited first.
    Exited,
    /// Neither within [`MARKER_WAIT`].
    TimedOut,
}

fn wait_for_marker(path: &Path, child: &mut Child) -> MarkerWait {
    let deadline = Instant::now() + MARKER_WAIT;
    loop {
        if path.exists() {
            return MarkerWait::Present;
        }
        if matches!(child.try_wait(), Ok(Some(_))) {
            // The marker may have landed between the two checks.
            return if path.exists() {
                MarkerWait::Present
            } else {
                MarkerWait::Exited
            };
        }
        if Instant::now() >= deadline {
            return MarkerWait::TimedOut;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn reader_fail_msg(dir: &Path) -> String {
    std::fs::read_to_string(dir.join(READER_FAIL)).unwrap_or_else(|_| "(no message)".to_string())
}

/// Run one multi-process cycle: spawn a writer and a reader, kill the writer
/// mid-commit, join the env through the lock file while the reader is live,
/// verify, commit a marker, stop the reader, check the image and reopen.
/// `report.verified` is 1 on a clean verification.
#[must_use]
#[allow(clippy::too_many_lines)] // linear cycle script: spawn → kill → join → verify
pub fn run_mp_cycle(seed: u64, opts: &SigkillOpts) -> CutReport {
    let spec = gen_spec(seed);
    let mut report = CutReport {
        mode: Some(spec.mode),
        ..CutReport::default()
    };
    let violation = |detail: String| {
        Some(format!(
            "[mp] seed={seed} mode={} psize={} : {detail} [repro: crash-harness --repro mp:{seed}]",
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
    let dir = tmp.path();

    // 1. The writer: opens with a lock file, then waits for the reader.
    let mut writer = match Command::new(&opts.exe)
        .arg("--crash-child-lock")
        .arg(seed.to_string())
        .arg(dir)
        .arg(kill_arg(kill))
        .spawn()
    {
        Ok(c) => ChildGuard(c),
        Err(_) => {
            report.abandoned = true;
            return report;
        }
    };
    match wait_for_marker(&dir.join(WRITER_OPEN), &mut writer.0) {
        MarkerWait::Present => {}
        MarkerWait::Exited => {
            // Nothing kills the writer before `writer-open` (the hook is
            // installed after the open, and the async delay starts once the
            // reader is up), so the only designed early exit is an
            // acknowledged abandon such as a failed open.
            if read_sidecar(dir).abandon.is_some() {
                report.abandoned = true;
                return report;
            }
            let status = writer.0.wait().ok();
            report.violation = violation(format!(
                "writer exited before opening its env, without an abandon ack (status {status:?})"
            ));
            return report;
        }
        MarkerWait::TimedOut => {
            report.violation = violation(format!("writer did not open within {MARKER_WAIT:?}"));
            return report;
        }
    }

    // 2. The reader: a second process on the same lock-file env.
    let mut reader = match Command::new(&opts.exe)
        .arg("--crash-reader")
        .arg(dir)
        .arg(spec.page_size.to_string())
        .arg(spec.map_size.to_string())
        .spawn()
    {
        Ok(c) => ChildGuard(c),
        Err(_) => {
            report.abandoned = true;
            return report;
        }
    };
    match wait_for_marker(&dir.join(READER_UP), &mut reader.0) {
        MarkerWait::Present => {}
        MarkerWait::Exited => {
            report.violation = violation(format!(
                "reader exited before it was up: {}",
                reader_fail_msg(dir)
            ));
            return report;
        }
        MarkerWait::TimedOut => {
            report.violation = violation(format!("reader not up within {MARKER_WAIT:?}"));
            return report;
        }
    }

    // 3. Kill the writer.
    if let Kill::AsyncMs(ms) = kill {
        std::thread::sleep(Duration::from_millis(ms));
        let _ = writer.0.kill(); // SIGKILL; ignore "already exited"
    }
    let Ok(status) = writer.0.wait() else {
        report.abandoned = true;
        return report;
    };

    let side = read_sidecar(dir);
    report.spilled_txns = side.spilled;
    if let Some(d) = side.drift {
        report.violation = violation(format!("child model/engine drift: {d}"));
        return report;
    }
    if side.abandon.is_some() {
        report.abandoned = true;
        return report;
    }

    // Parent replay: same seed ⇒ same ops ⇒ the standalone model rebuilds
    // the committed state for every txnid the writer can have reached.
    let mut exec = Exec::new(None);
    for round in 0..MAX_ROUNDS {
        for op in spec.ops_for_round(round) {
            // Standalone replay is infallible (no env to disagree with).
            let _ = exec.step(&op);
        }
    }
    exec.finish();
    // Sidecar acks must be a prefix of the replayed commit log (the
    // ADR-0008 D6.3 drift guard).
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

    // 4. Join the env through the lock file while the reader is live.
    let mut jopts = EnvOpenOptions::new();
    jopts
        .map_size(spec.map_size as usize)
        .page_size(spec.page_size)
        .max_dbs(16)
        .lock_file(true);
    let env = match jopts.open(dir) {
        Ok(e) => e,
        Err(e) => {
            report.violation = violation(format!("joining open failed: {e}"));
            return report;
        }
    };
    let r = env.txnid();
    // Legal recovered set. The joiner reads the txnid the last writer
    // published, which a commit stores only after H4: a hook kill leaves the
    // previous commit published at every hook point.
    let legal = match (side.done, kill) {
        (true, _) => r == acked,
        (false, Kill::Hook { commit, .. }) => {
            // The k-th pipeline entry carries txnid k (TXN-2 reuse), so the
            // writer acked exactly k − 1 (cross-checked here).
            if acked != commit - 1 {
                report.violation = violation(format!(
                    "hook-kill bookkeeping: died at commit {commit} but acked {acked}"
                ));
                return report;
            }
            r == commit - 1
        }
        // At most the one commit whose ack append the kill cut: a commit
        // publishes its txnid just before it returns, and the next commit
        // starts only after the ack is written.
        (false, Kill::AsyncMs(_)) => r >= acked && r <= acked + 1,
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
    // 5. Exact data at the published txnid.
    if let Err(d) = compare_env_world(&env, world) {
        report.violation = violation(format!("joined state mismatch at txnid {r}: {d}"));
        return report;
    }

    // 6. One marker write txn: takes over the dead writer's lock and
    // overwrites any meta page the dead writer left past the published txnid.
    let main = env.main_database();
    let committed = env.write_txn().and_then(|mut wtxn| {
        main.put(&mut wtxn, MARKER_KEY, MARKER_VAL)?;
        wtxn.commit()
    });
    if let Err(e) = committed {
        report.violation = violation(format!("marker commit after the writer died failed: {e}"));
        return report;
    }
    let readback = env
        .read_txn()
        .and_then(|rtxn| main.get(&rtxn, MARKER_KEY).map(|v| v.map(<[u8]>::to_vec)));
    match readback {
        Ok(Some(v)) if v == MARKER_VAL => {}
        other => {
            report.violation = violation(format!("marker read-back: {other:?}"));
            return report;
        }
    }

    // 7. Stop the reader; it must exit cleanly.
    if let Err(e) = std::fs::write(dir.join(READER_STOP), b"") {
        report.violation = violation(format!("writing {READER_STOP}: {e}"));
        return report;
    }
    match reader.0.wait() {
        Ok(s) if s.success() => {}
        Ok(s) => {
            report.violation = violation(format!("reader failed ({s}): {}", reader_fail_msg(dir)));
            return report;
        }
        Err(e) => {
            report.violation = violation(format!("waiting for the reader: {e}"));
            return report;
        }
    }

    // 8. Close, check the image, reopen without a lock file.
    drop(env);
    match std::fs::read(dir.join(zerodb::DATA_FILE_NAME)) {
        Ok(bytes) => {
            let v = check_image(&bytes, spec.page_size);
            if !v.is_empty() {
                report.violation = violation(format!("check_image failed: {v:?}"));
                return report;
            }
        }
        Err(e) => {
            report.violation = violation(format!("reading the data file: {e}"));
            return report;
        }
    }
    let mut ropts = EnvOpenOptions::new();
    ropts.max_dbs(16);
    let env = match ropts.open(dir) {
        Ok(e) => e,
        Err(e) => {
            report.violation = violation(format!("reopen without a lock file failed: {e}"));
            return report;
        }
    };
    if env.txnid() != r + 1 {
        report.violation = violation(format!(
            "reopened txnid {} but the marker commit should be {}",
            env.txnid(),
            r + 1
        ));
        return report;
    }
    let marker = env.read_txn().and_then(|rtxn| {
        env.main_database()
            .get(&rtxn, MARKER_KEY)
            .map(|v| v.map(<[u8]>::to_vec))
    });
    match marker {
        Ok(Some(v)) if v == MARKER_VAL => {}
        other => {
            report.violation = violation(format!("marker after reopen: {other:?}"));
            return report;
        }
    }
    drop(env);

    // A deterministically killed writer must have died abnormally.
    if !side.done && matches!(kill, Kill::Hook { .. }) && status.success() {
        report.violation = violation("hook-kill writer exited cleanly without DONE".to_string());
        return report;
    }
    report.verified = 1;
    report
}
