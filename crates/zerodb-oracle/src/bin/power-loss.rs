//! The power-loss harness binary (see `zerodb_oracle::crash::powerloss` and
//! `docs/maintainers/POWER-LOSS.md`). Driven by `scripts/power-loss/run.sh`,
//! which sets up the `dm-log-writes` devices; it needs Linux and root.
//!
//! ```text
//! power-loss workload --dir D (--mark-cmd "CMD ARGS" | --mark-file F) RUN
//! power-loss verify --log DEV --replay DEV --mnt M --hook SCRIPT
//!                   [--tools BIN] [--every N] [--max-points P] [--keep-going] RUN
//! power-loss inspect --log DEV
//!
//! RUN = --seed S --mode default|writemap|nometasync [--spill]
//!       [--rounds R] [--compact-after K|none] [--small N] [--no-large]
//! ```
//!
//! `workload` runs the seeded workload in `D/env` and inserts each mark by
//! running `CMD ARGS <mark>` (for `dm-log-writes`:
//! `dmsetup message <dev> 0 mark`), or appends it to `F` for a dry run.
//!
//! `verify` rebuilds the logged device at each cut point on `--replay`, runs
//! `SCRIPT mount` (which must mount a writable snapshot of the replay device
//! at `M`), checks the state, and runs `SCRIPT umount`. Exit status 1 on a
//! violation.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, ExitCode};

use zerodb_oracle::crash::powerloss::{
    build_model, power_mode, power_spec, replay_to, run_workload, select_points, verify_point,
    LogReader, MarkState, PointOutcome, PowerSpec, LOG_DISCARD, LOG_FLUSH, LOG_FUA,
};

fn usage() -> ! {
    eprintln!(
        "usage: power-loss workload --dir D (--mark-cmd CMD | --mark-file F) RUN\n\
         \x20      power-loss verify --log DEV --replay DEV --mnt M --hook SCRIPT [--tools BIN] \
         [--every N] [--max-points P] [--keep-going] RUN\n\
         \x20      power-loss inspect --log DEV\n\
         RUN = --seed S --mode default|writemap|nometasync [--spill] [--rounds R] \
         [--compact-after K|none] [--small N] [--no-large]"
    );
    std::process::exit(2)
}

#[derive(Default)]
struct Args {
    seed: Option<u64>,
    mode: Option<String>,
    spill: bool,
    rounds: u32,
    compact_after: Option<u32>,
    small: u32,
    no_large: bool,
    dir: Option<PathBuf>,
    mark_cmd: Option<String>,
    mark_file: Option<PathBuf>,
    log: Option<PathBuf>,
    replay: Option<PathBuf>,
    mnt: Option<PathBuf>,
    hook: Option<PathBuf>,
    tools: Option<PathBuf>,
    every: u64,
    max_points: usize,
    keep_going: bool,
}

fn parse(argv: &[String]) -> Args {
    let mut a = Args {
        rounds: 8,
        compact_after: Some(3),
        small: 16,
        ..Args::default()
    };
    let mut i = 0;
    let take = |i: &mut usize| -> String {
        *i += 1;
        argv.get(*i).cloned().unwrap_or_else(|| usage())
    };
    let num = |s: String| -> u64 { s.parse().unwrap_or_else(|_| usage()) };
    while i < argv.len() {
        match argv[i].as_str() {
            "--seed" => a.seed = Some(num(take(&mut i))),
            "--mode" => a.mode = Some(take(&mut i)),
            "--spill" => a.spill = true,
            "--rounds" => a.rounds = u32::try_from(num(take(&mut i))).unwrap_or_else(|_| usage()),
            "--compact-after" => {
                let v = take(&mut i);
                a.compact_after = if v == "none" {
                    None
                } else {
                    Some(u32::try_from(num(v)).unwrap_or_else(|_| usage()))
                };
            }
            "--small" => a.small = u32::try_from(num(take(&mut i))).unwrap_or_else(|_| usage()),
            "--no-large" => a.no_large = true,
            "--dir" => a.dir = Some(take(&mut i).into()),
            "--mark-cmd" => a.mark_cmd = Some(take(&mut i)),
            "--mark-file" => a.mark_file = Some(take(&mut i).into()),
            "--log" => a.log = Some(take(&mut i).into()),
            "--replay" => a.replay = Some(take(&mut i).into()),
            "--mnt" => a.mnt = Some(take(&mut i).into()),
            "--hook" => a.hook = Some(take(&mut i).into()),
            "--tools" => a.tools = Some(take(&mut i).into()),
            "--every" => a.every = num(take(&mut i)),
            "--max-points" => {
                a.max_points = usize::try_from(num(take(&mut i))).unwrap_or_else(|_| usage());
            }
            "--keep-going" => a.keep_going = true,
            _ => usage(),
        }
        i += 1;
    }
    a
}

fn run_spec(a: &Args) -> PowerSpec {
    let seed = a.seed.unwrap_or_else(|| usage());
    let mode = a
        .mode
        .as_deref()
        .and_then(power_mode)
        .unwrap_or_else(|| usage());
    let mut ps = power_spec(seed, mode, a.spill, a.rounds, a.compact_after);
    ps.small_commits = a.small;
    ps.large_commit = !a.no_large;
    ps
}

fn describe(ps: &PowerSpec) -> String {
    format!(
        "seed={} mode={} psize={} spill={} rounds={} compact_after={:?} small={} large={}",
        ps.spec.seed,
        ps.spec.mode.name(),
        ps.spec.page_size,
        ps.spec.dirty_limit.is_some(),
        ps.rounds,
        ps.compact_after,
        ps.small_commits,
        ps.large_commit
    )
}

fn workload(a: &Args) -> ExitCode {
    let ps = run_spec(a);
    let dir = a.dir.clone().unwrap_or_else(|| usage());
    let mut file = a.mark_file.as_ref().map(|p| {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(p)
            .unwrap_or_else(|e| {
                eprintln!("power-loss: {}: {e}", p.display());
                std::process::exit(2)
            })
    });
    let cmd: Option<Vec<String>> = a
        .mark_cmd
        .as_ref()
        .map(|c| c.split_whitespace().map(str::to_string).collect());
    if file.is_none() && cmd.as_ref().is_none_or(Vec::is_empty) {
        usage();
    }
    let mut mark = |name: &str| -> std::io::Result<()> {
        if let Some(f) = file.as_mut() {
            writeln!(f, "{name}")?;
        }
        if let Some(c) = &cmd {
            let st = Command::new(&c[0]).args(&c[1..]).arg(name).status()?;
            if !st.success() {
                return Err(std::io::Error::other(format!("{c:?} {name}: {st}")));
            }
        }
        Ok(())
    };
    eprintln!("power-loss workload: {}", describe(&ps));
    match run_workload(&ps, &dir, &mut mark) {
        Ok(s) => {
            println!(
                "workload done: {} commits acknowledged, last txnid {}, compacted snapshot {:?}",
                s.acks.len(),
                s.acks.last().copied().unwrap_or(0),
                s.compacted
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("power-loss workload failed: {e}");
            ExitCode::from(3)
        }
    }
}

fn hook(script: &PathBuf, what: &str) -> Result<(), String> {
    let st = Command::new(script)
        .arg(what)
        .status()
        .map_err(|e| format!("{} {what}: {e}", script.display()))?;
    if st.success() {
        Ok(())
    } else {
        Err(format!("{} {what}: {st}", script.display()))
    }
}

#[allow(clippy::too_many_lines)] // one linear replay-mount-verify loop
fn verify(a: &Args) -> ExitCode {
    let ps = run_spec(a);
    let (Some(log_path), Some(replay_path), Some(mnt), Some(script)) =
        (&a.log, &a.replay, &a.mnt, &a.hook)
    else {
        usage()
    };
    let log = match LogReader::open(log_path) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("power-loss: reading log {}: {e}", log_path.display());
            return ExitCode::from(2);
        }
    };
    let points = select_points(&log, a.every, a.max_points);
    if points.is_empty() {
        eprintln!("power-loss: no cut point after the start mark; did the workload run?");
        return ExitCode::from(2);
    }
    let replay = match OpenOptions::new().read(true).write(true).open(replay_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("power-loss: opening {}: {e}", replay_path.display());
            return ExitCode::from(2);
        }
    };
    let model = build_model(&ps);
    let last = log.entries.len() as u64 - 1;
    eprintln!(
        "power-loss verify: {} — {} log entries, {} cut points, {} modeled commits",
        describe(&ps),
        log.entries.len(),
        points.len(),
        model.commit_log.len()
    );

    let mut marks = MarkState::default();
    let mut marked_to = 0u64; // entries folded into `marks`
    let mut next = 0u64; // next entry to replay
    let (mut verified, mut creation, mut stale, mut beyond_ack, mut violations) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    for &p in &points {
        if let Err(e) = replay_to(&log, &replay, next, p).and_then(|n| {
            next = n;
            replay.sync_all()
        }) {
            eprintln!("power-loss: replaying to entry {p}: {e}");
            return ExitCode::from(2);
        }
        while marked_to <= p {
            if let Some(name) = &log.entries[marked_to as usize].mark {
                if let Err(e) = marks.apply(name) {
                    eprintln!("power-loss: {e}");
                    return ExitCode::from(2);
                }
            }
            marked_to += 1;
        }
        let final_state = p == last && marks.ended;
        let result = hook(script, "mount").and_then(|()| {
            let r = verify_point(mnt, &ps, &model, &marks, final_state, a.tools.as_deref());
            let u = hook(script, "umount");
            let r = r?;
            u.map(|()| r)
        });
        match result {
            Ok(PointOutcome::Verified { txnid }) => {
                verified += 1;
                if txnid > marks.acked {
                    beyond_ack += 1;
                }
            }
            Ok(PointOutcome::CreationWindow) => creation += 1,
            Ok(PointOutcome::StaleFallback { txnid }) => {
                stale += 1;
                eprintln!(
                    "power-loss: entry {p}: NO_META_SYNC stale fallback to {txnid} \
                     (acked {}, REC-10 window)",
                    marks.acked
                );
            }
            Err(e) => {
                violations += 1;
                let flags = log.entries[p as usize].flags;
                eprintln!(
                    "power-loss VIOLATION: {} entry {p}/{last} (flush {} fua {} discard {}) \
                     acked {} pending {}: {e}",
                    describe(&ps),
                    flags & LOG_FLUSH != 0,
                    flags & LOG_FUA != 0,
                    flags & LOG_DISCARD != 0,
                    marks.acked,
                    marks.pending
                );
                if !a.keep_going {
                    break;
                }
            }
        }
    }
    println!(
        "power-loss: {} | points {} verified {verified} (in-flight commit kept {beyond_ack}) \
         creation-window {creation} stale-fallback {stale} violations {violations}",
        describe(&ps),
        points.len()
    );
    if violations > 0 {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn inspect(a: &Args) -> ExitCode {
    let Some(path) = &a.log else { usage() };
    let log = match LogReader::open(path) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("power-loss: reading log {}: {e}", path.display());
            return ExitCode::from(2);
        }
    };
    let count = |f: u64| log.entries.iter().filter(|e| e.flags & f != 0).count();
    let marks = log.entries.iter().filter(|e| e.mark.is_some()).count();
    let writes = log.entries.iter().filter(|e| e.is_write()).count();
    println!(
        "sector size {} entries {} writes {writes} flushes {} fua {} discards {} marks {marks}",
        log.sectorsize,
        log.entries.len(),
        count(LOG_FLUSH),
        count(LOG_FUA),
        count(LOG_DISCARD)
    );
    for e in log.entries.iter().filter(|e| e.mark.is_some()).take(20) {
        println!("  mark {:>8}: {}", e.index, e.mark.as_deref().unwrap_or(""));
    }
    ExitCode::SUCCESS
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let Some(cmd) = argv.first() else { usage() };
    let a = parse(&argv[1..]);
    match cmd.as_str() {
        "workload" => workload(&a),
        "verify" => verify(&a),
        "inspect" => inspect(&a),
        _ => usage(),
    }
}
