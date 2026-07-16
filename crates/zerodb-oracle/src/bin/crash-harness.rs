//! The M1.11 crash-consistency harness binary (ADR-0008 D3; SPEC 06 §5).
//!
//! ```text
//! crash-harness [--cycles N] [--seed S] [--mechanism image|sigkill|both]
//!               [--jobs J] [--variants V]
//!               [--modes all|default|writemap|nometasync|nosync|mapasync]
//!               [--repro image:SEED|sigkill:SEED] [--keep-going] [--isolate]
//! ```
//!
//! One **cycle** = one recovered-and-verified crash state (ratified ADR-0008
//! OQ1). `--mechanism both` (default) targets ≈80% image / 20% SIGKILL **by
//! cycle** (one image cut ≈ `--variants` cycles, one SIGKILL cut = 1, hence
//! a 1:3 image:sigkill cut pattern); a full 10k run must land ≥1k verified
//! SIGKILL cycles (ratified OQ6) — checked at the end of full-sized runs.
//!
//! Exit status: nonzero iff a violation (or an unmet SIGKILL floor on a
//! ≥10k run) occurred. On a violation the seed, mode, cut, and repro
//! artifacts (image + fault plan + op stream) land under
//! `target/crash-repro/`, and `--repro <mech>:<seed>` replays that cycle.
//!
//! Hidden child mode (mechanism B): `crash-harness --crash-child <seed>
//! <dir> <kill>` — see `crash::sigkill`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use zerodb_io::fault::splitmix64;
use zerodb_oracle::crash::{
    gen_spec,
    image::{run_image_cut, ImageOpts},
    sigkill::{child_run, run_sigkill_cycle, SigkillOpts},
    CutReport, Mode,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mechanism {
    Image,
    Sigkill,
    Both,
}

struct Args {
    cycles: u64,
    seed: u64,
    mechanism: Mechanism,
    jobs: usize,
    variants: usize,
    keep_going: bool,
    isolate: bool,
    repro: Option<(Mechanism, u64)>,
    /// Restrict cycles to one durability mode (ADR-0008 D3 `--modes`): cut
    /// seeds whose derived spec is another mode are skipped (rejection
    /// sampling keeps seed→spec derivation pure). `None` = all modes.
    modes: Option<Mode>,
}

fn usage() -> ! {
    eprintln!(
        "usage: crash-harness [--cycles N] [--seed S] [--mechanism image|sigkill|both] \
         [--jobs J] [--variants V] [--modes all|default|writemap|nometasync|nosync|mapasync] \
         [--repro image:SEED|sigkill:SEED] [--keep-going] [--isolate]"
    );
    std::process::exit(2)
}

fn parse_mode(s: &str) -> Option<Mode> {
    match s {
        "all" => None,
        "default" => Some(Mode::Default),
        "writemap" => Some(Mode::WriteMap),
        "nometasync" => Some(Mode::NoMetaSync),
        "nosync" => Some(Mode::NoSync),
        "mapasync" => Some(Mode::MapAsync),
        _ => usage(),
    }
}

fn parse_args() -> Args {
    let mut args = Args {
        cycles: 200,
        seed: splitmix64(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(1),
        ),
        mechanism: Mechanism::Both,
        jobs: std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get),
        variants: 12,
        keep_going: false,
        isolate: false,
        repro: None,
        modes: None,
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    let take = |i: &mut usize, argv: &[String]| -> String {
        *i += 1;
        argv.get(*i).cloned().unwrap_or_else(|| usage())
    };
    while i < argv.len() {
        match argv[i].as_str() {
            "--cycles" => args.cycles = take(&mut i, &argv).parse().unwrap_or_else(|_| usage()),
            "--seed" => args.seed = take(&mut i, &argv).parse().unwrap_or_else(|_| usage()),
            "--jobs" => args.jobs = take(&mut i, &argv).parse().unwrap_or_else(|_| usage()),
            "--variants" => {
                args.variants = take(&mut i, &argv).parse().unwrap_or_else(|_| usage());
            }
            "--mechanism" => {
                args.mechanism = match take(&mut i, &argv).as_str() {
                    "image" => Mechanism::Image,
                    "sigkill" => Mechanism::Sigkill,
                    "both" => Mechanism::Both,
                    _ => usage(),
                }
            }
            "--repro" => {
                let v = take(&mut i, &argv);
                let (m, s) = v.split_once(':').unwrap_or_else(|| usage());
                let mech = match m {
                    "image" => Mechanism::Image,
                    "sigkill" => Mechanism::Sigkill,
                    _ => usage(),
                };
                args.repro = Some((mech, s.parse().unwrap_or_else(|_| usage())));
            }
            "--modes" => args.modes = parse_mode(&take(&mut i, &argv)),
            "--keep-going" => args.keep_going = true,
            "--isolate" => args.isolate = true,
            _ => usage(),
        }
        i += 1;
    }
    args
}

#[derive(Default)]
struct Totals {
    verified: u64,
    image_cuts: u64,
    sigkill_cycles: u64,
    sigkill_verified: u64,
    abandoned: u64,
    adv_probes: u64,
    adv_opened: u64,
    adv_invalid: u64,
    adv_walk_clean: u64,
    stale_fallback: u64,
    by_mode: std::collections::BTreeMap<&'static str, u64>,
    violations: Vec<String>,
}

impl Totals {
    fn absorb(&mut self, r: &CutReport, mech: Mechanism) {
        self.verified += r.verified;
        if mech == Mechanism::Image {
            self.image_cuts += 1;
        } else {
            self.sigkill_cycles += 1;
            self.sigkill_verified += r.verified;
        }
        if r.abandoned {
            self.abandoned += 1;
        }
        self.adv_probes += r.adv_probes;
        self.adv_opened += r.adv_opened;
        self.adv_invalid += r.adv_invalid;
        self.adv_walk_clean += r.adv_walk_clean;
        self.stale_fallback += r.stale_fallback;
        if let Some(m) = r.mode {
            *self.by_mode.entry(m.name()).or_default() += r.verified;
        }
        if let Some(v) = &r.violation {
            self.violations.push(v.clone());
        }
    }
}

fn main() {
    // Hidden child mode (mechanism B): positional, before flag parsing.
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).map(String::as_str) == Some("--crash-child") {
        if argv.len() != 5 {
            usage();
        }
        let seed: u64 = argv[2].parse().unwrap_or_else(|_| usage());
        let dir = PathBuf::from(&argv[3]);
        child_run(seed, &dir, &argv[4]); // never returns
    }

    let args = parse_args();
    let exe = std::env::current_exe().expect("own executable path");
    let repro_dir = PathBuf::from("target/crash-repro");

    // Single-cycle repro mode.
    if let Some((mech, seed)) = args.repro {
        let report = match mech {
            Mechanism::Image => {
                let mut opts = ImageOpts::for_worker(0, Some(repro_dir));
                opts.variants = args.variants;
                run_image_cut(seed, &opts)
            }
            _ => run_sigkill_cycle(
                seed,
                &SigkillOpts {
                    exe,
                    repro_dir: Some(repro_dir),
                },
            ),
        };
        match &report.violation {
            Some(v) => {
                eprintln!("VIOLATION: {v}");
                std::process::exit(1);
            }
            None => {
                println!(
                    "repro clean: {} cycles verified (abandoned: {})",
                    report.verified, report.abandoned
                );
                return;
            }
        }
    }

    println!(
        "crash-harness: target {} cycles, base seed {}, mechanism {:?}, {} jobs, {} variants/cut",
        args.cycles, args.seed, args.mechanism, args.jobs, args.variants
    );
    let started = std::time::Instant::now();
    let counted = AtomicU64::new(0);
    let next_cut = AtomicU64::new(0);
    let stop = AtomicBool::new(false);
    let totals = Mutex::new(Totals::default());

    std::thread::scope(|scope| {
        for w in 0..args.jobs {
            let counted = &counted;
            let next_cut = &next_cut;
            let stop = &stop;
            let totals = &totals;
            let args = &args;
            let exe = exe.clone();
            let repro_dir = repro_dir.clone();
            scope.spawn(move || {
                loop {
                    if stop.load(Ordering::Relaxed)
                        || counted.load(Ordering::Relaxed) >= args.cycles
                    {
                        return;
                    }
                    let cut = next_cut.fetch_add(1, Ordering::Relaxed);
                    // Cycle seeds are a pure function of (base seed, cut
                    // index): a run is reproducible regardless of --jobs.
                    let seed = splitmix64(args.seed ^ (cut.wrapping_mul(0x9E37_79B9)));
                    // --modes filter: reject cut seeds of other modes so the
                    // seed→spec derivation stays pure (a repro of a filtered
                    // run needs no filter flag).
                    if let Some(want) = args.modes {
                        if gen_spec(seed).mode != want {
                            continue;
                        }
                    }
                    // ≈80/20 image/SIGKILL split **by counted cycle** (the
                    // ratified OQ1 unit): one image cut yields ~`--variants`
                    // (12) cycles, one SIGKILL cut yields 1, so a 1:3
                    // image:sigkill cut pattern lands 12:3 = 80/20 by cycles
                    // and comfortably clears the ≥1k SIGKILL floor on a 10k
                    // run (ratified OQ6). Deterministic in the cut index so a
                    // run's mechanism assignment is `--jobs`-independent.
                    let mech = match args.mechanism {
                        Mechanism::Image => Mechanism::Image,
                        Mechanism::Sigkill => Mechanism::Sigkill,
                        Mechanism::Both => {
                            if cut % 4 == 3 {
                                Mechanism::Image
                            } else {
                                Mechanism::Sigkill
                            }
                        }
                    };
                    let report = match mech {
                        Mechanism::Sigkill => run_sigkill_cycle(
                            seed,
                            &SigkillOpts {
                                exe: exe.clone(),
                                repro_dir: Some(repro_dir.clone()),
                            },
                        ),
                        _ => {
                            let mut opts = ImageOpts::for_worker(w, Some(repro_dir.clone()));
                            opts.variants = args.variants;
                            if args.isolate {
                                run_isolated_image_cut(&exe, seed, &opts)
                            } else {
                                run_image_cut(seed, &opts)
                            }
                        }
                    };
                    counted.fetch_add(report.verified, Ordering::Relaxed);
                    let mut t = totals.lock().expect("totals lock");
                    let had_violation = report.violation.is_some();
                    t.absorb(&report, mech);
                    drop(t);
                    if had_violation && !args.keep_going {
                        stop.store(true, Ordering::Relaxed);
                        return;
                    }
                }
            });
        }
    });

    let t = totals.into_inner().expect("totals lock");
    let secs = started.elapsed().as_secs_f64();
    println!(
        "\n=== crash-harness summary ===\n\
         cycles verified : {} ({} image cuts, {} sigkill cycles → {} sigkill-verified)\n\
         abandoned cuts  : {}\n\
         by mode         : {:?}\n\
         adversarial     : {} probes — {} opened, {} designed-Invalid, {} walk-clean (characterization, not gated)\n\
         stale fallbacks : {} (NO_META_SYNC reclaim-clobber window, REC-10 as amended — walk/data waived)\n\
         wall time       : {:.1}s ({:.1} cycles/s)",
        t.verified,
        t.image_cuts,
        t.sigkill_cycles,
        t.sigkill_verified,
        t.abandoned,
        t.by_mode,
        t.adv_probes,
        t.adv_opened,
        t.adv_invalid,
        t.adv_walk_clean,
        t.stale_fallback,
        secs,
        t.verified as f64 / secs.max(0.001),
    );

    let mut failed = false;
    if !t.violations.is_empty() {
        failed = true;
        eprintln!("\n{} VIOLATION(S):", t.violations.len());
        for v in &t.violations {
            eprintln!("  - {v}");
        }
        eprintln!(
            "(base seed {}; repro artifacts under target/crash-repro/)",
            args.seed
        );
    }
    // Ratified OQ6: a full-sized run must include ≥1k SIGKILL cycles.
    if args.cycles >= 10_000 && args.mechanism == Mechanism::Both && t.sigkill_verified < 1_000 {
        failed = true;
        eprintln!(
            "\nSIGKILL floor unmet: {} < 1000 verified sigkill cycles on a full run (ADR-0008 OQ6)",
            t.sigkill_verified
        );
    }
    if failed {
        std::process::exit(1);
    }
}

/// `--isolate`: run one image cut in a child process (contamination triage,
/// ADR-0008 D6.2). The child is this binary in `--repro image:<seed>` mode;
/// its exit status carries the verdict, its stdout the count.
fn run_isolated_image_cut(exe: &std::path::Path, seed: u64, opts: &ImageOpts) -> CutReport {
    let out = std::process::Command::new(exe)
        .args([
            "--repro".to_string(),
            format!("image:{seed}"),
            "--variants".to_string(),
            opts.variants.to_string(),
        ])
        .output();
    let mut report = CutReport::default();
    match out {
        Err(e) => {
            report.violation = Some(format!("[image/isolated] seed={seed}: spawn failed: {e}"));
        }
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            // "repro clean: N cycles verified (abandoned: B)"
            if out.status.success() {
                report.verified = stdout
                    .split_whitespace()
                    .find_map(|w| w.parse::<u64>().ok())
                    .unwrap_or(0);
                report.abandoned = stdout.contains("abandoned: true");
            } else {
                report.violation = Some(format!(
                    "[image/isolated] seed={seed}: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                ));
            }
        }
    }
    report
}
