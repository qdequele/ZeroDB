//! `zerodb-tools` — utilities for zerodb env directories.
//!
//! A single binary with subcommands:
//!
//! - `stat  <env-dir>` — env + per-DB statistics.
//! - `dump  <env-dir> [--out FILE]` — logical dump (mdb_dump-shaped, hex).
//! - `load  <dump-file> <env-dir>` — rebuild a fresh env from a dump.
//! - `check <env-dir>` — run the invariant walker; exit 1 on any violation.
//! - `migrate-from-lmdb <src> <dst>` — stream a real LMDB env into a fresh
//!   zerodb env (needs the `migrate-lmdb` feature).
//!
//! ## Live environments (ADR-0023 amendment 18)
//!
//! `stat`, `dump` and `check` open the env `READ_ONLY` through the engine,
//! with its lock file, and so attach to an env that other processes have
//! open, as LMDB's `mdb_stat` and `mdb_dump` do: they take a reader slot and
//! read one pinned snapshot, while writers keep committing. `check` walks
//! that snapshot; only when the env does not open as a valid one does it walk
//! the raw file, and then only while holding the lock file exclusively
//! ([`zerodb::ExclusiveLock`]), refusing if another process has the env open.
//!
//! `load` and `migrate-from-lmdb` create a fresh env: they refuse a
//! non-empty destination, open it through the engine, refuse it unless this
//! process opened it alone ([`zerodb::Env::opened_exclusively`]), and hold it
//! for the whole run, writing through ordinary write txns.
//!
//! ## Which data file? (ADR-0010)
//!
//! The data-file name depends on the stack that created the env — `zerodb.dat`
//! natively, `data.mdb` through the `heed-zerodb` adapter. The read tools
//! (`stat`/`dump`/`check`) therefore **probe both names** (`crate::naming`)
//! and open the lock file named after the one found
//! ([`zerodb::derived_lock_file_name`]: `zerodb.lock` or `lock.mdb`), the
//! same lock file as the processes they attach to; a directory holding *both*
//! data files is a hard error, never a silent pick. `stat` reports the engine
//! and format read from the file's own magic, so an operator never has to
//! infer it from the name.
//!
//! The crate is a library (so the dump format and command functions are
//! unit- and integration-testable) plus a thin `main.rs` that parses argv and
//! dispatches through [`run`].

#![deny(missing_docs)]
pub mod commands;
pub mod common;
pub mod dump_format;
pub mod naming;

#[cfg(feature = "migrate-lmdb")]
pub mod migrate;

use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::process::ExitCode;

/// Default DB page size when `load`/`migrate` create a new env (SPEC 02 §0).
const DEFAULT_PAGE_SIZE: u32 = 4096;
/// Default map size for a freshly created env (256 MiB; `--map-size` overrides).
const DEFAULT_MAP_SIZE: u64 = 256 << 20;

/// Parse `argv` (excluding the program name) and run the selected subcommand.
/// Returns the process exit code: `0` success, `1` = `check` found violations,
/// `2` = usage/other error.
#[must_use]
pub fn run(args: &[String]) -> ExitCode {
    match dispatch(args) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("zerodb-tools: error: {e}");
            ExitCode::from(2)
        }
    }
}

fn dispatch(args: &[String]) -> Result<ExitCode, common::BoxErr> {
    let (cmd, rest) = match args.split_first() {
        Some(x) => x,
        None => {
            eprintln!("{USAGE}");
            return Ok(ExitCode::from(2));
        }
    };

    match cmd.as_str() {
        "stat" => {
            let dir = require_positional(rest, "stat <env-dir>")?;
            print!("{}", commands::cmd_stat(&dir)?);
            Ok(ExitCode::SUCCESS)
        }
        "dump" => {
            let (positional, opts) = parse_opts(rest)?;
            let dir = first_positional(&positional, "dump <env-dir> [--out FILE]")?;
            match opts.out {
                Some(out) => commands::write_dump(&dir, BufWriter::new(File::create(out)?))?,
                None => commands::write_dump(&dir, std::io::stdout().lock())?,
            }
            Ok(ExitCode::SUCCESS)
        }
        "load" => {
            let (positional, opts) = parse_opts(rest)?;
            if positional.len() < 2 {
                return Err(
                    "usage: load <dump-file> <env-dir> [--page-size N] [--map-size BYTES]".into(),
                );
            }
            let dump = PathBuf::from(&positional[0]);
            let dir = PathBuf::from(&positional[1]);
            let psize = opts.page_size.unwrap_or(DEFAULT_PAGE_SIZE);
            let map_size = opts.map_size.unwrap_or(DEFAULT_MAP_SIZE);
            commands::cmd_load(&dump, &dir, psize, map_size)?;
            println!("loaded {} -> {}", dump.display(), dir.display());
            Ok(ExitCode::SUCCESS)
        }
        "check" => {
            let dir = require_positional(rest, "check <env-dir>")?;
            let (report, clean) = commands::cmd_check(&dir)?;
            print!("{report}");
            Ok(if clean {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
        "migrate-from-lmdb" => run_migrate(rest),
        "-h" | "--help" | "help" => {
            println!("{USAGE}");
            Ok(ExitCode::SUCCESS)
        }
        "-V" | "--version" | "version" => {
            println!(
                "zerodb-tools {} (on-disk format version {})",
                env!("CARGO_PKG_VERSION"),
                zerodb_core::page::FORMAT_VERSION
            );
            Ok(ExitCode::SUCCESS)
        }
        other => Err(format!("unknown subcommand {other:?}\n{USAGE}").into()),
    }
}

#[cfg(feature = "migrate-lmdb")]
fn run_migrate(rest: &[String]) -> Result<ExitCode, common::BoxErr> {
    let (positional, opts) = parse_opts(rest)?;
    if positional.len() < 2 {
        return Err(
            "usage: migrate-from-lmdb <src-lmdb-dir> <dst-env-dir> [--page-size N] [--map-size BYTES]"
                .into(),
        );
    }
    let src = PathBuf::from(&positional[0]);
    let dst = PathBuf::from(&positional[1]);
    let psize = opts.page_size.unwrap_or(DEFAULT_PAGE_SIZE);
    let map_size = opts.map_size.unwrap_or(DEFAULT_MAP_SIZE) as usize;
    let report = migrate::cmd_migrate(&src, &dst, map_size, psize)?;
    print!("{report}");
    Ok(ExitCode::SUCCESS)
}

#[cfg(not(feature = "migrate-lmdb"))]
fn run_migrate(_rest: &[String]) -> Result<ExitCode, common::BoxErr> {
    Err("migrate-from-lmdb requires building zerodb-tools with --features migrate-lmdb".into())
}

const USAGE: &str = "\
zerodb-tools <SUBCOMMAND> [ARGS]

Subcommands:
  stat  <env-dir>                       env + per-DB statistics
  dump  <env-dir> [--out FILE]          logical dump (mdb_dump-shaped)
  load  <dump-file> <env-dir>           rebuild a fresh env from a dump
        [--page-size N] [--map-size BYTES]
  check <env-dir>                       invariant check (exit 1 on violations)
  migrate-from-lmdb <src> <dst>         stream an LMDB env into a fresh env
        [--page-size N] [--map-size BYTES]   (requires --features migrate-lmdb)
  -V, --version                         print the zerodb-tools version
  -h, --help                            this text

stat, dump and check attach read-only to an env other processes have open,
through its lock file, and read one consistent snapshot. load and
migrate-from-lmdb refuse a destination that is non-empty or open elsewhere.";

/// Parsed optional flags shared by several subcommands.
#[derive(Default)]
struct Opts {
    out: Option<PathBuf>,
    page_size: Option<u32>,
    map_size: Option<u64>,
}

/// Split `args` into positional args and recognised `--flag value` options.
/// Unknown `--flags` are treated as positional (kept simple; hand-rolled, no
/// `clap`). Returns `(positional, opts)`.
fn parse_opts(args: &[String]) -> Result<(Vec<String>, Opts), common::BoxErr> {
    let mut positional = Vec::new();
    let mut opts = Opts::default();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            // An unknown `--flag` is a usage error, not a path: `check --verbose
            // dir` must not try to open a directory named `--verbose`.
            flag if flag.starts_with("--")
                && !matches!(flag, "--out" | "--page-size" | "--map-size") =>
            {
                return Err(format!("unknown option {flag:?}\n{USAGE}").into());
            }
            "--out" => {
                if let Some(v) = args.get(i + 1) {
                    opts.out = Some(PathBuf::from(v));
                    i += 2;
                    continue;
                }
            }
            "--page-size" => {
                if let Some(v) = args.get(i + 1).and_then(|s| s.parse().ok()) {
                    opts.page_size = Some(v);
                    i += 2;
                    continue;
                }
            }
            "--map-size" => {
                if let Some(v) = args.get(i + 1).and_then(|s| s.parse().ok()) {
                    opts.map_size = Some(v);
                    i += 2;
                    continue;
                }
            }
            _ => {}
        }
        positional.push(args[i].clone());
        i += 1;
    }
    Ok((positional, opts))
}

fn require_positional(rest: &[String], usage: &str) -> Result<PathBuf, common::BoxErr> {
    let (positional, _) = parse_opts(rest)?;
    first_positional(&positional, usage)
}

fn first_positional(positional: &[String], usage: &str) -> Result<PathBuf, common::BoxErr> {
    positional
        .first()
        .map(PathBuf::from)
        .ok_or_else(|| format!("usage: {usage}").into())
}
