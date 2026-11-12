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
//! ## Live environments (ADR-0023)
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
            let [dir] = positionals(parse_opts(rest)?.0, "stat <env-dir>")?;
            print!("{}", commands::cmd_stat(&dir)?);
            Ok(ExitCode::SUCCESS)
        }
        "dump" => {
            let (positional, opts) = parse_opts(rest)?;
            let [dir] = positionals(positional, "dump <env-dir> [--out FILE]")?;
            match opts.out {
                Some(out) => commands::write_dump(&dir, BufWriter::new(File::create(out)?))?,
                None => commands::write_dump(&dir, std::io::stdout().lock())?,
            }
            Ok(ExitCode::SUCCESS)
        }
        "load" => {
            let (positional, opts) = parse_opts(rest)?;
            let [dump, dir] = positionals(
                positional,
                "load <dump-file> <env-dir> [--page-size N] [--map-size BYTES]",
            )?;
            let psize = opts.page_size.unwrap_or(DEFAULT_PAGE_SIZE);
            let map_size = opts.map_size.unwrap_or(DEFAULT_MAP_SIZE);
            commands::cmd_load(&dump, &dir, psize, map_size)?;
            println!("loaded {} -> {}", dump.display(), dir.display());
            Ok(ExitCode::SUCCESS)
        }
        "check" => {
            let [dir] = positionals(parse_opts(rest)?.0, "check <env-dir>")?;
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
    let [src, dst] = positionals(
        positional,
        "migrate-from-lmdb <src-lmdb-dir> <dst-env-dir> [--page-size N] [--map-size BYTES]",
    )?;
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

/// Split `args` into positional args and the `--out`, `--page-size` and
/// `--map-size` options, each followed by its value.
///
/// # Errors
///
/// An unknown `--flag`, a flag without a value, or a value that does not
/// parse: each is a usage error, never taken as a path or a default.
fn parse_opts(args: &[String]) -> Result<(Vec<String>, Opts), common::BoxErr> {
    let mut positional = Vec::new();
    let mut opts = Opts::default();
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let flag = arg.as_str();
        if !flag.starts_with("--") {
            positional.push(arg.clone());
            continue;
        }
        let value = || -> Result<&String, common::BoxErr> {
            Err(format!("option {flag} needs a value\n{USAGE}").into())
        };
        let invalid = |v: &str| -> common::BoxErr {
            format!("option {flag}: {v:?} is not a valid number\n{USAGE}").into()
        };
        match flag {
            "--out" => {
                let v = args.next().map_or_else(value, Ok)?;
                opts.out = Some(PathBuf::from(v));
            }
            "--page-size" => {
                let v = args.next().map_or_else(value, Ok)?;
                opts.page_size = Some(v.parse().map_err(|_| invalid(v))?);
            }
            "--map-size" => {
                let v = args.next().map_or_else(value, Ok)?;
                opts.map_size = Some(v.parse().map_err(|_| invalid(v))?);
            }
            // `check --verbose dir` must not open a directory named `--verbose`.
            _ => return Err(format!("unknown option {flag:?}\n{USAGE}").into()),
        }
    }
    Ok((positional, opts))
}

/// The subcommand's exactly `N` positional arguments, as paths.
///
/// # Errors
///
/// Too few arguments, or an extra one: a mistyped command line must not run
/// on a prefix of what was typed.
fn positionals<const N: usize>(
    positional: Vec<String>,
    usage: &str,
) -> Result<[PathBuf; N], common::BoxErr> {
    if let Some(extra) = positional.get(N) {
        return Err(format!("unexpected argument {extra:?}\nusage: {usage}").into());
    }
    let paths: Vec<PathBuf> = positional.into_iter().map(PathBuf::from).collect();
    paths
        .try_into()
        .map_err(|_| format!("usage: {usage}").into())
}

#[cfg(test)]
mod tests {
    use super::{dispatch, parse_opts, positionals};
    use std::path::PathBuf;

    fn args(a: &[&str]) -> Vec<String> {
        a.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn parses_positionals_and_options() {
        let (positional, opts) = parse_opts(&args(&[
            "in",
            "--page-size",
            "8192",
            "out",
            "--map-size",
            "1048576",
        ]))
        .unwrap();
        assert_eq!(positional, args(&["in", "out"]));
        assert_eq!(opts.page_size, Some(8192));
        assert_eq!(opts.map_size, Some(1 << 20));
    }

    #[test]
    fn rejects_bad_or_missing_values_and_unknown_flags() {
        for bad in [
            &["dir", "--page-size", "abc"][..],
            &["dir", "--map-size", "-1"],
            &["dir", "--page-size"],
            &["dir", "--out"],
            &["--verbose", "dir"],
        ] {
            assert!(parse_opts(&args(bad)).is_err(), "{bad:?} was accepted");
        }
    }

    #[test]
    fn positionals_takes_exactly_the_expected_count() {
        let [a, b] = positionals(args(&["in", "out"]), "cmd <a> <b>").unwrap();
        assert_eq!((a, b), (PathBuf::from("in"), PathBuf::from("out")));

        let missing = positionals::<2>(args(&["in"]), "cmd <a> <b>").unwrap_err();
        assert_eq!(missing.to_string(), "usage: cmd <a> <b>");

        let extra = positionals::<1>(args(&["in", "stray"]), "cmd <a>").unwrap_err();
        assert_eq!(
            extra.to_string(),
            "unexpected argument \"stray\"\nusage: cmd <a>"
        );
    }

    /// An extra argument is refused before the subcommand touches any path:
    /// none of these paths exist, and the error names the stray argument
    /// instead of a missing file.
    #[test]
    fn subcommands_reject_extra_positional_arguments() {
        for bad in [
            &["stat", "/nonexistent/env", "stray"][..],
            &["check", "/nonexistent/env", "stray"],
            &["dump", "/nonexistent/env", "stray"],
            &[
                "dump",
                "/nonexistent/env",
                "--out",
                "/nonexistent/o",
                "stray",
            ],
            &["load", "/nonexistent/dump", "/nonexistent/env", "stray"],
        ] {
            let err = dispatch(&args(bad)).unwrap_err().to_string();
            assert!(
                err.starts_with("unexpected argument \"stray\""),
                "{bad:?}: {err}"
            );
        }
    }
}
