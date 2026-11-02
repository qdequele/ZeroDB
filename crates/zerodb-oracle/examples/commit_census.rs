//! Per-commit census for the `commit/batch/n1` gap: N single-put commits on a
//! fresh named database, the same shape and settings as the ladder rung
//! (NO_SYNC, 256-byte values, ascending 8-byte keys, zerodb pinned to the OS
//! page size), with nothing else in the process. Run it under `strace -c -f` or
//! `perf stat` and divide by N; unlike the criterion binary, no other suite's
//! fixtures are built first.
//!
//! ```text
//! cargo run --release -p zerodb-oracle --example commit_census -- <lmdb|zerodb> [N] [phases] [empty|del]
//! ```
//!
//! With `phases`, each commit's begin / put / commit is timed separately
//! (three `Instant` reads per commit, ~20-30 ns each with a TSC clocksource).
//!
//! `empty` commits N write txns that change nothing (the `env/txn/rw_empty_commit`
//! shape). `del` first fills the database with N keys of 1536-byte values in one
//! txn, untimed, then deletes them in shuffled order, one delete per committed
//! txn (the `del/random/n1` shape); the timed loop is the deletes.
//!
//! On macOS the timed loop's user+system instructions and cycles per commit
//! are printed too (`proc_pid_rusage`); on Linux use `perf stat`.

use std::hint::black_box;
use std::time::{Duration, Instant};

use zerodb_oracle::tempdir::TempDir;

macro_rules! census {
    ($name:ident, $heed:ident, $setpage:tt) => {
        fn $name(n: usize, phases: bool, write_map: bool, mode: Mode) {
            use $heed::types::Bytes;
            use $heed::{Database, EnvFlags, EnvOpenOptions};

            let dir = TempDir::new().expect("tempdir");
            let mut opts = EnvOpenOptions::new().read_txn_without_tls();
            opts.map_size(1 << 30);
            opts.max_dbs(16);
            census!(@setpage opts, $setpage);
            // SAFETY: single-process private temp dir; NO_SYNC (plus the
            // optional WRITE_MAP A/B toggle, ADR-0021) are the only flags.
            let env = unsafe {
                let mut flags = EnvFlags::NO_SYNC;
                if write_map {
                    flags |= EnvFlags::WRITE_MAP;
                }
                opts.flags(flags);
                opts.open(dir.path())
            }
            .expect("open env");
            let mut w = env.write_txn().expect("write_txn");
            let db: Database<Bytes, Bytes> =
                env.create_database(&mut w, Some("bench")).expect("create_database");
            w.commit().expect("commit");

            let val = vec![0xABu8; if mode == Mode::Del { 1536 } else { 256 }];
            let mut keys: Vec<[u8; 8]> = (0..n as u64).map(|i| i.to_be_bytes()).collect();
            if mode == Mode::Del {
                let mut w = env.write_txn().expect("write_txn");
                for k in &keys {
                    db.put(&mut w, k, &val).expect("put");
                }
                w.commit().expect("commit");
                shuffle(&mut keys);
            }
            let op = |db: Database<Bytes, Bytes>, w: &mut $heed::RwTxn<'_>, k: &[u8; 8], v: &[u8]| {
                match mode {
                    Mode::Put => db.put(w, k, v).expect("put"),
                    Mode::Empty => {}
                    Mode::Del => assert!(db.delete(w, k).expect("delete"), "key present"),
                }
            };
            let (mut t_begin, mut t_put, mut t_commit) =
                (Duration::ZERO, Duration::ZERO, Duration::ZERO);
            let counters_before = counters();
            let start = Instant::now();
            for k in &keys {
                if phases {
                    let a = Instant::now();
                    let mut w = env.write_txn().expect("write_txn");
                    let b = Instant::now();
                    op(db, &mut w, k, &val);
                    let c = Instant::now();
                    w.commit().expect("commit");
                    let d = Instant::now();
                    t_begin += b - a;
                    t_put += c - b;
                    t_commit += d - c;
                } else {
                    let mut w = env.write_txn().expect("write_txn");
                    op(db, &mut w, black_box(k), &val);
                    w.commit().expect("commit");
                }
            }
            let total = start.elapsed();
            let counters_after = counters();
            let per = |d: Duration| d.as_nanos() as f64 / n as f64;
            println!(
                "{} n={n} total/commit={:.0} ns{}",
                stringify!($name),
                per(total),
                if phases {
                    format!(
                        " begin={:.0} put={:.0} commit={:.0} ns",
                        per(t_begin),
                        per(t_put),
                        per(t_commit)
                    )
                } else {
                    String::new()
                }
            );
            if let (Some(a), Some(b)) = (counters_before, counters_after) {
                println!(
                    "{} instructions/commit={:.0} cycles/commit={:.0}",
                    stringify!($name),
                    (b.0 - a.0) as f64 / n as f64,
                    (b.1 - a.1) as f64 / n as f64
                );
            }
            let t = Instant::now();
            drop(env);
            drop(dir);
            println!("{} env+dir drop={:.1} us", stringify!($name), t.elapsed().as_secs_f64() * 1e6);
        }
    };
    (@setpage $opts:ident, set) => {
        let page = zerodb_oracle::os_page_size();
        $opts.page_size(page);
    };
    (@setpage $opts:ident, noset) => {};
}

/// This process's retired instructions and CPU cycles so far.
#[cfg(target_os = "macos")]
fn counters() -> Option<(u64, u64)> {
    let mut info = std::mem::MaybeUninit::<libc::rusage_info_v4>::zeroed();
    // SAFETY: `proc_pid_rusage` writes one `rusage_info_v4` for the
    // `RUSAGE_INFO_V4` flavor into the buffer it is given, which is that size.
    let rc = unsafe {
        libc::proc_pid_rusage(
            libc::getpid(),
            libc::RUSAGE_INFO_V4,
            info.as_mut_ptr().cast::<libc::rusage_info_t>(),
        )
    };
    if rc != 0 {
        return None;
    }
    // SAFETY: the call succeeded, so the struct is initialized.
    let info = unsafe { info.assume_init() };
    Some((info.ri_instructions, info.ri_cycles))
}

/// Counters are read only on macOS; elsewhere use `perf stat`.
#[cfg(not(target_os = "macos"))]
fn counters() -> Option<(u64, u64)> {
    None
}

/// What each timed write txn does.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// One put of a new key.
    Put,
    /// Nothing: the txn is committed unchanged.
    Empty,
    /// One delete of an existing key.
    Del,
}

/// Fisher-Yates with a fixed xorshift seed: the same order for both engines.
fn shuffle(keys: &mut [[u8; 8]]) {
    let mut s = 0x9E37_79B9_7F4A_7C15u64;
    for i in (1..keys.len()).rev() {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        keys.swap(i, (s % (i as u64 + 1)) as usize);
    }
}

census!(lmdb, heed, noset);
census!(zerodb, heed_zerodb, set);

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let engine = args.get(1).map(String::as_str).unwrap_or("zerodb");
    let n: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(20_000);
    let phases = args.iter().any(|a| a == "phases");
    // `writemap` anywhere in the tail sets `EnvFlags::WRITE_MAP` on either
    // engine — the ADR-0021 fair-comparison toggle (WRITE_MAP on both sides).
    let write_map = args.iter().any(|a| a == "writemap");
    let mode = if args.iter().any(|a| a == "empty") {
        Mode::Empty
    } else if args.iter().any(|a| a == "del") {
        Mode::Del
    } else {
        Mode::Put
    };
    match engine {
        "lmdb" => lmdb(n, phases, write_map, mode),
        "zerodb" => zerodb(n, phases, write_map, mode),
        other => panic!("engine must be lmdb or zerodb, not {other}"),
    }
}
