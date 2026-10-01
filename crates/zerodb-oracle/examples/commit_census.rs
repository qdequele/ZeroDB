//! Per-commit census for the `commit/batch/n1` gap: N single-put commits on a
//! fresh named database, the same shape and settings as the ladder rung
//! (NO_SYNC, 256-byte values, ascending 8-byte keys, zerodb pinned to the OS
//! page size), with nothing else in the process. Run it under `strace -c -f` or
//! `perf stat` and divide by N; unlike the criterion binary, no other suite's
//! fixtures are built first.
//!
//! ```text
//! cargo run --release -p zerodb-oracle --example commit_census -- <lmdb|zerodb> [N] [phases]
//! ```
//!
//! With `phases`, each commit's begin / put / commit is timed separately
//! (three `Instant` reads per commit, ~20-30 ns each with a TSC clocksource).

use std::hint::black_box;
use std::time::{Duration, Instant};

use zerodb_oracle::tempdir::TempDir;

macro_rules! census {
    ($name:ident, $heed:ident, $setpage:tt) => {
        fn $name(n: usize, phases: bool) {
            use $heed::types::Bytes;
            use $heed::{Database, EnvFlags, EnvOpenOptions};

            let dir = TempDir::new().expect("tempdir");
            let mut opts = EnvOpenOptions::new().read_txn_without_tls();
            opts.map_size(1 << 30);
            opts.max_dbs(16);
            census!(@setpage opts, $setpage);
            // SAFETY: single-process private temp dir; NO_SYNC is the only flag.
            let env = unsafe {
                opts.flags(EnvFlags::NO_SYNC);
                opts.open(dir.path())
            }
            .expect("open env");
            let mut w = env.write_txn().expect("write_txn");
            let db: Database<Bytes, Bytes> =
                env.create_database(&mut w, Some("bench")).expect("create_database");
            w.commit().expect("commit");

            let val = vec![0xABu8; 256];
            let keys: Vec<[u8; 8]> = (0..n as u64).map(|i| i.to_be_bytes()).collect();
            let (mut t_begin, mut t_put, mut t_commit) =
                (Duration::ZERO, Duration::ZERO, Duration::ZERO);
            let start = Instant::now();
            for k in &keys {
                if phases {
                    let a = Instant::now();
                    let mut w = env.write_txn().expect("write_txn");
                    let b = Instant::now();
                    db.put(&mut w, k, &val).expect("put");
                    let c = Instant::now();
                    w.commit().expect("commit");
                    let d = Instant::now();
                    t_begin += b - a;
                    t_put += c - b;
                    t_commit += d - c;
                } else {
                    let mut w = env.write_txn().expect("write_txn");
                    db.put(&mut w, black_box(k), &val).expect("put");
                    w.commit().expect("commit");
                }
            }
            let total = start.elapsed();
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
            let t = Instant::now();
            drop(env);
            drop(dir);
            println!("{} env+dir drop={:.1} us", stringify!($name), t.elapsed().as_secs_f64() * 1e6);
        }
    };
    (@setpage $opts:ident, set) => {
        // SAFETY: sysconf is always safe to call.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u32;
        $opts.page_size(page);
    };
    (@setpage $opts:ident, noset) => {};
}

census!(lmdb, heed, noset);
census!(zerodb, heed_zerodb, set);

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let engine = args.get(1).map(String::as_str).unwrap_or("zerodb");
    let n: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(20_000);
    let phases = args.get(3).map(String::as_str) == Some("phases");
    match engine {
        "lmdb" => lmdb(n, phases),
        "zerodb" => zerodb(n, phases),
        other => panic!("engine must be lmdb or zerodb, not {other}"),
    }
}
