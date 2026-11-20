//! Small-commit cost after one large delete, LMDB vs ZeroDB.
//!
//! One transaction clears a database of `pages` overflow pages, so its
//! freed-page list is too long for the meta page and goes to the free-page
//! database as one record. Then `commits` small write transactions follow,
//! each putting one short key into another database; their pages come from
//! that record. The example times every small commit, for both engines at
//! the OS page size, and prints the first two commits and the spread of the
//! rest. A run with `pages = 0` is the control: the same small commits with
//! no large record.
//!
//! ```text
//! cargo run --release -p zerodb-oracle --example freelist_big_record -- \
//!     [commits] [sync] [warmup] [pages...]
//! ```
//!
//! `sync = 0` opens both environments with `NO_SYNC`, so the times are the
//! commit's CPU and page-cache writes; `sync = 1` keeps the default flush.
//! `warmup` empty write transactions run first, so the large delete gets a
//! higher transaction id: LMDB re-saves what is left of a record it drew
//! from under keys below that id, and a fresh environment has few of them.

use zerodb_oracle::tempdir::TempDir;

/// Times of the small commits, in microseconds, in commit order.
struct Outcome {
    commit_us: Vec<f64>,
    delete_ms: f64,
}

macro_rules! engine {
    ($m:ident, $heed:ident) => {
        mod $m {
            use super::Outcome;
            use std::path::Path;
            use std::time::Instant;
            use $heed::types::Bytes;
            use $heed::{Database, EnvFlags, EnvOpenOptions};

            pub fn run(dir: &Path, pages: u64, commits: usize, sync: bool, warmup: u64) -> Outcome {
                let mut o = EnvOpenOptions::new().read_txn_without_tls();
                o.map_size(64 << 30).max_dbs(2);
                if !sync {
                    // SAFETY: a private temp dir; durability is not measured.
                    unsafe { o.flags(EnvFlags::NO_SYNC) };
                }
                // SAFETY: a private temp dir, one process.
                let env = unsafe { o.open(dir) }.expect("open env");
                let mut w = env.write_txn().unwrap();
                let big: Database<Bytes, Bytes> = env.create_database(&mut w, Some("big")).unwrap();
                let small: Database<Bytes, Bytes> =
                    env.create_database(&mut w, Some("small")).unwrap();
                w.commit().unwrap();
                for k in 0..warmup {
                    let mut w = env.write_txn().unwrap();
                    small.put(&mut w, b"warmup", &k.to_be_bytes()).unwrap();
                    w.commit().unwrap();
                }
                let ps = big.stat(&env.read_txn().unwrap()).unwrap().page_size as u64;
                // Three overflow pages per value.
                let value = vec![7u8; (2 * ps + ps / 2) as usize];
                let mut w = env.write_txn().unwrap();
                for k in 0..pages / 3 {
                    big.put(&mut w, &k.to_be_bytes(), &value).unwrap();
                }
                w.commit().unwrap();
                let t = Instant::now();
                let mut w = env.write_txn().unwrap();
                big.clear(&mut w).unwrap();
                w.commit().unwrap();
                let delete_ms = t.elapsed().as_secs_f64() * 1e3;
                let mut commit_us = Vec::with_capacity(commits);
                for k in 0..commits as u64 {
                    let t = Instant::now();
                    let mut w = env.write_txn().unwrap();
                    small.put(&mut w, &k.to_be_bytes(), &[1u8; 64]).unwrap();
                    w.commit().unwrap();
                    commit_us.push(t.elapsed().as_secs_f64() * 1e6);
                }
                Outcome {
                    commit_us,
                    delete_ms,
                }
            }
        }
    };
}

engine!(lmdb, heed);
engine!(zerodb, heed_zerodb);

fn percentile(sorted: &[f64], p: f64) -> f64 {
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

fn report(name: &str, pages: u64, o: &Outcome) {
    let mut rest = o.commit_us[2..].to_vec();
    rest.sort_by(f64::total_cmp);
    println!(
        "{name:7} {pages:>8} {:>10.1} {:>10.1} {:>10.1} {:>10.1} {:>10.1} {:>10.1}",
        o.delete_ms,
        o.commit_us[0],
        o.commit_us[1],
        percentile(&rest, 0.5),
        percentile(&rest, 0.9),
        rest.iter().sum::<f64>() / rest.len() as f64,
    );
}

fn main() {
    let mut args = std::env::args().skip(1);
    let commits: usize = args.next().map_or(200, |a| a.parse().unwrap());
    let sync = args.next().is_some_and(|a| a == "1");
    let warmup: u64 = args.next().map_or(0, |a| a.parse().unwrap());
    let mut sizes: Vec<u64> = args.map(|a| a.parse().unwrap()).collect();
    if sizes.is_empty() {
        sizes = vec![0, 3_000, 12_000, 48_000];
    }
    assert!(commits >= 3, "need at least three small commits");
    println!(
        "small commits {commits}  sync {sync}  warmup {warmup}  (times: delete ms, commits µs)"
    );
    println!(
        "{:7} {:>8} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "engine", "pages", "delete", "commit 1", "commit 2", "p50 3..", "p90 3..", "mean 3.."
    );
    for &pages in &sizes {
        let (a, b) = (TempDir::new().unwrap(), TempDir::new().unwrap());
        report(
            "lmdb",
            pages,
            &lmdb::run(a.path(), pages, commits, sync, warmup),
        );
        report(
            "zerodb",
            pages,
            &zerodb::run(b.path(), pages, commits, sync, warmup),
        );
    }
}
