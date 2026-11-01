//! Bulk-write census under `WRITE_MAP` (issue #13): the shapes of the ladder
//! rungs `put/order/seq`, `put/order/rand`, `put/val/v4k` and
//! `commit/batch/n10k`, each timed over many fresh envs, reporting the
//! minimum and the median ns per put. The minimum is the number to compare
//! on a loaded machine: interference only ever adds time.
//!
//! ```text
//! cargo run --release -p zerodb-oracle --example writemap_bulk_census -- <lmdb|zerodb> [reps] [shape]
//! ```
//!
//! `CENSUS_WRITE_MAP=0` runs the same shapes without `WRITE_MAP`;
//! `CENSUS_TRUST=1` opens ZeroDB with the trusting page-validation policy
//! (ADR-0014).

use std::hint::black_box;
use std::time::Instant;

use zerodb_oracle::tempdir::TempDir;

/// xorshift64*: deterministic, identical stream for both engines.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

/// A shape: its keys in insertion order, the value width and the puts per
/// write txn.
struct Shape {
    name: &'static str,
    keys: Vec<[u8; 8]>,
    val: usize,
    per_txn: usize,
}

fn shapes() -> Vec<Shape> {
    let asc = |n: u64| (0..n).map(u64::to_be_bytes).collect::<Vec<_>>();
    let mut rand = asc(50_000);
    let mut rng = Rng(0x1234_5678);
    for i in (1..rand.len()).rev() {
        rand.swap(i, (rng.next() % (i as u64 + 1)) as usize);
    }
    vec![
        Shape {
            name: "seq",
            keys: asc(50_000),
            val: 100,
            per_txn: usize::MAX,
        },
        Shape {
            name: "rand",
            keys: rand,
            val: 100,
            per_txn: usize::MAX,
        },
        Shape {
            name: "v4k",
            keys: asc(10_000),
            val: 4096,
            per_txn: usize::MAX,
        },
        Shape {
            name: "n10k",
            keys: asc(50_000),
            val: 100,
            per_txn: 10_000,
        },
    ]
}

macro_rules! census {
    ($name:ident, $heed:ident, $setpage:tt) => {
        /// ns per put of one run of `shape` in a fresh env.
        fn $name(shape: &Shape, write_map: bool) -> f64 {
            use $heed::types::Bytes;
            use $heed::{Database, EnvFlags, EnvOpenOptions};

            let dir = TempDir::new().expect("tempdir");
            let mut opts = EnvOpenOptions::new().read_txn_without_tls();
            opts.map_size(1 << 30);
            census!(@setpage opts, $setpage);
            let mut flags = EnvFlags::NO_SYNC;
            if write_map {
                flags |= EnvFlags::WRITE_MAP;
            }
            // SAFETY: single-process private temp dir; NO_SYNC/WRITE_MAP are
            // the bench's own flags.
            unsafe { opts.flags(flags) };
            // SAFETY: as above.
            let env = unsafe { opts.open(dir.path()) }.expect("open env");
            let mut w = env.write_txn().expect("write_txn");
            let db: Database<Bytes, Bytes> = env.create_database(&mut w, None).expect("db");
            w.commit().expect("commit");
            let val = vec![0xABu8; shape.val];

            let t0 = Instant::now();
            for chunk in shape.keys.chunks(shape.per_txn.min(shape.keys.len())) {
                let mut w = env.write_txn().expect("write_txn");
                for k in chunk {
                    db.put(&mut w, k, &val).expect("put");
                }
                w.commit().expect("commit");
            }
            let dt = t0.elapsed();
            black_box(&env);
            dt.as_nanos() as f64 / shape.keys.len() as f64
        }
    };
    (@setpage $opts:ident, set) => {
        // Pin zerodb to the OS page size, as LMDB derives it.
        $opts.page_size(os_page_size());
        if std::env::var("CENSUS_TRUST").as_deref() == Ok("1") {
            // SAFETY: a fresh temp directory written only by zerodb in this
            // process — the `trust_contents` contract.
            $opts.file_trust(unsafe { heed_zerodb::FileTrust::trust_contents() });
        }
    };
    (@setpage $opts:ident, noset) => {};
}

fn os_page_size() -> u32 {
    // SAFETY: sysconf with a valid name has no preconditions.
    let ps = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    u32::try_from(ps).unwrap_or(4096)
}

census!(run_lmdb, heed, noset);
census!(run_zerodb, heed_zerodb, set);

fn main() {
    let mut args = std::env::args().skip(1);
    let engine = args.next().unwrap_or_else(|| "zerodb".into());
    let reps: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(15);
    let only = args.next();
    let write_map = std::env::var("CENSUS_WRITE_MAP").as_deref() != Ok("0");
    for shape in shapes() {
        if only.as_deref().is_some_and(|o| o != shape.name) {
            continue;
        }
        let mut ns: Vec<f64> = (0..reps)
            .map(|_| match engine.as_str() {
                "lmdb" => run_lmdb(&shape, write_map),
                "zerodb" => run_zerodb(&shape, write_map),
                other => panic!("unknown engine {other}"),
            })
            .collect();
        ns.sort_by(f64::total_cmp);
        println!(
            "{engine} {} {:<5} min {:7.1} ns/put  median {:7.1}",
            if write_map { "wm" } else { "--" },
            shape.name,
            ns[0],
            ns[ns.len() / 2]
        );
    }
}
