//! File growth under multi-page value churn, LMDB vs ZeroDB (issue #29).
//!
//! Drives the same seeded workload through the Meilisearch LMDB fork (heed)
//! and through ZeroDB (heed-zerodb), both at the OS page size, and prints the
//! file's page count next to the pages the tree actually uses. The workload
//! rewrites a fraction of a key set every commit with values whose length is
//! drawn anew, so overflow runs are freed and re-allocated with changing
//! lengths: the pattern of Meilisearch documents (`obkv`) and compressed id
//! sets updated across indexing batches. Where a value needs a run that only
//! exists across two freed-page records, ZeroDB extends the file and LMDB
//! does not (`tests/lmdb_free_run_observation.rs`); this measures what that
//! costs over many commits.
//!
//! ```text
//! cargo run --release -p zerodb-oracle --example free_run_churn -- \
//!     [keys] [cycles] [rewrites_per_cycle] [max_value_pages]
//! ```
//!
//! `max_value_pages = 0` keeps every value inline (a control: no overflow
//! runs, so any size difference comes from elsewhere).

use zerodb_oracle::tempdir::TempDir;

/// Deterministic xorshift64*, the same stream for both engines.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

struct Params {
    keys: u64,
    cycles: usize,
    rewrites: u64,
    max_pages: u64,
}

/// The value length of one put: inline (up to a quarter page) half the
/// time, else an overflow value of 2..=max_pages pages.
fn value_len(rng: &mut Rng, ps: u64, max_pages: u64) -> usize {
    let r = rng.next();
    if max_pages < 2 || r.is_multiple_of(2) {
        (64 + (r >> 8) % (ps / 4 - 64)) as usize
    } else {
        let pages = 2 + (r >> 8) % (max_pages - 1);
        ((pages - 1) * ps + ps / 2) as usize
    }
}

struct Outcome {
    file_pages: usize,
    used_pages: usize,
    secs: f64,
}

macro_rules! engine {
    ($m:ident, $heed:ident) => {
        mod $m {
            use super::{value_len, Outcome, Params, Rng};
            use std::path::Path;
            use $heed::types::Bytes;
            use $heed::{Database, EnvOpenOptions};

            pub fn run(dir: &Path, p: &Params) -> Outcome {
                let mut o = EnvOpenOptions::new().read_txn_without_tls();
                o.map_size(64 << 30);
                // SAFETY: a private temp dir, one process.
                let env = unsafe { o.open(dir) }.expect("open env");
                let mut w = env.write_txn().unwrap();
                let db: Database<Bytes, Bytes> = env.create_database(&mut w, None).unwrap();
                w.commit().unwrap();
                let ps = db.stat(&env.read_txn().unwrap()).unwrap().page_size as u64;
                let mut rng = Rng(0x5EED_0029);
                let start = std::time::Instant::now();
                let mut w = env.write_txn().unwrap();
                for k in 0..p.keys {
                    let len = value_len(&mut rng, ps, p.max_pages);
                    db.put(&mut w, &k.to_be_bytes(), &vec![k as u8; len])
                        .unwrap();
                }
                w.commit().unwrap();
                for c in 0..p.cycles {
                    let mut w = env.write_txn().unwrap();
                    for _ in 0..p.rewrites {
                        let k = rng.next() % p.keys;
                        let len = value_len(&mut rng, ps, p.max_pages);
                        db.put(&mut w, &k.to_be_bytes(), &vec![c as u8; len])
                            .unwrap();
                    }
                    w.commit().unwrap();
                }
                let secs = start.elapsed().as_secs_f64();
                let r = env.read_txn().unwrap();
                let s = db.stat(&r).unwrap();
                let used = s.branch_pages + s.leaf_pages + s.overflow_pages;
                Outcome {
                    file_pages: env.info().last_page_number + 1,
                    used_pages: used,
                    secs,
                }
            }
        }
    };
}

engine!(lmdb, heed);
engine!(zerodb, heed_zerodb);

fn arg(i: usize, default: u64) -> u64 {
    std::env::args()
        .nth(i)
        .map(|a| a.parse().expect("numeric argument"))
        .unwrap_or(default)
}

fn report(name: &str, o: &Outcome) {
    println!(
        "{name:7} file {:>9} pages  used {:>9} pages  file/used {:.3}  {:.2} s",
        o.file_pages,
        o.used_pages,
        o.file_pages as f64 / o.used_pages as f64,
        o.secs
    );
}

fn main() {
    let p = Params {
        keys: arg(1, 20_000),
        cycles: arg(2, 200) as usize,
        rewrites: arg(3, 2_000),
        max_pages: arg(4, 16),
    };
    println!(
        "keys {} cycles {} rewrites/cycle {} max value pages {}",
        p.keys, p.cycles, p.rewrites, p.max_pages
    );
    let (a, b) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let l = lmdb::run(a.path(), &p);
    let z = zerodb::run(b.path(), &p);
    report("lmdb", &l);
    report("zerodb", &z);
    println!(
        "zerodb/lmdb file pages {:.3}",
        z.file_pages as f64 / l.file_pages as f64
    );
}
