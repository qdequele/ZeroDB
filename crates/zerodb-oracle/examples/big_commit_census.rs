//! Large-commit census for Meilisearch's indexing commit
//! (`indexing::scheduler::commit`, hackernews at 1M documents): batches shaped
//! like a milli indexing txn — one write txn per batch, many puts in random
//! key order spread over several named databases, a Meilisearch-like value
//! size mix (mostly tiny bitmaps, some mid-size, a few overflow values), and
//! later batches rewriting existing keys so the free list grows — each
//! committed durably (no NO_SYNC), as Meilisearch commits. Put and commit are
//! timed separately per batch. Run it under `perf record -g` to see where a
//! commit spends its time.
//!
//! ```text
//! cargo run --release -p zerodb-oracle --example big_commit_census -- <lmdb|zerodb> [batches] [puts_per_batch]
//! ```

use std::hint::black_box;
use std::time::{Duration, Instant};

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

/// Value length: 70 % tiny (8–28 B, a small CboRoaringBitmap), 25 % mid
/// (50–1500 B), 5 % overflow-sized (2–12 KB, a frequent word's bitmap or a
/// document).
fn value_len(r: u64) -> usize {
    match r % 100 {
        0..=69 => 8 + (r >> 8) as usize % 21,
        70..=94 => 50 + (r >> 8) as usize % 1451,
        _ => 2048 + (r >> 8) as usize % 10_240,
    }
}

const DBS: usize = 8;

macro_rules! census {
    ($name:ident, $heed:ident, $setpage:tt) => {
        fn $name(batches: usize, puts: usize) -> (Duration, Duration) {
            use $heed::types::Bytes;
            use $heed::{Database, EnvOpenOptions};

            let dir = TempDir::new().expect("tempdir");
            let mut opts = EnvOpenOptions::new().read_txn_without_tls();
            opts.map_size(64 << 30);
            opts.max_dbs(16);
            census!(@setpage opts, $setpage);
            // SAFETY: single-process private temp dir, default (durable) flags.
            let env = unsafe { opts.open(dir.path()) }.expect("open env");
            let mut w = env.write_txn().expect("write_txn");
            let dbs: Vec<Database<Bytes, Bytes>> = (0..DBS)
                .map(|i| {
                    env.create_database(&mut w, Some(&format!("db{i}")))
                        .expect("create_database")
                })
                .collect();
            w.commit().expect("commit");

            let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
            let buf = vec![0xA5u8; 12 * 1024 + 2048];
            // The key space grows with the batches; half of each batch
            // rewrites keys from earlier batches (read-modify-write in milli),
            // which frees their old pages.
            let key_space = |b: usize| ((b + 1) * puts) as u64;
            let (mut put_total, mut commit_total) = (Duration::ZERO, Duration::ZERO);
            for b in 0..batches {
                let t0 = Instant::now();
                let mut w = env.write_txn().expect("write_txn");
                for _ in 0..puts {
                    let r = rng.next();
                    let db = &dbs[(r % DBS as u64) as usize];
                    let k = (rng.next() % key_space(b)).to_be_bytes();
                    let len = value_len(rng.next());
                    db.put(&mut w, &k, &buf[..len]).expect("put");
                }
                let t1 = Instant::now();
                w.commit().expect("commit");
                let t2 = Instant::now();
                put_total += t1 - t0;
                commit_total += t2 - t1;
                eprintln!(
                    "batch {b:2}: put {:8.1} ms  commit {:8.1} ms",
                    (t1 - t0).as_secs_f64() * 1e3,
                    (t2 - t1).as_secs_f64() * 1e3
                );
            }
            black_box(&env);
            (put_total, commit_total)
        }
    };
    (@setpage $opts:ident, set) => {
        // Pin zerodb to the OS page size, as LMDB derives it (the ladder's
        // single asymmetry; see the engine_comparison backend).
        $opts.page_size(os_page_size());
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
    let batches: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(10);
    let puts: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(200_000);
    let (p, c) = match engine.as_str() {
        "lmdb" => run_lmdb(batches, puts),
        "zerodb" => run_zerodb(batches, puts),
        other => panic!("unknown engine {other}"),
    };
    println!(
        "{engine}: {batches} batches x {puts} puts — put {:.1} ms, commit {:.1} ms",
        p.as_secs_f64() * 1e3,
        c.as_secs_f64() * 1e3
    );
}
