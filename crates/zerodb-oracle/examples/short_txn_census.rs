//! One-operation transactions, the shape of rust-storage-bench's YCSB runs
//! (Phase D): every point read opens its own read txn, every write its own
//! write txn + commit. Times `read_txn + get + drop` and `write_txn + put +
//! commit` (NO_SYNC) per op, for both engines, over a pre-filled DB.
//!
//! ```text
//! cargo run --release -p zerodb-oracle --example short_txn_census -- <lmdb|zerodb> [items] [ops]
//! ```

use std::hint::black_box;
use std::time::Instant;

use zerodb_oracle::tempdir::TempDir;

/// xorshift64*: identical key stream for both engines.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

macro_rules! census {
    ($name:ident, $heed:ident, $setpage:tt) => {
        fn $name(items: u64, ops: usize) {
            use $heed::types::Bytes;
            use $heed::{Database, EnvFlags, EnvOpenOptions};

            let dir = TempDir::new().expect("tempdir");
            let mut opts = EnvOpenOptions::new().read_txn_without_tls();
            opts.map_size(16 << 30);
            opts.max_dbs(4);
            census!(@setpage opts, $setpage);
            // SAFETY: single-process private temp dir; NO_SYNC is the only flag.
            let env = unsafe {
                opts.flags(EnvFlags::NO_SYNC);
                opts.open(dir.path())
            }
            .expect("open env");
            let mut w = env.write_txn().expect("write_txn");
            let db: Database<Bytes, Bytes> = env.create_database(&mut w, None).expect("db");
            let val = [0x5Au8; 128];
            for i in 0..items {
                db.put(&mut w, &i.to_be_bytes(), &val).expect("put");
            }
            w.commit().expect("commit");

            let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
            let t = Instant::now();
            for _ in 0..ops {
                let k = (rng.next() % items).to_be_bytes();
                let r = env.read_txn().expect("read_txn");
                black_box(db.get(&r, &k).expect("get"));
            }
            let read_ns = t.elapsed().as_nanos() as f64 / ops as f64;
            if std::env::var("CENSUS_READS_ONLY").is_ok() {
                println!("{}: read txn+get {read_ns:.0} ns/op", stringify!($name));
                return;
            }

            let t = Instant::now();
            for _ in 0..ops {
                let k = (rng.next() % items).to_be_bytes();
                let mut w = env.write_txn().expect("write_txn");
                db.put(&mut w, &k, &val).expect("put");
                w.commit().expect("commit");
            }
            let write_ns = t.elapsed().as_nanos() as f64 / ops as f64;
            println!(
                "{}: {items} items, {ops} ops — read txn+get {read_ns:.0} ns/op, write txn+put+commit {write_ns:.0} ns/op",
                stringify!($name)
            );
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
    let mut args = std::env::args().skip(1);
    let engine = args.next().unwrap_or_else(|| "zerodb".into());
    let items: u64 = args
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let ops: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(200_000);
    match engine.as_str() {
        "lmdb" => lmdb(items, ops),
        "zerodb" => zerodb(items, ops),
        other => panic!("unknown engine {other}"),
    }
}
