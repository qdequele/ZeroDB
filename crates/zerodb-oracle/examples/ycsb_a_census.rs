//! Profiling workload for the public suite's `a_nosync` gap: the same shape as
//! rust-storage-bench's YCSB A (10M ascending 16-byte big-endian `u128` keys
//! loaded in one write txn, 100-byte values, `NO_SYNC | NO_READ_AHEAD`, one
//! unnamed database, default page size), then a timed run phase with one txn
//! per operation, as that harness's heed backend does.
//!
//! ```text
//! cargo run --release -p zerodb-oracle --example ycsb_a_census -- \
//!     <lmdb|zerodb> <dir> <mix|write|read|empty> [items] [secs]
//! ```
//!
//! `mix` is 50 % reads / 50 % updates of uniformly random keys; `write` and
//! `read` keep one side only; `empty` commits empty write txns. When the load
//! is done the process prints `READY <pid>` (attach `perf record -p` then) and
//! runs for `secs` seconds. Each op is timed with two `Instant` reads; the
//! summary gives per-op mean, p50 and p99 for reads and writes, and for writes
//! the begin / put / commit split.
//!
//! `CENSUS_PAGE_SIZE=<bytes>` sets ZeroDB's page size (LMDB always uses the
//! OS page size), so a 16 KiB-page host can reproduce a 4 KiB-page file. The
//! data files are read once through the page cache after the load, so the
//! timed phase measures the engine rather than first-touch disk reads.

use std::hint::black_box;
use std::io::Write as _;
use std::time::{Duration, Instant};

/// xorshift64*: deterministic, no dependency, cheap next to a txn.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

fn pct(v: &mut [u32], p: f64) -> u32 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    v[((v.len() - 1) as f64 * p) as usize]
}

fn report(name: &str, what: &str, lat: &mut [u32], wall: Duration) {
    if lat.is_empty() {
        return;
    }
    let n = lat.len();
    let mean = lat.iter().map(|&x| x as u64).sum::<u64>() as f64 / n as f64;
    println!(
        "{name} {what}: n={n} ({:.1} kops/s of wall) mean={mean:.0} p50={} p99={} ns",
        n as f64 / wall.as_secs_f64() / 1e3,
        pct(lat, 0.50),
        pct(lat, 0.99),
    );
}

macro_rules! census {
    ($name:ident, $heed:ident, |$o:ident| $extra:block) => {
        fn $name(dir: &std::path::Path, mode: &str, items: u64, secs: u64) {
            use $heed::types::Bytes;
            use $heed::{Database, EnvFlags, EnvOpenOptions};

            std::fs::create_dir_all(dir).expect("create dir");
            // The harness's 10^12-byte map, rounded down to a 64 KiB multiple
            // as the public-suite report describes.
            let map_size = 1_000_000_000_000usize & !0xFFFF;
            // SAFETY: a private directory used by this process only; the
            // flags are the harness's no-sync set.
            let env = unsafe {
                let mut opts = EnvOpenOptions::new();
                opts.map_size(map_size);
                opts.flags(EnvFlags::NO_SYNC | EnvFlags::NO_READ_AHEAD);
                let $o = &mut opts;
                $extra
                opts.open(dir)
            }
            .expect("open env");
            let mut w = env.write_txn().expect("write_txn");
            let db: Database<Bytes, Bytes> =
                env.create_database(&mut w, None).expect("create_database");
            w.commit().expect("commit");

            let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
            let mut val = [0u8; 100];
            for b in val.iter_mut() {
                *b = rng.next() as u8;
            }

            let t = Instant::now();
            let mut w = env.write_txn().expect("write_txn");
            for i in 0..items as u128 {
                val[0] = i as u8;
                db.put(&mut w, &i.to_be_bytes(), &val).expect("load put");
            }
            w.commit().expect("load commit");
            println!(
                "{} load {items} items: {:.1} s",
                stringify!($name),
                t.elapsed().as_secs_f64()
            );
            warm_page_cache(dir);
            println!("READY {}", std::process::id());
            std::io::stdout().flush().ok();
            // With `CENSUS_GO=<path>`, hold the run until that file exists, so
            // a script can change the page cache (sync, drop) between phases.
            if let Some(go) = std::env::var_os("CENSUS_GO") {
                while !std::path::Path::new(&go).exists() {
                    std::thread::sleep(Duration::from_millis(100));
                }
            }

            let (mut reads, mut writes) = (Vec::new(), Vec::new());
            let (mut t_begin, mut t_put, mut t_commit) =
                (Duration::ZERO, Duration::ZERO, Duration::ZERO);
            let deadline = Instant::now() + Duration::from_secs(secs);
            let start = Instant::now();
            let mut ops = 0u64;
            loop {
                if ops & 1023 == 0 && Instant::now() >= deadline {
                    break;
                }
                ops += 1;
                let r = rng.next();
                let key = ((r >> 1) % items) as u128;
                let read = match mode {
                    "mix" => r & 1 == 0,
                    "read" => true,
                    _ => false,
                };
                if read {
                    let a = Instant::now();
                    let txn = env.read_txn().expect("read_txn");
                    let v = db
                        .get(&txn, &key.to_be_bytes())
                        .expect("get")
                        .map(<[u8]>::to_vec);
                    drop(txn);
                    black_box(v);
                    reads.push(a.elapsed().as_nanos() as u32);
                } else if mode == "empty" {
                    let a = Instant::now();
                    let w = env.write_txn().expect("write_txn");
                    w.commit().expect("commit");
                    writes.push(a.elapsed().as_nanos() as u32);
                } else {
                    val[0] = r as u8;
                    let a = Instant::now();
                    let mut w = env.write_txn().expect("write_txn");
                    let b = Instant::now();
                    db.put(&mut w, &key.to_be_bytes(), &val).expect("put");
                    let c = Instant::now();
                    w.commit().expect("commit");
                    let d = Instant::now();
                    t_begin += b - a;
                    t_put += c - b;
                    t_commit += d - c;
                    writes.push((d - a).as_nanos() as u32);
                }
            }
            let wall = start.elapsed();
            let name = stringify!($name);
            report(name, "read", &mut reads, wall);
            report(name, "write", &mut writes, wall);
            if mode == "mix" || mode == "write" {
                let n = writes.len().max(1) as f64;
                let per = |d: Duration| d.as_nanos() as f64 / n;
                println!(
                    "{name} write split: begin={:.0} put={:.0} commit={:.0} ns",
                    per(t_begin),
                    per(t_put),
                    per(t_commit)
                );
            }
            println!(
                "{name} total {:.1} kops/s",
                ops as f64 / wall.as_secs_f64() / 1e3
            );
        }
    };
}

/// Read every file in `dir` once, so its pages sit in the page cache.
fn warm_page_cache(dir: &std::path::Path) {
    use std::io::Read as _;
    let mut buf = vec![0u8; 1 << 20];
    for entry in std::fs::read_dir(dir).expect("read dir") {
        let mut f = std::fs::File::open(entry.expect("dir entry").path()).expect("open");
        while f.read(&mut buf).expect("read") > 0 {}
    }
}

census!(lmdb, heed, |_opts| {});
census!(zerodb, heed_zerodb, |opts| {
    if let Some(ps) = std::env::var("CENSUS_PAGE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        opts.page_size(ps);
    }
});

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let engine = args.get(1).map(String::as_str).unwrap_or("zerodb");
    let dir = std::path::PathBuf::from(args.get(2).expect("data dir"));
    let mode = args.get(3).map(String::as_str).unwrap_or("mix");
    let items: u64 = args
        .get(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000_000);
    let secs: u64 = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(60);
    assert!(
        matches!(mode, "mix" | "write" | "read" | "empty"),
        "mode must be mix, write, read or empty"
    );
    match engine {
        "lmdb" => lmdb(&dir, mode, items, secs),
        "zerodb" => zerodb(&dir, mode, items, secs),
        other => panic!("engine must be lmdb or zerodb, not {other}"),
    }
}
