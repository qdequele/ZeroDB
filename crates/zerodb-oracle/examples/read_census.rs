//! The read rungs of the ladder timed alone, with no fixture in the timed
//! region, in thread CPU time: `scan/edge/first_last`, `get/access/hot`,
//! `get/access/miss`, `get/access/rand` and `scan/full/fwd` over the ladder's
//! tree (50 000 ascending 8-byte keys, 256-byte values, one named database).
//! Each batch opens one read txn, as the ladder's rungs do. The tool prints
//! the median and the minimum over the batches, per operation, so a loaded
//! machine still gives a usable figure.
//!
//! ```text
//! cargo run --release -p zerodb-oracle --example read_census -- <lmdb|zerodb> <op> [batches]
//! ```
//!
//! `op` is one of `first_last`, `hot`, `miss`, `rand`, `fwd`.

use std::hint::black_box;

use zerodb_oracle::tempdir::TempDir;

/// Keys in the tree (`data::N` of the ladder).
const N: usize = 50_000;
/// Operations per batch (`data::N_PROBE`).
const N_PROBE: usize = 10_000;
/// Value size (`data::VAL`).
const VAL: usize = 256;

macro_rules! census {
    ($name:ident, $heed:ident, $setpage:tt) => {
        fn $name(op: &str, batches: usize) -> Vec<f64> {
            use $heed::types::Bytes;
            use $heed::{Database, EnvFlags, EnvOpenOptions};

            let dir = TempDir::new().expect("tempdir");
            let mut opts = EnvOpenOptions::new().read_txn_without_tls();
            opts.map_size(1 << 30);
            opts.max_dbs(4);
            census!(@setpage opts, $setpage);
            // SAFETY: single-process private temp dir; NO_SYNC is the only flag.
            let env = unsafe {
                opts.flags(EnvFlags::NO_SYNC);
                opts.open(dir.path())
            }
            .expect("open env");
            let keys: Vec<[u8; 8]> = (0..N as u64).map(u64::to_be_bytes).collect();
            let val = [0xABu8; VAL];
            let mut w = env.write_txn().expect("write_txn");
            let db: Database<Bytes, Bytes> = env
                .create_database(&mut w, Some("census"))
                .expect("db");
            for k in &keys {
                db.put(&mut w, k, &val).expect("put");
            }
            w.commit().expect("commit");

            // Probes: present keys in a fixed pseudo-random order, and absent
            // keys (odd values past the tree's range interleaved inside it).
            let mut x = 0x9E37_79B9_7F4A_7C15u64;
            let rand: Vec<[u8; 8]> = (0..N_PROBE)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    keys[(x % N as u64) as usize]
                })
                .collect();
            let miss: Vec<[u8; 9]> = (0..N_PROBE)
                .map(|i| {
                    let mut k = [0xFFu8; 9];
                    k[..8].copy_from_slice(&keys[i * (N / N_PROBE)]);
                    k
                })
                .collect();
            let hot = keys[N / 2];
            let ops = if op == "fwd" { N } else { N_PROBE };

            let mut per_op = Vec::with_capacity(batches);
            for _ in 0..batches {
                let t = thread_cpu_ns();
                let r = env.read_txn().expect("read_txn");
                let mut acc = 0usize;
                match op {
                    "first_last" => {
                        for _ in 0..N_PROBE {
                            acc += db.first(&r).expect("first").map_or(0, |(k, _)| k.len());
                            acc += db.last(&r).expect("last").map_or(0, |(k, _)| k.len());
                        }
                    }
                    "hot" => {
                        for _ in 0..N_PROBE {
                            acc += db.get(&r, &hot[..]).expect("get").map_or(0, <[u8]>::len);
                        }
                    }
                    "rand" => {
                        for k in &rand {
                            acc += db.get(&r, &k[..]).expect("get").map_or(0, <[u8]>::len);
                        }
                    }
                    "miss" => {
                        for k in &miss {
                            acc += db.get(&r, &k[..]).expect("get").map_or(0, <[u8]>::len);
                        }
                    }
                    "fwd" => {
                        for kv in db.iter(&r).expect("iter") {
                            acc += kv.expect("entry").1.len();
                        }
                    }
                    other => panic!("unknown op {other}"),
                }
                black_box(acc);
                drop(r);
                per_op.push((thread_cpu_ns() - t) as f64 / ops as f64);
            }
            per_op
        }
    };
    (@setpage $opts:ident, set) => {
        let page = zerodb_oracle::os_page_size();
        $opts.page_size(page);
    };
    (@setpage $opts:ident, noset) => {};
}

/// CPU time of the calling thread, in nanoseconds: unlike wall time, it does
/// not count the time other processes hold the core.
fn thread_cpu_ns() -> u64 {
    // SAFETY: `clock_gettime` writes only into the zeroed struct we pass.
    let ts = unsafe {
        let mut ts: libc::timespec = std::mem::zeroed();
        libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts);
        ts
    };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

census!(lmdb, heed, noset);
census!(zerodb, heed_zerodb, set);

/// On macOS, ask for the performance cores: a thread the scheduler moves to
/// an efficiency core runs several times slower and blurs the figure.
fn prefer_fast_cores() {
    #[cfg(target_os = "macos")]
    // SAFETY: sets the calling thread's QoS class; takes no pointers.
    unsafe {
        libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_USER_INTERACTIVE, 0);
    }
}

fn main() {
    prefer_fast_cores();
    let mut args = std::env::args().skip(1);
    let engine = args.next().unwrap_or_else(|| "zerodb".into());
    let op = args.next().unwrap_or_else(|| "first_last".into());
    let batches: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(200);
    assert!(batches > 0, "batches must be positive");
    let mut per_op = match engine.as_str() {
        "lmdb" => lmdb(&op, batches),
        "zerodb" => zerodb(&op, batches),
        other => panic!("unknown engine {other}"),
    };
    per_op.sort_by(f64::total_cmp);
    println!(
        "{engine} {op}: p50 {:.2} ns/op, min {:.2} ns/op, p10 {:.2} ns/op cpu ({batches} batches)",
        per_op[per_op.len() / 2],
        per_op[0],
        per_op[per_op.len() / 10]
    );
}
