//! The shape of Meilisearch's indexing merge step (`merge_scan_and_send_docids`
//! in milli's `update/new/merger.rs`): while one thread holds the write
//! transaction, `threads` workers each open their own read transaction, read
//! the stored value of every key in their share in ascending order, build the
//! merged value, and send it through a bounded channel to the writer thread,
//! which puts it.
//!
//! The database is pre-filled with `keys` values whose sizes follow posting
//! lists: most are a few dozen bytes, one in 64 spans several pages and one in
//! 4096 runs to hundreds of KiB. One key in `stride` is merged.
//!
//! Prints the workers' wall time (the merge span), the time the workers spent
//! inside `get` alone, the time the writer spent inside `put`, and the commit.
//! `MODE=reads` drops the writer: workers discard their output, which isolates
//! the reads from channel back-pressure.
//!
//! ```text
//! cargo run --release -p zerodb-oracle --example merge_census -- <lmdb|zerodb|zerodb-nolock> [keys] [stride] [threads]
//! ```

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::sync_channel;
use std::time::{Duration, Instant};

use zerodb_oracle::tempdir::TempDir;

/// splitmix64: identical value sizes for both engines.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Size of key `i`'s stored value.
fn value_len(i: u64) -> usize {
    let h = mix(i);
    if h.is_multiple_of(4096) {
        64 * 1024 + (h >> 20) as usize % (512 * 1024)
    } else if h.is_multiple_of(64) {
        8 * 1024 + (h >> 20) as usize % (24 * 1024)
    } else {
        8 + (h >> 20) as usize % 120
    }
}

/// A word-like key: an 8-byte big-endian id spread over a short prefix, so
/// keys are not all the same length.
fn key(i: u64) -> Vec<u8> {
    let mut k = format!("w{:x}", mix(i) % 4096).into_bytes();
    k.extend_from_slice(&i.to_be_bytes());
    k
}

struct Times {
    merge: Duration,
    gets: Duration,
    puts: Duration,
    commit: Duration,
    read_bytes: u64,
}

macro_rules! census {
    ($name:ident, $heed:ident, $setpage:tt) => {
        fn $name(keys: u64, stride: u64, threads: usize, no_lock: bool, reads_only: bool) -> Times {
            use $heed::types::Bytes;
            use $heed::{Database, EnvFlags, EnvOpenOptions};

            let dir = TempDir::new().expect("tempdir");
            let mut opts = EnvOpenOptions::new().read_txn_without_tls();
            opts.map_size(64 << 30);
            opts.max_dbs(4);
            opts.max_readers(1024);
            census!(@setpage opts, $setpage);
            if no_lock {
                // SAFETY: single-process private temp dir.
                unsafe { opts.flags(EnvFlags::NO_LOCK) };
            }
            // SAFETY: single-process private temp dir.
            let env = unsafe { opts.open(dir.path()) }.expect("open env");

            let mut all: Vec<Vec<u8>> = (0..keys).map(key).collect();
            let mut w = env.write_txn().expect("write_txn");
            let db: Database<Bytes, Bytes> = env.create_database(&mut w, None).expect("db");
            let mut order: Vec<u64> = (0..keys).collect();
            order.sort_by(|a, b| all[*a as usize].cmp(&all[*b as usize]));
            let mut buf = Vec::new();
            for (n, &i) in order.iter().enumerate() {
                buf.clear();
                buf.resize(value_len(i), i as u8);
                db.put(&mut w, &all[i as usize], &buf).expect("put");
                if n % 200_000 == 199_999 {
                    w.commit().expect("commit");
                    w = env.write_txn().expect("write_txn");
                }
            }
            w.commit().expect("commit");
            all.sort();

            // One share per worker, each sorted, as milli's frozen caches are.
            let merged: Vec<&[u8]> = all
                .iter()
                .enumerate()
                .filter(|(n, _)| mix(*n as u64 ^ 0xA5A5).is_multiple_of(stride))
                .map(|(_, k)| k.as_slice())
                .collect();
            let shares: Vec<Vec<&[u8]>> = (0..threads)
                .map(|t| merged.iter().copied().skip(t).step_by(threads).collect())
                .collect();

            let gets_ns = AtomicU64::new(0);
            let read_bytes = AtomicU64::new(0);
            let mut wtxn = env.write_txn().expect("write_txn");
            let (tx, rx) = sync_channel::<(Vec<u8>, Vec<u8>)>(1024);
            let start = Instant::now();
            let mut merge = Duration::ZERO;
            let mut puts = Duration::ZERO;
            std::thread::scope(|s| {
                let workers: Vec<_> = shares
                    .iter()
                    .map(|share| {
                        let tx = tx.clone();
                        let env = &env;
                        let gets_ns = &gets_ns;
                        let read_bytes = &read_bytes;
                        s.spawn(move || {
                            let rtxn = env.read_txn().expect("read_txn");
                            let mut in_get = Duration::ZERO;
                            let mut bytes = 0u64;
                            for k in share {
                                let t = Instant::now();
                                let cur = db.get(&rtxn, k).expect("get");
                                in_get += t.elapsed();
                                let cur = cur.unwrap_or(&[]);
                                bytes += cur.len() as u64;
                                // Read every byte, as deserializing a bitmap does.
                                let sum = cur.iter().fold(0u8, |a, b| a.wrapping_add(*b));
                                let mut v = Vec::with_capacity(cur.len() + 8);
                                v.extend_from_slice(cur);
                                v.extend_from_slice(&[sum; 8]);
                                if reads_only {
                                    black_box(v);
                                } else {
                                    tx.send((k.to_vec(), v)).expect("send");
                                }
                            }
                            gets_ns.fetch_add(in_get.as_nanos() as u64, Ordering::Relaxed);
                            read_bytes.fetch_add(bytes, Ordering::Relaxed);
                        })
                    })
                    .collect();
                drop(tx);
                for (k, v) in rx.iter() {
                    let t = Instant::now();
                    db.put(&mut wtxn, &k, &v).expect("put");
                    puts += t.elapsed();
                }
                for w in workers {
                    w.join().expect("worker");
                }
                merge = start.elapsed();
            });
            let t = Instant::now();
            wtxn.commit().expect("commit");
            Times {
                merge,
                gets: Duration::from_nanos(gets_ns.load(Ordering::Relaxed)),
                puts,
                commit: t.elapsed(),
                read_bytes: read_bytes.load(Ordering::Relaxed),
            }
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
    let keys: u64 = args
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);
    let stride: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(4);
    let threads: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(4);
    assert!(
        keys > 0 && stride > 0 && threads > 0,
        "keys, stride and threads must be positive"
    );
    let reads_only = std::env::var("MODE").as_deref() == Ok("reads");
    let t = match engine.as_str() {
        "lmdb" => lmdb(keys, stride, threads, false, reads_only),
        "zerodb" => zerodb(keys, stride, threads, false, reads_only),
        "zerodb-nolock" => zerodb(keys, stride, threads, true, reads_only),
        other => panic!("unknown engine {other}"),
    };
    println!(
        "{engine:14} merge {:8.1} ms  gets {:8.1} ms (all workers)  puts {:8.1} ms  commit {:7.1} ms  read {} MiB",
        t.merge.as_secs_f64() * 1e3,
        t.gets.as_secs_f64() * 1e3,
        t.puts.as_secs_f64() * 1e3,
        t.commit.as_secs_f64() * 1e3,
        t.read_bytes >> 20,
    );
    // SAFETY: getrusage writes into the struct it is given.
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    // ru_maxrss is in bytes on macOS and in KiB on Linux.
    let rss = if cfg!(target_os = "macos") {
        ru.ru_maxrss as u64
    } else {
        ru.ru_maxrss as u64 * 1024
    };
    println!("{engine:14} peak RSS {} MiB", rss >> 20);
}
