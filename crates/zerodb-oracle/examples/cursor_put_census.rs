//! The two write paths of issue #138, timed alone so a profile of this
//! binary shows them and not a fixture: the write-cursor rewrite loop of
//! `put/cursor/rewrite` (`range_mut` + `put_current`, same-size values) and
//! the `MDB_APPEND` load of `put/order/append`. Each round opens one write
//! txn, runs the loop and aborts, so the commit's file writes stay out too.
//!
//! ```text
//! cargo run --release -p zerodb-oracle --example cursor_put_census -- <lmdb|zerodb> <rewrite|append> [rounds]
//! ```

use std::hint::black_box;
use std::ops::Bound;
use std::time::Instant;

use zerodb_oracle::tempdir::TempDir;

/// Keys in the tree (`data::N` of the ladder).
const N: usize = 50_000;
/// Entries the rewrite loop visits (`data::N_PROBE`).
const N_PROBE: usize = 10_000;
/// Value size (`data::VAL`).
const VAL: usize = 256;

macro_rules! census {
    ($name:ident, $heed:ident, $setpage:tt) => {
        fn $name(op: &str, rounds: usize) {
            use $heed::types::Bytes;
            use $heed::{Database, EnvFlags, EnvOpenOptions};

            let dir = TempDir::new().expect("tempdir");
            let mut opts = EnvOpenOptions::new().read_txn_without_tls();
            opts.map_size(4 << 30);
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
            if op == "rewrite" {
                for k in &keys {
                    db.put(&mut w, k, &val).expect("put");
                }
            }
            w.commit().expect("commit");

            let new_val = [0xCDu8; VAL];
            let ops_per_round = if op == "rewrite" { N_PROBE } else { N };
            let t = Instant::now();
            let cpu = cpu_ns();
            for _ in 0..rounds {
                let mut w = env.write_txn().expect("write_txn");
                match op {
                    "rewrite" => {
                        let span = (
                            Bound::Included(&keys[0][..]),
                            Bound::Included(&keys[N_PROBE - 1][..]),
                        );
                        let mut it = db.range_mut(&mut w, &span).expect("range_mut");
                        let mut key = Vec::new();
                        while let Some(kv) = it.next() {
                            let (k, _) = kv.expect("entry");
                            key.clear();
                            key.extend_from_slice(k);
                            // SAFETY: the key was copied out and the entry
                            // borrow from `next` is dead before this call.
                            black_box(unsafe { it.put_current(&key, &new_val) }.expect("put"));
                        }
                    }
                    "append" => {
                        for k in &keys {
                            db.put_with_flags(&mut w, $heed::PutFlags::APPEND, k, &val)
                                .expect("append");
                        }
                    }
                    other => panic!("unknown op {other}"),
                }
                w.abort();
            }
            let ops = (rounds * ops_per_round) as f64;
            let cpu = (cpu_ns() - cpu) as f64 / ops;
            let ns = t.elapsed().as_nanos() as f64 / ops;
            println!(
                "{}: {op} {ns:.1} ns/op wall, {cpu:.1} ns/op cpu ({rounds} rounds)",
                stringify!($name)
            );
        }
    };
    (@setpage $opts:ident, set) => {
        let page = zerodb_oracle::os_page_size();
        $opts.page_size(page);
    };
    (@setpage $opts:ident, noset) => {};
}

/// User + system CPU time of this process, in nanoseconds: steadier than
/// wall time on a loaded machine.
fn cpu_ns() -> u64 {
    // SAFETY: `getrusage` writes only into the zeroed struct we pass.
    let ru = unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        ru
    };
    let ns = |t: libc::timeval| t.tv_sec as u64 * 1_000_000_000 + t.tv_usec as u64 * 1_000;
    ns(ru.ru_utime) + ns(ru.ru_stime)
}

census!(lmdb, heed, noset);
census!(zerodb, heed_zerodb, set);

fn main() {
    let mut args = std::env::args().skip(1);
    let engine = args.next().unwrap_or_else(|| "zerodb".into());
    let op = args.next().unwrap_or_else(|| "rewrite".into());
    let rounds: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(200);
    assert!(rounds > 0, "rounds must be positive");
    match engine.as_str() {
        "lmdb" => lmdb(&op, rounds),
        "zerodb" => zerodb(&op, rounds),
        other => panic!("unknown engine {other}"),
    }
}
