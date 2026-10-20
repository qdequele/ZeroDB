//! Raw (non-compacting) environment copy of a large env, LMDB vs ZeroDB, in
//! both of heed's forms: `copy_to_path` (a path) and `copy_to_file` (an open
//! file handle). The criterion rung `maint/copy/raw` uses a small env; this
//! measures one of several GiB, where the copy method dominates.
//!
//! ```text
//! cargo run --release -p zerodb-oracle --example raw_copy_bench -- [MiB] [rounds] [dest-dir]
//! ```
//!
//! The two source envs live in the system temp dir, so `TMPDIR` picks the
//! source filesystem (for example `TMPDIR=/mnt/xfs` to measure the XFS
//! reflink route); the run prints where they are. Copies land in a
//! subdirectory the benchmark creates under `dest-dir` (default: the ZeroDB
//! source's temp dir) and removes at the end; pass a directory on another
//! filesystem to measure a cross-filesystem copy. Run it built at two
//! revisions for a before/after; LMDB's column is the drift control between
//! the two runs.

use std::path::{Path, PathBuf};
use std::time::Instant;
use zerodb_oracle::tempdir::TempDir;

const VAL: usize = 4000;

macro_rules! fill {
    ($heed:ident, $dir:expr, $n:expr) => {{
        use $heed::types::Bytes;
        use $heed::{Database, EnvOpenOptions};
        let mut o = EnvOpenOptions::new().read_txn_without_tls();
        o.map_size(64 << 30);
        // SAFETY: a private temp dir, opened once by this process.
        let env = unsafe { o.open($dir) }.expect("open env");
        let mut w = env.write_txn().unwrap();
        let db: Database<Bytes, Bytes> = env.create_database(&mut w, None).unwrap();
        w.commit().unwrap();
        let val = vec![0xA5u8; VAL];
        let mut i = 0u64;
        while i < $n {
            let mut w = env.write_txn().unwrap();
            for _ in 0..10_000 {
                if i >= $n {
                    break;
                }
                db.put(&mut w, &i.to_be_bytes(), &val).unwrap();
                i += 1;
            }
            w.commit().unwrap();
        }
        // Free some pages too, so the copy carries a non-empty free list.
        let mut w = env.write_txn().unwrap();
        for i in (0..$n).step_by(16) {
            db.delete(&mut w, &i.to_be_bytes()).unwrap();
        }
        w.commit().unwrap();
        env.force_sync().unwrap();
        env
    }};
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn time(f: impl FnOnce()) -> f64 {
    let t = Instant::now();
    f();
    t.elapsed().as_secs_f64()
}

fn fresh(dest: &Path, name: &str) -> PathBuf {
    let p = dest.join(name);
    let _ = std::fs::remove_file(&p);
    p
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mib: u64 = args.next().map_or(2048, |s| s.parse().unwrap());
    let rounds: usize = args.next().map_or(5, |s| s.parse().unwrap());
    let n = mib * (1 << 20) / 4096;

    let ld = TempDir::new().unwrap();
    let zd = TempDir::new().unwrap();
    let dest_root = args
        .next()
        .map_or_else(|| zd.path().to_path_buf(), PathBuf::from);
    // A directory of its own, so the fixed copy names below never touch a
    // file the caller keeps in `dest-dir`.
    let dest = dest_root.join(format!("raw_copy_bench-{}", std::process::id()));
    std::fs::create_dir(&dest).unwrap();
    println!(
        "sources: {} and {}; copies in {}",
        ld.path().display(),
        zd.path().display(),
        dest.display()
    );
    println!("building ~{mib} MiB envs ({n} x {VAL} B values)");
    let lenv = fill!(heed, ld.path(), n);
    let zenv = fill!(heed_zerodb, zd.path(), n);
    let len = |p: &Path| std::fs::metadata(p).unwrap().len() >> 20;
    println!(
        "data.mdb: lmdb {} MiB, zerodb {} MiB",
        len(&ld.path().join("data.mdb")),
        len(&zd.path().join("data.mdb"))
    );

    let (mut lp, mut zp, mut lf, mut zf) = (vec![], vec![], vec![], vec![]);
    for r in 0..rounds {
        // Alternate which engine goes first, so drift is not one-sided.
        let run_l = |lp: &mut Vec<f64>, lf: &mut Vec<f64>| {
            let p = fresh(&dest, "l.mdb");
            lp.push(time(|| {
                drop(
                    lenv.copy_to_path(&p, heed::CompactionOption::Disabled)
                        .unwrap(),
                )
            }));
            std::fs::remove_file(&p).unwrap();
            let p = fresh(&dest, "lf.mdb");
            let mut f = std::fs::File::create(&p).unwrap();
            lf.push(time(|| {
                lenv.copy_to_file(&mut f, heed::CompactionOption::Disabled)
                    .unwrap()
            }));
            std::fs::remove_file(&p).unwrap();
        };
        let run_z = |zp: &mut Vec<f64>, zf: &mut Vec<f64>| {
            let p = fresh(&dest, "z.mdb");
            zp.push(time(|| {
                drop(
                    zenv.copy_to_path(&p, heed_zerodb::CompactionOption::Disabled)
                        .unwrap(),
                )
            }));
            std::fs::remove_file(&p).unwrap();
            let p = fresh(&dest, "zf.mdb");
            let mut f = std::fs::File::create(&p).unwrap();
            zf.push(time(|| {
                zenv.copy_to_file(&mut f, heed_zerodb::CompactionOption::Disabled)
                    .unwrap()
            }));
            std::fs::remove_file(&p).unwrap();
        };
        if r % 2 == 0 {
            run_l(&mut lp, &mut lf);
            run_z(&mut zp, &mut zf);
        } else {
            run_z(&mut zp, &mut zf);
            run_l(&mut lp, &mut lf);
        }
        println!(
            "round {r}: path lmdb {:.3}s zerodb {:.4}s | handle lmdb {:.3}s zerodb {:.3}s",
            lp[r], zp[r], lf[r], zf[r]
        );
    }
    println!(
        "median of {rounds}: path lmdb {:.3}s zerodb {:.4}s | handle lmdb {:.3}s zerodb {:.3}s",
        median(lp),
        median(zp),
        median(lf),
        median(zf)
    );
    std::fs::remove_dir_all(&dest).unwrap();
}
