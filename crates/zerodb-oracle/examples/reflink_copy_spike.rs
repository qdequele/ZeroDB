//! Spike (issue #64, not for merge): raw env copy time, LMDB vs ZeroDB vs a
//! whole-file clone (`std::fs::copy`, which is `fclonefileat` on APFS).
//!
//! cargo run --release -p zerodb-oracle --example reflink_copy_spike -- [mib]

use std::os::unix::fs::FileExt;
use std::path::Path;
use std::time::Instant;
use zerodb_oracle::tempdir::TempDir;

const VAL: usize = 4000;

macro_rules! fill {
    ($heed:ident, $dir:expr, $n:expr, $flags:expr) => {{
        use $heed::types::Bytes;
        use $heed::{Database, EnvOpenOptions};
        let mut o = EnvOpenOptions::new().read_txn_without_tls();
        o.map_size(16 << 30);
        // SAFETY: a private temp dir, one process.
        unsafe { o.flags($flags) };
        // SAFETY: as above.
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
        (env, db)
    }};
}

fn secs(t: Instant) -> f64 {
    t.elapsed().as_secs_f64()
}

fn file_len(p: &Path) -> u64 {
    std::fs::metadata(p).unwrap().len()
}

fn main() {
    let mib: u64 = std::env::args()
        .nth(1)
        .map(|s| s.parse().unwrap())
        .unwrap_or(1024);
    let n = mib * (1 << 20) / 4096;
    let rounds = 3;

    // --- coherence: does a clone see bytes written but never fsynced? ---
    {
        let d = TempDir::new().unwrap();
        let src = d.path().join("src");
        let f = std::fs::File::create(&src).unwrap();
        f.set_len(64 << 20).unwrap();
        f.sync_all().unwrap();
        let pat: Vec<u8> = (0..(64usize << 20)).map(|i| (i * 31 % 251) as u8).collect();
        f.write_all_at(&pat, 0).unwrap(); // no fsync
        std::fs::copy(&src, d.path().join("dst")).unwrap();
        let got = std::fs::read(d.path().join("dst")).unwrap();
        println!("coherence pwrite-no-fsync clone matches: {}", got == pat);
    }
    // WRITE_MAP + NO_SYNC ZeroDB env: commit, clone the data file with no
    // sync, open the clone and count entries.
    {
        let d = TempDir::new().unwrap();
        let flags = heed_zerodb::EnvFlags::WRITE_MAP | heed_zerodb::EnvFlags::NO_SYNC;
        let (env, _db) = fill!(heed_zerodb, d.path(), 20_000u64, flags);
        let d2 = TempDir::new().unwrap();
        std::fs::copy(d.path().join("data.mdb"), d2.path().join("data.mdb")).unwrap();
        let mut o = heed_zerodb::EnvOpenOptions::new().read_txn_without_tls();
        o.map_size(16 << 30);
        // SAFETY: private temp dir.
        let e2 = unsafe { o.open(d2.path()) }.expect("open clone");
        let r = e2.read_txn().unwrap();
        let db: heed_zerodb::Database<heed_zerodb::types::Bytes, heed_zerodb::types::Bytes> =
            e2.open_database(&r, None).unwrap().unwrap();
        println!(
            "coherence WRITE_MAP|NO_SYNC clone entries: {} (want 20000)",
            db.len(&r).unwrap()
        );
        drop(env);
    }

    println!("building ~{mib} MiB envs ({n} x {VAL} B values)");
    let ld = TempDir::new().unwrap();
    let (lenv, _) = fill!(heed, ld.path(), n, heed::EnvFlags::empty());
    let zd = TempDir::new().unwrap();
    let (zenv, _) = fill!(heed_zerodb, zd.path(), n, heed_zerodb::EnvFlags::empty());
    lenv.force_sync().unwrap();
    zenv.force_sync().unwrap();
    println!(
        "data.mdb: lmdb {} MiB, zerodb {} MiB",
        file_len(&ld.path().join("data.mdb")) >> 20,
        file_len(&zd.path().join("data.mdb")) >> 20
    );

    for r in 0..rounds {
        let out = TempDir::new().unwrap();
        let t = Instant::now();
        drop(
            lenv.copy_to_path(out.path().join("l.mdb"), heed::CompactionOption::Disabled)
                .unwrap(),
        );
        let l = secs(t);
        std::fs::remove_file(out.path().join("l.mdb")).unwrap();

        let t = Instant::now();
        drop(
            zenv.copy_to_path(
                out.path().join("z.mdb"),
                heed_zerodb::CompactionOption::Disabled,
            )
            .unwrap(),
        );
        let z = secs(t);
        std::fs::remove_file(out.path().join("z.mdb")).unwrap();

        // Clone under a read-txn pin, then the two meta pages and a trim,
        // as the safe design would do.
        let t = Instant::now();
        let rtxn = zenv.read_txn().unwrap();
        let dst = out.path().join("c.mdb");
        std::fs::copy(zd.path().join("data.mdb"), &dst).unwrap();
        let f = std::fs::OpenOptions::new().write(true).open(&dst).unwrap();
        f.write_all_at(&[0u8; 8192], 0).unwrap();
        let len = file_len(&dst);
        f.set_len(len).unwrap();
        drop(rtxn);
        let c = secs(t);
        std::fs::remove_file(&dst).unwrap();

        // Open-file form (what Meilisearch's raw S3 snapshot uses).
        let mut tf = std::fs::File::create(out.path().join("lf.mdb")).unwrap();
        let t = Instant::now();
        lenv.copy_to_file(&mut tf, heed::CompactionOption::Disabled)
            .unwrap();
        let lf = secs(t);
        let mut zf = std::fs::File::create(out.path().join("zf.mdb")).unwrap();
        let t = Instant::now();
        zenv.copy_to_file(&mut zf, heed_zerodb::CompactionOption::Disabled)
            .unwrap();
        let zff = secs(t);

        println!(
            "round {r}: path-form  lmdb {l:.3}s  zerodb-main {z:.3}s  zerodb-clone {c:.4}s | open-file form  lmdb {lf:.3}s  zerodb-main {zff:.3}s"
        );
    }
}
