//! SPEC 05 GC-23 (amended 2026-09-29): `non_free_pages_size` is heed's
//! definition — the branch, leaf and overflow pages of the main DB and of
//! every named DB, times the page size — computed from the catalog records,
//! not from the file length or a free-list walk. Pinned here against the
//! per-database stats, with overflow values and churn, and under
//! `WRITE_MAP`, where the file is extended to the whole map and a
//! file-length formula would report almost the entire map as used. Do not
//! weaken (AGENTS.md rule 2).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{free_page_count, Env, EnvFlags, EnvOpenOptions};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-nonfree-def-{pid}-{seq}"));
        std::fs::create_dir_all(&path).expect("create temp dir");
        TempDir { path }
    }
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

const PS: u32 = 4096;
const MAP: usize = 256 << 20;
const NAMES: [&[u8]; 3] = [b"words", b"docs", b"empty"];

fn open(dir: &Path, write_map: bool) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    opts.max_dbs(8);
    if write_map {
        opts.flags(EnvFlags::WRITE_MAP);
    }
    opts.open(dir).expect("open env")
}

/// A main DB with user entries, two named DBs (one with overflow values) and
/// one empty named DB, across several commits with deletes and rewrites.
fn populate(env: &Env) {
    let main = env.main_database();
    let mut w = env.write_txn().unwrap();
    let words = env.create_database(&mut w, Some(NAMES[0])).unwrap();
    let docs = env.create_database(&mut w, Some(NAMES[1])).unwrap();
    env.create_database(&mut w, Some(NAMES[2])).unwrap();
    for i in 0..2000u32 {
        main.put(&mut w, format!("m{i:05}").as_bytes(), &[1u8; 90])
            .unwrap();
        words
            .put(&mut w, format!("w{i:05}").as_bytes(), &[2u8; 30])
            .unwrap();
        if i % 10 == 0 {
            docs.put(&mut w, &i.to_be_bytes(), &vec![3u8; 9000])
                .unwrap();
        }
    }
    w.commit().unwrap();
    for round in 0..3u32 {
        let mut w = env.write_txn().unwrap();
        for i in (round..2000).step_by(4) {
            main.delete(&mut w, format!("m{i:05}").as_bytes()).unwrap();
            if i % 10 == 0 {
                docs.put(
                    &mut w,
                    &i.to_be_bytes(),
                    &vec![4u8; 5000 + round as usize * 1000],
                )
                .unwrap();
            }
        }
        w.commit().unwrap();
    }
}

/// The expected value from the per-DB stats: main + every named DB.
fn expected(env: &Env) -> u64 {
    let rtxn = env.read_txn().unwrap();
    let pages = |b: u64, l: u64, o: u64| b + l + o;
    let m = env.main_database().stat(&rtxn).unwrap();
    let mut total = pages(m.branch_pages, m.leaf_pages, m.overflow_pages);
    for name in NAMES {
        let db = env.open_database(&rtxn, Some(name)).unwrap().unwrap();
        let s = db.stat(&rtxn).unwrap();
        total += pages(s.branch_pages, s.leaf_pages, s.overflow_pages);
    }
    total * u64::from(PS)
}

#[test]
fn equals_the_sum_of_database_pages() {
    let dir = TempDir::new();
    let env = open(dir.path(), false);
    populate(&env);
    let non_free = env.non_free_pages_size().unwrap();
    assert_eq!(non_free, expected(&env));
    assert!(non_free > 0);
    // With the file partition (INV-27): user trees + GC tree + free pages + the
    // two meta slots are exactly the file.
    let rtxn = env.read_txn().unwrap();
    let g = rtxn.snapshot().free_db;
    let gc_tree = g.branch_pages + g.leaf_pages + g.overflow_pages;
    let free = free_page_count(&rtxn).unwrap();
    drop(rtxn);
    assert_eq!(
        non_free + (free + gc_tree + 2) * u64::from(PS),
        env.real_disk_size().unwrap()
    );
}

#[test]
fn write_map_reports_database_pages_not_the_map() {
    let (a, b) = (TempDir::new(), TempDir::new());
    let plain = open(a.path(), false);
    let mapped = open(b.path(), true);
    populate(&plain);
    populate(&mapped);
    let v = mapped.non_free_pages_size().unwrap();
    // The WRITE_MAP file spans the whole map; the used size must not.
    assert_eq!(mapped.real_disk_size().unwrap(), MAP as u64);
    assert_eq!(v, expected(&mapped));
    assert!(
        v < (MAP as u64) / 16,
        "reported {v} bytes used of a {MAP}-byte map"
    );
    // The same writes give the same used size with or without WRITE_MAP.
    assert_eq!(v, plain.non_free_pages_size().unwrap());
}
