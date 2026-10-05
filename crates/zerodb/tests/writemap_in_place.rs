//! In-place `WRITE_MAP` (ADR-0021; SPEC 04 §6.4 TXN-45b): with the writable
//! map backing, a write txn's dirty pages are realized **in the map** at
//! their freshly-COW'd page numbers — no heap staging, no commit write-back.
//! This battery pins the observable contract against the default (heap-staged)
//! mode and the invariants the realization leans on:
//!
//! - a seeded mixed workload (inline puts, overflow runs, mid-page inserts
//!   that force the general-split scratch copy, deletes, cursor rewrites,
//!   `put_reserved`, nested reads) gives byte-identical results in a
//!   `WRITE_MAP` env and a default env, before and after reopen, and every
//!   committed image passes `check_image`;
//! - abort = don't advance the meta (TXN-45b): the in-place stores of an
//!   aborted txn are unreferenced garbage, the last commit survives intact
//!   (also across reopen), and the next txn reuses the scribbled space;
//! - nested read children see the writer's in-map dirty state;
//! - `put_reserved` fills the in-map frame (inline and overflow-run).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{check, Database, Env, EnvFlags, EnvOpenOptions};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-wmip-{pid}-{seq}"));
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

fn open(dir: &Path, flags: EnvFlags) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(256 << 20)
        .page_size(PS)
        .max_dbs(4)
        .flags(flags);
    opts.open(dir).expect("open env")
}

fn assert_clean(dir: &Path) {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = check::check_image(&bytes, PS);
    assert!(v.is_empty(), "invariant violations: {v:#?}");
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn contents(txn: &impl zerodb::TxnRead, db: Database) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.iter(txn)
        .map(|e| {
            let (k, v) = e.unwrap();
            (k.to_vec(), v.to_vec())
        })
        .collect()
}

/// One seeded round of mixed mutations: random-order keys (mid-page inserts →
/// the general split's scratch-copy path), overflow values (in-map runs),
/// `put_reserved`, deletes, cursor rewrites/deletes, nested reads and writer
/// reads checked against the model.
fn round(env: &Env, db: Database, model: &mut BTreeMap<Vec<u8>, Vec<u8>>, seed: u64, r: u64) {
    let mut rng = Rng(seed);
    let mut w = env.write_txn().unwrap();
    for step in 0..3_000u64 {
        let k = format!("key-{:06}", rng.below(8_000)).into_bytes();
        match rng.below(16) {
            0..=8 => {
                let len = if rng.below(12) == 0 {
                    4_500 + rng.below(12_000) as usize // overflow run
                } else {
                    30 + rng.below(400) as usize
                };
                let v: Vec<u8> = format!("v{r}.{step}-").bytes().cycle().take(len).collect();
                db.put(&mut w, &k, &v).unwrap();
                model.insert(k, v);
            }
            9..=10 => {
                let len = 20 + rng.below(5_000) as usize;
                let v: Vec<u8> = format!("r{r}.{step}-").bytes().cycle().take(len).collect();
                db.put_reserved(&mut w, &k, len, |buf| buf.copy_from_slice(&v))
                    .unwrap();
                model.insert(k, v);
            }
            11..=12 => {
                let was = db.delete(&mut w, &k).unwrap();
                assert_eq!(was, model.remove(&k).is_some(), "delete, step {step}");
            }
            13 => {
                let mut cur = db.rw_cursor(&mut w);
                let mut i = 0u32;
                let mut seen = cur.seek_ge(&k).unwrap().map(|(k, _)| k.to_vec());
                while let Some(kk) = seen {
                    if i >= 20 {
                        break;
                    }
                    if i.is_multiple_of(4) {
                        cur.del_current().unwrap();
                        model.remove(&kk);
                    } else if i.is_multiple_of(3) {
                        let v = format!("cur{r}.{i}").into_bytes();
                        cur.put_current(&v).unwrap();
                        model.insert(kk, v);
                    }
                    i += 1;
                    seen = cur.move_next().unwrap().map(|(k, _)| k.to_vec());
                }
            }
            14 => {
                // Nested read child: sees the writer's in-map dirty state.
                let child = w.nested_read_txn().unwrap();
                assert_eq!(
                    db.get(&child, &k).unwrap(),
                    model.get(&k).map(Vec::as_slice),
                    "nested read, step {step}"
                );
            }
            _ => {
                assert_eq!(
                    db.get(&w, &k).unwrap(),
                    model.get(&k).map(Vec::as_slice),
                    "writer read, step {step}"
                );
            }
        }
    }
    let want: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(contents(&w, db), want, "writer view before commit");
    w.commit().unwrap();
}

/// The same seeded workload in a `WRITE_MAP` (in-place) env and a default
/// (heap-staged) env gives identical contents, before and after reopen, and
/// clean images throughout.
#[test]
fn in_place_matches_default_mode() {
    let wm_dir = TempDir::new();
    let hp_dir = TempDir::new();
    let wm = open(wm_dir.path(), EnvFlags::NO_SYNC | EnvFlags::WRITE_MAP);
    let hp = open(hp_dir.path(), EnvFlags::NO_SYNC);
    let wdb = wm.main_database();
    let hdb = hp.main_database();
    let mut wmodel = BTreeMap::new();
    let mut hmodel = BTreeMap::new();
    for r in 0..3u64 {
        let seed = 0xADB0_0021 + r;
        round(&wm, wdb, &mut wmodel, seed, r);
        round(&hp, hdb, &mut hmodel, seed, r);
        assert_clean(wm_dir.path());
        assert_clean(hp_dir.path());
    }
    let (rw, rh) = (wm.read_txn().unwrap(), hp.read_txn().unwrap());
    assert_eq!(contents(&rw, wdb), contents(&rh, hdb));
    drop((rw, rh));
    // Reopen the writemap env (fresh map) and compare again.
    drop(wm);
    let wm = open(wm_dir.path(), EnvFlags::NO_SYNC | EnvFlags::WRITE_MAP);
    let rw = wm.read_txn().unwrap();
    let rh = hp.read_txn().unwrap();
    assert_eq!(
        contents(&rw, wm.main_database()),
        contents(&rh, hdb),
        "after reopen"
    );
}

/// TXN-45b abort semantics: the aborted txn's in-place stores (fresh pages
/// and GC-reclaimed pages alike) are unreferenced garbage; the last commit
/// survives byte-intact, also across reopen, and the next txn reuses the
/// scribbled space.
#[test]
fn abort_leaves_the_last_commit() {
    let dir = TempDir::new();
    let env = open(dir.path(), EnvFlags::WRITE_MAP);
    let db = env.main_database();

    // Commit a base, then free pages so the aborted txn below draws
    // GC-reclaimed pgnos (in-place scribbles on reclaimed space too).
    let mut w = env.write_txn().unwrap();
    for i in 0..3_000u32 {
        db.put(&mut w, format!("base{i:05}").as_bytes(), &[b'b'; 120])
            .unwrap();
    }
    w.commit().unwrap();
    let mut w = env.write_txn().unwrap();
    for i in 0..3_000u32 {
        if i % 2 == 0 {
            db.delete(&mut w, format!("base{i:05}").as_bytes()).unwrap();
        }
    }
    w.commit().unwrap();
    let before = contents(&env.read_txn().unwrap(), db);
    assert_clean(dir.path());

    // A txn that dirties plenty of pages in place (inline + overflow), then
    // aborts.
    let mut w = env.write_txn().unwrap();
    for i in 0..8_000u32 {
        let v = if i % 50 == 0 {
            vec![b'T'; 9_000] // overflow runs in the map
        } else {
            vec![b't'; 300]
        };
        db.put(&mut w, format!("temp{i:07}").as_bytes(), &v)
            .unwrap();
    }
    w.abort();
    assert_eq!(contents(&env.read_txn().unwrap(), db), before);
    assert_clean(dir.path());

    // Reopen: the meta never advanced, the garbage is unreferenced.
    drop(env);
    let env = open(dir.path(), EnvFlags::WRITE_MAP);
    let db = env.main_database();
    assert_eq!(
        contents(&env.read_txn().unwrap(), db),
        before,
        "after reopen"
    );
    // The next txn reuses the space the aborted txn scribbled on.
    let mut w = env.write_txn().unwrap();
    for i in 0..4_000u32 {
        db.put(&mut w, format!("next{i:05}").as_bytes(), &[b'n'; 300])
            .unwrap();
    }
    w.commit().unwrap();
    assert_clean(dir.path());
    assert_eq!(
        contents(&env.read_txn().unwrap(), db).len(),
        before.len() + 4_000
    );
}

/// `put_reserved` hands the closure a slice into the **in-map** frame (leaf
/// slot and overflow run); the committed bytes are exactly what the closure
/// wrote.
#[test]
fn put_reserved_fills_the_map_frame() {
    let dir = TempDir::new();
    let env = open(dir.path(), EnvFlags::WRITE_MAP);
    let db = env.main_database();
    let inline: Vec<u8> = (0..1_000u32).map(|i| (i % 251) as u8).collect();
    let big: Vec<u8> = (0..20_000u32).map(|i| (i % 249) as u8).collect();

    let mut w = env.write_txn().unwrap();
    db.put_reserved(&mut w, b"inline", inline.len(), |buf| {
        buf.copy_from_slice(&inline);
    })
    .unwrap();
    db.put_reserved(&mut w, b"big", big.len(), |buf| {
        buf.copy_from_slice(&big);
    })
    .unwrap();
    assert_eq!(db.get(&w, b"inline").unwrap(), Some(inline.as_slice()));
    assert_eq!(db.get(&w, b"big").unwrap(), Some(big.as_slice()));
    w.commit().unwrap();
    assert_clean(dir.path());

    let r = env.read_txn().unwrap();
    assert_eq!(db.get(&r, b"inline").unwrap(), Some(inline.as_slice()));
    assert_eq!(db.get(&r, b"big").unwrap(), Some(big.as_slice()));
}
