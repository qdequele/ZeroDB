//! ADR-0018 soundness pins: the env-wide cache of validated page versions
//! keys a page by (pgno, header txnid stamp), so no two byte images the env
//! ever exposes may share that pair (SPEC 04 TXN-38). These tests drive page
//! churn — replaces, deletes, overflow values, named DBs, aborted txns, a
//! reader holding an old snapshot so reuse is delayed — and check:
//!
//! - after every commit, every leaf/branch page in the file carries a stamp
//!   no newer than the committed txnid, and a (pgno, stamp) pair seen at any
//!   earlier point always has the same bytes;
//! - short read txns (each one hitting the cache for pages an earlier txn
//!   validated) return exactly the model's contents while pgnos are reused.
//!
//! Do not weaken (CLAUDE.md rule 2).

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{Env, EnvOpenOptions};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-stamp-{pid}-{seq}"));
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

fn open(dir: &Path) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(64 << 20);
    opts.page_size(PS);
    opts.max_dbs(4);
    opts.open(dir).expect("open env")
}

/// SPEC 02 §2 common header: pgno at 0, txnid stamp at 8, flags at 16.
fn u64_at(b: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(b[off..off + 8].try_into().unwrap())
}

const P_LEAF: u16 = 0x0001;
const P_BRANCH: u16 = 0x0002;

/// Records every tree page's (pgno, stamp) → bytes and fails if a pair ever
/// reappears with different bytes, or carries a stamp from the future.
#[derive(Default)]
struct VersionLedger {
    seen: HashMap<(u64, u64), Vec<u8>>,
}

impl VersionLedger {
    fn scan(&mut self, dir: &Path, committed: u64) {
        let file = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
        let ps = PS as usize;
        for (pgno, page) in file.chunks_exact(ps).enumerate().skip(2) {
            let flags = u16::from_le_bytes([page[16], page[17]]);
            if flags != P_LEAF && flags != P_BRANCH {
                continue;
            }
            // Overflow continuation pages hold raw value bytes; a tree page
            // names itself in its header.
            if u64_at(page, 0) != pgno as u64 {
                continue;
            }
            let stamp = u64_at(page, 8);
            assert!(
                stamp <= committed,
                "page {pgno} carries stamp {stamp} after commit {committed}"
            );
            match self.seen.get(&(pgno as u64, stamp)) {
                Some(prev) => assert!(
                    prev.as_slice() == page,
                    "page {pgno} changed bytes under the same stamp {stamp}"
                ),
                None => {
                    self.seen.insert((pgno as u64, stamp), page.to_vec());
                }
            }
        }
    }
}

/// Deterministic xorshift, so a failure replays.
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

fn value(rng: &mut Rng, round: u64) -> Vec<u8> {
    // Mostly inline values, sometimes an overflow run, so overflow pages are
    // freed and later reused as tree pages.
    let len = if rng.below(10) == 0 {
        5_000 + rng.below(6_000) as usize
    } else {
        20 + rng.below(300) as usize
    };
    let tag = format!("r{round}-");
    tag.bytes().cycle().take(len).collect()
}

#[test]
fn a_page_version_names_one_byte_image() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let main = env.main_database();
    let named = {
        let mut w = env.write_txn().unwrap();
        let a = env.create_database(&mut w, Some(b"alpha")).unwrap();
        let b = env.create_database(&mut w, Some(b"beta")).unwrap();
        w.commit().unwrap();
        [a, b]
    };
    let mut ledger = VersionLedger::default();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut held = None;
    for round in 0..150u64 {
        let mut w = env.write_txn().unwrap();
        for _ in 0..(20 + rng.below(200)) {
            let db = match rng.below(3) {
                0 => main,
                n => named[n as usize - 1],
            };
            let k = format!("key{:05}", rng.below(2_000)).into_bytes();
            if rng.below(3) == 0 {
                db.delete(&mut w, &k).unwrap();
            } else {
                let v = value(&mut rng, round);
                db.put(&mut w, &k, &v).unwrap();
            }
        }
        if rng.below(7) == 0 {
            // Aborted work must not leave a second image under any stamp.
            w.abort();
        } else {
            w.commit().unwrap();
        }
        // Hold a reader across some commits so reuse is delayed, then let
        // it go so freed pages come back.
        match rng.below(5) {
            0 => held = Some(env.clone().static_read_txn().unwrap()),
            1 => held = None,
            _ => {}
        }
        let committed = env.read_txn().unwrap().txnid();
        ledger.scan(dir.path(), committed);
    }
    drop(held);
    assert!(ledger.seen.len() > 200, "the workload reused too few pages");
}

#[test]
fn short_read_txns_see_exact_contents_while_pages_are_reused() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    for round in 0..120u64 {
        let mut w = env.write_txn().unwrap();
        for _ in 0..(10 + rng.below(150)) {
            let k = format!("key{:05}", rng.below(1_500)).into_bytes();
            if rng.below(3) == 0 {
                db.delete(&mut w, &k).unwrap();
                model.remove(&k);
            } else {
                let v = value(&mut rng, round);
                db.put(&mut w, &k, &v).unwrap();
                model.insert(k, v);
            }
        }
        w.commit().unwrap();
        // Many one-op txns: after the first, each resolves the hot pages
        // through the env-wide cache instead of its own memo.
        for _ in 0..50 {
            let k = format!("key{:05}", rng.below(1_500)).into_bytes();
            let r = env.read_txn().unwrap();
            assert_eq!(
                db.get(&r, &k).unwrap(),
                model.get(&k).map(Vec::as_slice),
                "round {round}, key {k:?}"
            );
        }
        // And one full scan per round.
        let r = env.read_txn().unwrap();
        let got: Vec<(Vec<u8>, Vec<u8>)> = db
            .iter(&r)
            .map(|e| {
                let (k, v) = e.unwrap();
                (k.to_vec(), v.to_vec())
            })
            .collect();
        let want: Vec<(Vec<u8>, Vec<u8>)> =
            model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(got, want, "round {round}");
    }
}
