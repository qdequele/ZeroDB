//! ADR-0014 — the opt-in trusted-file page-validation policy.
//!
//! On a valid file the trusting policy must be observably identical to the
//! validating default: every read, write, cursor, range and nested-read result
//! the same. These tests drive a validating env and a trusting env through the
//! same seeded random workload and compare them after every commit, then
//! reopen each file under the other policy. The last test pins that
//! `check::check_image`, and a validating open, still catch corruption in a
//! file a trusting env wrote.
//!
//! Hostile-image tests stay validating-only: under the trusting policy a
//! corrupt file is undefined behaviour by contract, so no test here ever
//! reads a corrupt file through a trusting env.

use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{check, Database, Env, EnvOpenOptions, FileTrust, RoTxn};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-trusted-{pid}-{seq}"));
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

fn trusting() -> FileTrust {
    // SAFETY: every env these tests open trusting is a fresh temp directory
    // written only by zerodb in this process, and no test corrupts a file
    // before reading it through a trusting env.
    unsafe { FileTrust::trust_contents() }
}

fn open(dir: &Path, policy: FileTrust) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    opts.max_dbs(4);
    opts.file_trust(policy);
    opts.open(dir).expect("open env")
}

/// xorshift64*: deterministic, dependency-free workload generator.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn key(i: u64) -> Vec<u8> {
    format!("k{i:07}").into_bytes()
}

/// Inline small, inline large, and overflow values, so the workload crosses
/// every value shape the page views read.
fn value(rng: &mut Rng, i: u64) -> Vec<u8> {
    let len = match rng.below(10) {
        0 => 5000 + rng.below(9000) as usize, // overflow run
        1..=3 => 200 + rng.below(800) as usize,
        _ => 1 + rng.below(40) as usize,
    };
    let mut v = format!("v{i}-").into_bytes();
    v.resize(len, b'a' + (i % 26) as u8);
    v
}

type Snapshot = Vec<(Vec<u8>, Vec<u8>)>;

/// Everything observable about one DB under `txn`: forward and reverse scans
/// (checked against each other), a bounded range, and point lookups.
fn observe(db: &Database, txn: &RoTxn<'_>, probes: &[Vec<u8>]) -> (Snapshot, Snapshot, Vec<bool>) {
    let fwd: Snapshot = db
        .iter(txn)
        .map(|r| {
            let (k, v) = r.expect("iter");
            (k.to_vec(), v.to_vec())
        })
        .collect();
    let mut rev: Snapshot = db
        .rev_iter(txn)
        .map(|r| {
            let (k, v) = r.expect("rev_iter");
            (k.to_vec(), v.to_vec())
        })
        .collect();
    rev.reverse();
    assert_eq!(fwd, rev, "forward and reverse scans disagree");
    let lo = key(1000);
    let hi = key(3000);
    let range: Snapshot = db
        .range(
            txn,
            Bound::Included(lo.as_slice()),
            Bound::Excluded(hi.as_slice()),
        )
        .map(|r| {
            let (k, v) = r.expect("range");
            (k.to_vec(), v.to_vec())
        })
        .collect();
    let hits = probes
        .iter()
        .map(|k| db.get(txn, k).expect("get").is_some())
        .collect();
    (fwd, range, hits)
}

fn assert_same(a: &Env, b: &Env, names: &[Option<&[u8]>]) {
    let (ta, tb) = (a.read_txn().unwrap(), b.read_txn().unwrap());
    let probes: Vec<Vec<u8>> = (0..4000).step_by(37).map(key).collect();
    for name in names {
        let da = a.open_database(&ta, *name).unwrap().expect("db in a");
        let db = b.open_database(&tb, *name).unwrap().expect("db in b");
        assert_eq!(
            observe(&da, &ta, &probes),
            observe(&db, &tb, &probes),
            "db {name:?} differs between the two policies"
        );
        assert_eq!(da.stat(&ta).unwrap(), db.stat(&tb).unwrap());
    }
}

enum Op {
    Put(usize, Vec<u8>, Vec<u8>),
    Del(usize, Vec<u8>),
    DelRange(usize, Vec<u8>, Vec<u8>),
}

const NAMES: [Option<&[u8]>; 3] = [None, Some(b"alpha"), Some(b"beta")];

/// One round of random ops across the three DBs.
fn gen_round(rng: &mut Rng) -> Vec<Op> {
    let n_ops = 150 + rng.below(250);
    (0..n_ops)
        .map(|_| {
            let db = rng.below(3) as usize;
            let i = rng.below(4000);
            match rng.below(20) {
                0 => Op::DelRange(db, key(i), key(i + rng.below(60))),
                1..=5 => Op::Del(db, key(i)),
                _ => Op::Put(db, key(i), value(rng, i)),
            }
        })
        .collect()
}

fn apply(env: &Env, ops: &[Op], abort: bool, round: usize) {
    let mut w = env.write_txn().unwrap();
    let dbs: Vec<Database> = NAMES
        .iter()
        .map(|n| env.open_database(&w, *n).unwrap().expect("db"))
        .collect();
    for op in ops {
        match op {
            Op::Put(d, k, v) => dbs[*d].put(&mut w, k, v).unwrap(),
            Op::Del(d, k) => {
                dbs[*d].delete(&mut w, k).unwrap();
            }
            Op::DelRange(d, lo, hi) => {
                dbs[*d]
                    .delete_range(
                        &mut w,
                        Bound::Included(lo.as_slice()),
                        Bound::Excluded(hi.as_slice()),
                    )
                    .unwrap();
            }
        }
    }
    // A nested read txn sees the writer's uncommitted state through the
    // parent's memo (ADR-0007), under either policy.
    let nested = w.nested_read_txn().unwrap();
    let seen = dbs[0].iter(&nested).count() as u64;
    drop(nested);
    assert_eq!(
        seen,
        dbs[0].len(&w).unwrap(),
        "nested read txn, round {round}"
    );
    if abort {
        w.abort();
    } else {
        w.commit().unwrap();
    }
}

/// One seeded workload applied to both envs in lockstep.
fn run_workload(seed: u64, a: &Env, b: &Env) {
    for env in [a, b] {
        let mut w = env.write_txn().unwrap();
        for name in NAMES.iter().skip(1) {
            env.create_database(&mut w, *name).unwrap();
        }
        w.commit().unwrap();
    }
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    for round in 0..12 {
        let abort = rng.below(6) == 0;
        let ops = gen_round(&mut rng);
        apply(a, &ops, abort, round);
        apply(b, &ops, abort, round);
        assert_same(a, b, &NAMES);
    }
}

#[test]
fn default_policy_validates() {
    let dir = TempDir::new();
    let mut opts = EnvOpenOptions::new();
    assert_eq!(opts.get_file_trust(), FileTrust::VALIDATE);
    opts.map_size(MAP);
    let env = opts.open(dir.path()).unwrap();
    assert!(!env.file_trust().is_trusted());
}

#[test]
fn trusting_policy_is_reported() {
    let dir = TempDir::new();
    let env = open(dir.path(), trusting());
    assert!(env.file_trust().is_trusted());
}

#[test]
fn trusting_and_validating_envs_agree_on_valid_files() {
    for seed in 0..6 {
        let (da, db) = (TempDir::new(), TempDir::new());
        {
            let a = open(da.path(), FileTrust::VALIDATE);
            let b = open(db.path(), trusting());
            run_workload(seed, &a, &b);
        }
        // Swap the policies over the same files: each file must read the same
        // under the policy that did not write it.
        let a = open(da.path(), trusting());
        let b = open(db.path(), FileTrust::VALIDATE);
        assert_same(&a, &b, &[None, Some(b"alpha"), Some(b"beta")]);
        // Both files are structurally sound.
        drop((a, b));
        for d in [&da, &db] {
            let bytes = std::fs::read(d.path().join(zerodb::DATA_FILE_NAME)).unwrap();
            let v = check::check_image(&bytes, PS);
            assert!(v.is_empty(), "seed {seed}: invariant violations: {v:#?}");
        }
    }
}

/// `check_image` ignores the policy (it always validates), and a validating
/// open of a file a trusting env wrote still turns a corrupt cell into an
/// error. The corrupt file is never read through a trusting env.
#[test]
fn corruption_in_a_trusted_env_file_is_still_caught() {
    let dir = TempDir::new();
    {
        let env = open(dir.path(), trusting());
        let db = env.main_database();
        let mut w = env.write_txn().unwrap();
        for i in 0..10 {
            db.put(&mut w, format!("trust-key-{i:03}").as_bytes(), b"value")
                .unwrap();
        }
        w.commit().unwrap();
    }
    let path = dir.path().join(zerodb::DATA_FILE_NAME);
    let mut bytes = std::fs::read(&path).unwrap();
    let ps = PS as usize;
    // The one leaf holding the keys (one commit, so no stale copy exists).
    let leaf = (2..bytes.len() / ps)
        .find(|&p| {
            bytes[p * ps..(p + 1) * ps]
                .windows(13)
                .any(|w| w == b"trust-key-000")
        })
        .expect("leaf page holding the keys");
    // Point the first node pointer (the u16 after the 32-byte header) past
    // the end of the page.
    let ptr = leaf * ps + 32;
    bytes[ptr..ptr + 2].copy_from_slice(&0xFFF0u16.to_le_bytes());
    std::fs::write(&path, &bytes).unwrap();

    let v = check::check_image(&bytes, PS);
    assert!(!v.is_empty(), "check_image missed a corrupt node pointer");

    let env = open(dir.path(), FileTrust::VALIDATE);
    let txn = env.read_txn().unwrap();
    assert!(
        env.main_database().get(&txn, b"trust-key-000").is_err(),
        "a validating open must reject the corrupt leaf"
    );
}
