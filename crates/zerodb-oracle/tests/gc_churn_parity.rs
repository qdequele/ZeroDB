//! M1.5 acceptance: file-size parity with the LMDB fork under insert/delete
//! churn (PLAN §1.5; ADR-0005 D5, bands approved 2026-07-16).
//!
//! Two identical seeded workloads drive the fork (via heed, psize fixed 4096)
//! and zerodb (psize 4096) side by side; `real_disk_size` is recorded after
//! every commit, giving two growth curves. Assertions:
//!
//! 1. **Boundedness** (the real acceptance): zerodb's curve reaches a steady
//!    state — `size(last) <= 1.05 x size(half)`.
//! 2. **Tolerance band**: final steady-state zerodb size within
//!    `[0.5x, 1.5x]` of LMDB's for the general (overflow-bearing) workload,
//!    and within `+/-25%` for the no-overflow variant. The general band is
//!    asymmetric-generous because of (i) 32-vs-16-byte page/overflow headers,
//!    (ii) different split-fill policies, and (iii) — dominant — GC-21's
//!    within-PIL-only run search vs the fork's cross-entry `me_pghead`
//!    merging, which lets LMDB reuse fragmented space for overflow runs where
//!    zerodb extends (spec-sanctioned; Phase 3.1 fixes it). The no-overflow
//!    variant removes (iii), hence the tighter band.
//!
//! `Env::real_disk_size` parity is also asserted structurally: zerodb's value
//! must equal the data file's `fstat` length (SPEC 00 row 18 / GC-22).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use heed::types::Bytes;
use heed::{EnvOpenOptions as HeedOpts, WithoutTls};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(tag: &str) -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-churn-{tag}-{pid}-{seq}"));
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
const MAP: usize = 64 << 20;
const CYCLES: usize = 200;
const KEYS: u32 = 1000;

/// Deterministic tiny PRNG (xorshift64*), same stream on both engines.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

/// One churn workload cycle description, engine-agnostic: the (key, value-len)
/// puts and the keys to delete, derived from the shared seed stream.
struct Cycle {
    puts: Vec<(Vec<u8>, usize)>,
    dels: Vec<Vec<u8>>,
}

fn make_cycles(seed: u64, overflow: bool) -> Vec<Cycle> {
    let mut rng = Rng(seed);
    let mut cycles = Vec::with_capacity(CYCLES);
    for _ in 0..CYCLES {
        let mut puts = Vec::with_capacity(KEYS as usize);
        for i in 0..KEYS {
            let key = format!("key{i:06}").into_bytes();
            // 50..2000 B general (~1/8 overflow-sized at psize 4096 where the
            // inline max is 2030 - 8 - klen); 50..800 B for the no-overflow
            // variant (always inline).
            let len = if overflow {
                50 + (rng.next() % 1950) as usize
            } else {
                50 + (rng.next() % 750) as usize
            };
            puts.push((key, len));
        }
        let mut dels = Vec::with_capacity(KEYS as usize / 2);
        for i in 0..KEYS {
            if rng.next() % 2 == 0 {
                dels.push(format!("key{i:06}").into_bytes());
            }
        }
        cycles.push(Cycle { puts, dels });
    }
    cycles
}

fn value_bytes(len: usize, cycle: usize) -> Vec<u8> {
    vec![(cycle % 251) as u8; len]
}

/// Drive the workload through the fork; return per-commit real_disk_size.
fn run_lmdb(dir: &Path, cycles: &[Cycle]) -> Vec<u64> {
    let mut opts = HeedOpts::new().read_txn_without_tls();
    opts.map_size(MAP);
    // SAFETY: no cross-process env flags; private temp dir, single-threaded.
    let env: heed::Env<WithoutTls> = unsafe { opts.open(dir) }.expect("open lmdb env");
    let db: heed::Database<Bytes, Bytes> = {
        let mut wtxn = env.write_txn().expect("wtxn");
        let db = env
            .create_database(&mut wtxn, None)
            .expect("create unnamed db");
        wtxn.commit().expect("commit");
        db
    };
    let mut sizes = Vec::with_capacity(cycles.len());
    for (c, cycle) in cycles.iter().enumerate() {
        let mut wtxn = env.write_txn().expect("wtxn");
        for (k, len) in &cycle.puts {
            db.put(&mut wtxn, k, &value_bytes(*len, c)).expect("put");
        }
        for k in &cycle.dels {
            db.delete(&mut wtxn, k).expect("del");
        }
        wtxn.commit().expect("commit");
        sizes.push(env.real_disk_size().expect("size"));
    }
    sizes
}

/// Drive the identical workload through zerodb; return per-commit sizes.
fn run_zerodb(dir: &Path, cycles: &[Cycle]) -> Vec<u64> {
    let mut opts = zerodb::EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    let env = opts.open(dir).expect("open zerodb env");
    let db = env.main_database();
    let mut sizes = Vec::with_capacity(cycles.len());
    for (c, cycle) in cycles.iter().enumerate() {
        let mut wtxn = env.write_txn().expect("wtxn");
        for (k, len) in &cycle.puts {
            db.put(&mut wtxn, k, &value_bytes(*len, c)).expect("put");
        }
        for k in &cycle.dels {
            db.delete(&mut wtxn, k).expect("del");
        }
        wtxn.commit().expect("commit");
        let size = env.real_disk_size().expect("size");
        // GC-22 / SPEC 00 row 18: real_disk_size is the fstat length.
        assert_eq!(
            size,
            std::fs::metadata(dir.join(zerodb::DATA_FILE_NAME))
                .unwrap()
                .len(),
            "real_disk_size must be the data file's fstat length"
        );
        sizes.push(size);
    }
    // The committed image stays structurally leak-free (INV-22).
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = zerodb::check::check_image(&bytes, PS);
    assert!(v.is_empty(), "invariant violations after churn: {v:#?}");
    sizes
}

fn assert_curves(tag: &str, lmdb: &[u64], zdb: &[u64], lo: f64, hi: f64) {
    // 1. Boundedness (flatness), approved 1.05x half-to-full.
    let half = zdb[CYCLES / 2] as f64;
    let last = zdb[CYCLES - 1] as f64;
    assert!(
        last <= half * 1.05,
        "[{tag}] zerodb growth not bounded: half {half}, final {last}"
    );
    // 2. Tolerance band on the steady-state (final) sizes.
    let l = lmdb[CYCLES - 1] as f64;
    let ratio = last / l;
    println!(
        "[{tag}] steady-state: lmdb={l} zerodb={last} ratio={ratio:.3} \
         (band [{lo}, {hi}]); lmdb half={} zerodb half={half}",
        lmdb[CYCLES / 2]
    );
    assert!(
        ratio >= lo && ratio <= hi,
        "[{tag}] steady-state ratio {ratio:.3} outside the approved band [{lo}, {hi}] \
         (lmdb {l}, zerodb {last})"
    );
}

/// General churn (values 50..2000 B, ~1/8 overflow-sized): band [0.5, 1.5].
#[test]
fn churn_parity_general() {
    let cycles = make_cycles(0x5EED_0001, true);
    let ldir = TempDir::new("l-gen");
    let zdir = TempDir::new("z-gen");
    let lmdb = run_lmdb(ldir.path(), &cycles);
    let zdb = run_zerodb(zdir.path(), &cycles);
    assert_curves("general", &lmdb, &zdb, 0.5, 1.5);
}

/// No-overflow churn (values 50..800 B, always inline): band +/-25%.
#[test]
fn churn_parity_no_overflow() {
    let cycles = make_cycles(0x5EED_0002, false);
    let ldir = TempDir::new("l-inl");
    let zdir = TempDir::new("z-inl");
    let lmdb = run_lmdb(ldir.path(), &cycles);
    let zdb = run_zerodb(zdir.path(), &cycles);
    assert_curves("no-overflow", &lmdb, &zdb, 0.75, 1.25);
}
