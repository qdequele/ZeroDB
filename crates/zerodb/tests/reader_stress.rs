//! Milestone 1.8 acceptance gate 2 (PLAN §1.8; ADR-0006 §Test plan): N reader
//! threads + 1 GC-churn writer over a **real file**, with MVCC invariant
//! checks on every pinned snapshot, plus the deterministic slot-lifecycle
//! tests (`ReadersFull` exhaustion/recovery — TXN-16; `static_read_txn`
//! blocking close — TXN-24/52).
//!
//! Duration: ~5 s by default (runs in the normal suite); the minutes-long
//! acceptance variant is `just stress` (`ZERODB_STRESS_SECS`, nightly CI +
//! mandatory before the 1.14 gate — ADR-0006 ratification record (3)).
//! Debug builds additionally arm the writer-side shadow gate
//! (`RwTxn::debug_assert_gate`): every GC draw re-scans the live reader table
//! and asserts no live reader is pinned below the entry's freeing txnid —
//! "GC never reclaims a page a live reader can reach", asserted at the
//! reclaim site. `just stress` runs unoptimized so those asserts stay armed.
//!
//! Reader-side, the MVCC check is end-to-end: each reader walks its pinned
//! snapshot **twice**, with writer commits landing in between; every value
//! carries its key, generation, and a checksum, so a page reclaimed (reused)
//! under the reader shows up as a checksum/shape mismatch, and any snapshot
//! mutation shows up as a walk-digest mismatch (TXN-11 immutability).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use zerodb::{Env, EnvOpenOptions, Error, MdbError, RoTxn};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-rdrstress-{pid}-{seq}"));
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
const KEYSPACE: u64 = 1500;
const READERS: usize = 8;

fn open(dir: &Path, max_readers: u32) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    opts.max_readers(max_readers);
    opts.open(dir).expect("open env")
}

/// Deterministic value for `(key, gen)`: `[key BE 8 | gen BE 8 | payload]`,
/// payload byte `i` = `(key ^ gen ^ i) as u8`, length varied so a slice of
/// keys spills to overflow runs (multi-page values exercise run reclamation).
fn value_for(key: u64, generation: u64) -> Vec<u8> {
    let len = 24 + ((key.wrapping_mul(31) ^ generation.wrapping_mul(17)) % 2200) as usize;
    let mut v = Vec::with_capacity(len);
    v.extend_from_slice(&key.to_be_bytes());
    v.extend_from_slice(&generation.to_be_bytes());
    for i in 0..(len - 16) {
        v.push((key ^ generation ^ i as u64) as u8);
    }
    v
}

/// Verify a `(key, value)` pair is internally consistent (any torn /
/// reclaimed-under-us page read shows up here).
fn verify_entry(key: &[u8], value: &[u8]) {
    assert!(key.len() == 8, "stress keys are 8-byte BE");
    assert!(value.len() >= 16, "value too short: {}", value.len());
    let k = u64::from_be_bytes(key.try_into().unwrap());
    let vk = u64::from_be_bytes(value[0..8].try_into().unwrap());
    assert_eq!(vk, k, "value's embedded key mismatches its tree key");
    let generation = u64::from_be_bytes(value[8..16].try_into().unwrap());
    let expect = value_for(k, generation);
    assert_eq!(
        expect.len(),
        value.len(),
        "value length wrong for (key={k}, gen={generation})"
    );
    assert_eq!(
        &expect[16..],
        &value[16..],
        "payload corrupt for (key={k}, gen={generation})"
    );
}

/// Walk a whole snapshot, verifying every entry; returns `(count, digest)` —
/// an FNV-1a digest over all bytes, so two walks of one snapshot must match.
fn walk_and_digest(db: zerodb::Database, txn: &RoTxn<'_>) -> (u64, u64) {
    let mut count = 0u64;
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for r in db.iter(txn) {
        let (k, v) = r.expect("walk read");
        verify_entry(k, v);
        for &b in k.iter().chain(v.iter()) {
            h = (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01B3);
        }
        count += 1;
    }
    (count, h)
}

/// LCG for deterministic churn (no `rand` dev-dependency).
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 11
    }
}

fn stress_duration() -> Duration {
    let secs = std::env::var("ZERODB_STRESS_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(5);
    Duration::from_secs(secs)
}

/// The main acceptance stress: 8 readers + 1 GC-churn writer for
/// `ZERODB_STRESS_SECS` (default 5). Readers mix plain and env-owning
/// (`static_read_txn`) shapes and hold snapshots across commits.
#[test]
fn stress_readers_vs_gc_churn_writer() {
    let dir = TempDir::new();
    let env = open(dir.path(), 64);
    let db = env.main_database();

    // Seed a full keyspace so readers always see a populated tree.
    {
        let mut wtxn = env.write_txn().expect("begin seed txn");
        for k in 0..KEYSPACE {
            db.put(&mut wtxn, &k.to_be_bytes(), &value_for(k, 0))
                .expect("seed put");
        }
        wtxn.commit().expect("seed commit");
    }

    let deadline = Instant::now() + stress_duration();
    let stop = Arc::new(AtomicBool::new(false));
    let commits = Arc::new(AtomicU64::new(0));
    let walks = Arc::new(AtomicU64::new(0));

    std::thread::scope(|s| {
        // ---- the single writer: GC-heavy churn ----
        {
            let env = env.clone();
            let stop = Arc::clone(&stop);
            let commits = Arc::clone(&commits);
            s.spawn(move || {
                let db = env.main_database();
                let mut rng = Lcg(0x05ee_d1e8);
                let mut generation = 1u64;
                while Instant::now() < deadline {
                    let mut wtxn = env.write_txn().expect("begin churn txn");
                    // Overwrites (COW frees the old leaves/overflow runs → GC
                    // entries), plus deletes and re-inserts (rebalance churn).
                    for _ in 0..120 {
                        let k = rng.next() % KEYSPACE;
                        match rng.next() % 4 {
                            0 => {
                                let _ = db.delete(&mut wtxn, &k.to_be_bytes()).expect("del");
                            }
                            _ => {
                                db.put(&mut wtxn, &k.to_be_bytes(), &value_for(k, generation))
                                    .expect("put");
                            }
                        }
                    }
                    wtxn.commit().expect("churn commit");
                    generation += 1;
                    commits.fetch_add(1, Ordering::Relaxed);
                }
                stop.store(true, Ordering::Release);
            });
        }

        // ---- N readers: pin, verify, hold across commits, re-verify ----
        for r in 0..READERS {
            let env = env.clone();
            let stop = Arc::clone(&stop);
            let walks = Arc::clone(&walks);
            s.spawn(move || {
                let db = env.main_database();
                let mut iters = 0u64;
                // Readers keep going briefly past the writer's stop so release
                // paths race the final scans too.
                while !stop.load(Ordering::Acquire) || iters == 0 {
                    // Alternate the two slot-lifecycle shapes (TXN-23/24).
                    if (iters + r as u64) % 4 == 3 {
                        let txn = env
                            .clone()
                            .static_read_txn()
                            .expect("static read txn (64 slots, 8 readers)");
                        let (c1, h1) = walk_and_digest(db, &txn);
                        std::thread::yield_now();
                        let (c2, h2) = walk_and_digest(db, &txn);
                        assert_eq!((c1, h1), (c2, h2), "static snapshot mutated (TXN-11)");
                        drop(txn);
                    } else {
                        let txn = env.read_txn().expect("read txn (64 slots, 8 readers)");
                        let t = txn.txnid();
                        let (c1, h1) = walk_and_digest(db, &txn);
                        // Hold the pin while the writer commits and reclaims.
                        std::thread::sleep(Duration::from_micros(500));
                        let (c2, h2) = walk_and_digest(db, &txn);
                        assert_eq!(
                            (c1, h1),
                            (c2, h2),
                            "snapshot txnid={t} mutated under a live pin (TXN-11/20)"
                        );
                        assert!(t <= env.txnid(), "pinned txnid ahead of commit point");
                        drop(txn);
                    }
                    iters += 1;
                    walks.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });

    let n_commits = commits.load(Ordering::Relaxed);
    let n_walks = walks.load(Ordering::Relaxed);
    println!("stress: {n_commits} writer commits, {n_walks} reader double-walks");
    assert!(n_commits > 0, "writer never committed");
    assert!(n_walks > 0, "readers never walked");

    // Post-stress: full structural + GC-partition check of the final image
    // (INV-1..27 incl. the reachable-XOR-free partition, INV-22).
    drop(env);
    let bytes = std::fs::read(dir.path().join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = zerodb::check::check_image(&bytes, PS);
    assert!(v.is_empty(), "invariant violations after stress: {v:#?}");
}

/// TXN-16: slot exhaustion errors with `ReadersFull`; releasing one slot
/// makes `read_txn` succeed again. Also pins the exact error variant the
/// oracle compares against LMDB (`readers_full_differential.rs`).
#[test]
fn readers_full_and_recover() {
    let dir = TempDir::new();
    let env = open(dir.path(), 4);
    let mut held = Vec::new();
    for _ in 0..4 {
        held.push(env.read_txn().expect("slots available"));
    }
    match env.read_txn() {
        Err(Error::Mdb(MdbError::ReadersFull)) => {}
        Err(other) => {
            panic!("5th reader on a 4-slot table errored, but not ReadersFull: {other:?}")
        }
        Ok(_) => panic!("5th reader on a 4-slot table unexpectedly succeeded"),
    }
    // static_read_txn draws from the same table (TXN-24).
    match env.clone().static_read_txn() {
        Err(Error::Mdb(MdbError::ReadersFull)) => {}
        Err(other) => panic!("static txn errored, but not ReadersFull: {other:?}"),
        Ok(_) => panic!("static txn unexpectedly succeeded on a full table"),
    }
    drop(held.pop());
    let again = env.read_txn().expect("released slot is reusable");
    drop(again);
    drop(held);
}

/// TXN-24/52: an env-owning `static_read_txn` keeps the env open — the close
/// event fires only after the txn drops — and it reads its pinned snapshot
/// after every other handle is gone. `Send` is exercised by moving it to
/// another thread before dropping (TXN-13).
#[test]
fn static_read_txn_blocks_close_and_reads_after_handles_drop() {
    let dir = TempDir::new();
    let env = open(dir.path(), 8);
    let db = env.main_database();
    {
        let mut wtxn = env.write_txn().expect("begin");
        db.put(&mut wtxn, b"k", b"v").expect("put");
        wtxn.commit().expect("commit");
    }
    let txn = env.clone().static_read_txn().expect("static txn");
    let ev = env.prepare_for_closing();
    assert!(
        !ev.wait_timeout(Duration::from_millis(0)),
        "close must block while the env-owning txn lives (TXN-52)"
    );
    // The txn still serves reads with no other Env handle in existence, and
    // moves across threads (Send).
    let ev2 = ev.clone();
    let h = std::thread::spawn(move || {
        assert_eq!(db.get(&txn, b"k").expect("get"), Some(&b"v"[..]));
        assert!(
            !ev2.wait_timeout(Duration::from_millis(0)),
            "still open while the moved txn lives"
        );
        drop(txn);
    });
    h.join().expect("reader thread");
    ev.wait();
    assert!(ev.wait_timeout(Duration::from_millis(0)), "env closed");
}

/// A reader pinned *before* a burst of GC churn still walks its snapshot
/// intact afterwards — the deterministic (non-timing) form of the gate check,
/// with the debug shadow assert armed underneath in debug builds.
#[test]
fn pinned_reader_survives_reclaim_burst() {
    let dir = TempDir::new();
    let env = open(dir.path(), 8);
    let db = env.main_database();
    {
        let mut wtxn = env.write_txn().expect("begin");
        for k in 0..400u64 {
            db.put(&mut wtxn, &k.to_be_bytes(), &value_for(k, 0))
                .expect("put");
        }
        wtxn.commit().expect("commit");
    }
    let pinned = env.read_txn().expect("pin");
    let before = walk_and_digest(db, &pinned);

    // Churn hard: overwrite everything several times (frees + reclaims), then
    // delete half (rebalance + more GC traffic).
    for generation in 1..=6u64 {
        let mut wtxn = env.write_txn().expect("begin churn");
        for k in 0..400u64 {
            db.put(&mut wtxn, &k.to_be_bytes(), &value_for(k, generation))
                .expect("put");
        }
        wtxn.commit().expect("commit churn");
    }
    {
        let mut wtxn = env.write_txn().expect("begin dels");
        for k in (0..400u64).step_by(2) {
            db.delete(&mut wtxn, &k.to_be_bytes()).expect("del");
        }
        wtxn.commit().expect("commit dels");
    }

    let after = walk_and_digest(db, &pinned);
    assert_eq!(before, after, "pinned snapshot changed under GC churn");
    drop(pinned);

    // With the pin gone, later txns may reclaim its pages; the image stays
    // partition-clean.
    let mut wtxn = env.write_txn().expect("begin post");
    db.put(&mut wtxn, b"post", b"post").expect("put");
    wtxn.commit().expect("commit post");
    drop(env);
    let bytes = std::fs::read(dir.path().join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = zerodb::check::check_image(&bytes, PS);
    assert!(v.is_empty(), "invariant violations: {v:#?}");
}
