//! Milestone 1.9 acceptance: replay of the milli/hannoy nested-read fan-out
//! patterns on a **real file** with **real threads** (PLAN §1.9, SPEC 04 §5,
//! ADR-0007 D5 item 4).
//!
//! The six consumer call sites (SPEC 00 Findings §A — milli ×5, hannoy ×1)
//! share one shape: open a write txn, stage uncommitted writes, open
//! `N = rayon_threads(+1)` nested read children, fan them out to workers that
//! read the uncommitted state in parallel while the writer is paused, join,
//! resume writing, commit. `std::thread::scope` stands in for rayon (not a
//! dependency): the scoped spawn/join edges are the same synchronization the
//! rayon scope provides, and moving each `NestedRoTxn` into its worker is the
//! real `Send` assertion (compiler-derived from `RwTxn: Sync`, ADR-0007 D1 —
//! no `unsafe impl` anywhere).
//!
//! Values embed `(key, generation, checksum-ish payload)` so any read of a
//! wrong / stale / torn page fails loudly, and a slice of the keyspace uses
//! multi-page values so workers read **dirty overflow runs** (TXN-27 through
//! the dirty arm) as well as committed-untouched pages (the map arm).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;

use zerodb::{Database, Env, EnvOpenOptions, NestedRoTxn, TxnRead};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-nestfan-{pid}-{seq}"));
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
/// milli/hannoy shape: `rayon::current_num_threads() + 1`.
const N_WORKERS: usize = 9;
const KEYSPACE: u64 = 800;

fn open(dir: &Path) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    opts.max_dbs(4);
    opts.open(dir).expect("open env")
}

fn key_bytes(key: u64) -> [u8; 8] {
    key.to_be_bytes()
}

/// Deterministic value for `(key, gen)`. Every 16th key gets a multi-page
/// (~3.5-page) value so the fan-out reads dirty **overflow runs**.
fn value_for(key: u64, generation: u64) -> Vec<u8> {
    let len = if key % 16 == 0 {
        14_000
    } else {
        24 + ((key.wrapping_mul(31) ^ generation.wrapping_mul(17)) % 300) as usize
    };
    let mut v = Vec::with_capacity(len);
    v.extend_from_slice(&key.to_be_bytes());
    v.extend_from_slice(&generation.to_be_bytes());
    for i in 0..(len - 16) {
        v.push((key ^ generation ^ i as u64) as u8);
    }
    v
}

/// A worker's read pass over its shard of the keyspace, all through the
/// nested child `txn`: exact-byte gets (incl. dirty overflow runs), a prefix
/// walk, and a full-iteration digest of entry count.
fn worker_read_pass<T: TxnRead>(
    txn: &T,
    db: &Database,
    worker: usize,
    expected_gen_for: impl Fn(u64) -> u64,
) -> u64 {
    // Shard the keyspace like milli's `prefix_index % thread_count`.
    let mut checked = 0u64;
    for key in 0..KEYSPACE {
        if key as usize % N_WORKERS != worker {
            continue;
        }
        let expect = value_for(key, expected_gen_for(key));
        let got = db
            .get(txn, &key_bytes(key))
            .expect("get through nested child")
            .unwrap_or_else(|| panic!("key {key} missing through nested child"));
        assert_eq!(
            got,
            expect.as_slice(),
            "key {key}: nested child read wrong bytes (gen mismatch or torn page)"
        );
        checked += 1;
    }
    // Full-iteration count: the merged uncommitted view must be complete.
    let n = db.iter(txn).count() as u64;
    assert_eq!(n, KEYSPACE, "iteration through nested child incomplete");
    checked
}

/// The milli pattern (`words_prefix_docids.rs`, `facet_bulk.rs`,
/// `indexer/mod.rs`, `upgrade/v1_32.rs`): `iter::repeat_with(||
/// wtxn.nested_read_txn()).take(N)` → `into_par_iter()` → join → writer
/// resumes → commit. Two paused windows: workers in window 2 must see both
/// generations (gen-1 keys rewritten to gen 2 between the windows).
#[test]
fn milli_fanout_two_windows_then_commit() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();

    // A committed baseline for half the keyspace, so children also read
    // committed-untouched pages through the map (TXN-27's second arm).
    let mut wtxn = env.write_txn().unwrap();
    for key in 0..KEYSPACE / 2 {
        db.put(&mut wtxn, &key_bytes(key), &value_for(key, 1))
            .unwrap();
    }
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    // Stage the other half uncommitted (gen 1).
    for key in KEYSPACE / 2..KEYSPACE {
        db.put(&mut wtxn, &key_bytes(key), &value_for(key, 1))
            .unwrap();
    }

    // ---- paused window 1: everything is gen 1. ----
    let children: Vec<NestedRoTxn<'_>> = std::iter::repeat_with(|| wtxn.nested_read_txn())
        .take(N_WORKERS)
        .collect::<zerodb::Result<Vec<_>>>()
        .unwrap();
    std::thread::scope(|s| {
        let handles: Vec<_> = children
            .into_iter()
            .enumerate()
            .map(|(worker, child)| {
                let db = &db;
                // `move`ing the child into the worker is the Send assertion.
                s.spawn(move || worker_read_pass(&child, db, worker, |_| 1))
            })
            .collect();
        let total: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total, KEYSPACE, "every key checked exactly once");
        // children dropped inside the scope (consumed by the workers).
    });

    // ---- writer resumes (RAII: last child drop reopened the window). ----
    for key in (0..KEYSPACE).step_by(3) {
        db.put(&mut wtxn, &key_bytes(key), &value_for(key, 2))
            .unwrap();
    }

    // ---- paused window 2: gen 2 for key % 3 == 0, else gen 1. ----
    let children: Vec<NestedRoTxn<'_>> = std::iter::repeat_with(|| wtxn.nested_read_txn())
        .take(N_WORKERS)
        .collect::<zerodb::Result<Vec<_>>>()
        .unwrap();
    std::thread::scope(|s| {
        let handles: Vec<_> = children
            .into_iter()
            .enumerate()
            .map(|(worker, child)| {
                let db = &db;
                s.spawn(move || {
                    worker_read_pass(&child, db, worker, |k| if k % 3 == 0 { 2 } else { 1 })
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    });

    // ---- resume once more, then commit; a fresh reader sees the final state.
    db.put(&mut wtxn, &key_bytes(KEYSPACE), &value_for(KEYSPACE, 7))
        .unwrap();
    wtxn.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    assert_eq!(rtxn.txnid(), 2);
    for key in 0..=KEYSPACE {
        let generation = if key == KEYSPACE {
            7
        } else if key % 3 == 0 {
            2
        } else {
            1
        };
        assert_eq!(
            db.get(&rtxn, &key_bytes(key)).unwrap().unwrap(),
            value_for(key, generation).as_slice(),
            "post-commit key {key}"
        );
    }
    assert_eq!(db.len(&rtxn).unwrap(), KEYSPACE + 1);
}

/// The hannoy pattern (`parallel.rs::FrozenReader`): children are opened
/// sequentially into a **channel pool**; each worker takes one from the pool
/// (thread_local-style), does many point-reads, and the children drop on the
/// worker threads (the Release-decrement path runs off the writer thread).
#[test]
fn hannoy_channel_pool_fanout() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();

    let mut wtxn = env.write_txn().unwrap();
    for key in 0..KEYSPACE {
        db.put(&mut wtxn, &key_bytes(key), &value_for(key, 1))
            .unwrap();
    }

    // "We make sure to have one more thread so the current/main thread has a
    // nested rtxn." — hannoy opens N+1 sequentially and pools them (its
    // crossbeam Receiver is Sync; std's is not, so the shared pool sits
    // behind a Mutex — same take-one-per-worker discipline).
    let (sender, pool) = mpsc::sync_channel::<NestedRoTxn<'_>>(N_WORKERS + 1);
    for _ in 0..=N_WORKERS {
        sender.try_send(wtxn.nested_read_txn().unwrap()).unwrap();
    }
    // The channel endpoints' types carry the children's `&wtxn` lifetime, so
    // both must be gone before the writer resumes (the compile-time TXN-30
    // guarantee at work): sender now, pool right after the scope.
    drop(sender);
    let pool = std::sync::Mutex::new(pool);

    std::thread::scope(|s| {
        let pool = &pool;
        let db = &db;
        let handles: Vec<_> = (0..N_WORKERS)
            .map(|worker| {
                s.spawn(move || {
                    let rtxn = pool
                        .lock()
                        .unwrap()
                        .recv()
                        .expect("pool has a child per worker");
                    let mut checked = 0u64;
                    for key in 0..KEYSPACE {
                        if key as usize % N_WORKERS != worker {
                            continue;
                        }
                        let got = db.get(&rtxn, &key_bytes(key)).unwrap().unwrap();
                        assert_eq!(got, value_for(key, 1).as_slice(), "key {key}");
                        checked += 1;
                    }
                    checked // rtxn drops HERE, on the worker thread
                })
            })
            .collect();
        // The main thread uses the extra child, like hannoy's current thread.
        let main_child = pool.lock().unwrap().recv().unwrap();
        assert_eq!(db.len(&main_child).unwrap(), KEYSPACE);
        drop(main_child);
        let total: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total, KEYSPACE);
    });
    drop(pool);

    // All children released on their worker threads; the writer resumes,
    // commits, and the data is durable.
    db.delete(&mut wtxn, &key_bytes(0)).unwrap();
    wtxn.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    assert_eq!(db.get(&rtxn, &key_bytes(0)).unwrap(), None);
    assert_eq!(db.len(&rtxn).unwrap(), KEYSPACE - 1);
}

/// Named DBs through the fan-out: an uncommitted named DB (created this txn,
/// catalog entry still dirty) is readable from every worker (TXN-26 +
/// `record_for` delegation, ADR-0007 D2).
#[test]
fn fanout_reads_uncommitted_named_db() {
    let dir = TempDir::new();
    let env = open(dir.path());

    let mut wtxn = env.write_txn().unwrap();
    let named = env.create_database(&mut wtxn, Some(b"vectors")).unwrap();
    for key in 0..200u64 {
        named
            .put(&mut wtxn, &key_bytes(key), &value_for(key, 1))
            .unwrap();
    }

    let children: Vec<NestedRoTxn<'_>> = std::iter::repeat_with(|| wtxn.nested_read_txn())
        .take(4)
        .collect::<zerodb::Result<Vec<_>>>()
        .unwrap();
    std::thread::scope(|s| {
        for (worker, child) in children.into_iter().enumerate() {
            let named = &named;
            s.spawn(move || {
                for key in 0..200u64 {
                    if key as usize % 4 != worker {
                        continue;
                    }
                    assert_eq!(
                        named.get(&child, &key_bytes(key)).unwrap().unwrap(),
                        value_for(key, 1).as_slice()
                    );
                }
            });
        }
    });

    wtxn.commit().unwrap();
    let rtxn = env.read_txn().unwrap();
    let reopened = env.open_database(&rtxn, Some(b"vectors")).unwrap().unwrap();
    assert_eq!(reopened.len(&rtxn).unwrap(), 200);
}
