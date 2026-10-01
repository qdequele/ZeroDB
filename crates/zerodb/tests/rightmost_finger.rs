//! Roadmap #6 — rightmost-leaf finger (SPEC 03 §6.6) end to end on real
//! files. The finger is writer-private, in-memory state: nothing about the
//! commit pipeline or the on-disk format may change. These tests drive the
//! workloads the finger accelerates — APPEND and plain-ascending loads,
//! interleaved with out-of-order puts, deletes and a range delete, across
//! commit AND abort cycles — and pin that every committed image stays
//! invariant-clean (`check_image`, INV-22 included), that an abort leaves the
//! previous image byte-identical, and that a reopened env sees exactly the
//! `BTreeMap` model. In debug builds every fast-path hit additionally
//! re-descends and asserts path equality inside the engine.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{check, Database, Env, EnvOpenOptions, PutFlags, RoTxn};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-finger-{pid}-{seq}"));
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

/// Open with the sequential-writes setting on (ADR-0015), so every tree
/// keeps a finger.
fn open(dir: &Path) -> Env {
    open_with(dir, true)
}

fn open_with(dir: &Path, sequential_writes: bool) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    opts.max_dbs(4);
    opts.sequential_writes(sequential_writes);
    opts.open(dir).expect("open env")
}

fn key(i: u32) -> Vec<u8> {
    format!("key-{i:06}").into_bytes()
}

fn val(i: u32, tag: &str) -> Vec<u8> {
    let mut v = format!("{tag}-{i:06}-").into_bytes();
    v.resize(120, b'x');
    v
}

/// The committed data file, `check_image`-clean.
fn image(dir: &Path) -> Vec<u8> {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = check::check_image(&bytes, PS);
    assert!(v.is_empty(), "invariant violations: {v:#?}");
    bytes
}

/// Every user entry of `db`, in key order.
fn dump(db: &Database, rtxn: &RoTxn<'_>) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.iter(rtxn)
        .map(|r| {
            let (k, v) = r.unwrap();
            (k.to_vec(), v.to_vec())
        })
        .collect()
}

/// Sequential and APPEND loads across commit/abort cycles: each committed
/// image is invariant-clean, an abort changes nothing on disk, and the final
/// reopened contents equal the model.
#[test]
fn finger_loads_across_commit_and_abort_cycles() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let main = env.main_database();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    // Cycle 1: APPEND load a multi-level tree, commit.
    const N: u32 = 1500;
    let mut txn = env.write_txn().unwrap();
    for i in 0..N {
        main.put_with_flags(&mut txn, PutFlags::APPEND, &key(i), &val(i, "a"))
            .unwrap();
        model.insert(key(i), val(i, "a"));
    }
    txn.commit().unwrap();
    let after_c1 = image(dir.path());

    // Cycle 2: more appends and rewrites — then ABORT. The file must stay
    // byte-identical and the reader view unchanged.
    let mut txn = env.write_txn().unwrap();
    for i in N..N + 300 {
        main.put_with_flags(&mut txn, PutFlags::APPEND, &key(i), &val(i, "b"))
            .unwrap();
    }
    for i in (0..N).step_by(7) {
        main.put(&mut txn, &key(i), &val(i, "b")).unwrap();
    }
    txn.abort();
    assert_eq!(image(dir.path()), after_c1, "abort must not touch the file");
    {
        let rtxn = env.read_txn().unwrap();
        assert_eq!(
            dump(&main, &rtxn),
            model
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>()
        );
    }

    // Cycle 3: mixed load — plain ascending resumes past the committed max
    // (the finger re-establishes over COWed, committed pages), out-of-order
    // rewrites, a tail delete + re-append, a middle range delete, and a named
    // DB APPEND-loaded in the same txn. Commit and re-check.
    let mut txn = env.write_txn().unwrap();
    let aux = env.create_database(&mut txn, Some(b"!aux")).unwrap();
    for i in N..N + 400 {
        main.put(&mut txn, &key(i), &val(i, "c")).unwrap();
        model.insert(key(i), val(i, "c"));
    }
    for i in (0..N).step_by(13) {
        main.put(&mut txn, &key(i), &val(i, "d")).unwrap();
        model.insert(key(i), val(i, "d"));
    }
    for i in N + 380..N + 400 {
        assert!(main.delete(&mut txn, &key(i)).unwrap());
        model.remove(&key(i));
    }
    for i in N + 400..N + 450 {
        main.put_with_flags(&mut txn, PutFlags::APPEND, &key(i), &val(i, "e"))
            .unwrap();
        model.insert(key(i), val(i, "e"));
    }
    let (lo, hi) = (key(200), key(400));
    let got = main
        .delete_range(
            &mut txn,
            std::ops::Bound::Included(lo.as_slice()),
            std::ops::Bound::Excluded(hi.as_slice()),
        )
        .unwrap();
    let covered: Vec<Vec<u8>> = model
        .range::<[u8], _>((
            std::ops::Bound::Included(lo.as_slice()),
            std::ops::Bound::Excluded(hi.as_slice()),
        ))
        .map(|(k, _)| k.clone())
        .collect();
    assert_eq!(got, covered.len() as u64);
    let mut aux_model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for k in covered {
        model.remove(&k);
    }
    for i in 0..500u32 {
        aux.put_with_flags(&mut txn, PutFlags::APPEND, &key(i), &val(i, "f"))
            .unwrap();
        aux_model.insert(key(i), val(i, "f"));
    }
    txn.commit().unwrap();
    image(dir.path());

    // Reopen from disk and verify both trees against the models. The main
    // dump includes the named DB's catalog entry ("!aux" sorts before every
    // "key-" user key), so skip it.
    drop(env);
    let env = open(dir.path());
    let rtxn = env.read_txn().unwrap();
    let main = env.main_database();
    let aux = env.open_database(&rtxn, Some(b"!aux")).unwrap().unwrap();
    let main_dump: Vec<(Vec<u8>, Vec<u8>)> = dump(&main, &rtxn)
        .into_iter()
        .filter(|(k, _)| k.as_slice() != b"!aux")
        .collect();
    assert_eq!(
        main_dump,
        model
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        dump(&aux, &rtxn),
        aux_model
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>()
    );
}

/// ADR-0015: the setting changes speed only. The same workload — APPEND and
/// plain-ascending loads, out-of-order rewrites, tail deletes, a range
/// delete, a named DB with the override flipped against the env default, an
/// aborted txn — committed once with the setting on and once off must leave
/// byte-identical data files (the fast path lands entries through the same
/// leaf-insert code as a descent, so page images do not differ).
#[test]
fn sequential_writes_on_and_off_write_identical_files() {
    fn run(dir: &Path, env_default: bool) -> Vec<u8> {
        let env = open_with(dir, env_default);
        let main = env.main_database();
        let mut txn = env.write_txn().unwrap();
        let aux = env.create_database(&mut txn, Some(b"!aux")).unwrap();
        txn.commit().unwrap();
        // The named DB takes the opposite of the env default.
        env.set_sequential_writes(&aux, Some(!env_default));
        for round in 0..4u32 {
            let base = round * 600;
            let mut txn = env.write_txn().unwrap();
            for i in base..base + 500 {
                main.put_with_flags(&mut txn, PutFlags::APPEND, &key(i), &val(i, "a"))
                    .unwrap();
                aux.put(&mut txn, &key(i), &val(i, "x")).unwrap();
            }
            for i in base + 500..base + 600 {
                main.put(&mut txn, &key(i), &val(i, "b")).unwrap();
            }
            for i in (0..base + 600).step_by(11) {
                main.put(&mut txn, &key(i), &val(i, "c")).unwrap();
            }
            for i in base + 580..base + 600 {
                assert!(main.delete(&mut txn, &key(i)).unwrap());
            }
            let (lo, hi) = (key(base + 100), key(base + 150));
            main.delete_range(
                &mut txn,
                std::ops::Bound::Included(lo.as_slice()),
                std::ops::Bound::Excluded(hi.as_slice()),
            )
            .unwrap();
            if round == 2 {
                txn.abort();
            } else {
                txn.commit().unwrap();
            }
        }
        drop(env);
        image(dir)
    }
    let (on, off) = (TempDir::new(), TempDir::new());
    let a = run(on.path(), true);
    let b = run(off.path(), false);
    assert!(a == b, "the setting changed the committed file");
}
