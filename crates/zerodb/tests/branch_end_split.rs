//! Structural checks for the end-of-page branch split (SPEC 03 §6.4).
//!
//! An ascending load inserts every leaf split's separator at the end of its
//! parent branch; a full branch then splits with exactly two children on the
//! new right page. These tests build deep trees that way — ~500-byte keys keep
//! branch fanout at about 8 children per 4 KiB page, so a few thousand keys
//! reach depth 4 and above — and run the `check` invariant walker (SPEC 03
//! §11, including the two-children minimum for branches, INV-8) on every
//! committed image: after ascending loads, after random inserts and deletes on
//! top of them, and after many small ascending commits.
//! Page-count parity with LMDB lives in
//! `crates/zerodb-oracle/tests/differential/ascending_fill_parity.rs`.

mod common;
use common::TempDir;

use std::collections::BTreeMap;
use std::path::Path;

use zerodb::{check, Env, EnvOpenOptions, PutFlags, TxnRead};

const PS: u32 = 4096;
const MAP: usize = 256 << 20;

fn open(dir: &Path) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(PS);
    opts.open(dir).expect("open env")
}

fn assert_clean(dir: &Path, when: &str) {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = check::check_image(&bytes, PS);
    assert!(v.is_empty(), "invariant violations {when}: {v:#?}");
}

/// A ~500-byte key that sorts by `(i, suffix)`.
fn wide_key(i: u32, suffix: &str) -> Vec<u8> {
    let mut k = format!("wk{i:08}{suffix}").into_bytes();
    k.resize(500, b'.');
    k
}

/// Deterministic xorshift64 generator (no `rand` dependency in this crate).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// The committed tree holds exactly `model`, in order.
fn assert_contents(env: &Env, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let db = env.main_database();
    let rtxn = env.read_txn().unwrap();
    let got: Vec<(Vec<u8>, Vec<u8>)> = db
        .iter(&rtxn)
        .map(|r| {
            let (k, v) = r.unwrap();
            (k.to_vec(), v.to_vec())
        })
        .collect();
    let want: Vec<(Vec<u8>, Vec<u8>)> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(got.len(), want.len(), "entry count");
    assert!(got == want, "tree contents differ from the model");
}

/// One ascending transaction deep enough for several branch levels: the image
/// is structurally clean and the branch pages are packed (about 7 children
/// each, against about 4 under a median split).
#[test]
fn ascending_load_deep_tree_is_clean_and_packed() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let n = 6_000u32;
    let mut model = BTreeMap::new();
    let mut wtxn = env.write_txn().unwrap();
    for i in 0..n {
        let (k, v) = (wide_key(i, ""), format!("v{i}").into_bytes());
        db.put(&mut wtxn, &k, &v).unwrap();
        model.insert(k, v);
    }
    let rec = *TxnRead::main_record(&wtxn);
    wtxn.commit().unwrap();
    assert_clean(dir.path(), "after the ascending load");
    assert_contents(&env, &model);
    assert!(rec.depth >= 4, "expected depth >= 4, got {}", rec.depth);
    // Every page but the root is some branch's child.
    let fanout = (rec.leaf_pages + rec.branch_pages - 1) as f64 / rec.branch_pages as f64;
    assert!(
        fanout > 6.0,
        "ascending load left branches under-filled: {fanout:.2} children per \
         branch ({} branches over {} leaves)",
        rec.branch_pages,
        rec.leaf_pages
    );
}

/// The same load through `PutFlags::APPEND`.
#[test]
fn append_load_deep_tree_is_clean() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let mut model = BTreeMap::new();
    let mut wtxn = env.write_txn().unwrap();
    for i in 0..6_000u32 {
        let (k, v) = (wide_key(i, ""), format!("a{i}").into_bytes());
        db.put_with_flags(&mut wtxn, PutFlags::APPEND, &k, &v)
            .unwrap();
        model.insert(k, v);
    }
    assert!(TxnRead::main_record(&wtxn).depth >= 4);
    wtxn.commit().unwrap();
    assert_clean(dir.path(), "after the APPEND load");
    assert_contents(&env, &model);
}

/// Random inserts (between existing keys and past the end) and random deletes
/// on top of an ascending-built tree, several commits each, checked after
/// every commit: the packed branches split, borrow and merge like any other.
#[test]
fn random_writes_after_ascending_load_stay_clean() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let n = 4_000u32;
    let mut model = BTreeMap::new();
    let mut wtxn = env.write_txn().unwrap();
    for i in 0..n {
        let (k, v) = (wide_key(i, ""), vec![b'x'; 8]);
        db.put(&mut wtxn, &k, &v).unwrap();
        model.insert(k, v);
    }
    wtxn.commit().unwrap();
    assert_clean(dir.path(), "after the ascending load");

    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for round in 0..6 {
        let mut wtxn = env.write_txn().unwrap();
        for _ in 0..1_500 {
            // Mostly inserts in the middle of the key space; some past the
            // end; some deletes of existing keys.
            match rng.below(10) {
                0..=5 => {
                    let i = rng.below(u64::from(n)) as u32;
                    let k = wide_key(i, &format!("r{}", rng.below(1_000)));
                    let v = vec![b'r'; 1 + rng.below(40) as usize];
                    db.put(&mut wtxn, &k, &v).unwrap();
                    model.insert(k, v);
                }
                6..=7 => {
                    let i = n + rng.below(u64::from(n)) as u32;
                    let k = wide_key(i, "");
                    let v = vec![b'e'; 4];
                    db.put(&mut wtxn, &k, &v).unwrap();
                    model.insert(k, v);
                }
                _ => {
                    if model.is_empty() {
                        continue;
                    }
                    let nth = rng.below(model.len() as u64) as usize;
                    let k = model.keys().nth(nth).cloned().unwrap();
                    assert!(db.delete(&mut wtxn, &k).unwrap());
                    model.remove(&k);
                }
            }
        }
        wtxn.commit().unwrap();
        assert_clean(dir.path(), &format!("after random round {round}"));
    }
    assert_contents(&env, &model);
}

/// Many small ascending commits: each transaction copies the right spine and
/// appends to it, so end-of-page branch splits happen on pages written by
/// earlier transactions.
#[test]
fn small_ascending_commits_stay_clean() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let mut model = BTreeMap::new();
    let mut next = 0u32;
    for batch in 0..120 {
        let mut wtxn = env.write_txn().unwrap();
        for _ in 0..40 {
            let (k, v) = (wide_key(next, ""), next.to_be_bytes().to_vec());
            db.put(&mut wtxn, &k, &v).unwrap();
            model.insert(k, v);
            next += 1;
        }
        wtxn.commit().unwrap();
        if batch % 20 == 19 {
            assert_clean(dir.path(), &format!("after batch {batch}"));
        }
    }
    assert_clean(dir.path(), "after the last batch");
    assert_contents(&env, &model);
    let rtxn = env.read_txn().unwrap();
    assert!(TxnRead::main_record(&rtxn).depth >= 4);
}
