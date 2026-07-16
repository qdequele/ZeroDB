//! Milestone 1.3 zerodb-only read-API tests. These drive the public
//! `zerodb::{Env, RoTxn, Database}` read surface directly over a file produced
//! by the bulk-load builder — covering `range`/`rev_range` (all `Bound`
//! combinations) and a multi-megabyte overflow value, which the oracle `Op`
//! model does not exercise (its values are bounded, and it has no range op).

use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::EnvOpenOptions;
use zerodb_core::builder::{build_single_db_image, DEFAULT_FILL_PERMILLE};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}
impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("zerodb-read-{pid}-{nanos}-{seq}"));
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
const MAP: u64 = 16 << 20;

/// Materialize `entries` into `dir/zerodb.dat` and open it.
fn open_with(dir: &TempDir, entries: &[(Vec<u8>, Vec<u8>)]) -> zerodb::Env {
    let image = build_single_db_image(PS, MAP, 1, entries, DEFAULT_FILL_PERMILLE).unwrap();
    std::fs::write(dir.path().join(zerodb::DATA_FILE_NAME), &image).unwrap();
    let mut o = EnvOpenOptions::new();
    o.map_size(MAP as usize);
    o.page_size(PS);
    o.open(dir.path()).unwrap()
}

fn kv(k: &[u8], v: &[u8]) -> (Vec<u8>, Vec<u8>) {
    (k.to_vec(), v.to_vec())
}

fn collect(iter: zerodb::RoRange<'_>) -> Vec<(Vec<u8>, Vec<u8>)> {
    iter.map(|r| {
        let (k, v) = r.unwrap();
        (k.to_vec(), v.to_vec())
    })
    .collect()
}

#[test]
fn get_len_first_last() {
    let dir = TempDir::new();
    let entries: Vec<_> = (0u16..1000)
        .map(|i| kv(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes()))
        .collect();
    let env = open_with(&dir, &entries);
    let rtxn = env.read_txn().unwrap();
    let db = env.main_database();

    assert_eq!(db.len(&rtxn).unwrap(), 1000);
    assert!(!db.is_empty(&rtxn).unwrap());
    assert_eq!(db.get(&rtxn, b"k0500").unwrap(), Some(b"v500".as_slice()));
    assert_eq!(db.get(&rtxn, b"nope").unwrap(), None);
    assert_eq!(
        db.first(&rtxn).unwrap().map(|(k, _)| k.to_vec()),
        Some(b"k0000".to_vec())
    );
    assert_eq!(
        db.last(&rtxn).unwrap().map(|(k, _)| k.to_vec()),
        Some(b"k0999".to_vec())
    );
}

#[test]
fn range_all_bound_combinations() {
    let dir = TempDir::new();
    let entries: Vec<_> = ["a", "b", "c", "d", "e"]
        .iter()
        .map(|s| kv(s.as_bytes(), b"x"))
        .collect();
    let env = open_with(&dir, &entries);
    let rtxn = env.read_txn().unwrap();
    let db = env.main_database();
    let keys = |it: zerodb::RoRange<'_>| -> Vec<Vec<u8>> {
        collect(it).into_iter().map(|(k, _)| k).collect()
    };

    // Forward.
    assert_eq!(
        keys(db.range(&rtxn, Bound::Included(b"b"), Bound::Included(b"d"))),
        vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
    );
    assert_eq!(
        keys(db.range(&rtxn, Bound::Excluded(b"b"), Bound::Excluded(b"d"))),
        vec![b"c".to_vec()]
    );
    assert_eq!(
        keys(db.range(&rtxn, Bound::Unbounded, Bound::Excluded(b"c"))),
        vec![b"a".to_vec(), b"b".to_vec()]
    );
    assert_eq!(
        keys(db.range(&rtxn, Bound::Included(b"c"), Bound::Unbounded)),
        vec![b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]
    );
    assert_eq!(
        keys(db.range(&rtxn, Bound::Unbounded, Bound::Unbounded)).len(),
        5
    );

    // Reverse.
    assert_eq!(
        keys(db.rev_range(&rtxn, Bound::Included(b"b"), Bound::Included(b"d"))),
        vec![b"d".to_vec(), b"c".to_vec(), b"b".to_vec()]
    );
    assert_eq!(
        keys(db.rev_range(&rtxn, Bound::Excluded(b"a"), Bound::Excluded(b"e"))),
        vec![b"d".to_vec(), b"c".to_vec(), b"b".to_vec()]
    );
    assert_eq!(
        keys(db.rev_iter(&rtxn)),
        vec![
            b"e".to_vec(),
            b"d".to_vec(),
            b"c".to_vec(),
            b"b".to_vec(),
            b"a".to_vec()
        ]
    );
}

#[test]
fn prefix_and_rev_prefix() {
    let dir = TempDir::new();
    let entries: Vec<_> = ["aa", "ab", "ac", "b", "ba"]
        .iter()
        .map(|s| kv(s.as_bytes(), b"x"))
        .collect();
    let env = open_with(&dir, &entries);
    let rtxn = env.read_txn().unwrap();
    let db = env.main_database();

    let fwd: Vec<Vec<u8>> = collect(db.prefix_iter(&rtxn, b"a"))
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(fwd, vec![b"aa".to_vec(), b"ab".to_vec(), b"ac".to_vec()]);
    let rev: Vec<Vec<u8>> = collect(db.rev_prefix_iter(&rtxn, b"a"))
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(rev, vec![b"ac".to_vec(), b"ab".to_vec(), b"aa".to_vec()]);
}

#[test]
fn multi_megabyte_overflow_value() {
    // A single large overflow run (many pages), read back zero-copy — the size
    // the bounded oracle values do not reach.
    let dir = TempDir::new();
    let big = vec![0x37u8; 5_000_000];
    let entries = vec![kv(b"a", b"small"), kv(b"big", &big), kv(b"z", b"end")];
    let env = open_with(&dir, &entries);
    let rtxn = env.read_txn().unwrap();
    let db = env.main_database();
    assert_eq!(db.get(&rtxn, b"big").unwrap(), Some(big.as_slice()));
    assert_eq!(collect(db.iter(&rtxn)).len(), 3);
}

#[test]
fn empty_db_reads() {
    let dir = TempDir::new();
    let env = open_with(&dir, &[]);
    let rtxn = env.read_txn().unwrap();
    let db = env.main_database();
    assert_eq!(db.len(&rtxn).unwrap(), 0);
    assert!(db.is_empty(&rtxn).unwrap());
    assert_eq!(db.get(&rtxn, b"x").unwrap(), None);
    assert_eq!(db.first(&rtxn).unwrap(), None);
    assert_eq!(collect(db.iter(&rtxn)), Vec::new());
}
