//! Write-cursor puts that split (SPEC 03 §5.4b): a put of the key under the
//! cursor that no longer fits its leaf splits it, and the cursor keeps its
//! path, pointed at whichever half now holds the entry — through a branch
//! split cascade and a root grow too — as LMDB fixes up the splitting
//! cursor in `mdb_page_split`. The next step must then yield the entry's
//! neighbours exactly, with no re-seek.
//!
//! Covered here, in heap and `WRITE_MAP` modes:
//! - every slot of a single-leaf tree rewritten past the page (the root leaf
//!   splits, the root grows, the entry lands on the left or the right half);
//! - full cursor walks, both directions, whose rewrites alternate growing
//!   and shrinking values over a multi-level tree with long keys (leaf
//!   splits that cascade into branch splits and grow the root);
//! - a seeded random mix of steps, seeks and growing/shrinking cursor puts
//!   checked against a model, then committed and checked on disk.
//!
//! Debug builds also re-derive every kept path from the root after each
//! cursor put (`RwTxn::shadow_check_put_path`), so a wrong repair fails
//! there even when the yielded entries happen to agree.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{check, Database, Env, EnvFlags, EnvOpenOptions, PutFlags};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-cursor-split-{pid}-{seq}"));
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
const MODES: [EnvFlags; 2] = [EnvFlags::EMPTY, EnvFlags::WRITE_MAP];

fn open(dir: &Path, flags: EnvFlags) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(64 << 20).page_size(PS).flags(flags);
    opts.open(dir).expect("open env")
}

fn assert_clean(dir: &Path) {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = check::check_image(&bytes, PS);
    assert!(v.is_empty(), "invariant violations: {v:#?}");
}

fn dump(db: &Database, env: &Env) -> Vec<(Vec<u8>, Vec<u8>)> {
    let rtxn = env.read_txn().expect("read txn");
    db.iter(&rtxn)
        .map(|e| {
            let (k, v) = e.expect("entry");
            (k.to_vec(), v.to_vec())
        })
        .collect()
}

/// A single-leaf tree (ten 300-byte values), every slot in turn rewritten
/// with a value the leaf cannot hold: the root leaf splits and the root grows
/// to depth 2, putting the entry on one half or the other depending on the
/// slot. The cursor must then step to the exact neighbours, rewrite the
/// entry again where it now sits, and walk the rest in order.
#[test]
fn put_that_splits_the_root_leaf_keeps_the_position() {
    for flags in MODES {
        let dir = TempDir::new();
        let env = open(dir.path(), flags);
        let db = env.main_database();
        let keys: Vec<Vec<u8>> = (0..10u32)
            .map(|i| format!("k{i:02}").into_bytes())
            .collect();
        let mut w = env.write_txn().unwrap();
        for k in &keys {
            db.put(&mut w, k, &[b'.'; 300]).unwrap();
        }
        w.commit().unwrap();
        for slot in 0..keys.len() {
            let mut w = env.write_txn().unwrap();
            assert_eq!(db.stat(&w).unwrap().depth, 1, "starts as one leaf");
            {
                let mut cur = db.rw_cursor(&mut w);
                assert_eq!(
                    cur.seek_ge(&keys[slot]).unwrap().unwrap().0,
                    &keys[slot][..]
                );
                // Inline (under the ~2 KiB node limit) but past the leaf.
                let big = vec![b'B'; 1500];
                if slot.is_multiple_of(2) {
                    assert!(cur.put_current(&big).unwrap());
                } else {
                    cur.put(PutFlags::EMPTY, &keys[slot], &big).unwrap();
                }
                // The first step after the put reads the kept path: the
                // successor (then back), else the predecessor (then back).
                if let Some(next) = keys.get(slot + 1) {
                    assert_eq!(cur.move_next().unwrap().unwrap().0, &next[..]);
                    let (k, v) = cur.move_prev().unwrap().unwrap();
                    assert_eq!((k, v), (&keys[slot][..], &big[..]));
                }
                if slot > 0 {
                    assert_eq!(cur.move_prev().unwrap().unwrap().0, &keys[slot - 1][..]);
                    let (k, v) = cur.move_next().unwrap().unwrap();
                    assert_eq!((k, v), (&keys[slot][..], &big[..]));
                }
                // A second put at the kept position: back to the old size.
                assert!(cur.put_current(&[b'.'; 300]).unwrap());
                let rest: Vec<Vec<u8>> =
                    std::iter::from_fn(|| cur.move_next().unwrap().map(|(k, _)| k.to_vec()))
                        .collect();
                assert_eq!(
                    rest,
                    keys[slot + 1..].to_vec(),
                    "slot {slot}: walk after the split"
                );
            }
            assert_eq!(db.stat(&w).unwrap().depth, 2, "the root leaf split");
            w.abort();
        }
    }
}

/// 48-byte keys, values that alternate between growing far past and
/// shrinking back below a leaf's share, rewritten on every entry of a full
/// cursor walk in each direction. Across a few hundred entries the puts split
/// leaves in both halves' positions, cascade into branch splits, and grow the
/// root; the walk must visit every entry once, in order, and leave exactly
/// the model's contents.
#[test]
fn rewrite_walk_through_branch_splits_and_root_grow() {
    for flags in MODES {
        for backward in [false, true] {
            let dir = TempDir::new();
            let env = open(dir.path(), flags);
            let db = env.main_database();
            let mut model = BTreeMap::new();
            let mut w = env.write_txn().unwrap();
            for i in 0..600u32 {
                let k = format!("{i:05}-{}", "x".repeat(42)).into_bytes();
                let v = vec![b'a' + (i % 26) as u8; 8];
                db.put(&mut w, &k, &v).unwrap();
                model.insert(k, v);
            }
            let depth_before = db.stat(&w).unwrap().depth;
            let mut seen = Vec::new();
            {
                let mut cur = db.rw_cursor(&mut w);
                let mut i = 0usize;
                loop {
                    let e = if backward {
                        cur.move_prev().unwrap()
                    } else {
                        cur.move_next().unwrap()
                    };
                    let Some((k, _)) = e else { break };
                    let k = k.to_vec();
                    // Mostly large (a few per leaf), now and then tiny.
                    let len = if i % 5 == 4 { 3 } else { 700 + (i * 97) % 900 };
                    let nv = vec![b'A' + (i % 26) as u8; len];
                    if i.is_multiple_of(2) {
                        assert!(cur.put_current(&nv).unwrap());
                    } else {
                        cur.put(PutFlags::EMPTY, &k, &nv).unwrap();
                    }
                    model.insert(k.clone(), nv);
                    seen.push(k);
                    i += 1;
                }
            }
            let depth_after = db.stat(&w).unwrap().depth;
            assert!(
                depth_after > depth_before,
                "the walk was meant to grow the root ({depth_before} -> {depth_after})"
            );
            let mut expected: Vec<Vec<u8>> = model.keys().cloned().collect();
            if backward {
                expected.reverse();
            }
            assert_eq!(seen, expected, "walk skipped or repeated entries");
            w.commit().unwrap();
            assert_clean(dir.path());
            let want: Vec<(Vec<u8>, Vec<u8>)> = model.into_iter().collect();
            assert_eq!(dump(&db, &env), want);
        }
    }
}

/// xorshift64*, so the random mix is reproducible.
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

/// Where the random walk's cursor is, for the exact-step check.
#[derive(Debug)]
enum Edge {
    /// On the entry `at` (after a step, a seek or a put that kept it there).
    On,
    /// Fresh, or `prev` ran past the minimum (before-begin).
    Unpositioned,
    /// `next` ran past the maximum.
    Eof,
    /// After a seek that found nothing.
    Unmodeled,
}

/// Random steps, seeks and cursor puts of random sizes (many of which split)
/// against a `BTreeMap` model: every yielded entry must be the model's entry
/// at that position, then the committed image must check clean and match.
#[test]
fn random_cursor_puts_match_a_model() {
    for flags in MODES {
        for seed in 1..=6u64 {
            let dir = TempDir::new();
            let env = open(dir.path(), flags);
            let db = env.main_database();
            let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            let mut w = env.write_txn().unwrap();
            for _ in 0..200 {
                let klen = 1 + rng.below(40) as usize;
                let k: Vec<u8> = (0..klen).map(|_| b'a' + rng.below(4) as u8).collect();
                let v = vec![b'v'; rng.below(64) as usize];
                db.put(&mut w, &k, &v).unwrap();
                model.insert(k, v);
            }
            {
                let mut cur = db.rw_cursor(&mut w);
                let mut at: Option<Vec<u8>> = None;
                let mut edge = Edge::Unpositioned;
                for _ in 0..2_000 {
                    let op = rng.below(10);
                    let forward = op <= 3;
                    let got = match op {
                        0..=3 => cur.move_next().unwrap().map(|(k, _)| k.to_vec()),
                        4..=5 => cur.move_prev().unwrap().map(|(k, _)| k.to_vec()),
                        6 => {
                            let probe: Vec<u8> = (0..1 + rng.below(6))
                                .map(|_| b'a' + rng.below(4) as u8)
                                .collect();
                            let got = cur.seek_ge(&probe).unwrap().map(|(k, _)| k.to_vec());
                            let want = model.range(probe..).next().map(|(k, _)| k.clone());
                            assert_eq!(got, want, "seek_ge");
                            // A seek past the last key leaves a position
                            // this test does not model: check the next
                            // step for membership only.
                            edge = if got.is_some() {
                                Edge::On
                            } else {
                                Edge::Unmodeled
                            };
                            at = got;
                            continue;
                        }
                        _ => {
                            let Some(k) = at.clone() else { continue };
                            // Sizes from tiny to ~1.9 KiB: most growths split.
                            let len = match rng.below(3) {
                                0 => rng.below(16) as usize,
                                _ => 400 + rng.below(1_500) as usize,
                            };
                            let v = vec![b'P'; len];
                            if rng.below(2) == 0 {
                                assert!(cur.put_current(&v).unwrap());
                            } else {
                                cur.put(PutFlags::EMPTY, &k, &v).unwrap();
                            }
                            model.insert(k, v);
                            continue;
                        }
                    };
                    // Every step lands exactly on the model's neighbour in
                    // its direction, edges included (SPEC 03 §4).
                    let want = match &edge {
                        Edge::On => {
                            let k = at.clone().expect("on an entry");
                            if forward {
                                model
                                    .range((Bound::Excluded(k), Bound::Unbounded))
                                    .next()
                                    .map(|(k, _)| k.clone())
                            } else {
                                model.range(..k).next_back().map(|(k, _)| k.clone())
                            }
                        }
                        // Unpositioned (fresh, or `prev` ran past the
                        // minimum): `next` is `first`, `prev` is `last`.
                        Edge::Unpositioned if forward => model.keys().next().cloned(),
                        Edge::Unpositioned => model.keys().next_back().cloned(),
                        // `next` ran past the maximum: `next` stays `None`,
                        // `prev` steps back onto the last entry.
                        Edge::Eof if forward => None,
                        Edge::Eof => model.keys().next_back().cloned(),
                        Edge::Unmodeled => {
                            if let Some(k) = &got {
                                assert!(model.contains_key(k), "yielded a key the model lacks");
                            }
                            got.clone()
                        }
                    };
                    assert_eq!(got, want, "step from {edge:?} at {at:?}");
                    edge = match (&got, forward) {
                        (Some(_), _) => Edge::On,
                        (None, true) => Edge::Eof,
                        (None, false) => Edge::Unpositioned,
                    };
                    at = got;
                }
            }
            w.commit().unwrap();
            assert_clean(dir.path());
            let want: Vec<(Vec<u8>, Vec<u8>)> = model.into_iter().collect();
            assert_eq!(dump(&db, &env), want, "seed {seed}");
        }
    }
}
