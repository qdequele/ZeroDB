//! Bounded dirty memory (ADR-0017; SPEC 04 §6.3a, TXN-68..72): a write txn
//! past its dirty limit spills its highest-numbered dirty pages to the file,
//! as LMDB's `mdb_page_spill` does.
//!
//! - spilling is invisible to results: the same seeded workload (puts with
//!   overflow values, deletes, range deletes, cursor rewrites and deletes,
//!   nested reads, reads of spilled pages mid-txn) under a tiny limit and
//!   under the default gives identical contents, before and after reopen,
//!   in heap and `WRITE_MAP` modes, and every committed image passes
//!   `check_image`;
//! - the dirty set stays near the limit through a large txn;
//! - an abort after spilling leaves exactly the last commit;
//! - a txn that stays under the limit never spills.
//!
//! Do not weaken (AGENTS.md rule 2).

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{check, Database, Env, EnvFlags, EnvOpenOptions, RwTxn};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-spill-{pid}-{seq}"));
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
/// The smallest limit the option allows (SPEC 04 TXN-68): 128 pages.
const TINY: usize = 128 * PS as usize;

fn open(dir: &Path, limit: Option<usize>, flags: EnvFlags) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(256 << 20)
        .page_size(PS)
        .max_dbs(4)
        .flags(flags);
    if let Some(b) = limit {
        opts.max_dirty_bytes(b);
    }
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

type Model = [BTreeMap<Vec<u8>, Vec<u8>>; 3];

fn key(rng: &mut Rng) -> Vec<u8> {
    format!("key-{:06}", rng.below(40_000)).into_bytes()
}

fn value(rng: &mut Rng, round: u64) -> Vec<u8> {
    let len = if rng.below(16) == 0 {
        4_500 + rng.below(12_000) as usize // overflow run
    } else {
        40 + rng.below(400) as usize
    };
    format!("v{round}.{}-", rng.below(1000))
        .bytes()
        .cycle()
        .take(len)
        .collect()
}

fn contents(txn: &impl zerodb::TxnRead, db: Database) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.iter(txn)
        .map(|e| {
            let (k, v) = e.unwrap();
            (k.to_vec(), v.to_vec())
        })
        .collect()
}

/// One big txn of mixed mutations with reads checked against the model as
/// it goes (reads of spilled pages included once spills start).
fn big_txn(env: &Env, dbs: &[Database; 3], model: &mut Model, rng: &mut Rng, round: u64) -> usize {
    let mut w = env.write_txn().unwrap();
    let mut max_spilled = 0;
    for step in 0..6_000u64 {
        let d = rng.below(3) as usize;
        match rng.below(20) {
            0..=11 => {
                let (k, v) = (key(rng), value(rng, round));
                dbs[d].put(&mut w, &k, &v).unwrap();
                model[d].insert(k, v);
            }
            12..=14 => {
                let k = key(rng);
                let had = dbs[d].delete(&mut w, &k).unwrap();
                assert_eq!(had, model[d].remove(&k).is_some());
            }
            15 => {
                let (a, b) = (key(rng), key(rng));
                let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
                let n = dbs[d]
                    .delete_range(&mut w, Bound::Included(&lo), Bound::Excluded(&hi))
                    .unwrap();
                let gone: Vec<_> = model[d]
                    .range(lo.clone()..hi.clone())
                    .map(|(k, _)| k.clone())
                    .collect();
                assert_eq!(n as usize, gone.len());
                for k in gone {
                    model[d].remove(&k);
                }
            }
            16 => {
                // Cursor pass over a slice of the keyspace: rewrite and
                // delete through the write cursor.
                let start = key(rng);
                let mut cur = dbs[d].rw_cursor(&mut w);
                let mut i = 0u32;
                let mut seen = cur.seek_ge(&start).unwrap().map(|(k, _)| k.to_vec());
                while let Some(k) = seen {
                    if i >= 40 {
                        break;
                    }
                    if i.is_multiple_of(5) {
                        cur.del_current().unwrap();
                        model[d].remove(&k);
                    } else if i.is_multiple_of(3) {
                        let v = format!("cur{round}.{i}").into_bytes();
                        cur.put_current(&v).unwrap();
                        model[d].insert(k, v);
                    }
                    i += 1;
                    seen = cur.move_next().unwrap().map(|(k, _)| k.to_vec());
                }
            }
            17 => {
                // Nested read child: sees the writer's state.
                let child = w.nested_read_txn().unwrap();
                let k = key(rng);
                assert_eq!(
                    dbs[d].get(&child, &k).unwrap(),
                    model[d].get(&k).map(Vec::as_slice),
                    "nested read, step {step}"
                );
            }
            _ => {
                let k = key(rng);
                assert_eq!(
                    dbs[d].get(&w, &k).unwrap(),
                    model[d].get(&k).map(Vec::as_slice),
                    "writer read, step {step}"
                );
            }
        }
        max_spilled = max_spilled.max(w.spilled_pgnos());
    }
    for (d, db) in dbs.iter().enumerate() {
        let want: Vec<_> = model[d]
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(contents(&w, *db), want, "writer view before commit, db {d}");
    }
    w.commit().unwrap();
    max_spilled
}

/// Three named DBs (the main DB also holds their catalog records, which the
/// model does not track).
const NAMES: [&[u8]; 3] = [b"alpha", b"beta", b"gamma"];

fn create_dbs(env: &Env) -> [Database; 3] {
    let mut w = env.write_txn().unwrap();
    let dbs = NAMES.map(|n| env.create_database(&mut w, Some(n)).unwrap());
    w.commit().unwrap();
    dbs
}

fn differential(flags: EnvFlags) {
    let tiny_dir = TempDir::new();
    let plain_dir = TempDir::new();
    let tiny = open(tiny_dir.path(), Some(TINY), flags);
    let plain = open(plain_dir.path(), None, flags);
    let tdbs = create_dbs(&tiny);
    let pdbs = create_dbs(&plain);
    let mut tmodel: Model = Default::default();
    let mut pmodel: Model = Default::default();
    let mut spilled = 0;
    for round in 0..3u64 {
        let seed = 0x5EED_0000 + round;
        spilled = spilled.max(big_txn(&tiny, &tdbs, &mut tmodel, &mut Rng(seed), round));
        let unspilled = big_txn(&plain, &pdbs, &mut pmodel, &mut Rng(seed), round);
        assert_eq!(unspilled, 0, "the default limit is never reached here");
        assert_clean(tiny_dir.path());
        assert_clean(plain_dir.path());
    }
    assert!(spilled > 0, "the tiny limit must actually spill");
    let (rt, rp) = (tiny.read_txn().unwrap(), plain.read_txn().unwrap());
    for d in 0..3 {
        assert_eq!(contents(&rt, tdbs[d]), contents(&rp, pdbs[d]), "db {d}");
    }
    drop((rt, rp));
    // What was spilled really reached the file.
    drop(tiny);
    let tiny = open(tiny_dir.path(), Some(TINY), flags);
    let rt = tiny.read_txn().unwrap();
    let rp = plain.read_txn().unwrap();
    for (d, name) in NAMES.iter().enumerate() {
        let db = tiny.open_database(&rt, Some(name)).unwrap().unwrap();
        assert_eq!(
            contents(&rt, db),
            contents(&rp, pdbs[d]),
            "db {d} after reopen"
        );
    }
}

#[test]
fn spilling_is_invisible_to_results() {
    differential(EnvFlags::NO_SYNC);
}

#[test]
fn spilling_is_invisible_to_results_under_write_map() {
    differential(EnvFlags::NO_SYNC | EnvFlags::WRITE_MAP);
}

#[test]
fn dirty_memory_stays_near_the_limit() {
    let dir = TempDir::new();
    let env = open(dir.path(), Some(TINY), EnvFlags::NO_SYNC);
    let db = env.main_database();
    let limit = (TINY / PS as usize) as u64;
    let mut rng = Rng(0xD1_27E5);
    let mut model = BTreeMap::new();
    let mut w: RwTxn<'_> = env.write_txn().unwrap();
    let mut peak = 0;
    for _ in 0..60_000 {
        let k = format!("k{:08}", rng.below(1 << 30)).into_bytes();
        let v = vec![b'x'; 200];
        db.put(&mut w, &k, &v).unwrap();
        model.insert(k, v);
        peak = peak.max(w.dirty_pages());
        // One put adds a handful of pages past the start-of-call check.
        assert!(
            w.dirty_pages() <= limit + 64,
            "dirty {} over limit {limit}",
            w.dirty_pages()
        );
    }
    assert!(w.spilled_pgnos() > 0);
    w.commit().unwrap();
    assert_clean(dir.path());
    let r = env.read_txn().unwrap();
    let want: Vec<_> = model.into_iter().collect();
    assert_eq!(contents(&r, db), want);
    assert!(
        peak > limit / 2,
        "the workload should reach the limit (peak {peak})"
    );
}

#[test]
fn abort_after_spilling_leaves_the_last_commit() {
    abort_after_spill(EnvFlags::EMPTY);
}

/// ADR-0021 M3: abort-after-spill under in-place `WRITE_MAP` (TXN-45b) —
/// the "spilled" pages were stored in the map at allocation time and the
/// spill was pure bookkeeping, so the abort leaves them as unreferenced
/// scribbles; the last commit survives intact, also across reopen, and the
/// space is reused.
#[test]
fn abort_after_spilling_leaves_the_last_commit_writemap_in_place() {
    abort_after_spill(EnvFlags::WRITE_MAP);
}

fn abort_after_spill(flags: EnvFlags) {
    let dir = TempDir::new();
    let env = open(dir.path(), Some(TINY), flags);
    let db = env.main_database();
    let mut w = env.write_txn().unwrap();
    assert_eq!(
        w.dirty_in_map_mode(),
        flags.contains(EnvFlags::WRITE_MAP),
        "dirty-page realization must match the env flags (ADR-0021)"
    );
    for i in 0..2_000u32 {
        db.put(&mut w, format!("base{i:05}").as_bytes(), b"committed")
            .unwrap();
    }
    w.commit().unwrap();
    let before = contents(&env.read_txn().unwrap(), db);

    let mut w = env.write_txn().unwrap();
    for i in 0..30_000u32 {
        db.put(&mut w, format!("temp{i:07}").as_bytes(), &[b't'; 300])
            .unwrap();
        if i % 7 == 0 {
            db.delete(&mut w, format!("base{:05}", i % 2_000).as_bytes())
                .unwrap();
        }
    }
    assert!(
        w.spilled_pgnos() > 0,
        "the txn must have spilled before the abort"
    );
    w.abort();
    assert_eq!(contents(&env.read_txn().unwrap(), db), before);
    assert_clean(dir.path());
    drop(env);

    let env = open(dir.path(), Some(TINY), flags);
    assert_eq!(
        contents(&env.read_txn().unwrap(), db),
        before,
        "after reopen"
    );
    // The next txn reuses the space the aborted txn wrote into.
    let mut w = env.write_txn().unwrap();
    for i in 0..5_000u32 {
        db.put(&mut w, format!("next{i:05}").as_bytes(), &[b'n'; 300])
            .unwrap();
    }
    w.commit().unwrap();
    assert_clean(dir.path());
    assert_eq!(
        contents(&env.read_txn().unwrap(), db).len(),
        before.len() + 5_000
    );
}

#[test]
fn a_txn_under_the_limit_never_spills() {
    let dir = TempDir::new();
    let env = open(dir.path(), None, EnvFlags::NO_SYNC);
    let db = env.main_database();
    let mut w = env.write_txn().unwrap();
    for i in 0..20_000u32 {
        db.put(&mut w, format!("k{i:06}").as_bytes(), &[7u8; 100])
            .unwrap();
    }
    assert_eq!(w.spilled_pgnos(), 0);
    w.commit().unwrap();
}

/// Fill `db` with inline values inside `w` until the txn has spilled (and a
/// bit past), returning the keys written.
fn fill_until_spilled(db: Database, w: &mut RwTxn<'_>, tag: &str) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    let mut i = 0u32;
    while w.spilled_pgnos() == 0 || !i.is_multiple_of(500) {
        let k = format!("{tag}{i:07}").into_bytes();
        db.put(w, &k, &[b'i'; 300]).unwrap();
        keys.push(k);
        i += 1;
        assert!(i < 200_000, "never spilled");
    }
    keys
}

/// `clear` and `drop_db` of trees whose leaves are spilled this-txn pages.
/// `inline` holds inline values only, so `clear` takes the leaf-skipping
/// collection (SPEC 02 §6.1), which must accept spilled leaves (TXN-72).
#[test]
fn clear_and_drop_of_spilled_trees() {
    let dir = TempDir::new();
    let env = open(dir.path(), Some(TINY), EnvFlags::NO_SYNC);
    let mut w = env.write_txn().unwrap();
    let inline = env.create_database(&mut w, Some(b"inline")).unwrap();
    let doomed = env.create_database(&mut w, Some(b"doomed")).unwrap();
    let kept = env.create_database(&mut w, Some(b"kept")).unwrap();
    fill_until_spilled(inline, &mut w, "a");
    fill_until_spilled(doomed, &mut w, "b");
    let kept_keys = fill_until_spilled(kept, &mut w, "c");
    assert!(w.spilled_pgnos() > 0);
    inline.clear(&mut w).unwrap();
    doomed.drop_db(&mut w).unwrap();
    assert_eq!(inline.len(&w).unwrap(), 0);
    w.commit().unwrap();
    assert_clean(dir.path());

    let r = env.read_txn().unwrap();
    assert_eq!(inline.len(&r).unwrap(), 0);
    assert!(env.open_database(&r, Some(b"doomed")).unwrap().is_none());
    let got: Vec<Vec<u8>> = contents(&r, kept).into_iter().map(|(k, _)| k).collect();
    assert_eq!(got, kept_keys);
}

/// `put_reserved` (inline and overflow arms) and `create_database` inside a
/// txn that has spilled.
#[test]
fn reserve_and_create_in_a_spilled_txn() {
    let dir = TempDir::new();
    let env = open(dir.path(), Some(TINY), EnvFlags::NO_SYNC);
    let mut w = env.write_txn().unwrap();
    let base = env.create_database(&mut w, Some(b"base")).unwrap();
    let keys = fill_until_spilled(base, &mut w, "k");
    let late = env.create_database(&mut w, Some(b"late")).unwrap();
    for (i, len) in [(0u32, 200usize), (1, 9_000), (2, 70_000), (3, 5)].iter() {
        let key = format!("res{i}").into_bytes();
        let fill = b'a' + *i as u8;
        base.put_reserved(&mut w, &key, *len, |buf| buf.fill(fill))
            .unwrap();
        late.put_reserved(&mut w, &key, *len, |buf| buf.fill(fill))
            .unwrap();
    }
    // Rewrite some spilled leaves after the reserves.
    for k in keys.iter().step_by(97) {
        base.put(&mut w, k, b"rewritten").unwrap();
    }
    w.commit().unwrap();
    assert_clean(dir.path());

    let r = env.read_txn().unwrap();
    for (i, len) in [(0u32, 200usize), (1, 9_000), (2, 70_000), (3, 5)].iter() {
        let key = format!("res{i}").into_bytes();
        let want = vec![b'a' + *i as u8; *len];
        assert_eq!(base.get(&r, &key).unwrap(), Some(want.as_slice()));
        assert_eq!(late.get(&r, &key).unwrap(), Some(want.as_slice()));
    }
    for (j, k) in keys.iter().enumerate() {
        let want: &[u8] = if j % 97 == 0 {
            b"rewritten"
        } else {
            &[b'i'; 300]
        };
        assert_eq!(base.get(&r, k).unwrap(), Some(want));
    }
}
