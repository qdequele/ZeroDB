//! Map growth on demand (ADR-0036): with `initial_map_size`, a write txn
//! that needs a page past the map grows the map (and the data file under
//! `WRITE_MAP`) instead of failing with `MapFull`, up to the `map_size`
//! ceiling.
//!
//! - growth is invisible to results: the same seeded workload, under a tiny
//!   dirty limit so pages spill past the old mapping mid-txn, gives the same
//!   contents and the same high-water page as a fixed map of the ceiling's
//!   size, in heap and in-place `WRITE_MAP` modes, before and after reopen;
//! - a read txn opened before the growths keeps its borrowed value;
//! - `MapFull` comes at the same point as with a fixed map of the ceiling.

mod common;
use common::{assert_clean, TempDir};

use std::collections::BTreeMap;
use std::path::Path;

use zerodb::{Database, Env, EnvFlags, EnvOpenOptions, MdbError};

const PS: u32 = 4096;
const CEILING: usize = 256 << 20;
const INITIAL: usize = 64 << 10;
/// The smallest dirty limit (SPEC 04 TXN-68), so large txns spill.
const TINY: usize = 128 * PS as usize;

fn open(dir: &Path, growth: bool, flags: EnvFlags) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(CEILING)
        .page_size(PS)
        .max_dbs(4)
        .max_dirty_bytes(TINY)
        .flags(flags);
    if growth {
        opts.initial_map_size(INITIAL);
    }
    // SAFETY: only ZeroDB writes this environment's files.
    unsafe { opts.open(dir) }.expect("open env")
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

fn contents(txn: &impl zerodb::TxnRead, db: Database) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.iter(txn)
        .map(|e| {
            let (k, v) = e.unwrap();
            (k.to_vec(), v.to_vec())
        })
        .collect()
}

/// One large txn of puts (some with overflow values) and deletes, with
/// reads checked against the model as it goes, so pages spilled past the
/// old mapping are read back mid-txn.
fn big_txn(env: &Env, db: Database, model: &mut BTreeMap<Vec<u8>, Vec<u8>>, seed: u64) {
    let mut rng = Rng(seed);
    let mut w = env.write_txn().unwrap();
    for step in 0..8_000u64 {
        let k = format!("key-{:06}", rng.below(50_000)).into_bytes();
        match rng.below(10) {
            0..=6 => {
                let len = if rng.below(12) == 0 {
                    4_500 + rng.below(9_000) as usize
                } else {
                    60 + rng.below(500) as usize
                };
                let v: Vec<u8> = format!("{seed}.{step}-")
                    .bytes()
                    .cycle()
                    .take(len)
                    .collect();
                db.put(&mut w, &k, &v).unwrap();
                model.insert(k, v);
            }
            7 => {
                let had = db.delete(&mut w, &k).unwrap();
                assert_eq!(had, model.remove(&k).is_some());
            }
            _ => assert_eq!(
                db.get(&w, &k).unwrap(),
                model.get(&k).map(Vec::as_slice),
                "writer read, step {step}"
            ),
        }
    }
    assert!(w.spilled_pgnos() > 0, "the tiny dirty limit must spill");
    w.commit().unwrap();
}

fn differential(flags: EnvFlags) {
    let grow_dir = TempDir::new();
    let fixed_dir = TempDir::new();
    let grow = open(grow_dir.path(), true, flags);
    let fixed = open(fixed_dir.path(), false, flags);
    let (gdb, fdb) = (grow.main_database(), fixed.main_database());
    assert_eq!(
        grow.info().map_size,
        CEILING as u64,
        "info reports the ceiling"
    );
    let first_map = grow.backing_bytes().len();
    assert!(
        first_map < CEILING / 16,
        "the initial map is small: {first_map}"
    );

    // A value borrowed before any growth must stay valid across all of them.
    let mut w = grow.write_txn().unwrap();
    gdb.put(&mut w, b"pinned", b"before growth").unwrap();
    w.commit().unwrap();
    let pin_txn = grow.read_txn().unwrap();
    let pinned = gdb.get(&pin_txn, b"pinned").unwrap().unwrap();
    let mut w = fixed.write_txn().unwrap();
    fdb.put(&mut w, b"pinned", b"before growth").unwrap();
    w.commit().unwrap();
    // The same pinned snapshot, so both envs reuse the same freed pages.
    let fixed_pin = fixed.read_txn().unwrap();

    let (mut gmodel, mut fmodel) = (BTreeMap::new(), BTreeMap::new());
    for round in 0..3u64 {
        let seed = 0x6_0000 + round;
        big_txn(&grow, gdb, &mut gmodel, seed);
        big_txn(&fixed, fdb, &mut fmodel, seed);
        assert_clean(grow_dir.path(), PS);
        assert_clean(fixed_dir.path(), PS);
        assert_eq!(
            grow.info().last_pgno,
            fixed.info().last_pgno,
            "growth changes no allocation, round {round}"
        );
    }
    assert_eq!(pinned, b"before growth", "a borrow from before the growths");
    drop((pin_txn, fixed_pin));
    let grown_map = grow.backing_bytes().len();
    assert!(
        grown_map > first_map,
        "the map grew: {first_map} -> {grown_map}"
    );
    assert!(
        grown_map < CEILING,
        "the map stayed below the ceiling: {grown_map}"
    );
    if flags.contains(EnvFlags::WRITE_MAP) {
        let len = grow.real_disk_size().unwrap();
        assert!(
            len < CEILING as u64 / 4,
            "the file grew by steps, not to the ceiling: {len}"
        );
        assert_eq!(fixed.real_disk_size().unwrap(), CEILING as u64);
    }
    let (rg, rf) = (grow.read_txn().unwrap(), fixed.read_txn().unwrap());
    assert_eq!(contents(&rg, gdb), contents(&rf, fdb));
    let want = contents(&rf, fdb);
    drop((rg, rf));

    // Reopen with and without the option: both read the grown file.
    drop(grow);
    for growth in [true, false] {
        let env = open(grow_dir.path(), growth, flags);
        let r = env.read_txn().unwrap();
        assert_eq!(
            contents(&r, env.main_database()),
            want,
            "reopen, growth {growth}"
        );
    }
}

#[test]
fn growth_is_invisible_to_results() {
    differential(EnvFlags::NO_SYNC);
}

#[test]
fn growth_is_invisible_to_results_under_write_map() {
    differential(EnvFlags::NO_SYNC | EnvFlags::WRITE_MAP);
}

#[test]
fn growth_is_invisible_to_results_with_durable_commits() {
    differential(EnvFlags::WRITE_MAP);
}

/// Commits of 1 KiB values until `MapFull`: a growing map of a 2 MiB
/// ceiling fails at the same put as a fixed 2 MiB map, and the failed txn
/// leaves the previous commit intact.
fn map_full_at_the_ceiling(flags: EnvFlags) {
    let full_at = |growth: bool| {
        let dir = TempDir::new();
        let mut opts = EnvOpenOptions::new();
        opts.map_size(2 << 20).page_size(PS).flags(flags);
        if growth {
            opts.initial_map_size(INITIAL);
        }
        // SAFETY: only ZeroDB writes this environment's files.
        let env = unsafe { opts.open(dir.path()) }.unwrap();
        let db = env.main_database();
        let mut committed = 0u32;
        loop {
            let mut w = env.write_txn().unwrap();
            let k = format!("k{committed:06}");
            match db.put(&mut w, k.as_bytes(), &[7u8; 1024]) {
                Ok(()) => {
                    w.commit().unwrap();
                    committed += 1;
                }
                Err(zerodb::Error::Mdb(MdbError::MapFull)) => break,
                Err(e) => panic!("put {committed}: {e:?}"),
            }
        }
        let r = env.read_txn().unwrap();
        assert_eq!(db.len(&r).unwrap(), u64::from(committed));
        (committed, env.info().last_pgno)
    };
    assert_eq!(full_at(true), full_at(false));
}

#[test]
fn map_full_comes_at_the_ceiling() {
    map_full_at_the_ceiling(EnvFlags::NO_SYNC);
}

#[test]
fn map_full_comes_at_the_ceiling_under_write_map() {
    map_full_at_the_ceiling(EnvFlags::NO_SYNC | EnvFlags::WRITE_MAP);
}

// ---------------------------------------------------------------------------
// Two processes (ADR-0023 lock file): the test binary re-runs itself as the
// second process through `child_entry`.
// ---------------------------------------------------------------------------

const ROLE: &str = "ZERODB_GROWTH_ROLE";
const DIR: &str = "ZERODB_GROWTH_DIR";
const DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);

/// A child process, killed and reaped on drop so a failed test never leaves
/// it running.
struct ChildGuard(std::process::Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn child(role: &str, dir: &Path) -> ChildGuard {
    let c = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_entry", "--nocapture", "--test-threads=1"])
        .env(ROLE, role)
        .env(DIR, dir)
        .spawn()
        .expect("spawn child");
    ChildGuard(c)
}

fn wait_for(path: &Path) {
    let end = std::time::Instant::now() + DEADLINE;
    while !path.exists() {
        assert!(
            std::time::Instant::now() < end,
            "timed out waiting for {}",
            path.display()
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

fn wait_success(c: &mut ChildGuard) {
    let end = std::time::Instant::now() + DEADLINE;
    loop {
        if let Some(st) = c.0.try_wait().unwrap() {
            assert!(st.success(), "child failed: {st}");
            return;
        }
        assert!(
            std::time::Instant::now() < end,
            "timed out waiting for the child"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

fn mark(dir: &Path, name: &str) {
    std::fs::write(dir.join(name), b"").unwrap();
}

/// A lock-file env: `growth` gives it a 64 KiB initial map under the
/// 256 MiB ceiling, otherwise a fixed `map` bytes.
fn open_shared(dir: &Path, growth: bool, map: usize, flags: EnvFlags) -> Env {
    let mut opts = EnvOpenOptions::new();
    opts.map_size(map)
        .page_size(PS)
        .max_readers(16)
        .flags(flags);
    if growth {
        opts.initial_map_size(INITIAL);
    }
    // SAFETY: only ZeroDB writes this environment's files.
    unsafe { opts.open(dir) }.expect("open lock-file env")
}

/// About 4 MiB of 3 KiB values under keys `prefix00000..`, in one txn.
fn write_4mib(env: &Env, prefix: &str) {
    let db = env.main_database();
    let mut w = env.write_txn().unwrap();
    for i in 0..1_300u32 {
        db.put(
            &mut w,
            format!("{prefix}{i:05}").as_bytes(),
            &[i as u8; 3_000],
        )
        .unwrap();
    }
    w.commit().unwrap();
}

/// Child entry point: a no-op unless run by `child()`.
#[test]
fn child_entry() {
    let (Ok(role), Ok(dir)) = (std::env::var(ROLE), std::env::var(DIR)) else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    match role.as_str() {
        // A plain process with a fixed 256 KiB map, as an LMDB-parity
        // reader: its borrow survives the other process's growth, and it
        // follows the grown map with `resize(0)` after `MapResized`.
        "plain_reader" => {
            let env = open_shared(&dir, false, 256 << 10, EnvFlags::EMPTY);
            let db = env.main_database();
            let r = env.read_txn().unwrap();
            let pinned = db.get(&r, b"pinned").unwrap().unwrap();
            mark(&dir, "reader-ready");
            wait_for(&dir.join("grown"));
            assert_eq!(pinned, b"before growth");
            drop(r);
            match env.read_txn() {
                Err(zerodb::Error::Mdb(MdbError::MapResized)) => {}
                other => panic!("expected MapResized, got {:?}", other.map(|_| ())),
            }
            env.resize(0).unwrap();
            let r = env.read_txn().unwrap();
            assert_eq!(db.len(&r).unwrap(), 1 + 1_300);
        }
        // A process with map growth, opened before the other grew the map:
        // its borrow survives, and its read and write begins follow the
        // growth without `MapResized` (ADR-0036).
        "growing_reader" => {
            let env = open_shared(&dir, true, CEILING, EnvFlags::EMPTY);
            let db = env.main_database();
            let r = env.read_txn().unwrap();
            let pinned = db.get(&r, b"pinned").unwrap().unwrap();
            let first = env.backing_bytes().len();
            mark(&dir, "reader-ready");
            wait_for(&dir.join("grown"));
            assert_eq!(pinned, b"before growth");
            drop(r);
            let r = env.read_txn().expect("a read begin follows the growth");
            assert_eq!(db.len(&r).unwrap(), 1 + 1_300);
            drop(r);
            assert!(env.backing_bytes().len() > first, "this process's map grew");
            let mut w = env.write_txn().expect("a write begin follows the growth");
            db.put(&mut w, b"from-the-reader", b"v").unwrap();
            w.commit().unwrap();
        }
        // A growing `WRITE_MAP` process joining after the other grew the
        // file: it adopts the grown size at open, then grows the map and the
        // file further inside its own write txn.
        "growing_writer" => {
            let env = open_shared(&dir, true, CEILING, EnvFlags::WRITE_MAP);
            let before = env.real_disk_size().unwrap();
            write_4mib(&env, "b");
            assert!(
                env.real_disk_size().unwrap() > before,
                "the file grew again"
            );
            assert_eq!(env.info().map_size, CEILING as u64);
        }
        other => panic!("unknown role {other}"),
    }
}

#[test]
fn another_process_keeps_its_borrows_and_follows_the_growth() {
    let dir = TempDir::new();
    let env = open_shared(dir.path(), true, CEILING, EnvFlags::EMPTY);
    let mut w = env.write_txn().unwrap();
    env.main_database()
        .put(&mut w, b"pinned", b"before growth")
        .unwrap();
    w.commit().unwrap();
    let mut reader = child("plain_reader", dir.path());
    wait_for(&dir.path().join("reader-ready"));
    let first = env.backing_bytes().len();
    write_4mib(&env, "a");
    assert!(env.backing_bytes().len() > first, "the map grew");
    mark(dir.path(), "grown");
    wait_success(&mut reader);
}

#[test]
fn a_process_with_growth_follows_another_without_map_resized() {
    for flags in [EnvFlags::EMPTY, EnvFlags::WRITE_MAP] {
        let dir = TempDir::new();
        let env = open_shared(dir.path(), true, CEILING, flags);
        let mut w = env.write_txn().unwrap();
        env.main_database()
            .put(&mut w, b"pinned", b"before growth")
            .unwrap();
        w.commit().unwrap();
        let mut reader = child("growing_reader", dir.path());
        wait_for(&dir.path().join("reader-ready"));
        write_4mib(&env, "a");
        mark(dir.path(), "grown");
        wait_success(&mut reader);
        let r = env.read_txn().unwrap();
        assert_eq!(env.main_database().len(&r).unwrap(), 2 + 1_300);
        drop(r);
        assert_clean(dir.path(), PS);
    }
}

#[test]
fn a_write_map_process_grows_after_another_did() {
    let dir = TempDir::new();
    let env = open_shared(dir.path(), true, CEILING, EnvFlags::WRITE_MAP);
    write_4mib(&env, "a");
    let mut writer = child("growing_writer", dir.path());
    wait_success(&mut writer);
    // This process follows the other's commit past its own map and reads
    // both (ADR-0036).
    let r = env.read_txn().expect("a read begin follows the growth");
    assert_eq!(env.main_database().len(&r).unwrap(), 2 * 1_300);
    drop(r);
    assert_clean(dir.path(), PS);
}

/// `resize` under map growth (SPEC 02 §8): a non-zero size sets the ceiling
/// without mapping it, and `MapFull` then comes at the new ceiling; `0`
/// adopts the meta's size and never lowers the ceiling below it.
#[test]
fn resize_sets_the_ceiling_under_map_growth() {
    let dir = TempDir::new();
    let mut opts = EnvOpenOptions::new();
    opts.map_size(1 << 20)
        .initial_map_size(INITIAL)
        .page_size(PS)
        .flags(EnvFlags::NO_SYNC);
    // SAFETY: only ZeroDB writes this environment's files.
    let env = unsafe { opts.open(dir.path()) }.unwrap();
    let db = env.main_database();
    let fill = |env: &Env, prefix: &str| -> u32 {
        let mut n = 0;
        loop {
            let mut w = env.write_txn().unwrap();
            match db.put(&mut w, format!("{prefix}{n:06}").as_bytes(), &[1u8; 1024]) {
                Ok(()) => w.commit().unwrap(),
                Err(zerodb::Error::Mdb(MdbError::MapFull)) => return n,
                Err(e) => panic!("put: {e:?}"),
            }
            n += 1;
        }
    };
    let first = fill(&env, "a");
    env.resize(4 << 20).unwrap();
    assert_eq!(env.info().map_size, 4 << 20, "the new ceiling");
    assert!(
        env.backing_bytes().len() < 4 << 20,
        "the ceiling is not mapped"
    );
    let second = fill(&env, "b");
    assert!(
        second > 2 * first,
        "the larger ceiling holds more: {first} then {second}"
    );
    env.resize(0).unwrap();
    assert_eq!(
        env.info().map_size,
        4 << 20,
        "resize(0) keeps a higher ceiling"
    );
}
