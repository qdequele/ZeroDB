//! The database page size against the OS page size (SPEC 02 §0: `psize` is
//! independent of the OS page). Every database page size in {4, 16, 64} KiB
//! runs on whatever kernel executes the suite, so each host covers the cases
//! its own OS page size makes possible:
//!
//! | OS page | DB page smaller | DB page equal | DB page larger |
//! |---|---|---|---|
//! | 4 KiB (x86-64, most aarch64 CI) | — | 4 K | 16 K, 64 K |
//! | 16 KiB (Apple silicon, some aarch64 Linux) | 4 K | 16 K | 64 K |
//! | 64 KiB (RHEL for ARM, some aarch64 images) | 4 K, 16 K | 64 K | — |
//!
//! The paths that round to the OS page are the ones exercised here: the
//! read-only and writable maps over a file whose length is not an OS-page
//! multiple (pages read and written in the last, partly backed OS page), the
//! ranged `msync` of sub-OS-page ranges at offsets that are not OS-page
//! aligned (SPEC 06 REC-12), `madvise(MADV_RANDOM)` under `NO_READ_AHEAD`,
//! map growth to sizes that are not OS-page multiples, and a store holding
//! only its two meta pages (shorter than one OS page when `2 * psize` is).
//!
//! The OS page size is read with `getconf PAGESIZE` and printed, so a test log
//! records which row of the table above it covers. `scripts/page-size-matrix.sh`
//! runs the whole suite on such a host; docs/maintainers/HARDENING-RUNS.md says
//! which machines to use.

mod common;
use common::{assert_clean, TempDir};

use std::collections::BTreeMap;
use std::path::Path;

use zerodb::{Database, Env, EnvFlags, EnvOpenOptions};

/// The database page sizes this file runs, one per row of the table above.
const PSIZES: [u32; 3] = [4096, 16384, 65536];

/// The backings and flags whose code paths touch the OS page: the default
/// `pwrite` backing, the writable map with its ranged `msync`, both under
/// `madvise(MADV_RANDOM)`, and the asynchronous flush.
const MODES: [(&str, EnvFlags); 5] = [
    ("default", EnvFlags::EMPTY),
    ("no-read-ahead", EnvFlags::NO_READ_AHEAD),
    ("write-map", EnvFlags::WRITE_MAP),
    (
        "write-map+no-read-ahead",
        EnvFlags::from_bits_truncate(EnvFlags::WRITE_MAP.bits() | EnvFlags::NO_READ_AHEAD.bits()),
    ),
    (
        "write-map+map-async",
        EnvFlags::from_bits_truncate(EnvFlags::WRITE_MAP.bits() | EnvFlags::MAP_ASYNC.bits()),
    ),
];

/// The OS page size, or `None` when `getconf` is unavailable.
fn os_page_size() -> Option<u64> {
    let out = std::process::Command::new("getconf")
        .arg("PAGESIZE")
        .output()
        .ok()?;
    String::from_utf8(out.stdout).ok()?.trim().parse().ok()
}

/// A map size that is a multiple of `psize` and of no larger power of two, so
/// it is not a multiple of any OS page larger than `psize`.
fn odd_map_size(psize: u32, pages: u64) -> usize {
    let pages = pages | 1;
    usize::try_from(pages * u64::from(psize)).expect("map size fits usize")
}

fn open(dir: &Path, psize: u32, map_size: usize, flags: EnvFlags) -> Env {
    let mut o = EnvOpenOptions::new();
    o.map_size(map_size)
        .page_size(psize)
        .max_dbs(4)
        .flags(flags);
    // SAFETY: only ZeroDB writes this environment's files.
    unsafe { o.open(dir) }.expect("open env")
}

fn data_len(dir: &Path) -> u64 {
    std::fs::metadata(dir.join(zerodb::DATA_FILE_NAME))
        .expect("stat data file")
        .len()
}

fn contents(env: &Env, db: Database) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let r = env.read_txn().expect("read txn");
    db.iter(&r)
        .map(|e| {
            let (k, v) = e.expect("iterate");
            (k.to_vec(), v.to_vec())
        })
        .collect()
}

/// A value of `len` bytes that names its key and commit, so a page read from
/// the wrong offset shows up as a mismatch rather than a coincidence.
fn value(key: u64, commit: u64, len: usize) -> Vec<u8> {
    format!("{key}:{commit}|")
        .bytes()
        .cycle()
        .take(len)
        .collect()
}

/// Commits that each add one DB page or so of data, a few overflow runs whose
/// last page ends inside an OS page, and rewrites of earlier keys (pages freed
/// and reused). Returns the model and every data-file length seen after a
/// commit.
fn grow_one_page_at_a_time(
    env: &Env,
    db: Database,
    psize: u32,
    commits: u64,
    model: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    dir: &Path,
) -> Vec<u64> {
    let ps = psize as usize;
    let mut lengths = Vec::new();
    for c in 0..commits {
        let mut w = env.write_txn().expect("write txn");
        let base = model.len() as u64;
        // Inline values: three per leaf, so the leaf level grows by about one
        // page per commit.
        for i in 0..3 {
            let k = base + i;
            let v = value(k, c, ps / 4);
            db.put(&mut w, format!("k{k:08}").as_bytes(), &v)
                .expect("put");
            model.insert(format!("k{k:08}").into_bytes(), v);
        }
        // An overflow run of 1.5 to 3.5 DB pages: its tail ends mid-OS-page
        // whenever the OS page is larger than the DB page.
        if c % 4 == 1 {
            let k = 1_000_000 + c;
            let v = value(k, c, ps + ps / 2 + (c as usize % 3) * ps);
            db.put(&mut w, format!("o{k:08}").as_bytes(), &v)
                .expect("put overflow");
            model.insert(format!("o{k:08}").into_bytes(), v);
        }
        // Rewrite an older key, so freed pages come back from the GC.
        if c > 4 {
            let k = (c * 7) % base.max(1);
            let key = format!("k{k:08}").into_bytes();
            let v = value(k, c, ps / 5);
            db.put(&mut w, &key, &v).expect("rewrite");
            model.insert(key, v);
        }
        w.commit().expect("commit");
        lengths.push(data_len(dir));
    }
    lengths
}

/// Growth, rewrites, a resize to a size that is no OS-page multiple, then a
/// reopen in every mode: the contents match the model and the image passes
/// the invariant checker, at every DB page size and in every backing.
#[test]
fn growth_resize_and_reopen_in_every_backing() {
    let os = os_page_size();
    eprintln!("OS page size: {os:?}");
    for psize in PSIZES {
        for (name, flags) in MODES {
            let dir = TempDir::new();
            let map = odd_map_size(psize, 257);
            let mut model = BTreeMap::new();
            let lengths = {
                let env = open(dir.path(), psize, map, flags);
                assert_eq!(env.page_size(), psize, "{name} psize={psize}");
                let db = env.main_database();
                let mut lengths =
                    grow_one_page_at_a_time(&env, db, psize, 24, &mut model, dir.path());
                // Grow the map to another size that is no OS-page multiple,
                // then keep writing past the old end.
                let bigger = odd_map_size(psize, 521) as u64;
                env.resize(bigger).expect("resize");
                assert_eq!(env.info().map_size, bigger, "{name} psize={psize}");
                lengths.extend(grow_one_page_at_a_time(
                    &env,
                    db,
                    psize,
                    40,
                    &mut model,
                    dir.path(),
                ));
                assert_eq!(contents(&env, db), model, "{name} psize={psize} live");
                lengths
            };
            assert_clean(dir.path(), psize);
            // Every length is a whole number of DB pages.
            for len in &lengths {
                assert_eq!(len % u64::from(psize), 0, "{name} psize={psize}");
            }
            // When the OS page is larger than the DB page, the run reached
            // file lengths that end inside an OS page.
            if let Some(os) = os.filter(|&os| os > u64::from(psize)) {
                assert!(
                    lengths.iter().any(|len| len % os != 0),
                    "{name} psize={psize}: no file length ended mid-OS-page: {lengths:?}"
                );
            }
            // Reopen in every mode: the persisted page size and contents.
            for (reopen, rflags) in MODES {
                let env = open(dir.path(), psize, 0x1_0000, rflags);
                assert_eq!(env.page_size(), psize, "{name} → {reopen}");
                let db = env.main_database();
                assert_eq!(contents(&env, db), model, "{name} → {reopen} psize={psize}");
            }
        }
    }
}

/// Many one-key commits: under `WRITE_MAP` each commit's ranged `msync`
/// covers a few DB pages at offsets that are not OS-page aligned when the DB
/// page is smaller than the OS page. The values survive a reopen through the
/// `pwrite` backing, which reads them from the file, not the map.
#[test]
fn small_commits_flush_ranges_smaller_than_an_os_page() {
    for psize in PSIZES {
        for (name, flags) in MODES {
            let dir = TempDir::new();
            let mut model = BTreeMap::new();
            {
                let env = open(dir.path(), psize, odd_map_size(psize, 129), flags);
                let db = env.main_database();
                for c in 0..200u64 {
                    let mut w = env.write_txn().expect("write txn");
                    let k = (c * 37) % 61;
                    let key = format!("key{k:03}").into_bytes();
                    let v = value(k, c, 40 + (c as usize % 7) * 13);
                    db.put(&mut w, &key, &v).expect("put");
                    model.insert(key, v);
                    w.commit().expect("commit");
                }
                env.force_sync().expect("force_sync");
            }
            assert_clean(dir.path(), psize);
            let env = open(dir.path(), psize, 0x1_0000, EnvFlags::EMPTY);
            let db = env.main_database();
            assert_eq!(contents(&env, db), model, "{name} psize={psize}");
        }
    }
}

/// A store that holds only its two meta pages is `2 * psize` long, shorter
/// than one OS page when `2 * psize` is smaller than it: both meta slots sit in
/// one partly backed OS page. It reopens, takes writes, and reopens again, in
/// every mode.
#[test]
fn a_store_shorter_than_one_os_page_opens_and_grows() {
    for psize in PSIZES {
        for (name, flags) in MODES {
            let dir = TempDir::new();
            // Create and close without a write txn.
            drop(open(
                dir.path(),
                psize,
                odd_map_size(psize, 65),
                EnvFlags::EMPTY,
            ));
            assert_eq!(
                data_len(dir.path()),
                2 * u64::from(psize),
                "psize={psize}: a fresh store is its two meta pages"
            );
            {
                let env = open(dir.path(), psize, odd_map_size(psize, 65), flags);
                let db = env.main_database();
                assert!(contents(&env, db).is_empty(), "{name} psize={psize}");
                let mut w = env.write_txn().expect("write txn");
                db.put(&mut w, b"first", b"value").expect("put");
                w.commit().expect("commit");
            }
            assert_clean(dir.path(), psize);
            let env = open(dir.path(), psize, 0x1_0000, flags);
            let r = env.read_txn().expect("read txn");
            assert_eq!(
                env.main_database().get(&r, b"first").expect("get"),
                Some(&b"value"[..]),
                "{name} psize={psize}"
            );
        }
    }
}

/// A read txn opened before the file and the map grow keeps reading its
/// snapshot, and a new one sees the growth, across a resize to a size that is
/// no OS-page multiple (grow-only mappings, ADR-0023).
#[test]
fn a_reader_spans_growth_past_an_os_page_boundary() {
    for psize in PSIZES {
        for (name, flags) in MODES {
            let dir = TempDir::new();
            let env = open(dir.path(), psize, odd_map_size(psize, 33), flags);
            let db = env.main_database();
            let mut model = BTreeMap::new();
            grow_one_page_at_a_time(&env, db, psize, 6, &mut model, dir.path());
            let before = model.clone();
            let old = env.read_txn().expect("read txn");
            env.resize(odd_map_size(psize, 1025) as u64)
                .expect("resize");
            grow_one_page_at_a_time(&env, db, psize, 30, &mut model, dir.path());
            let seen: BTreeMap<_, _> = db
                .iter(&old)
                .map(|e| {
                    let (k, v) = e.expect("iterate");
                    (k.to_vec(), v.to_vec())
                })
                .collect();
            assert_eq!(seen, before, "{name} psize={psize}: old snapshot");
            drop(old);
            assert_eq!(
                contents(&env, db),
                model,
                "{name} psize={psize}: new snapshot"
            );
        }
    }
}
