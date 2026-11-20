//! Apple silicon: 16 KiB OS pages. A database page can be smaller than an OS
//! page there, so one OS page holds several database pages, the ranged
//! `msync` of a `WRITE_MAP` commit (SPEC 06 REC-12) rounds its ranges out to
//! OS pages, and the map length is rounded to a 16 KiB multiple. These tests
//! pin that the macOS CI job runs on such a host, and that 4 KiB and 8 KiB
//! stores commit and read back on it, with and without `WRITE_MAP`.

#![cfg(all(target_os = "macos", target_arch = "aarch64"))]
// heed 0.22.1's API; the `unique-txns` feature replaces it (ADR-0027).
#![cfg(not(feature = "unique-txns"))]

use heed::types::Bytes;
use heed::{Database, EnvFlags, EnvOpenOptions, WithoutTls};

const OS_PAGE: usize = 16 * 1024;
const MAP: usize = 64 * OS_PAGE;

fn os_page() -> usize {
    // SAFETY: `sysconf` with a valid name is a pure query.
    let v = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(v).unwrap()
}

fn opts(page_size: u32, write_map: bool) -> EnvOpenOptions<WithoutTls> {
    let mut o = EnvOpenOptions::new().read_txn_without_tls();
    o.map_size(MAP).max_dbs(2).page_size(page_size);
    if write_map {
        // SAFETY: the flag only selects the writable-map backing; this test
        // owns the directory and opens the environment once at a time.
        unsafe { o.flags(EnvFlags::WRITE_MAP) };
    }
    o
}

fn key(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

fn value(i: u32, round: u32) -> Vec<u8> {
    // Large enough that a few hundred entries span many database pages.
    vec![(i ^ round) as u8; 200]
}

/// Commit `rounds` transactions, each rewriting a scattered subset of the
/// keys, so a commit's dirty pages fall in several ranges, some of which
/// share an OS page with pages it does not touch. Then reopen and compare.
fn round_trip(page_size: u32, write_map: bool) {
    const KEYS: u32 = 400;
    let rounds = 12;
    let dir = tempfile::tempdir().unwrap();
    let mut expect = vec![0u32; KEYS as usize];
    {
        // SAFETY: a fresh directory owned by this test.
        let env = unsafe { opts(page_size, write_map).open(dir.path()) }.unwrap();
        assert_eq!(env.info().map_size % OS_PAGE, 0);
        let mut wtxn = env.write_txn().unwrap();
        let db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, None).unwrap();
        for i in 0..KEYS {
            db.put(&mut wtxn, &key(i), &value(i, 0)).unwrap();
        }
        wtxn.commit().unwrap();
        for round in 1..=rounds {
            let mut wtxn = env.write_txn().unwrap();
            // A stride coprime to KEYS touches pages all over the tree.
            for i in (0..KEYS).filter(|i| (i * 7 + round) % 5 == 0) {
                db.put(&mut wtxn, &key(i), &value(i, round)).unwrap();
                expect[i as usize] = round;
            }
            wtxn.commit().unwrap();
        }
        env.force_sync().unwrap();
        assert_eq!(env.stat().page_size, page_size);
    }
    // SAFETY: as above; the previous environment is closed.
    let env = unsafe { opts(page_size, write_map).open(dir.path()) }.unwrap();
    assert_eq!(env.stat().page_size, page_size);
    let rtxn = env.read_txn().unwrap();
    let db: Database<Bytes, Bytes> = env.open_database(&rtxn, None).unwrap().unwrap();
    for i in 0..KEYS {
        let got = db.get(&rtxn, &key(i)).unwrap().expect("key present");
        assert_eq!(got, value(i, expect[i as usize]), "key {i}");
    }
}

/// The macOS CI job is meant to cover a host whose OS page is larger than
/// the 4 KiB default database page. If the runner image ever changes to a
/// 4 KiB-page host, this fails instead of silently losing that coverage.
#[test]
fn the_os_page_is_16_kib() {
    assert_eq!(os_page(), OS_PAGE);
}

#[test]
fn a_4_kib_store_round_trips() {
    round_trip(4096, false);
}

#[test]
fn a_4_kib_store_round_trips_with_write_map() {
    round_trip(4096, true);
}

#[test]
fn an_8_kib_store_round_trips_with_write_map() {
    round_trip(8192, true);
}

/// Without a page size, a store is created at the OS page size (LMDB
/// parity, SPEC 00): 16 KiB here.
#[test]
fn the_default_store_page_is_16_kib() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = EnvOpenOptions::new().read_txn_without_tls();
    o.map_size(MAP);
    // SAFETY: a fresh directory owned by this test.
    let env = unsafe { o.open(dir.path()) }.unwrap();
    assert_eq!(env.stat().page_size as usize, OS_PAGE);
}
