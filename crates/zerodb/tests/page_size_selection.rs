//! Milestone 2.6 — `EnvOpenOptions::page_size` as a supported public knob.
//!
//! This is a **ZeroDB extension**: LMDB 0.9 derives its page size from the OS
//! and exposes no selector, so there is no cross-engine differential to run
//! (you cannot ask LMDB for a 64 K page and compare). Per the Phase 2
//! acceptance line, this is therefore the "doc + unit tests where it's
//! zerodb-defined" case, and the strongest available property is
//! **zerodb-vs-zerodb self-consistency**: identical logical content from
//! identical operation streams at every supported page size.
//!
//! Coverage:
//!   * validation + error taxonomy for every rejected value;
//!   * full write / read / delete / GC-reclaim / reopen round-trip at each of
//!     4 K, 8 K, 16 K, 32 K, 64 K, with `check_image` (the invariant walker)
//!     run at the correct geometry every time;
//!   * cross-page-size equivalence of an identical op stream, including
//!     entries with keys and values that straddle each geometry's overflow
//!     threshold;
//!   * the reopen contract: the persisted page size wins, silently.
//!
//! Do not weaken these (CLAUDE.md rule 2).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{check, free_page_count, Env, EnvOpenOptions, Error};

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
        let path = std::env::temp_dir().join(format!("zerodb-psize-{pid}-{nanos}-{seq}"));
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

/// Every supported page size (SPEC 02 §0).
const PSIZES: [u32; 5] = [4096, 8192, 16384, 32768, 65536];

const MAP: usize = 64 << 20;

fn open_at(dir: &Path, psize: u32) -> Env {
    let mut o = EnvOpenOptions::new();
    o.map_size(MAP);
    o.max_dbs(16);
    o.page_size(psize);
    o.open(dir).expect("open env")
}

fn assert_clean(dir: &Path, psize: u32) {
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = check::check_image(&bytes, psize);
    assert!(
        v.is_empty(),
        "invariant violations at psize={psize}: {v:#?}"
    );
}

// ---------------------------------------------------------------------------
// Validation / error taxonomy
// ---------------------------------------------------------------------------

#[test]
fn every_supported_page_size_opens() {
    for psize in PSIZES {
        let dir = TempDir::new();
        let env = open_at(dir.path(), psize);
        assert_eq!(env.page_size(), psize, "effective page size");
        assert_eq!(env.stat().page_size, psize, "stat().page_size agrees");
    }
}

#[test]
fn invalid_page_sizes_are_rejected_as_io_invalid_input() {
    // Below the minimum, above the maximum, and non-powers-of-two in range.
    let rejected: [u32; 10] = [0, 1, 512, 2048, 4095, 6144, 12288, 65535, 65537, 131_072];
    for psize in rejected {
        let dir = TempDir::new();
        let mut o = EnvOpenOptions::new();
        o.map_size(MAP);
        o.page_size(psize);
        match o.open(dir.path()) {
            Err(Error::Io(e)) => {
                assert_eq!(
                    e.kind(),
                    std::io::ErrorKind::InvalidInput,
                    "page_size={psize} must be InvalidInput, got {e:?}"
                );
                let msg = e.to_string();
                assert!(
                    msg.contains("page_size") && msg.contains(&psize.to_string()),
                    "error message should name the field and the value: {msg}"
                );
            }
            other => panic!("page_size={psize} should have been rejected, got {other:?}"),
        }
        // A rejected open must not have created a store.
        assert!(
            !dir.path().join(zerodb::DATA_FILE_NAME).exists(),
            "rejected open at page_size={psize} left a data file behind"
        );
    }
}

#[test]
fn the_boundary_values_are_accepted() {
    for psize in [zerodb::MIN_PAGE_SIZE, zerodb::MAX_PAGE_SIZE] {
        let dir = TempDir::new();
        assert_eq!(open_at(dir.path(), psize).page_size(), psize);
    }
}

#[test]
fn the_default_is_used_when_unset_and_the_getter_reports_it() {
    let o = EnvOpenOptions::new();
    assert_eq!(o.get_page_size(), zerodb::DEFAULT_PAGE_SIZE);
    let mut o = EnvOpenOptions::new();
    o.map_size(MAP);
    let dir = TempDir::new();
    let env = o.open(dir.path()).unwrap();
    assert_eq!(env.page_size(), zerodb::DEFAULT_PAGE_SIZE);

    let mut o = EnvOpenOptions::new();
    o.page_size(32768);
    assert_eq!(o.get_page_size(), 32768, "getter reflects the setter");
}

// ---------------------------------------------------------------------------
// Reopen contract
// ---------------------------------------------------------------------------

#[test]
fn reopening_adopts_the_persisted_page_size_and_ignores_the_request() {
    // Documented behavior: the persisted geometry is authoritative; a mismatched
    // request is silently ignored, exactly as for map_size. There is deliberately
    // no "wrong expectation" error — but the data must still be readable, which
    // is the property that actually matters.
    for created in PSIZES {
        let dir = TempDir::new();
        {
            let env = open_at(dir.path(), created);
            let mut w = env.write_txn().unwrap();
            let db = env.create_database(&mut w, Some(b"d")).unwrap();
            for i in 0..500u32 {
                db.put(&mut w, format!("k{i:06}").as_bytes(), b"value")
                    .unwrap();
            }
            w.commit().unwrap();
        }
        // Reopen requesting every *other* page size.
        for requested in PSIZES {
            let env = open_at(dir.path(), requested);
            assert_eq!(
                env.page_size(),
                created,
                "created at {created}, requested {requested}: the persisted \
                 page size must win"
            );
            let r = env.read_txn().unwrap();
            let db = env.open_database(&r, Some(b"d")).unwrap().unwrap();
            assert_eq!(db.len(&r).unwrap(), 500);
            assert_eq!(db.get(&r, b"k000250").unwrap(), Some(b"value".as_slice()));
        }
        assert_clean(dir.path(), created);
    }
}

// ---------------------------------------------------------------------------
// Full round-trip at each page size
// ---------------------------------------------------------------------------

/// A deterministic op stream sized to straddle every geometry's boundaries:
/// tiny values, values near a 4 K page, and values far past a 64 K page (so
/// overflow runs are exercised at *every* psize, with different run lengths).
fn workload() -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..900u32)
        .map(|i| {
            let key = format!("key-{i:06}").into_bytes();
            let vlen = match i % 9 {
                0 => 0,
                1 => 7,
                2 => 100,
                3 => 2000,
                4 => 4000,
                5 => 5000,
                6 => 40000,
                7 => 70000,
                _ => 200_000,
            };
            let val: Vec<u8> = (0..vlen).map(|j| (i as u8).wrapping_add(j as u8)).collect();
            (key, val)
        })
        .collect()
}

/// Run the full lifecycle at `psize` and return the final logical contents.
fn round_trip(psize: u32) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let dir = TempDir::new();
    let items = workload();

    // 1. Bulk insert.
    {
        let env = open_at(dir.path(), psize);
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, Some(b"d")).unwrap();
        for (k, v) in &items {
            db.put(&mut w, k, v).unwrap();
        }
        w.commit().unwrap();
        assert_eq!(env.stat().page_size, psize);
    }
    assert_clean(dir.path(), psize);

    // 2. Delete a third, then overwrite another third with different sizes —
    // this frees pages (incl. overflow runs) and exercises GC at this geometry.
    {
        let env = open_at(dir.path(), psize);
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, Some(b"d")).unwrap();
        for (i, (k, _)) in items.iter().enumerate() {
            if i % 3 == 0 {
                assert!(
                    db.delete(&mut w, k).unwrap(),
                    "delete {k:?} at psize={psize}"
                );
            } else if i % 3 == 1 {
                db.put(&mut w, k, b"overwritten").unwrap();
            }
        }
        w.commit().unwrap();
    }
    assert_clean(dir.path(), psize);

    // 3. GC must have reclaimed something at every geometry.
    {
        let env = open_at(dir.path(), psize);
        let r = env.read_txn().unwrap();
        let free = free_page_count(&r).unwrap();
        assert!(
            free > 0,
            "GC reclaimed nothing at psize={psize} (freed pages = 0)"
        );
    }

    // 4. Reinsert the deleted third, reusing freed pages.
    {
        let env = open_at(dir.path(), psize);
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, Some(b"d")).unwrap();
        for (i, (k, v)) in items.iter().enumerate() {
            if i % 3 == 0 {
                db.put(&mut w, k, v).unwrap();
            }
        }
        w.commit().unwrap();
    }
    assert_clean(dir.path(), psize);

    // 5. Reopen from scratch and read the whole thing back.
    let env = open_at(dir.path(), psize);
    assert_eq!(env.page_size(), psize);
    let r = env.read_txn().unwrap();
    let db = env.open_database(&r, Some(b"d")).unwrap().unwrap();
    let mut out = BTreeMap::new();
    for entry in db.iter(&r) {
        let (k, v) = entry.unwrap();
        out.insert(k.to_vec(), v.to_vec());
    }
    assert_eq!(
        out.len() as u64,
        db.len(&r).unwrap(),
        "iteration count != len() at psize={psize}"
    );
    out
}

#[test]
fn full_round_trip_at_4k() {
    assert!(!round_trip(4096).is_empty());
}

#[test]
fn full_round_trip_at_8k() {
    assert!(!round_trip(8192).is_empty());
}

#[test]
fn full_round_trip_at_64k() {
    assert!(!round_trip(65536).is_empty());
}

// ---------------------------------------------------------------------------
// The cross-page-size equivalence property
// ---------------------------------------------------------------------------

#[test]
fn identical_op_streams_produce_identical_content_at_every_page_size() {
    // The strong new property this milestone buys: page size is a *storage*
    // parameter with no logical effect. The same op stream must yield
    // byte-identical logical content — same keys, same values, same order —
    // at 4 K through 64 K, despite completely different page layouts,
    // split points, overflow thresholds and GC behavior.
    let baseline = round_trip(PSIZES[0]);
    assert!(!baseline.is_empty(), "the workload must produce entries");
    for psize in &PSIZES[1..] {
        let other = round_trip(*psize);
        assert_eq!(
            baseline.len(),
            other.len(),
            "entry count differs between psize={} and psize={psize}",
            PSIZES[0]
        );
        // Compare key-by-key so a failure names the offending key rather than
        // dumping two multi-megabyte maps.
        for ((bk, bv), (ok, ov)) in baseline.iter().zip(other.iter()) {
            assert_eq!(
                bk, ok,
                "key order diverges at psize={psize}: {bk:?} vs {ok:?}"
            );
            assert_eq!(
                bv.len(),
                ov.len(),
                "value length for {bk:?} differs at psize={psize}"
            );
            assert_eq!(bv, ov, "value bytes for {bk:?} differ at psize={psize}");
        }
    }
}

#[test]
fn page_size_changes_the_page_accounting_but_not_the_entry_count() {
    // Sanity that the page sizes are really doing different things — otherwise
    // the equivalence property above would be vacuous.
    let mut leaf_pages = Vec::new();
    let mut entries = Vec::new();
    for psize in PSIZES {
        let dir = TempDir::new();
        let env = open_at(dir.path(), psize);
        {
            let mut w = env.write_txn().unwrap();
            let db = env.create_database(&mut w, Some(b"d")).unwrap();
            for i in 0..3000u32 {
                db.put(&mut w, format!("k{i:08}").as_bytes(), &[b'v'; 48])
                    .unwrap();
            }
            w.commit().unwrap();
        }
        let r = env.read_txn().unwrap();
        let s = env.open_database(&r, Some(b"d")).unwrap().unwrap();
        let st = s.stat(&r).unwrap();
        entries.push(st.entries);
        leaf_pages.push(st.leaf_pages);
        drop(r);
        drop(env);
        assert_clean(dir.path(), psize);
    }
    assert!(
        entries.iter().all(|&e| e == 3000),
        "entry count must be page-size-independent: {entries:?}"
    );
    assert!(
        leaf_pages[0] > *leaf_pages.last().unwrap(),
        "a 4 K env should need more leaf pages than a 64 K env: {leaf_pages:?}"
    );
    // Strictly decreasing: each doubling of the page size should reduce the
    // leaf count. (Not asserted as an exact ratio — headers and fill factor
    // make it approximate.)
    for w in leaf_pages.windows(2) {
        assert!(
            w[0] > w[1],
            "leaf pages should shrink as the page size grows: {leaf_pages:?}"
        );
    }
}
