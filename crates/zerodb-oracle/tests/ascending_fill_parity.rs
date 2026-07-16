//! Parity of leaf-page fill on ascending plain-put workloads (SPEC 03 §6.4
//! end-of-page insert-point rule, ADR-0005 D5, ratified 2026-07-16).
//!
//! milli's dominant write pattern is ascending keys (facet ids, docids, sorted
//! bulk). Before the amendment, zerodb's §6.4 median-fit split left a cascade
//! of ~50 %-full leaves on such loads — ~2× the fork's leaf count (ADR-0005 D5:
//! 400 ascending ~500 B puts → 132 leaves zerodb vs ~68 fork at 4 KiB pages).
//! The ratified end-of-page insert-point rule (all existing cells stay left,
//! the new key alone starts the right page for any `newindx == nkeys` insert,
//! matching `mdb_page_split`) closes that gap. This test drives identical
//! ascending plain puts (NOT `MDB_APPEND` — the whole point is plain puts now
//! pack too) into the fork (via heed) and zerodb and asserts the leaf counts
//! track within a tight ±20 % band.
//!
//! **Page size.** LMDB fixes its page size to the OS page at env creation and
//! offers no runtime override (4 KiB on Linux x86, 16 KiB on macOS ARM, up to
//! 64 KiB on some ARM distros). A raw leaf-count band is only meaningful at
//! equal page sizes, so the test **reads the fork's page size and opens zerodb
//! to match** (zerodb's page size is runtime-chosen, CLAUDE.md), keeping the
//! comparison valid on every platform instead of hard-coding 4 KiB.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use heed::types::Bytes;
use heed::{EnvOpenOptions as HeedOpts, WithoutTls};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(tag: &str) -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("zerodb-ascfill-{tag}-{pid}-{seq}"));
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

const MAP: usize = 256 << 20;

/// Ascending key/value pairs, engine-agnostic. `wide` gives ~500 B keys (small
/// branch fanout, D5's exact shape); otherwise compact 8-byte keys.
fn make_pairs(n: u32, wide: bool, vlen: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..n)
        .map(|i| {
            let mut k = format!("k{i:08}").into_bytes();
            if wide {
                k.resize(500, b'.');
            }
            (k, vec![0x5Au8; vlen])
        })
        .collect()
}

/// Leaf-page count + page size for the fork after inserting `pairs` in order.
fn lmdb_stat(dir: &Path, pairs: &[(Vec<u8>, Vec<u8>)]) -> (u64, u32) {
    let mut opts = HeedOpts::new().read_txn_without_tls();
    opts.map_size(MAP);
    // SAFETY: no cross-process env flags; private temp dir, single-threaded.
    let env: heed::Env<WithoutTls> = unsafe { opts.open(dir) }.expect("open lmdb env");
    let mut wtxn = env.write_txn().expect("wtxn");
    let db: heed::Database<Bytes, Bytes> = env
        .create_database(&mut wtxn, None)
        .expect("create unnamed db");
    for (k, v) in pairs {
        db.put(&mut wtxn, k, v).expect("put");
    }
    wtxn.commit().expect("commit");
    let rtxn = env.read_txn().expect("rtxn");
    let st = db.stat(&rtxn).expect("stat");
    (st.leaf_pages as u64, st.page_size)
}

/// Leaf-page count for zerodb (at page size `ps`) after the same puts.
fn zerodb_leaf_pages(dir: &Path, ps: u32, pairs: &[(Vec<u8>, Vec<u8>)]) -> u64 {
    let mut opts = zerodb::EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(ps);
    let env = opts.open(dir).expect("open zerodb env");
    let db = env.main_database();
    let mut wtxn = env.write_txn().expect("wtxn");
    for (k, v) in pairs {
        db.put(&mut wtxn, k, v).expect("put");
    }
    let leaves = zerodb::TxnRead::main_record(&wtxn).leaf_pages;
    wtxn.commit().expect("commit");
    // The committed image is structurally sound (SPEC 03 §11).
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = zerodb::check::check_image(&bytes, ps);
    assert!(v.is_empty(), "invariant violations: {v:#?}");
    leaves
}

/// Assert zerodb's leaf count is within `tol` (fractional) of the fork's — both
/// at the fork's page size — and print both so the measurement is visible under
/// `cargo test -- --nocapture`.
fn assert_leaf_parity(tag: &str, n: u32, wide: bool, vlen: usize, tol: f64) {
    let pairs = make_pairs(n, wide, vlen);
    let ldir = TempDir::new(&format!("l-{tag}"));
    let zdir = TempDir::new(&format!("z-{tag}"));
    let (lmdb, ps) = lmdb_stat(ldir.path(), &pairs);
    let zdb = zerodb_leaf_pages(zdir.path(), ps, &pairs);
    let ratio = zdb as f64 / lmdb as f64;
    println!(
        "[{tag}] n={n} wide={wide} vlen={vlen} psize={ps}: lmdb_leaves={lmdb} \
         zerodb_leaves={zdb} ratio={ratio:.3} (band [{:.2}, {:.2}])",
        1.0 - tol,
        1.0 + tol
    );
    assert!(
        ratio >= 1.0 - tol && ratio <= 1.0 + tol,
        "[{tag}] leaf-page ratio {ratio:.3} outside +/-{tol} band \
         (zerodb {zdb} leaves vs fork {lmdb} at psize {ps}); the §6.4 end-of-page \
         rule must pack ascending plain puts like the fork (ADR-0005 D5)"
    );
}

/// D5's exact shape: 400 ascending ~500 B keys, tiny values. Pre-amendment this
/// was ~1.9× the fork's leaf count (median split → ~50 %-full leaves); the
/// ratified rule must bring it into a tight band around 1.0.
#[test]
fn ascending_wide_keys_400() {
    assert_leaf_parity("wide400", 400, true, 4, 0.20);
}

/// Deeper compact-key tree (8-byte keys, 50 B values): stresses leaf fill
/// across a multi-level tree the median rule would have halved.
#[test]
fn ascending_compact_keys_20k() {
    assert_leaf_parity("compact20k", 20_000, false, 50, 0.20);
}
