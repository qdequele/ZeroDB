//! Parity of leaf-page fill on ascending plain-put workloads (SPEC 03 §6.4
//! end-of-page insert-point rule, ADR-0005 D5).
//!
//! milli's dominant write pattern is ascending keys (facet ids, docids, sorted
//! bulk). A median-fit split leaves ~50 %-full leaves on such loads, ~2× the
//! fork's leaf count. The end-of-page insert-point rule (all existing cells
//! stay left, the new key alone starts the right page for any
//! `newindx == nkeys` insert, matching `mdb_page_split`) packs like the fork.
//! This test drives identical ascending plain puts (NOT `MDB_APPEND` — plain
//! puts must pack too) into the fork (via heed) and zerodb and asserts the
//! leaf counts track within a ±20 % band.
//!
//! The same rule governs branch splits: every leaf split of an ascending load
//! inserts its separator at the end of the parent branch, and the fork splits
//! a full branch on such an end insert so that two children land on the new
//! right page. The `*_branch_parity` tests assert both engines' branch-page
//! counts and heights equal what that split rule predicts from each engine's
//! own leaf count and branch capacity.
//!
//! **Page size.** LMDB fixes its page size to the OS page at env creation and
//! offers no runtime override (4 KiB on Linux x86, 16 KiB on macOS ARM, up to
//! 64 KiB on some ARM distros). A raw leaf-count band is only meaningful at
//! equal page sizes, so the test **reads the fork's page size and opens zerodb
//! to match** (zerodb's page size is runtime-chosen), keeping the
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
    make_pairs_klen(n, if wide { 500 } else { 0 }, vlen)
}

/// Ascending key/value pairs whose keys are padded to at least `klen` bytes
/// (the unpadded key is 9 bytes, `k` plus an 8-digit index).
fn make_pairs_klen(n: u32, klen: usize, vlen: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..n)
        .map(|i| {
            let mut k = format!("k{i:08}").into_bytes();
            if k.len() < klen {
                k.resize(klen, b'.');
            }
            (k, vec![0x5Au8; vlen])
        })
        .collect()
}

/// Tree shape after a load: page counts and height.
#[derive(Debug, Clone, Copy)]
struct Shape {
    leaf_pages: u64,
    branch_pages: u64,
    depth: u32,
}

/// Tree shape + page size for the fork after inserting `pairs` in order.
fn lmdb_stat(dir: &Path, pairs: &[(Vec<u8>, Vec<u8>)]) -> (Shape, u32) {
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
    let shape = Shape {
        leaf_pages: st.leaf_pages as u64,
        branch_pages: st.branch_pages as u64,
        depth: st.depth,
    };
    (shape, st.page_size)
}

/// Tree shape for zerodb (at page size `ps`) after the same puts.
fn zerodb_shape(dir: &Path, ps: u32, pairs: &[(Vec<u8>, Vec<u8>)]) -> Shape {
    let mut opts = zerodb::EnvOpenOptions::new();
    opts.map_size(MAP);
    opts.page_size(ps);
    let env = opts.open(dir).expect("open zerodb env");
    let db = env.main_database();
    let mut wtxn = env.write_txn().expect("wtxn");
    for (k, v) in pairs {
        db.put(&mut wtxn, k, v).expect("put");
    }
    let rec = *zerodb::TxnRead::main_record(&wtxn);
    let shape = Shape {
        leaf_pages: rec.leaf_pages,
        branch_pages: rec.branch_pages,
        depth: u32::from(rec.depth),
    };
    wtxn.commit().expect("commit");
    // The committed image is structurally sound (SPEC 03 §11).
    let bytes = std::fs::read(dir.join(zerodb::DATA_FILE_NAME)).expect("read data file");
    let v = zerodb::check::check_image(&bytes, ps);
    assert!(v.is_empty(), "invariant violations: {v:#?}");
    shape
}

/// Assert zerodb's leaf count is within `tol` (fractional) of the fork's — both
/// at the fork's page size — and print both so the measurement is visible under
/// `cargo test -- --nocapture`.
fn assert_leaf_parity(tag: &str, n: u32, wide: bool, vlen: usize, tol: f64) {
    let pairs = make_pairs(n, wide, vlen);
    let ldir = TempDir::new(&format!("l-{tag}"));
    let zdir = TempDir::new(&format!("z-{tag}"));
    let (lmdb, ps) = lmdb_stat(ldir.path(), &pairs);
    let lmdb = lmdb.leaf_pages;
    let zdb = zerodb_shape(zdir.path(), ps, &pairs).leaf_pages;
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

/// D5's exact shape: 400 ascending ~500 B keys, tiny values. A median split
/// gives ~1.9× the fork's leaf count; the §6.4 rule must land near 1.0.
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

/// Children a full branch page of the fork holds when every separator key is
/// `ksize` bytes. Fork page format (`mdb.c`): a 16-byte page header, then per
/// node a 2-byte slot plus an 8-byte node header and the key, rounded up to
/// an even size; node 0 carries no key. The `*_branch_parity` tests check
/// this value against the fork's own `stat` (through the split model below),
/// so a wrong constant here fails the fork side of the assertion.
fn lmdb_branch_capacity(ps: u32, ksize: usize) -> u64 {
    let even = |x: usize| (x + 1) & !1;
    let body = ps as usize - 16;
    let node0 = even(8) + 2;
    let cell = even(8 + ksize) + 2;
    (1 + (body - node0) / cell) as u64
}

/// Children a full zerodb branch page holds when every separator key is
/// `ksize` bytes, measured by filling an empty branch page until it reports
/// full.
fn zerodb_branch_capacity(ps: u32, ksize: usize) -> u64 {
    let mut buf = vec![0u8; ps as usize];
    let mut br = zerodb_core::page::BranchMut::init(&mut buf, ps, 3, 1).expect("init branch");
    br.insert(0, &[], 1).expect("node 0");
    let mut n = 1usize;
    loop {
        let key = format!("{n:0ksize$}").into_bytes();
        if br.insert(n, &key, n as u64 + 1).is_err() {
            return n as u64;
        }
        n += 1;
    }
}

/// Branch pages and tree height an ascending load produces over `leaves`
/// leaves under the end-of-page branch split (SPEC 03 §6.4), for branches of
/// `cap` children. Every insert into a branch level lands at its end; a full
/// page (`cap` children) taking one more child splits into a left page of
/// `cap - 1` children and a right page of 2. So a level over `c > cap`
/// children has `1 + ceil((c - cap) / (cap - 1))` pages; the levels stack up
/// to a single root.
fn ascending_branch_model(leaves: u64, cap: u64) -> (u64, u32) {
    let (mut children, mut branches, mut depth) = (leaves, 0u64, 1u32);
    while children > 1 {
        let pages = if children <= cap {
            1
        } else {
            1 + (children - cap).div_ceil(cap - 1)
        };
        branches += pages;
        depth += 1;
        children = pages;
    }
    (branches, depth)
}

/// Assert that both engines build the branch levels of an ascending load the
/// same way: each engine's branch-page count and height must equal what the
/// end-of-page branch split predicts from that engine's own leaf count and
/// branch capacity (SPEC 03 §6.4).
///
/// An ascending load inserts every leaf split's separator at the end of its
/// parent branch, so branch fill is decided by how a full branch splits on an
/// end insert. The fork splits it so that exactly two children land on the
/// new right page, leaving the left page one child short of full; a median
/// split would leave branches about half full instead.
///
/// The raw branch counts of the two engines are not compared directly: the
/// page formats differ (zerodb's page header and branch nodes are wider, and
/// its leaves pack slightly differently), so leaf counts and branch
/// capacities differ, and the branch count follows from both. The model makes
/// the comparison format-independent; checking the fork against it as well
/// confirms the model is the fork's observed behavior.
///
/// `need_depth3` asserts the fork's tree has at least three levels, so the
/// load splits branches below the root; it is set for the wide-key shape,
/// which reaches depth 3 at every supported page size (4–64 KiB).
fn assert_branch_parity(tag: &str, n: u32, klen: usize, vlen: usize, need_depth3: bool) {
    let pairs = make_pairs_klen(n, klen, vlen);
    let ksize = pairs[0].0.len();
    let ldir = TempDir::new(&format!("lb-{tag}"));
    let zdir = TempDir::new(&format!("zb-{tag}"));
    let (lmdb, ps) = lmdb_stat(ldir.path(), &pairs);
    let zdb = zerodb_shape(zdir.path(), ps, &pairs);
    let (lcap, zcap) = (
        lmdb_branch_capacity(ps, ksize),
        zerodb_branch_capacity(ps, ksize),
    );
    let lmodel = ascending_branch_model(lmdb.leaf_pages, lcap);
    let zmodel = ascending_branch_model(zdb.leaf_pages, zcap);
    println!(
        "[{tag}] n={n} ksize={ksize} vlen={vlen} psize={ps}: \
         lmdb branch={} leaf={} depth={} cap={lcap} model={lmodel:?} | \
         zerodb branch={} leaf={} depth={} cap={zcap} model={zmodel:?}",
        lmdb.branch_pages, lmdb.leaf_pages, lmdb.depth, zdb.branch_pages, zdb.leaf_pages, zdb.depth
    );
    if need_depth3 {
        assert!(
            lmdb.depth >= 3,
            "[{tag}] workload too small: the fork's tree has depth {} (< 3), so it \
             does not split branches below the root",
            lmdb.depth
        );
    }
    assert_eq!(
        (lmdb.branch_pages, lmdb.depth),
        lmodel,
        "[{tag}] the fork's (branch pages, depth) differ from the end-of-page \
         split model at psize {ps}: the model, not zerodb, is wrong"
    );
    assert_eq!(
        (zdb.branch_pages, zdb.depth),
        zmodel,
        "[{tag}] zerodb's (branch pages, depth) differ from the end-of-page split \
         model at psize {ps}; an end insert on a full branch must split with two \
         children on the new right page, as the fork does (SPEC 03 §6.4)"
    );
}

/// ~500 B keys keep branch fanout small, so an ascending load reaches depth
/// >= 3 with many branch splits on every branch level.
#[test]
fn ascending_wide_keys_branch_parity() {
    assert_branch_parity("wide20k", 20_000, 500, 4, true);
}

/// 100 B keys: a mid-size branch fanout (depth 3 or more at 4–16 KiB pages).
#[test]
fn ascending_medium_keys_branch_parity() {
    assert_branch_parity("medium60k", 60_000, 100, 8, false);
}

/// Compact 9 B keys: the widest fanout; branch splits are rare (depth 3 at
/// 4–16 KiB pages), but each one still follows the end-of-page rule.
#[test]
fn ascending_compact_keys_branch_parity() {
    assert_branch_parity("compact400k", 400_000, 0, 50, false);
}
