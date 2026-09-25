//! The `Backend` trait and the generic rung shapes.
//!
//! Why a trait and not two copy-pasted blocks: every timed body below is
//! written **once**, as a generic function, and instantiated for each engine.
//! Nothing about a rung can drift between LMDB and zerodb — not the operation
//! (one macro body in `backend.rs`), not the setup, not the criterion wiring.
//! That is the whole fairness argument of this harness, and it is enforced by
//! the type system rather than by review.

use std::hint::black_box;
use std::path::Path;

use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup};
use zerodb_oracle::tempdir::TempDir;

use crate::data::MAP;

/// A criterion group measured in wall time — the only measurement this harness
/// uses.
pub type Group<'a> = BenchmarkGroup<'a, WallTime>;

/// The operations both engines expose, with identical signatures.
///
/// Implemented by `backend.rs`'s macro for `Lmdb` and `Zerodb`; every method
/// forwards to the single shared macro body, so an implementation cannot
/// diverge either.
pub trait Backend {
    /// The criterion function name for this engine (`"lmdb"` / `"zerodb"`).
    const NAME: &'static str;
    /// The engine's environment handle.
    type Env;
    /// The engine's database handle.
    type Db: Copy;

    // -- lifecycle -------------------------------------------------------
    fn open(dir: &Path, map_size: usize, no_sync: bool, page: u32) -> Self::Env;
    fn create_db(env: &Self::Env, name: Option<&str>) -> Self::Db;
    fn create_dbs(env: &Self::Env, names: &[String]) -> Vec<Self::Db>;
    fn open_create_close(dirs: &[std::path::PathBuf], map_size: usize, page: u32);
    fn reopen(dir: &Path, map_size: usize, page: u32, n: usize);
    fn rotxn_churn(env: &Self::Env, n: usize) -> usize;
    fn empty_commit_churn(env: &Self::Env, n: usize);

    // -- write -----------------------------------------------------------
    fn bulk_put(env: &Self::Env, db: Self::Db, keys: &[Vec<u8>], val: &[u8]);
    fn bulk_put_append(env: &Self::Env, db: Self::Db, keys: &[Vec<u8>], val: &[u8]);
    fn bulk_put_reserved(env: &Self::Env, db: Self::Db, keys: &[Vec<u8>], val: &[u8]);
    fn commit_churn(env: &Self::Env, db: Self::Db, keys: &[Vec<u8>], val: &[u8], per_txn: usize);
    fn delete_keys(env: &Self::Env, db: Self::Db, keys: &[Vec<u8>]) -> usize;
    fn delete_range(env: &Self::Env, db: Self::Db, lo: &[u8], hi: &[u8]) -> usize;
    fn clear_db(env: &Self::Env, db: Self::Db);
    fn cursor_drain(env: &Self::Env, db: Self::Db, keys: &[Vec<u8>], val: &[u8]);
    fn delete_reinsert(env: &Self::Env, db: Self::Db, keys: &[Vec<u8>], val: &[u8], rounds: usize);

    // -- read ------------------------------------------------------------
    fn point_get(env: &Self::Env, db: Self::Db, keys: &[Vec<u8>]) -> usize;
    /// Same as `point_get`, but reads the first/last byte of every returned
    /// value through `black_box` — the control for the `get/val/*_touch` rungs,
    /// so overflow-value rungs measure the value chase and copy rather than an
    /// elided read.
    fn point_get_touch(env: &Self::Env, db: Self::Db, keys: &[Vec<u8>]) -> usize;
    fn point_get_multi(env: &Self::Env, dbs: &[Self::Db], keys: &[Vec<u8>]) -> usize;
    fn point_get_hot(env: &Self::Env, db: Self::Db, key: &[u8], n: usize) -> usize;
    fn scan(env: &Self::Env, db: Self::Db) -> usize;
    fn rev_scan(env: &Self::Env, db: Self::Db) -> usize;
    fn range_scan(env: &Self::Env, db: Self::Db, lo: &[u8], hi: &[u8]) -> usize;
    fn prefix_scan(env: &Self::Env, db: Self::Db, prefix: &[u8]) -> usize;
    fn first_last(env: &Self::Env, db: Self::Db, n: usize) -> usize;
    fn db_len(env: &Self::Env, db: Self::Db, n: usize) -> usize;
    fn seek_ge(env: &Self::Env, db: Self::Db, probes: &[Vec<u8>]) -> usize;

    // -- composite / maintenance -----------------------------------------
    fn mixed_rw(env: &Self::Env, dbs: &[Self::Db], keys: &[Vec<u8>], val: &[u8]) -> usize;
    #[allow(clippy::too_many_arguments)]
    /// Returns the elapsed time of the writer loop ONLY (first `write_txn` to
    /// last `commit`), so a caller timing this with `iter_custom` excludes the
    /// reader threads' join tail and the fixture's drop.
    fn writer_under_readers(
        env: &Self::Env,
        db: Self::Db,
        keys: &[Vec<u8>],
        val: &[u8],
        readers: usize,
        batches: usize,
        per_batch: usize,
    ) -> std::time::Duration;
    fn copy_to(env: &Self::Env, dest: &Path, compact: bool);
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A populated single-database environment, kept alive for the rung's lifetime.
///
/// Field order matters: `env` must drop (unmapping the file) before `dir` is
/// removed, and Rust drops fields in declaration order.
pub struct Fixture<B: Backend> {
    /// The engine handle.
    pub env: B::Env,
    /// The database under test.
    pub db: B::Db,
    /// Every named database, when the rung asked for more than one.
    pub dbs: Vec<B::Db>,
    dir: TempDir,
}

impl<B: Backend> Fixture<B> {
    /// Build a single-DB fixture holding `keys` → `val`. `name` = `None` uses
    /// the unnamed root database.
    pub fn single(page: u32, name: Option<&str>, keys: &[Vec<u8>], val: &[u8]) -> Fixture<B> {
        let dir = TempDir::new().expect("tempdir");
        let env = B::open(dir.path(), MAP, true, page);
        let db = B::create_db(&env, name);
        B::bulk_put(&env, db, keys, val);
        Fixture {
            env,
            db,
            dbs: vec![db],
            dir,
        }
    }

    /// Build an empty single-DB fixture (for rungs that time the first write).
    pub fn empty(page: u32, name: Option<&str>, no_sync: bool) -> Fixture<B> {
        let dir = TempDir::new().expect("tempdir");
        let env = B::open(dir.path(), MAP, no_sync, page);
        let db = B::create_db(&env, name);
        Fixture {
            env,
            db,
            dbs: vec![db],
            dir,
        }
    }

    /// Build an `names.len()`-database fixture, `keys` spread round-robin.
    pub fn multi(page: u32, names: &[String], keys: &[Vec<u8>], val: &[u8]) -> Fixture<B> {
        let dir = TempDir::new().expect("tempdir");
        let env = B::open(dir.path(), MAP, true, page);
        let dbs = B::create_dbs(&env, names);
        for (i, chunk) in split_round_robin(keys, dbs.len()).iter().enumerate() {
            B::bulk_put(&env, dbs[i], chunk, val);
        }
        let db = *dbs.last().expect("at least one db");
        Fixture { env, db, dbs, dir }
    }

    /// CLOSE the environment, keeping its populated directory alive.
    ///
    /// Required by any rung that times `open` itself: both engines refuse a
    /// second open of the same path from the same process (`EnvAlreadyOpened`),
    /// so the fixture that wrote the data must be gone before the rung runs.
    /// Destructuring rather than dropping `self` makes the order explicit —
    /// the env unmaps first, the directory outlives it.
    pub fn into_dir(self) -> TempDir {
        let Fixture {
            env,
            db: _,
            dbs,
            dir,
        } = self;
        // The handles are inert once the env is gone; the env must unmap before
        // the directory is removed, so drop it explicitly and return `dir`.
        drop(dbs);
        drop(env);
        dir
    }
}

/// Deal `keys` into `n` buckets round-robin — matches `point_get_multi`'s and
/// `mixed_rw`'s `i % dbs.len()` dispatch, so every probe finds its key.
fn split_round_robin(keys: &[Vec<u8>], n: usize) -> Vec<Vec<Vec<u8>>> {
    let mut out = vec![Vec::new(); n];
    for (i, k) in keys.iter().enumerate() {
        out[i % n].push(k.clone());
    }
    out
}

// ---------------------------------------------------------------------------
// Rung shapes — one generic body each, instantiated per engine.
// ---------------------------------------------------------------------------

/// The four things that describe a rung's *data*, bundled so the shape
/// functions stay readable (and under clippy's argument limit).
#[derive(Clone, Copy)]
pub struct Seed<'a> {
    /// DB page size, pinned to the OS page size for both engines.
    pub page: u32,
    /// Database to use; `None` = the unnamed root DB.
    pub db: Option<&'a str>,
    /// Entries loaded into the fixture before the rung runs.
    pub keys: &'a [Vec<u8>],
    /// Value written for each of `keys`.
    pub val: &'a [u8],
}

impl<'a> Seed<'a> {
    /// The common case: a named database called `bench`.
    pub fn named(page: u32, keys: &'a [Vec<u8>], val: &'a [u8]) -> Seed<'a> {
        Seed {
            page,
            db: Some("bench"),
            keys,
            val,
        }
    }

    /// The unnamed root database — the `get/db/root` baseline.
    pub fn root(page: u32, keys: &'a [Vec<u8>], val: &'a [u8]) -> Seed<'a> {
        Seed {
            page,
            db: None,
            keys,
            val,
        }
    }
}

/// A timed op taking a probe list: `point_get`, `seek_ge`.
pub type ProbeOp<B> = fn(&<B as Backend>::Env, <B as Backend>::Db, &[Vec<u8>]) -> usize;
/// A timed op taking nothing but the handles: `scan`, `rev_scan`.
pub type WholeOp<B> = fn(&<B as Backend>::Env, <B as Backend>::Db) -> usize;
/// A timed op taking a repeat count: `first_last`, `db_len`.
pub type RepeatOp<B> = fn(&<B as Backend>::Env, <B as Backend>::Db, usize) -> usize;
/// A timed op taking two byte bounds: `range_scan` (and `prefix_scan`, adapted).
pub type SpanOp<B> = fn(&<B as Backend>::Env, <B as Backend>::Db, &[u8], &[u8]) -> usize;
/// A timed write op: every `bulk_put*` variant, and the adapted delete ops.
pub type WriteOp<B> = fn(&<B as Backend>::Env, <B as Backend>::Db, &[Vec<u8>], &[u8]);

/// Read-only rung whose timed op takes a probe list.
pub fn ro_probe<B: Backend>(
    g: &mut Group<'_>,
    seed: &Seed<'_>,
    probes: &[Vec<u8>],
    op: ProbeOp<B>,
) {
    let f = Fixture::<B>::single(seed.page, seed.db, seed.keys, seed.val);
    g.bench_function(B::NAME, |b| b.iter(|| black_box(op(&f.env, f.db, probes))));
}

/// Read-only rung over several named databases (`point_get_multi`).
pub fn ro_probe_multi<B: Backend>(
    g: &mut Group<'_>,
    page: u32,
    names: &[String],
    keys: &[Vec<u8>],
    val: &[u8],
    probes: &[Vec<u8>],
) {
    let f = Fixture::<B>::multi(page, names, keys, val);
    // Guard the round-robin invariant permanently: if the probe list and the
    // key deal ever disagree, this rung would quietly become a miss benchmark.
    assert_eq!(
        B::point_get_multi(&f.env, &f.dbs, probes),
        probes.len(),
        "every multi-DB probe must hit — see data::round_robin_probes"
    );
    g.bench_function(B::NAME, |b| {
        b.iter(|| black_box(B::point_get_multi(&f.env, &f.dbs, probes)))
    });
}

/// Read-only rung whose timed op takes no argument.
pub fn ro_whole<B: Backend>(g: &mut Group<'_>, seed: &Seed<'_>, op: WholeOp<B>) {
    let f = Fixture::<B>::single(seed.page, seed.db, seed.keys, seed.val);
    g.bench_function(B::NAME, |b| b.iter(|| black_box(op(&f.env, f.db))));
}

/// Read-only rung whose timed op takes a repeat count.
pub fn ro_repeat<B: Backend>(g: &mut Group<'_>, seed: &Seed<'_>, reps: usize, op: RepeatOp<B>) {
    let f = Fixture::<B>::single(seed.page, seed.db, seed.keys, seed.val);
    g.bench_function(B::NAME, |b| b.iter(|| black_box(op(&f.env, f.db, reps))));
}

/// Read-only rung over a byte-bounded span.
pub fn ro_span<B: Backend>(
    g: &mut Group<'_>,
    seed: &Seed<'_>,
    lo: &[u8],
    hi: &[u8],
    op: SpanOp<B>,
) {
    let f = Fixture::<B>::single(seed.page, seed.db, seed.keys, seed.val);
    g.bench_function(B::NAME, |b| b.iter(|| black_box(op(&f.env, f.db, lo, hi))));
}

/// Write rung: a fresh EMPTY env per iteration (untimed), then the timed write.
pub fn wr_fresh<B: Backend>(
    g: &mut Group<'_>,
    page: u32,
    no_sync: bool,
    keys: &[Vec<u8>],
    val: &[u8],
    op: WriteOp<B>,
) {
    g.bench_function(B::NAME, |b| {
        b.iter_batched(
            || Fixture::<B>::empty(page, Some("bench"), no_sync),
            |f| {
                op(&f.env, f.db, keys, val);
                // Returned, not dropped: iter_batched drops routine outputs
                // after it stops the clock, so teardown stays untimed.
                f
            },
            BatchSize::PerIteration,
        )
    });
}

/// Write rung against a PRE-POPULATED env: overwrite, delete, churn. `seed`
/// describes the untimed population; only `op` over `keys`/`val` is measured.
pub fn wr_loaded<B: Backend>(
    g: &mut Group<'_>,
    seed: &Seed<'_>,
    keys: &[Vec<u8>],
    val: &[u8],
    op: WriteOp<B>,
) {
    let seed = *seed;
    g.bench_function(B::NAME, |b| {
        b.iter_batched(
            || {
                let f = Fixture::<B>::empty(seed.page, seed.db, true);
                B::bulk_put(&f.env, f.db, seed.keys, seed.val);
                f
            },
            |f| {
                op(&f.env, f.db, keys, val);
                // Returned, not dropped: iter_batched drops routine outputs
                // after it stops the clock, so teardown stays untimed.
                f
            },
            BatchSize::PerIteration,
        )
    });
}

// ---------------------------------------------------------------------------
// Pairing
// ---------------------------------------------------------------------------

/// Run one rung shape for BOTH engines, naming the shape and the operation
/// exactly once.
///
/// Without this, every rung would spell out `lmdb::Marker` / `zerodb::Marker`
/// and the op twice, and nothing would stop a paste from comparing one engine's
/// `scan` against the other's `rev_scan`. Here the op is a single `$op` token
/// expanded under both markers, so the two sides cannot disagree.
///
/// ```ignore
/// pair!(ro_whole, scan, &mut g, cfg.page, Some("bench"), &keys, &val);
/// ```
#[macro_export]
macro_rules! pair {
    ($shape:ident, $op:ident, $g:expr $(, $arg:expr)* $(,)?) => {{
        $shape::<$crate::backend::lmdb::Marker>(
            $g,
            $($arg,)*
            <$crate::backend::lmdb::Marker as $crate::harness::Backend>::$op,
        );
        $shape::<$crate::backend::zerodb::Marker>(
            $g,
            $($arg,)*
            <$crate::backend::zerodb::Marker as $crate::harness::Backend>::$op,
        );
    }};
}

/// [`pair!`] for shapes that carry their operation internally (the local
/// `case_*` functions, and `ro_probe_multi`).
#[macro_export]
macro_rules! pair_shape {
    ($shape:ident, $g:expr $(, $arg:expr)* $(,)?) => {{
        $shape::<$crate::backend::lmdb::Marker>($g $(, $arg)*);
        $shape::<$crate::backend::zerodb::Marker>($g $(, $arg)*);
    }};
}

/// [`pair!`] for a rung whose operation is a *locally defined* generic function
/// rather than a trait method — used where an op has to be adapted to a shape's
/// signature (e.g. `delete_keys`, which returns a count the shape does not
/// want). Same guarantee: `$op` is written once.
#[macro_export]
macro_rules! pair_op {
    ($shape:ident, $op:ident, $g:expr $(, $arg:expr)* $(,)?) => {{
        $shape::<$crate::backend::lmdb::Marker>(
            $g,
            $($arg,)*
            $op::<$crate::backend::lmdb::Marker>,
        );
        $shape::<$crate::backend::zerodb::Marker>(
            $g,
            $($arg,)*
            $op::<$crate::backend::zerodb::Marker>,
        );
    }};
}
