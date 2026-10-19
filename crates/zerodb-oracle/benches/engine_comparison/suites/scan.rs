//! Suite `scan` — cursor iteration, where the per-entry cost is amortized over
//! a page instead of paid per descent.
//!
//! Ladder: `full/fwd` is the reference (one descent, then pure leaf walking —
//! this is the rung the cursor leaf memo was built for). `full/rev` is the same
//! walk against the sibling-link direction. `range/*` re-adds a positioned
//! descent and a per-step bound test. `edge/first_last` is descent-only with no
//! walking at all, and `meta/len` reads the DB record without touching the
//! tree — together they bracket what a scan's fixed cost is. `skip_take/*` is a
//! paged listing (`skip(5000).take(100)`): a full-scan prefix the caller
//! throws away, where `Iterator::nth` decides whether the skipped entries are
//! stepped one by one or crossed a leaf at a time.

use criterion::{Criterion, Throughput};

use crate::data::{ascending_keys, N, N_PROBE, VAL};
use crate::harness::{ro_repeat, ro_span, ro_whole, Backend, Fixture, Group, Seed};
use crate::{pair, pair_shape, Cfg};

/// The key whose 7-byte prefix selects one bucket. With 8-byte big-endian keys
/// below 2^16, bytes 0..6 are always zero and byte 6 is the bucket, so a 7-byte
/// prefix matches the 256 keys sharing it.
const PREFIX_KEY: usize = 12_345;
/// Prefix width, in bytes — see `PREFIX_KEY`.
const PREFIX_LEN: usize = 7;

pub fn run(c: &mut Criterion, cfg: &Cfg) {
    let keys = ascending_keys(N);
    let val = vec![0xABu8; VAL];

    full(c, cfg, &keys, &val);
    skips(c, cfg, &keys, &val);
    ranges(c, cfg, &keys, &val);
    edges(c, cfg, &keys, &val);
}

/// Whole-database walks, both directions.
fn full(c: &mut Criterion, cfg: &Cfg, keys: &[Vec<u8>], val: &[u8]) {
    let mut g = c.benchmark_group("scan/full/fwd");
    g.throughput(Throughput::Elements(N as u64));
    pair!(ro_whole, scan, &mut g, &Seed::named(cfg.page, keys, val));
    g.finish();

    let mut g = c.benchmark_group("scan/full/rev");
    g.throughput(Throughput::Elements(N as u64));
    pair!(
        ro_whole,
        rev_scan,
        &mut g,
        &Seed::named(cfg.page, keys, val)
    );
    g.finish();
}

/// Entries a paged listing skips, then yields — crud-bench's
/// `start(5000) limit(100)` scan.
const SKIP: usize = 5_000;
const TAKE: usize = 100;

/// Paged listings, both directions.
fn skips(c: &mut Criterion, cfg: &Cfg, keys: &[Vec<u8>], val: &[u8]) {
    for rev in [false, true] {
        let mut g = c.benchmark_group(format!(
            "scan/skip_take/{}",
            if rev { "rev" } else { "fwd" }
        ));
        g.throughput(Throughput::Elements((SKIP + TAKE) as u64));
        pair_shape!(
            case_skip_take,
            &mut g,
            &Seed::named(cfg.page, keys, val),
            rev
        );
        g.finish();
    }
}

/// `skip_take` / `rev_skip_take` take two counts; no generic shape fits.
fn case_skip_take<B: Backend>(g: &mut Group<'_>, seed: &Seed<'_>, rev: bool) {
    let f = Fixture::<B>::single(seed.page, seed.db, seed.keys, seed.val);
    let op = if rev { B::rev_skip_take } else { B::skip_take };
    assert_eq!(
        op(&f.env, f.db, SKIP, TAKE),
        TAKE,
        "the window must be full"
    );
    g.bench_function(B::NAME, |b| {
        b.iter(|| std::hint::black_box(op(&f.env, f.db, SKIP, TAKE)))
    });
}

/// Bounded spans: a positioned descent plus a bound test per step.
fn ranges(c: &mut Criterion, cfg: &Cfg, keys: &[Vec<u8>], val: &[u8]) {
    for (label, pct) in [("1pct", 1usize), ("10pct", 10)] {
        let span = N * pct / 100;
        let lo = N / 2 - span / 2;
        let hi = lo + span;
        let mut g = c.benchmark_group(format!("scan/range/{label}"));
        g.throughput(Throughput::Elements(span as u64));
        pair!(
            ro_span,
            range_scan,
            &mut g,
            &Seed::named(cfg.page, keys, val),
            &keys[lo],
            &keys[hi]
        );
        g.finish();
    }

    let prefix = &keys[PREFIX_KEY][..PREFIX_LEN];
    let mut g = c.benchmark_group("scan/prefix/bucket");
    g.throughput(Throughput::Elements(256));
    pair_shape!(
        case_prefix,
        &mut g,
        &Seed::named(cfg.page, keys, val),
        prefix
    );
    g.finish();
}

/// `prefix_scan` takes one bound; adapt it to the two-bound span shape.
fn case_prefix<B: Backend>(g: &mut Group<'_>, seed: &Seed<'_>, prefix: &[u8]) {
    fn op<B: Backend>(env: &B::Env, db: B::Db, prefix: &[u8], _unused: &[u8]) -> usize {
        B::prefix_scan(env, db, prefix)
    }
    ro_span::<B>(g, seed, prefix, prefix, op::<B>);
}

/// Descent without walking (`first_last`) and the DB record without a descent
/// (`meta/len`) — the two ends of a scan's fixed cost.
fn edges(c: &mut Criterion, cfg: &Cfg, keys: &[Vec<u8>], val: &[u8]) {
    let mut g = c.benchmark_group("scan/edge/first_last");
    g.throughput(Throughput::Elements(N_PROBE as u64));
    pair!(
        ro_repeat,
        first_last,
        &mut g,
        &Seed::named(cfg.page, keys, val),
        N_PROBE
    );
    g.finish();

    let mut g = c.benchmark_group("scan/meta/len");
    g.throughput(Throughput::Elements(N_PROBE as u64));
    pair!(
        ro_repeat,
        db_len,
        &mut g,
        &Seed::named(cfg.page, keys, val),
        N_PROBE
    );
    g.finish();
}
