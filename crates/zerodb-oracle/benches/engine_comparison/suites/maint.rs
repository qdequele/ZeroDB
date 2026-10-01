//! Suite `maint` — whole-environment maintenance: `mdb_env_copy2`, both modes.
//!
//! Meilisearch calls this on every snapshot, so it is a user-visible latency,
//! not an internal detail. The two modes measure different things: `copy/raw`
//! is essentially a page-for-page file copy, so it is bounded by I/O and by how
//! large the environment is; `copy/compact` rebuilds the tree densely, so it is
//! bounded by the walk plus the rebuild.
//!
//! `compact / raw`, taken per engine, is the price of compaction. Between
//! engines, `raw` also reports the **on-disk size** difference indirectly — a
//! denser store has less to copy (zerodb measured ~17 % denser in July 2026),
//! so a `raw` win here may be a density win rather than a speed win. Check
//! `zerodb-tools stat` before claiming either.

use criterion::{BatchSize, Criterion, Throughput};

use crate::data::{ascending_keys, N, VAL};
use crate::harness::{Backend, Fixture, Group};
use crate::{pair_shape, Cfg};

pub fn run(c: &mut Criterion, cfg: &Cfg) {
    let keys = ascending_keys(N);
    let val = vec![0xABu8; VAL];

    for (label, compact) in [("compact", true), ("raw", false)] {
        let mut g = c.benchmark_group(format!("maint/copy/{label}"));
        g.throughput(Throughput::Elements(N as u64));
        crate::heavy(&mut g);
        pair_shape!(case, &mut g, cfg.page, &keys, &val, compact);
        g.finish();
    }
}

fn case<B: Backend>(g: &mut Group<'_>, page: u32, keys: &[Vec<u8>], val: &[u8], compact: bool) {
    // The source env is built once and reused: `copy_to` does not modify it, so
    // only the destination has to be fresh per iteration.
    let f = Fixture::<B>::single(page, Some("bench"), keys, val);
    g.bench_function(B::NAME, |b| {
        b.iter_batched(
            || zerodb_oracle::tempdir::TempDir::new().expect("tempdir"),
            |dest| {
                B::copy_to(&f.env, &dest.path().join("copy.mdb"), compact);
                // Returned, not dropped: iter_batched drops routine outputs
                // after it stops the clock, so teardown stays untimed.
                dest
            },
            BatchSize::PerIteration,
        )
    });
}
