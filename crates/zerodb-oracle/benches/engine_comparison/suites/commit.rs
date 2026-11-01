//! Suite `commit` — the transaction boundary, swept two ways.
//!
//! **Batch size** (`batch/n1` → `n100` → `n10k`, nosync): the same total number
//! of puts, spread over fewer and fewer transactions. `n1` is almost pure
//! per-commit overhead; `n10k` amortizes it to nearly nothing and what remains
//! is dirty-page write-out. The slope between them is the fixed cost of a
//! commit — read it against `env/txn/rw_empty_commit`, which is that cost with
//! zero pages to write.
//!
//! **Durability** (`sync/*`): the same rungs with fsync ON. The delta against
//! the matching nosync rung is the barrier, and on this machine it is a laptop
//! SSD. It is the rung that will move most on the real target — Graviton + EBS
//! gp3, where a barrier is a network round-trip and where the coalesced
//! commit writes (docs/PERFORMANCE.md, one `pwrite` per dirty page) are
//! supposed to pay off. Numbers taken anywhere else are indicative only.
//!
//! **Map size** (`sync/n1_map16g`): `sync/n1` on a 16 GiB map instead of
//! 1 GiB, the data unchanged. A barrier whose cost follows the map rather
//! than the data shows here; production maps (Meilisearch's) are far larger
//! than the data. Run it with `ZERODB_BENCH_WRITE_MAP=1` for the `msync`
//! barriers (SPEC 06 REC-12).

use criterion::{Criterion, Throughput};

use crate::data::{ascending_keys, N, N_COMMIT, VAL};
use crate::harness::{Backend, Fixture, Group};
use crate::{pair_shape, Cfg};

use criterion::BatchSize;

pub fn run(c: &mut Criterion, cfg: &Cfg) {
    batches(c, cfg);
    durable(c, cfg);
}

/// Puts per committed transaction, fsync off. Total puts held constant within
/// each rung's own throughput figure so per-element numbers are comparable.
fn batches(c: &mut Criterion, cfg: &Cfg) {
    let val = vec![0xABu8; VAL];
    for (label, total, per_txn) in [
        ("n1", 2_000usize, 1usize),
        ("n100", 20_000, 100),
        ("n10k", N, 10_000),
    ] {
        let keys = ascending_keys(total);
        let mut g = c.benchmark_group(format!("commit/batch/{label}"));
        g.throughput(Throughput::Elements(total as u64));
        crate::heavy(&mut g);
        pair_shape!(case, &mut g, cfg.page, true, &keys, &val, per_txn);
        g.finish();
    }
}

/// The same shape with fsync ON. `N_COMMIT` single-put txns is deliberately
/// small: each one is a real disk barrier.
fn durable(c: &mut Criterion, cfg: &Cfg) {
    let val = vec![0xABu8; VAL];

    let keys = ascending_keys(N_COMMIT);
    let mut g = c.benchmark_group("commit/sync/n1");
    g.throughput(Throughput::Elements(N_COMMIT as u64));
    crate::durable(&mut g);
    pair_shape!(case, &mut g, cfg.page, false, &keys, &val, 1);
    g.finish();

    let keys = ascending_keys(N_COMMIT * 100);
    let mut g = c.benchmark_group("commit/sync/n100");
    g.throughput(Throughput::Elements((N_COMMIT * 100) as u64));
    crate::durable(&mut g);
    pair_shape!(case, &mut g, cfg.page, false, &keys, &val, 100);
    g.finish();

    let keys = ascending_keys(N_COMMIT);
    let mut g = c.benchmark_group("commit/sync/n1_map16g");
    g.throughput(Throughput::Elements(N_COMMIT as u64));
    crate::durable(&mut g);
    pair_shape!(case_map, &mut g, cfg.page, &keys, &val, 16 << 30);
    g.finish();
}

/// One durable single-put txn per key, on a `map_size`-byte map.
fn case_map<B: Backend>(
    g: &mut Group<'_>,
    page: u32,
    keys: &[Vec<u8>],
    val: &[u8],
    map_size: usize,
) {
    g.bench_function(B::NAME, |b| {
        b.iter_batched(
            || Fixture::<B>::empty_with_map(page, Some("bench"), false, map_size),
            |f| {
                B::commit_churn(&f.env, f.db, keys, val, 1);
                // Returned, not dropped, for the reason given in `case`.
                f
            },
            BatchSize::PerIteration,
        )
    });
}

fn case<B: Backend>(
    g: &mut Group<'_>,
    page: u32,
    no_sync: bool,
    keys: &[Vec<u8>],
    val: &[u8],
    per_txn: usize,
) {
    g.bench_function(B::NAME, |b| {
        b.iter_batched(
            || Fixture::<B>::empty(page, Some("bench"), no_sync),
            |f| {
                B::commit_churn(&f.env, f.db, keys, val, per_txn);
                // Return `f` instead of dropping it here: `iter_batched`
                // drops routine outputs AFTER it stops the clock, whereas a
                // `drop(f)` inside this closure would charge the fixture's
                // teardown (env unmap, tempdir removal) to the timed commits.
                f
            },
            BatchSize::PerIteration,
        )
    });
}
