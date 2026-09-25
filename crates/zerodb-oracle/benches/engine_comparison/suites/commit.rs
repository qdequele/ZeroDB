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
//! gp3, where a barrier is a network round-trip and where PERF-GAP B4's
//! coalesced writes are supposed to pay off. Numbers taken anywhere else are
//! indicative only.

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
                drop(f);
            },
            BatchSize::PerIteration,
        )
    });
}
