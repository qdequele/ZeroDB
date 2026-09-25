//! Suite `concurrent` (long tier) — a writer committing while readers scan.
//!
//! Every other suite is single-threaded, which hides the whole class of costs
//! MVCC exists to manage: the reader table under contention, and a writer whose
//! page reclamation is blocked by the oldest live reader. Meilisearch runs
//! exactly this shape — search traffic on a snapshot while indexing commits.
//!
//! The timed value is the **writer's** work, measured while `r` reader threads
//! full-scan in a loop. Read it against `commit/batch/n100`, which is the same
//! writer with nobody else in the environment: the ratio between them, taken
//! per engine, is what concurrency costs that engine. A gap between the two
//! engines' ratios is a reader-table or reclamation finding (ADR-0006, SPEC 05)
//! rather than a tree finding.
//!
//! Long tier only: it is the slowest rung here and the noisiest on a laptop,
//! where reader threads compete with the writer for the same few cores.

use criterion::{BatchSize, Criterion, Throughput};

use crate::data::{ascending_keys, N, VAL};
use crate::harness::{Backend, Fixture, Group};
use crate::{pair_shape, Cfg};

/// Reader threads scanning while the writer works.
const READERS: [usize; 2] = [1, 4];
/// Committed write transactions per timed iteration.
const BATCHES: usize = 20;
/// Puts per committed transaction.
const PER_BATCH: usize = 500;

pub fn run(c: &mut Criterion, cfg: &Cfg) {
    if !cfg.long() {
        return;
    }
    let keys = ascending_keys(N);
    let val = vec![0xABu8; VAL];

    for r in READERS {
        let mut g = c.benchmark_group(format!("concurrent/writer/r{r}"));
        g.throughput(Throughput::Elements((BATCHES * PER_BATCH) as u64));
        crate::heavy(&mut g);
        pair_shape!(case, &mut g, cfg.page, &keys, &val, r);
        g.finish();
    }
}

fn case<B: Backend>(g: &mut Group<'_>, page: u32, keys: &[Vec<u8>], val: &[u8], readers: usize) {
    g.bench_function(B::NAME, |b| {
        b.iter_batched(
            || {
                // Pre-populated: the readers must have something to scan, and
                // the writer's puts must be overwrites rather than a fresh
                // tree, so page reclamation is actually exercised.
                let f = Fixture::<B>::empty(page, Some("bench"), true);
                B::bulk_put(&f.env, f.db, keys, val);
                f
            },
            |f| {
                B::writer_under_readers(&f.env, f.db, keys, val, readers, BATCHES, PER_BATCH);
                drop(f);
            },
            BatchSize::PerIteration,
        )
    });
}
