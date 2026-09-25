//! Suite `mixed` — the milli-shaped rung: several named databases, written and
//! read back inside one write transaction.
//!
//! This is the only rung that reads through a *write* txn, which is a different
//! path from every `get/*` rung: the lookup has to see the transaction's own
//! uncommitted pages, so it consults the dirty store before the map. milli's
//! extractor → `write_db` phase does exactly this, thousands of times per
//! batch, across a dozen named databases.
//!
//! Read it against `get/db/named_x8` (same DB fan-out, read-only txn) and
//! `put/order/rand` (same writes, no reads): if `mixed` is worse than both, the
//! cost is the dirty-store lookup rather than either half on its own.

use criterion::{BatchSize, Criterion, Throughput};

use crate::data::{db_names, shuffled_keys, N, VAL};
use crate::harness::{Backend, Fixture, Group};
use crate::{pair_shape, Cfg};

const SEED: u64 = 0x5A5A_A5A5;

pub fn run(c: &mut Criterion, cfg: &Cfg) {
    let names = db_names();
    let keys = shuffled_keys(N, SEED);
    let val = vec![0xABu8; VAL];

    let mut g = c.benchmark_group("mixed/rw/8dbs");
    g.throughput(Throughput::Elements(keys.len() as u64));
    crate::heavy(&mut g);
    pair_shape!(case, &mut g, cfg.page, &names, &keys, &val);
    g.finish();
}

fn case<B: Backend>(g: &mut Group<'_>, page: u32, names: &[String], keys: &[Vec<u8>], val: &[u8]) {
    g.bench_function(B::NAME, |b| {
        b.iter_batched(
            // An empty multi-DB env: the rung times the writes AND the
            // read-backs, so nothing may be pre-populated.
            || Fixture::<B>::multi(page, names, &[], val),
            |f| {
                let hits = B::mixed_rw(&f.env, &f.dbs, keys, val);
                assert_eq!(hits, keys.len(), "every read-back must see its own put");
                drop(f);
            },
            BatchSize::PerIteration,
        )
    });
}
