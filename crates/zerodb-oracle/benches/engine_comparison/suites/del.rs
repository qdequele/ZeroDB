//! Suite `del` — removal and the freelist behind it.
//!
//! Deletion is where the two engines' page-reclamation designs become visible.
//! LMDB pushes freed pages onto the FREE_DBI list keyed by txnid; zerodb keeps
//! its own GC (SPEC 05). A gap that shows up in `churn/reinsert` and nowhere
//! else points at reclamation, not at the tree (PERF-GAP B7, issue #29).
//!
//! Ladder:
//! * `bulk/all` empties the tree key by key — every leaf eventually merges.
//! * `bulk/half` leaves it populated: rebalance without collapse.
//! * `range/half` removes the same span through one `delete_range` cursor walk,
//!   so `range/half ÷ bulk/half` is per-key descent vs a single positioned walk.
//! * `clear/all` drops the whole tree in one operation, which should be
//!   page-list work rather than per-key work.
//! * `churn/reinsert` alternates delete and re-insert, so freed pages have to be
//!   reclaimed and handed straight back out. This is the freelist's rung.

use criterion::{BatchSize, Criterion, Throughput};

use crate::data::{ascending_keys, N, VAL};
use crate::harness::{wr_loaded, Backend, Fixture, Group, Seed};
use crate::{pair, pair_op, pair_shape, Cfg};

/// Delete/re-insert cycles per timed iteration.
const CHURN_ROUNDS: usize = 3;
/// Keys touched by the churn rung — a slice, not the whole set, so each round
/// stays short enough to repeat.
const CHURN_KEYS: usize = 5_000;

pub fn run(c: &mut Criterion, cfg: &Cfg) {
    let keys = ascending_keys(N);
    let val = vec![0xABu8; VAL];

    bulk(c, cfg, &keys, &val);
    range(c, cfg, &keys, &val);
    cursor(c, cfg, &keys, &val);
    clear(c, cfg, &keys, &val);
    churn(c, cfg, &keys, &val);
}

/// `delete_keys` returns a hit count the write shape does not take; adapt it,
/// and assert the count so a rung can never silently measure a no-op.
fn del_op<B: Backend>(env: &B::Env, db: B::Db, keys: &[Vec<u8>], _val: &[u8]) {
    let n = B::delete_keys(env, db, keys);
    assert_eq!(n, keys.len(), "every delete must hit an existing key");
}

/// Per-key deletion, whole tree and half tree.
fn bulk(c: &mut Criterion, cfg: &Cfg, keys: &[Vec<u8>], val: &[u8]) {
    let mut g = group(c, "del/bulk/all", keys.len());
    pair_op!(
        wr_loaded,
        del_op,
        &mut g,
        &Seed::named(cfg.page, keys, val),
        keys,
        val
    );
    g.finish();

    let half = &keys[..keys.len() / 2];
    let mut g = group(c, "del/bulk/half", half.len());
    pair_op!(
        wr_loaded,
        del_op,
        &mut g,
        &Seed::named(cfg.page, keys, val),
        half,
        val
    );
    g.finish();
}

/// The same half-tree span, removed through one `delete_range` cursor walk.
fn range(c: &mut Criterion, cfg: &Cfg, keys: &[Vec<u8>], val: &[u8]) {
    let half = keys.len() / 2;
    let mut g = group(c, "del/range/half", half);
    pair_shape!(
        case_range,
        &mut g,
        cfg.page,
        keys,
        val,
        &keys[0],
        &keys[half]
    );
    g.finish();
}

fn case_range<B: Backend>(
    g: &mut Group<'_>,
    page: u32,
    keys: &[Vec<u8>],
    val: &[u8],
    lo: &[u8],
    hi: &[u8],
) {
    g.bench_function(B::NAME, |b| {
        b.iter_batched(
            || {
                let f = Fixture::<B>::empty(page, Some("bench"), true);
                B::bulk_put(&f.env, f.db, keys, val);
                f
            },
            |f| {
                let n = B::delete_range(&f.env, f.db, lo, hi);
                assert!(n > 0, "delete_range must remove something");
                drop(f);
            },
            BatchSize::PerIteration,
        )
    });
}

/// One drop of the whole tree, handle kept.
fn clear(c: &mut Criterion, cfg: &Cfg, keys: &[Vec<u8>], val: &[u8]) {
    fn clear_op<B: Backend>(env: &B::Env, db: B::Db, _keys: &[Vec<u8>], _val: &[u8]) {
        B::clear_db(env, db);
    }
    let mut g = group(c, "del/clear/all", keys.len());
    pair_op!(
        wr_loaded,
        clear_op,
        &mut g,
        &Seed::named(cfg.page, keys, val),
        keys,
        val
    );
    g.finish();
}

/// The same half-tree span drained through the WRITE CURSOR. `bulk/half`
/// and `range/half` both reach the tree by key; this one reaches it by
/// cursor, so it is the only rung where post-delete cursor position costs
/// anything (PERF-GAP B8a, SPEC 03 §5.4a). Read it against `range/half`:
/// same span, same result, different mechanism.
fn cursor(c: &mut Criterion, cfg: &Cfg, keys: &[Vec<u8>], val: &[u8]) {
    let half = &keys[..keys.len() / 2];
    let mut g = group(c, "del/cursor/drain", half.len());
    pair!(
        wr_loaded,
        cursor_drain,
        &mut g,
        &Seed::named(cfg.page, keys, val),
        half,
        val
    );
    g.finish();
}

/// Delete then re-insert the same keys, repeatedly.
fn churn(c: &mut Criterion, cfg: &Cfg, keys: &[Vec<u8>], val: &[u8]) {
    fn churn_op<B: Backend>(env: &B::Env, db: B::Db, keys: &[Vec<u8>], val: &[u8]) {
        B::delete_reinsert(env, db, keys, val, CHURN_ROUNDS);
    }
    let touched = &keys[..CHURN_KEYS];
    let mut g = group(c, "del/churn/reinsert", CHURN_KEYS * CHURN_ROUNDS * 2);
    pair_op!(
        wr_loaded,
        churn_op,
        &mut g,
        &Seed::named(cfg.page, keys, val),
        touched,
        val
    );
    g.finish();
}

/// A write group: element throughput plus the `heavy` criterion profile,
/// because every iteration rebuilds a populated environment in untimed setup.
fn group<'a>(c: &'a mut Criterion, name: &str, elements: usize) -> Group<'a> {
    let mut g = c.benchmark_group(name);
    g.throughput(Throughput::Elements(elements as u64));
    crate::heavy(&mut g);
    g
}
