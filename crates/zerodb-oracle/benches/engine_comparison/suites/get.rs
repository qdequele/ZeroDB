//! Suite `get` — the point-lookup ladder. This is where the read gap, if there
//! is one, gets localized.
//!
//! Each family holds one variable and moves another:
//!
//! | family | held | moved | names |
//! |---|---|---|---|
//! | `db` | data, probes | how the DB handle resolves | root vs named vs 8 named |
//! | `access` | data, DB | probe locality | hot, seq, rand, miss |
//! | `size` | key/value size | entry count → tree depth | 1k, 50k, 1M |
//! | `key` | entry count, value | key width | 8 B, 32 B, 128 B |
//! | `val` | entry count, key | value width | 8 B, 256 B, 4 K, 2×page |
//!
//! Reading it: `db/root` is the floor — a descent with no catalog record to
//! resolve. `db/named` adds exactly the named-DB resolution (the named-DB
//! record lookup in docs/PERF-GAP-VS-LMDB.md), so `named / root` is that
//! mechanism's price. `access/hot` removes the descent variance and leaves
//! per-call overhead. `size` moves tree depth and nothing else, so
//! `n1m / n50k` is the per-level cost (branch levels re-resolved per cursor
//! step, docs/PERF-GAP-VS-LMDB.md). `key` moves the comparison and the
//! cells-per-page density; `val` moves the value memcpy and, at 2×page,
//! crosses into overflow pages.

use criterion::{Criterion, Throughput};

use crate::data::{
    ascending_keys, ascending_keys_wide, db_names, missing_keys, os_page_size, probes,
    round_robin_probes, shuffle, shuffled_keys, N, N_DBS, N_LARGE, N_PROBE, N_SMALL, VAL,
};
use crate::harness::{ro_probe, ro_probe_multi, ro_repeat, Backend, Group, Seed};
use crate::{pair, pair_shape, Cfg};

/// Entry count for the value-size sweep. Smaller than `N` so the 2×page rung
/// stays well inside the 1 GiB map; constant across the sweep so the only
/// variable is the value width.
const N_VAL: usize = 10_000;

/// Seeds. Distinct per family so no two rungs accidentally share an order.
const SEED_RAND: u64 = 0xBEEF_CAFE;
const SEED_MISS: u64 = 0x1357_9BDF;
const SEED_PROBE: u64 = 0x0F0F_1E1E;

pub fn run(c: &mut Criterion, cfg: &Cfg) {
    family_db(c, cfg);
    family_access(c, cfg);
    family_size(c, cfg);
    family_key(c, cfg);
    family_val(c, cfg);
}

// ---------------------------------------------------------------------------
// db: how much does resolving the database handle cost?
// ---------------------------------------------------------------------------

fn family_db(c: &mut Criterion, cfg: &Cfg) {
    let keys = ascending_keys(N);
    let val = vec![0xABu8; VAL];
    let probes = probes(&keys, N_PROBE, SEED_RAND);

    rung(
        c,
        "get/db/root",
        &Seed::root(cfg.page, &keys, &val),
        &probes,
    );
    rung(
        c,
        "get/db/named",
        &Seed::named(cfg.page, &keys, &val),
        &probes,
    );

    // 8 named DBs: every consecutive lookup resolves a DIFFERENT catalog
    // record. The probes are scattered, same as the two rungs above — see
    // `round_robin_probes` for why they cannot simply be reused.
    let names = db_names();
    let multi_probes = round_robin_probes(&keys, N_DBS, N_PROBE, SEED_RAND);
    let mut g = c.benchmark_group("get/db/named_x8");
    g.throughput(Throughput::Elements(multi_probes.len() as u64));
    pair_shape!(
        ro_probe_multi,
        &mut g,
        cfg.page,
        &names,
        &keys,
        &val,
        &multi_probes
    );
    g.finish();
}

// ---------------------------------------------------------------------------
// access: how much does probe locality matter?
// ---------------------------------------------------------------------------

fn family_access(c: &mut Criterion, cfg: &Cfg) {
    let keys = ascending_keys(N);
    let val = vec![0xABu8; VAL];

    // One key, over and over: no descent variance, everything resident and
    // every memo warm. What is left is per-call overhead.
    let mut g = c.benchmark_group("get/access/hot");
    g.throughput(Throughput::Elements(N_PROBE as u64));
    pair_shape!(case_hot, &mut g, &Seed::named(cfg.page, &keys, &val));
    g.finish();

    let seed = Seed::named(cfg.page, &keys, &val);

    let seq: Vec<Vec<u8>> = keys[..N_PROBE].to_vec();
    rung(c, "get/access/seq", &seed, &seq);

    let rand = probes(&keys, N_PROBE, SEED_PROBE);
    rung(c, "get/access/rand", &seed, &rand);

    // Absent keys: full descent, failed leaf lookup, no value returned.
    let miss = missing_keys(N_PROBE, SEED_MISS);
    rung(c, "get/access/miss", &seed, &miss);
}

fn case_hot<B: Backend>(g: &mut Group<'_>, seed: &Seed<'_>) {
    // `point_get_hot` takes the key by slice; `ro_repeat` supplies only a count,
    // so bind the key through a wrapper: the middle key of the set.
    fn hot<B: Backend>(env: &B::Env, db: B::Db, n: usize) -> usize {
        B::point_get_hot(env, db, &HOT_KEY, n)
    }
    ro_repeat::<B>(g, seed, N_PROBE, hot::<B>);
}

/// The single key every `access/hot` lookup asks for — the middle of the
/// ascending set, so the descent is a typical one rather than an edge.
static HOT_KEY: [u8; 8] = (N as u64 / 2).to_be_bytes();

// ---------------------------------------------------------------------------
// size: how much does one more tree level cost?
// ---------------------------------------------------------------------------

fn family_size(c: &mut Criterion, cfg: &Cfg) {
    let val = vec![0xABu8; VAL];

    for (label, n) in [("n1k", N_SMALL), ("n50k", N)] {
        let keys = ascending_keys(n);
        let p = probes(&keys, N_PROBE, SEED_RAND);
        rung(
            c,
            &format!("get/size/{label}"),
            &Seed::named(cfg.page, &keys, &val),
            &p,
        );
    }

    if cfg.long() {
        let keys = ascending_keys(N_LARGE);
        let p = probes(&keys, N_PROBE, SEED_RAND);
        rung(c, "get/size/n1m", &Seed::named(cfg.page, &keys, &val), &p);
    }
}

// ---------------------------------------------------------------------------
// key: how much does the comparison and the cell density cost?
// ---------------------------------------------------------------------------

fn family_key(c: &mut Criterion, cfg: &Cfg) {
    let val = vec![0xABu8; VAL];
    for width in [8usize, 32, 128] {
        let keys = ascending_keys_wide(N, width);
        let p = shuffle(keys.clone(), SEED_RAND)[..N_PROBE].to_vec();
        rung(
            c,
            &format!("get/key/k{width}"),
            &Seed::named(cfg.page, &keys, &val),
            &p,
        );
    }
}

// ---------------------------------------------------------------------------
// val: how much does moving the value cost, and what does overflow add?
// ---------------------------------------------------------------------------

fn family_val(c: &mut Criterion, cfg: &Cfg) {
    let keys = ascending_keys(N_VAL);
    let p = shuffled_keys(N_VAL, SEED_RAND);
    let page = os_page_size() as usize;

    for (label, width) in [
        ("v8".to_string(), 8usize),
        ("v256".to_string(), 256),
        ("v4k".to_string(), 4096),
        ("v2page".to_string(), page * 2),
    ] {
        let val = vec![0xCDu8; width];
        rung(
            c,
            &format!("get/val/{label}"),
            &Seed::named(cfg.page, &keys, &val),
            &p,
        );
    }

    // `point_get` never reads the returned value's bytes, so on `v4k`/`v2page`
    // the overflow-page chase and value memcpy can be an artifact the
    // optimizer (or, on the LMDB side, the OS page cache) never has to pay
    // for. These `_touch` variants read the first and last byte of every
    // returned value through `black_box`; `v8_touch` is the family-local
    // control so `v4k_touch ÷ v8_touch` / `v2page_touch ÷ v8_touch` isolate
    // the touch's own cost the same way the non-touch rungs do.
    for (label, width) in [
        ("v8_touch".to_string(), 8usize),
        ("v4k_touch".to_string(), 4096),
        ("v2page_touch".to_string(), page * 2),
    ] {
        let val = vec![0xCDu8; width];
        rung_touch(
            c,
            &format!("get/val/{label}"),
            &Seed::named(cfg.page, &keys, &val),
            &p,
        );
    }
}

// ---------------------------------------------------------------------------

/// One `point_get` rung, both engines.
fn rung(c: &mut Criterion, name: &str, seed: &Seed<'_>, p: &[Vec<u8>]) {
    let mut g = c.benchmark_group(name);
    g.throughput(Throughput::Elements(p.len() as u64));
    pair!(ro_probe, point_get, &mut g, seed, p);
    g.finish();
}

/// One `point_get_touch` rung, both engines — see `family_val`'s `_touch` loop.
fn rung_touch(c: &mut Criterion, name: &str, seed: &Seed<'_>, p: &[Vec<u8>]) {
    let mut g = c.benchmark_group(name);
    g.throughput(Throughput::Elements(p.len() as u64));
    pair!(ro_probe, point_get_touch, &mut g, seed, p);
    g.finish();
}
