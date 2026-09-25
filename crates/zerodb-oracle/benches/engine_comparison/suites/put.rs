//! Suite `put` — the insert ladder, all with fsync off so nothing here is a
//! disk-barrier measurement (that is `commit/*`'s job).
//!
//! Ladder:
//! * `order/{seq,rand,append}` — the same keys, three insertion orders. `seq`
//!   splits leaves at the right edge; `rand` splits everywhere and dirties far
//!   more pages; `append` (`MDB_APPEND`) tells the engine what `seq` only
//!   implies. `rand / seq` is the split-and-COW cost; `seq / append` is what
//!   the engine failed to infer from an ascending key order.
//! * `val/*` — one insert order, four value widths. Isolates the value memcpy,
//!   and at 2×page crosses into overflow pages.
//! * `api/{plain,reserved}` — the same bytes written through `put` and through
//!   `MDB_RESERVE`. milli serializes documents through the reserved path, so a
//!   gap here is a gap in milli's hottest write call (PERF-GAP B6, issue #10).
//! * `over/{same_size,grow}` — overwriting into a populated tree. Same-size can
//!   be replaced where it sits; growing may force the page to split. The delta
//!   is that split.

use criterion::{Criterion, Throughput};

use crate::data::{ascending_keys, os_page_size, shuffled_keys, N, N_OVF, N_PROBE, VAL};
use crate::harness::{wr_fresh, wr_loaded, Seed};
use crate::{pair, Cfg};

/// Entry count for the value-size sweep — see `get`'s `N_VAL`, same reasoning:
/// held constant across the sweep so value width is the only variable.
const N_VAL: usize = 10_000;
const SEED_RAND: u64 = 0x1234_5678;

pub fn run(c: &mut Criterion, cfg: &Cfg) {
    family_order(c, cfg);
    family_val(c, cfg);
    family_api(c, cfg);
    family_over(c, cfg);
}

fn family_order(c: &mut Criterion, cfg: &Cfg) {
    let val = vec![0xABu8; VAL];
    let asc = ascending_keys(N);
    let rnd = shuffled_keys(N, SEED_RAND);

    let mut g = group(c, "put/order/seq", asc.len());
    pair!(wr_fresh, bulk_put, &mut g, cfg.page, true, &asc, &val);
    g.finish();

    let mut g = group(c, "put/order/rand", rnd.len());
    pair!(wr_fresh, bulk_put, &mut g, cfg.page, true, &rnd, &val);
    g.finish();

    let mut g = group(c, "put/order/append", asc.len());
    pair!(
        wr_fresh,
        bulk_put_append,
        &mut g,
        cfg.page,
        true,
        &asc,
        &val
    );
    g.finish();
}

fn family_val(c: &mut Criterion, cfg: &Cfg) {
    let keys = ascending_keys(N_VAL);
    for (label, width) in [("v8", 8usize), ("v256", 256), ("v4k", 4096)] {
        let val = vec![0xCDu8; width];
        let mut g = group(c, &format!("put/val/{label}"), keys.len());
        pair!(wr_fresh, bulk_put, &mut g, cfg.page, true, &keys, &val);
        g.finish();
    }

    // Overflow: fewer, much larger entries, so the rung stays bounded inside
    // the 1 GiB map.
    let ovf_keys = ascending_keys(N_OVF);
    let val = vec![0xCDu8; os_page_size() as usize * 2];
    let mut g = group(c, "put/val/v2page", ovf_keys.len());
    pair!(wr_fresh, bulk_put, &mut g, cfg.page, true, &ovf_keys, &val);
    g.finish();
}

fn family_api(c: &mut Criterion, cfg: &Cfg) {
    let keys = ascending_keys(N);
    let val = vec![0xABu8; VAL];

    let mut g = group(c, "put/api/plain", keys.len());
    pair!(wr_fresh, bulk_put, &mut g, cfg.page, true, &keys, &val);
    g.finish();

    let mut g = group(c, "put/api/reserved", keys.len());
    pair!(
        wr_fresh,
        bulk_put_reserved,
        &mut g,
        cfg.page,
        true,
        &keys,
        &val
    );
    g.finish();
}

fn family_over(c: &mut Criterion, cfg: &Cfg) {
    let keys = ascending_keys(N);
    let seed_val = vec![0xABu8; VAL];
    let hits = &keys[..N_PROBE];

    // Same-size overwrite: the replacement fits where the old cell sits.
    let mut g = group(c, "put/over/same_size", hits.len());
    pair!(
        wr_loaded,
        bulk_put,
        &mut g,
        &Seed::named(cfg.page, &keys, &seed_val),
        hits,
        &seed_val
    );
    g.finish();

    // Growing overwrite: it no longer fits, so the page must make room.
    let big_val = vec![0xEEu8; VAL * 4];
    let mut g = group(c, "put/over/grow", hits.len());
    pair!(
        wr_loaded,
        bulk_put,
        &mut g,
        &Seed::named(cfg.page, &keys, &seed_val),
        hits,
        &big_val
    );
    g.finish();
}

/// A write group: element throughput, and the `heavy` criterion profile because
/// every iteration rebuilds its environment in untimed setup.
fn group<'a>(c: &'a mut Criterion, name: &str, elements: usize) -> crate::harness::Group<'a> {
    let mut g = c.benchmark_group(name);
    g.throughput(Throughput::Elements(elements as u64));
    crate::heavy(&mut g);
    g
}
