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
//!   and at 2×page crosses into overflow pages. `v2page_abort` runs the same
//!   overflow puts and aborts, so the put path is measured without the
//!   commit's file writes.
//! * `api/{plain,reserved}` — the same bytes written through `put` and through
//!   `MDB_RESERVE`. milli serializes documents through the reserved path, so a
//!   gap here is a gap in milli's hottest write call (adapter `ReservedSpace`
//!   in docs/PERF-GAP-VS-LMDB.md, issue #10).
//! * `over/{same_size,grow}` — overwriting into a populated tree. Same-size can
//!   be replaced where it sits; growing may force the page to split. The delta
//!   is that split.
//! * `cursor/rewrite` — the same-size overwrites of `over/same_size`, reached
//!   through the write cursor (`range_mut` + `put_current`) instead of by key:
//!   the only rung where the cursor's position after a put costs anything.
//! * `gc/drain_big` — overwrites whose copy-on-write pages all come from ONE
//!   large free-list entry (a contiguous half of a 300k-key tree was deleted
//!   first). Every other rung starts with an empty or tiny free list, so this
//!   is the only one where the cost of drawing reused pages scales with the
//!   entry's length (the O(1) free-list front draw, SPEC 05 GC-19/20).
//! * `gc/ovf_across`, `gc/ovf_miss` — 6-page values written into a free list
//!   split over two freed-page records whose 3-page runs interleave
//!   (`across`: every fitting run spans both records) or are separated by
//!   live values (`miss`: no run fits, the file is extended). They time the
//!   overflow-run search when no single record holds a run (SPEC 05 GC-21):
//!   the cross-record merge on a hit, and its cost on a miss.

use criterion::{BatchSize, Criterion, Throughput};

use crate::data::{ascending_keys, os_page_size, shuffle, shuffled_keys, N, N_OVF, N_PROBE, VAL};
use crate::harness::{wr_fresh, wr_loaded, Backend, Fixture, Group, Seed};
use crate::{pair, pair_shape, Cfg};

/// Entry count for the value-size sweep — see `get`'s `N_VAL`, same reasoning:
/// held constant across the sweep so value width is the only variable.
const N_VAL: usize = 10_000;
const SEED_RAND: u64 = 0x1234_5678;

pub fn run(c: &mut Criterion, cfg: &Cfg) {
    family_order(c, cfg);
    family_val(c, cfg);
    family_api(c, cfg);
    family_over(c, cfg);
    family_gc(c, cfg);
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

    // The same overflow puts, aborted instead of committed: isolates the put
    // path's per-value cost (overflow-frame setup and value copy) from the
    // commit's file writes, which dominate `v2page` and its noise.
    let mut g = group(c, "put/val/v2page_abort", ovf_keys.len());
    pair!(
        wr_fresh,
        bulk_put_abort,
        &mut g,
        cfg.page,
        true,
        &ovf_keys,
        &val
    );
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

    // The same-size rewrite reached through the write cursor (`range_mut` +
    // `put_current`) instead of by key: read it against `over/same_size`.
    let rewrite_val = vec![0xCDu8; VAL];
    let mut g = group(c, "put/cursor/rewrite", hits.len());
    pair!(
        wr_loaded,
        cursor_rewrite,
        &mut g,
        &Seed::named(cfg.page, &keys, &seed_val),
        hits,
        &rewrite_val
    );
    g.finish();
}

/// Keys in the tree `gc/drain_big` builds before deleting its lower half.
const N_GC: usize = 300_000;
/// Overwrites in the timed txn, all in the surviving upper half.
const N_GC_TOUCH: usize = 20_000;
const SEED_GC: u64 = 0x6c63_6772_6169_6e21;

fn family_gc(c: &mut Criterion, cfg: &Cfg) {
    let val = vec![0xABu8; VAL];
    let keys = ascending_keys(N_GC);
    let mid = N_GC / 2;
    let touch: Vec<Vec<u8>> = shuffle(keys[mid..].to_vec(), SEED_GC)
        .into_iter()
        .take(N_GC_TOUCH)
        .collect();
    let mut g = group(c, "put/gc/drain_big", touch.len());
    pair_shape!(gc_drain_case, &mut g, cfg.page, &keys, mid, &touch, &val);
    g.finish();

    // Two freed-page records that each hold more ids than ZeroDB keeps in
    // its meta page (`(page - 176) / 8`), so both live in the free-page
    // database for both engines.
    let ps = cfg.page as usize;
    let per_record = (ps - 176) / 8 / OVF_FREED_PAGES + 64;
    let small = ovf_value(ps, OVF_FREED_PAGES);
    let big = ovf_value(ps, 2 * OVF_FREED_PAGES);
    for (name, stride) in [("put/gc/ovf_across", 2usize), ("put/gc/ovf_miss", 4)] {
        let keys = ascending_keys(per_record * stride);
        // Record 1 frees keys at offset 0 of each stride, record 2 at offset
        // `stride / 2`; with stride 4 the keys in between stay live.
        let frees: [Vec<Vec<u8>>; 2] =
            [0, stride / 2].map(|off| keys.iter().skip(off).step_by(stride).cloned().collect());
        let fresh: Vec<Vec<u8>> = (0..N_OVF_TOUCH as u64)
            .map(|i| (u64::MAX - i).to_be_bytes().to_vec())
            .collect();
        let mut g = group(c, name, fresh.len());
        pair_shape!(
            gc_ovf_case,
            &mut g,
            cfg.page,
            &keys,
            &small,
            &frees,
            &fresh,
            &big
        );
        g.finish();
    }
}

/// Pages per value the `gc/ovf_*` rungs free; their timed puts are twice as
/// long.
const OVF_FREED_PAGES: usize = 3;
/// Overflow values written in the timed txn of the `gc/ovf_*` rungs.
const N_OVF_TOUCH: usize = 200;

/// A value of exactly `pages` overflow pages at page size `ps` for both
/// engines: `pages - 1` full pages plus half a page leaves room for either
/// engine's overflow-page header.
fn ovf_value(ps: usize, pages: usize) -> Vec<u8> {
    vec![0xCDu8; (pages - 1) * ps + ps / 2]
}

/// Untimed: load `keys` with `small` values, delete each set of `frees` in
/// its own txn (one freed-page record each), then one more commit so both
/// pass LMDB's stricter reuse gate too. Timed: one txn putting `big` under
/// each of `fresh` (new keys) plus its commit.
fn gc_ovf_case<B: Backend>(
    g: &mut Group<'_>,
    page: u32,
    keys: &[Vec<u8>],
    small: &[u8],
    frees: &[Vec<Vec<u8>>; 2],
    fresh: &[Vec<u8>],
    big: &[u8],
) {
    g.bench_function(B::NAME, |b| {
        b.iter_batched(
            || {
                let f = Fixture::<B>::empty(page, Some("bench"), true);
                B::bulk_put(&f.env, f.db, keys, small);
                for set in frees {
                    B::delete_keys(&f.env, f.db, set);
                }
                B::bulk_put(&f.env, f.db, &keys[..1], small);
                f
            },
            |f| {
                B::bulk_put(&f.env, f.db, fresh, big);
                f
            },
            BatchSize::PerIteration,
        )
    });
}

/// Untimed: load `keys`, delete `keys[..mid]` in one txn (one large GC entry),
/// then one more commit so the entry passes LMDB's stricter reuse gate too
/// (F < oldest) as well as ZeroDB's. Timed: one txn overwriting `touch` (all
/// in the surviving half) plus its commit; its COW pages are drawn from that
/// entry. The fixture is returned, not dropped, so teardown is untimed.
fn gc_drain_case<B: Backend>(
    g: &mut Group<'_>,
    page: u32,
    keys: &[Vec<u8>],
    mid: usize,
    touch: &[Vec<u8>],
    val: &[u8],
) {
    g.bench_function(B::NAME, |b| {
        b.iter_batched(
            || {
                let f = Fixture::<B>::empty(page, Some("bench"), true);
                B::bulk_put(&f.env, f.db, keys, val);
                B::delete_range(&f.env, f.db, &keys[0], &keys[mid]);
                B::bulk_put(&f.env, f.db, &keys[mid..mid + 1], val);
                f
            },
            |f| {
                B::bulk_put(&f.env, f.db, touch, val);
                f
            },
            BatchSize::PerIteration,
        )
    });
}

/// A write group: element throughput, and the `heavy` criterion profile because
/// every iteration rebuilds its environment in untimed setup.
fn group<'a>(c: &'a mut Criterion, name: &str, elements: usize) -> crate::harness::Group<'a> {
    let mut g = c.benchmark_group(name);
    g.throughput(Throughput::Elements(elements as u64));
    crate::heavy(&mut g);
    g
}
