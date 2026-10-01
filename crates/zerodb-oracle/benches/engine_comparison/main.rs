//! Dual-backend microbenchmark ladder: **zerodb** (the native engine, through
//! the `heed-zerodb` adapter) vs the **Meilisearch LMDB fork** (`heed` =0.22.1),
//! both linked into this one binary via the oracle's existing dual dependency.
//! Same two-backends-in-one-process setup the differential fuzzer uses, reused
//! for timing instead of correctness.
//!
//! ## What this harness is for
//! Not "is zerodb fast?" — `just consumer-bench` and `just hannoy-bench` answer
//! that at the consumer level. This answers **where** the two engines diverge
//! and **which mechanism** is responsible, by arranging the rungs as a *ladder*:
//! adjacent rungs differ by exactly one mechanism, so a ratio that jumps between
//! two rungs names the cost. `docs/BENCH-MAP.md` maps every rung to the
//! mechanism it isolates and the `docs/PERF-GAP-VS-LMDB.md` item it implicates.
//!
//! ## Fair by construction
//! * **Identical operation bodies** — `backend.rs` holds one macro body expanded
//!   over the two API-identical crate paths, so neither engine can be given a
//!   hand-written advantage.
//! * **Identical data** — deterministic, seeded (no `rand`), so a Graviton run
//!   compares the same bytes in the same order as a laptop run.
//! * **Identical page size** — LMDB is locked to the OS page size and offers no
//!   selector, so zerodb is pinned to that same value. Without this a
//!   4 KiB-vs-16 KiB geometry gap would swamp the engine comparison.
//! * **Named databases by default** — exactly how milli/hannoy use the store.
//!   The `get/db/root` rung is the deliberate exception: it is what the named
//!   rungs are measured *against*.
//! * **Matched durability** — rungs are `nosync` unless the name says `sync`.
//!
//! ## Suites (`just bench <suite>`)
//! `env` `get` `scan` `seek` `put` `del` `commit` `mixed` `concurrent` `maint`
//!
//! ## Tiers
//! Default runs every suite at small/medium sizes. `ZERODB_BENCH_TIER=long`
//! (`just bench-long`) additionally enables the 1M-entry depth rung and the
//! concurrent suite. Tiering is a runtime skip, not a `cfg`, so both tiers
//! compile the same code.
//!
//! ## Reading the numbers
//! macOS numbers are **indicative**: they isolate CPU/allocator/memcpy and a
//! laptop-SSD fsync. The claim that matters — linux-aarch64 (Graviton) + EBS
//! gp3, where a page fault or barrier is a network round-trip — must be run
//! there. This harness runs unchanged on that hardware; only the medians move.
//! `scripts/bench-report.py` turns a completed run into the ratio table.

mod backend;
mod data;
mod harness;
mod suites;

use criterion::{criterion_group, criterion_main, Criterion};

/// Which rungs to run. Tiering is a runtime skip so both tiers compile
/// identically — a `cfg` would let the long rungs rot.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    /// Every suite, small/medium sizes. Minutes, not hours.
    Default,
    /// Adds the 1M-entry depth rung and the whole `concurrent` suite.
    Long,
}

/// Geometry + tier, resolved once and threaded through every suite.
#[derive(Clone, Copy)]
pub struct Cfg {
    /// DB page size, pinned to the OS page size for both engines.
    pub page: u32,
    /// Which rungs are enabled.
    pub tier: Tier,
}

impl Cfg {
    fn resolve() -> Cfg {
        let tier = match std::env::var("ZERODB_BENCH_TIER").as_deref() {
            Ok("long") => Tier::Long,
            _ => Tier::Default,
        };
        Cfg {
            page: data::os_page_size(),
            tier,
        }
    }

    /// Whether `long`-tier rungs run.
    pub fn long(&self) -> bool {
        self.tier == Tier::Long
    }
}

/// Criterion settings for rungs whose *setup* dominates (a fresh, populated env
/// per iteration). Defaults would spend most of the run in untimed setup.
pub fn heavy(g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    g.sample_size(10)
        .warm_up_time(std::time::Duration::from_secs(1))
        .measurement_time(std::time::Duration::from_secs(8));
}

/// Criterion settings for rungs that touch the disk barrier (sync commits).
pub fn durable(g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    g.sample_size(10)
        .warm_up_time(std::time::Duration::from_millis(500))
        .measurement_time(std::time::Duration::from_secs(10));
}

fn all_suites(c: &mut Criterion) {
    let cfg = Cfg::resolve();
    suites::env::run(c, &cfg);
    suites::get::run(c, &cfg);
    suites::scan::run(c, &cfg);
    suites::seek::run(c, &cfg);
    suites::put::run(c, &cfg);
    suites::del::run(c, &cfg);
    suites::commit::run(c, &cfg);
    suites::mixed::run(c, &cfg);
    suites::concurrent::run(c, &cfg);
    suites::maint::run(c, &cfg);
}

criterion_group!(benches, all_suites);
criterion_main!(benches);
