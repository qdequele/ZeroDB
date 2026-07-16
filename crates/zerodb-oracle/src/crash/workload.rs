//! Seeded crash workloads (ADR-0008 D2): the oracle [`Op`] model decoded from
//! a deterministic PRNG byte stream, with value inflation to reach REC-21's
//! 0 B–16 MB band, plus the per-cycle durability-mode pick (SPEC 06 §3).

use zerodb::EnvFlags;
use zerodb_core::env::DurabilityFlags;
use zerodb_io::fault::{splitmix64, Rng};

use crate::{decode_ops, Op};

/// Durability mode of one crash cycle (SPEC 06 REC-9..12; ADR-0008 D4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Both barriers run — loses nothing on crash (REC-6).
    Default,
    /// `WRITE_MAP`: same barriers via msync — REC-6 holds verbatim (REC-12).
    WriteMap,
    /// `NO_META_SYNC`: data barrier only; may lose recent commits'
    /// *visibility*, never corruption (REC-10).
    NoMetaSync,
    /// `NO_SYNC`: no barriers; structural consistency only under ordered
    /// writeback (REC-11) — verified under the ordered sub-model, only
    /// characterized under the adversarial one (ratified OQ3).
    NoSync,
    /// `WRITE_MAP | MAP_ASYNC`: async msync — same window as `NO_SYNC`
    /// (REC-9/REC-11).
    MapAsync,
}

impl Mode {
    /// Core durability flags for [`zerodb_core::env::open_with_backing`]
    /// (mechanism A).
    #[must_use]
    pub fn durability(self) -> DurabilityFlags {
        match self {
            Mode::Default => DurabilityFlags::default(),
            Mode::WriteMap => DurabilityFlags {
                write_map: true,
                ..DurabilityFlags::default()
            },
            Mode::NoMetaSync => DurabilityFlags {
                no_meta_sync: true,
                ..DurabilityFlags::default()
            },
            Mode::NoSync => DurabilityFlags {
                no_sync: true,
                ..DurabilityFlags::default()
            },
            Mode::MapAsync => DurabilityFlags {
                write_map: true,
                map_async: true,
                ..DurabilityFlags::default()
            },
        }
    }

    /// Public env flags for the mechanism-B child's real open.
    #[must_use]
    pub fn env_flags(self) -> EnvFlags {
        match self {
            Mode::Default => EnvFlags::EMPTY,
            Mode::WriteMap => EnvFlags::WRITE_MAP,
            Mode::NoMetaSync => EnvFlags::NO_META_SYNC,
            Mode::NoSync => EnvFlags::NO_SYNC,
            Mode::MapAsync => EnvFlags::WRITE_MAP | EnvFlags::MAP_ASYNC,
        }
    }

    /// Modes whose acknowledged commits are barrier-covered before `commit`
    /// returns: assert REC-18.4 monotonic durability (`floor ≥ acked`).
    #[must_use]
    pub fn strict_ack(self) -> bool {
        matches!(self, Mode::Default | Mode::WriteMap)
    }

    /// Modes where at most one commit separates the durable floor from the
    /// issued ceiling (`ceil − floor ≤ 1`): default/writemap trivially;
    /// `NO_META_SYNC` because every C3 data barrier folds the *previous*
    /// commit's pending meta write too (`fdatasync` covers the whole file,
    /// REC-10). These run the full adversarial fault model **with full
    /// REC-18 verification** — REC-9 promises corruption-freedom here.
    #[must_use]
    pub fn bounded_window(self) -> bool {
        matches!(self, Mode::Default | Mode::WriteMap | Mode::NoMetaSync)
    }

    /// Reporting name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Mode::Default => "default",
            Mode::WriteMap => "writemap",
            Mode::NoMetaSync => "nometasync",
            Mode::NoSync => "nosync",
            Mode::MapAsync => "mapasync",
        }
    }
}

/// One cycle's workload parameters, fully derived from the cycle seed —
/// mechanism-B parent and child, and any repro run, derive identical specs.
#[derive(Debug, Clone)]
pub struct Spec {
    /// Cycle seed (everything below is a pure function of it).
    pub seed: u64,
    /// Durability mode.
    pub mode: Mode,
    /// Env page size.
    pub page_size: u32,
    /// Env map size.
    pub map_size: u64,
    /// Whether this is a big-value cycle (one multi-MB overflow value,
    /// REC-21's 16 MB end of the band).
    pub big: bool,
}

/// Ops decoded per workload round (mechanism B loops rounds so an async kill
/// lands mid-run; mechanism A uses round 0).
pub const OPS_PER_ROUND: usize = 96;

/// Derive the cycle spec from its seed.
#[must_use]
pub fn gen_spec(seed: u64) -> Spec {
    let mut rng = Rng::new(splitmix64(seed ^ 0x0000_57EC_0000_57EC));
    let mode = match rng.below(100) {
        0..=39 => Mode::Default,
        40..=54 => Mode::WriteMap,
        55..=69 => Mode::NoMetaSync,
        70..=84 => Mode::NoSync,
        _ => Mode::MapAsync,
    };
    let page_size = if rng.ratio(1, 8) { 8192 } else { 4096 };
    let big = rng.ratio(1, 48);
    // Generous maps: `MapFull` is unmodeled (cycles abandon on it) and the
    // fault backend's memory tracks *usage*, not the map (ADR-0008 D1).
    let map_size: u64 = if big { 128 << 20 } else { 64 << 20 };
    Spec {
        seed,
        mode,
        page_size,
        map_size,
        big,
    }
}

impl Spec {
    /// The ops of workload round `round` — decoded from a deterministic byte
    /// stream through the oracle's shared [`decode_ops`] (the same `Arbitrary`
    /// shapes the differential fuzz drives), then value-inflated so overflow
    /// runs (64 KiB..256 KiB, ~1/16) and the multi-MB band (one value up to
    /// 16 MiB on big cycles) are exercised per REC-21.
    #[must_use]
    pub fn ops_for_round(&self, round: u32) -> Vec<Op> {
        let mut rng = Rng::new(splitmix64(
            self.seed ^ (0x0125_0000_0000 + u64::from(round)),
        ));
        let mut buf = vec![0u8; 4096];
        rng.fill(&mut buf);
        let mut ops = decode_ops(&buf, OPS_PER_ROUND);
        let mut did_big = false;
        for op in &mut ops {
            let val = match op {
                Op::Put { val, .. } | Op::PutFlagged { val, .. } | Op::PutReserved { val, .. } => {
                    val
                }
                _ => continue,
            };
            if self.big && round == 0 && !did_big && rng.ratio(1, 4) {
                let len = 1 + rng.below(16 << 20);
                let mut v = vec![0u8; len];
                rng.fill(&mut v);
                val.0 = v;
                did_big = true;
            } else if rng.ratio(1, 16) {
                let len = (64 << 10) + rng.below(192 << 10);
                let mut v = vec![0u8; len];
                rng.fill(&mut v);
                val.0 = v;
            }
        }
        ops
    }
}
