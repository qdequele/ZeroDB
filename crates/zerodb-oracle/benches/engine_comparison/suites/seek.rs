//! Suite `seek` — `MDB_SET_RANGE` positioning, the operation milli's filter and
//! facet paths lean on hardest.
//!
//! Why it is not folded into `get`: a `get` that misses can stop at the leaf,
//! but a seek must *settle* on the first key ≥ the probe, which on a boundary
//! means stepping to the next leaf. And unlike `scan`, consecutive seeks share
//! no cursor state, so nothing a leaf memo caches survives between probes.
//!
//! Ladder: `ge/rand` is the scattered case (no locality to exploit); `ge/seq`
//! walks the probes in ascending order, so an engine that keeps per-level state
//! across seeks can show it here and nowhere else. The `seq / rand` ratio, read
//! per engine, is that locality benefit — a gap between the two engines'
//! ratios points at cursor state, not at descent cost.

use criterion::{Criterion, Throughput};

use crate::data::{ascending_keys, missing_keys, probes, N, N_PROBE, VAL};
use crate::harness::{ro_probe, Seed};
use crate::{pair, Cfg};

const SEED_SEEK: u64 = 0x2468_ACE0;
const SEED_GAP: u64 = 0x9876_5432;

pub fn run(c: &mut Criterion, cfg: &Cfg) {
    let keys = ascending_keys(N);
    let val = vec![0xABu8; VAL];

    // Scattered probes that land exactly on existing keys.
    let rand = probes(&keys, N_PROBE, SEED_SEEK);
    rung(c, cfg, "seek/ge/rand", &keys, &val, &rand);

    // The same count of probes in ascending order.
    let mut seq = rand.clone();
    seq.sort();
    rung(c, cfg, "seek/ge/seq", &keys, &val, &seq);

    // Probes that fall BETWEEN keys, so every seek has to settle forward rather
    // than land on an exact match — the boundary-crossing cost.
    let gaps: Vec<Vec<u8>> = missing_keys(N_PROBE, SEED_GAP)
        .iter()
        .map(|k| {
            // Fold the random key back into the populated range, then bias it
            // off an exact key so the probe sits in a gap.
            let v = u64::from_be_bytes(k[..8].try_into().expect("8 bytes"));
            let inside = v % (N as u64 - 1);
            let mut b = inside.to_be_bytes().to_vec();
            b.push(0x01); // strictly greater than key `inside`, less than `inside+1`
            b
        })
        .collect();
    rung(c, cfg, "seek/ge/gap", &keys, &val, &gaps);
}

fn rung(c: &mut Criterion, cfg: &Cfg, name: &str, keys: &[Vec<u8>], val: &[u8], p: &[Vec<u8>]) {
    let mut g = c.benchmark_group(name);
    g.throughput(Throughput::Elements(p.len() as u64));
    pair!(
        ro_probe,
        seek_ge,
        &mut g,
        &Seed::named(cfg.page, keys, val),
        p
    );
    g.finish();
}
