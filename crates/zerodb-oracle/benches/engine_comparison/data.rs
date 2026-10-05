//! Deterministic workload data + geometry, shared by every suite.
//!
//! No `rand`: every key set is derived from a seeded splitmix64 so a laptop run
//! and a Graviton run compare the *same* bytes in the *same* order.

/// 1 GiB sparse map — larger than any dataset here, and a multiple of every
/// supported page size (4K/8K/16K/32K/64K) so LMDB's OS-page-multiple rule and
/// zerodb's own `map_size` check both accept it.
pub const MAP: usize = 1 << 30;

/// The reference dataset size. Every ladder rung that is not itself sweeping
/// dataset size uses this, so rungs are comparable to one another.
pub const N: usize = 50_000;
/// Small rung of the dataset-size sweep (fits in a shallow tree).
pub const N_SMALL: usize = 1_000;
/// Large rung of the dataset-size sweep — `long` tier only.
pub const N_LARGE: usize = 1_000_000;

/// Single-put txns for the commit-cost rungs (each fsyncs in sync mode).
pub const N_COMMIT: usize = 200;
/// Keys for the overflow rungs (each value spans > 1 page → overflow pages).
pub const N_OVF: usize = 2_000;
/// Probe count for the read rungs that would otherwise be too quick to time.
pub const N_PROBE: usize = 10_000;
/// Named databases in the multi-DB rungs (milli opens on this order).
pub const N_DBS: usize = 8;

/// The reference inline value size (bytes).
pub const VAL: usize = 256;

/// splitmix64 — a tiny deterministic PRNG so shuffles are reproducible across
/// platforms without pulling in `rand`.
pub fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// `n` ascending 8-byte big-endian keys — the sequential / append-friendly case.
pub fn ascending_keys(n: usize) -> Vec<Vec<u8>> {
    (0..n as u64).map(|i| i.to_be_bytes().to_vec()).collect()
}

/// `n` ascending keys padded out to `width` bytes (>= 8): the 8-byte big-endian
/// counter stays the *prefix*, so ordering is unchanged and the only variable is
/// how many bytes each comparison and each cell has to move.
pub fn ascending_keys_wide(n: usize, width: usize) -> Vec<Vec<u8>> {
    assert!(width >= 8, "key width must cover the 8-byte counter");
    (0..n as u64)
        .map(|i| {
            let mut k = vec![0xA5u8; width];
            k[..8].copy_from_slice(&i.to_be_bytes());
            k
        })
        .collect()
}

/// The same key set in a deterministic shuffled order (random-access pattern).
pub fn shuffled_keys(n: usize, seed: u64) -> Vec<Vec<u8>> {
    shuffle(ascending_keys(n), seed)
}

/// Deterministic Fisher-Yates over an owned key set.
pub fn shuffle(mut keys: Vec<Vec<u8>>, seed: u64) -> Vec<Vec<u8>> {
    let mut s = seed;
    for i in (1..keys.len()).rev() {
        let j = (splitmix64(&mut s) % (i as u64 + 1)) as usize;
        keys.swap(i, j);
    }
    keys
}

/// `n` keys that are guaranteed *absent* from `ascending_keys(m)` for any
/// `m <= u32::MAX`: the high half of the u64 space, shuffled.
pub fn missing_keys(n: usize, seed: u64) -> Vec<Vec<u8>> {
    let mut s = seed;
    (0..n)
        .map(|_| {
            let v = splitmix64(&mut s) | (1u64 << 63);
            v.to_be_bytes().to_vec()
        })
        .collect()
}

/// Take `n` entries of `keys` in a deterministic scattered order — the probe set
/// for read rungs where the dataset is larger than the probe count.
pub fn probes(keys: &[Vec<u8>], n: usize, seed: u64) -> Vec<Vec<u8>> {
    let mut s = seed;
    (0..n)
        .map(|_| keys[(splitmix64(&mut s) % keys.len() as u64) as usize].clone())
        .collect()
}

/// Probes for the multi-database rungs.
///
/// `Fixture::multi` deals key `i` into database `i % dbs`, and
/// `point_get_multi` dispatches the probe at position `p` to database
/// `p % dbs`. So the probe at position `p` MUST be a key whose index is
/// congruent to `p` modulo `dbs`, or the rung silently measures misses.
///
/// Within that constraint the order is scattered, which is the point: the
/// `get/db` family holds the access pattern fixed and moves only how the
/// database handle is resolved. Handing this rung sequential probes would give
/// it locality its family peers do not have, and the ladder step would measure
/// locality instead of resolution.
pub fn round_robin_probes(keys: &[Vec<u8>], dbs: usize, count: usize, seed: u64) -> Vec<Vec<u8>> {
    let mut buckets: Vec<Vec<&Vec<u8>>> = vec![Vec::new(); dbs];
    for (i, k) in keys.iter().enumerate() {
        buckets[i % dbs].push(k);
    }
    let mut s = seed;
    (0..count)
        .map(|p| {
            let b = &buckets[p % dbs];
            b[(splitmix64(&mut s) % b.len() as u64) as usize].clone()
        })
        .collect()
}

/// The OS page size (what LMDB is locked to), clamped to zerodb's supported
/// `[4096, 65536]` power-of-two range so zerodb can be pinned to the same value.
pub fn os_page_size() -> u32 {
    // SAFETY: `sysconf(_SC_PAGESIZE)` is a pure query — no preconditions, no side
    // effects. FFI in the oracle crate is sanctioned by the AGENTS.md unsafe
    // policy; this is bench-only code.
    let v = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    match u32::try_from(v) {
        Ok(p) if (4096..=65536).contains(&p) && p.is_power_of_two() => p,
        _ => 4096,
    }
}

/// Names for the multi-DB rungs. Stable and ordered so the "last DB" probe hits
/// the same catalog position on both engines.
pub fn db_names() -> Vec<String> {
    (0..N_DBS).map(|i| format!("bench{i}")).collect()
}
