//! CRC32C (Castagnoli): a hardware path on aarch64 and an in-house software
//! table (slicing-by-8) everywhere else.
//!
//! Polynomial `0x1EDC6F41` (reflected form `0x82F6_3B78`), reflected input and
//! output, initial value `0xFFFF_FFFF`, final XOR `0xFFFF_FFFF`. This is the
//! algorithm mandated by [ADR-0002 §D1] and used for the mandatory meta-page CRC
//! (SPEC 02 §3.3).
//!
//! On aarch64 CPUs with the `crc` feature (every Graviton and Apple M-series
//! core; mandatory from ARMv8.1), the ARMv8 `crc32c*` instructions compute
//! exactly this polynomial, eight bytes per instruction ([ADR-0031]). The
//! feature is detected at run time, because the `aarch64-unknown-linux-gnu`
//! target does not enable it at compile time. The table path serves every
//! other CPU and miri, and both must agree bit for bit with the byte-wise
//! reference below.
//!
//! In-house rather than a dependency (ADR-0002 §D1: no CRC crate is on the
//! allowlist).
//!
//! [ADR-0002 §D1]: ../../../../docs/adr/0002-on-disk-format.md
//! [ADR-0031]: ../../../../docs/adr/0031-hardware-crc32c.md

/// Reflected CRC32C generator polynomial: bit-reversal of `0x1EDC6F41`.
const POLY_REFLECTED: u32 = 0x82F6_3B78;

/// Precomputed lookup tables for the slicing-by-8 algorithm, built at compile
/// time so there is no runtime initialization cost and they land in read-only
/// static memory (8 KiB).
///
/// `TABLES[0]` is the classic byte-wise table: the CRC of each single byte.
/// `TABLES[k][b]` is the CRC contribution of byte `b` followed by `k` zero
/// bytes, so eight bytes can be folded with eight independent lookups instead
/// of a chain of eight dependent ones.
static TABLES: [[u32; 256]; 8] = build_tables();

const fn build_tables() -> [[u32; 256]; 8] {
    let mut tables = [[0u32; 256]; 8];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ POLY_REFLECTED;
            } else {
                crc >>= 1;
            }
            bit += 1;
        }
        tables[0][i] = crc;
        i += 1;
    }
    let mut k = 1usize;
    while k < 8 {
        let mut i = 0usize;
        while i < 256 {
            let prev = tables[k - 1][i];
            tables[k][i] = (prev >> 8) ^ tables[0][(prev & 0xFF) as usize];
            i += 1;
        }
        k += 1;
    }
    tables
}

/// Fold `bytes` into a running CRC32C state one byte at a time (the reference
/// algorithm; also the tail of [`crc_update`]).
#[inline]
fn crc_update_bytewise(mut crc: u32, bytes: &[u8]) -> u32 {
    for &byte in bytes {
        let idx = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ TABLES[0][idx];
    }
    crc
}

/// Fold `bytes` into a running (non-finalized) CRC32C state, on the hardware
/// path when this CPU has it and on the table path otherwise. The single entry
/// point shared by [`crc32c`] and [`crc32c_concat`], so the two cannot drift.
#[inline]
fn crc_update(crc: u32, bytes: &[u8]) -> u32 {
    #[cfg(all(target_arch = "aarch64", not(miri)))]
    if std::arch::is_aarch64_feature_detected!("crc") {
        // ADR-0031: the one sanctioned `unsafe` in this module.
        // SAFETY: `crc_update_hw`'s only precondition is the `crc` CPU feature
        // its `#[target_feature]` names, and this CPU has it (detected on the
        // line above). Its body does no pointer or memory-unsafe work.
        #[allow(unsafe_code)]
        return unsafe { crc_update_hw(crc, bytes) };
    }
    crc_update_table(crc, bytes)
}

/// The hardware path: the ARMv8 CRC32C instructions, eight bytes per
/// `crc32cx` and the trailing `len % 8` bytes one `crc32cb` each. Callable
/// only where the `crc` feature is present (see [`crc_update`]).
#[cfg(all(target_arch = "aarch64", not(miri)))]
#[target_feature(enable = "crc")]
fn crc_update_hw(mut crc: u32, bytes: &[u8]) -> u32 {
    use std::arch::aarch64::{__crc32cb, __crc32cd};
    let (words, rest) = bytes.as_chunks::<8>();
    for w in words {
        // Little-endian, so the word's first byte is the first one folded,
        // as the reflected CRC consumes the stream.
        crc = __crc32cd(crc, u64::from_le_bytes(*w));
    }
    for &byte in rest {
        crc = __crc32cb(crc, byte);
    }
    crc
}

/// The table path, slicing-by-8: each 8-byte block is XORed into the state and folded through
/// eight table lookups that do not depend on each other, then the last
/// `len % 8` bytes go through the byte-wise step. The result is bit-identical
/// to the byte-wise algorithm (pinned by the tests below), about five times
/// faster on the meta CRC that every commit computes.
#[inline]
fn crc_update_table(mut crc: u32, bytes: &[u8]) -> u32 {
    let (blocks, tail) = bytes.as_chunks::<8>();
    for b in blocks {
        let lo = u32::from_le_bytes([b[0], b[1], b[2], b[3]]) ^ crc;
        let hi = u32::from_le_bytes([b[4], b[5], b[6], b[7]]);
        crc = TABLES[7][(lo & 0xFF) as usize]
            ^ TABLES[6][((lo >> 8) & 0xFF) as usize]
            ^ TABLES[5][((lo >> 16) & 0xFF) as usize]
            ^ TABLES[4][(lo >> 24) as usize]
            ^ TABLES[3][(hi & 0xFF) as usize]
            ^ TABLES[2][((hi >> 8) & 0xFF) as usize]
            ^ TABLES[1][((hi >> 16) & 0xFF) as usize]
            ^ TABLES[0][(hi >> 24) as usize];
    }
    crc_update_bytewise(crc, tail)
}

/// Compute the CRC32C (Castagnoli) checksum of `data`.
///
/// Returns the finalized checksum (post final-XOR), i.e. the value written to a
/// meta page's `meta_crc` field.
#[must_use]
pub fn crc32c(data: &[u8]) -> u32 {
    crc_update(0xFFFF_FFFF, data) ^ 0xFFFF_FFFF
}

/// CRC32C over the concatenation of `parts`, as one stream — equal to
/// `crc32c` of the parts joined into a single buffer, without the join.
/// Used for the meta CRC's split coverage (SPEC 02 §3.3,
/// ADR-0022: `[0, 172)` then the annex ids, skipping the CRC field itself).
#[must_use]
pub fn crc32c_concat(parts: &[&[u8]]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for part in parts {
        crc = crc_update(crc, part);
    }
    crc ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::{crc32c, crc32c_concat, crc_update, crc_update_bytewise, crc_update_table};
    use proptest::prelude::*;

    /// Proptest config that also runs under miri: failure persistence writes a
    /// regressions file (it calls `getcwd`, which miri's isolation refuses),
    /// and miri is ~1000x slower, so it gets fewer cases.
    fn prop_config() -> ProptestConfig {
        ProptestConfig {
            failure_persistence: None,
            cases: if cfg!(miri) { 16 } else { 256 },
            ..ProptestConfig::default()
        }
    }

    /// Every length from 0 to 40 (each block/remainder split) and the meta
    /// CRC's own lengths: the dispatched path (hardware where this CPU has
    /// it, else slicing-by-8) equals the byte-wise reference.
    #[test]
    fn sliced_equals_bytewise_every_split() {
        let data: Vec<u8> = (0..400u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        let lens = (0..=40).chain([171, 172, 173, 180, 196, 400]);
        for len in lens {
            for start in [0usize, 1, 3, 7] {
                let s = &data[start..start + len.min(data.len() - start)];
                assert_eq!(
                    crc_update(0xFFFF_FFFF, s),
                    crc_update_bytewise(0xFFFF_FFFF, s),
                    "len {len}, start {start}"
                );
            }
        }
    }

    proptest! {
        #![proptest_config(prop_config())]
        /// Any bytes from any starting state: the dispatched path equals the
        /// byte-wise reference, and a split stream equals the joined one.
        #[test]
        fn sliced_equals_bytewise(
            data in proptest::collection::vec(any::<u8>(), 0..=300),
            init in any::<u32>(),
            cut in any::<usize>(),
        ) {
            prop_assert_eq!(crc_update(init, &data), crc_update_bytewise(init, &data));
            let cut = cut % (data.len() + 1);
            prop_assert_eq!(crc32c_concat(&[&data[..cut], &data[cut..]]), crc32c(&data));
        }
    }

    // Known-answer vectors. The four fixed-block vectors are from RFC 3720
    // Appendix B.4 (iSCSI CRC32C); "123456789" is the canonical CRC check value;
    // the empty input is 0 by definition (init XOR final).
    #[test]
    fn kat_empty() {
        assert_eq!(crc32c(b""), 0x0000_0000);
    }

    #[test]
    fn kat_check_string() {
        // The standard CRC "check" value for CRC-32C/CASTAGNOLI.
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn kat_rfc3720_zeros() {
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
    }

    #[test]
    fn kat_rfc3720_ones() {
        assert_eq!(crc32c(&[0xFFu8; 32]), 0x62A8_AB43);
    }

    #[test]
    fn kat_rfc3720_incrementing() {
        let mut buf = [0u8; 32];
        for (i, b) in buf.iter_mut().enumerate() {
            *b = i as u8;
        }
        assert_eq!(crc32c(&buf), 0x46DD_794E);
    }

    #[test]
    fn kat_rfc3720_decrementing() {
        let mut buf = [0u8; 32];
        for (i, b) in buf.iter_mut().enumerate() {
            *b = (31 - i) as u8;
        }
        assert_eq!(crc32c(&buf), 0x113F_DB5C);
    }

    #[test]
    fn deterministic_and_content_sensitive() {
        let a = crc32c(b"The quick brown fox jumps over the lazy dog");
        let b = crc32c(b"The quick brown fox jumps over the lazy dog");
        assert_eq!(a, b, "same input must yield the same checksum");
        // A single-bit change must change the checksum.
        assert_ne!(a, crc32c(b"The quick brown fox jumps over the lazy dov"));
    }

    /// The table path on its own (the fallback, and what miri and x86-64
    /// run) equals the byte-wise reference, also on CPUs whose dispatched
    /// path is the hardware one: every length 0–300 from every block offset,
    /// from three starting states, and whole 4 and 16 KiB pages.
    #[test]
    fn table_path_equals_bytewise() {
        let data: Vec<u8> = (0..16 * 1024 + 16u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        let max_len = if cfg!(miri) { 40 } else { 300 };
        for start in 0..8 {
            for len in 0..=max_len {
                let s = &data[start..start + len];
                for init in [0xFFFF_FFFF, 0, 0x1234_5678] {
                    assert_eq!(
                        crc_update_table(init, s),
                        crc_update_bytewise(init, s),
                        "start {start} len {len} init {init:#x}"
                    );
                }
            }
        }
        for len in [4096, 16 * 1024] {
            let s = &data[..len];
            assert_eq!(
                crc_update_table(0xFFFF_FFFF, s),
                crc_update_bytewise(0xFFFF_FFFF, s)
            );
        }
    }

    /// ADR-0031: where this CPU has the `crc` feature, the hardware path and
    /// the table path agree on every length up to 300 bytes from every offset,
    /// from three starting states, and on whole pages up to 64 KiB. On other
    /// CPUs the test checks nothing: the table path is the only path.
    #[cfg(all(target_arch = "aarch64", not(miri)))]
    #[test]
    fn hardware_path_matches_table_path() {
        if !std::arch::is_aarch64_feature_detected!("crc") {
            return;
        }
        let data: Vec<u8> = (0..64 * 1024 + 16u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        let hw = |crc: u32, s: &[u8]| {
            // SAFETY: the `crc` feature was detected at the top of this test.
            #[allow(unsafe_code)]
            unsafe {
                super::crc_update_hw(crc, s)
            }
        };
        for start in 0..16 {
            for len in 0..=300 {
                let s = &data[start..start + len];
                for init in [0xFFFF_FFFF, 0, 0x1234_5678] {
                    assert_eq!(
                        hw(init, s),
                        crc_update_table(init, s),
                        "start {start} len {len} init {init:#x}"
                    );
                }
            }
        }
        for len in [4096 - 176, 4096, 16 * 1024, 64 * 1024] {
            let s = &data[..len];
            assert_eq!(
                hw(0xFFFF_FFFF, s),
                crc_update_table(0xFFFF_FFFF, s),
                "len {len}"
            );
        }
    }
}
