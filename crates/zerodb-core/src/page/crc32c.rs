//! CRC32C (Castagnoli) — in-house software table implementation.
//!
//! Polynomial `0x1EDC6F41` (reflected form `0x82F6_3B78`), reflected input and
//! output, initial value `0xFFFF_FFFF`, final XOR `0xFFFF_FFFF`. This is the
//! algorithm mandated by [ADR-0002 §D1] and used for the mandatory meta-page CRC
//! (SPEC 02 §3.3). Phase 1 uses this software table; Phase 3.9 will switch to
//! the ARMv8-A `crc32c*` hardware instructions, which compute exactly this
//! polynomial and must agree bit-for-bit with this table.
//!
//! No new dependency is used (CLAUDE.md / ADR-0002 §D1: an in-house table is
//! mandated because no CRC crate is on the allowlist).
//!
//! [ADR-0002 §D1]: ../../../../docs/adr/0002-on-disk-format.md

/// Reflected CRC32C generator polynomial: bit-reversal of `0x1EDC6F41`.
const POLY_REFLECTED: u32 = 0x82F6_3B78;

/// Precomputed 256-entry lookup table for the reflected byte-wise algorithm.
///
/// Built at compile time so there is no runtime initialization cost and the
/// table lands in read-only static memory.
const TABLE: [u32; 256] = build_table();

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
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
        table[i] = crc;
        i += 1;
    }
    table
}

/// Compute the CRC32C (Castagnoli) checksum of `data`.
///
/// Returns the finalized checksum (post final-XOR), i.e. the value written to a
/// meta page's `meta_crc` field.
#[must_use]
pub fn crc32c(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        let idx = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ TABLE[idx];
    }
    crc ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::crc32c;

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
}
