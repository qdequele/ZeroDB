//! File geometry helpers — page-size validation, map sizing, the `MapFull`
//! boundary, the inline-vs-overflow threshold, overflow run math, and the GC-DB
//! big-endian txnid key codec (SPEC 02 §4.2, §5, §7, §8).

use super::{PageError, HEADER_SIZE, MAX_PAGE_SIZE, MIN_PAGE_SIZE};

/// Validate a page size: a power of two in `[MIN_PAGE_SIZE, MAX_PAGE_SIZE]`
/// (SPEC 02 §0, §3.2 rule 3).
///
/// # Errors
///
/// [`PageError::InvalidPageSize`] if `psize` is out of range or not a power of
/// two.
pub fn validate_page_size(psize: u32) -> Result<(), PageError> {
    if !(MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&psize) || !psize.is_power_of_two() {
        return Err(PageError::InvalidPageSize(psize));
    }
    Ok(())
}

/// Usable body size of a page: `psize - HEADER_SIZE`.
///
/// Callers must pass a validated `psize`.
#[must_use]
pub fn body_size(psize: u32) -> usize {
    psize as usize - HEADER_SIZE
}

/// Number of pages the map can hold: `map_size / psize` (SPEC 02 §8). Valid page
/// numbers are `0 .. map_pages - 1`.
///
/// Callers must pass a validated `psize` (`> 0`).
#[must_use]
pub fn map_pages(map_size: u64, psize: u32) -> u64 {
    map_size / psize as u64
}

/// The `MAP_FULL` predicate (SPEC 02 §8, SPEC 05 GC-17).
///
/// Allocating a run of `n` pages by extending the file hands out
/// `[next_pgno .. next_pgno + n)` and would make the new high-water
/// `next_pgno + n`. The allocation fails with `MapFull` iff that end exceeds the
/// map: `next_pgno + n > map_pages`. Equivalently, the run's last page
/// `next_pgno + n - 1` must be `<= map_pages - 1`; a run that would *end past*
/// the map is rejected, but a run that exactly fills it is allowed.
///
/// Uses saturating arithmetic so an absurd `next_pgno`/`n` cannot overflow;
/// saturation to `u64::MAX` still compares as "full".
#[must_use]
pub fn is_map_full(next_pgno: u64, n: u64, map_pages: u64) -> bool {
    next_pgno.saturating_add(n) > map_pages
}

/// The largest cell that still guarantees two entries fit on a page — ZeroDB's
/// analogue of LMDB's `me_nodemax` (SPEC 02 §4.2, ADR-0002 §D5):
///
/// ```text
/// max_node_size = ((psize - HEADER_SIZE) / 2 rounded DOWN to even) - 2
/// ```
///
/// Callers must pass a validated `psize`.
#[must_use]
pub fn max_node_size(psize: u32) -> usize {
    let half = (psize as usize - HEADER_SIZE) / 2;
    let half_even = half & !1; // round down to even
    half_even - 2
}

/// Decide whether a value is stored **inline** in the leaf cell (vs. on an
/// overflow run), per SPEC 02 §4.2: inline iff the pre-padding inline leaf cell
/// `8 + ksize + dsize` is `<= max_node_size(psize)`.
///
/// Callers must pass a validated `psize`.
#[must_use]
pub fn value_is_inline(ksize: usize, dsize: u64, psize: u32) -> bool {
    // Use u64 throughout so a huge dsize cannot overflow usize on 32-bit.
    let inline_cell = 8u64 + ksize as u64 + dsize;
    inline_cell <= max_node_size(psize) as u64
}

/// Number of contiguous pages an overflow run needs to hold `dsize` value bytes
/// (SPEC 02 §5): `ceil((HEADER_SIZE + dsize) / psize)`.
///
/// The result is always `>= 1`. Callers must pass a validated `psize`.
#[must_use]
pub fn overflow_page_count(dsize: u64, psize: u32) -> u64 {
    let total = HEADER_SIZE as u64 + dsize;
    total.div_ceil(psize as u64)
}

/// Total value-payload capacity of an `n`-page overflow run (SPEC 02 §5):
/// `n * psize - HEADER_SIZE`.
///
/// Callers must pass a validated `psize` and `n >= 1`.
#[must_use]
pub fn overflow_capacity(n: u64, psize: u32) -> u64 {
    n * psize as u64 - HEADER_SIZE as u64
}

/// Encode a GC-DB key: an 8-byte txnid stored **big-endian** (SPEC 02 §7,
/// SPEC 05 GC-2). This is the sole engine-internal key that is not
/// little-endian; big-endian makes memcmp order equal numeric-ascending order,
/// which the reclamation scan relies on.
#[must_use]
pub fn gc_key_encode(txnid: u64) -> [u8; 8] {
    txnid.to_be_bytes()
}

/// Decode a GC-DB key (big-endian; SPEC 02 §7). Returns `None` if the key is not
/// exactly 8 bytes.
#[must_use]
pub fn gc_key_decode(key: &[u8]) -> Option<u64> {
    let arr: [u8; 8] = key.try_into().ok()?;
    Some(u64::from_be_bytes(arr))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_size_validation() {
        for good in [4096u32, 8192, 16384, 32768, 65536] {
            assert!(validate_page_size(good).is_ok(), "{good} should be valid");
        }
        for bad in [0u32, 1, 2048, 4095, 4097, 6144, 65535, 131072] {
            assert!(validate_page_size(bad).is_err(), "{bad} should be invalid");
        }
    }

    #[test]
    fn max_node_size_worked_values() {
        // SPEC 02 §4.2 worked numbers.
        assert_eq!(max_node_size(4096), 2030);
        assert_eq!(max_node_size(65536), 32750);
    }

    #[test]
    fn map_full_boundary_1mib_4k() {
        // SPEC 02 §8 worked boundary: map_size 1 MiB, psize 4096 -> 256 pages,
        // valid pgnos 0..255.
        let mp = map_pages(1024 * 1024, 4096);
        assert_eq!(mp, 256);
        // next_pgno = 255, single page: 255 + 1 = 256 <= 256 -> allowed
        // (hands out pgno 255, the last usable page).
        assert!(!is_map_full(255, 1, mp));
        // next_pgno = 256, single page: 256 + 1 = 257 > 256 -> MapFull.
        assert!(is_map_full(256, 1, mp));
        // Run exactly filling the map: next_pgno = 254, n = 2 -> 256 <= 256.
        assert!(!is_map_full(254, 2, mp));
        // Run ending one past: next_pgno = 255, n = 2 -> 257 > 256.
        assert!(is_map_full(255, 2, mp));
    }

    #[test]
    fn overflow_math_worked_example() {
        // SPEC 02 §5.1: value length 5000, psize 4096 -> N = 2.
        assert_eq!(overflow_page_count(5000, 4096), 2);
        // Boundary: exactly one page of capacity = psize - HEADER_SIZE = 4064.
        assert_eq!(overflow_page_count(4064, 4096), 1);
        assert_eq!(overflow_page_count(4065, 4096), 2);
        assert_eq!(overflow_page_count(0, 4096), 1);
        assert_eq!(overflow_capacity(2, 4096), 2 * 4096 - 32);
    }

    #[test]
    fn inline_threshold() {
        // psize 4096, max_node_size 2030. Inline iff 8 + ksize + dsize <= 2030.
        assert!(value_is_inline(2, 2020, 4096)); // 8+2+2020 = 2030 -> inline
        assert!(!value_is_inline(2, 2021, 4096)); // 2031 -> overflow
    }

    #[test]
    fn gc_key_codec_roundtrip_and_order() {
        for txnid in [0u64, 1, 255, 256, 0x0102_0304_0506_0708, u64::MAX] {
            assert_eq!(gc_key_decode(&gc_key_encode(txnid)), Some(txnid));
        }
        // Big-endian: memcmp order == numeric order.
        assert!(gc_key_encode(1) < gc_key_encode(2));
        assert!(gc_key_encode(255) < gc_key_encode(256));
        assert!(gc_key_encode(0x00FF) < gc_key_encode(0x0100));
        assert_eq!(gc_key_decode(&[0, 0, 0]), None);
    }
}
