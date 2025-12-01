//! B+tree implementation for ZeroDB.
//!
//! This module implements a B+tree data structure that stores key-value pairs
//! in sorted order, matching LMDB's format exactly.

mod cursor;
mod insert;
mod node;
mod page_ops;
mod search;

pub use cursor::{CursorLevel, CursorOps, CursorState};
pub use insert::{InsertResult, insert_into_branch, insert_into_leaf};
pub use node::{Node, NodeRef};
pub use page_ops::{BranchPage, LeafPage, PageBuilder};
pub use search::search_page;

use crate::page::PageNo;

/// Invalid/null page number marker.
pub const P_INVALID: PageNo = PageNo::MAX;

/// Result of a key comparison during search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchResult {
    /// Exact match found at the given index.
    Found(usize),
    /// Key not found, would be inserted at the given index.
    NotFound(usize),
}

impl SearchResult {
    /// Returns the index, whether found or not.
    pub fn index(&self) -> usize {
        match *self {
            SearchResult::Found(i) | SearchResult::NotFound(i) => i,
        }
    }

    /// Returns true if the key was found.
    pub fn is_found(&self) -> bool {
        matches!(self, SearchResult::Found(_))
    }
}

/// Comparison function type for keys.
pub type CompareFn = fn(&[u8], &[u8]) -> std::cmp::Ordering;

/// Default key comparison (lexicographic).
/// Uses SIMD optimization for keys >= 16 bytes on supported architectures.
#[inline]
pub fn default_compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    // Use SIMD for longer keys where it provides benefit
    #[cfg(target_arch = "x86_64")]
    {
        if a.len() >= 16 && b.len() >= 16 {
            return simd_compare_x86_64(a, b);
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if a.len() >= 16 && b.len() >= 16 {
            return simd_compare_aarch64(a, b);
        }
    }
    a.cmp(b)
}

/// SIMD-optimized comparison for x86_64.
/// Compares 16 bytes at a time using SSE2.
#[cfg(target_arch = "x86_64")]
#[inline]
fn simd_compare_x86_64(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    use std::arch::x86_64::*;

    let min_len = a.len().min(b.len());
    let mut i = 0;

    // Compare 16 bytes at a time
    while i + 16 <= min_len {
        unsafe {
            let va = _mm_loadu_si128(a.as_ptr().add(i) as *const __m128i);
            let vb = _mm_loadu_si128(b.as_ptr().add(i) as *const __m128i);

            // Compare for equality
            let eq = _mm_cmpeq_epi8(va, vb);
            let mask = _mm_movemask_epi8(eq) as u32;

            if mask != 0xFFFF {
                // Found a difference - find first differing byte
                let diff_pos = mask.trailing_ones() as usize;
                return a[i + diff_pos].cmp(&b[i + diff_pos]);
            }
        }
        i += 16;
    }

    // Compare remaining bytes
    a[i..].cmp(&b[i..])
}

/// SIMD-optimized comparison for aarch64.
/// Compares 16 bytes at a time using NEON.
#[cfg(target_arch = "aarch64")]
#[inline]
fn simd_compare_aarch64(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    use std::arch::aarch64::*;

    let min_len = a.len().min(b.len());
    let mut i = 0;

    // Compare 16 bytes at a time
    while i + 16 <= min_len {
        unsafe {
            let va = vld1q_u8(a.as_ptr().add(i));
            let vb = vld1q_u8(b.as_ptr().add(i));

            // Compare for equality
            let eq = vceqq_u8(va, vb);

            // Check if all bytes are equal (all bits set = 0xFF per byte)
            let min_val = vminvq_u8(eq);

            if min_val != 0xFF {
                // Found a difference - compare byte by byte in this chunk
                for j in 0..16 {
                    if a[i + j] != b[i + j] {
                        return a[i + j].cmp(&b[i + j]);
                    }
                }
            }
        }
        i += 16;
    }

    // Compare remaining bytes
    a[i..].cmp(&b[i..])
}

/// Reverse key comparison.
pub fn reverse_compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    b.cmp(a)
}

/// Integer key comparison (native byte order).
pub fn integer_compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    // Assumes keys are the same size
    match a.len() {
        4 => {
            let a_val = u32::from_ne_bytes([a[0], a[1], a[2], a[3]]);
            let b_val = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
            a_val.cmp(&b_val)
        }
        8 => {
            let a_val = u64::from_ne_bytes([a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7]]);
            let b_val = u64::from_ne_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
            a_val.cmp(&b_val)
        }
        _ => a.cmp(b), // Fall back to lexicographic
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn test_default_compare_short_keys() {
        assert_eq!(default_compare(b"abc", b"abc"), Ordering::Equal);
        assert_eq!(default_compare(b"abc", b"abd"), Ordering::Less);
        assert_eq!(default_compare(b"abd", b"abc"), Ordering::Greater);
        assert_eq!(default_compare(b"ab", b"abc"), Ordering::Less);
    }

    #[test]
    fn test_default_compare_long_keys() {
        // Test with keys >= 16 bytes to exercise SIMD path
        let key1 = b"0123456789abcdef_key1";
        let key2 = b"0123456789abcdef_key2";
        let key3 = b"0123456789abcdef_key1";

        assert_eq!(default_compare(key1, key3), Ordering::Equal);
        assert_eq!(default_compare(key1, key2), Ordering::Less);
        assert_eq!(default_compare(key2, key1), Ordering::Greater);
    }

    #[test]
    fn test_default_compare_long_keys_early_diff() {
        // Difference in first 16 bytes
        let key1 = b"0123456789abcdef_more_data";
        let key2 = b"0123456789abcdeX_more_data";

        assert_eq!(default_compare(key1, key2), Ordering::Greater); // 'f' > 'X'
        assert_eq!(default_compare(key2, key1), Ordering::Less);
    }

    #[test]
    fn test_default_compare_long_keys_late_diff() {
        // Difference after first 16 bytes
        let key1 = b"exactly16bytes__xyz";
        let key2 = b"exactly16bytes__abc";

        assert_eq!(default_compare(key1, key2), Ordering::Greater); // 'x' > 'a'
        assert_eq!(default_compare(key2, key1), Ordering::Less);
    }

    #[test]
    fn test_default_compare_different_lengths() {
        let key1 = b"0123456789abcdef";
        let key2 = b"0123456789abcdef_extra";

        assert_eq!(default_compare(key1, key2), Ordering::Less);
        assert_eq!(default_compare(key2, key1), Ordering::Greater);
    }
}
