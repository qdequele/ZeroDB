//! Embedded duplicate sub-page (DUPSORT spike, ADR-0011 Decision 1).
//!
//! A key with two or more values in a DUPSORT database is stored as one leaf
//! cell flagged [`F_DUPDATA`](super::F_DUPDATA) whose value area is a
//! miniature leaf page holding the values:
//!
//! | Off | Size | Field | Meaning |
//! |----:|-----:|-------|---------|
//! | 0 | 2 | `flags` | [`SUBPAGE_FLAGS`] (`P_SUBP \| P_LEAF`). |
//! | 2 | 2 | `lower` | Body-relative end of the pointer array; `count = lower / 2`. |
//! | 4 | 2 | `upper` | Body-relative start of the cell heap. Always equal to `lower`: a sub-page carries no free space, since every change rewrites the whole leaf cell. |
//! | 6 | 2 | `leaf2_ksize` | Item size of a packed (DUPFIXED) sub-page; 0 here. |
//! | 8 | … | body | `count` u16 cell offsets (body-relative, in duplicate order), then the cells: `dvsize: u16` + bytes, each padded to even length. |
//!
//! The layout depends on nothing outside the cell: not the page header width
//! (so it is unaffected by ADR-0020) and not the meta page (format v2).
//! This module is pure slice logic, with no mmap involvement.

use super::{PageError, P_LEAF, P_SUBP};

/// Size of the sub-page header.
pub const SUBPAGE_HEADER: usize = 8;

/// The `flags` of an (unpacked) duplicate sub-page.
pub const SUBPAGE_FLAGS: u16 = P_SUBP | P_LEAF;

fn cell_len(v: &[u8]) -> usize {
    (2 + v.len() + 1) & !1
}

/// The encoded size of a sub-page holding `vals`.
pub fn encoded_len<'a>(vals: impl IntoIterator<Item = &'a [u8]>) -> usize {
    SUBPAGE_HEADER + vals.into_iter().map(|v| 2 + cell_len(v)).sum::<usize>()
}

/// Encode `vals` (already in duplicate order, each at most `u16::MAX` bytes)
/// as a sub-page, replacing the contents of `out`.
///
/// # Panics
///
/// If the sub-page would exceed `u16::MAX` body bytes; callers bound it by
/// the leaf's `max_node_size` first.
pub fn encode(vals: &[&[u8]], out: &mut Vec<u8>) {
    let lower = 2 * vals.len();
    let body_len = encoded_len(vals.iter().copied()) - SUBPAGE_HEADER;
    let lower16 = u16::try_from(lower).expect("sub-page pointer array fits u16");
    assert!(u16::try_from(body_len).is_ok(), "sub-page body fits u16");
    out.clear();
    out.resize(SUBPAGE_HEADER + body_len, 0);
    out[0..2].copy_from_slice(&SUBPAGE_FLAGS.to_le_bytes());
    out[2..4].copy_from_slice(&lower16.to_le_bytes());
    out[4..6].copy_from_slice(&lower16.to_le_bytes());
    let body = &mut out[SUBPAGE_HEADER..];
    let mut at = lower;
    for (i, v) in vals.iter().enumerate() {
        body[2 * i..2 * i + 2].copy_from_slice(&(at as u16).to_le_bytes());
        body[at..at + 2].copy_from_slice(&(v.len() as u16).to_le_bytes());
        body[at + 2..at + 2 + v.len()].copy_from_slice(v);
        at += cell_len(v);
    }
    debug_assert_eq!(at, body_len);
}

/// A validated, borrowed view of a sub-page.
#[derive(Debug, Clone, Copy)]
pub struct SubPage<'a> {
    body: &'a [u8],
    count: usize,
}

impl<'a> SubPage<'a> {
    /// Validate `bytes` as a sub-page: the header, both bounds, and every cell
    /// lies inside the body. Ordering and the minimum count are the check
    /// tool's concern, not the decoder's.
    ///
    /// # Errors
    ///
    /// A [`PageError`] naming the first malformed field.
    pub fn parse(bytes: &'a [u8]) -> Result<SubPage<'a>, PageError> {
        if bytes.len() < SUBPAGE_HEADER {
            return Err(PageError::CellOutOfBounds {
                offset: 0,
                needed: SUBPAGE_HEADER,
                body_size: bytes.len(),
            });
        }
        let rd = |o: usize| u16::from_le_bytes([bytes[o], bytes[o + 1]]);
        let flags = rd(0);
        if flags != SUBPAGE_FLAGS {
            return Err(PageError::ReservedFlagSet { flags });
        }
        let (lower, upper, ksize) = (rd(2), rd(4), rd(6));
        if ksize != 0 {
            return Err(PageError::ReservedFieldNonZero {
                field: "subpage.leaf2_ksize",
                value: u64::from(ksize),
            });
        }
        let body = &bytes[SUBPAGE_HEADER..];
        if lower % 2 != 0 || lower == 0 || upper != lower || usize::from(upper) > body.len() {
            return Err(PageError::BadBounds {
                lower,
                upper,
                body_size: body.len(),
            });
        }
        let count = usize::from(lower) / 2;
        let sp = SubPage { body, count };
        for i in 0..count {
            let off = sp.offset(i);
            let needed = if off >= usize::from(upper) && off + 2 <= body.len() {
                2 + usize::from(u16::from_le_bytes([body[off], body[off + 1]]))
            } else {
                2
            };
            if off < usize::from(upper) || off + needed > body.len() {
                return Err(PageError::CellOutOfBounds {
                    offset: off,
                    needed,
                    body_size: body.len(),
                });
            }
        }
        Ok(sp)
    }

    fn offset(&self, i: usize) -> usize {
        usize::from(u16::from_le_bytes([self.body[2 * i], self.body[2 * i + 1]]))
    }

    /// Number of values.
    #[must_use]
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether the sub-page holds no value (never true for a parsed one).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The `i`-th value in duplicate order.
    ///
    /// # Panics
    ///
    /// If `i >= len()`.
    #[must_use]
    pub fn get(&self, i: usize) -> &'a [u8] {
        assert!(i < self.count, "sub-page index out of range");
        let off = self.offset(i);
        let len = usize::from(u16::from_le_bytes([self.body[off], self.body[off + 1]]));
        &self.body[off + 2..off + 2 + len]
    }

    /// The values in duplicate order.
    pub fn iter(&self) -> impl Iterator<Item = &'a [u8]> + '_ {
        (0..self.count).map(move |i| self.get(i))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let vals: [&[u8]; 4] = [b"", b"a", b"bcd", &[7u8; 511]];
        let mut buf = Vec::new();
        encode(&vals, &mut buf);
        assert_eq!(buf.len(), encoded_len(vals.iter().copied()));
        let sp = SubPage::parse(&buf).unwrap();
        assert_eq!(sp.len(), 4);
        assert_eq!(sp.iter().collect::<Vec<_>>(), vals);
    }

    #[test]
    fn rejects_bad_header_and_cells() {
        let mut buf = Vec::new();
        encode(&[b"x", b"yy"], &mut buf);
        let mut bad = buf.clone();
        bad[0] ^= 0x20; // P_LEAF2 instead of P_LEAF
        assert!(SubPage::parse(&bad).is_err());
        let mut bad = buf.clone();
        bad[4] += 2; // upper != lower
        assert!(SubPage::parse(&bad).is_err());
        let mut bad = buf.clone();
        bad[SUBPAGE_HEADER] = 0xFF; // first cell offset past the body
        assert!(SubPage::parse(&bad).is_err());
        let mut bad = buf.clone();
        let last = bad.len() - 4;
        bad[last] = 0x40; // a cell length running past the body
        assert!(SubPage::parse(&bad).is_err());
        assert!(SubPage::parse(&buf[..5]).is_err());
        assert!(SubPage::parse(&buf).is_ok());
    }

    /// The sub-page sizes against the leaf's inline bound for both header
    /// widths: the 32-byte header of format v2 and the 24-byte header ADR-0020
    /// proposes. A sub-page lives in a leaf cell, so only `max_node_size`
    /// moves with the header; the sub-page layout itself does not.
    #[test]
    fn capacity_under_both_header_widths() {
        let max_node = |psize: usize, header: usize| ((psize - header) / 2 & !1) - 2;
        // `key` 16 bytes, 8-byte duplicates (a docid-like payload).
        let key = 16;
        let item = 2 + cell_len(&[0u8; 8]);
        let fit = |psize, header| (max_node(psize, header) - 8 - key - SUBPAGE_HEADER) / item;
        assert_eq!(max_node(4096, 32), 2030);
        assert_eq!(fit(4096, 32), 166);
        assert_eq!(fit(4096, 24), 166);
        assert_eq!(fit(65536, 32), 2726);
        assert_eq!(fit(65536, 24), 2726);
    }
}
