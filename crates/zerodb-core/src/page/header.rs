//! Common 32-byte page header (SPEC 02 §2) and the generic [`PageRef`] view.
//!
//! The header layout, by absolute byte offset:
//!
//! | Off | Size | Field       |
//! |----:|-----:|-------------|
//! |   0 |    8 | `pgno`      |
//! |   8 |    8 | `txnid`     |
//! |  16 |    2 | `flags`     |
//! |  18 |    2 | `reserved0` |
//! |  20 |    4 | `checksum`  |
//! |  24 |    8 | variant tail (§2.1) |

use super::geometry::validate_page_size;
use super::raw::{read_u16, read_u32, read_u64, write_u16, write_u64};
use super::{
    page_type_of, tree, BranchRef, LeafRef, MetaPage, MetaValidity, OverflowRef, PageError,
    PageType, HEADER_SIZE,
};

// Field offsets within the common header.
pub(crate) const OFF_PGNO: usize = 0;
pub(crate) const OFF_TXNID: usize = 8;
pub(crate) const OFF_FLAGS: usize = 16;
pub(crate) const OFF_RESERVED0: usize = 18;
pub(crate) const OFF_CHECKSUM: usize = 20;
/// First byte of the 8-byte variant tail (§2.1).
pub(crate) const OFF_VARIANT: usize = 24;

/// The three universal fields of the common page header.
///
/// `reserved0` and `checksum` (the Phase-3.9 data-page checksum) are always zero
/// in Phase 1 and are not represented here; the variant tail (offsets 24–31) is
/// interpreted by the type-specific views.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommonHeader {
    /// This page's own number.
    pub pgno: u64,
    /// txnid of the write transaction that last wrote this page (writer stamp).
    pub txnid: u64,
    /// Page-type bitfield (§1).
    pub flags: u16,
}

impl CommonHeader {
    /// Decode the common header from the first 32 bytes of `buf`.
    ///
    /// The caller must guarantee `buf.len() >= HEADER_SIZE`.
    #[must_use]
    pub fn read(buf: &[u8]) -> CommonHeader {
        CommonHeader {
            pgno: read_u64(buf, OFF_PGNO),
            txnid: read_u64(buf, OFF_TXNID),
            flags: read_u16(buf, OFF_FLAGS),
        }
    }

    /// Write the common header into the first 24 bytes of `buf`, zeroing the
    /// reserved `reserved0` (offset 18) and `checksum` (offset 20) fields. The
    /// variant tail (offsets 24–31) is left untouched for the caller to fill.
    ///
    /// The caller must guarantee `buf.len() >= HEADER_SIZE`.
    pub fn write(self, buf: &mut [u8]) {
        write_u64(buf, OFF_PGNO, self.pgno);
        write_u64(buf, OFF_TXNID, self.txnid);
        write_u16(buf, OFF_FLAGS, self.flags);
        write_u16(buf, OFF_RESERVED0, 0);
        // checksum (u32) reserved 0 in Phase 1.
        buf[OFF_CHECKSUM..OFF_CHECKSUM + 4].fill(0);
    }
}

/// A validated, read-only view over one page's bytes.
///
/// Construction validates the page size, that the buffer holds at least one
/// page, and that the `flags` field names exactly one structural page type with
/// no reserved bit set (SPEC 02 §1). It does **not** validate the body — call
/// [`PageRef::as_leaf`] / [`as_branch`](PageRef::as_branch) /
/// [`as_overflow`](PageRef::as_overflow) / [`as_meta`](PageRef::as_meta) to
/// obtain a body-validated view.
#[derive(Debug, Clone, Copy)]
pub struct PageRef<'a> {
    buf: &'a [u8],
    psize: u32,
    page_type: PageType,
}

impl<'a> PageRef<'a> {
    /// Validate and wrap `buf` as a page of size `psize`.
    ///
    /// For an overflow head page whose run spans multiple pages, `buf` may be
    /// longer than `psize` (it should cover the whole run); for every other page
    /// type `buf` should be exactly `psize` bytes, but any buffer `>= psize` is
    /// accepted here (the type-specific views bound their reads to `psize`).
    ///
    /// # Errors
    ///
    /// Propagates [`PageError::InvalidPageSize`], [`PageError::BufferTooSmall`],
    /// and the classification errors from [`page_type_of`].
    pub fn new(buf: &'a [u8], psize: u32) -> Result<PageRef<'a>, PageError> {
        validate_page_size(psize)?;
        if buf.len() < psize as usize {
            return Err(PageError::BufferTooSmall {
                got: buf.len(),
                psize: psize as usize,
            });
        }
        let flags = read_u16(buf, OFF_FLAGS);
        let page_type = page_type_of(flags)?;
        Ok(PageRef {
            buf,
            psize,
            page_type,
        })
    }

    /// The page's own number (header offset 0).
    #[must_use]
    pub fn pgno(&self) -> u64 {
        read_u64(self.buf, OFF_PGNO)
    }

    /// The writer txnid stamp (header offset 8).
    #[must_use]
    pub fn txnid(&self) -> u64 {
        read_u64(self.buf, OFF_TXNID)
    }

    /// The raw `flags` field (header offset 16).
    #[must_use]
    pub fn flags(&self) -> u16 {
        read_u16(self.buf, OFF_FLAGS)
    }

    /// The Phase-3.9 data-page checksum field (header offset 20). Always zero in
    /// Phase 1; exposed for completeness and future use.
    #[must_use]
    pub fn checksum(&self) -> u32 {
        read_u32(self.buf, OFF_CHECKSUM)
    }

    /// The classified page type.
    #[must_use]
    pub fn page_type(&self) -> PageType {
        self.page_type
    }

    /// The page size this view was constructed with.
    #[must_use]
    pub fn page_size(&self) -> u32 {
        self.psize
    }

    /// The whole backing buffer.
    #[must_use]
    pub fn bytes(&self) -> &'a [u8] {
        self.buf
    }

    /// Body-validated leaf view.
    ///
    /// # Errors
    ///
    /// [`PageError::WrongPageType`] if this is not a leaf page; otherwise any
    /// body-validation error from [`LeafRef::new`].
    pub fn as_leaf(&self) -> Result<LeafRef<'a>, PageError> {
        if self.page_type != PageType::Leaf {
            return Err(PageError::WrongPageType {
                expected: PageType::Leaf,
                found: self.page_type,
            });
        }
        LeafRef::new(self.buf, self.psize)
    }

    /// Body-validated branch view.
    ///
    /// # Errors
    ///
    /// [`PageError::WrongPageType`] if this is not a branch page; otherwise any
    /// body-validation error from [`BranchRef::new`].
    pub fn as_branch(&self) -> Result<BranchRef<'a>, PageError> {
        if self.page_type != PageType::Branch {
            return Err(PageError::WrongPageType {
                expected: PageType::Branch,
                found: self.page_type,
            });
        }
        BranchRef::new(self.buf, self.psize)
    }

    /// Body-validated overflow head view.
    ///
    /// # Errors
    ///
    /// [`PageError::WrongPageType`] if this is not an overflow page; otherwise
    /// any body-validation error from [`OverflowRef::new`].
    pub fn as_overflow(&self) -> Result<OverflowRef<'a>, PageError> {
        if self.page_type != PageType::Overflow {
            return Err(PageError::WrongPageType {
                expected: PageType::Overflow,
                found: self.page_type,
            });
        }
        OverflowRef::new(self.buf, self.psize)
    }

    /// Decode and validate this page as a meta page.
    ///
    /// Returns the [`MetaValidity`] verdict (never an error for a well-typed
    /// meta page — validity is data, not a decode failure), or
    /// [`PageError::WrongPageType`] if this is not a meta page.
    ///
    /// # Errors
    ///
    /// [`PageError::WrongPageType`] if this is not a meta page.
    pub fn as_meta(&self) -> Result<MetaValidity, PageError> {
        if self.page_type != PageType::Meta {
            return Err(PageError::WrongPageType {
                expected: PageType::Meta,
                found: self.page_type,
            });
        }
        MetaPage::validate(self.buf, self.psize)
    }
}

/// Validate the free-space bounds of a branch/leaf page against the body size,
/// shared by the tree views. Returns `(lower, upper)` on success.
pub(crate) fn read_and_check_bounds(buf: &[u8], psize: u32) -> Result<(u16, u16), PageError> {
    let body_size = psize as usize - HEADER_SIZE;
    let lower = read_u16(buf, tree::OFF_LOWER);
    let upper = read_u16(buf, tree::OFF_UPPER);
    // lower must be even (whole u16 pointers), lower <= upper <= body_size.
    if lower % 2 != 0 || lower as usize > upper as usize || upper as usize > body_size {
        return Err(PageError::BadBounds {
            lower,
            upper,
            body_size,
        });
    }
    Ok((lower, upper))
}
