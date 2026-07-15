//! Overflow pages and value chains (SPEC 02 §5).
//!
//! A large value is stored on a contiguous run of `N` pages. The **head** page
//! (lowest pgno of the run) carries the common header with `flags = P_OVERFLOW`
//! and `ovf_pages = N` in its variant tail (offset 24, u32). Interior pages have
//! **no** header — they are pure payload. The value bytes begin at the head
//! page's body (absolute offset [`HEADER_SIZE`]) and continue densely through
//! the run.

use super::geometry::{overflow_capacity, validate_page_size};
use super::header::{CommonHeader, OFF_VARIANT};
use super::raw::{read_u32, write_u32};
use super::{page_type_of, PageError, PageType, HEADER_SIZE, P_OVERFLOW};

/// `ovf_pages` occupies the first 4 bytes of the variant tail; the next 4 are
/// `reserved2` (MUST be 0).
const OFF_OVF_PAGES: usize = OFF_VARIANT;
const OFF_RESERVED2: usize = OFF_VARIANT + 4;

/// Read-only view over the head page of an overflow run.
///
/// The backing buffer must cover the **whole** run (`>= N * psize` bytes) for
/// [`payload`](OverflowRef::payload) to succeed.
#[derive(Debug, Clone, Copy)]
pub struct OverflowRef<'a> {
    buf: &'a [u8],
    psize: u32,
    ovf_pages: u32,
}

impl<'a> OverflowRef<'a> {
    /// Validate and wrap `buf` as an overflow head page of size `psize`.
    ///
    /// Validates the type flag, that `ovf_pages >= 1`, and that `reserved2` is
    /// zero. Does **not** require the buffer to cover the whole run; that is
    /// checked when [`payload`](OverflowRef::payload) is called with a `dsize`.
    ///
    /// # Errors
    ///
    /// [`PageError::WrongPageType`], [`PageError::BadOverflowRun`], or
    /// [`PageError::ReservedFieldNonZero`].
    pub fn new(buf: &'a [u8], psize: u32) -> Result<OverflowRef<'a>, PageError> {
        validate_page_size(psize)?;
        if buf.len() < psize as usize {
            return Err(PageError::BufferTooSmall {
                got: buf.len(),
                psize: psize as usize,
            });
        }
        let hdr = CommonHeader::read(buf);
        if page_type_of(hdr.flags)? != PageType::Overflow {
            return Err(PageError::WrongPageType {
                expected: PageType::Overflow,
                found: page_type_of(hdr.flags)?,
            });
        }
        let ovf_pages = read_u32(buf, OFF_OVF_PAGES);
        if ovf_pages < 1 {
            return Err(PageError::BadOverflowRun(ovf_pages));
        }
        let reserved2 = read_u32(buf, OFF_RESERVED2);
        if reserved2 != 0 {
            return Err(PageError::ReservedFieldNonZero {
                field: "reserved2",
                value: reserved2 as u64,
            });
        }
        Ok(OverflowRef {
            buf,
            psize,
            ovf_pages,
        })
    }

    /// This page's own number (header offset 0).
    #[must_use]
    pub fn pgno(&self) -> u64 {
        CommonHeader::read(self.buf).pgno
    }

    /// The writer txnid stamp (header offset 8).
    #[must_use]
    pub fn txnid(&self) -> u64 {
        CommonHeader::read(self.buf).txnid
    }

    /// Number of contiguous pages in this run (`>= 1`).
    #[must_use]
    pub fn ovf_pages(&self) -> u32 {
        self.ovf_pages
    }

    /// Extract exactly `dsize` payload bytes from the run.
    ///
    /// # Errors
    ///
    /// - [`PageError::BadValueSize`] if `dsize` exceeds the run's capacity
    ///   (`ovf_pages * psize - HEADER_SIZE`), i.e. `ovf_pages` is inconsistent
    ///   with the declared value length.
    /// - [`PageError::BufferTooSmall`] if the backing buffer does not cover the
    ///   requested payload.
    pub fn payload(&self, dsize: u32) -> Result<&'a [u8], PageError> {
        let capacity = overflow_capacity(self.ovf_pages as u64, self.psize);
        if dsize as u64 > capacity {
            return Err(PageError::BadValueSize(dsize as u64));
        }
        let end = HEADER_SIZE + dsize as usize;
        if self.buf.len() < end {
            return Err(PageError::BufferTooSmall {
                got: self.buf.len(),
                psize: end,
            });
        }
        Ok(&self.buf[HEADER_SIZE..end])
    }
}

/// Write an overflow head page: the common header, `ovf_pages`, zeroed
/// `reserved2`, and the leading `payload` bytes into the head page's body.
///
/// Only the head page's own bytes (first `psize`) are written here; a multi-page
/// run's interior pages are filled by the caller from `payload[body_cap..]`.
/// Returns the number of payload bytes written into this head page.
///
/// # Errors
///
/// [`PageError::InvalidPageSize`] or [`PageError::BufferTooSmall`].
pub fn write_overflow_head(
    buf: &mut [u8],
    psize: u32,
    pgno: u64,
    txnid: u64,
    ovf_pages: u32,
    payload: &[u8],
) -> Result<usize, PageError> {
    validate_page_size(psize)?;
    if buf.len() < psize as usize {
        return Err(PageError::BufferTooSmall {
            got: buf.len(),
            psize: psize as usize,
        });
    }
    CommonHeader {
        pgno,
        txnid,
        flags: P_OVERFLOW,
    }
    .write(buf);
    write_u32(buf, OFF_OVF_PAGES, ovf_pages);
    write_u32(buf, OFF_RESERVED2, 0);
    let body_cap = psize as usize - HEADER_SIZE;
    let n = payload.len().min(body_cap);
    buf[HEADER_SIZE..HEADER_SIZE + n].copy_from_slice(&payload[..n]);
    Ok(n)
}
