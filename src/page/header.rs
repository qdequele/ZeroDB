//! Page header structure and flags.

use bitflags::bitflags;

use super::PageNo;
use crate::error::{Error, Result};

/// Size of the page header in bytes.
pub const PAGE_HEADER_SIZE: usize = 16;

bitflags! {
    /// Page type and status flags.
    ///
    /// These flags indicate the type of page and its current status.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct PageFlags: u16 {
        /// Branch page - contains keys and child page pointers.
        const BRANCH = 0x01;
        /// Leaf page - contains keys and values.
        const LEAF = 0x02;
        /// Overflow page - contains large value data.
        const OVERFLOW = 0x04;
        /// Meta page - contains database metadata.
        const META = 0x08;
        /// Page has been modified in current transaction.
        const DIRTY = 0x10;
        /// Leaf page with DUPFIXED sub-pages.
        const LEAF2 = 0x20;
        /// Sub-page inside a leaf page.
        const SUBP = 0x40;
        /// Page was freed in current transaction.
        const LOOSE = 0x4000;
        /// Page should be kept after transaction.
        const KEEP = 0x8000;
    }
}

impl PageFlags {
    /// Returns true if this is a branch page.
    #[inline(always)]
    pub fn is_branch(self) -> bool {
        self.contains(PageFlags::BRANCH)
    }

    /// Returns true if this is a leaf page.
    #[inline(always)]
    pub fn is_leaf(self) -> bool {
        self.contains(PageFlags::LEAF)
    }

    /// Returns true if this is an overflow page.
    #[inline(always)]
    pub fn is_overflow(self) -> bool {
        self.contains(PageFlags::OVERFLOW)
    }

    /// Returns true if this is a meta page.
    #[inline(always)]
    pub fn is_meta(self) -> bool {
        self.contains(PageFlags::META)
    }

    /// Returns true if this page has been modified.
    #[inline(always)]
    pub fn is_dirty(self) -> bool {
        self.contains(PageFlags::DIRTY)
    }
}

/// Page header structure.
///
/// Every page in the database starts with this 16-byte header.
/// The layout matches LMDB exactly for binary compatibility.
///
/// ```text
/// Offset  Size  Field
/// 0       8     page_no (page number)
/// 8       2     pad (reserved/padding)
/// 10      2     flags (page type flags)
/// 12      2     lower (offset to end of page keys)
/// 14      2     upper (offset to start of page data)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct PageHeader {
    /// Page number (position in file, counted in pages).
    pub page_no: PageNo,
    /// Padding/reserved bytes.
    pub pad: u16,
    /// Page flags indicating type and status.
    pub flags: PageFlags,
    /// Lower bound - offset to end of node pointers array.
    /// Points to the first free byte after the key index.
    pub lower: u16,
    /// Upper bound - offset to start of node data.
    /// Points to the first byte of actual node data (grows down).
    pub upper: u16,
}

impl PageHeader {
    /// Creates a new page header.
    pub fn new(page_no: PageNo, flags: PageFlags, page_size: usize) -> Self {
        Self {
            page_no,
            pad: 0,
            flags,
            lower: PAGE_HEADER_SIZE as u16,
            upper: page_size as u16,
        }
    }

    /// Creates a new branch page header.
    pub fn new_branch(page_no: PageNo, page_size: usize) -> Self {
        Self::new(page_no, PageFlags::BRANCH, page_size)
    }

    /// Creates a new leaf page header.
    pub fn new_leaf(page_no: PageNo, page_size: usize) -> Self {
        Self::new(page_no, PageFlags::LEAF, page_size)
    }

    /// Creates a new overflow page header.
    pub fn new_overflow(page_no: PageNo) -> Self {
        Self {
            page_no,
            pad: 0,
            flags: PageFlags::OVERFLOW,
            lower: 0,
            upper: 0,
        }
    }

    /// Creates a new meta page header.
    pub fn new_meta(page_no: PageNo) -> Self {
        Self {
            page_no,
            pad: 0,
            flags: PageFlags::META,
            lower: 0,
            upper: 0,
        }
    }

    /// Returns the amount of free space in this page.
    pub fn free_space(&self) -> usize {
        if self.upper >= self.lower {
            (self.upper - self.lower) as usize
        } else {
            0
        }
    }

    /// Returns the number of node entries in this page.
    ///
    /// This is calculated from the lower bound offset.
    #[inline(always)]
    pub fn num_keys(&self) -> usize {
        if self.lower > PAGE_HEADER_SIZE as u16 {
            (self.lower as usize - PAGE_HEADER_SIZE) / 2
        } else {
            0
        }
    }

    /// Reads a page header from a byte slice.
    ///
    /// # Errors
    ///
    /// Returns an error if the slice is too small.
    #[inline]
    pub fn read_from(data: &[u8]) -> Result<Self> {
        if data.len() < PAGE_HEADER_SIZE {
            return Err(Error::Corrupted);
        }

        let page_no = u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let pad = u16::from_le_bytes([data[8], data[9]]);
        let flags_bits = u16::from_le_bytes([data[10], data[11]]);
        let lower = u16::from_le_bytes([data[12], data[13]]);
        let upper = u16::from_le_bytes([data[14], data[15]]);

        let flags = PageFlags::from_bits_truncate(flags_bits);

        Ok(Self {
            page_no,
            pad,
            flags,
            lower,
            upper,
        })
    }

    /// Writes the page header to a byte slice.
    ///
    /// # Errors
    ///
    /// Returns an error if the slice is too small.
    pub fn write_to(&self, data: &mut [u8]) -> Result<()> {
        if data.len() < PAGE_HEADER_SIZE {
            return Err(Error::Corrupted);
        }

        data[0..8].copy_from_slice(&self.page_no.to_le_bytes());
        data[8..10].copy_from_slice(&self.pad.to_le_bytes());
        data[10..12].copy_from_slice(&self.flags.bits().to_le_bytes());
        data[12..14].copy_from_slice(&self.lower.to_le_bytes());
        data[14..16].copy_from_slice(&self.upper.to_le_bytes());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_header_size() {
        assert_eq!(PAGE_HEADER_SIZE, 16);
        assert_eq!(std::mem::size_of::<PageHeader>(), PAGE_HEADER_SIZE);
    }

    #[test]
    fn page_header_roundtrip() {
        let header = PageHeader {
            page_no: 42,
            pad: 0,
            flags: PageFlags::LEAF | PageFlags::DIRTY,
            lower: 24,
            upper: 4000,
        };

        let mut buf = [0u8; PAGE_HEADER_SIZE];
        header.write_to(&mut buf).unwrap();

        let recovered = PageHeader::read_from(&buf).unwrap();
        assert_eq!(header, recovered);
    }

    #[test]
    fn page_flags() {
        assert!(PageFlags::BRANCH.is_branch());
        assert!(!PageFlags::BRANCH.is_leaf());
        assert!(PageFlags::LEAF.is_leaf());
        assert!(PageFlags::OVERFLOW.is_overflow());
        assert!(PageFlags::META.is_meta());
        assert!((PageFlags::LEAF | PageFlags::DIRTY).is_dirty());
    }

    #[test]
    fn new_page_headers() {
        let branch = PageHeader::new_branch(1, 4096);
        assert!(branch.flags.is_branch());
        assert_eq!(branch.lower, PAGE_HEADER_SIZE as u16);
        assert_eq!(branch.upper, 4096);

        let leaf = PageHeader::new_leaf(2, 4096);
        assert!(leaf.flags.is_leaf());

        let overflow = PageHeader::new_overflow(3);
        assert!(overflow.flags.is_overflow());

        let meta = PageHeader::new_meta(0);
        assert!(meta.flags.is_meta());
    }

    #[test]
    fn free_space_calculation() {
        let header = PageHeader {
            page_no: 0,
            pad: 0,
            flags: PageFlags::LEAF,
            lower: 100,
            upper: 3000,
        };
        assert_eq!(header.free_space(), 2900);
    }

    #[test]
    fn num_keys_calculation() {
        // Each key pointer is 2 bytes, header is 16 bytes
        // lower = 16 + 2*N where N is number of keys
        let header = PageHeader {
            page_no: 0,
            pad: 0,
            flags: PageFlags::LEAF,
            lower: 16 + 10, // 5 keys (10 bytes of pointers)
            upper: 4096,
        };
        assert_eq!(header.num_keys(), 5);
    }
}
