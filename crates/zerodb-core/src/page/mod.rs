//! On-disk page formats — the byte-level codec for ZeroDB.
//!
//! This module implements [SPEC 02](../../../../docs/SPEC/02-pages.md) exactly:
//! the common 32-byte page header, meta pages (with the mandatory CRC32C), the
//! branch/leaf node-pointer-array + cell-heap layout, overflow runs, and the
//! file-geometry helpers. It is the sole source of truth for how bytes are laid
//! out; the B+tree algorithms that consume these layouts live in `super` /
//! SPEC 03.
//!
//! # Conventions (SPEC 02 §0)
//!
//! - **Little-endian only** for every engine integer field. The one exception
//!   is the GC-DB txnid key, which is big-endian (SPEC 05); see
//!   [`geometry::gc_key_encode`].
//! - **No `#[repr(C)]` casts of unaligned data.** All fields are read/written by
//!   explicit offset through [`raw`] (safe `from_le_bytes`/`to_le_bytes`). This
//!   module contains **zero** `unsafe`.
//! - **Body-relative offsets:** intra-page offsets (`lower`, `upper`, node
//!   pointers) are measured from the first byte after the header (absolute
//!   offset [`HEADER_SIZE`]).
//! - **Runtime page size** `psize`, a power of two in `[4096, 65536]`, chosen at
//!   env creation and stored in the meta page.

mod crc32c;
pub mod geometry;
mod header;
pub mod meta;
mod overflow;
mod raw;
mod tree;

pub use crc32c::crc32c;
pub use header::{CommonHeader, PageRef};
pub use meta::{select as select_meta, DBRecord, MetaChoice, MetaPage, MetaValidity};
pub use overflow::{write_overflow_head, OverflowRef};
pub use tree::{BranchMut, BranchRef, LeafMut, LeafRef, LeafValue};

// ---------------------------------------------------------------------------
// §1 — Constants
// ---------------------------------------------------------------------------

/// File identifier, ASCII `"ZDB1"`. Stored as a byte array (endianness-free).
pub const MAGIC: [u8; 4] = *b"ZDB1";

/// On-disk format version. Bumped only on an incompatible change (ADR-0002 §D8).
pub const FORMAT_VERSION: u32 = 1;

/// Size of the common page header, in bytes.
pub const HEADER_SIZE: usize = 32;

/// Sentinel page number meaning "no page" (empty tree / end of chain).
pub const PGNO_INVALID: u64 = 0xFFFF_FFFF_FFFF_FFFF;

/// Fixed page number of meta slot A.
pub const META_A_PGNO: u64 = 0;

/// Fixed page number of meta slot B.
pub const META_B_PGNO: u64 = 1;

/// Lowest page number a tree/overflow page may occupy.
pub const FIRST_DATA_PGNO: u64 = 2;

/// Maximum key length, in bytes (Phase 1 parity, SPEC 01 §S4). An empty key
/// (length 0) is invalid for leaf/user keys.
pub const MAX_KEY_SIZE: usize = 511;

/// Maximum value length, in bytes (~4 GiB); matches LMDB `MAXDATASIZE`.
pub const MAX_DATA_SIZE: u32 = u32::MAX;

/// Maximum named-DB name length (a catalog key; equals [`MAX_KEY_SIZE`]).
pub const MAX_DB_NAME: usize = 511;

/// Fill threshold in permille (25.0 %): below this a page is a merge/borrow
/// candidate (SPEC 03).
pub const FILL_THRESHOLD_PERMILLE: u32 = 250;

/// Minimum entries a non-root leaf may hold after a delete.
pub const MIN_KEYS_LEAF: usize = 1;

/// Minimum children a non-root branch may hold.
pub const MIN_KEYS_BRANCH: usize = 2;

/// Number of leading bytes of a meta page covered by its CRC (SPEC 02 §3.3).
pub const META_CONTENT_LEN: usize = 168;

/// Smallest permitted page size, in bytes.
pub const MIN_PAGE_SIZE: u32 = 4096;

/// Largest permitted page size, in bytes.
pub const MAX_PAGE_SIZE: u32 = 65536;

// ---------------------------------------------------------------------------
// Page-type flags (§1). A page has exactly one of the first four structural
// bits set.
// ---------------------------------------------------------------------------

/// Leaf page: key → value entries.
pub const P_LEAF: u16 = 0x0001;
/// Branch (internal) page: separator key → child pgno.
pub const P_BRANCH: u16 = 0x0002;
/// Overflow page: head of a contiguous run holding one large value.
pub const P_OVERFLOW: u16 = 0x0004;
/// Meta page (slots 0 and 1 only).
pub const P_META: u16 = 0x0008;
/// **Reserved, Phase 2.8** — DUPFIXED packed-key leaf. Never set in Phase 1.
pub const P_LEAF2: u16 = 0x0020;
/// **Reserved, Phase 2.8** — DUPSORT embedded sub-page. Never set in Phase 1.
pub const P_SUBP: u16 = 0x0040;

/// Mask of the four structural page-type bits.
pub const STRUCTURAL_MASK: u16 = P_LEAF | P_BRANCH | P_OVERFLOW | P_META;

// ---------------------------------------------------------------------------
// Leaf-node flags (§4.2).
// ---------------------------------------------------------------------------

/// Value is stored on an overflow run; the leaf cell's value area is the 8-byte
/// head pgno of the run. `dsize` still holds the true logical value length.
pub const F_BIGDATA: u16 = 0x0001;
/// Leaf value is a 48-byte sub-DB [`DBRecord`] (a named-DB catalog entry).
/// Active in Phase 1 (M1.6); its DUPSORT interaction is deferred to Phase 2.8.
pub const F_SUBDATA: u16 = 0x0002;
/// **Reserved, Phase 2.8** — value is a DUPSORT sub-page/sub-tree (D-004).
pub const F_DUPDATA: u16 = 0x0004;

/// Mask of leaf-node flags that are valid to *see set* in Phase 1. `F_DUPDATA`
/// and all higher bits are rejected by the leaf decoder (SPEC 02 §10).
pub const LEAF_FLAGS_PHASE1_MASK: u16 = F_BIGDATA | F_SUBDATA;

// ---------------------------------------------------------------------------
// Page-type classification
// ---------------------------------------------------------------------------

/// The structural type of a page, derived from its `flags` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    /// Leaf page ([`P_LEAF`]).
    Leaf,
    /// Branch page ([`P_BRANCH`]).
    Branch,
    /// Overflow head page ([`P_OVERFLOW`]).
    Overflow,
    /// Meta page ([`P_META`]).
    Meta,
}

impl PageType {
    /// This page type's structural flag bit.
    #[must_use]
    pub fn flag_bit(self) -> u16 {
        match self {
            PageType::Leaf => P_LEAF,
            PageType::Branch => P_BRANCH,
            PageType::Overflow => P_OVERFLOW,
            PageType::Meta => P_META,
        }
    }
}

/// Classify a `flags` field into a [`PageType`], enforcing the Phase-1 rule:
/// exactly one structural bit set and no reserved bit (including the Phase-2.8
/// `P_LEAF2`/`P_SUBP` hooks) set (SPEC 02 §1, §10).
///
/// # Errors
///
/// - [`PageError::NoPageType`] if no structural bit is set.
/// - [`PageError::MultiplePageTypes`] if more than one structural bit is set.
/// - [`PageError::ReservedFlagSet`] if any non-structural bit is set.
pub fn page_type_of(flags: u16) -> Result<PageType, PageError> {
    let structural = flags & STRUCTURAL_MASK;
    if flags & !STRUCTURAL_MASK != 0 {
        return Err(PageError::ReservedFlagSet { flags });
    }
    match structural {
        0 => Err(PageError::NoPageType { flags }),
        P_LEAF => Ok(PageType::Leaf),
        P_BRANCH => Ok(PageType::Branch),
        P_OVERFLOW => Ok(PageType::Overflow),
        P_META => Ok(PageType::Meta),
        _ => Err(PageError::MultiplePageTypes { flags }),
    }
}

// ---------------------------------------------------------------------------
// Error taxonomy
// ---------------------------------------------------------------------------

/// Errors produced while decoding or validating a page.
///
/// Every fallible decode path returns one of these rather than panicking, so
/// that untrusted bytes (a corrupt or truncated file, or fuzz input) can never
/// crash the decoder.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PageError {
    /// The provided page size is not a power of two in `[4096, 65536]`.
    #[error("invalid page size {0}: must be a power of two in [4096, 65536]")]
    InvalidPageSize(u32),

    /// The backing buffer is smaller than one page.
    #[error("buffer of {got} bytes is smaller than the page size {psize}")]
    BufferTooSmall {
        /// Bytes available in the buffer.
        got: usize,
        /// Required page size.
        psize: usize,
    },

    /// No structural page-type bit is set in `flags`.
    #[error("no page-type bit set in flags {flags:#06x}")]
    NoPageType {
        /// The offending flags value.
        flags: u16,
    },

    /// More than one structural page-type bit is set.
    #[error("multiple page-type bits set in flags {flags:#06x}")]
    MultiplePageTypes {
        /// The offending flags value.
        flags: u16,
    },

    /// A reserved flag bit (including Phase-2.8 hooks) is set in Phase 1.
    #[error("reserved flag bit set in flags {flags:#06x}")]
    ReservedFlagSet {
        /// The offending flags value.
        flags: u16,
    },

    /// A page was decoded as the wrong type (e.g. a leaf parser on a branch).
    #[error("expected {expected:?} page, found {found:?}")]
    WrongPageType {
        /// The type the caller asked for.
        expected: PageType,
        /// The type actually present.
        found: PageType,
    },

    /// A reserved header/body field that must be zero in Phase 1 was non-zero.
    #[error("reserved field {field} must be zero, found {value:#x}")]
    ReservedFieldNonZero {
        /// A short name of the field.
        field: &'static str,
        /// The non-zero value observed.
        value: u64,
    },

    /// The free-space bounds (`lower`/`upper`) are inconsistent.
    #[error("bad free-space bounds: lower={lower}, upper={upper}, body_size={body_size}")]
    BadBounds {
        /// End of the node-pointer array (body-relative).
        lower: u16,
        /// Start of the cell heap (body-relative).
        upper: u16,
        /// Usable body size, `psize - HEADER_SIZE`.
        body_size: usize,
    },

    /// A node pointer or cell extends outside the page body.
    #[error(
        "cell out of bounds at body offset {offset}: needs {needed} bytes, body_size={body_size}"
    )]
    CellOutOfBounds {
        /// Body-relative offset of the cell.
        offset: usize,
        /// Bytes the cell needs from that offset.
        needed: usize,
        /// Usable body size.
        body_size: usize,
    },

    /// A key length is out of the valid range for its context.
    #[error("bad key size {ksize} (valid range for this context is documented in SPEC 02)")]
    BadKeySize {
        /// The offending key size.
        ksize: usize,
    },

    /// A value length exceeds [`MAX_DATA_SIZE`].
    #[error("value size {0} exceeds MAX_DATA_SIZE")]
    BadValueSize(u64),

    /// An overflow run declares fewer than one page.
    #[error("overflow run must span at least 1 page, found {0}")]
    BadOverflowRun(u32),

    /// The page did not fit the requested insertion (page-full).
    #[error("page full: need {needed} bytes, have {available} free")]
    PageFull {
        /// Bytes required (cell + pointer slot).
        needed: usize,
        /// Free bytes available.
        available: usize,
    },
}
