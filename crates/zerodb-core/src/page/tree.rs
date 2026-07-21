//! Branch and leaf pages: the node-pointer array + cell heap layout (SPEC 02
//! §2.2, §4), with in-page insert/remove/lookup and free-space accounting.
//!
//! A tree page's body (absolute offset [`HEADER_SIZE`] onward) holds:
//!
//! ```text
//! | ptr0 ptr1 ... ptrN (u16, sorted asc) |  free  | cellK ... cell0 (heap) |
//!  ^ body-relative 0                       ^lower  ^upper
//! ```
//!
//! `lower` (header offset 24) is the end of the pointer array
//! (`num_keys = lower / 2`); `upper` (offset 26) is the lowest occupied cell.
//! Free space is `upper - lower`. All intra-page offsets are **body-relative**
//! (measured from absolute offset [`HEADER_SIZE`]); cells are even-length
//! (2-byte alignment) and their u32/u64 fields are read unaligned via [`raw`].

use crate::cmp::KeyCmp;

use super::geometry::body_size;
use super::header::{read_and_check_bounds, CommonHeader};
use super::raw::{
    read_u16, read_u16_unchecked, read_u32, read_u32_unchecked, read_u64_unchecked, write_u16,
    write_u32, write_u64,
};
use super::{
    page_type_of, PageError, PageType, F_BIGDATA, HEADER_SIZE, LEAF_FLAGS_PHASE1_MASK,
    MAX_DATA_SIZE, MAX_KEY_SIZE, P_BRANCH, P_LEAF,
};

// Variant-tail field offsets (branch/leaf), SPEC 02 §2.1.
pub(crate) const OFF_LOWER: usize = 24;
pub(crate) const OFF_UPPER: usize = 26;
pub(crate) const OFF_LEAF2_KSIZE: usize = 28;
pub(crate) const OFF_RESERVED1: usize = 30;

/// Leaf node header size (SPEC 02 §4.2): flags(2) + ksize(2) + dsize(4).
pub(crate) const LEAF_NODE_HEADER: usize = 8;
/// Branch node header size (SPEC 02 §4.1): child_pgno(8) + ksize(2).
pub(crate) const BRANCH_NODE_HEADER: usize = 10;
/// Size of a leaf `F_BIGDATA` value area (the head pgno).
pub(crate) const BIGDATA_VALUE_LEN: usize = 8;

/// Round `n` up to the next even number (2-byte cell alignment; LMDB `EVEN`).
#[inline]
pub(crate) fn even(n: usize) -> usize {
    (n + 1) & !1
}

/// The value stored by a leaf node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafValue<'a> {
    /// Value bytes stored inline in the cell.
    Inline(&'a [u8]),
    /// Value stored on an overflow run; carries the head pgno and true length.
    Overflow {
        /// Head page number of the overflow run.
        head_pgno: u64,
        /// True logical value length (`dsize`).
        dsize: u32,
    },
}

// ===========================================================================
// Shared cell-length / validation helpers
// ===========================================================================

/// Compute the padded length of the leaf cell at absolute offset `abs`, bounds-
/// checking every field read against `psize`.
fn leaf_cell_len(buf: &[u8], abs: usize, psize: u32) -> Result<usize, PageError> {
    let body = body_size(psize);
    let rel = abs - HEADER_SIZE;
    // Header must be fully in-body.
    if rel + LEAF_NODE_HEADER > body {
        return Err(PageError::CellOutOfBounds {
            offset: rel,
            needed: LEAF_NODE_HEADER,
            body_size: body,
        });
    }
    let flags = read_u16(buf, abs);
    let ksize = read_u16(buf, abs + 2) as usize;
    let dsize = read_u32(buf, abs + 4);
    if flags & !LEAF_FLAGS_PHASE1_MASK != 0 {
        return Err(PageError::ReservedFlagSet { flags });
    }
    if ksize == 0 || ksize > MAX_KEY_SIZE {
        return Err(PageError::BadKeySize { ksize });
    }
    let value_area = if flags & F_BIGDATA != 0 {
        BIGDATA_VALUE_LEN
    } else {
        dsize as usize
    };
    let unpadded = LEAF_NODE_HEADER + ksize + value_area;
    let clen = even(unpadded);
    if rel + clen > body {
        return Err(PageError::CellOutOfBounds {
            offset: rel,
            needed: clen,
            body_size: body,
        });
    }
    Ok(clen)
}

/// Compute the padded length of the branch cell at absolute offset `abs`,
/// bounds-checking against `psize`. `index0` allows the empty separator key.
fn branch_cell_len(buf: &[u8], abs: usize, psize: u32, index0: bool) -> Result<usize, PageError> {
    let body = body_size(psize);
    let rel = abs - HEADER_SIZE;
    if rel + BRANCH_NODE_HEADER > body {
        return Err(PageError::CellOutOfBounds {
            offset: rel,
            needed: BRANCH_NODE_HEADER,
            body_size: body,
        });
    }
    let ksize = read_u16(buf, abs + 8) as usize;
    if index0 {
        if ksize != 0 {
            return Err(PageError::BadKeySize { ksize });
        }
    } else if ksize == 0 || ksize > MAX_KEY_SIZE {
        return Err(PageError::BadKeySize { ksize });
    }
    let clen = even(BRANCH_NODE_HEADER + ksize);
    if rel + clen > body {
        return Err(PageError::CellOutOfBounds {
            offset: rel,
            needed: clen,
            body_size: body,
        });
    }
    Ok(clen)
}

/// Read the body-relative pointer at array index `i`.
#[inline]
fn ptr_at(buf: &[u8], i: usize) -> u16 {
    read_u16(buf, HEADER_SIZE + i * 2)
}

// ===========================================================================
// Leaf pages
// ===========================================================================

/// Read-only view over a validated leaf page (SPEC 02 §4.2).
#[derive(Debug, Clone, Copy)]
pub struct LeafRef<'a> {
    buf: &'a [u8],
    lower: u16,
    upper: u16,
}

impl<'a> LeafRef<'a> {
    /// Validate and wrap `buf` as a leaf page of size `psize`.
    ///
    /// Validates the type flag, reserved fields, free-space bounds, and that
    /// every node pointer references an in-bounds, well-formed cell.
    ///
    /// # Errors
    ///
    /// Any [`PageError`] arising from the checks above.
    pub fn new(buf: &'a [u8], psize: u32) -> Result<LeafRef<'a>, PageError> {
        let view = Self::new_prevalidated(buf, psize)?;
        // Validate every cell.
        for i in 0..view.num_keys() {
            let rel = ptr_at(buf, i) as usize;
            check_ptr_in_heap(rel, view.upper, psize)?;
            leaf_cell_len(buf, HEADER_SIZE + rel, psize)?;
        }
        Ok(view)
    }

    /// [`LeafRef::new`] minus the per-cell loop: type flag, reserved fields
    /// and free-space bounds are still checked (O(1)); the O(`num_keys`) cell
    /// walk is skipped.
    ///
    /// The accessors do unchecked slicing and rely on the cells having been
    /// validated, so this may only be used when **these same bytes** already
    /// passed [`LeafRef::new`] earlier — i.e. behind the txn-scoped
    /// validated-pages memo over an immutable source
    /// (`btree::ValidatedPages`; docs/PERF-GAP-VS-LMDB.md A2).
    pub(crate) fn new_prevalidated(buf: &'a [u8], psize: u32) -> Result<LeafRef<'a>, PageError> {
        let hdr = CommonHeader::read(buf);
        if page_type_of(hdr.flags)? != PageType::Leaf {
            return Err(PageError::WrongPageType {
                expected: PageType::Leaf,
                found: page_type_of(hdr.flags)?,
            });
        }
        check_reserved_tail_fields(buf)?;
        let (lower, upper) = read_and_check_bounds(buf, psize)?;
        Ok(LeafRef { buf, lower, upper })
    }

    /// [`new_prevalidated`](Self::new_prevalidated) minus every check — two
    /// raw header reads (PERF-GAP A8). Only for a **kind-tagged memo hit**:
    /// these same bytes passed [`LeafRef::new`] earlier this txn *and* the
    /// memo key records that they validated as a **leaf**
    /// (`btree::ValidatedPages` tags the page kind), so not even the type
    /// flag needs a re-read. The debug assert keeps the claim honest.
    pub(crate) fn new_trusted(buf: &'a [u8]) -> LeafRef<'a> {
        debug_assert!(matches!(
            page_type_of(CommonHeader::read(buf).flags),
            Ok(PageType::Leaf)
        ));
        LeafRef {
            buf,
            lower: read_u16(buf, OFF_LOWER),
            upper: read_u16(buf, OFF_UPPER),
        }
    }

    /// Number of entries on this page (`lower / 2`).
    #[must_use]
    pub fn num_keys(&self) -> usize {
        self.lower as usize / 2
    }

    /// Free bytes available for a new cell + pointer (`upper - lower`).
    #[must_use]
    pub fn free_space(&self) -> usize {
        self.upper as usize - self.lower as usize
    }

    /// Absolute offset of cell `i`'s header (A3: unchecked pointer-array
    /// read). Callers guarantee `i < num_keys` (public accessors `assert!`
    /// it; the lookup loops maintain it as a binary-search invariant).
    #[allow(unsafe_code)]
    fn cell_abs(&self, i: usize) -> usize {
        debug_assert!(i < self.num_keys());
        // SAFETY: `i < num_keys = lower/2`, so the slot at
        // `HEADER_SIZE + i*2` lies inside `[HEADER_SIZE, HEADER_SIZE+lower)`,
        // and `lower <= upper <= body_size` held at view construction (or is
        // inherited via the A8 trusted-view / engine-authorship contract).
        HEADER_SIZE + unsafe { read_u16_unchecked(self.buf, HEADER_SIZE + i * 2) } as usize
    }

    /// The node flags of entry `i`. Panics if `i >= num_keys`.
    #[must_use]
    #[allow(unsafe_code)]
    pub fn node_flags(&self, i: usize) -> u16 {
        assert!(i < self.num_keys(), "leaf entry index out of range");
        // SAFETY (A3 view contract): cell `i` was proven in-bounds by the
        // full validation walk, or inherits that proof (kind-tagged memo hit
        // / engine-authored dirty frame — batch 3/A8); its 8-byte header is
        // inside `buf`.
        unsafe { read_u16_unchecked(self.buf, self.cell_abs(i)) }
    }

    /// The key bytes of entry `i`. Panics if `i >= num_keys`.
    #[must_use]
    #[allow(unsafe_code)]
    pub fn key(&self, i: usize) -> &'a [u8] {
        assert!(i < self.num_keys(), "leaf entry index out of range");
        let abs = self.cell_abs(i);
        // SAFETY (A3 view contract, as `node_flags`): validation bounded the
        // whole cell — header AND `header + ksize` key span — inside `buf`
        // (`leaf_cell_len`), so the unchecked `ksize` read and key slice are
        // in bounds.
        unsafe {
            let ksize = read_u16_unchecked(self.buf, abs + 2) as usize;
            self.buf
                .get_unchecked(abs + LEAF_NODE_HEADER..abs + LEAF_NODE_HEADER + ksize)
        }
    }

    /// The value of entry `i` (inline slice or overflow reference). Panics if
    /// `i >= num_keys`.
    #[must_use]
    #[allow(unsafe_code)]
    pub fn value(&self, i: usize) -> LeafValue<'a> {
        assert!(i < self.num_keys(), "leaf entry index out of range");
        let abs = self.cell_abs(i);
        // SAFETY (A3 view contract, as `key`): `leaf_cell_len` bounded the
        // header, key span, and value area (`dsize` inline bytes, or the
        // 8-byte overflow head under `F_BIGDATA`) inside `buf`.
        unsafe {
            let flags = read_u16_unchecked(self.buf, abs);
            let ksize = read_u16_unchecked(self.buf, abs + 2) as usize;
            let dsize = read_u32_unchecked(self.buf, abs + 4);
            let val_off = abs + LEAF_NODE_HEADER + ksize;
            if flags & F_BIGDATA != 0 {
                LeafValue::Overflow {
                    head_pgno: read_u64_unchecked(self.buf, val_off),
                    dsize,
                }
            } else {
                LeafValue::Inline(self.buf.get_unchecked(val_off..val_off + dsize as usize))
            }
        }
    }

    /// Binary-search for `key`. `Ok(i)` if entry `i` equals `key`; `Err(i)` if
    /// absent, where `i` is the lower-bound insertion index (SPEC 03 §2).
    pub fn lookup(&self, key: &[u8]) -> Result<usize, usize> {
        leaf_lookup(self.buf, self.num_keys(), key, KeyCmp::Default)
    }

    /// As [`Self::lookup`], under an explicit ordering (milestone 2.4).
    pub fn lookup_with(&self, key: &[u8], cmp: KeyCmp<'_>) -> Result<usize, usize> {
        leaf_lookup(self.buf, self.num_keys(), key, cmp)
    }
}

/// Mutable leaf-page builder.
pub struct LeafMut<'a> {
    buf: &'a mut [u8],
    psize: u32,
}

impl<'a> LeafMut<'a> {
    /// Initialize `buf` as an empty leaf page with the given identity.
    ///
    /// Writes the common header (`flags = P_LEAF`), sets `lower = 0`,
    /// `upper = body_size`, and zeroes the reserved tail fields.
    ///
    /// # Errors
    ///
    /// [`PageError::BufferTooSmall`] or [`PageError::InvalidPageSize`] via
    /// [`super::geometry::validate_page_size`].
    pub fn init(
        buf: &'a mut [u8],
        psize: u32,
        pgno: u64,
        txnid: u64,
    ) -> Result<LeafMut<'a>, PageError> {
        super::geometry::validate_page_size(psize)?;
        if buf.len() < psize as usize {
            return Err(PageError::BufferTooSmall {
                got: buf.len(),
                psize: psize as usize,
            });
        }
        CommonHeader {
            pgno,
            txnid,
            flags: P_LEAF,
        }
        .write(buf);
        let body = body_size(psize) as u16;
        write_u16(buf, OFF_LOWER, 0);
        write_u16(buf, OFF_UPPER, body);
        write_u16(buf, OFF_LEAF2_KSIZE, 0);
        write_u16(buf, OFF_RESERVED1, 0);
        Ok(LeafMut { buf, psize })
    }

    /// Wrap an already-validated leaf page for mutation.
    ///
    /// Callers only ever hand this **engine-authored dirty frames** (a COW
    /// copy of a page fully validated on its first map access this txn, or
    /// the output of this txn's own page encoders — PERF-GAP batch 3 / A8),
    /// so validation is the same O(1) structural checks the read-side dirty
    /// path uses ([`LeafRef::new_prevalidated`]: type, reserved fields,
    /// bounds). Until A8 this silently re-ran the **full O(`num_keys`)**
    /// [`LeafRef::new`] cell walk — on every put (`insert_into_leaf` wraps
    /// the target leaf per call), which the milli write-phase profile showed
    /// as the single hottest zerodb cost.
    ///
    /// # Errors
    ///
    /// Propagates [`LeafRef::new_prevalidated`] validation.
    pub fn from_valid(buf: &'a mut [u8], psize: u32) -> Result<LeafMut<'a>, PageError> {
        LeafRef::new_prevalidated(buf, psize)?;
        Ok(LeafMut { buf, psize })
    }

    fn lower(&self) -> u16 {
        read_u16(self.buf, OFF_LOWER)
    }
    fn upper(&self) -> u16 {
        read_u16(self.buf, OFF_UPPER)
    }

    /// Number of entries currently on the page.
    #[must_use]
    pub fn num_keys(&self) -> usize {
        self.lower() as usize / 2
    }

    /// Free bytes available (`upper - lower`).
    #[must_use]
    pub fn free_space(&self) -> usize {
        self.upper() as usize - self.lower() as usize
    }

    /// Binary-search for `key` (see [`LeafRef::lookup`]).
    pub fn lookup(&self, key: &[u8]) -> Result<usize, usize> {
        leaf_lookup(self.buf, self.num_keys(), key, KeyCmp::Default)
    }

    /// As [`Self::lookup`], under an explicit ordering (milestone 2.4).
    pub fn lookup_with(&self, key: &[u8], cmp: KeyCmp<'_>) -> Result<usize, usize> {
        leaf_lookup(self.buf, self.num_keys(), key, cmp)
    }

    /// Insert an inline value at sorted index `idx`.
    ///
    /// # Errors
    ///
    /// [`PageError::BadKeySize`] for an empty/oversized key,
    /// [`PageError::BadValueSize`] for an oversized value, or
    /// [`PageError::PageFull`] if the cell does not fit.
    pub fn insert_inline(
        &mut self,
        idx: usize,
        key: &[u8],
        node_flags: u16,
        value: &[u8],
    ) -> Result<(), PageError> {
        if value.len() as u64 > MAX_DATA_SIZE as u64 {
            return Err(PageError::BadValueSize(value.len() as u64));
        }
        self.insert_raw(idx, key, node_flags, value.len() as u32, value)
    }

    /// Insert a `F_BIGDATA` pointer at sorted index `idx`: the value lives on an
    /// overflow run whose head is `head_pgno`, with true length `dsize`.
    ///
    /// # Errors
    ///
    /// As [`insert_inline`](LeafMut::insert_inline).
    pub fn insert_bigdata(
        &mut self,
        idx: usize,
        key: &[u8],
        dsize: u32,
        head_pgno: u64,
    ) -> Result<(), PageError> {
        let area = head_pgno.to_le_bytes();
        self.insert_raw(idx, key, F_BIGDATA, dsize, &area)
    }

    /// Insert an inline cell whose value bytes are **reserved** rather than
    /// copied (`MDB_RESERVE`, SPEC 01 §S3 / SPEC 04 TXN-47): the cell header,
    /// key, and pad byte are written, but the `dsize`-byte value area keeps
    /// whatever bytes the frame held (uninitialized-but-owned — the engine
    /// must not zero it). Returns the **absolute** byte offset of the value
    /// area within the page buffer; the caller re-borrows the frame to fill
    /// it before the txn's next operation (TXN-47).
    ///
    /// # Errors
    ///
    /// As [`insert_inline`](LeafMut::insert_inline).
    pub fn insert_inline_reserved(
        &mut self,
        idx: usize,
        key: &[u8],
        dsize: u32,
    ) -> Result<usize, PageError> {
        if dsize as u64 > MAX_DATA_SIZE as u64 {
            return Err(PageError::BadValueSize(dsize as u64));
        }
        self.insert_cell(idx, key, 0, dsize, dsize as usize, None)
    }

    /// Absolute byte offset of entry `idx`'s inline value area, together with
    /// its `dsize`. Errors if the entry is `F_BIGDATA` (its value lives on an
    /// overflow run, not in this page).
    ///
    /// # Errors
    ///
    /// [`PageError::WrongPageType`]-free; returns [`PageError::ReservedFlagSet`]
    /// never — a `F_BIGDATA` entry yields [`PageError::BadValueSize`] carrying
    /// the logical size, signalling "not inline".
    pub fn inline_value_at(&self, idx: usize) -> Result<(usize, u32), PageError> {
        debug_assert!(idx < self.num_keys(), "index out of range");
        let abs = HEADER_SIZE + ptr_at(self.buf, idx) as usize;
        let flags = read_u16(self.buf, abs);
        let ksize = read_u16(self.buf, abs + 2) as usize;
        let dsize = read_u32(self.buf, abs + 4);
        if flags & F_BIGDATA != 0 {
            return Err(PageError::BadValueSize(dsize as u64));
        }
        Ok((abs + LEAF_NODE_HEADER + ksize, dsize))
    }

    /// Low-level insert: places a cell whose value area is exactly `value_area`
    /// and whose logical length is `dsize`.
    fn insert_raw(
        &mut self,
        idx: usize,
        key: &[u8],
        node_flags: u16,
        dsize: u32,
        value_area: &[u8],
    ) -> Result<(), PageError> {
        self.insert_cell(
            idx,
            key,
            node_flags,
            dsize,
            value_area.len(),
            Some(value_area),
        )
        .map(|_| ())
    }

    /// Shared insert machinery: place a cell with a `value_area_len`-byte value
    /// region, copied from `fill` when given, left untouched when `None`
    /// (RESERVE). Returns the absolute offset of the value area.
    fn insert_cell(
        &mut self,
        idx: usize,
        key: &[u8],
        node_flags: u16,
        dsize: u32,
        value_area_len: usize,
        fill: Option<&[u8]>,
    ) -> Result<usize, PageError> {
        if key.is_empty() || key.len() > MAX_KEY_SIZE {
            return Err(PageError::BadKeySize { ksize: key.len() });
        }
        if node_flags & !LEAF_FLAGS_PHASE1_MASK != 0 {
            return Err(PageError::ReservedFlagSet { flags: node_flags });
        }
        let num_keys = self.num_keys();
        debug_assert!(idx <= num_keys, "insert index out of range");
        let clen = even(LEAF_NODE_HEADER + key.len() + value_area_len);
        let need = clen + 2;
        let avail = self.free_space();
        if need > avail {
            return Err(PageError::PageFull {
                needed: need,
                available: avail,
            });
        }
        let new_upper = self.upper() as usize - clen;
        let abs = HEADER_SIZE + new_upper;
        // Write the cell.
        write_u16(self.buf, abs, node_flags);
        write_u16(self.buf, abs + 2, key.len() as u16);
        write_u32(self.buf, abs + 4, dsize);
        self.buf[abs + LEAF_NODE_HEADER..abs + LEAF_NODE_HEADER + key.len()].copy_from_slice(key);
        let voff = abs + LEAF_NODE_HEADER + key.len();
        if let Some(value_area) = fill {
            self.buf[voff..voff + value_area.len()].copy_from_slice(value_area);
        }
        // Even-pad byte, if any.
        if clen > LEAF_NODE_HEADER + key.len() + value_area_len {
            self.buf[voff + value_area_len] = 0;
        }
        insert_pointer(self.buf, idx, num_keys, new_upper as u16);
        Ok(voff)
    }

    /// Remove entry `idx`, compacting the cell heap (SPEC 02 §2.2).
    ///
    /// # Errors
    ///
    /// Never fails for a valid `idx`; returns via `debug_assert` in debug if
    /// `idx` is out of range.
    pub fn remove(&mut self, idx: usize) {
        let num_keys = self.num_keys();
        debug_assert!(idx < num_keys, "remove index out of range");
        let cpos = ptr_at(self.buf, idx) as usize;
        let abs = HEADER_SIZE + cpos;
        let clen =
            leaf_cell_len(self.buf, abs, self.psize).expect("cell was validated on construction");
        remove_cell(self.buf, idx, num_keys, cpos, clen, self.upper() as usize);
    }
}

// ===========================================================================
// Branch pages
// ===========================================================================

/// Read-only view over a validated branch page (SPEC 02 §4.1).
#[derive(Debug, Clone, Copy)]
pub struct BranchRef<'a> {
    buf: &'a [u8],
    lower: u16,
    upper: u16,
}

impl<'a> BranchRef<'a> {
    /// Validate and wrap `buf` as a branch page of size `psize`.
    ///
    /// # Errors
    ///
    /// As [`LeafRef::new`], but for branch cells (index 0 may have an empty
    /// separator key).
    pub fn new(buf: &'a [u8], psize: u32) -> Result<BranchRef<'a>, PageError> {
        let view = Self::new_prevalidated(buf, psize)?;
        for i in 0..view.num_keys() {
            let rel = ptr_at(buf, i) as usize;
            check_ptr_in_heap(rel, view.upper, psize)?;
            branch_cell_len(buf, HEADER_SIZE + rel, psize, i == 0)?;
        }
        Ok(view)
    }

    /// [`BranchRef::new`] minus the per-cell loop — same contract as
    /// [`LeafRef::new_prevalidated`]: only for bytes that already passed the
    /// full constructor earlier in the same txn, behind
    /// `btree::ValidatedPages`.
    pub(crate) fn new_prevalidated(buf: &'a [u8], psize: u32) -> Result<BranchRef<'a>, PageError> {
        let hdr = CommonHeader::read(buf);
        if page_type_of(hdr.flags)? != PageType::Branch {
            return Err(PageError::WrongPageType {
                expected: PageType::Branch,
                found: page_type_of(hdr.flags)?,
            });
        }
        check_reserved_tail_fields(buf)?;
        let (lower, upper) = read_and_check_bounds(buf, psize)?;
        Ok(BranchRef { buf, lower, upper })
    }

    /// [`new_prevalidated`](Self::new_prevalidated) minus every check — same
    /// contract as [`LeafRef::new_trusted`], for a memo hit kind-tagged
    /// **branch** (PERF-GAP A8).
    pub(crate) fn new_trusted(buf: &'a [u8]) -> BranchRef<'a> {
        debug_assert!(matches!(
            page_type_of(CommonHeader::read(buf).flags),
            Ok(PageType::Branch)
        ));
        BranchRef {
            buf,
            lower: read_u16(buf, OFF_LOWER),
            upper: read_u16(buf, OFF_UPPER),
        }
    }

    /// Number of children on this page (`lower / 2`).
    #[must_use]
    pub fn num_keys(&self) -> usize {
        self.lower as usize / 2
    }

    /// Free bytes available (`upper - lower`).
    #[must_use]
    pub fn free_space(&self) -> usize {
        self.upper as usize - self.lower as usize
    }

    /// Absolute offset of cell `i`'s header (A3 — same contract as
    /// [`LeafRef`]'s `cell_abs`: callers guarantee `i < num_keys`).
    #[allow(unsafe_code)]
    fn cell_abs(&self, i: usize) -> usize {
        debug_assert!(i < self.num_keys());
        // SAFETY: as `LeafRef::cell_abs` — slot inside `[HEADER_SIZE,
        // HEADER_SIZE + lower)`, bounds checked at view construction or
        // inherited via the trusted-view contract.
        HEADER_SIZE + unsafe { read_u16_unchecked(self.buf, HEADER_SIZE + i * 2) } as usize
    }

    /// The child page number of entry `i`. Panics if `i >= num_keys`.
    #[must_use]
    #[allow(unsafe_code)]
    pub fn child_pgno(&self, i: usize) -> u64 {
        assert!(i < self.num_keys(), "branch entry index out of range");
        // SAFETY (A3 view contract): cell `i` was proven in-bounds by the
        // full validation walk, or inherits that proof (kind-tagged memo hit
        // / engine-authored dirty frame); its 10-byte header is inside `buf`.
        unsafe { read_u64_unchecked(self.buf, self.cell_abs(i)) }
    }

    /// The separator key of entry `i` (empty slice for index 0). Panics if
    /// `i >= num_keys`.
    #[must_use]
    #[allow(unsafe_code)]
    pub fn key(&self, i: usize) -> &'a [u8] {
        assert!(i < self.num_keys(), "branch entry index out of range");
        let abs = self.cell_abs(i);
        // SAFETY (A3 view contract, as `child_pgno`): `branch_cell_len`
        // bounded the header and `header + ksize` separator span in `buf`.
        unsafe {
            let ksize = read_u16_unchecked(self.buf, abs + 8) as usize;
            self.buf
                .get_unchecked(abs + BRANCH_NODE_HEADER..abs + BRANCH_NODE_HEADER + ksize)
        }
    }

    /// The child index whose subtree covers `key` (SPEC 03 §2:
    /// `branch_child_index` — greatest `i` with `sep(i) <= key`, node 0 = −∞).
    #[must_use]
    pub fn child_index(&self, key: &[u8]) -> usize {
        self.child_index_with(key, KeyCmp::Default)
    }

    /// As [`Self::child_index`], under an explicit ordering (milestone 2.4).
    #[must_use]
    pub fn child_index_with(&self, key: &[u8], cmp: KeyCmp<'_>) -> usize {
        // Node 0 is -inf and always qualifies; scan separators 1..num_keys.
        let n = self.num_keys();
        let mut lo = 1usize;
        let mut hi = n; // first index whose sep > key
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if cmp.compare(self.key(mid), key) != std::cmp::Ordering::Greater {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo - 1
    }
}

/// Mutable branch-page builder.
pub struct BranchMut<'a> {
    buf: &'a mut [u8],
    psize: u32,
}

impl<'a> BranchMut<'a> {
    /// Wrap an already-validated branch page for mutation.
    ///
    /// Same contract and same A8 change as [`LeafMut::from_valid`]: callers
    /// only hand this engine-authored dirty frames, so validation is the
    /// O(1) structural checks ([`BranchRef::new_prevalidated`]) — the full
    /// per-cell walk ran here on every parent-chain touch until A8.
    ///
    /// # Errors
    ///
    /// Propagates [`BranchRef::new_prevalidated`] validation.
    pub fn from_valid(buf: &'a mut [u8], psize: u32) -> Result<BranchMut<'a>, PageError> {
        BranchRef::new_prevalidated(buf, psize)?;
        Ok(BranchMut { buf, psize })
    }

    /// Overwrite the child pgno of entry `idx` in place (COW parent-chain
    /// pointer rewrite, SPEC 03 §5.3). The cell length is unchanged.
    pub fn set_child_pgno(&mut self, idx: usize, child_pgno: u64) {
        debug_assert!(idx < self.num_keys(), "index out of range");
        let abs = HEADER_SIZE + ptr_at(self.buf, idx) as usize;
        write_u64(self.buf, abs, child_pgno);
    }

    /// Initialize `buf` as an empty branch page with the given identity.
    ///
    /// # Errors
    ///
    /// As [`LeafMut::init`].
    pub fn init(
        buf: &'a mut [u8],
        psize: u32,
        pgno: u64,
        txnid: u64,
    ) -> Result<BranchMut<'a>, PageError> {
        super::geometry::validate_page_size(psize)?;
        if buf.len() < psize as usize {
            return Err(PageError::BufferTooSmall {
                got: buf.len(),
                psize: psize as usize,
            });
        }
        CommonHeader {
            pgno,
            txnid,
            flags: P_BRANCH,
        }
        .write(buf);
        let body = body_size(psize) as u16;
        write_u16(buf, OFF_LOWER, 0);
        write_u16(buf, OFF_UPPER, body);
        write_u16(buf, OFF_LEAF2_KSIZE, 0);
        write_u16(buf, OFF_RESERVED1, 0);
        Ok(BranchMut { buf, psize })
    }

    fn lower(&self) -> u16 {
        read_u16(self.buf, OFF_LOWER)
    }
    fn upper(&self) -> u16 {
        read_u16(self.buf, OFF_UPPER)
    }

    /// Number of children currently on the page.
    #[must_use]
    pub fn num_keys(&self) -> usize {
        self.lower() as usize / 2
    }

    /// Free bytes available (`upper - lower`).
    #[must_use]
    pub fn free_space(&self) -> usize {
        self.upper() as usize - self.lower() as usize
    }

    /// Insert a `(separator key -> child pgno)` at sorted index `idx`. Index 0
    /// must carry an empty key; all other indices a key of 1–511 bytes.
    ///
    /// # Errors
    ///
    /// [`PageError::BadKeySize`] on an invalid key for the position, or
    /// [`PageError::PageFull`] if the cell does not fit.
    pub fn insert(&mut self, idx: usize, key: &[u8], child_pgno: u64) -> Result<(), PageError> {
        if idx == 0 {
            if !key.is_empty() {
                return Err(PageError::BadKeySize { ksize: key.len() });
            }
        } else if key.is_empty() || key.len() > MAX_KEY_SIZE {
            return Err(PageError::BadKeySize { ksize: key.len() });
        }
        let num_keys = self.num_keys();
        debug_assert!(idx <= num_keys, "insert index out of range");
        let clen = even(BRANCH_NODE_HEADER + key.len());
        let need = clen + 2;
        let avail = self.free_space();
        if need > avail {
            return Err(PageError::PageFull {
                needed: need,
                available: avail,
            });
        }
        let new_upper = self.upper() as usize - clen;
        let abs = HEADER_SIZE + new_upper;
        write_u64(self.buf, abs, child_pgno);
        write_u16(self.buf, abs + 8, key.len() as u16);
        self.buf[abs + BRANCH_NODE_HEADER..abs + BRANCH_NODE_HEADER + key.len()]
            .copy_from_slice(key);
        if clen > BRANCH_NODE_HEADER + key.len() {
            self.buf[abs + BRANCH_NODE_HEADER + key.len()] = 0;
        }
        insert_pointer(self.buf, idx, num_keys, new_upper as u16);
        Ok(())
    }

    /// Remove child entry `idx`, compacting the cell heap.
    pub fn remove(&mut self, idx: usize) {
        let num_keys = self.num_keys();
        debug_assert!(idx < num_keys, "remove index out of range");
        let cpos = ptr_at(self.buf, idx) as usize;
        let abs = HEADER_SIZE + cpos;
        let clen = branch_cell_len(self.buf, abs, self.psize, idx == 0)
            .expect("cell was validated on construction");
        remove_cell(self.buf, idx, num_keys, cpos, clen, self.upper() as usize);
    }
}

// ===========================================================================
// Shared array/heap primitives
// ===========================================================================

/// Insert `new_ptr` at pointer-array index `idx`, shifting the tail up, and
/// bump `lower` (+2) and `upper` (to `new_ptr`).
fn insert_pointer(buf: &mut [u8], idx: usize, num_keys: usize, new_ptr: u16) {
    // Shift pointers [idx..num_keys] one slot right (2 bytes each).
    let src_start = HEADER_SIZE + idx * 2;
    let src_end = HEADER_SIZE + num_keys * 2;
    let dst_start = HEADER_SIZE + (idx + 1) * 2;
    buf.copy_within(src_start..src_end, dst_start);
    write_u16(buf, src_start, new_ptr);
    let new_lower = (num_keys + 1) * 2;
    write_u16(buf, OFF_LOWER, new_lower as u16);
    write_u16(buf, OFF_UPPER, new_ptr);
}

/// Remove pointer `idx` and compact the heap: cells below `cpos` move up by
/// `clen`, affected pointers are adjusted, `lower` drops by 2, `upper` rises by
/// `clen`.
fn remove_cell(
    buf: &mut [u8],
    idx: usize,
    num_keys: usize,
    cpos: usize,
    clen: usize,
    upper: usize,
) {
    // 1. Slide the heap region [upper, cpos) up by clen bytes.
    let abs_upper = HEADER_SIZE + upper;
    let abs_cpos = HEADER_SIZE + cpos;
    buf.copy_within(abs_upper..abs_cpos, abs_upper + clen);
    // 2. Adjust every pointer that referenced a cell below the removed one.
    for j in 0..num_keys {
        let p = ptr_at(buf, j) as usize;
        if p < cpos {
            write_u16(buf, HEADER_SIZE + j * 2, (p + clen) as u16);
        }
    }
    // 3. Remove pointer idx: shift [idx+1..num_keys] down one slot.
    let src_start = HEADER_SIZE + (idx + 1) * 2;
    let src_end = HEADER_SIZE + num_keys * 2;
    let dst_start = HEADER_SIZE + idx * 2;
    buf.copy_within(src_start..src_end, dst_start);
    // 4. Update bounds.
    let new_lower = (num_keys - 1) * 2;
    write_u16(buf, OFF_LOWER, new_lower as u16);
    write_u16(buf, OFF_UPPER, (upper + clen) as u16);
}

/// Binary-search a leaf's sorted pointer array for `key` under `cmp`
/// (milestone 2.4: the ordering is the tree's, not necessarily memcmp —
/// SPEC 03 §2.0).
///
/// A3 contract: `buf` is a validated/trusted leaf page's buffer and
/// `num_keys` is **that page's** entry count — every caller derives both from
/// a constructed view (`LeafRef::lookup{,_with}`, `btree`'s descent over
/// `leaf_view`s), so `mid < num_keys` makes the unchecked reads in-bounds by
/// the view contract.
#[allow(unsafe_code)]
pub(crate) fn leaf_lookup(
    buf: &[u8],
    num_keys: usize,
    key: &[u8],
    cmp: KeyCmp<'_>,
) -> Result<usize, usize> {
    let mut lo = 0usize;
    let mut hi = num_keys;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        // SAFETY: `mid < num_keys` (binary-search invariant) and the A3 view
        // contract above — pointer slot, cell header, and key span were all
        // bounds-proven when the page validated (or are engine-authored).
        let mid_key = unsafe {
            let abs = HEADER_SIZE + read_u16_unchecked(buf, HEADER_SIZE + mid * 2) as usize;
            let ksize = read_u16_unchecked(buf, abs + 2) as usize;
            buf.get_unchecked(abs + LEAF_NODE_HEADER..abs + LEAF_NODE_HEADER + ksize)
        };
        match cmp.compare(mid_key, key) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => return Ok(mid),
        }
    }
    Err(lo)
}

/// Validate that a node pointer references the heap region: even, `>= upper`,
/// and its header start is in-body.
fn check_ptr_in_heap(rel: usize, upper: u16, psize: u32) -> Result<(), PageError> {
    let body = body_size(psize);
    if rel % 2 != 0 || rel < upper as usize || rel >= body {
        return Err(PageError::CellOutOfBounds {
            offset: rel,
            needed: 0,
            body_size: body,
        });
    }
    Ok(())
}

/// Reject a tree page whose reserved `leaf2_ksize` (offset 28) or `reserved1`
/// (offset 30) tail fields are non-zero (SPEC 02 §2.1, §10).
fn check_reserved_tail_fields(buf: &[u8]) -> Result<(), PageError> {
    let leaf2 = read_u16(buf, OFF_LEAF2_KSIZE);
    if leaf2 != 0 {
        return Err(PageError::ReservedFieldNonZero {
            field: "leaf2_ksize",
            value: leaf2 as u64,
        });
    }
    let reserved1 = read_u16(buf, OFF_RESERVED1);
    if reserved1 != 0 {
        return Err(PageError::ReservedFieldNonZero {
            field: "reserved1",
            value: reserved1 as u64,
        });
    }
    Ok(())
}
