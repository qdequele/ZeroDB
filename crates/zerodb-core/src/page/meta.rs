//! Meta pages (slots 0 and 1) — SPEC 02 §3.
//!
//! Pages 0 and 1 are the double-buffered meta slots. Each carries the common
//! header (`flags = P_META`) followed by the meta body, and is protected by a
//! mandatory CRC32C over its first [`META_CONTENT_LEN`] bytes. This module owns
//! the full field map ([`DBRecord`], [`MetaPage`]), the CRC coverage, the
//! validation predicate ([`MetaPage::validate`], the single owner of SPEC 02
//! §3.2's numbered list), and the double-buffer selection formula
//! ([`select`]).

use super::crc32c::crc32c_concat;
use super::geometry::validate_page_size;
use super::header::CommonHeader;
use super::raw::{read_u16, read_u32, read_u64, write_u16, write_u32, write_u64};
use super::{
    PageError, FORMAT_VERSION, MAGIC, META_ANNEX_OFF, META_CONTENT_LEN, PGNO_INVALID, P_META,
};

// Meta body field offsets (absolute within the page).
const OFF_MAGIC: usize = 32;
const OFF_FORMAT_VERSION: usize = 36;
const OFF_PAGE_SIZE: usize = 40;
const OFF_ENV_FLAGS: usize = 44;
const OFF_MAP_SIZE: usize = 48;
const OFF_LAST_PG: usize = 56;
const OFF_BODY_TXNID: usize = 64;
const OFF_FREE_DB: usize = 72;
const OFF_MAIN_DB: usize = 120;
const OFF_FL_COUNT: usize = 168;
const OFF_META_CRC: usize = META_CONTENT_LEN; // 172 (SPEC 02 §3, format v2)

/// Maximum number of free-list annex ids a meta page of `psize` bytes can
/// carry (SPEC 02 §3, ADR-0022): the ids start at [`META_ANNEX_OFF`] and run
/// to the end of the page.
#[must_use]
pub fn meta_annex_cap(psize: u32) -> usize {
    (psize as usize).saturating_sub(META_ANNEX_OFF) / 8
}

/// The meta CRC32C with the ADR-0022 split coverage: `[0, 172)` followed by
/// the `8·fl_count` annex-id bytes at [`META_ANNEX_OFF`], skipping the CRC
/// field itself. `buf` must hold the whole page; `fl_count` must already be
/// bounds-checked against [`meta_annex_cap`].
fn meta_crc_of(buf: &[u8], fl_count: usize) -> u32 {
    crc32c_concat(&[
        &buf[..META_CONTENT_LEN],
        &buf[META_ANNEX_OFF..META_ANNEX_OFF + 8 * fl_count],
    ])
}

/// Size of a [`DBRecord`], in bytes.
pub const DBRECORD_LEN: usize = 48;

// DBRecord field offsets (relative to the record's base).
const DB_OFF_ROOT: usize = 0;
const DB_OFF_BRANCH_PAGES: usize = 8;
const DB_OFF_LEAF_PAGES: usize = 16;
const DB_OFF_OVERFLOW_PAGES: usize = 24;
const DB_OFF_ENTRIES: usize = 32;
const DB_OFF_DEPTH: usize = 40;
const DB_OFF_FLAGS: usize = 42;
const DB_OFF_LEAF2_KSIZE: usize = 44;

/// The root and statistics of one B+tree (SPEC 02 §3.1). 48 bytes on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DBRecord {
    /// Root page number, or [`PGNO_INVALID`] for an empty tree.
    pub root: u64,
    /// Count of branch pages in the tree.
    pub branch_pages: u64,
    /// Count of leaf pages.
    pub leaf_pages: u64,
    /// Count of overflow pages (sum of all runs).
    pub overflow_pages: u64,
    /// Number of key/value pairs.
    pub entries: u64,
    /// Tree height (0 = empty, 1 = root-is-leaf).
    pub depth: u16,
    /// Persistent DB flags — reserved (Phase 2.8); 0 in Phase 1.
    pub flags: u16,
    /// Reserved (Phase 2.8 DUPFIXED); 0 in Phase 1.
    pub leaf2_ksize: u32,
}

impl DBRecord {
    /// An empty tree's record: `root = PGNO_INVALID`, all stats 0.
    #[must_use]
    pub fn empty() -> DBRecord {
        DBRecord {
            root: PGNO_INVALID,
            branch_pages: 0,
            leaf_pages: 0,
            overflow_pages: 0,
            entries: 0,
            depth: 0,
            flags: 0,
            leaf2_ksize: 0,
        }
    }

    /// Encode this record as a standalone 48-byte catalog value (SPEC 02 §6:
    /// the value of a named DB's `F_SUBDATA` catalog entry in the main tree).
    #[must_use]
    pub fn to_bytes(&self) -> [u8; DBRECORD_LEN] {
        let mut buf = [0u8; DBRECORD_LEN];
        self.write(&mut buf, 0);
        buf
    }

    /// Decode a `DBRecord` from a standalone 48-byte catalog value (SPEC 02
    /// §6). Returns `None` if the slice is not exactly [`DBRECORD_LEN`] bytes —
    /// a catalog entry whose value is the wrong size is not a valid sub-DB
    /// record (a user-key collision, not a named DB).
    #[must_use]
    pub fn from_bytes(buf: &[u8]) -> Option<DBRecord> {
        if buf.len() != DBRECORD_LEN {
            return None;
        }
        Some(DBRecord::read(buf, 0))
    }

    /// Decode a `DBRecord` from `buf[base..base + 48]`.
    fn read(buf: &[u8], base: usize) -> DBRecord {
        DBRecord {
            root: read_u64(buf, base + DB_OFF_ROOT),
            branch_pages: read_u64(buf, base + DB_OFF_BRANCH_PAGES),
            leaf_pages: read_u64(buf, base + DB_OFF_LEAF_PAGES),
            overflow_pages: read_u64(buf, base + DB_OFF_OVERFLOW_PAGES),
            entries: read_u64(buf, base + DB_OFF_ENTRIES),
            depth: read_u16(buf, base + DB_OFF_DEPTH),
            flags: read_u16(buf, base + DB_OFF_FLAGS),
            leaf2_ksize: read_u32(buf, base + DB_OFF_LEAF2_KSIZE),
        }
    }

    /// Encode this `DBRecord` into `buf[base..base + 48]`.
    fn write(&self, buf: &mut [u8], base: usize) {
        write_u64(buf, base + DB_OFF_ROOT, self.root);
        write_u64(buf, base + DB_OFF_BRANCH_PAGES, self.branch_pages);
        write_u64(buf, base + DB_OFF_LEAF_PAGES, self.leaf_pages);
        write_u64(buf, base + DB_OFF_OVERFLOW_PAGES, self.overflow_pages);
        write_u64(buf, base + DB_OFF_ENTRIES, self.entries);
        write_u16(buf, base + DB_OFF_DEPTH, self.depth);
        write_u16(buf, base + DB_OFF_FLAGS, self.flags);
        write_u32(buf, base + DB_OFF_LEAF2_KSIZE, self.leaf2_ksize);
    }
}

/// A decoded meta page (SPEC 02 §3). The `pgno` selects the slot (0 or 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaPage {
    /// This slot's page number (0 or 1).
    pub pgno: u64,
    /// Commit txnid of this meta (header stamp; equals the body copy).
    pub txnid: u64,
    /// File identifier, must equal [`MAGIC`].
    pub magic: [u8; 4],
    /// On-disk format version.
    pub format_version: u32,
    /// The DB's page size (authoritative record).
    pub page_size: u32,
    /// Persistent env flags (reserved; 0 in Phase 1).
    pub env_flags: u32,
    /// Configured map size in bytes.
    pub map_size: u64,
    /// Highest page number allocated as of this txn (file high-water).
    pub last_pg: u64,
    /// Root/stats of the free (GC) DB.
    pub free_db: DBRecord,
    /// Root/stats of the main/catalog DB.
    pub main_db: DBRecord,
    /// Free-list annex id count (SPEC 02 §3 format v2, ADR-0022). The ids
    /// themselves stay in the page buffer (offset [`META_ANNEX_OFF`]) and are
    /// read with [`MetaPage::read_annex`]; this decoded struct stays `Copy`.
    pub fl_count: u32,
}

impl MetaPage {
    /// Build the initial meta for `slot` (0 or 1) of a freshly created env
    /// (SPEC 02 §3.4): txnid 0, empty `free_db`/`main_db`, `last_pg = 1`.
    #[must_use]
    pub fn create(slot: u64, page_size: u32, map_size: u64) -> MetaPage {
        MetaPage {
            pgno: slot,
            txnid: 0,
            magic: MAGIC,
            format_version: FORMAT_VERSION,
            page_size,
            env_flags: 0,
            map_size,
            last_pg: 1,
            free_db: DBRecord::empty(),
            main_db: DBRecord::empty(),
            fl_count: 0,
        }
    }

    /// Encode this meta into `buf` (which must be at least `page_size` bytes),
    /// with an **empty** free-list annex, computing and writing the CRC and
    /// zeroing the reserved tail. See [`MetaPage::encode_with_annex`].
    ///
    /// # Errors
    ///
    /// [`PageError::InvalidPageSize`] or [`PageError::BufferTooSmall`].
    pub fn encode(&self, buf: &mut [u8]) -> Result<(), PageError> {
        self.encode_with_annex(buf, &[])
    }

    /// Encode this meta into `buf` with `annex` as the free-list annex ids
    /// (SPEC 02 §3 format v2, ADR-0022; SPEC 05 §2a). The caller guarantees
    /// the GC-29 shape (strictly ascending, unique, in range) — `freelist_save`
    /// produces exactly that; this encoder only enforces the capacity bound.
    ///
    /// # Errors
    ///
    /// [`PageError::InvalidPageSize`], [`PageError::BufferTooSmall`], or
    /// [`PageError::BadValueSize`] if `annex` exceeds [`meta_annex_cap`]
    /// (engine bug: the save's fit check owns that bound).
    pub fn encode_with_annex(&self, buf: &mut [u8], annex: &[u64]) -> Result<(), PageError> {
        validate_page_size(self.page_size)?;
        let psize = self.page_size as usize;
        if buf.len() < psize {
            return Err(PageError::BufferTooSmall {
                got: buf.len(),
                psize,
            });
        }
        if annex.len() > meta_annex_cap(self.page_size) {
            return Err(PageError::BadValueSize(annex.len() as u64));
        }
        debug_assert_eq!(
            self.fl_count as usize,
            annex.len(),
            "MetaPage.fl_count must match the annex slice (single source: the ids)"
        );
        // Zero the whole page first so every reserved byte is 0.
        buf[..psize].fill(0);
        // Common header (zeros reserved0 + checksum; variant tail already 0).
        CommonHeader {
            pgno: self.pgno,
            txnid: self.txnid,
            flags: P_META,
        }
        .write(buf);
        // Meta body.
        buf[OFF_MAGIC..OFF_MAGIC + 4].copy_from_slice(&self.magic);
        write_u32(buf, OFF_FORMAT_VERSION, self.format_version);
        write_u32(buf, OFF_PAGE_SIZE, self.page_size);
        write_u32(buf, OFF_ENV_FLAGS, self.env_flags);
        write_u64(buf, OFF_MAP_SIZE, self.map_size);
        write_u64(buf, OFF_LAST_PG, self.last_pg);
        write_u64(buf, OFF_BODY_TXNID, self.txnid);
        self.free_db.write(buf, OFF_FREE_DB);
        self.main_db.write(buf, OFF_MAIN_DB);
        // Free-list annex (format v2): count at 168, ids from 176.
        write_u32(buf, OFF_FL_COUNT, annex.len() as u32);
        for (i, &id) in annex.iter().enumerate() {
            write_u64(buf, META_ANNEX_OFF + 8 * i, id);
        }
        // CRC over [0, 172) ∪ the annex ids (SPEC 02 §3.3).
        let crc = meta_crc_of(buf, annex.len());
        write_u32(buf, OFF_META_CRC, crc);
        Ok(())
    }

    /// Read the free-list annex ids out of a meta page buffer (SPEC 02 §3,
    /// format v2). Returns `None` if `fl_count` exceeds the page's capacity —
    /// callers treat that as a corrupt freelist (`MdbError::Invalid`), though
    /// for a slot that passed [`MetaPage::validate`] the bound already held.
    /// The ids' GC-29 shape (ascending, in range) is **not** checked here:
    /// the consumer runs `validate_pil_ids` before any id is handed out
    /// (SPEC 05 GC-33), exactly as for a tree PIL.
    #[must_use]
    pub fn read_annex(buf: &[u8], psize: u32) -> Option<Vec<u64>> {
        if buf.len() < psize as usize {
            return None;
        }
        let count = read_u32(buf, OFF_FL_COUNT) as usize;
        if count > meta_annex_cap(psize) {
            return None;
        }
        let mut ids = Vec::with_capacity(count);
        for i in 0..count {
            ids.push(read_u64(buf, META_ANNEX_OFF + 8 * i));
        }
        Some(ids)
    }

    /// Validate `buf` as a meta slot, following SPEC 02 §3.2's numbered list
    /// (the single owner of the meta-slot validation predicate). Returns a rich
    /// [`MetaValidity`] verdict; validity failures are *data*, not errors.
    ///
    /// # Errors
    ///
    /// [`PageError::InvalidPageSize`] if the reader's `psize` is out of range,
    /// or [`PageError::BufferTooSmall`] if `buf` is shorter than one page. (The
    /// meta's *own* `page_size` field is checked as rule 3 and surfaced via
    /// [`MetaValidity::BadPageSize`], not as an error.)
    pub fn validate(buf: &[u8], psize: u32) -> Result<MetaValidity, PageError> {
        validate_page_size(psize)?;
        if buf.len() < psize as usize {
            return Err(PageError::BufferTooSmall {
                got: buf.len(),
                psize: psize as usize,
            });
        }
        // Rule 1: magic.
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&buf[OFF_MAGIC..OFF_MAGIC + 4]);
        if magic != MAGIC {
            return Ok(MetaValidity::BadMagic);
        }
        // Rule 2: format version.
        let format_version = read_u32(buf, OFF_FORMAT_VERSION);
        if format_version != FORMAT_VERSION {
            return Ok(MetaValidity::BadVersion(format_version));
        }
        // Rule 3: page_size power of two in range.
        let page_size = read_u32(buf, OFF_PAGE_SIZE);
        if validate_page_size(page_size).is_err() {
            return Ok(MetaValidity::BadPageSize(page_size));
        }
        // Rule 4: header txnid (offset 8) == body txnid (offset 64).
        let hdr = CommonHeader::read(buf);
        let body_txnid = read_u64(buf, OFF_BODY_TXNID);
        if hdr.txnid != body_txnid {
            return Ok(MetaValidity::TxnidMismatch {
                header: hdr.txnid,
                body: body_txnid,
            });
        }
        // Rule 5 (format v2, ADR-0022): the annex count is bounds-checked
        // BEFORE the CRC — a hostile count must not drive the CRC read out
        // of the page — then the CRC covers [0, 172) ∪ the annex ids.
        let fl_count = read_u32(buf, OFF_FL_COUNT) as usize;
        if fl_count > meta_annex_cap(psize) {
            return Ok(MetaValidity::BadAnnexCount(fl_count as u32));
        }
        let stored = read_u32(buf, OFF_META_CRC);
        let computed = meta_crc_of(buf, fl_count);
        if stored != computed {
            return Ok(MetaValidity::BadCrc { stored, computed });
        }
        // All rules passed: decode the full meta.
        Ok(MetaValidity::Valid(MetaPage {
            pgno: hdr.pgno,
            txnid: hdr.txnid,
            magic,
            format_version,
            page_size,
            env_flags: read_u32(buf, OFF_ENV_FLAGS),
            map_size: read_u64(buf, OFF_MAP_SIZE),
            last_pg: read_u64(buf, OFF_LAST_PG),
            free_db: DBRecord::read(buf, OFF_FREE_DB),
            main_db: DBRecord::read(buf, OFF_MAIN_DB),
            fl_count: fl_count as u32,
        }))
    }
}

/// The verdict of validating one meta slot (SPEC 02 §3.2). The env layer (M1.2)
/// maps these to `MdbError` values; this crate never panics on a bad slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaValidity {
    /// The slot is fully valid; carries the decoded meta.
    Valid(MetaPage),
    /// `magic` does not match — not a ZeroDB env.
    BadMagic,
    /// `format_version` mismatch (the observed version).
    BadVersion(u32),
    /// `page_size` is not a power of two in range (the observed value).
    BadPageSize(u32),
    /// `fl_count` exceeds the page's annex capacity (SPEC 02 §3.2 rule 5,
    /// format v2) — the slot is invalid (torn or hostile).
    BadAnnexCount(u32),
    /// Header txnid and body txnid disagree — a torn write (INV-2).
    TxnidMismatch {
        /// Header stamp (offset 8).
        header: u64,
        /// Body copy (offset 64).
        body: u64,
    },
    /// CRC mismatch — the slot is torn.
    BadCrc {
        /// The stored `meta_crc`.
        stored: u32,
        /// The recomputed CRC over `[0, 168)`.
        computed: u32,
    },
}

impl MetaValidity {
    /// The decoded meta if this slot is [`MetaValidity::Valid`].
    #[must_use]
    pub fn meta(&self) -> Option<&MetaPage> {
        match self {
            MetaValidity::Valid(m) => Some(m),
            _ => None,
        }
    }

    /// Whether this slot is fully valid.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        matches!(self, MetaValidity::Valid(_))
    }
}

/// The outcome of double-buffer selection (SPEC 02 §3.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaChoice {
    /// Both slots were valid; selection applied the higher-txnid (or, under
    /// `prev_snapshot`, the lower-txnid) rule.
    Both {
        /// The chosen slot index (0 or 1).
        chosen: usize,
        /// The chosen meta.
        meta: MetaPage,
    },
    /// Exactly one slot was valid (torn-meta recovery): it wins regardless of
    /// txnid, and — because there is no older valid slot — regardless of
    /// `prev_snapshot`. See the M1.1 report note on REC-2 (pending human
    /// ratification of the one-valid + `prev_snapshot` policy).
    OnlyOne {
        /// The chosen slot index (0 or 1).
        chosen: usize,
        /// The chosen meta.
        meta: MetaPage,
    },
    /// Neither slot was valid — unrecoverable (the env layer maps this to
    /// `MdbError::Invalid`).
    None,
}

/// Select the live meta between two validated slots (SPEC 02 §3.2).
///
/// Among CRC-valid slots the chosen index is
/// `(txnid[0] < txnid[1]) XOR prev_snapshot`. If exactly one slot is valid, it
/// wins regardless of txnid (torn-meta recovery). If neither is valid, the
/// result is [`MetaChoice::None`].
#[must_use]
pub fn select(slot0: &MetaValidity, slot1: &MetaValidity, prev_snapshot: bool) -> MetaChoice {
    match (slot0.meta(), slot1.meta()) {
        (Some(m0), Some(m1)) => {
            let pick_higher_is_1 = m0.txnid < m1.txnid;
            let chosen = if pick_higher_is_1 ^ prev_snapshot {
                1
            } else {
                0
            };
            let meta = if chosen == 1 { *m1 } else { *m0 };
            MetaChoice::Both { chosen, meta }
        }
        (Some(m0), None) => MetaChoice::OnlyOne {
            chosen: 0,
            meta: *m0,
        },
        (None, Some(m1)) => MetaChoice::OnlyOne {
            chosen: 1,
            meta: *m1,
        },
        (None, None) => MetaChoice::None,
    }
}
