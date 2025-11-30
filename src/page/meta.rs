//! Meta page structure containing database metadata.

use super::{PageHeader, PageNo, TxnId};
use crate::error::{Error, Result};
use crate::{MDB_MAGIC, MDB_VERSION};

/// Size of the meta page structure in bytes (excluding page header).
pub const META_PAGE_SIZE: usize = 136;

/// Size of the DbInfo structure in bytes.
pub const DB_INFO_SIZE: usize = 48;

/// Database information structure.
///
/// Contains statistics and root page information for a single database.
/// This structure appears twice in the meta page: once for the freelist
/// database and once for the main database.
///
/// ```text
/// Offset  Size  Field
/// 0       4     pad
/// 4       2     flags
/// 6       2     depth
/// 8       8     branch_pages
/// 16      8     leaf_pages
/// 24      8     overflow_pages
/// 32      8     entries
/// 40      8     root
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(C)]
pub struct DbInfo {
    /// Padding for alignment.
    pub pad: u32,
    /// Database flags (MDB_DUPSORT, etc.).
    pub flags: u16,
    /// Depth of B+tree (0 = empty, 1 = root only).
    pub depth: u16,
    /// Number of internal (branch) pages.
    pub branch_pages: u64,
    /// Number of leaf pages.
    pub leaf_pages: u64,
    /// Number of overflow pages.
    pub overflow_pages: u64,
    /// Number of data entries.
    pub entries: u64,
    /// Root page number (P_INVALID if empty).
    pub root: PageNo,
}

impl DbInfo {
    /// Creates a new empty database info.
    pub fn new() -> Self {
        Self {
            pad: 0,
            flags: 0,
            depth: 0,
            branch_pages: 0,
            leaf_pages: 0,
            overflow_pages: 0,
            entries: 0,
            root: PageNo::MAX, // P_INVALID
        }
    }

    /// Returns true if this database is empty.
    pub fn is_empty(&self) -> bool {
        self.entries == 0
    }

    /// Reads a DbInfo from a byte slice.
    pub fn read_from(data: &[u8]) -> Result<Self> {
        if data.len() < DB_INFO_SIZE {
            return Err(Error::Corrupted);
        }

        Ok(Self {
            pad: u32::from_le_bytes([data[0], data[1], data[2], data[3]]),
            flags: u16::from_le_bytes([data[4], data[5]]),
            depth: u16::from_le_bytes([data[6], data[7]]),
            branch_pages: u64::from_le_bytes([
                data[8], data[9], data[10], data[11],
                data[12], data[13], data[14], data[15],
            ]),
            leaf_pages: u64::from_le_bytes([
                data[16], data[17], data[18], data[19],
                data[20], data[21], data[22], data[23],
            ]),
            overflow_pages: u64::from_le_bytes([
                data[24], data[25], data[26], data[27],
                data[28], data[29], data[30], data[31],
            ]),
            entries: u64::from_le_bytes([
                data[32], data[33], data[34], data[35],
                data[36], data[37], data[38], data[39],
            ]),
            root: u64::from_le_bytes([
                data[40], data[41], data[42], data[43],
                data[44], data[45], data[46], data[47],
            ]),
        })
    }

    /// Writes the DbInfo to a byte slice.
    pub fn write_to(&self, data: &mut [u8]) -> Result<()> {
        if data.len() < DB_INFO_SIZE {
            return Err(Error::Corrupted);
        }

        data[0..4].copy_from_slice(&self.pad.to_le_bytes());
        data[4..6].copy_from_slice(&self.flags.to_le_bytes());
        data[6..8].copy_from_slice(&self.depth.to_le_bytes());
        data[8..16].copy_from_slice(&self.branch_pages.to_le_bytes());
        data[16..24].copy_from_slice(&self.leaf_pages.to_le_bytes());
        data[24..32].copy_from_slice(&self.overflow_pages.to_le_bytes());
        data[32..40].copy_from_slice(&self.entries.to_le_bytes());
        data[40..48].copy_from_slice(&self.root.to_le_bytes());

        Ok(())
    }
}

/// Meta page structure.
///
/// The meta page contains all the metadata needed to access the database.
/// There are always two meta pages at the beginning of the file (pages 0 and 1),
/// and they alternate on each commit to provide atomic updates.
///
/// ```text
/// Offset  Size  Field
/// 0       16    Page header
/// 16      4     magic (0xBEEFC0DE)
/// 20      4     version (1)
/// 24      8     address (fixed mmap address, usually 0)
/// 32      8     map_size
/// 40      48    free_db (freelist database info)
/// 88      48    main_db (main database info)
/// 136     8     last_pgno (last used page number)
/// 144     8     last_txnid (transaction ID)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct MetaPage {
    /// Page header.
    pub header: PageHeader,
    /// Magic number - must be MDB_MAGIC (0xBEEFC0DE).
    pub magic: u32,
    /// Data format version - must be MDB_VERSION (1).
    pub version: u32,
    /// Address for fixed mmap (0 for portable).
    pub address: u64,
    /// Size of the mmap region.
    pub map_size: u64,
    /// Info about the freelist database.
    pub free_db: DbInfo,
    /// Info about the main database.
    pub main_db: DbInfo,
    /// Last used page number.
    pub last_pgno: PageNo,
    /// Last committed transaction ID.
    pub last_txnid: TxnId,
}

impl MetaPage {
    /// Creates a new meta page with default values.
    pub fn new(page_no: PageNo, map_size: u64) -> Self {
        Self {
            header: PageHeader::new_meta(page_no),
            magic: MDB_MAGIC,
            version: MDB_VERSION,
            address: 0,
            map_size,
            free_db: DbInfo::new(),
            main_db: DbInfo::new(),
            last_pgno: 1, // Meta pages 0 and 1 are used
            last_txnid: 0,
        }
    }

    /// Validates the meta page.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Magic number doesn't match
    /// - Version is unsupported
    /// - Page is not marked as meta
    pub fn validate(&self) -> Result<()> {
        if self.magic != MDB_MAGIC {
            return Err(Error::Invalid);
        }
        if self.version != MDB_VERSION {
            return Err(Error::VersionMismatch);
        }
        if !self.header.flags.is_meta() {
            return Err(Error::Corrupted);
        }
        Ok(())
    }

    /// Returns true if this meta page is newer than the other.
    pub fn is_newer_than(&self, other: &MetaPage) -> bool {
        self.last_txnid > other.last_txnid
    }

    /// Reads a meta page from a byte slice.
    pub fn read_from(data: &[u8]) -> Result<Self> {
        const TOTAL_SIZE: usize = 16 + META_PAGE_SIZE;
        if data.len() < TOTAL_SIZE {
            return Err(Error::Corrupted);
        }

        let header = PageHeader::read_from(&data[0..16])?;

        let magic = u32::from_le_bytes([data[16], data[17], data[18], data[19]]);
        let version = u32::from_le_bytes([data[20], data[21], data[22], data[23]]);
        let address = u64::from_le_bytes([
            data[24], data[25], data[26], data[27],
            data[28], data[29], data[30], data[31],
        ]);
        let map_size = u64::from_le_bytes([
            data[32], data[33], data[34], data[35],
            data[36], data[37], data[38], data[39],
        ]);
        let free_db = DbInfo::read_from(&data[40..88])?;
        let main_db = DbInfo::read_from(&data[88..136])?;
        let last_pgno = u64::from_le_bytes([
            data[136], data[137], data[138], data[139],
            data[140], data[141], data[142], data[143],
        ]);
        let last_txnid = u64::from_le_bytes([
            data[144], data[145], data[146], data[147],
            data[148], data[149], data[150], data[151],
        ]);

        let meta = Self {
            header,
            magic,
            version,
            address,
            map_size,
            free_db,
            main_db,
            last_pgno,
            last_txnid,
        };

        meta.validate()?;
        Ok(meta)
    }

    /// Writes the meta page to a byte slice.
    pub fn write_to(&self, data: &mut [u8]) -> Result<()> {
        const TOTAL_SIZE: usize = 16 + META_PAGE_SIZE;
        if data.len() < TOTAL_SIZE {
            return Err(Error::Corrupted);
        }

        self.header.write_to(&mut data[0..16])?;
        data[16..20].copy_from_slice(&self.magic.to_le_bytes());
        data[20..24].copy_from_slice(&self.version.to_le_bytes());
        data[24..32].copy_from_slice(&self.address.to_le_bytes());
        data[32..40].copy_from_slice(&self.map_size.to_le_bytes());
        self.free_db.write_to(&mut data[40..88])?;
        self.main_db.write_to(&mut data[88..136])?;
        data[136..144].copy_from_slice(&self.last_pgno.to_le_bytes());
        data[144..152].copy_from_slice(&self.last_txnid.to_le_bytes());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn db_info_size() {
        assert_eq!(DB_INFO_SIZE, 48);
        assert_eq!(std::mem::size_of::<DbInfo>(), DB_INFO_SIZE);
    }

    #[test]
    fn db_info_roundtrip() {
        let info = DbInfo {
            pad: 0,
            flags: 0x04, // DUPSORT
            depth: 3,
            branch_pages: 100,
            leaf_pages: 500,
            overflow_pages: 10,
            entries: 10000,
            root: 42,
        };

        let mut buf = [0u8; DB_INFO_SIZE];
        info.write_to(&mut buf).unwrap();

        let recovered = DbInfo::read_from(&buf).unwrap();
        assert_eq!(info, recovered);
    }

    #[test]
    fn meta_page_roundtrip() {
        let meta = MetaPage::new(0, 10 * 1024 * 1024);

        let mut buf = vec![0u8; 4096];
        meta.write_to(&mut buf).unwrap();

        let recovered = MetaPage::read_from(&buf).unwrap();
        assert_eq!(meta, recovered);
    }

    #[test]
    fn meta_page_validation() {
        let mut meta = MetaPage::new(0, 1024 * 1024);
        assert!(meta.validate().is_ok());

        // Bad magic
        meta.magic = 0xDEADBEEF;
        assert!(matches!(meta.validate(), Err(Error::Invalid)));

        // Fix magic, bad version
        meta.magic = MDB_MAGIC;
        meta.version = 99;
        assert!(matches!(meta.validate(), Err(Error::VersionMismatch)));
    }

    #[test]
    fn meta_page_newer() {
        let meta1 = MetaPage {
            last_txnid: 100,
            ..MetaPage::new(0, 1024 * 1024)
        };

        let meta2 = MetaPage {
            last_txnid: 101,
            ..MetaPage::new(1, 1024 * 1024)
        };

        assert!(meta2.is_newer_than(&meta1));
        assert!(!meta1.is_newer_than(&meta2));
    }
}
