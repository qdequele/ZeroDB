//! zerodb — the public, heed-shaped engine API.
//!
//! Milestone 1.2 exposes the environment lifecycle surface (SPEC 00 rows 1–12,
//! 17–22): [`EnvOpenOptions`] → [`Env`], with `map_size`/`max_dbs`/`max_readers`
//! stored, the `PREV_SNAPSHOT` / `READ_ONLY` env flags, the directory-env
//! convention (the env path is a **directory**; the data lives in a single file
//! `zerodb.dat` inside it — D-002 / SPEC 02 §8), [`Env::info`], [`Env::path`],
//! [`Env::real_disk_size`], [`Env::try_clone_inner_file`], and deferred close via
//! [`Env::prepare_for_closing`] / [`EnvClosingEvent`].
//!
//! Transactions, databases, and the read/write paths are later milestones and
//! are not exposed here yet. The error taxonomy mirrors `heed::Error`
//! ([`Error`] / [`MdbError`]); the `heed-zerodb` adapter (M1.13) maps it 1:1.

use std::path::Path;

pub use zerodb_core::env::{Env, EnvClosingEvent, EnvInfo};
pub use zerodb_core::error::{Error, MdbError, Result};

/// The name of the single data file inside an env directory (D-002, SPEC 02 §8).
pub const DATA_FILE_NAME: &str = "zerodb.dat";

/// Default map size when the caller does not set one (1 MiB). LMDB/heed require
/// a `map_size`; every production consumer sets it (SPEC 00 row 3), so this
/// default only matters for tests.
const DEFAULT_MAP_SIZE: u64 = 1 << 20;

/// Default DB page size when not overridden (SPEC 02 §0; Phase 1 exposes no heed
/// selector, SPEC 01 §S4).
const DEFAULT_PAGE_SIZE: u32 = 4096;

/// Environment open flags (SPEC 01 Table 1). A hand-rolled bitset — no
/// `bitflags` dependency (not on the CLAUDE.md allowlist).
///
/// Phase-1 in-scope flags: [`EnvFlags::PREV_SNAPSHOT`] (MUST — milli's
/// `Index::rollback`, opens the older meta) and [`EnvFlags::READ_ONLY`]
/// (SHOULD — accepted and stored; write-rejection is deferred to the txn
/// milestones, M1.10). Durability and writemap flags (`NO_SYNC`, `WRITE_MAP`, …)
/// land with the write path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EnvFlags(u32);

impl EnvFlags {
    /// No flags.
    pub const EMPTY: EnvFlags = EnvFlags(0);
    /// `MDB_PREVSNAPSHOT` — open on the older of the two meta pages (SPEC 01
    /// Table 1, §S5).
    pub const PREV_SNAPSHOT: EnvFlags = EnvFlags(0x0200_0000);
    /// `MDB_RDONLY` — env-level read-only (SPEC 01 Table 1). Accepted and stored
    /// in Phase 1; the write-txn rejection is M1.10.
    pub const READ_ONLY: EnvFlags = EnvFlags(0x0002_0000);

    /// Whether all bits in `other` are set.
    #[must_use]
    pub fn contains(self, other: EnvFlags) -> bool {
        self.0 & other.0 == other.0
    }

    /// The raw bit value (matches the LMDB constants).
    #[must_use]
    pub fn bits(self) -> u32 {
        self.0
    }
}

impl std::ops::BitOr for EnvFlags {
    type Output = EnvFlags;
    fn bitor(self, rhs: EnvFlags) -> EnvFlags {
        EnvFlags(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for EnvFlags {
    fn bitor_assign(&mut self, rhs: EnvFlags) {
        self.0 |= rhs.0;
    }
}

/// Builder for opening an [`Env`] (SPEC 00 rows 1–12), shaped like heed's
/// `EnvOpenOptions`.
#[derive(Debug, Clone)]
pub struct EnvOpenOptions {
    map_size: Option<u64>,
    max_dbs: u32,
    max_readers: u32,
    page_size: u32,
    flags: EnvFlags,
}

impl Default for EnvOpenOptions {
    fn default() -> Self {
        EnvOpenOptions::new()
    }
}

impl EnvOpenOptions {
    /// A fresh builder with defaults (SPEC 00 row 1).
    #[must_use]
    pub fn new() -> EnvOpenOptions {
        EnvOpenOptions {
            map_size: None,
            max_dbs: 0,
            // LMDB's default max readers is 126; consumers override it (SPEC 00
            // row 5). Stored now; the reader table is sized in M1.8.
            max_readers: 126,
            page_size: DEFAULT_PAGE_SIZE,
            flags: EnvFlags::EMPTY,
        }
    }

    /// Set the map size (SPEC 00 row 3). Stored; the mmap ceiling and
    /// `MdbError::MapFull` boundary use it.
    pub fn map_size(&mut self, size: usize) -> &mut EnvOpenOptions {
        self.map_size = Some(size as u64);
        self
    }

    /// Set the named-DB catalog capacity (SPEC 00 row 4). Stored; consumed when
    /// named DBs land (M1.6).
    pub fn max_dbs(&mut self, n: u32) -> &mut EnvOpenOptions {
        self.max_dbs = n;
        self
    }

    /// Set the reader-table size (SPEC 00 row 5). Stored; consumed in M1.8.
    pub fn max_readers(&mut self, n: u32) -> &mut EnvOpenOptions {
        self.max_readers = n;
        self
    }

    /// Set the DB page size (internal option; SPEC 02 §0). Only used when
    /// **creating** a new env — for an existing env the persisted page size
    /// wins (SPEC 02 §3.2). Must be a power of two in `[4096, 65536]`.
    pub fn page_size(&mut self, size: u32) -> &mut EnvOpenOptions {
        self.page_size = size;
        self
    }

    /// Set the env flags (SPEC 00 row 6). Only [`EnvFlags::PREV_SNAPSHOT`] and
    /// [`EnvFlags::READ_ONLY`] are meaningful in M1.2.
    pub fn flags(&mut self, flags: EnvFlags) -> &mut EnvOpenOptions {
        self.flags = flags;
        self
    }

    /// The configured max DBs.
    #[must_use]
    pub fn get_max_dbs(&self) -> u32 {
        self.max_dbs
    }

    /// The configured max readers.
    #[must_use]
    pub fn get_max_readers(&self) -> u32 {
        self.max_readers
    }

    /// Open (or create) the environment at `path` (SPEC 00 row 7).
    ///
    /// `path` is a **directory** (the directory-env convention, SPEC 00 row 7);
    /// the data lives in `path/`[`DATA_FILE_NAME`]. The directory must already
    /// exist (LMDB parity). If the data file is absent or empty, a fresh empty
    /// env is created there (SPEC 02 §3.4).
    ///
    /// Unlike heed, this is a safe function: no `EnvFlags` value reachable here
    /// enables cross-process behavior (D-001).
    ///
    /// # Errors
    ///
    /// - [`Error::Io`] if the directory does not exist, on any file/mmap I/O
    ///   error, or if a `READ_ONLY` open finds no existing store.
    /// - [`Error::Mdb`]`(`[`MdbError::Invalid`]`)` if the file is not a valid
    ///   zerodb env (bad magic/version/page-size, both meta slots torn, or the
    ///   one-valid + `PREV_SNAPSHOT` combination — SPEC 06 REC-2†).
    /// - [`Error::EnvAlreadyOpened`] if this process already holds a live handle
    ///   for the same canonical path (SPEC 04 TXN-51).
    pub fn open(&self, path: impl AsRef<Path>) -> Result<Env> {
        let dir = path.as_ref();
        if self.page_size < 4096 || self.page_size > 65536 || !self.page_size.is_power_of_two() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "page_size must be a power of two in [4096, 65536]",
            )));
        }
        // Canonicalize the directory so the registry keys one env per real path
        // (SPEC 04 TXN-51). The directory must exist (LMDB parity).
        let canonical_dir = dir.canonicalize().map_err(Error::Io)?;
        let data_path = canonical_dir.join(DATA_FILE_NAME);

        let read_only = self.flags.contains(EnvFlags::READ_ONLY);
        let prev_snapshot = self.flags.contains(EnvFlags::PREV_SNAPSHOT);

        let opened = zerodb_io::open_or_create(
            &data_path,
            self.page_size,
            self.map_size,
            DEFAULT_MAP_SIZE,
            read_only,
        )?;

        zerodb_core::env::open_with_backing(
            canonical_dir,
            opened.backing,
            opened.page_size,
            opened.map_size,
            prev_snapshot,
        )
    }
}
