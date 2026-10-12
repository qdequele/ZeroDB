//! ZeroDB — a pure-Rust embedded key-value store with LMDB's architecture.
//!
//! A transactional, memory-mapped KV engine: a single writer, any number of
//! lock-free MVCC readers over the memory map, copy-on-write B+trees, and two
//! durable CRC32C meta pages — LMDB's design, reimplemented in safe Rust with no
//! C dependency. The API is heed-shaped, so the same code runs here or on real
//! LMDB through the [`heed`](https://github.com/meilisearch/heed) adapter.
//!
//! An environment ([`Env`]) is a **directory** holding a single data file
//! (named [`DATA_FILE_NAME`] natively, [`HEED_DATA_FILE_NAME`] through the
//! `heed-zerodb` adapter; ADR-0010). It holds a main database plus any number of
//! named ones. Writes go through one [`RwTxn`] at a time; reads ([`RoTxn`]) are
//! lock-free snapshots that never block the writer. Keys and values are byte
//! slices, and the error taxonomy mirrors heed's ([`Error`] / [`MdbError`]).
//!
//! ```rust,no_run
//! use zerodb::EnvOpenOptions;
//!
//! # fn main() -> zerodb::Result<()> {
//! // An environment is a directory holding one data file.
//! let env = EnvOpenOptions::new()
//!     .map_size(10 * 1024 * 1024) // 10 MiB map ceiling
//!     .max_dbs(4)
//!     .open("books.db")?;
//!
//! // All writes go through a single write transaction.
//! let mut wtxn = env.write_txn()?;
//! let db = env.create_database(&mut wtxn, Some(&b"books"[..]))?;
//! db.put(&mut wtxn, b"1984", b"Orwell")?;
//! wtxn.commit()?;
//!
//! // Reads are lock-free MVCC snapshots — any number can run concurrently.
//! let rtxn = env.read_txn()?;
//! assert_eq!(db.get(&rtxn, b"1984")?, Some(&b"Orwell"[..]));
//! # Ok(())
//! # }
//! ```

#![deny(missing_docs)]
use std::ffi::{OsStr, OsString};
use std::path::{Component, Path};

mod copy;
pub use copy::{CompactionOption, CopyProgress, CopyToFile};

pub use zerodb_core::builder::{
    build_multi_db_image, EnvStream, NamedDbData, PageSink, StreamBuildError, DEFAULT_FILL_PERMILLE,
};
pub use zerodb_core::check;
pub use zerodb_core::cmp::{Comparator, ComparatorError, DefaultComparator, FnComparator, KeyCmp};
pub use zerodb_core::env::{
    CommitHook, DurabilityFlags, Env, EnvClosingEvent, EnvInfo, EnvStat, HookPoint, ReaderEntry,
    Snapshot,
};
pub use zerodb_core::error::{Error, MdbError, Result};
pub use zerodb_core::nested::NestedRoTxn;
pub use zerodb_core::page::geometry::MAX_MAP_SIZE;
pub use zerodb_core::page::{FileTrust, FIRST_DATA_PGNO, MAX_KEY_SIZE};
pub use zerodb_core::rotxn::{
    collect_entries_flagged, for_each_entry_flagged, free_page_count, named_databases, Database,
    DatabaseStat, RoRange, RoTxn, TxnRead,
};
pub use zerodb_core::rwtxn::{PutFlags, RwCursor, RwTxn};

/// The **native default** name of the single data file inside an env directory
/// (ZeroDB's own file format, SPEC 02 §8).
///
/// This is only a default: the opener chooses the name via
/// [`EnvOpenOptions::data_file_name`]. Envs opened through the `heed-zerodb`
/// adapter use [`HEED_DATA_FILE_NAME`] instead, so that heed consumers that
/// hardcode LMDB's `data.mdb` (Meilisearch does, in production paths) see the
/// file they expect (ADR-0010).
pub const DATA_FILE_NAME: &str = "zerodb.dat";

/// The data-file name the `heed-zerodb` adapter uses, matching LMDB's
/// directory-env contract (ADR-0010).
///
/// Exposed here so the adapter, `zerodb-tools`' two-name probe, and tests all
/// agree on one constant. A `lock.mdb` next to it (the name
/// [`derived_lock_file_name`] gives it) appears only when
/// [`EnvOpenOptions::lock_file`] is turned on (ADR-0023); it is off by
/// default, so a default adapter-opened env dir still contains only this one
/// file.
pub const HEED_DATA_FILE_NAME: &str = "data.mdb";

/// The lock-file name of a multi-process environment whose data file has the
/// native default name [`DATA_FILE_NAME`] (ADR-0023). Other data-file names
/// derive other lock names; see [`derived_lock_file_name`].
pub const LOCK_FILE_NAME: &str = "zerodb.lock";

/// The lock-file name derived from a data-file name (ADR-0023 amendment 16):
/// [`DATA_FILE_NAME`] (`zerodb.dat`) gives [`LOCK_FILE_NAME`]
/// (`zerodb.lock`), [`HEED_DATA_FILE_NAME`] (`data.mdb`) gives LMDB's
/// `lock.mdb`, and any other name `<data_file_name>-lock` (LMDB's
/// `MDB_NOSUBDIR` convention). Two environments with different data-file
/// names in one directory therefore never share a lock file.
///
/// [`EnvOpenOptions::lock_file_name`] overrides the derived name.
#[must_use]
pub fn derived_lock_file_name(data_file_name: &OsStr) -> OsString {
    if data_file_name == DATA_FILE_NAME {
        OsString::from(LOCK_FILE_NAME)
    } else if data_file_name == HEED_DATA_FILE_NAME {
        OsString::from("lock.mdb")
    } else {
        let mut name = data_file_name.to_os_string();
        name.push("-lock");
        name
    }
}

/// Whether `name` is a single, non-empty path component — i.e. it names a file
/// directly inside the env directory and cannot escape it (ADR-0010).
///
/// Rejects a name containing *any* path separator, per the ADR, rather than
/// only one that survives normalization: `"a/"` normalizes to the single
/// component `a`, but accepting it would mean the file on disk is not the name
/// the caller passed. An integration knob whose value silently differs from
/// what lands on disk is a trap, so the check is on the raw bytes.
fn is_single_component(name: &OsStr) -> bool {
    if name.is_empty() {
        return false;
    }
    if name
        .to_string_lossy()
        .contains(['/', std::path::MAIN_SEPARATOR])
    {
        return false;
    }
    let mut comps = Path::new(name).components();
    matches!(comps.next(), Some(Component::Normal(_))) && comps.next().is_none()
}

/// Default map size when the caller does not set one (1 MiB). LMDB/heed require
/// a `map_size`; every production consumer sets it (SPEC 00 row 3), so this
/// default only matters for tests.
const DEFAULT_MAP_SIZE: u64 = 1 << 20;

/// Default DB page size when not overridden (SPEC 02 §0). Selectable via
/// [`EnvOpenOptions::page_size`].
pub const DEFAULT_PAGE_SIZE: u32 = 4096;

/// The smallest selectable DB page size (SPEC 02 §0).
pub const MIN_PAGE_SIZE: u32 = 4096;

/// The largest selectable DB page size (SPEC 02 §0).
pub const MAX_PAGE_SIZE: u32 = 65536;

/// Environment open flags (SPEC 01 Table 1), as a hand-rolled bitset.
///
/// LMDB-parity flags (all MUST/SHOULD per SPEC 01 Table 1):
/// [`EnvFlags::PREV_SNAPSHOT`] (milli's `Index::rollback`), [`EnvFlags::READ_ONLY`]
/// (write-txn → `EACCES`), [`EnvFlags::WRITE_MAP`] (writable-mmap write
/// path), and the durability flags [`EnvFlags::NO_SYNC`] /
/// [`EnvFlags::NO_META_SYNC`] / [`EnvFlags::MAP_ASYNC`] (SPEC 01 §S6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EnvFlags(u32);

impl EnvFlags {
    /// No flags.
    pub const EMPTY: EnvFlags = EnvFlags(0);
    /// `MDB_NOSYNC` — skip both data and meta fsync on commit (SPEC 01 Table 1,
    /// §S6). Durability restored by [`Env::force_sync`].
    pub const NO_SYNC: EnvFlags = EnvFlags(0x0001_0000);
    /// `MDB_RDONLY` — env-level read-only (SPEC 01 Table 1). A write txn on such
    /// an env returns `EACCES`.
    pub const READ_ONLY: EnvFlags = EnvFlags(0x0002_0000);
    /// `MDB_NOMETASYNC` — fsync data but skip the meta fsync this commit
    /// (SPEC 01 Table 1, §S6, REC-10).
    pub const NO_META_SYNC: EnvFlags = EnvFlags(0x0004_0000);
    /// `MDB_WRITEMAP` — writes go through a writable mmap instead of
    /// heap-buffer + `pwrite` (SPEC 01 Table 1, §S7, SPEC 04 §6.4).
    pub const WRITE_MAP: EnvFlags = EnvFlags(0x0008_0000);
    /// `MDB_MAPASYNC` — with `WRITE_MAP`, use `msync(MS_ASYNC)` for the commit
    /// flushes (SPEC 01 Table 1, §S6). No effect without `WRITE_MAP`.
    pub const MAP_ASYNC: EnvFlags = EnvFlags(0x0010_0000);
    /// `MDB_NORDAHEAD` — advise the kernel that the map is read at random
    /// (`madvise(MADV_RANDOM)`), turning off readahead around page faults,
    /// as LMDB does (SPEC 01 Table 1). Matters when the data does not fit in
    /// memory and reads are random; it changes no result.
    pub const NO_READ_AHEAD: EnvFlags = EnvFlags(0x0080_0000);
    /// `MDB_PREVSNAPSHOT` — open on the older of the two meta pages (SPEC 01
    /// Table 1, §S5).
    pub const PREV_SNAPSHOT: EnvFlags = EnvFlags(0x0200_0000);

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
    data_file_name: OsString,
    file_trust: FileTrust,
    sequential_writes: bool,
    /// Dirty limit in bytes (ADR-0017); `None` = LMDB's default.
    max_dirty_bytes: Option<usize>,
    /// Whether to coordinate through a cross-process lock file (ADR-0023).
    lock_file: bool,
    /// The lock file's explicit name, when `lock_file` is set; `None` derives
    /// it from the data-file name ([`derived_lock_file_name`]).
    lock_file_name: Option<OsString>,
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
            // row 5). The reader table is sized from it at open.
            max_readers: 126,
            page_size: DEFAULT_PAGE_SIZE,
            flags: EnvFlags::EMPTY,
            data_file_name: OsString::from(DATA_FILE_NAME),
            file_trust: FileTrust::VALIDATE,
            sequential_writes: false,
            max_dirty_bytes: None,
            lock_file: false,
            lock_file_name: None,
        }
    }

    /// Set the map size (SPEC 00 row 3). Stored; the mmap ceiling and
    /// `MdbError::MapFull` boundary use it.
    pub fn map_size(&mut self, size: usize) -> &mut EnvOpenOptions {
        self.map_size = Some(size as u64);
        self
    }

    /// Set the named-DB catalog capacity (SPEC 00 row 4): the number of distinct
    /// named DBs the env can open.
    pub fn max_dbs(&mut self, n: u32) -> &mut EnvOpenOptions {
        self.max_dbs = n;
        self
    }

    /// Set the reader-table size (SPEC 00 row 5; SPEC 04 TXN-14, default 126
    /// = LMDB `DEFAULT_READERS`). The table is allocated once at open and
    /// never resized; when it is exhausted, `read_txn` fails with
    /// [`MdbError::ReadersFull`] (TXN-16).
    ///
    /// With [`EnvOpenOptions::lock_file`] on, the process that opens the
    /// environment first sizes the shared table; a process that joins it
    /// adopts that size whatever it asks for, as LMDB does (ADR-0023 §1 as
    /// amended). [`Env::info`] reports the size in use.
    pub fn max_readers(&mut self, n: u32) -> &mut EnvOpenOptions {
        self.max_readers = n;
        self
    }

    /// Select the DB page size (SPEC 02 §0).
    ///
    /// This is a **ZeroDB extension**: LMDB 0.9 derives the page size from the
    /// OS and offers no selector, so there is no heed API to mirror and no
    /// cross-engine differential to run. The page size is a runtime value
    /// persisted in the meta page (SPEC 02 §0/§3.2).
    ///
    /// Semantics:
    ///
    /// - **Creation-only.** The value is used only when [`EnvOpenOptions::open`]
    ///   *creates* a new store. Opening an **existing** env ignores it entirely
    ///   and adopts the persisted page size from the meta page (SPEC 02 §3.2) —
    ///   opening a 64 K store with `page_size(4096)` succeeds and yields a 64 K
    ///   env. Read the effective value back with [`Env::page_size`] (or
    ///   [`EnvStat::page_size`]); it is never silently wrong, just not what was
    ///   requested. There is deliberately no "wrong expectation" error: the
    ///   persisted geometry is authoritative, exactly as for `map_size`.
    /// - **Independent of the OS page size.** ARM distros commonly run 64 K OS
    ///   pages; the DB page size is chosen here and does not track them.
    /// - **Validated at `open`**, not here, so the builder stays chainable —
    ///   see [`EnvOpenOptions::open`] for the error.
    ///
    /// # Panics
    ///
    /// Never. An invalid `size` is reported by [`EnvOpenOptions::open`].
    pub fn page_size(&mut self, size: u32) -> &mut EnvOpenOptions {
        self.page_size = size;
        self
    }

    /// The configured page size. For a value that reflects an
    /// *existing* store, use [`Env::page_size`] after opening.
    #[must_use]
    pub fn get_page_size(&self) -> u32 {
        self.page_size
    }

    /// Set the env flags (SPEC 00 row 6). All LMDB-parity flags are honored:
    /// `PREV_SNAPSHOT`, `READ_ONLY`, `WRITE_MAP`, `NO_SYNC`, `NO_META_SYNC`,
    /// `MAP_ASYNC` (SPEC 01 Table 1).
    pub fn flags(&mut self, flags: EnvFlags) -> &mut EnvOpenOptions {
        self.flags = flags;
        self
    }

    /// Choose the name of the data file inside the env directory
    /// (**ADR-0010**). Default: [`DATA_FILE_NAME`] (`zerodb.dat`).
    ///
    /// This is an **integration knob**, not a format knob: the name lives
    /// outside the on-disk format (`format_version` is unaffected) and is
    /// resolved once, at [`EnvOpenOptions::open`], before any write — so no
    /// SPEC 06 crash-safety invariant depends on it.
    ///
    /// It exists because heed/LMDB's directory-env contract names the map
    /// `data.mdb`, and that name leaks into consumer code: Meilisearch
    /// hardcodes it in production compaction and snapshot paths. The
    /// `heed-zerodb` adapter therefore sets [`HEED_DATA_FILE_NAME`]
    /// unconditionally, re-imposing the heed contract at the heed boundary
    /// (the same pattern as the adapter's `map_size`, `max_readers(0)` and
    /// NUL-in-DB-name checks) while the native engine keeps its honest
    /// `ZDB1`-format name.
    ///
    /// **There is no fallback probing.** `open` uses exactly the configured
    /// name: pointing this at `data.mdb` in a directory that holds only
    /// `zerodb.dat` creates a *fresh, empty* env at `data.mdb`, it does not
    /// adopt the other file. Determinism matters more than convenience here —
    /// the adapter must always read the same name it writes. (`zerodb-tools`,
    /// which only reads, does probe both names; both present is a hard error
    /// there.)
    ///
    /// # Errors
    ///
    /// Validated at [`EnvOpenOptions::open`], not here, so the builder stays
    /// chainable: an empty name, or one that is not a single path component
    /// (contains a separator, or is `.`/`..`), is `Io(InvalidInput)`.
    pub fn data_file_name(&mut self, name: impl Into<OsString>) -> &mut EnvOpenOptions {
        self.data_file_name = name.into();
        self
    }

    /// The configured data-file name (ADR-0010).
    #[must_use]
    pub fn get_data_file_name(&self) -> &OsStr {
        &self.data_file_name
    }

    /// Choose the page-validation policy (**ADR-0014**; ZeroDB extension,
    /// LMDB has no validating mode). Default: [`FileTrust::VALIDATE`], under
    /// which a corrupt or hostile file yields a typed error.
    ///
    /// Passing the value of the `unsafe` [`FileTrust::trust_contents`] makes
    /// the env read page cells without validating them, as LMDB does; its
    /// `# Safety` section is the contract. The setter itself is safe because
    /// only that `unsafe` constructor can produce the trusting value.
    pub fn file_trust(&mut self, policy: FileTrust) -> &mut EnvOpenOptions {
        self.file_trust = policy;
        self
    }

    /// The configured page-validation policy (ADR-0014).
    #[must_use]
    pub fn get_file_trust(&self) -> FileTrust {
        self.file_trust
    }

    /// Turn on the sequential-writes fast path for every database of the env
    /// by default (**ADR-0015**; ZeroDB extension, LMDB has no counterpart).
    /// Default: off.
    ///
    /// With it on, a write txn remembers the path to each tree's rightmost
    /// leaf and appends there without descending from the root when a key
    /// sorts after every key already in that leaf. Measured on x86-64:
    /// ascending and APPEND loads 14–24 % faster, random-key writes 3–5 %
    /// slower. Results are identical either way. Override it per database
    /// with [`Env::set_sequential_writes`].
    pub fn sequential_writes(&mut self, on: bool) -> &mut EnvOpenOptions {
        self.sequential_writes = on;
        self
    }

    /// The configured sequential-writes default (ADR-0015).
    #[must_use]
    pub fn get_sequential_writes(&self) -> bool {
        self.sequential_writes
    }

    /// Bound a write txn's dirty memory to about `bytes` (**ADR-0017**;
    /// ZeroDB extension — LMDB's limit is a compile-time constant). Default:
    /// LMDB's limit, 131,072 dirty pages (512 MiB at 4 KiB pages).
    ///
    /// Past the limit, a write txn writes its highest-numbered dirty pages to
    /// the file at the start of its next mutating call and drops them from
    /// memory, as LMDB's `mdb_page_spill` does; pages it touches again are
    /// read back. The limit is `max(bytes / page_size, 128)` pages. Results
    /// and committed files never depend on it; a lower limit trades memory
    /// for extra writes in very large txns.
    pub fn max_dirty_bytes(&mut self, bytes: usize) -> &mut EnvOpenOptions {
        self.max_dirty_bytes = Some(bytes);
        self
    }

    /// The configured dirty limit in bytes (ADR-0017); `None` = LMDB's
    /// default.
    #[must_use]
    pub fn get_max_dirty_bytes(&self) -> Option<usize> {
        self.max_dirty_bytes
    }

    /// Let other processes open this environment at the same time, through a
    /// shared lock file next to the data file (ADR-0023). Off by default in
    /// this release; the heed adapter turns it on unless heed's `NO_LOCK` is
    /// set (ADR-0023 amendment 17).
    pub fn lock_file(&mut self, on: bool) -> &mut EnvOpenOptions {
        self.lock_file = on;
        self
    }

    /// Whether the lock file is enabled (ADR-0023).
    #[must_use]
    pub fn get_lock_file(&self) -> bool {
        self.lock_file
    }

    /// Override the lock file's name inside the environment directory: a
    /// single path component that differs from the data-file name, and that
    /// must not name the data file under another spelling (a hard link, or a
    /// case variant on a case-insensitive filesystem), which the open refuses.
    /// By default the name is derived from the data-file name
    /// ([`derived_lock_file_name`]: `zerodb.lock`, `lock.mdb` for the heed
    /// adapter's `data.mdb`, otherwise `<data_file_name>-lock`).
    pub fn lock_file_name(&mut self, name: impl Into<OsString>) -> &mut EnvOpenOptions {
        self.lock_file_name = Some(name.into());
        self
    }

    /// The lock-file name an open would use (ADR-0023): the explicit
    /// [`EnvOpenOptions::lock_file_name`] if set, otherwise the name
    /// [`derived_lock_file_name`] gives the current data-file name.
    #[must_use]
    pub fn get_lock_file_name(&self) -> OsString {
        match &self.lock_file_name {
            Some(name) => name.clone(),
            None => derived_lock_file_name(&self.data_file_name),
        }
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

    /// ADR-0010: the data-file name must be a single path component, so it
    /// can only ever name a file *inside* the env directory.
    fn check_data_file_name(&self) -> Result<()> {
        if !is_single_component(&self.data_file_name) {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "data_file_name ({:?}) must be a single, non-empty path component",
                    self.data_file_name
                ),
            )));
        }
        Ok(())
    }

    /// ADR-0023: the lock-file name must be a single path component, and
    /// must not be the data file. The lock-file initializer truncates its
    /// file, so sharing the name would destroy the data; it is rejected before
    /// either file is opened. Other names of the data file (hard links, case
    /// variants) are caught by the same-file guard in `LockFile::open`
    /// (amendment 16).
    fn check_lock_file_name(&self, lock_file_name: &OsStr) -> Result<()> {
        if !is_single_component(lock_file_name) {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "lock_file_name ({lock_file_name:?}) must be a single, non-empty path component"
                ),
            )));
        }
        if lock_file_name == self.data_file_name {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("lock_file_name ({lock_file_name:?}) must differ from data_file_name"),
            )));
        }
        Ok(())
    }

    /// Open (or create) the environment at `path` (SPEC 00 row 7).
    ///
    /// `path` is a **directory** (the directory-env convention, SPEC 00 row 7);
    /// the data lives in `path/`[`EnvOpenOptions::get_data_file_name`]
    /// (default [`DATA_FILE_NAME`], ADR-0010). The directory must already
    /// exist (LMDB parity). If the data file is absent or empty, a fresh empty
    /// env is created there (SPEC 02 §3.4).
    ///
    /// Unlike heed, this is a safe function: no `EnvFlags` value reachable here
    /// enables cross-process behavior; cross-process access is instead opt-in
    /// through [`EnvOpenOptions::lock_file`] (ADR-0023). Without it, ZeroDB is
    /// single-process (see docs/DIVERGENCES.md).
    ///
    /// # Errors
    ///
    /// - [`Error::Io`] (`InvalidInput`) if [`EnvOpenOptions::page_size`] is not
    ///   a power of two in `[`[`MIN_PAGE_SIZE`]`, `[`MAX_PAGE_SIZE`]`]`.
    ///   `Io(InvalidInput)` is the taxonomy ZeroDB uses for open-time
    ///   argument rejection (cf. the `max_readers(0)` and non-page-multiple
    ///   `map_size` checks at the heed boundary); LMDB has no error for this
    ///   because it has no such option.
    /// - [`Error::Io`] (`InvalidInput`) if
    ///   [`EnvOpenOptions::data_file_name`] is empty or is not a single path
    ///   component (ADR-0010); likewise for
    ///   [`EnvOpenOptions::lock_file_name`] when
    ///   [`EnvOpenOptions::lock_file`] is on (ADR-0023), or when that name
    ///   equals the data-file name.
    /// - [`Error::Io`] (`InvalidInput`), with [`EnvOpenOptions::lock_file`]
    ///   on, if the lock file is the data file under another name (a hard
    ///   link, or a case variant on a case-insensitive filesystem), or if it
    ///   is a non-empty file that is neither a ZeroDB lock file nor the
    ///   all-zero remains of one, such as another environment's data file or
    ///   a stale LMDB `lock.mdb` (ADR-0023 amendment 16; SPEC 02 §8 gives the
    ///   exact rule). Neither file is modified.
    /// - [`Error::Io`] if the directory does not exist, on any file/mmap I/O
    ///   error, or if a `READ_ONLY` open finds no existing store.
    /// - [`Error::Io`] (`EROFS`), with [`EnvOpenOptions::lock_file`] on, if
    ///   the lock file cannot be created or opened for writing on a read-only
    ///   filesystem and the open is not [`EnvFlags::READ_ONLY`]. A `READ_ONLY`
    ///   open there proceeds without a lock file, as LMDB does
    ///   ([`Env::has_lock_file`] is then `false`; ADR-0023 amendment 17).
    ///   Any other lock-file error, such as `EACCES` in a directory this
    ///   process cannot write, fails the open, also when `READ_ONLY`.
    /// - [`Error::Mdb`]`(`[`MdbError::Invalid`]`)` if the file is not a valid
    ///   zerodb env (bad magic/version/page-size, both meta slots torn, or the
    ///   one-valid + `PREV_SNAPSHOT` combination — SPEC 06 REC-2†).
    /// - [`Error::EnvAlreadyOpened`] if this process already holds a live handle
    ///   for the same canonical path (SPEC 04 TXN-51), or, with
    ///   [`EnvOpenOptions::lock_file`] on, already has that lock file open
    ///   (ADR-0023).
    /// - [`Error::Io`] (`EAGAIN`) with [`EnvFlags::PREV_SNAPSHOT`] and
    ///   [`EnvOpenOptions::lock_file`] both on, if another process already
    ///   has the environment open: `PREV_SNAPSHOT` requires winning the
    ///   lock file's first-opener election, matching LMDB as observed
    ///   (ADR-0023 §5, `OBS_PREV_SNAPSHOT_JOINER`).
    pub fn open(&self, path: impl AsRef<Path>) -> Result<Env> {
        let dir = path.as_ref();
        // Validate the page-size selector here rather than in the setter
        // so the builder stays chainable.
        if self.page_size < MIN_PAGE_SIZE
            || self.page_size > MAX_PAGE_SIZE
            || !self.page_size.is_power_of_two()
        {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "page_size ({}) must be a power of two in [{MIN_PAGE_SIZE}, {MAX_PAGE_SIZE}]",
                    self.page_size
                ),
            )));
        }
        self.check_data_file_name()?;
        // ADR-0023 amendment 16: the explicit lock-file name, or the one
        // derived from the data-file name.
        let lock_file_name = self.get_lock_file_name();
        if self.lock_file {
            self.check_lock_file_name(&lock_file_name)?;
        }
        // Reject out-of-range limits before any file exists: the lock file is
        // sized from `max_readers`, and its initializer writes every slot.
        zerodb_core::env::check_open_limits(self.max_readers, self.max_dbs)?;
        // Canonicalize the directory so the registry keys one env per real path
        // (SPEC 04 TXN-51). The directory must exist (LMDB parity).
        let canonical_dir = dir.canonicalize().map_err(Error::Io)?;
        let data_path = canonical_dir.join(&self.data_file_name);

        // ADR-0023 §1: the lock file is opened (the election) *before* the
        // data file, so a joining process waits for the initializer rather
        // than racing it to create the data file.
        let read_only = self.flags.contains(EnvFlags::READ_ONLY);
        let lock = if self.lock_file {
            let path = canonical_dir.join(&lock_file_name);
            match zerodb_io::lockfile::LockFile::open(&path, &data_path, self.max_readers) {
                Ok(l) => Some(l),
                // ADR-0023 amendment 17 (`OBS_RDONLY_LOCK_READONLY_FS = 0`): a
                // read-only env on read-only media runs without a lock file,
                // as LMDB does. A writable env fails with the `EROFS`.
                Err(zerodb_io::lockfile::LockOpenError::ReadOnlyFs) if read_only => None,
                Err(zerodb_io::lockfile::LockOpenError::ReadOnlyFs) => {
                    return Err(Error::Io(std::io::Error::from_raw_os_error(libc::EROFS)))
                }
                Err(zerodb_io::lockfile::LockOpenError::AlreadyOpen) => {
                    return Err(Error::EnvAlreadyOpened)
                }
                Err(zerodb_io::lockfile::LockOpenError::Mismatch(m)) => {
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        m,
                    )))
                }
                Err(zerodb_io::lockfile::LockOpenError::Io(e)) => return Err(Error::Io(e)),
            }
        } else {
            None
        };

        let prev_snapshot = self.flags.contains(EnvFlags::PREV_SNAPSHOT);
        // ADR-0023 §5: `PREV_SNAPSHOT` requires winning the first-opener
        // election (no other process alive), matching LMDB as observed
        // (`OBS_PREV_SNAPSHOT_JOINER = EAGAIN`). A joiner's lock file is
        // released before the error returns; the data file is never opened.
        if prev_snapshot {
            if let Some(l) = &lock {
                if !zerodb_core::shared::SharedRegion::is_initializer(l.as_ref()) {
                    drop(lock);
                    return Err(Error::Io(std::io::Error::from_raw_os_error(libc::EAGAIN)));
                }
            }
        }
        // `WRITE_MAP` is meaningful only on a writable env; the backing maps
        // read-only under `READ_ONLY` regardless (LMDB parity).
        let durability = DurabilityFlags {
            read_only,
            no_sync: self.flags.contains(EnvFlags::NO_SYNC),
            no_meta_sync: self.flags.contains(EnvFlags::NO_META_SYNC),
            map_async: self.flags.contains(EnvFlags::MAP_ASYNC),
            write_map: self.flags.contains(EnvFlags::WRITE_MAP) && !read_only,
        };

        let opened = zerodb_io::open_or_create_with_advice(
            &data_path,
            self.page_size,
            self.map_size,
            DEFAULT_MAP_SIZE,
            read_only,
            durability.write_map,
            self.flags.contains(EnvFlags::NO_READ_AHEAD),
            // ADR-0023 amendment 15, decision 4: only the sole process may
            // grow a WRITE_MAP data file at open. A joiner's write txns grow
            // it under both writer locks.
            lock.as_ref()
                .is_none_or(|l| zerodb_core::shared::SharedRegion::is_initializer(l.as_ref())),
            prev_snapshot,
        )?;

        if let Some(l) = &lock {
            l.bind_page_size(opened.page_size).map_err(Error::Io)?;
        }

        let env = zerodb_core::env::open_with_backing_shared(
            canonical_dir,
            opened.backing,
            opened.page_size,
            prev_snapshot,
            self.max_dbs,
            self.max_readers,
            durability,
            self.file_trust,
            self.sequential_writes,
            self.max_dirty_bytes
                .map(|b| zerodb_core::env::dirty_limit_pages(Some(b), opened.page_size)),
            lock.clone()
                .map(|l| l as std::sync::Arc<dyn zerodb_core::shared::SharedRegion>),
        )?;

        if let Some(l) = &lock {
            // Let joiners in only after T is seeded (ADR-0023 §1).
            l.finish_open().map_err(Error::Io)?;
        }
        Ok(env)
    }
}

/// Holds an environment's lock file as an unfinished initializer: every other
/// opener waits in the first-opener election until this guard drops (ADR-0023
/// amendment 18).
///
/// `zerodb-tools check` takes it before walking the raw data file of an
/// environment that does not open, so no process opens the environment, and
/// so writes the file, during the walk. It is the process's only descriptor
/// of the lock file: the in-process registry refuses a second open of it,
/// including an [`EnvOpenOptions::open`] of the same environment, while the
/// guard lives.
pub struct ExclusiveLock {
    _lock: std::sync::Arc<zerodb_io::lockfile::LockFile>,
}

impl std::fmt::Debug for ExclusiveLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExclusiveLock").finish_non_exhaustive()
    }
}

impl ExclusiveLock {
    /// Take the lock file of the environment in directory `dir`, opened as
    /// `opts` would open it: the data file is
    /// [`EnvOpenOptions::get_data_file_name`] and the lock file
    /// [`EnvOpenOptions::get_lock_file_name`] (whether or not
    /// [`EnvOpenOptions::lock_file`] is on), with
    /// [`EnvOpenOptions::get_max_readers`] slots.
    ///
    /// Succeeds only if this process wins the first-opener election at once,
    /// that is, no other process has the environment open or is opening it.
    /// It never waits. On success the lock file is initialized as by a first
    /// opener that has not finished opening; dropping the guard leaves it in
    /// a state the next opener accepts.
    ///
    /// # Errors
    ///
    /// - [`Error::Io`] of kind `WouldBlock` ("environment in use") if another
    ///   process holds the election.
    /// - [`Error::EnvAlreadyOpened`] if this process already has the lock
    ///   file open.
    /// - [`Error::Io`] (`InvalidInput`) for an invalid data-file or lock-file
    ///   name, or when a lock-file guard refuses the file (as
    ///   [`EnvOpenOptions::open`]); other I/O errors, including `EROFS` when
    ///   the lock file cannot be created on read-only media.
    pub fn acquire(dir: &Path, opts: &EnvOpenOptions) -> Result<ExclusiveLock> {
        opts.check_data_file_name()?;
        let lock_file_name = opts.get_lock_file_name();
        opts.check_lock_file_name(&lock_file_name)?;
        zerodb_core::env::check_open_limits(opts.max_readers, opts.max_dbs)?;
        let canonical_dir = dir.canonicalize().map_err(Error::Io)?;
        let data_path = canonical_dir.join(&opts.data_file_name);
        let path = canonical_dir.join(&lock_file_name);
        match zerodb_io::lockfile::LockFile::try_open_exclusive(&path, &data_path, opts.max_readers)
        {
            Ok(Some(lock)) => Ok(ExclusiveLock { _lock: lock }),
            Ok(None) => Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "environment in use",
            ))),
            Err(zerodb_io::lockfile::LockOpenError::AlreadyOpen) => Err(Error::EnvAlreadyOpened),
            Err(zerodb_io::lockfile::LockOpenError::ReadOnlyFs) => {
                Err(Error::Io(std::io::Error::from_raw_os_error(libc::EROFS)))
            }
            Err(zerodb_io::lockfile::LockOpenError::Mismatch(m)) => Err(Error::Io(
                std::io::Error::new(std::io::ErrorKind::InvalidInput, m),
            )),
            Err(zerodb_io::lockfile::LockOpenError::Io(e)) => Err(Error::Io(e)),
        }
    }
}
