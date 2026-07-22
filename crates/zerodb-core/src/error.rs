//! The engine error taxonomy, shaped like heed's (SPEC 00 rows 54–58).
//!
//! [`Error`] mirrors `heed::Error`: an [`Error::Io`] wrapper, an [`Error::Mdb`]
//! LMDB-code variant, and [`Error::EnvAlreadyOpened`] for the same-process
//! registry (SPEC 04 TXN-51). The `heed-zerodb` adapter (M1.13) maps these 1:1
//! onto `heed::Error`; until then the public `zerodb` crate re-exports them
//! verbatim.
//!
//! SPEC note: several distinct open-time failures (bad magic, wrong
//! `format_version`, bad `page_size`, torn/CRC-failed meta, both-slots-invalid)
//! all collapse to [`MdbError::Invalid`] — this is the mapping SPEC 02 §3.2 /
//! SPEC 06 REC-1..5 mandate (heed's `MdbError::Invalid` → `InvalidStoreFile`,
//! SPEC 00 row 56). There is no separate `VersionMismatch`/`PageSizeMismatch`
//! variant: the spec folds them into `Invalid`.

/// The top-level engine error, mirroring `heed::Error`'s shape.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An I/O error from the underlying file/mmap layer.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// An LMDB-shaped engine error code (SPEC 00 rows 55–58).
    #[error(transparent)]
    Mdb(#[from] MdbError),

    /// The same-process env registry already holds a live handle for this
    /// canonical path (SPEC 04 TXN-51; heed `Error::EnvAlreadyOpened`).
    #[error("environment already opened for this path in this process")]
    EnvAlreadyOpened,
}

/// LMDB-code-shaped errors (`heed::MdbError` subset in the SPEC 00 surface).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MdbError {
    /// `MDB_INVALID` — not a valid store file (bad magic/version/page-size, a
    /// torn/CRC-failed meta, or both meta slots invalid). SPEC 00 row 56.
    #[error("invalid store file")]
    Invalid,

    /// `MDB_MAP_FULL` — the configured `map_size` cannot hold the request.
    /// SPEC 00 row 55.
    #[error("environment map is full")]
    MapFull,

    /// `MDB_BAD_VALSIZE` — a key/value violates the size bounds (SPEC 00
    /// row 57). Not produced by env open; reserved for the write/read paths.
    #[error("key or value size is out of bounds")]
    BadValSize,

    /// `MDB_KEYEXIST` — key already present (`APPEND`/`NO_OVERWRITE`). SPEC 00
    /// row 58. Not produced by env open; reserved for the write path.
    #[error("key already exists")]
    KeyExist,

    /// `MDB_NOTFOUND` — key not found. Reserved for the read/write paths.
    #[error("key not found")]
    NotFound,

    /// `MDB_DBS_FULL` — the named-DB catalog is full (`create_database` beyond
    /// `max_dbs`; SPEC 00 row 4, SPEC 01 §S8). Its `Debug` renders as `DbsFull`,
    /// matching `heed::MdbError::DbsFull` so the oracle's error taxonomy agrees.
    #[error("environment maxdbs reached")]
    DbsFull,

    /// `MDB_INCOMPATIBLE` — a name is opened with flags/shape incompatible with
    /// the existing entry: in Phase 1 this is a `create_database`/`open_database`
    /// on a name that already exists in the main tree as a **plain user key**
    /// (not an `F_SUBDATA` sub-DB record). SPEC 01 §S8; SPEC 02 §6. `Debug`
    /// renders as `Incompatible`, matching `heed::MdbError::Incompatible`.
    #[error("database is incompatible with the requested operation")]
    Incompatible,

    /// `MDB_READERS_FULL` — every reader-table slot is occupied at
    /// `read_txn`/`static_read_txn` begin (SPEC 04 TXN-16; the table is sized
    /// by `EnvOpenOptions::max_readers`, default 126). Raised immediately —
    /// under D-001 (single process) there are no stale cross-process slots to
    /// reap first. `Debug` renders as `ReadersFull`, matching
    /// `heed::MdbError::ReadersFull` for oracle taxonomy parity.
    #[error("environment maxreaders limit reached")]
    ReadersFull,

    /// `MDB_BAD_TXN` — the transaction encountered a mid-operation failure
    /// (e.g. `MapFull` inside a split cascade) and must be aborted; further
    /// operations and `commit` refuse to run on it (LMDB `MDB_TXN_ERROR`
    /// parity; SPEC 04 TXN-59 clean-abort guarantee).
    #[error("transaction must abort")]
    BadTxn,

    /// `MDB_BAD_DBI` analog — a stale [`Database`](crate::rotxn::Database)
    /// handle: the dbi it captured was **closed** after the handle was
    /// obtained, either because the write txn that created the named DB ended
    /// without committing (SPEC 04 TXN-59/60), or because
    /// `Database::drop_db` (`mdb_drop(_, 1)`) closed it env-wide at call time
    /// (SPEC 04 TXN-68; ADR-0013, D-013). Unlike `BadTxn` this does **not**
    /// poison the transaction — the op is refused, the txn stays usable
    /// (probed fork behavior: the `EINVAL` from `TXN_DBI_EXIST` leaves the
    /// txn committable). The vendored fork reports this state as raw `EINVAL`
    /// on every op class (probed 2026-07-22), which heed surfaces as
    /// `Io(InvalidInput)`; the `heed-zerodb` adapter maps this variant to
    /// exactly that observable. `Debug` renders as `BadDbi`, matching
    /// `heed::MdbError::BadDbi`'s name for native diagnostics.
    #[error("the specified DBI handle was closed or changed unexpectedly")]
    BadDbi,
}

/// A convenience result alias for engine operations.
pub type Result<T> = std::result::Result<T, Error>;
