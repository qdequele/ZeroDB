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
}

/// A convenience result alias for engine operations.
pub type Result<T> = std::result::Result<T, Error>;
