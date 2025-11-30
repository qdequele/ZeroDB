//! Error types for ZeroDB, matching LMDB error codes exactly.

use std::io;
use thiserror::Error;

/// Result type alias for ZeroDB operations.
pub type Result<T> = std::result::Result<T, Error>;

/// LMDB-compatible error codes.
///
/// These match the LMDB error codes exactly for compatibility.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum Error {
    /// Key/data pair already exists.
    ///
    /// May also be returned by append functions when data doesn't
    /// respect the database ordering.
    #[error("MDB_KEYEXIST: Key/data pair already exists")]
    KeyExist,

    /// Key/data pair was not found (EOF).
    #[error("MDB_NOTFOUND: No matching key/data pair found")]
    NotFound,

    /// Requested page not found - this usually indicates corruption.
    #[error("MDB_PAGE_NOTFOUND: Requested page not found")]
    PageNotFound,

    /// Located page was wrong type - indicates corruption.
    #[error("MDB_CORRUPTED: Located page was wrong type")]
    Corrupted,

    /// Update of meta page failed or environment had fatal error.
    #[error("MDB_PANIC: Update of meta page failed or environment had fatal error")]
    Panic,

    /// Environment version mismatch.
    #[error("MDB_VERSION_MISMATCH: Environment version mismatch")]
    VersionMismatch,

    /// File is not a valid LMDB file.
    #[error("MDB_INVALID: File is not a valid LMDB file")]
    Invalid,

    /// Environment map size reached.
    #[error("MDB_MAP_FULL: Environment mapsize reached")]
    MapFull,

    /// Environment max databases reached.
    #[error("MDB_DBS_FULL: Environment maxdbs reached")]
    DbsFull,

    /// Environment max readers reached.
    #[error("MDB_READERS_FULL: Environment maxreaders reached")]
    ReadersFull,

    /// Too many TLS keys in use - Windows only.
    #[error("MDB_TLS_FULL: Too many TLS keys in use")]
    TlsFull,

    /// Transaction has too many dirty pages.
    #[error("MDB_TXN_FULL: Transaction has too many dirty pages")]
    TxnFull,

    /// Cursor stack too deep - internal error.
    #[error("MDB_CURSOR_FULL: Cursor stack too deep")]
    CursorFull,

    /// Page has not enough space - internal error.
    #[error("MDB_PAGE_FULL: Page has not enough space")]
    PageFull,

    /// Database contents grew beyond environment map size.
    #[error("MDB_MAP_RESIZED: Database contents grew beyond environment mapsize")]
    MapResized,

    /// Operation and DB incompatible, or DB type changed.
    ///
    /// This can mean:
    /// - The operation expects an MDB_DUPSORT / MDB_DUPFIXED database.
    /// - Opening a named DB when the unnamed DB has MDB_DUPSORT / MDB_INTEGERKEY.
    /// - Accessing a data record as a database, or vice versa.
    /// - The database was dropped and recreated with different flags.
    #[error("MDB_INCOMPATIBLE: Operation and DB incompatible, or DB type changed")]
    Incompatible,

    /// Invalid reuse of reader locktable slot.
    #[error("MDB_BAD_RSLOT: Invalid reuse of reader locktable slot")]
    BadRslot,

    /// Transaction cannot recover - it must be aborted.
    #[error("MDB_BAD_TXN: Transaction must abort, has a child, or is invalid")]
    BadTxn,

    /// Unsupported size of key/DB name/data, or wrong DUPFIXED size.
    ///
    /// Common causes:
    /// - Zero-length key
    /// - Key longer than max allowed (511 bytes by default)
    /// - With DUPSORT, value longer than max key size
    #[error("MDB_BAD_VALSIZE: Unsupported size of key/DB name/data, or wrong DUPFIXED size")]
    BadValSize,

    /// The specified DBI was changed unexpectedly.
    #[error("MDB_BAD_DBI: The specified DBI was changed unexpectedly")]
    BadDbi,

    /// Unexpected problem - transaction should abort.
    #[error("MDB_PROBLEM: Unexpected problem - txn should abort")]
    Problem,

    /// I/O error.
    #[error("I/O error ({kind}): {message}")]
    Io {
        /// The kind of I/O error.
        kind: IoErrorKind,
        /// Error message.
        message: String,
    },

    /// Environment is already open.
    #[error("Environment is already open in this process")]
    EnvAlreadyOpened,
}

/// Simplified I/O error kind for Clone + Eq support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoErrorKind {
    /// File not found.
    NotFound,
    /// Permission denied.
    PermissionDenied,
    /// Already exists.
    AlreadyExists,
    /// Invalid input.
    InvalidInput,
    /// Invalid data.
    InvalidData,
    /// Would block.
    WouldBlock,
    /// Other error.
    Other,
}

impl std::fmt::Display for IoErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IoErrorKind::NotFound => write!(f, "not found"),
            IoErrorKind::PermissionDenied => write!(f, "permission denied"),
            IoErrorKind::AlreadyExists => write!(f, "already exists"),
            IoErrorKind::InvalidInput => write!(f, "invalid input"),
            IoErrorKind::InvalidData => write!(f, "invalid data"),
            IoErrorKind::WouldBlock => write!(f, "would block"),
            IoErrorKind::Other => write!(f, "other"),
        }
    }
}

impl From<io::ErrorKind> for IoErrorKind {
    fn from(kind: io::ErrorKind) -> Self {
        match kind {
            io::ErrorKind::NotFound => IoErrorKind::NotFound,
            io::ErrorKind::PermissionDenied => IoErrorKind::PermissionDenied,
            io::ErrorKind::AlreadyExists => IoErrorKind::AlreadyExists,
            io::ErrorKind::InvalidInput => IoErrorKind::InvalidInput,
            io::ErrorKind::InvalidData => IoErrorKind::InvalidData,
            io::ErrorKind::WouldBlock => IoErrorKind::WouldBlock,
            _ => IoErrorKind::Other,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io {
            kind: err.kind().into(),
            message: err.to_string(),
        }
    }
}

impl Error {
    /// Returns `true` if this is a `NotFound` error.
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::NotFound)
    }

    /// Returns `true` if this is a `KeyExist` error.
    pub fn is_key_exist(&self) -> bool {
        matches!(self, Error::KeyExist)
    }

    /// Converts to the LMDB error code.
    pub fn to_err_code(&self) -> i32 {
        match self {
            Error::KeyExist => -30799,
            Error::NotFound => -30798,
            Error::PageNotFound => -30797,
            Error::Corrupted => -30796,
            Error::Panic => -30795,
            Error::VersionMismatch => -30794,
            Error::Invalid => -30793,
            Error::MapFull => -30792,
            Error::DbsFull => -30791,
            Error::ReadersFull => -30790,
            Error::TlsFull => -30789,
            Error::TxnFull => -30788,
            Error::CursorFull => -30787,
            Error::PageFull => -30786,
            Error::MapResized => -30785,
            Error::Incompatible => -30784,
            Error::BadRslot => -30783,
            Error::BadTxn => -30782,
            Error::BadValSize => -30781,
            Error::BadDbi => -30780,
            Error::Problem => -30779,
            Error::Io { .. } => -1,
            Error::EnvAlreadyOpened => -30778,
        }
    }

    /// Creates an error from an LMDB error code.
    pub fn from_err_code(code: i32) -> Self {
        match code {
            -30799 => Error::KeyExist,
            -30798 => Error::NotFound,
            -30797 => Error::PageNotFound,
            -30796 => Error::Corrupted,
            -30795 => Error::Panic,
            -30794 => Error::VersionMismatch,
            -30793 => Error::Invalid,
            -30792 => Error::MapFull,
            -30791 => Error::DbsFull,
            -30790 => Error::ReadersFull,
            -30789 => Error::TlsFull,
            -30788 => Error::TxnFull,
            -30787 => Error::CursorFull,
            -30786 => Error::PageFull,
            -30785 => Error::MapResized,
            -30784 => Error::Incompatible,
            -30783 => Error::BadRslot,
            -30782 => Error::BadTxn,
            -30781 => Error::BadValSize,
            -30780 => Error::BadDbi,
            -30779 => Error::Problem,
            _ => Error::Problem,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_roundtrip() {
        let errors = [
            Error::KeyExist,
            Error::NotFound,
            Error::PageNotFound,
            Error::Corrupted,
            Error::Panic,
            Error::VersionMismatch,
            Error::Invalid,
            Error::MapFull,
            Error::DbsFull,
            Error::ReadersFull,
            Error::TlsFull,
            Error::TxnFull,
            Error::CursorFull,
            Error::PageFull,
            Error::MapResized,
            Error::Incompatible,
            Error::BadRslot,
            Error::BadTxn,
            Error::BadValSize,
            Error::BadDbi,
            Error::Problem,
        ];

        for err in errors {
            let code = err.to_err_code();
            let recovered = Error::from_err_code(code);
            assert_eq!(err, recovered, "Error roundtrip failed for {:?}", err);
        }
    }

    #[test]
    fn not_found_helper() {
        assert!(Error::NotFound.is_not_found());
        assert!(!Error::KeyExist.is_not_found());
    }

    #[test]
    fn io_error_conversion() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err: Error = io_err.into();

        match err {
            Error::Io { kind, message } => {
                assert_eq!(kind, IoErrorKind::NotFound);
                assert!(message.contains("not found"));
            }
            _ => panic!("Expected Io error"),
        }
    }
}
