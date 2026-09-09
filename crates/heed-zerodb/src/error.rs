//! The error taxonomy, 1:1 with `heed::Error` / `heed::MdbError` (SPEC 00
//! rows 54–58, SPEC 04 §8.1; ADR-0003 §C5). The adapter maps ZeroDB's native
//! [`zerodb::Error`] / [`zerodb::MdbError`] onto these so consumers that match
//! `heed::Error::{Io, Mdb, Encoding, Decoding, EnvAlreadyOpened}` and the
//! `MdbError` variants (`MapFull`, `Invalid`, `BadValSize`, `KeyExist`, …) see
//! identical shapes. `Encoding`/`Decoding` are publicly constructible from a
//! [`BoxedError`] (consumers construct them — C5).

use std::{error, fmt, io, result};

pub use heed_traits::BoxedError;

/// An error that encapsulates all possible errors in this crate — the exact
/// shape of `heed::Error` (SPEC 00 row 54).
#[derive(Debug)]
pub enum Error {
    /// I/O error: can come from the standard library or be a rewrapped
    /// [`MdbError`].
    Io(io::Error),
    /// LMDB-shaped engine error.
    Mdb(MdbError),
    /// Encoding error (constructed by consumer codecs).
    Encoding(BoxedError),
    /// Decoding error (constructed by consumer codecs).
    Decoding(BoxedError),
    /// The environment is already open in this program; close it to be able to
    /// open it again with different options (SPEC 04 TXN-51).
    EnvAlreadyOpened,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Io(error) => write!(f, "{error}"),
            Error::Mdb(error) => write!(f, "{error}"),
            Error::Encoding(error) => write!(f, "error while encoding: {error}"),
            Error::Decoding(error) => write!(f, "error while decoding: {error}"),
            Error::EnvAlreadyOpened => f.write_str(
                "environment already open in this program; \
                close it to be able to open it again with different options",
            ),
        }
    }
}

impl error::Error for Error {}

impl From<MdbError> for Error {
    fn from(error: MdbError) -> Error {
        match error {
            MdbError::Other(e) => Error::Io(io::Error::from_raw_os_error(e)),
            _ => Error::Mdb(error),
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Error {
        Error::Io(error)
    }
}

/// Map ZeroDB's native error onto the heed-shaped taxonomy (SPEC 04 §8.1).
impl From<zerodb::Error> for Error {
    fn from(e: zerodb::Error) -> Error {
        match e {
            zerodb::Error::Io(io) => Error::Io(io),
            zerodb::Error::Mdb(m) => Error::Mdb(m.into()),
            zerodb::Error::EnvAlreadyOpened => Error::EnvAlreadyOpened,
            // `zerodb::Error` is #[non_exhaustive]; a future variant maps to the
            // catch-all `Problem` until the taxonomy grows a peer.
            _ => Error::Mdb(MdbError::Problem),
        }
    }
}

/// Either a success or an [`Error`].
pub type Result<T> = result::Result<T, Error>;

/// An LMDB error kind — the exact variant set of `heed::MdbError`
/// (`mdb.master` branch; the `master3`-only encryption/checksum variants are
/// out of Phase 1 scope, SPEC 00 WON'T). ZeroDB emits its own codes, so the
/// raw-`c_int`→string mapping heed does over LMDB return codes is dropped
/// (ADR-0003 heed-suite scoping: `lmdb_error.rs` is *partially IN* — keep the
/// variant identities, drop the raw-rc assertions).
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MdbError {
    /// A key/data pair already exists (also `APPEND`/`NO_OVERWRITE` misuse).
    KeyExist,
    /// A key/data pair was not found (EOF).
    NotFound,
    /// Requested page not found — usually indicates corruption.
    PageNotFound,
    /// Located page was the wrong type.
    Corrupted,
    /// Update of meta page failed or environment had a fatal error.
    Panic,
    /// Environment version mismatch.
    VersionMismatch,
    /// File is not a valid LMDB file.
    Invalid,
    /// Environment mapsize reached.
    MapFull,
    /// Environment maxdbs reached.
    DbsFull,
    /// Environment maxreaders reached.
    ReadersFull,
    /// Too many TLS keys in use — Windows only.
    TlsFull,
    /// Txn has too many dirty pages.
    TxnFull,
    /// Cursor stack too deep — internal error.
    CursorFull,
    /// Page has not enough space — internal error.
    PageFull,
    /// Database contents grew beyond environment mapsize.
    MapResized,
    /// Operation and DB incompatible, or DB type changed.
    Incompatible,
    /// Invalid reuse of reader locktable slot.
    BadRslot,
    /// Transaction cannot recover — it must be aborted.
    BadTxn,
    /// Unsupported size of key/DB name/data, or wrong DUP_FIXED size.
    BadValSize,
    /// The specified DBI was changed unexpectedly.
    BadDbi,
    /// Unexpected problem — transaction should abort.
    Problem,
    /// Other error (raw OS error code).
    Other(i32),
}

impl MdbError {
    /// Returns `true` if the given error is [`MdbError::NotFound`].
    pub fn not_found(&self) -> bool {
        *self == MdbError::NotFound
    }
}

impl fmt::Display for MdbError {
    // The exact `mdb_strerror` text of the vendored fork (`mdb_errstr` table in
    // liblmdb/mdb.c), which is what heed's `Display` prints: consumers format
    // these into logs and HTTP error bodies, so the wording is part of the
    // observable surface. Pinned against real heed by
    // `zerodb-oracle/tests/adapter_surface_parity.rs`.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MdbError::KeyExist => f.write_str("MDB_KEYEXIST: Key/data pair already exists"),
            MdbError::NotFound => f.write_str("MDB_NOTFOUND: No matching key/data pair found"),
            MdbError::PageNotFound => f.write_str("MDB_PAGE_NOTFOUND: Requested page not found"),
            MdbError::Corrupted => f.write_str("MDB_CORRUPTED: Located page was wrong type"),
            MdbError::Panic => {
                f.write_str("MDB_PANIC: Update of meta page failed or environment had fatal error")
            }
            MdbError::VersionMismatch => {
                f.write_str("MDB_VERSION_MISMATCH: Database environment version mismatch")
            }
            MdbError::Invalid => f.write_str("MDB_INVALID: File is not an LMDB file"),
            MdbError::MapFull => f.write_str("MDB_MAP_FULL: Environment mapsize limit reached"),
            MdbError::DbsFull => f.write_str("MDB_DBS_FULL: Environment maxdbs limit reached"),
            MdbError::ReadersFull => {
                f.write_str("MDB_READERS_FULL: Environment maxreaders limit reached")
            }
            MdbError::TlsFull => f.write_str(
                "MDB_TLS_FULL: Thread-local storage keys full - too many environments open",
            ),
            MdbError::TxnFull => f.write_str(
                "MDB_TXN_FULL: Transaction has too many dirty pages - transaction too big",
            ),
            MdbError::CursorFull => {
                f.write_str("MDB_CURSOR_FULL: Internal error - cursor stack limit reached")
            }
            MdbError::PageFull => {
                f.write_str("MDB_PAGE_FULL: Internal error - page has no more space")
            }
            MdbError::MapResized => {
                f.write_str("MDB_MAP_RESIZED: Database contents grew beyond environment mapsize")
            }
            MdbError::Incompatible => {
                f.write_str("MDB_INCOMPATIBLE: Operation and DB incompatible, or DB flags changed")
            }
            MdbError::BadRslot => {
                f.write_str("MDB_BAD_RSLOT: Invalid reuse of reader locktable slot")
            }
            MdbError::BadTxn => {
                f.write_str("MDB_BAD_TXN: Transaction must abort, has a child, or is invalid")
            }
            MdbError::BadValSize => f.write_str(
                "MDB_BAD_VALSIZE: Unsupported size of key/DB name/data, or wrong DUPFIXED size",
            ),
            MdbError::BadDbi => {
                f.write_str("MDB_BAD_DBI: The specified DBI handle was closed/changed unexpectedly")
            }
            MdbError::Problem => f.write_str("MDB_PROBLEM: Unexpected problem - txn should abort"),
            // heed prints the bare `strerror` text for an OS error code; Rust's
            // `io::Error` Display appends " (os error N)", so trim that suffix.
            MdbError::Other(code) => {
                let text = io::Error::from_raw_os_error(*code).to_string();
                let suffix = format!(" (os error {code})");
                f.write_str(text.strip_suffix(suffix.as_str()).unwrap_or(&text))
            }
        }
    }
}

impl error::Error for MdbError {}

/// Map ZeroDB's native `MdbError` onto heed's variant set (SPEC 04 §8.1).
/// ZeroDB's taxonomy is a strict subset of LMDB's; each maps to its namesake.
impl From<zerodb::MdbError> for MdbError {
    fn from(m: zerodb::MdbError) -> MdbError {
        match m {
            zerodb::MdbError::Invalid => MdbError::Invalid,
            zerodb::MdbError::MapFull => MdbError::MapFull,
            zerodb::MdbError::BadValSize => MdbError::BadValSize,
            zerodb::MdbError::KeyExist => MdbError::KeyExist,
            zerodb::MdbError::NotFound => MdbError::NotFound,
            zerodb::MdbError::DbsFull => MdbError::DbsFull,
            zerodb::MdbError::Incompatible => MdbError::Incompatible,
            zerodb::MdbError::ReadersFull => MdbError::ReadersFull,
            zerodb::MdbError::BadTxn => MdbError::BadTxn,
            // `zerodb::MdbError` is #[non_exhaustive]; map a future code to the
            // generic `Problem` until it earns a dedicated peer.
            _ => MdbError::Problem,
        }
    }
}
