//! Normalized results of applying an [`Op`](crate::Op), compared across engines.

/// The normalized error taxonomy the oracle compares on. Maps from
/// `heed::Error` / `heed::MdbError` on the LMDB side; the native zerodb engine
/// (M1.2+) must map its errors into the same variants under the same conditions
/// (see `docs/SPEC` error-taxonomy notes and SPEC 00 rows 54–58).
#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
pub enum OracleError {
    /// `MDB_NOTFOUND`.
    #[error("not-found")]
    NotFound,
    /// `MDB_KEYEXIST` (e.g. `APPEND` misuse, `NO_OVERWRITE` on an existing key).
    #[error("key-exist")]
    KeyExist,
    /// `MDB_MAP_FULL`.
    #[error("map-full")]
    MapFull,
    /// `MDB_BAD_VALSIZE` (empty key, or key/value beyond the size bounds).
    #[error("bad-val-size")]
    BadValSize,
    /// `MDB_INVALID` (not a valid DB file).
    #[error("invalid")]
    Invalid,
    /// Any other error, rendered stably so both engines can agree on the text.
    #[error("other: {0}")]
    Other(String),
}

/// A harness-level precondition that made an op a no-op on *both* engines.
///
/// These are engine-agnostic (structured, not free text) so the LMDB and zerodb
/// engines agree by construction rather than by matching error strings.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Skip {
    /// No transaction is active.
    NoTxn,
    /// A write op was issued without an active write txn.
    NoWriteTxn,
    /// A write op was issued while a nested reader is active (writer paused).
    WriteBlockedByNested,
    /// A db op referenced a database but none are open.
    NoDb,
    /// `BeginNestedRo` without an active write txn (or one already nested).
    NoWriteTxnForNested,
    /// `EndNestedRo` with no nested reader active.
    NoNestedToEnd,
    /// `Begin*` while a transaction is already active.
    TxnAlreadyOpen,
}

/// The observable outcome of applying one [`Op`](crate::Op).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum OpResult {
    /// Success with no interesting return value (put, commit, clear, create…).
    Ok,
    /// A boolean result (`delete` existed; `del_current`/`put_current` acted).
    Bool(bool),
    /// A count (`len`).
    Count(u64),
    /// An optional value (`get`, exact seek).
    MaybeVal(Option<Vec<u8>>),
    /// An optional key/value entry (`first`/`last`/`>=`/`>`/`<=`).
    MaybeEntry(Option<(Vec<u8>, Vec<u8>)>),
    /// An ordered snapshot of entries (iteration ops).
    Entries(Vec<(Vec<u8>, Vec<u8>)>),
    /// The op was a no-op on both engines for a structured reason.
    Skipped(Skip),
    /// The op produced an error.
    Err(OracleError),
}
