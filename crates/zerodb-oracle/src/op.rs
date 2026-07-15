//! The operation model driven against every [`Engine`](crate::Engine).
//!
//! An [`Op`] is one observable action from the SPEC 00 "MUST" surface
//! (`docs/SPEC/00-api-surface.md`). A whole test/fuzz input is a `Vec<Op>`,
//! replayed op-by-op against two engines whose results are compared
//! ([`crate::run`]).
//!
//! ## Modeling choices (so the enum stays fuzz-friendly and self-referential-free)
//!
//! * Keys and values are [`Key`]/[`Value`] wrappers over `Vec<u8>` with bounded
//!   [`Arbitrary`] sizes, so a random byte stream yields valid-ish sequences.
//! * There is **one active transaction at a time** in this model (a linear
//!   script), plus optionally one nested read txn parented to it. This is a
//!   deliberate simplification of LMDB's concurrency; it is sufficient for the
//!   SPEC 00 surface and keeps the harness free of persisted cursors.
//! * Cursor/iteration ops are **whole-snapshot** ops: an iteration op returns
//!   the full ordered `Vec<(key, value)>` (compared verbatim), and positioning
//!   ops (`first`/`last`/exact/`>=`/`>`/`<=`) return a single optional entry.
//!   No cursor object is held across [`Engine::apply`] calls.
//! * In-place cursor mutation (`iter_mut` + `put_current`/`del_current`) is
//!   modeled as a single op that opens the mutable iterator, advances to the
//!   `nth` entry, mutates, and finishes — all within one `apply` call.
//!
//! Databases are addressed by a small `u8` index taken modulo the number of
//! open databases at apply time, so ops are almost always "valid" against a
//! non-empty engine.

use arbitrary::{Arbitrary, Unstructured};

/// A key: usually 0..=16 bytes, occasionally 500..=520 bytes so the generator
/// crosses the SPEC 01 §S4 511-byte max-key boundary (exercising both valid
/// near-max keys and `BadValSize` rejections). Empty keys are intentionally
/// reachable so the harness exercises LMDB's empty-key rejection (SPEC 01
/// §S4).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Key(pub Vec<u8>);

/// A value: usually 0..=64 bytes, occasionally into the multi-megabyte range
/// so the generator reaches overflow pages. The large branch is rare (about
/// 1 in 256) to keep proptest/fuzz runtime bounded.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Value(pub Vec<u8>);

impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Key({})", hex(&self.0))
    }
}

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Value({})", hex(&self.0))
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2 + 2);
    s.push_str("0x");
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Lower/upper bound (inclusive) of the "crosses the SPEC 01 §S4 511-byte
/// max-key boundary" branch: spans just under, at, and just over the fork's
/// constant 511-byte max key so both `Ok` and `BadValSize` are reachable.
const KEY_BOUNDARY_RANGE: (u16, u16) = (500, 520);

impl<'a> Arbitrary<'a> for Key {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        // About 1 in 32: cross the 511-byte max-key boundary (SPEC 01 §S4)
        // instead of the common small-key case.
        let len = if u.ratio(1u8, 32u8)? {
            u.int_in_range(KEY_BOUNDARY_RANGE.0..=KEY_BOUNDARY_RANGE.1)? as usize
        } else {
            u.int_in_range(0u16..=16)? as usize
        };
        let mut v = vec![0u8; len];
        // `fill_buffer` never errors: it zero-pads once the input is exhausted.
        u.fill_buffer(&mut v)?;
        Ok(Key(v))
    }
}

impl<'a> Arbitrary<'a> for Value {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        // About 1 in 256: a multi-megabyte value (overflow pages), kept rare
        // so proptest/fuzz runtime stays bounded. Otherwise the common small
        // case.
        let len = if u.ratio(1u16, 256u16)? {
            u.int_in_range(1_000_000u32..=3_000_000)? as usize
        } else {
            u.int_in_range(0u16..=64)? as usize
        };
        let mut v = vec![0u8; len];
        u.fill_buffer(&mut v)?;
        Ok(Value(v))
    }
}

/// Selects which database an op targets by *name*, mapped to a bounded set of
/// catalog names so fuzzing keeps hitting the same handful of databases.
#[derive(Arbitrary, Debug, Clone, PartialEq, Eq)]
pub enum DbName {
    /// The unnamed (main) database.
    Unnamed,
    /// A named database `db0`..`db3` (index taken modulo 4).
    Named(u8),
}

impl DbName {
    /// The catalog name (`None` = unnamed/main DB).
    pub fn resolve(&self) -> Option<String> {
        match self {
            DbName::Unnamed => None,
            DbName::Named(n) => Some(format!("db{}", n % 4)),
        }
    }
}

/// A put flag exercised through `put_with_flags`.
#[derive(Arbitrary, Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutFlag {
    /// `MDB_APPEND` — strictly-ascending bulk insert (SPEC 01 §S1).
    Append,
    /// `MDB_NOOVERWRITE` — insert only if key absent (SPEC 01 §S2).
    NoOverwrite,
}

/// One operation in a differential sequence.
#[derive(Arbitrary, Debug, Clone, PartialEq, Eq)]
pub enum Op {
    // ---- environment ----
    /// Drop everything and reopen the env with a (monotonically non-decreasing)
    /// map size. heed 0.22 has no `Env::resize`; growth = drop env + reopen
    /// larger (SPEC 00 row 3).
    Reopen {
        /// Extra map size, in units of 4 KiB, added on top of the 1 MiB base.
        map_size_kib: u16,
    },

    // ---- transactions ----
    /// Begin a top-level write txn (no-op if a txn is already active).
    BeginRw,
    /// Begin a top-level read txn.
    BeginRo,
    /// Commit the active txn.
    Commit,
    /// Abort the active txn.
    Abort,
    /// Open a read txn nested in the active write txn — sees its uncommitted
    /// state (fork-only; SPEC 00 row 16, SPEC 01 §S9). While open, reads are
    /// served from it and writes are blocked.
    BeginNestedRo,
    /// Close the nested read txn and resume the parent write txn.
    EndNestedRo,

    // ---- databases ----
    /// Create (or open) a database by name, inside the active write txn.
    CreateDb {
        /// The database to create.
        name: DbName,
    },
    /// Empty a database (`mdb_drop(_, 0)`).
    ClearDb {
        /// Target database index.
        db: u8,
    },
    /// Delete a database and its catalog entry (`mdb_drop(_, 1)`).
    DropDb {
        /// Target database index.
        db: u8,
    },

    // ---- key/value ----
    /// `get` (exact) via `Database::get`.
    Get {
        /// Target database index.
        db: u8,
        /// Key to look up.
        key: Key,
    },
    /// `put` (overwrite allowed).
    Put {
        /// Target database index.
        db: u8,
        /// Key to insert.
        key: Key,
        /// Value to store.
        val: Value,
    },
    /// `put_with_flags`.
    PutFlagged {
        /// Target database index.
        db: u8,
        /// Key to insert.
        key: Key,
        /// Value to store.
        val: Value,
        /// The put flag to use.
        flag: PutFlag,
    },
    /// `put_reserved` — reserve `val.len()` bytes and write `val` into them.
    PutReserved {
        /// Target database index.
        db: u8,
        /// Key to insert.
        key: Key,
        /// Value written into the reserved space.
        val: Value,
    },
    /// `delete`.
    Del {
        /// Target database index.
        db: u8,
        /// Key to delete.
        key: Key,
    },
    /// `len` (entry count).
    Len {
        /// Target database index.
        db: u8,
    },
    /// `is_empty`.
    IsEmpty {
        /// Target database index.
        db: u8,
    },

    // ---- positioning (single-entry) ----
    /// `first` — minimum entry.
    First {
        /// Target database index.
        db: u8,
    },
    /// `last` — maximum entry.
    Last {
        /// Target database index.
        db: u8,
    },
    /// Cursor `SET` (exact match), returning the value.
    SetExact {
        /// Target database index.
        db: u8,
        /// Key to seek exactly.
        key: Key,
    },
    /// Cursor `SET_RANGE` — first entry `>=` key.
    SetRange {
        /// Target database index.
        db: u8,
        /// Lower bound key.
        key: Key,
    },
    /// `get_greater_than` — first entry `>` key (milli facet seek).
    GetGreaterThan {
        /// Target database index.
        db: u8,
        /// Exclusive lower bound key.
        key: Key,
    },
    /// `get_lower_than_or_equal_to` — last entry `<=` key (milli facet seek).
    GetLowerThanOrEqualTo {
        /// Target database index.
        db: u8,
        /// Upper bound key.
        key: Key,
    },

    // ---- iteration (whole-snapshot) ----
    /// Forward full scan.
    Iter {
        /// Target database index.
        db: u8,
    },
    /// Reverse full scan.
    RevIter {
        /// Target database index.
        db: u8,
    },
    /// Forward prefix scan.
    PrefixIter {
        /// Target database index.
        db: u8,
        /// Key prefix.
        prefix: Key,
    },
    /// Reverse prefix scan.
    RevPrefixIter {
        /// Target database index.
        db: u8,
        /// Key prefix.
        prefix: Key,
    },

    // ---- in-place cursor mutation ----
    /// `iter_mut`, advance to the `nth` entry, `put_current` (same key, new value).
    IterMutPutCurrent {
        /// Target database index.
        db: u8,
        /// Position to mutate.
        nth: u8,
        /// Replacement value.
        val: Value,
    },
    /// `iter_mut`, advance to the `nth` entry, `del_current`.
    IterMutDelCurrent {
        /// Target database index.
        db: u8,
        /// Position to delete.
        nth: u8,
    },

    // ---- post-txn verification ----
    /// Open a fresh independent read txn and read a key — models a post-commit
    /// verification read (sees only committed state).
    VerifyGet {
        /// Target database index.
        db: u8,
        /// Key to look up.
        key: Key,
    },
}
