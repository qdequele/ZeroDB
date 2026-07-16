//! The shared harness driver: the **single** authority that decides, for each
//! [`Op`], whether it applies given the tracked state, or is a structured
//! [`Skip`].
//!
//! ## Why this exists (M1.2 precondition)
//!
//! Before this module, [`LmdbEngine`](crate::LmdbEngine) derived every skip
//! decision inline from its own transaction/database fields. With a second
//! engine landing (the native `ZerodbEngine`), those decisions had to move to
//! one place so the two engines cannot *drift* on when an op is a no-op — a
//! drift would masquerade as a real divergence (or hide one). The M0.3 handback
//! flagged this hoist as a hard precondition for M1.2.
//!
//! Every engine calls [`classify`] at the top of its `apply`; if it returns
//! `Some(skip)`, the engine returns `OpResult::Skipped(skip)` without touching
//! its backend. The decision depends only on two facts about tracked state — the
//! current transaction kind and whether any database is open — so no per-engine
//! state can diverge here.

use crate::{Op, Skip};

/// The transaction state an engine is in, abstracted away from any backend.
///
/// This is the only transaction fact [`classify`] needs; each engine maps its
/// own concrete transaction representation onto it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    /// No transaction is active.
    None,
    /// A top-level write transaction is active.
    Rw,
    /// A top-level read transaction is active.
    Ro,
    /// A read transaction nested in the active write transaction is active
    /// (the write transaction is paused).
    RwNested,
}

impl TxnState {
    /// Whether reads are servable (any active txn can serve reads).
    fn can_read(self) -> bool {
        !matches!(self, TxnState::None)
    }

    /// The skip an op needing the active write txn incurs, or `None` if a write
    /// txn is available (`Rw`).
    fn write_txn_skip(self) -> Option<Skip> {
        match self {
            TxnState::Rw => None,
            TxnState::RwNested => Some(Skip::WriteBlockedByNested),
            TxnState::Ro | TxnState::None => Some(Skip::NoWriteTxn),
        }
    }
}

/// Decide whether `op` applies, given the tracked transaction kind and whether
/// any database is open. Returns `Some(skip)` if the op is a structured no-op on
/// both engines, or `None` to proceed.
///
/// This is the single owner of the op-validity precedence. The precedence for
/// database-targeting ops is: **no-database first**, then the transaction
/// requirement — matching the historical `LmdbEngine` ordering exactly, so the
/// hoist changes no observable result.
/// `cleared_in_txn` is the third tracked fact: whether the current write txn
/// has executed a `ClearDb`. It exists solely for the [`Skip::KnownForkBug`]
/// guard (`docs/UPSTREAM-BUGS.md` FORK-1): an `Append` put in a write txn that
/// has cleared a db can SEGV the vendored fork, so the combination is skipped
/// symmetrically until the fork is fixed. Engines must set the flag when they
/// execute a `ClearDb` and reset it at every txn boundary (`BeginRw`, `Commit`,
/// `Abort`) — a rule derived purely from the op stream, so engines cannot
/// drift on it.
#[must_use]
pub fn classify(op: &Op, txn: TxnState, dbs_empty: bool, cleared_in_txn: bool) -> Option<Skip> {
    use Op::*;

    // FORK-1 guard: see doc comment above and `docs/UPSTREAM-BUGS.md`.
    if cleared_in_txn {
        if let PutFlagged { flag, .. } = op {
            if matches!(flag, crate::PutFlag::Append) {
                return Some(Skip::KnownForkBug);
            }
        }
    }

    // A read op needs *some* active txn; `None` → NoTxn.
    let read_txn_skip = || {
        if txn.can_read() {
            None
        } else {
            Some(Skip::NoTxn)
        }
    };
    // A database-targeting read op: no-db first, then read-txn.
    let db_read = || {
        if dbs_empty {
            Some(Skip::NoDb)
        } else {
            read_txn_skip()
        }
    };
    // A database-targeting write op: no-db first, then write-txn.
    let db_write = || {
        if dbs_empty {
            Some(Skip::NoDb)
        } else {
            txn.write_txn_skip()
        }
    };

    match op {
        // Environment — always applies.
        Reopen { .. } => None,

        // Transactions.
        BeginRw | BeginRo => {
            if matches!(txn, TxnState::None) {
                None
            } else {
                Some(Skip::TxnAlreadyOpen)
            }
        }
        Commit | Abort => {
            if matches!(txn, TxnState::None) {
                Some(Skip::NoTxn)
            } else {
                None
            }
        }
        BeginNestedRo => {
            if matches!(txn, TxnState::Rw) {
                None
            } else {
                Some(Skip::NoWriteTxnForNested)
            }
        }
        EndNestedRo => {
            if matches!(txn, TxnState::RwNested) {
                None
            } else {
                Some(Skip::NoNestedToEnd)
            }
        }

        // Databases. `CreateDb` needs only a write txn (it opens/creates the db
        // itself); Clear/Drop target an existing db, so no-db first.
        CreateDb { .. } => txn.write_txn_skip(),
        ClearDb { .. } | DropDb { .. } => db_write(),

        // Key/value writes.
        Put { .. } | PutFlagged { .. } | PutReserved { .. } | Del { .. } => db_write(),

        // Key/value reads and positioning.
        Get { .. }
        | Len { .. }
        | IsEmpty { .. }
        | First { .. }
        | Last { .. }
        | SetExact { .. }
        | SetRange { .. }
        | GetGreaterThan { .. }
        | GetLowerThanOrEqualTo { .. }
        | Iter { .. }
        | RevIter { .. }
        | PrefixIter { .. }
        | RevPrefixIter { .. } => db_read(),

        // In-place cursor mutation needs a write txn.
        IterMutPutCurrent { .. } | IterMutDelCurrent { .. } => db_write(),

        // A fresh independent read: only needs a db to exist (its own txn is
        // opened internally).
        VerifyGet { .. } => {
            if dbs_empty {
                Some(Skip::NoDb)
            } else {
                None
            }
        }
    }
}
