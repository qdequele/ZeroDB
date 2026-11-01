//! Environment introspection results and the close event.

use super::*;

/// Environment info (SPEC 00 rows 20/60; `mdb_env_info` / `MDB_envinfo`).
///
/// Each field names its `MDB_envinfo` counterpart. `me_mapaddr` is not
/// exposed: ZeroDB never maps at a fixed address (`MDB_FIXEDMAP` is not
/// supported, SPEC 01). `last_pgno` counts pages of ZeroDB's own layout, so
/// it is not comparable with LMDB's value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvInfo {
    /// The configured/runtime map size, in bytes (`me_mapsize`).
    pub map_size: u64,
    /// Id of the last used page (`me_last_pgno`) — the file high-water of the
    /// live snapshot. Format-specific value (see the struct docs).
    pub last_pgno: u64,
    /// Id of the last committed transaction (`me_last_txnid`).
    pub last_txnid: u64,
    /// Reader-table size (`me_maxreaders`): configured, or adopted from the
    /// lock file by a process that joined a running environment.
    pub max_readers: u32,
    /// `me_numreaders`: the **high-water** reader-slot count — the maximum
    /// number of simultaneously live readers ever observed. It does **not**
    /// decrease when a read txn ends; that is LMDB's actual behavior, verified
    /// against the fork and reproduced here. In a lock-file environment it
    /// covers every process's readers, as LMDB's does. For the live count,
    /// use [`EnvInfo::live_readers`].
    pub num_readers: u32,
    /// **ZeroDB extension** (no `MDB_envinfo` counterpart): reader slots
    /// currently occupied, by any process in a lock-file environment. A
    /// concurrently sampled diagnostic count.
    pub live_readers: u32,
}

/// One occupied reader-table slot, as reported by [`Env::reader_list`]
/// (one `MDB_reader` row in `mdb_reader_list` output).
///
/// A **ZeroDB extension**: heed exposes no reader introspection whatsoever, so
/// there is no signature to mirror. Compared to LMDB's row there is no
/// `thread` column, since a `RoTxn` is `Send` and the owning thread is not a
/// stable property worth reporting. The `pid` column is reported in an
/// environment opened with a lock file ([`ReaderEntry::pid`]); without one
/// every reader is in this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReaderEntry {
    /// The slot's index in the reader table, in `0..max_readers`. Stable for
    /// the life of the read txn that owns it, then reusable by another reader.
    pub slot: u32,
    /// The snapshot txnid this reader has pinned — the transaction whose data
    /// it sees, and the value that holds GC back (SPEC 04 TXN-20).
    ///
    /// `None` means the slot is **claimed but not yet pinned**: a read txn has
    /// won the slot's CAS and is inside the publish-and-verify loop (SPEC 04
    /// §4.3), a window of a few instructions. Such a reader constrains nothing
    /// yet, and by the time you act on this value it is certainly pinned.
    pub txnid: Option<u64>,
    /// How many commits the environment has published since the pinned
    /// snapshot: `Env::txnid() - txnid`. `None` exactly when [`ReaderEntry::txnid`]
    /// is.
    ///
    /// This is the number that matters operationally — a large and growing age
    /// identifies the long-lived read transaction that is preventing free-page
    /// reclamation. It is computed against **one** commit point sampled for
    /// the whole [`Env::reader_list`] call, and saturates at 0 rather than
    /// underflowing if a reader pinned a commit that landed after that sample.
    pub age: Option<u64>,
    /// The PID of the process that owns the slot, in an environment opened
    /// with a lock file (ADR-0023). `None` without a lock file
    /// (every reader is in this process) and for a slot being claimed or
    /// released. LMDB's `mdb_reader_list` prints the same column.
    pub pid: Option<u32>,
}

/// Environment-level statistics (`mdb_env_stat` / `MDB_stat` over the **main**
/// DB). The per-database form is [`crate::rotxn::DatabaseStat`] (SPEC 00
/// row 49).
///
/// Each field names its `MDB_stat` counterpart. The page counts and depth
/// depend on ZeroDB's own on-disk format, so only `entries` (and
/// `page_size`) compare exactly with LMDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvStat {
    /// Database page size in bytes (`ms_psize`).
    pub page_size: u32,
    /// Height of the main tree (`ms_depth`); 0 = empty, 1 = root-is-leaf.
    pub depth: u16,
    /// Branch (internal) pages of the main tree (`ms_branch_pages`).
    pub branch_pages: u64,
    /// Leaf pages of the main tree (`ms_leaf_pages`).
    pub leaf_pages: u64,
    /// Overflow pages of the main tree (`ms_overflow_pages`).
    pub overflow_pages: u64,
    /// Entries in the main tree (`ms_entries`).
    pub entries: u64,
}

/// A signal fired once the environment is fully closed (SPEC 04 TXN-53,
/// SPEC 00 rows 23/24). Obtained from [`Env::prepare_for_closing`].
#[derive(Clone)]
pub struct EnvClosingEvent {
    pub(super) signal: Arc<SignalEvent>,
}

impl std::fmt::Debug for EnvClosingEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvClosingEvent").finish_non_exhaustive()
    }
}

impl EnvClosingEvent {
    /// Block until the environment is fully closed.
    pub fn wait(&self) {
        self.signal.wait();
    }

    /// Block until closed or `dur` elapses. Returns `true` if the env closed.
    #[must_use]
    pub fn wait_timeout(&self, dur: Duration) -> bool {
        self.signal.wait_timeout(dur)
    }
}
