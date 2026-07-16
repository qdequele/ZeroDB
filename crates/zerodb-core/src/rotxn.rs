//! Read transactions and the heed-shaped read API over the [`btree`] cursor
//! (SPEC 04 §3, SPEC 00 read-op rows). Milestones 1.3/1.4.
//!
//! A [`RoTxn`] pins the env's **published snapshot** at open: it `Arc`-clones
//! the current [`Snapshot`] object (SPEC 04 TXN-18 — never re-reading a durable
//! meta page, whose slot a later commit overwrites) and borrows the mapped
//! bytes for its life (TXN-37). Since M1.5 it also pins its snapshot txnid in
//! the env's **interim reader registry** (ADR-0005 OQ1; SPEC 04 TXN-21 as
//! amended) so the GC gate never reclaims a page a live reader can reach; the
//! M1.8 lock-free reader table replaces the registry's *implementation*, not
//! this API (TXN-10).
//!
//! The read API is generic over [`TxnRead`], so the same `Database` methods
//! serve a `RoTxn` (mapped bytes) **and** a write txn (`RwTxn`: dirty frames
//! first, map fallback — SPEC 04 TXN-38, ADR-0004 D2). heed's borrow model is
//! reproduced: reads take `&Txn` and return `&'txn [u8]`; mutations take
//! `&mut RwTxn`, so no read borrow can span a mutation (TXN-39).
//!
//! Named-database catalogs are out of scope (M1.6): only the main/unnamed
//! database is exposed, via [`Env::main_database`].

use std::ops::Bound;
use std::sync::Arc;

use crate::btree::{prefix_successor, Cursor, Source, Tree};
use crate::env::{Env, Snapshot};
use crate::error::{Error, MdbError, Result};
use crate::page::{DBRecord, PageError};

/// Map a structural tree-decode error to the public taxonomy. A corrupt page
/// reached during a read means the store is not a valid zerodb file
/// (SPEC 00 row 56); for a builder/write-path-produced tree this never fires.
pub(crate) fn map_page_err(_e: PageError) -> Error {
    Error::Mdb(MdbError::Invalid)
}

/// Anything the read API can read through: a [`RoTxn`] (mapped bytes of a
/// committed snapshot) or a [`crate::rwtxn::RwTxn`] (the writer's in-progress
/// view). The three accessors are exactly what [`Tree`] needs (ADR-0004 D2).
pub trait TxnRead {
    /// Where this txn's pages come from (SPEC 04 TXN-37/38).
    fn source(&self) -> Source<'_>;
    /// The main DB's root/stats as this txn observes them.
    fn main_record(&self) -> &DBRecord;
    /// The free (GC) DB's root/stats as this txn observes them (SPEC 05 §7 —
    /// the `non_free_pages_size` walk reads the GC DB under a snapshot).
    fn free_record(&self) -> &DBRecord;
    /// The env's page size.
    fn page_size(&self) -> u32;
}

/// A read-only transaction: a consistent view of one committed snapshot
/// (SPEC 04 §3). It borrows the [`Env`] for its whole life, which keeps the
/// mapped file alive so every `&'txn [u8]` it lends stays valid (TXN-37), and
/// holds an `Arc` to the pinned [`Snapshot`] so its roots survive later
/// commits (TXN-18).
pub struct RoTxn<'env> {
    env: &'env Env,
    bytes: &'env [u8],
    psize: u32,
    snap: Arc<Snapshot>,
}

impl RoTxn<'_> {
    /// The snapshot txnid this read txn observes (SPEC 04 TXN-4).
    #[must_use]
    pub fn txnid(&self) -> u64 {
        self.snap.txnid
    }
}

impl Drop for RoTxn<'_> {
    fn drop(&mut self) {
        // Interim reader registry (ADR-0005 OQ1, SPEC 04 TXN-21 as amended):
        // release this reader's pin so the GC gate can advance. Replaced by the
        // M1.8 reader-table slot release.
        self.env.inner().deregister_reader(self.snap.txnid);
    }
}

impl TxnRead for RoTxn<'_> {
    fn source(&self) -> Source<'_> {
        Source::Map { bytes: self.bytes }
    }
    fn main_record(&self) -> &DBRecord {
        &self.snap.main_db
    }
    fn free_record(&self) -> &DBRecord {
        &self.snap.free_db
    }
    fn page_size(&self) -> u32 {
        self.psize
    }
}

impl Env {
    /// Open a read transaction over the live published snapshot (SPEC 00
    /// row 13, SPEC 04 TXN-10). Pins the snapshot txnid in the interim reader
    /// registry (ADR-0005 OQ1) so GC never reclaims a page this reader can
    /// reach; the M1.8 reader table replaces the registry, not this API.
    ///
    /// # Errors
    ///
    /// Infallible pre-M1.8 (no slot to claim, no I/O); returns [`Result`] to
    /// match the heed shape and the future M1.8 slot-claim failure mode.
    pub fn read_txn(&self) -> Result<RoTxn<'_>> {
        // Clone-and-pin is atomic under the registry mutex (see
        // `EnvInner::pin_reader` for the race-freedom argument), so the GC
        // gate can never miss this reader while it holds a reachable page.
        let snap = self.inner().pin_reader();
        Ok(RoTxn {
            bytes: self.inner().backing_bytes(),
            psize: self.page_size(),
            snap,
            env: self,
        })
    }

    /// A handle to the main (unnamed) database (SPEC 00 row 10, `None` name).
    /// Always present — it is the meta's `main_db`. Named DBs are M1.6.
    #[must_use]
    pub fn main_database(&self) -> Database {
        Database { _priv: () }
    }

    /// `non_free_pages_size()` (SPEC 00 row 19 — MUST; SPEC 05 GC-23/GC-24):
    /// `real_disk_size() − free_page_count() * psize`, where the free-page
    /// count is the exact sum of every GC entry's PIL count under a fresh read
    /// snapshot. This is the native replacement for milli reading LMDB's
    /// freelist; it drives the `> 0.75 * map_size` auto-resize trigger.
    ///
    /// **TOCTOU note (GC-24):** the free count is exact *for the snapshot*,
    /// but the `fstat` length is sampled independently and can only be
    /// **larger** (a concurrent writer may extend the file; nothing ever
    /// truncates it in Phase 1). The result may therefore over-report
    /// non-free bytes by at most the concurrent growth — monotone-conservative
    /// for milli's resize trigger (it can only fire *earlier* than the exact
    /// value would, never later), and exact whenever no writer commits during
    /// the call. GC-24's precision claim is per-snapshot and holds.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] from `fstat`; [`MdbError::Invalid`] on a corrupt GC DB.
    pub fn non_free_pages_size(&self) -> Result<u64> {
        let rtxn = self.read_txn()?;
        let free_pages = free_page_count(&rtxn)?;
        let disk = self.inner().real_disk_size()?;
        Ok(disk.saturating_sub(free_pages * u64::from(self.page_size())))
    }
}

/// Sum of every GC entry's PIL `count` prefix reachable from the txn's
/// `free_db` root (SPEC 05 GC-23). Overflow-spilled PILs are read through
/// their run like any large value (the cursor already resolves them).
///
/// # Errors
///
/// [`MdbError::Invalid`] on a corrupt GC tree or a malformed PIL.
pub fn free_page_count<T: TxnRead>(txn: &T) -> Result<u64> {
    let rec = txn.free_record();
    let tree = Tree::new(txn.source(), txn.page_size(), rec.root, rec.depth);
    let mut cursor = tree.cursor();
    let mut total = 0u64;
    let mut entry = cursor.first().map_err(map_page_err)?;
    while let Some((_key, val)) = entry {
        // GC-3 shape validation via the shared PIL codec (a torn PIL errors
        // rather than silently mis-counting — INV-26's runtime cousin).
        let ids = crate::page::geometry::pil_decode(val).ok_or(Error::Mdb(MdbError::Invalid))?;
        total += ids.len() as u64;
        entry = cursor.next().map_err(map_page_err)?;
    }
    Ok(total)
}

/// A database handle. In Phase-1-so-far this only ever names the main/unnamed
/// database; its root/stats come from the transaction passed to each method.
///
/// **Key-size validation is intentionally lenient on the read side.** Read
/// methods are pure tree searches: an empty or oversized key is not rejected,
/// it simply matches nothing. LMDB's heed-observed read-key error taxonomy
/// (SPEC 03 §2.1) is a heed-API-boundary concern applied by the caller (the
/// oracle adapter today; `heed-zerodb` at M1.13). **Write** methods do
/// validate (SPEC 03 §6: `put*` rejects an empty or `> 511`-byte key and an
/// oversized value with `BadValSize`) — the split machinery's termination
/// proof relies on the key bound.
#[derive(Debug, Clone, Copy)]
pub struct Database {
    _priv: (),
}

/// The main DB's tree view over any readable txn (shared by the read API and
/// the write cursor).
pub(crate) fn tree_of<T: TxnRead + ?Sized>(txn: &T) -> Tree<'_> {
    let rec = txn.main_record();
    Tree::new(txn.source(), txn.page_size(), rec.root, rec.depth)
}

impl Database {
    pub(crate) fn tree<'txn, T: TxnRead + ?Sized>(&self, txn: &'txn T) -> Tree<'txn> {
        tree_of(txn)
    }

    /// `get(txn, key)` (SPEC 00 row 30): the value for `key`, or `Ok(None)` if
    /// absent — never an error for a missing key.
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] only on a structurally-corrupt tree.
    pub fn get<'txn, T: TxnRead>(&self, txn: &'txn T, key: &[u8]) -> Result<Option<&'txn [u8]>> {
        self.tree(txn).get(key).map_err(map_page_err)
    }

    /// `len(txn)` — entry count (SPEC 00 row 39), read from the snapshot's
    /// `DBRecord`.
    ///
    /// # Errors
    ///
    /// Infallible; returns [`Result`] for API shape.
    pub fn len<T: TxnRead>(&self, txn: &T) -> Result<u64> {
        Ok(txn.main_record().entries)
    }

    /// `is_empty(txn)` (SPEC 00 row 40).
    ///
    /// # Errors
    ///
    /// Infallible; returns [`Result`] for API shape.
    pub fn is_empty<T: TxnRead>(&self, txn: &T) -> Result<bool> {
        Ok(txn.main_record().entries == 0)
    }

    /// `first(txn)` — the minimum entry (SPEC 00 row 41).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn first<'txn, T: TxnRead>(
        &self,
        txn: &'txn T,
    ) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        self.tree(txn).cursor().first().map_err(map_page_err)
    }

    /// `last(txn)` — the maximum entry (SPEC 00 row 42).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn last<'txn, T: TxnRead>(&self, txn: &'txn T) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        self.tree(txn).cursor().last().map_err(map_page_err)
    }

    /// `get_greater_than_or_equal_to(txn, key)` — first entry `≥ key`
    /// (SPEC 00 SHOULD; the `set_range` primitive).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn get_greater_than_or_equal_to<'txn, T: TxnRead>(
        &self,
        txn: &'txn T,
        key: &[u8],
    ) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        self.tree(txn).cursor().set_range(key).map_err(map_page_err)
    }

    /// `get_greater_than(txn, key)` — first entry `> key` (SPEC 00 row 47).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn get_greater_than<'txn, T: TxnRead>(
        &self,
        txn: &'txn T,
        key: &[u8],
    ) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        self.tree(txn)
            .cursor()
            .get_greater_than(key)
            .map_err(map_page_err)
    }

    /// `get_lower_than_or_equal_to(txn, key)` — last entry `≤ key`
    /// (SPEC 00 row 48).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn get_lower_than_or_equal_to<'txn, T: TxnRead>(
        &self,
        txn: &'txn T,
        key: &[u8],
    ) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        self.tree(txn)
            .cursor()
            .get_lower_than_or_equal_to(key)
            .map_err(map_page_err)
    }

    /// `get_lower_than(txn, key)` — last entry `< key` (SPEC 00 SHOULD).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn get_lower_than<'txn, T: TxnRead>(
        &self,
        txn: &'txn T,
        key: &[u8],
    ) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        self.tree(txn)
            .cursor()
            .get_lower_than(key)
            .map_err(map_page_err)
    }

    /// `iter(txn)` — forward full scan (SPEC 00 row 43).
    #[must_use]
    pub fn iter<'txn, T: TxnRead>(&self, txn: &'txn T) -> RoRange<'txn> {
        self.range_impl(txn, Dir::Fwd, Bound::Unbounded, Bound::Unbounded)
    }

    /// `rev_iter(txn)` — reverse full scan (SPEC 00 SHOULD).
    #[must_use]
    pub fn rev_iter<'txn, T: TxnRead>(&self, txn: &'txn T) -> RoRange<'txn> {
        self.range_impl(txn, Dir::Rev, Bound::Unbounded, Bound::Unbounded)
    }

    /// `range(txn, lower, upper)` — forward scan over the bound pair
    /// (SPEC 00 row 44). Any `Bound` combination is supported.
    #[must_use]
    pub fn range<'txn, T: TxnRead>(
        &self,
        txn: &'txn T,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> RoRange<'txn> {
        self.range_impl(txn, Dir::Fwd, own(lower), own(upper))
    }

    /// `rev_range(txn, lower, upper)` — reverse scan over the bound pair
    /// (SPEC 00 row 44).
    #[must_use]
    pub fn rev_range<'txn, T: TxnRead>(
        &self,
        txn: &'txn T,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> RoRange<'txn> {
        self.range_impl(txn, Dir::Rev, own(lower), own(upper))
    }

    /// `prefix_iter(txn, prefix)` — forward scan of all keys with `prefix`
    /// (SPEC 00 row 45), realized as the range `[prefix, prefix_successor)`
    /// (SPEC 03 §4; the all-`0xFF` edge yields an unbounded upper).
    #[must_use]
    pub fn prefix_iter<'txn, T: TxnRead>(&self, txn: &'txn T, prefix: &[u8]) -> RoRange<'txn> {
        let (lo, hi) = prefix_bounds(prefix);
        self.range_impl(txn, Dir::Fwd, lo, hi)
    }

    /// `rev_prefix_iter(txn, prefix)` — reverse prefix scan (SPEC 00 row 46).
    #[must_use]
    pub fn rev_prefix_iter<'txn, T: TxnRead>(&self, txn: &'txn T, prefix: &[u8]) -> RoRange<'txn> {
        let (lo, hi) = prefix_bounds(prefix);
        self.range_impl(txn, Dir::Rev, lo, hi)
    }

    fn range_impl<'txn, T: TxnRead>(
        &self,
        txn: &'txn T,
        dir: Dir,
        lo: Bound<Vec<u8>>,
        hi: Bound<Vec<u8>>,
    ) -> RoRange<'txn> {
        RoRange {
            cursor: self.tree(txn).cursor(),
            dir,
            lo,
            hi,
            started: false,
            done: false,
        }
    }
}

fn own(b: Bound<&[u8]>) -> Bound<Vec<u8>> {
    match b {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(s) => Bound::Included(s.to_vec()),
        Bound::Excluded(s) => Bound::Excluded(s.to_vec()),
    }
}

/// The `[prefix, prefix_successor(prefix))` bounds for a prefix scan (SPEC 03
/// §4). An all-`0xFF` (or empty) prefix has no successor → unbounded upper.
fn prefix_bounds(prefix: &[u8]) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let lo = Bound::Included(prefix.to_vec());
    let hi = match prefix_successor(prefix) {
        Some(s) => Bound::Excluded(s),
        None => Bound::Unbounded,
    };
    (lo, hi)
}

#[derive(Debug, Clone, Copy)]
enum Dir {
    Fwd,
    Rev,
}

/// A lazy, zero-copy range/prefix iterator (SPEC 00 rows 43–46). Yields
/// `Result<(&'txn [u8], &'txn [u8])>`; the borrowed slices live for the txn
/// borrow, not for the `&mut` `next` call, so they can outlive iteration.
/// Works over both txn kinds ([`TxnRead`]); on a write txn the shared `&RwTxn`
/// borrow it holds forbids any mutation while it is alive (SPEC 04 TXN-39).
pub struct RoRange<'txn> {
    cursor: Cursor<'txn>,
    dir: Dir,
    lo: Bound<Vec<u8>>,
    hi: Bound<Vec<u8>>,
    started: bool,
    done: bool,
}

impl<'txn> RoRange<'txn> {
    fn seek_start(&mut self) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let r = match self.dir {
            Dir::Fwd => match &self.lo {
                Bound::Unbounded => self.cursor.first(),
                Bound::Included(l) => self.cursor.set_range(l),
                Bound::Excluded(l) => self.cursor.get_greater_than(l),
            },
            Dir::Rev => match &self.hi {
                Bound::Unbounded => self.cursor.last(),
                Bound::Included(h) => self.cursor.get_lower_than_or_equal_to(h),
                Bound::Excluded(h) => self.cursor.get_lower_than(h),
            },
        };
        r.map_err(map_page_err)
    }

    /// Whether `key` is within the bound that *terminates* iteration in this
    /// direction (the upper bound going forward, the lower bound going reverse).
    fn in_bounds(&self, key: &[u8]) -> bool {
        match self.dir {
            Dir::Fwd => match &self.hi {
                Bound::Unbounded => true,
                Bound::Included(h) => key <= h.as_slice(),
                Bound::Excluded(h) => key < h.as_slice(),
            },
            Dir::Rev => match &self.lo {
                Bound::Unbounded => true,
                Bound::Included(l) => key >= l.as_slice(),
                Bound::Excluded(l) => key > l.as_slice(),
            },
        }
    }
}

impl<'txn> Iterator for RoRange<'txn> {
    type Item = Result<(&'txn [u8], &'txn [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let step = if !self.started {
            self.started = true;
            self.seek_start()
        } else {
            match self.dir {
                Dir::Fwd => self.cursor.next().map_err(map_page_err),
                Dir::Rev => self.cursor.prev().map_err(map_page_err),
            }
        };
        match step {
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
            Ok(None) => {
                self.done = true;
                None
            }
            Ok(Some((k, v))) => {
                if self.in_bounds(k) {
                    Some(Ok((k, v)))
                } else {
                    self.done = true;
                    None
                }
            }
        }
    }
}
