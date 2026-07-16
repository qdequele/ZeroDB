//! Read transactions and the heed-shaped read API over the [`btree`] cursor
//! (SPEC 04 §3, SPEC 00 read-op rows). Milestone 1.3.
//!
//! A [`RoTxn`] is, in M1.3, **simply a handle pinning the env's live meta roots
//! at open** — it copies the `main_db` [`DBRecord`] out of the selected meta and
//! borrows the mapped bytes for its life (SPEC 04 TXN-37). There is no
//! reclamation to defend against yet, so no reader-table slot is claimed: that
//! is the **M1.8 seam**. When the reader table lands, `read_txn` additionally
//! claims a slot and pins the snapshot txnid (SPEC 04 TXN-10); the read API
//! below is unaffected because it already reads only the pinned roots.
//!
//! Named-database catalogs are out of M1.3 scope (they are M1.6): only the
//! main/unnamed database is exposed, via [`Env::main_database`]. Reads are
//! zero-copy `&'txn [u8]` borrows tied to the `&RoTxn`.

use std::ops::Bound;

use crate::btree::{prefix_successor, Cursor, Tree};
use crate::env::Env;
use crate::error::{Error, MdbError, Result};
use crate::page::{DBRecord, PageError};

/// Map a structural tree-decode error to the public taxonomy. A corrupt page
/// reached during a read means the store is not a valid zerodb file
/// (SPEC 00 row 56); for a builder/write-path-produced tree this never fires.
fn map_page_err(_e: PageError) -> Error {
    Error::Mdb(MdbError::Invalid)
}

/// A read-only transaction: a consistent snapshot of the env (SPEC 04 §3). It
/// borrows the [`Env`] for its whole life, which keeps the mapped file alive so
/// every `&'txn [u8]` it lends stays valid (SPEC 04 TXN-37).
pub struct RoTxn<'env> {
    _env: &'env Env,
    bytes: &'env [u8],
    psize: u32,
    main_db: DBRecord,
}

impl<'env> RoTxn<'env> {
    /// The snapshot txnid this read txn observes (the live meta's txnid at open).
    #[must_use]
    pub fn txnid(&self) -> u64 {
        // The pinned snapshot is the env's live meta; M1.8 will pin it in a
        // reader slot. Until then it cannot change (no write path).
        self._env.txnid()
    }
}

impl Env {
    /// Open a read transaction over the live snapshot (SPEC 00 row 13). See the
    /// module docs for the M1.8 reader-slot seam.
    ///
    /// # Errors
    ///
    /// Infallible in M1.3 (no slot to claim, no I/O); returns [`Result`] to
    /// match the heed shape and the future M1.8 slot-claim failure mode.
    pub fn read_txn(&self) -> Result<RoTxn<'_>> {
        Ok(RoTxn {
            bytes: self.inner().backing_bytes(),
            psize: self.page_size(),
            main_db: self.inner().meta().main_db,
            _env: self,
        })
    }

    /// A handle to the main (unnamed) database (SPEC 00 row 10, `None` name).
    /// Always present — it is the meta's `main_db`. Named DBs are M1.6.
    #[must_use]
    pub fn main_database(&self) -> Database {
        Database { _priv: () }
    }
}

/// A database handle. In M1.3 this only ever names the main/unnamed database;
/// its root/stats come from the [`RoTxn`] snapshot passed to each method.
///
/// **Key-size validation is intentionally lenient here.** These methods are
/// pure tree searches: an empty or oversized key is not rejected, it simply
/// matches nothing. LMDB's heed-observed key-size error taxonomy — `BadValSize`
/// on an empty `get`/seek, an empty forward-prefix, or an empty/oversized write
/// key (SPEC 03 §2.1) — is a heed-API-boundary concern applied by the caller
/// (the oracle adapter today; the `heed-zerodb` adapter at M1.13), not by this
/// core read path.
#[derive(Debug, Clone, Copy)]
pub struct Database {
    _priv: (),
}

impl Database {
    fn tree<'txn>(&self, txn: &RoTxn<'txn>) -> Tree<'txn> {
        Tree::new(txn.bytes, txn.psize, txn.main_db.root, txn.main_db.depth)
    }

    /// `get(txn, key)` (SPEC 00 row 30): the value for `key`, or `Ok(None)` if
    /// absent — never an error for a missing key.
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] only on a structurally-corrupt tree.
    pub fn get<'txn>(&self, txn: &RoTxn<'txn>, key: &[u8]) -> Result<Option<&'txn [u8]>> {
        self.tree(txn).get(key).map_err(map_page_err)
    }

    /// `len(txn)` — entry count (SPEC 00 row 39), read from the snapshot's
    /// `DBRecord`.
    ///
    /// # Errors
    ///
    /// Infallible; returns [`Result`] for API shape.
    pub fn len(&self, txn: &RoTxn<'_>) -> Result<u64> {
        Ok(txn.main_db.entries)
    }

    /// `is_empty(txn)` (SPEC 00 row 40).
    ///
    /// # Errors
    ///
    /// Infallible; returns [`Result`] for API shape.
    pub fn is_empty(&self, txn: &RoTxn<'_>) -> Result<bool> {
        Ok(txn.main_db.entries == 0)
    }

    /// `first(txn)` — the minimum entry (SPEC 00 row 41).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn first<'txn>(&self, txn: &RoTxn<'txn>) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        self.tree(txn).cursor().first().map_err(map_page_err)
    }

    /// `last(txn)` — the maximum entry (SPEC 00 row 42).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn last<'txn>(&self, txn: &RoTxn<'txn>) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        self.tree(txn).cursor().last().map_err(map_page_err)
    }

    /// `get_greater_than_or_equal_to(txn, key)` — first entry `≥ key`
    /// (SPEC 00 SHOULD; the `set_range` primitive).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn get_greater_than_or_equal_to<'txn>(
        &self,
        txn: &RoTxn<'txn>,
        key: &[u8],
    ) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        self.tree(txn).cursor().set_range(key).map_err(map_page_err)
    }

    /// `get_greater_than(txn, key)` — first entry `> key` (SPEC 00 row 47).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn get_greater_than<'txn>(
        &self,
        txn: &RoTxn<'txn>,
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
    pub fn get_lower_than_or_equal_to<'txn>(
        &self,
        txn: &RoTxn<'txn>,
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
    pub fn get_lower_than<'txn>(
        &self,
        txn: &RoTxn<'txn>,
        key: &[u8],
    ) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        self.tree(txn)
            .cursor()
            .get_lower_than(key)
            .map_err(map_page_err)
    }

    /// `iter(txn)` — forward full scan (SPEC 00 row 43).
    #[must_use]
    pub fn iter<'txn>(&self, txn: &RoTxn<'txn>) -> RoRange<'txn> {
        self.range_impl(txn, Dir::Fwd, Bound::Unbounded, Bound::Unbounded)
    }

    /// `rev_iter(txn)` — reverse full scan (SPEC 00 SHOULD).
    #[must_use]
    pub fn rev_iter<'txn>(&self, txn: &RoTxn<'txn>) -> RoRange<'txn> {
        self.range_impl(txn, Dir::Rev, Bound::Unbounded, Bound::Unbounded)
    }

    /// `range(txn, lower, upper)` — forward scan over the bound pair
    /// (SPEC 00 row 44). Any `Bound` combination is supported.
    #[must_use]
    pub fn range<'txn>(
        &self,
        txn: &RoTxn<'txn>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> RoRange<'txn> {
        self.range_impl(txn, Dir::Fwd, own(lower), own(upper))
    }

    /// `rev_range(txn, lower, upper)` — reverse scan over the bound pair
    /// (SPEC 00 row 44).
    #[must_use]
    pub fn rev_range<'txn>(
        &self,
        txn: &RoTxn<'txn>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> RoRange<'txn> {
        self.range_impl(txn, Dir::Rev, own(lower), own(upper))
    }

    /// `prefix_iter(txn, prefix)` — forward scan of all keys with `prefix`
    /// (SPEC 00 row 45), realized as the range `[prefix, prefix_successor)`
    /// (SPEC 03 §4; the all-`0xFF` edge yields an unbounded upper).
    #[must_use]
    pub fn prefix_iter<'txn>(&self, txn: &RoTxn<'txn>, prefix: &[u8]) -> RoRange<'txn> {
        let (lo, hi) = prefix_bounds(prefix);
        self.range_impl(txn, Dir::Fwd, lo, hi)
    }

    /// `rev_prefix_iter(txn, prefix)` — reverse prefix scan (SPEC 00 row 46).
    #[must_use]
    pub fn rev_prefix_iter<'txn>(&self, txn: &RoTxn<'txn>, prefix: &[u8]) -> RoRange<'txn> {
        let (lo, hi) = prefix_bounds(prefix);
        self.range_impl(txn, Dir::Rev, lo, hi)
    }

    fn range_impl<'txn>(
        &self,
        txn: &RoTxn<'txn>,
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
/// `Result<(&'txn [u8], &'txn [u8])>`; the borrowed slices live for the txn, not
/// for the `&mut` `next` call, so they can outlive iteration.
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
