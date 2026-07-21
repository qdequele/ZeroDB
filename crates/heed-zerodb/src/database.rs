//! `Database<KC, DC, C, CDUP>`, `DatabaseOpenOptions`, and `DatabaseStat`
//! (SPEC 00 rows 10–12, 30–51). Typed over the `heed-traits` codecs, byte-level
//! over ZeroDB. Read methods dispatch across the three txn sources
//! ([`with_read!`]); write methods target `&mut RwTxn`.

use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};

use heed_traits::{BytesDecode, BytesEncode, Comparator, LexicographicComparator};
use heed_types::LazyDecode;

use crate::env::{DefaultComparator, Env};
use crate::flags::{DatabaseFlags, PutFlags};
use crate::iterator::{
    Dir, RoIter, RoPrefix, RoRange, RoRevIter, RoRevPrefix, RoRevRange, RwGuts, RwIter, RwPrefix,
    RwRange, RwRevIter, RwRevPrefix, RwRevRange,
};
use crate::reserved_space::ReservedSpace;
use crate::txn::{RoTxn, RwTxn, WithTls};
use crate::{Error, MdbError, Result, Unspecified};

/// Statistics for a database (SPEC 00 rows 49/60; `mdb_stat`).
#[derive(Debug, Clone, Copy)]
pub struct DatabaseStat {
    /// Size of a database page.
    pub page_size: u32,
    /// Depth (height) of the B-tree.
    pub depth: u32,
    /// Number of internal (non-leaf) pages.
    pub branch_pages: usize,
    /// Number of leaf pages.
    pub leaf_pages: usize,
    /// Number of overflow pages.
    pub overflow_pages: usize,
    /// Number of data items.
    pub entries: usize,
}

/// Map heed `PutFlags` → ZeroDB `PutFlags` (SPEC 01 Table 3). Dup flags are
/// dropped (no DUPSORT in Phase 1, D-004).
pub(crate) fn to_zdb_put_flags(f: PutFlags) -> zerodb::PutFlags {
    let mut z = zerodb::PutFlags::EMPTY;
    if f.contains(PutFlags::APPEND) {
        z = z | zerodb::PutFlags::APPEND;
    }
    if f.contains(PutFlags::NO_OVERWRITE) {
        z = z | zerodb::PutFlags::NO_OVERWRITE;
    }
    z
}

/// LMDB read-key size validation, re-imposed at the heed boundary (SPEC 03
/// §2.1; the taxonomy the native engine's `bad_read_key` models, which SPEC
/// notes "the caller applies … heed-zerodb at M1.13"). An **empty** key errors
/// with `BadValSize` on `get`/`del`/neighbor-seeks/forward-prefix; an oversized
/// key is *not* rejected (the search simply finds nothing). No consumer sends an
/// empty key, so this only affects exact LMDB parity.
fn check_read_key(kb: &[u8]) -> Result<()> {
    if kb.is_empty() {
        Err(Error::Mdb(MdbError::BadValSize))
    } else {
        Ok(())
    }
}

/// Dispatch a read closure over the three txn sources (SPEC 04 §3–§5). Each arm
/// monomorphizes over the concrete `zerodb` txn type (all `TxnRead`).
macro_rules! with_read {
    ($txn:expr, |$t:ident| $e:expr) => {
        match &$txn.inner {
            $crate::txn::InnerTxn::Ro($t) => $e,
            $crate::txn::InnerTxn::Nested($t) => $e,
            $crate::txn::InnerTxn::Rw($t) => $e,
        }
    };
}

fn as_bound(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
    }
}

/// An owned lower/upper bound pair (encoded key bytes).
type OwnedBounds = (Bound<Vec<u8>>, Bound<Vec<u8>>);

fn encode_bounds<'a, KC, R>(range: &'a R) -> Result<OwnedBounds>
where
    KC: BytesEncode<'a>,
    R: RangeBounds<KC::EItem>,
{
    let enc = |b: Bound<&'a KC::EItem>| -> Result<Bound<Vec<u8>>> {
        Ok(match b {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(k) => {
                Bound::Included(KC::bytes_encode(k).map_err(Error::Encoding)?.into_owned())
            }
            Bound::Excluded(k) => {
                Bound::Excluded(KC::bytes_encode(k).map_err(Error::Encoding)?.into_owned())
            }
        })
    };
    Ok((enc(range.start_bound())?, enc(range.end_bound())?))
}

/// The `[prefix, prefix_successor)` bounds for a prefix scan (SPEC 03 §4).
fn prefix_bounds(prefix: &[u8]) -> OwnedBounds {
    let mut succ = prefix.to_vec();
    let upper = loop {
        match succ.last_mut() {
            None => break Bound::Unbounded,
            Some(b) if *b < 0xFF => {
                *b += 1;
                break Bound::Excluded(succ);
            }
            Some(_) => {
                succ.pop();
            }
        }
    };
    (Bound::Included(prefix.to_vec()), upper)
}

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

/// A typed database handle (SPEC 00 rows 30–51). `Copy`, like `heed::Database`.
pub struct Database<KC, DC, C = DefaultComparator, CDUP = DefaultComparator> {
    pub(crate) inner: zerodb::Database,
    pub(crate) page_size: u32,
    marker: PhantomData<(KC, DC, C, CDUP)>,
}

impl<KC, DC, C, CDUP> Clone for Database<KC, DC, C, CDUP> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<KC, DC, C, CDUP> Copy for Database<KC, DC, C, CDUP> {}

impl<KC, DC, C, CDUP> std::fmt::Debug for Database<KC, DC, C, CDUP> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Database").finish()
    }
}

impl<KC, DC, C, CDUP> Database<KC, DC, C, CDUP> {
    pub(crate) fn new(inner: zerodb::Database, page_size: u32) -> Database<KC, DC, C, CDUP> {
        Database {
            inner,
            page_size,
            marker: PhantomData,
        }
    }

    /// `get(txn, key)` (SPEC 00 row 30): the value for `key`, or `None`.
    ///
    /// # Errors
    ///
    /// `Encoding`/`Decoding` on codec failure; `Mdb(Invalid)` on corruption.
    pub fn get<'a, 'txn>(&self, txn: &'txn RoTxn, key: &'a KC::EItem) -> Result<Option<DC::DItem>>
    where
        KC: BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        let kb = KC::bytes_encode(key).map_err(Error::Encoding)?;
        check_read_key(&kb)?;
        let raw = with_read!(txn, |t| self.inner.get(t, &kb))?;
        match raw {
            Some(bytes) => Ok(Some(DC::bytes_decode(bytes).map_err(Error::Decoding)?)),
            None => Ok(None),
        }
    }

    /// `len(txn)` (SPEC 00 row 39).
    ///
    /// # Errors
    ///
    /// `Mdb(Invalid)` on corruption.
    pub fn len(&self, txn: &RoTxn) -> Result<u64> {
        Ok(with_read!(txn, |t| self.inner.len(t))?)
    }

    /// `is_empty(txn)` (SPEC 00 row 40).
    ///
    /// # Errors
    ///
    /// `Mdb(Invalid)` on corruption.
    pub fn is_empty(&self, txn: &RoTxn) -> Result<bool> {
        Ok(with_read!(txn, |t| self.inner.is_empty(t))?)
    }

    /// `stat(txn)` (SPEC 00 row 49).
    ///
    /// # Errors
    ///
    /// `Mdb(Invalid)` on corruption.
    pub fn stat(&self, txn: &RoTxn) -> Result<DatabaseStat> {
        let s = with_read!(txn, |t| self.inner.stat(t))?;
        Ok(DatabaseStat {
            page_size: self.page_size,
            depth: u32::from(s.depth),
            branch_pages: s.branch_pages as usize,
            leaf_pages: s.leaf_pages as usize,
            overflow_pages: s.overflow_pages as usize,
            entries: s.entries as usize,
        })
    }

    /// `first(txn)` (SPEC 00 row 41).
    ///
    /// # Errors
    ///
    /// `Decoding` / `Mdb(Invalid)`.
    pub fn first<'txn>(&self, txn: &'txn RoTxn) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        decode_opt::<KC, DC>(with_read!(txn, |t| self.inner.first(t))?)
    }

    /// `last(txn)` (SPEC 00 row 42).
    ///
    /// # Errors
    ///
    /// `Decoding` / `Mdb(Invalid)`.
    pub fn last<'txn>(&self, txn: &'txn RoTxn) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        decode_opt::<KC, DC>(with_read!(txn, |t| self.inner.last(t))?)
    }

    /// `get_lower_than(txn, key)` — last entry `< key` (SPEC 00 second table).
    ///
    /// # Errors
    ///
    /// `Encoding` / `Decoding` / `Mdb(Invalid)`.
    pub fn get_lower_than<'a, 'txn>(
        &self,
        txn: &'txn RoTxn,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        let kb = KC::bytes_encode(key).map_err(Error::Encoding)?;
        check_read_key(&kb)?;
        decode_opt::<KC, DC>(with_read!(txn, |t| self.inner.get_lower_than(t, &kb))?)
    }

    /// `get_lower_than_or_equal_to(txn, key)` — last entry `<= key`
    /// (SPEC 00 row 48).
    ///
    /// # Errors
    ///
    /// `Encoding` / `Decoding` / `Mdb(Invalid)`.
    pub fn get_lower_than_or_equal_to<'a, 'txn>(
        &self,
        txn: &'txn RoTxn,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        let kb = KC::bytes_encode(key).map_err(Error::Encoding)?;
        check_read_key(&kb)?;
        decode_opt::<KC, DC>(with_read!(txn, |t| self
            .inner
            .get_lower_than_or_equal_to(t, &kb))?)
    }

    /// `get_greater_than(txn, key)` — first entry `> key` (SPEC 00 row 47).
    ///
    /// # Errors
    ///
    /// `Encoding` / `Decoding` / `Mdb(Invalid)`.
    pub fn get_greater_than<'a, 'txn>(
        &self,
        txn: &'txn RoTxn,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        let kb = KC::bytes_encode(key).map_err(Error::Encoding)?;
        check_read_key(&kb)?;
        decode_opt::<KC, DC>(with_read!(txn, |t| self.inner.get_greater_than(t, &kb))?)
    }

    /// `get_greater_than_or_equal_to(txn, key)` — first entry `>= key`
    /// (SPEC 00 second table; the `set_range` primitive).
    ///
    /// # Errors
    ///
    /// `Encoding` / `Decoding` / `Mdb(Invalid)`.
    pub fn get_greater_than_or_equal_to<'a, 'txn>(
        &self,
        txn: &'txn RoTxn,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        let kb = KC::bytes_encode(key).map_err(Error::Encoding)?;
        check_read_key(&kb)?;
        decode_opt::<KC, DC>(with_read!(txn, |t| self
            .inner
            .get_greater_than_or_equal_to(t, &kb))?)
    }

    // -- read iterators ------------------------------------------------------

    /// `iter(txn)` (SPEC 00 row 43).
    ///
    /// # Errors
    ///
    /// Infallible construction; returns [`Result`] for heed shape.
    pub fn iter<'txn>(&self, txn: &'txn RoTxn) -> Result<RoIter<'txn, KC, DC>> {
        Ok(RoIter::new(with_read!(txn, |t| self.inner.iter(t))))
    }

    /// `rev_iter(txn)` (SPEC 00 second table).
    ///
    /// # Errors
    ///
    /// As [`Database::iter`].
    pub fn rev_iter<'txn>(&self, txn: &'txn RoTxn) -> Result<RoRevIter<'txn, KC, DC>> {
        Ok(RoRevIter::new(with_read!(txn, |t| self.inner.rev_iter(t))))
    }

    /// `range(txn, range)` (SPEC 00 row 44).
    ///
    /// # Errors
    ///
    /// `Encoding` on a bound codec failure.
    pub fn range<'a, 'txn, R>(
        &self,
        txn: &'txn RoTxn,
        range: &'a R,
    ) -> Result<RoRange<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
        R: RangeBounds<KC::EItem>,
    {
        let (lo, hi) = encode_bounds::<KC, R>(range)?;
        let (lo, hi) = (as_bound(&lo), as_bound(&hi));
        Ok(RoRange::new(with_read!(txn, |t| self
            .inner
            .range(t, lo, hi))))
    }

    /// `rev_range(txn, range)` (SPEC 00 row 44).
    ///
    /// # Errors
    ///
    /// As [`Database::range`].
    pub fn rev_range<'a, 'txn, R>(
        &self,
        txn: &'txn RoTxn,
        range: &'a R,
    ) -> Result<RoRevRange<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
        R: RangeBounds<KC::EItem>,
    {
        let (lo, hi) = encode_bounds::<KC, R>(range)?;
        let (lo, hi) = (as_bound(&lo), as_bound(&hi));
        Ok(RoRevRange::new(with_read!(txn, |t| self
            .inner
            .rev_range(t, lo, hi))))
    }

    /// `prefix_iter(txn, prefix)` (SPEC 00 row 45).
    ///
    /// # Errors
    ///
    /// `Encoding` on a prefix codec failure.
    pub fn prefix_iter<'a, 'txn>(
        &self,
        txn: &'txn RoTxn,
        prefix: &'a KC::EItem,
    ) -> Result<RoPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
        C: LexicographicComparator,
    {
        let pb = KC::bytes_encode(prefix).map_err(Error::Encoding)?;
        check_read_key(&pb)?;
        Ok(RoPrefix::new(with_read!(txn, |t| self
            .inner
            .prefix_iter(t, &pb))))
    }

    /// `rev_prefix_iter(txn, prefix)` (SPEC 00 row 46).
    ///
    /// # Errors
    ///
    /// As [`Database::prefix_iter`].
    pub fn rev_prefix_iter<'a, 'txn>(
        &self,
        txn: &'txn RoTxn,
        prefix: &'a KC::EItem,
    ) -> Result<RoRevPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
        C: LexicographicComparator,
    {
        let pb = KC::bytes_encode(prefix).map_err(Error::Encoding)?;
        Ok(RoRevPrefix::new(with_read!(txn, |t| self
            .inner
            .rev_prefix_iter(t, &pb))))
    }

    // -- write iterators -----------------------------------------------------

    /// `iter_mut(txn)` (SPEC 00 row 43).
    ///
    /// # Errors
    ///
    /// Infallible construction; returns [`Result`] for heed shape.
    pub fn iter_mut<'txn>(&self, txn: &'txn mut RwTxn) -> Result<RwIter<'txn, KC, DC>> {
        Ok(RwIter::new(RwGuts::new(
            txn,
            self.inner,
            Dir::Fwd,
            Bound::Unbounded,
            Bound::Unbounded,
        )))
    }

    /// `rev_iter_mut(txn)` (SPEC 00 second table).
    ///
    /// # Errors
    ///
    /// As [`Database::iter_mut`].
    pub fn rev_iter_mut<'txn>(&self, txn: &'txn mut RwTxn) -> Result<RwRevIter<'txn, KC, DC>> {
        Ok(RwRevIter::new(RwGuts::new(
            txn,
            self.inner,
            Dir::Rev,
            Bound::Unbounded,
            Bound::Unbounded,
        )))
    }

    /// `range_mut(txn, range)` (SPEC 00 second table).
    ///
    /// # Errors
    ///
    /// `Encoding` on a bound codec failure.
    pub fn range_mut<'a, 'txn, R>(
        &self,
        txn: &'txn mut RwTxn,
        range: &'a R,
    ) -> Result<RwRange<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
        R: RangeBounds<KC::EItem>,
    {
        let (lo, hi) = encode_bounds::<KC, R>(range)?;
        Ok(RwRange::new(RwGuts::new(txn, self.inner, Dir::Fwd, lo, hi)))
    }

    /// `rev_range_mut(txn, range)` (SPEC 00 second table).
    ///
    /// # Errors
    ///
    /// As [`Database::range_mut`].
    pub fn rev_range_mut<'a, 'txn, R>(
        &self,
        txn: &'txn mut RwTxn,
        range: &'a R,
    ) -> Result<RwRevRange<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
        R: RangeBounds<KC::EItem>,
    {
        let (lo, hi) = encode_bounds::<KC, R>(range)?;
        Ok(RwRevRange::new(RwGuts::new(
            txn,
            self.inner,
            Dir::Rev,
            lo,
            hi,
        )))
    }

    /// `prefix_iter_mut(txn, prefix)` (SPEC 00 row 45).
    ///
    /// # Errors
    ///
    /// `Encoding` on a prefix codec failure.
    pub fn prefix_iter_mut<'a, 'txn>(
        &self,
        txn: &'txn mut RwTxn,
        prefix: &'a KC::EItem,
    ) -> Result<RwPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
        C: LexicographicComparator,
    {
        let pb = KC::bytes_encode(prefix).map_err(Error::Encoding)?;
        let (lo, hi) = prefix_bounds(&pb);
        Ok(RwPrefix::new(RwGuts::new(
            txn,
            self.inner,
            Dir::Fwd,
            lo,
            hi,
        )))
    }

    /// `rev_prefix_iter_mut(txn, prefix)` (SPEC 00 second table).
    ///
    /// # Errors
    ///
    /// As [`Database::prefix_iter_mut`].
    pub fn rev_prefix_iter_mut<'a, 'txn>(
        &self,
        txn: &'txn mut RwTxn,
        prefix: &'a KC::EItem,
    ) -> Result<RwRevPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
        C: LexicographicComparator,
    {
        let pb = KC::bytes_encode(prefix).map_err(Error::Encoding)?;
        let (lo, hi) = prefix_bounds(&pb);
        Ok(RwRevPrefix::new(RwGuts::new(
            txn,
            self.inner,
            Dir::Rev,
            lo,
            hi,
        )))
    }

    // -- writes --------------------------------------------------------------

    /// `put(txn, key, data)` (SPEC 00 row 31).
    ///
    /// # Errors
    ///
    /// `Encoding`; `Mdb` (`BadValSize`/`MapFull`/`BadTxn`).
    pub fn put<'a>(&self, txn: &mut RwTxn, key: &'a KC::EItem, data: &'a DC::EItem) -> Result<()>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let kb = KC::bytes_encode(key).map_err(Error::Encoding)?;
        let vb = DC::bytes_encode(data).map_err(Error::Encoding)?;
        self.inner.put(txn.zdb_mut(), &kb, &vb).map_err(Into::into)
    }

    /// `put_with_flags(txn, flags, key, data)` (SPEC 00 row 32).
    ///
    /// # Errors
    ///
    /// As [`Database::put`], plus `Mdb(KeyExist)` for `APPEND`/`NO_OVERWRITE`.
    pub fn put_with_flags<'a>(
        &self,
        txn: &mut RwTxn,
        flags: PutFlags,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<()>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let kb = KC::bytes_encode(key).map_err(Error::Encoding)?;
        let vb = DC::bytes_encode(data).map_err(Error::Encoding)?;
        self.inner
            .put_with_flags(txn.zdb_mut(), to_zdb_put_flags(flags), &kb, &vb)
            .map_err(Into::into)
    }

    /// `put_reserved(txn, key, size, f)` (SPEC 00 row 35, `MDB_RESERVE`).
    ///
    /// # Errors
    ///
    /// As [`Database::put`]; `Encoding` if `f` returns an error.
    pub fn put_reserved<'a, F>(
        &self,
        txn: &mut RwTxn,
        key: &'a KC::EItem,
        data_size: usize,
        write_func: F,
    ) -> Result<()>
    where
        KC: BytesEncode<'a>,
        F: FnOnce(&mut ReservedSpace) -> std::io::Result<()>,
    {
        let kb = KC::bytes_encode(key).map_err(Error::Encoding)?;
        // PERF-GAP B6 (2026-07-21): hand the caller the engine's in-frame slot
        // directly (the `MDB_RESERVE` shape) instead of a zeroed heap buffer
        // copied in afterwards — one alloc + one full copy per reserved put
        // gone. Two deliberate semantics, both fork-pinned by the oracle's
        // `put_reserved_failing_closure_leaves_entry_parity`:
        //  - the engine reserves the slot BEFORE the closure runs, so a
        //    closure error leaves the entry in place (LMDB cannot un-put a
        //    reserve either) while the error still propagates as `Io` (the
        //    fork's variant; pre-B6 this adapter returned `Encoding` and no
        //    entry — a real divergence);
        //  - the slot may carry stale frame bytes (a COWed page's old cell
        //    heap), so the unwritten tail is zeroed either way, preserving
        //    the shipped zero-tail contract of the old heap buffer.
        let mut werr: Option<std::io::Error> = None;
        self.inner
            .put_reserved(txn.zdb_mut(), &kb, data_size, |slot| {
                let mut space = ReservedSpace::new(slot);
                if let Err(e) = write_func(&mut space) {
                    werr = Some(e);
                }
                space.zero_unwritten_tail();
            })
            .map_err(Into::<Error>::into)?;
        match werr {
            Some(e) => Err(Error::Io(e)),
            None => Ok(()),
        }
    }

    /// `delete(txn, key)` (SPEC 00 row 36): whether the key existed.
    ///
    /// # Errors
    ///
    /// `Encoding`; `Mdb(BadTxn)` / `Mdb(MapFull)`.
    pub fn delete<'a>(&self, txn: &mut RwTxn, key: &'a KC::EItem) -> Result<bool>
    where
        KC: BytesEncode<'a>,
    {
        let kb = KC::bytes_encode(key).map_err(Error::Encoding)?;
        check_read_key(&kb)?;
        self.inner.delete(txn.zdb_mut(), &kb).map_err(Into::into)
    }

    /// `delete_range(txn, range)` (SPEC 00 row 37): number deleted.
    ///
    /// # Errors
    ///
    /// As [`Database::delete`].
    pub fn delete_range<'a, 'txn, R>(&self, txn: &'txn mut RwTxn, range: &'a R) -> Result<usize>
    where
        KC: BytesEncode<'a>,
        R: RangeBounds<KC::EItem>,
    {
        let (lo, hi) = encode_bounds::<KC, R>(range)?;
        let n = self
            .inner
            .delete_range(txn.zdb_mut(), as_bound(&lo), as_bound(&hi))?;
        Ok(n as usize)
    }

    /// `clear(txn)` (SPEC 00 row 38): empty the DB, keep the handle.
    ///
    /// # Errors
    ///
    /// `Mdb(BadTxn)` / `Mdb(MapFull)`.
    pub fn clear(&self, txn: &mut RwTxn) -> Result<()> {
        self.inner.clear(txn.zdb_mut()).map_err(Into::into)
    }

    /// `remove(txn)` (`mdb_drop(_, 1)`): empty the DB **and** drop the handle
    /// (SPEC 00 second table; used by `delete_one_duplicate`-free consumers via
    /// the drop path). `unsafe` for heed signature parity.
    ///
    /// # Safety
    ///
    /// Mirrors `heed::Database::remove`; the dbi becomes stale afterwards.
    ///
    /// # Errors
    ///
    /// `Mdb(BadTxn)` / `Mdb(MapFull)`.
    pub unsafe fn remove(self, rwtxn: &mut RwTxn) -> Result<()> {
        self.inner.drop_db(rwtxn.zdb_mut()).map_err(Into::into)
    }

    // -- remap ---------------------------------------------------------------

    /// Change the key/data codecs (SPEC 00 row 51). Pure client-side retype.
    #[must_use]
    pub fn remap_types<KC2, DC2>(&self) -> Database<KC2, DC2, C> {
        Database::new(self.inner, self.page_size)
    }

    /// Change the key codec (SPEC 00 row 51).
    #[must_use]
    pub fn remap_key_type<KC2>(&self) -> Database<KC2, DC, C> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec (SPEC 00 row 51).
    #[must_use]
    pub fn remap_data_type<DC2>(&self) -> Database<KC, DC2, C> {
        self.remap_types::<KC, DC2>()
    }

    /// Wrap the data in a lazy decoder (SPEC 00 row 50).
    #[must_use]
    pub fn lazily_decode_data(&self) -> Database<KC, LazyDecode<DC>, C> {
        self.remap_types::<KC, LazyDecode<DC>>()
    }
}

fn decode_opt<'txn, KC, DC>(
    e: Option<(&'txn [u8], &'txn [u8])>,
) -> Result<Option<(KC::DItem, DC::DItem)>>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    match e {
        None => Ok(None),
        Some((k, v)) => {
            let key = KC::bytes_decode(k).map_err(Error::Decoding)?;
            let data = DC::bytes_decode(v).map_err(Error::Decoding)?;
            Ok(Some((key, data)))
        }
    }
}

// ---------------------------------------------------------------------------
// DatabaseOpenOptions
// ---------------------------------------------------------------------------

/// The typed database open-options builder (SPEC 00 row 12).
pub struct DatabaseOpenOptions<'e, 'n, T, KC, DC, C = DefaultComparator, CDUP = DefaultComparator> {
    env: &'e Env<T>,
    name: Option<&'n str>,
    flags: DatabaseFlags,
    marker: PhantomData<(KC, DC, C, CDUP)>,
}

impl<'e, T> DatabaseOpenOptions<'e, 'static, T, Unspecified, Unspecified> {
    /// A fresh builder over `env`.
    pub(crate) fn new(env: &'e Env<T>) -> Self {
        DatabaseOpenOptions {
            env,
            name: None,
            flags: DatabaseFlags::empty(),
            marker: PhantomData,
        }
    }
}

impl<'e, 'n, T, KC, DC, C, CDUP> DatabaseOpenOptions<'e, 'n, T, KC, DC, C, CDUP> {
    /// Change the codec types.
    #[must_use]
    pub fn types<NKC, NDC>(self) -> DatabaseOpenOptions<'e, 'n, T, NKC, NDC, C, CDUP> {
        DatabaseOpenOptions {
            env: self.env,
            name: self.name,
            flags: self.flags,
            marker: PhantomData,
        }
    }

    /// Change the key comparator (SPEC 00 second table — SHOULD; no consumer
    /// sets one).
    #[must_use]
    pub fn key_comparator<NC>(self) -> DatabaseOpenOptions<'e, 'n, T, KC, DC, NC, CDUP> {
        DatabaseOpenOptions {
            env: self.env,
            name: self.name,
            flags: self.flags,
            marker: PhantomData,
        }
    }

    /// Change the dup-sort comparator (SPEC 00 second table — SHOULD).
    #[must_use]
    pub fn dup_sort_comparator<NCDUP>(self) -> DatabaseOpenOptions<'e, 'n, T, KC, DC, C, NCDUP> {
        DatabaseOpenOptions {
            env: self.env,
            name: self.name,
            flags: self.flags,
            marker: PhantomData,
        }
    }

    /// Set the database name.
    pub fn name(&mut self, name: &'n str) -> &mut Self {
        self.name = Some(name);
        self
    }

    /// Set the database flags (SPEC 00 second table; D-004: non-empty rejected
    /// at create).
    pub fn flags(&mut self, flags: DatabaseFlags) -> &mut Self {
        self.flags = flags;
        self
    }
}

/// Convert a heed `&str` name to ZeroDB bytes, re-imposing the fork's C-string
/// rule (D-008 secondary). heed builds the dbi name with
/// `CString::new(name).unwrap()`, so an embedded NUL **panics** — that is the
/// fork's observable behavior (probed against heed =0.22.1's `raw_open_dbi`),
/// which we reproduce exactly rather than inventing a clean error. No consumer
/// ever passes a NUL name, so this never fires in practice.
fn name_bytes(name: Option<&str>) -> Option<&[u8]> {
    if let Some(n) = name {
        // Reproduce heed's `CString::new(n).unwrap()` panic on an embedded NUL.
        let _ = std::ffi::CString::new(n).unwrap();
    }
    name.map(str::as_bytes)
}

impl<'e, 'n, T, KC, DC, C, CDUP> DatabaseOpenOptions<'e, 'n, T, KC, DC, C, CDUP> {
    fn check_flags(&self) -> Result<()> {
        // D-004: no consumer passes any DatabaseFlags; DUPSORT/INTEGER_KEY/etc.
        // are unsupported in Phase 1. Reject a non-empty value cleanly rather
        // than silently ignoring it (documented divergence note, D-004).
        if !self.flags.is_empty() {
            return Err(Error::Mdb(MdbError::Incompatible));
        }
        Ok(())
    }

    /// Open an existing database (SPEC 00 rows 10/12).
    ///
    /// # Errors
    ///
    /// `Mdb`(`BadValSize`/`Incompatible`); `Io` on an embedded-NUL name (D-008).
    pub fn open(&self, rtxn: &RoTxn) -> Result<Option<Database<KC, DC, C, CDUP>>>
    where
        KC: 'static,
        DC: 'static,
        C: Comparator + 'static,
    {
        self.check_flags()?;
        let name = name_bytes(self.name);
        let page_size = self.env.zdb().page_size();
        let z = self.env.zdb();
        let res = match custom_comparator::<C>() {
            None => with_read!(rtxn, |t| z.open_database(t, name))?,
            Some(cmp) => with_read!(rtxn, |t| z.open_database_with_comparator(t, name, cmp))?,
        };
        Ok(res.map(|db| Database::new(db, page_size)))
    }

    /// Create the database if absent (SPEC 00 rows 11/12).
    ///
    /// # Errors
    ///
    /// As [`DatabaseOpenOptions::open`], plus `Mdb(DbsFull)`.
    pub fn create(&self, wtxn: &mut RwTxn) -> Result<Database<KC, DC, C, CDUP>>
    where
        KC: 'static,
        DC: 'static,
        C: Comparator + 'static,
    {
        self.check_flags()?;
        let name = name_bytes(self.name);
        let page_size = self.env.zdb().page_size();
        let db = match custom_comparator::<C>() {
            None => self.env.zdb().create_database(wtxn.zdb_mut(), name)?,
            Some(cmp) => {
                self.env
                    .zdb()
                    .create_database_with_comparator(wtxn.zdb_mut(), name, cmp)?
            }
        };
        Ok(Database::new(db, page_size))
    }
}

impl<T, KC, DC, C, CDUP> Clone for DatabaseOpenOptions<'_, '_, T, KC, DC, C, CDUP> {
    fn clone(&self) -> Self {
        DatabaseOpenOptions {
            env: self.env,
            name: self.name,
            flags: self.flags,
            marker: PhantomData,
        }
    }
}

/// Bridge from heed's **type-level** [`Comparator`] (an associated `compare`
/// function, no receiver) to ZeroDB's object-safe `zerodb::Comparator`
/// (**milestone 2.4**).
///
/// heed's shape is a marker type, so this is a zero-sized forwarder; the
/// `type_name` is a stable-enough identity for ZeroDB's in-process
/// mismatch check, and it is never written to disk.
///
/// `C` appears only behind `fn() -> C`, a function-pointer type that is
/// unconditionally `Send + Sync`, so this is `Send + Sync` for **any** `C`
/// with no `unsafe impl` — which matters, because CLAUDE.md's unsafe policy
/// for this crate covers only what heed's pointer model forces, and this does
/// not need to be on that list.
struct HeedComparator<C>(PhantomData<fn() -> C>);

impl<C: Comparator + 'static> zerodb::Comparator for HeedComparator<C> {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        C::compare(a, b)
    }

    fn name(&self) -> &str {
        std::any::type_name::<C>()
    }
}

/// The ZeroDB comparator to register for a database typed with heed
/// comparator `C`, or `None` when `C` is heed's [`DefaultComparator`]
/// (**milestone 2.4**).
///
/// `DefaultComparator` is memcmp — exactly ZeroDB's built-in ordering — so
/// registering a forwarder for it would trade an inlined `slice::cmp` for a
/// vtable call on the hot path of every consumer, all of which use it (SPEC 00
/// row 53). Returning `None` keeps the default path bit-for-bit what it was
/// before this milestone.
fn custom_comparator<C: Comparator + 'static>() -> Option<Box<dyn zerodb::Comparator>> {
    if std::any::TypeId::of::<C>() == std::any::TypeId::of::<DefaultComparator>() {
        None
    } else {
        Some(Box::new(HeedComparator::<C>(PhantomData)))
    }
}

// A tiny use so `WithTls` import isn't flagged when default T is inferred.
#[allow(dead_code)]
fn _tls_marker(_: PhantomData<WithTls>) {}
