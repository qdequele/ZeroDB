//! Iterators — `Ro{Iter,RevIter,Range,RevRange,Prefix,RevPrefix}` and their
//! `Rw` mutating siblings (SPEC 00 rows 43–46, 50; SPEC 03 §4/§7).
//!
//! ## Read iterators (zero-copy, no `unsafe`)
//!
//! Each wraps a native [`zerodb::RoRange`], which already yields `&'txn`
//! borrows over the txn's view (committed map, or a write/nested txn's dirty
//! frames). `next` just decodes with the codecs.
//!
//! ## Write iterators (the pointer-based cursor, mirroring heed)
//!
//! heed's `RwIter` holds a raw `MDB_txn` and alternates read (`next`) and
//! mutate (`del_current`/`put_current`). ZeroDB's `RwTxn` is a Rust type, so we
//! store a lifetime-erased [`NonNull`] to it and re-derive `&'txn` /
//! `&'txn mut` views per call — sound under heed's documented contract: **no
//! `&` borrow from a prior `next` may be live across a mutating call** (the
//! reason `del_current`/`put_current` are `unsafe fn`). Positioning is by key
//! (like `zerodb::RwCursor`): `next` seeks strictly past the last yielded key;
//! after a delete it re-seeks with `set_range`. This is the single place the
//! adapter needs raw pointers, exactly as heed does.

use std::marker::PhantomData;
use std::ops::Bound;
use std::ptr::NonNull;

use heed_traits::BytesDecode;
use heed_types::LazyDecode;

pub use crate::iteration_method::{MoveBetweenKeys, MoveThroughDuplicateValues};
use crate::txn::RwTxn;
use crate::{Error, PutFlags, Result};

/// Decode a `(key, value)` byte pair with the codecs, mapping failures to
/// `Error::Decoding`.
fn decode_pair<'txn, KC, DC>(k: &'txn [u8], v: &'txn [u8]) -> Result<(KC::DItem, DC::DItem)>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    let key = KC::bytes_decode(k).map_err(Error::Decoding)?;
    let data = DC::bytes_decode(v).map_err(Error::Decoding)?;
    Ok((key, data))
}

// ---------------------------------------------------------------------------
// Read iterators
// ---------------------------------------------------------------------------

macro_rules! ro_iterator {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        pub struct $name<'txn, KC, DC, IM = MoveThroughDuplicateValues> {
            pub(crate) range: zerodb::RoRange<'txn>,
            pub(crate) _marker: PhantomData<(KC, DC, IM)>,
        }

        impl<'txn, KC, DC, IM> $name<'txn, KC, DC, IM> {
            pub(crate) fn new(range: zerodb::RoRange<'txn>) -> $name<'txn, KC, DC, IM> {
                $name {
                    range,
                    _marker: PhantomData,
                }
            }

            /// Change the key/data codecs of this iterator (SPEC 00 row 51).
            #[must_use]
            pub fn remap_types<KC2, DC2>(self) -> $name<'txn, KC2, DC2, IM> {
                $name {
                    range: self.range,
                    _marker: PhantomData,
                }
            }

            /// Change the key codec (SPEC 00 row 51).
            #[must_use]
            pub fn remap_key_type<KC2>(self) -> $name<'txn, KC2, DC, IM> {
                self.remap_types::<KC2, DC>()
            }

            /// Change the data codec (SPEC 00 row 51).
            #[must_use]
            pub fn remap_data_type<DC2>(self) -> $name<'txn, KC, DC2, IM> {
                self.remap_types::<KC, DC2>()
            }

            /// Wrap the data in a lazy decoder (SPEC 00 row 50).
            #[must_use]
            pub fn lazily_decode_data(self) -> $name<'txn, KC, LazyDecode<DC>, IM> {
                self.remap_types::<KC, LazyDecode<DC>>()
            }

            /// Iteration method shim (no DUPSORT in Phase 1 — a no-op retag).
            #[must_use]
            pub fn move_between_keys(self) -> $name<'txn, KC, DC, MoveBetweenKeys> {
                $name {
                    range: self.range,
                    _marker: PhantomData,
                }
            }

            /// Iteration method shim (no DUPSORT in Phase 1 — a no-op retag).
            #[must_use]
            pub fn move_through_duplicate_values(
                self,
            ) -> $name<'txn, KC, DC, MoveThroughDuplicateValues> {
                $name {
                    range: self.range,
                    _marker: PhantomData,
                }
            }
        }

        impl<'txn, KC, DC, IM> Iterator for $name<'txn, KC, DC, IM>
        where
            KC: BytesDecode<'txn>,
            DC: BytesDecode<'txn>,
        {
            type Item = Result<(KC::DItem, DC::DItem)>;

            fn next(&mut self) -> Option<Self::Item> {
                match self.range.next()? {
                    Ok((k, v)) => Some(decode_pair::<KC, DC>(k, v)),
                    Err(e) => Some(Err(e.into())),
                }
            }
        }

        impl<KC, DC, IM> std::fmt::Debug for $name<'_, KC, DC, IM> {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.debug_struct(stringify!($name)).finish()
            }
        }
    };
}

ro_iterator!(
    RoIter,
    "A read-only forward full-scan iterator (SPEC 00 row 43)."
);
ro_iterator!(RoRevIter, "A read-only reverse full-scan iterator.");
ro_iterator!(
    RoRange,
    "A read-only forward range iterator (SPEC 00 row 44)."
);
ro_iterator!(
    RoRevRange,
    "A read-only reverse range iterator (SPEC 00 row 44)."
);
ro_iterator!(
    RoPrefix,
    "A read-only forward prefix iterator (SPEC 00 row 45)."
);
ro_iterator!(
    RoRevPrefix,
    "A read-only reverse prefix iterator (SPEC 00 row 46)."
);

// ---------------------------------------------------------------------------
// Write iterators
// ---------------------------------------------------------------------------

/// The direction of a write iterator's walk.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Dir {
    Fwd,
    Rev,
}

/// Shared guts of every `Rw*` iterator: a lifetime-erased handle to the write
/// txn plus a key-based cursor position and range bounds. See the module docs
/// for the safety contract.
pub(crate) struct RwGuts<'txn> {
    txn: NonNull<zerodb::RwTxn<'txn>>,
    db: zerodb::Database,
    dir: Dir,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    /// Last yielded key (owned); `None` before the first `next`.
    last: Option<Vec<u8>>,
    started: bool,
    just_deleted: bool,
    _p: PhantomData<&'txn mut zerodb::RwTxn<'txn>>,
}

impl<'txn> RwGuts<'txn> {
    pub(crate) fn new(
        wtxn: &'txn mut RwTxn<'_>,
        db: zerodb::Database,
        dir: Dir,
        lower: Bound<Vec<u8>>,
        upper: Bound<Vec<u8>>,
    ) -> RwGuts<'txn> {
        // Erase the write txn's env lifetime to `'txn`. Sound: the `&'txn mut`
        // borrow guarantees the txn outlives `'txn`; the pointee layout is
        // lifetime-independent.
        let raw: NonNull<zerodb::RwTxn<'_>> = NonNull::from(wtxn.zdb_mut());
        let txn = raw.cast::<zerodb::RwTxn<'txn>>();
        RwGuts {
            txn,
            db,
            dir,
            lower,
            upper,
            last: None,
            started: false,
            just_deleted: false,
            _p: PhantomData,
        }
    }

    /// Immutable view of the write txn (`&'txn`) for a read step. See module
    /// safety contract.
    fn txn_ref(&self) -> &'txn zerodb::RwTxn<'txn> {
        // SAFETY: the `NonNull` was derived from a live `&'txn mut RwTxn` and no
        // `&mut` view is active during a `next` read; the pointee outlives
        // `'txn`.
        unsafe { self.txn.as_ref() }
    }

    /// Mutable view of the write txn (`&'txn mut`) for a mutate step.
    fn txn_mut(&mut self) -> &'txn mut zerodb::RwTxn<'txn> {
        // SAFETY: exclusive per the `&mut self` receiver and the contract that
        // no `&` borrow from a prior `next` is live across this mutation.
        unsafe { self.txn.as_mut() }
    }

    fn in_upper(&self, key: &[u8]) -> bool {
        match &self.upper {
            Bound::Unbounded => true,
            Bound::Included(h) => key <= h.as_slice(),
            Bound::Excluded(h) => key < h.as_slice(),
        }
    }
    fn in_lower(&self, key: &[u8]) -> bool {
        match &self.lower {
            Bound::Unbounded => true,
            Bound::Included(l) => key >= l.as_slice(),
            Bound::Excluded(l) => key > l.as_slice(),
        }
    }

    /// Advance and yield the next in-range `(key, value)` as `&'txn` borrows.
    fn step(&mut self) -> Option<Result<(&'txn [u8], &'txn [u8])>> {
        let txn = self.txn_ref();
        let db = self.db;
        let res = if !self.started || self.just_deleted {
            self.started = true;
            let seek_from_delete = self.just_deleted;
            self.just_deleted = false;
            match self.dir {
                Dir::Fwd => match (&self.last, &self.lower) {
                    // After a delete, re-seek at the deleted key (>=).
                    (Some(k), _) if seek_from_delete => db.get_greater_than_or_equal_to(txn, k),
                    (_, Bound::Unbounded) => db.first(txn),
                    (_, Bound::Included(l)) => db.get_greater_than_or_equal_to(txn, l),
                    (_, Bound::Excluded(l)) => db.get_greater_than(txn, l),
                },
                Dir::Rev => match (&self.last, &self.upper) {
                    (Some(k), _) if seek_from_delete => db.get_lower_than(txn, k),
                    (_, Bound::Unbounded) => db.last(txn),
                    (_, Bound::Included(h)) => db.get_lower_than_or_equal_to(txn, h),
                    (_, Bound::Excluded(h)) => db.get_lower_than(txn, h),
                },
            }
        } else {
            match (self.dir, &self.last) {
                (Dir::Fwd, Some(k)) => db.get_greater_than(txn, k),
                (Dir::Rev, Some(k)) => db.get_lower_than(txn, k),
                (_, None) => Ok(None),
            }
        };
        match res {
            Err(e) => Some(Err(e.into())),
            Ok(None) => None,
            Ok(Some((k, v))) => {
                let ok = match self.dir {
                    Dir::Fwd => self.in_upper(k),
                    Dir::Rev => self.in_lower(k),
                };
                if ok {
                    self.last = Some(k.to_vec());
                    Some(Ok((k, v)))
                } else {
                    None
                }
            }
        }
    }

    fn del_current(&mut self) -> Result<bool> {
        let Some(key) = self.last.clone() else {
            return Ok(false);
        };
        let db = self.db;
        let existed = db.delete(self.txn_mut(), &key)?;
        self.just_deleted = true;
        Ok(existed)
    }

    fn put_current(&mut self, key: &[u8], data: &[u8]) -> Result<bool> {
        let db = self.db;
        db.put(self.txn_mut(), key, data)?;
        self.last = Some(key.to_vec());
        Ok(true)
    }

    fn put_current_with_flags(&mut self, flags: PutFlags, key: &[u8], data: &[u8]) -> Result<()> {
        let db = self.db;
        db.put_with_flags(
            self.txn_mut(),
            crate::database::to_zdb_put_flags(flags),
            key,
            data,
        )?;
        self.last = Some(key.to_vec());
        Ok(())
    }
}

macro_rules! rw_iterator {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        pub struct $name<'txn, KC, DC, IM = MoveThroughDuplicateValues> {
            pub(crate) guts: RwGuts<'txn>,
            pub(crate) _marker: PhantomData<(KC, DC, IM)>,
        }

        impl<'txn, KC, DC, IM> $name<'txn, KC, DC, IM> {
            pub(crate) fn new(guts: RwGuts<'txn>) -> $name<'txn, KC, DC, IM> {
                $name {
                    guts,
                    _marker: PhantomData,
                }
            }

            /// Delete the entry the cursor is on (SPEC 00 row 34, SPEC 03 §7).
            ///
            /// # Safety
            ///
            /// No `&` borrow of the current entry may be live across this call.
            ///
            /// # Errors
            ///
            /// As `Database::delete`.
            pub unsafe fn del_current(&mut self) -> Result<bool> {
                self.guts.del_current()
            }

            /// Rewrite the current entry's value (SPEC 00 row 33, `MDB_CURRENT`).
            ///
            /// # Safety
            ///
            /// See [`Self::del_current`].
            ///
            /// # Errors
            ///
            /// As `Database::put`.
            pub unsafe fn put_current<'a>(
                &mut self,
                key: &'a KC::EItem,
                data: &'a DC::EItem,
            ) -> Result<bool>
            where
                KC: heed_traits::BytesEncode<'a>,
                DC: heed_traits::BytesEncode<'a>,
            {
                let kb = KC::bytes_encode(key).map_err(Error::Encoding)?;
                let vb = DC::bytes_encode(data).map_err(Error::Encoding)?;
                self.guts.put_current(&kb, &vb)
            }

            /// Rewrite the current entry with explicit flags and a different data
            /// codec (SPEC 00 row 33, `put_current_with_options`).
            ///
            /// # Safety
            ///
            /// See [`Self::del_current`].
            ///
            /// # Errors
            ///
            /// As `Database::put_with_flags`.
            pub unsafe fn put_current_with_options<'a, NDC>(
                &mut self,
                flags: PutFlags,
                key: &'a KC::EItem,
                data: &'a NDC::EItem,
            ) -> Result<()>
            where
                KC: heed_traits::BytesEncode<'a>,
                NDC: heed_traits::BytesEncode<'a>,
            {
                let kb = KC::bytes_encode(key).map_err(Error::Encoding)?;
                let vb = NDC::bytes_encode(data).map_err(Error::Encoding)?;
                self.guts.put_current_with_flags(flags, &kb, &vb)
            }

            /// Reserve-and-write the current entry (SPEC 00 row 33 sibling).
            ///
            /// # Safety
            ///
            /// See [`Self::del_current`].
            ///
            /// # Errors
            ///
            /// As `Database::put_reserved`.
            pub unsafe fn put_current_reserved_with_flags<'a, F>(
                &mut self,
                flags: PutFlags,
                key: &'a KC::EItem,
                data_size: usize,
                write_func: F,
            ) -> Result<bool>
            where
                KC: heed_traits::BytesEncode<'a>,
                F: FnOnce(&mut crate::ReservedSpace) -> std::io::Result<()>,
            {
                let kb = KC::bytes_encode(key).map_err(Error::Encoding)?;
                let mut buf = vec![0u8; data_size];
                {
                    let mut space = crate::ReservedSpace::new(&mut buf);
                    write_func(&mut space).map_err(|e| Error::Encoding(Box::new(e)))?;
                }
                self.guts.put_current_with_flags(flags, &kb, &buf)?;
                Ok(true)
            }

            /// Iteration method shim (no DUPSORT — no-op retag).
            #[must_use]
            pub fn move_between_keys(self) -> $name<'txn, KC, DC, MoveBetweenKeys> {
                $name {
                    guts: self.guts,
                    _marker: PhantomData,
                }
            }

            /// Iteration method shim (no DUPSORT — no-op retag).
            #[must_use]
            pub fn move_through_duplicate_values(
                self,
            ) -> $name<'txn, KC, DC, MoveThroughDuplicateValues> {
                $name {
                    guts: self.guts,
                    _marker: PhantomData,
                }
            }

            /// Change the key/data codecs (SPEC 00 row 51).
            #[must_use]
            pub fn remap_types<KC2, DC2>(self) -> $name<'txn, KC2, DC2, IM> {
                $name {
                    guts: self.guts,
                    _marker: PhantomData,
                }
            }

            /// Change the key codec (SPEC 00 row 51).
            #[must_use]
            pub fn remap_key_type<KC2>(self) -> $name<'txn, KC2, DC, IM> {
                self.remap_types::<KC2, DC>()
            }

            /// Change the data codec (SPEC 00 row 51).
            #[must_use]
            pub fn remap_data_type<DC2>(self) -> $name<'txn, KC, DC2, IM> {
                self.remap_types::<KC, DC2>()
            }

            /// Wrap the data in a lazy decoder (SPEC 00 row 50).
            #[must_use]
            pub fn lazily_decode_data(self) -> $name<'txn, KC, LazyDecode<DC>, IM> {
                self.remap_types::<KC, LazyDecode<DC>>()
            }
        }

        impl<'txn, KC, DC, IM> Iterator for $name<'txn, KC, DC, IM>
        where
            KC: BytesDecode<'txn>,
            DC: BytesDecode<'txn>,
        {
            type Item = Result<(KC::DItem, DC::DItem)>;

            fn next(&mut self) -> Option<Self::Item> {
                match self.guts.step()? {
                    Ok((k, v)) => Some(decode_pair::<KC, DC>(k, v)),
                    Err(e) => Some(Err(e)),
                }
            }
        }

        impl<KC, DC, IM> std::fmt::Debug for $name<'_, KC, DC, IM> {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.debug_struct(stringify!($name)).finish()
            }
        }
    };
}

rw_iterator!(
    RwIter,
    "A read-write forward full-scan iterator (SPEC 00 row 43)."
);
rw_iterator!(RwRevIter, "A read-write reverse full-scan iterator.");
rw_iterator!(RwRange, "A read-write forward range iterator.");
rw_iterator!(RwRevRange, "A read-write reverse range iterator.");
rw_iterator!(
    RwPrefix,
    "A read-write forward prefix iterator (SPEC 00 row 45)."
);
rw_iterator!(RwRevPrefix, "A read-write reverse prefix iterator.");
