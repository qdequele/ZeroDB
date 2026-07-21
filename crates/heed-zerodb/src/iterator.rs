//! Iterators — `Ro{Iter,RevIter,Range,RevRange,Prefix,RevPrefix}` and their
//! `Rw` mutating siblings (SPEC 00 rows 43–46, 50; SPEC 03 §4/§7).
//!
//! ## Read iterators (zero-copy, no `unsafe`)
//!
//! Each wraps a native [`zerodb::RoRange`], which already yields `&'txn`
//! borrows over the txn's view (committed map, or a write/nested txn's dirty
//! frames). `next` just decodes with the codecs.
//!
//! ## Write iterators (the engine write cursor, mirroring heed)
//!
//! heed's `RwIter` holds a raw `MDB_txn` and alternates read (`next`) and
//! mutate (`del_current`/`put_current`). ZeroDB's `RwTxn` is a Rust type, so we
//! erase its env lifetime **once at construction** and hand the resulting
//! `&'txn mut` to a native [`zerodb::RwCursor`] — the stack-carrying write
//! cursor (PERF-GAP B1): an advance is an amortized O(1) page-stack step
//! (LMDB's `mc_pg[]`/`mc_ki[]` walk), not a fresh O(log n) seek by the last
//! yielded key, and nothing is copied per step. The cursor yields lending
//! borrows (they die at its next call); [`RwGuts::step`] stretches them to
//! `'txn` — sound under heed's documented contract: **no `&` borrow from a
//! prior `next` may be live across a mutating call** (the reason
//! `del_current`/`put_current` are `unsafe fn`), and the yielded bytes live in
//! the txn's committed map or its dirty frames, which only a mutation through
//! this same iterator can replace. This is the single place the adapter needs
//! raw pointers / lifetime erasure, exactly as heed does (the M1.13-sanctioned
//! "lifetime-erased write cursor" unsafe).

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

/// Shared guts of every `Rw*` iterator: a native stack-carrying write cursor
/// over the lifetime-erased txn, plus the range bounds (PERF-GAP B1). See the
/// module docs for the safety contract.
pub(crate) struct RwGuts<'txn> {
    cursor: zerodb::RwCursor<'txn, 'txn>,
    dir: Dir,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    started: bool,
}

impl<'txn> RwGuts<'txn> {
    pub(crate) fn new(
        wtxn: &'txn mut RwTxn<'_>,
        db: zerodb::Database,
        dir: Dir,
        lower: Bound<Vec<u8>>,
        upper: Bound<Vec<u8>>,
    ) -> RwGuts<'txn> {
        // Erase the write txn's env lifetime to `'txn` and hand the exclusive
        // borrow to the native write cursor for the iterator's whole life.
        let raw: NonNull<zerodb::RwTxn<'_>> = NonNull::from(wtxn.zdb_mut());
        let txn = raw.cast::<zerodb::RwTxn<'txn>>();
        // SAFETY: `txn` came from a live `&'txn mut RwTxn`, so the pointee is
        // valid and exclusively ours for `'txn`; the raw-pointer deref gives
        // the unconstrained lifetime the cursor's `&'txn mut` needs, and the
        // pointee layout is lifetime-independent.
        let cursor = db.rw_cursor(unsafe { &mut *txn.as_ptr() });
        RwGuts {
            cursor,
            dir,
            lower,
            upper,
            started: false,
        }
    }

    /// Advance and yield the next in-range `(key, value)` as `&'txn` borrows.
    fn step(&mut self) -> Option<Result<(&'txn [u8], &'txn [u8])>> {
        let first = !self.started;
        self.started = true;
        let res = match (self.dir, first) {
            (Dir::Fwd, true) => match &self.lower {
                Bound::Unbounded => self.cursor.seek_first(),
                Bound::Included(l) => self.cursor.seek_ge(l),
                Bound::Excluded(l) => self.cursor.seek_gt(l),
            },
            (Dir::Fwd, false) => self.cursor.move_next(),
            (Dir::Rev, true) => match &self.upper {
                Bound::Unbounded => self.cursor.seek_last(),
                Bound::Included(h) => self.cursor.seek_le(h),
                Bound::Excluded(h) => self.cursor.seek_lt(h),
            },
            (Dir::Rev, false) => self.cursor.move_prev(),
        };
        match res {
            Err(e) => Some(Err(e.into())),
            Ok(None) => None,
            Ok(Some((k, v))) => {
                let ok = match self.dir {
                    Dir::Fwd => match &self.upper {
                        Bound::Unbounded => true,
                        Bound::Included(h) => k <= h.as_slice(),
                        Bound::Excluded(h) => k < h.as_slice(),
                    },
                    Dir::Rev => match &self.lower {
                        Bound::Unbounded => true,
                        Bound::Included(l) => k >= l.as_slice(),
                        Bound::Excluded(l) => k > l.as_slice(),
                    },
                };
                if ok {
                    // SAFETY (lifetime stretch to `'txn` — the M1.13
                    // "lifetime-erased write cursor" clause): the yielded
                    // bytes live in the txn's committed map or its dirty
                    // frames, both stable until the next mutation through
                    // this iterator; heed's contract forbids holding these
                    // borrows across such a mutation (`del_current`/
                    // `put_current` are `unsafe fn` for exactly this).
                    let (k, v) = unsafe {
                        (
                            std::slice::from_raw_parts(k.as_ptr(), k.len()),
                            std::slice::from_raw_parts(v.as_ptr(), v.len()),
                        )
                    };
                    Some(Ok((k, v)))
                } else {
                    None
                }
            }
        }
    }

    fn del_current(&mut self) -> Result<bool> {
        Ok(self.cursor.del_current()?)
    }

    fn put_current(&mut self, key: &[u8], data: &[u8]) -> Result<bool> {
        self.cursor.put(zerodb::PutFlags::EMPTY, key, data)?;
        Ok(true)
    }

    fn put_current_with_flags(&mut self, flags: PutFlags, key: &[u8], data: &[u8]) -> Result<()> {
        self.cursor
            .put(crate::database::to_zdb_put_flags(flags), key, data)?;
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
