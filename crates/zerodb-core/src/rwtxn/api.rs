//! The [`Database`] write API.

use super::*;

impl Database {
    /// `put(txn, key, value)` (SPEC 00 row 31): plain upsert.
    ///
    /// # Errors
    ///
    /// [`MdbError::BadValSize`] (empty/oversized key, oversized value),
    /// [`MdbError::MapFull`], [`MdbError::BadTxn`] on a poisoned txn.
    pub fn put(&self, txn: &mut RwTxn<'_>, key: &[u8], value: &[u8]) -> Result<()> {
        let tree = txn.ensure_open(self)?;
        txn.put_tree(tree, key, PutFlags::EMPTY, ValSrc::Val(value))
            .map(|_| ())
    }

    /// `put_with_flags(txn, flags, key, value)` (SPEC 00 row 32; SPEC 01
    /// §S1/§S2).
    ///
    /// # Errors
    ///
    /// As [`Database::put`], plus [`MdbError::KeyExist`] for `NO_OVERWRITE` on
    /// an existing key or `APPEND` with `key <=` the current last key.
    pub fn put_with_flags(
        &self,
        txn: &mut RwTxn<'_>,
        flags: PutFlags,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let tree = txn.ensure_open(self)?;
        txn.put_tree(tree, key, flags, ValSrc::Val(value))
            .map(|_| ())
    }

    /// `put_reserved(txn, key, len, f)` (SPEC 00 row 35, `MDB_RESERVE`,
    /// SPEC 04 TXN-47): the engine places a `len`-byte value slot and hands
    /// `f` the mutable slice into the dirty page (or overflow run frame) to
    /// fill. The closure must fully write the region; the slice is valid only
    /// for the closure's duration (the compile-time form of TXN-47's "until
    /// the next op").
    ///
    /// # Errors
    ///
    /// As [`Database::put`].
    pub fn put_reserved(
        &self,
        txn: &mut RwTxn<'_>,
        key: &[u8],
        len: usize,
        f: impl FnOnce(&mut [u8]),
    ) -> Result<()> {
        let tree = txn.ensure_open(self)?;
        let loc = txn.put_tree(tree, key, PutFlags::EMPTY, ValSrc::Reserve(len))?;
        f(txn.reserved_slot(tree, key, len, loc)?);
        Ok(())
    }

    /// `delete(txn, key)` (SPEC 00 row 36): whether the key existed.
    ///
    /// # Errors
    ///
    /// [`MdbError::BadTxn`] on a poisoned txn; [`MdbError::MapFull`] if the
    /// COW/rebalance ran out of map (which also poisons the txn).
    pub fn delete(&self, txn: &mut RwTxn<'_>, key: &[u8]) -> Result<bool> {
        let tree = txn.ensure_open(self)?;
        txn.delete_tree(tree, key)
    }

    /// `delete_range(txn, lower, upper)` (SPEC 00 row 37): delete every entry
    /// in the bound pair; returns the number deleted.
    ///
    /// Works leaf-at-a-time (SPEC 03, delete_range note): position at the
    /// first covered key, splice the whole covered span out of that leaf in
    /// one compaction, rebalance the leaf once, and re-descend after the last
    /// deleted key. heed over LMDB realizes row 37 as a cursor walk with one
    /// `mdb_cursor_del` per entry — LMDB has no bulk range-delete primitive
    /// to mirror — so this deliberately goes beyond LMDB's own technique: one
    /// COW + one rebalance per covered leaf instead of one descent + one
    /// rebalance per key, and no materialized key list. The observable result
    /// (count, survivors, bound semantics, comparator order) is identical to
    /// a per-entry cursor walk.
    ///
    /// # Errors
    ///
    /// As [`Database::delete`].
    pub fn delete_range(
        &self,
        txn: &mut RwTxn<'_>,
        lower: std::ops::Bound<&[u8]>,
        upper: std::ops::Bound<&[u8]>,
    ) -> Result<u64> {
        let tree = txn.ensure_open(self)?;
        let mut n = 0u64;
        // The key the walk resumes strictly after (`None` = start at
        // `lower`): always the last key deleted from the previous leaf, so
        // every descent runs against the already-rebalanced tree — no saved
        // path is trusted across a rebalance (SPEC 03 §5.4a).
        let mut resume: Option<Vec<u8>> = None;
        // The range-start leaf's deferred rebalance anchor (at most one, set
        // on the first leaf — see `delete_range_leaf_inner`).
        let mut deferred: Option<Vec<u8>> = None;
        loop {
            // TXN-68: each leaf step starts from an owned resume key with no
            // saved path, so a long range delete keeps its dirty set bounded.
            txn.maybe_spill()?;
            // Read phase: the first still-covered key, through the same
            // iterator `range` uses, so every `Bound` combination, inverted
            // bounds, and a custom comparator (SPEC 03 §2.0) position
            // identically to a per-entry walk.
            let lo = match &resume {
                None => lower,
                Some(k) => std::ops::Bound::Excluded(k.as_slice()),
            };
            let first: Vec<u8> = match self.range(&*txn, lo, upper).next() {
                None => break,
                Some(item) => item?.0.to_vec(),
            };
            let (deleted, last, anchor) = txn.delete_range_leaf(tree, &first, upper)?;
            n += deleted;
            if anchor.is_some() {
                deferred = anchor;
            }
            match last {
                // The covered span ran to its leaf's end: covered keys may
                // remain in later leaves.
                Some(k) => resume = Some(k),
                // The span stopped at the upper bound: nothing covered is
                // left.
                None => break,
            }
        }
        // Settle the boundary leaf the walk left below the fill threshold.
        if let Some(anchor) = &deferred {
            txn.delete_range_settle(tree, anchor)?;
        }
        Ok(n)
    }

    /// `clear(txn)` (SPEC 00 row 38, `mdb_drop(_, 0)`): empty the database,
    /// freeing every page, but keep the database (and, for a named DB, its
    /// catalog entry). The working record is reset to empty and, for a named
    /// DB, written back at commit (SPEC 02 §6).
    ///
    /// # Errors
    ///
    /// As [`Database::delete`].
    pub fn clear(&self, txn: &mut RwTxn<'_>) -> Result<()> {
        let tree = txn.ensure_open(self)?;
        txn.clear_tree(tree)
    }

    /// `drop(txn)` (`mdb_drop(_, 1)`): empty the database **and** remove its
    /// catalog entry (SPEC 02 §6). For a **named** DB this deletes the name
    /// from the main tree (decrementing `main_db.entries`) and the handle
    /// becomes stale (a later `open_database` returns `None` until re-created).
    /// For the **main** DB there is no catalog entry to remove — it behaves
    /// like [`Database::clear`] (LMDB `mdb_drop(MAIN_DBI, 1)` = clear, since the
    /// main dbi is a core DB).
    ///
    /// # Errors
    ///
    /// As [`Database::delete`].
    pub fn drop_db(&self, txn: &mut RwTxn<'_>) -> Result<()> {
        txn.drop_database(self)
    }

    /// A mutable cursor over this database (the `iter_mut` /
    /// `prefix_iter_mut` primitive, SPEC 00 rows 33/34; SPEC 03 §7).
    #[must_use]
    pub fn rw_cursor<'t, 'env>(&self, txn: &'t mut RwTxn<'env>) -> RwCursor<'t, 'env> {
        RwCursor {
            txn,
            db: *self,
            pos: CurPos::Start,
            saved: None,
            on_vacated_slot: false,
            #[cfg(debug_assertions)]
            repaired_keeps: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// RwCursor — the mutable cursor (SPEC 03 §7, ADR-0004)
// ---------------------------------------------------------------------------
