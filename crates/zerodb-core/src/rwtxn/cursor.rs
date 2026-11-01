//! The write cursor, [`RwCursor`] (SPEC 03 §5.4).

use super::*;

/// What a write cursor's put at its parked path ([`RwTxn::put_at_path`]) did
/// (SPEC 03 §5.4b).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PathPut {
    /// The entry under the path does not have the put's key: nothing changed.
    Elsewhere,
    /// Rewritten; the path still descends to the entry (repaired across a
    /// split).
    Kept,
    /// Rewritten through a split cascade that grew the root: this frame (the
    /// new root's) goes on top of the path.
    Grown((u64, usize)),
}

impl<'env> RwTxn<'env> {
    /// Overwrite the value of the existing entry at the end of `path` — the
    /// write cursor's put at its parked position (SPEC 03 §5.4b): the same
    /// §6.1 replace as [`put_tree`](Self::put_tree) on a found key, minus the
    /// descent. `path` is left descending to the rewritten entry: same slot
    /// when the replace stayed inside its leaf (the pgnos remapped in place
    /// by `touch_path`), or the slot a leaf split gave it, with every branch
    /// frame above following any split cascade.
    ///
    /// `path` MUST be the live root-to-leaf path to an existing entry, as for
    /// [`delete_at_path`](Self::delete_at_path). When that entry's key is not
    /// `key`, nothing changes ([`PathPut::Elsewhere`]): the caller runs the
    /// keyed put. The caller handles the sequential-writes and flag variants
    /// of a put; this is the plain overwrite.
    pub(super) fn put_at_path(
        &mut self,
        tree: TreeId,
        path: &mut [(u64, usize)],
        key: &[u8],
        val: &[u8],
    ) -> Result<PathPut> {
        self.guard_ok()?;
        // The key is the stored one, so it already passed the key bounds;
        // the value is new (SPEC 03 §6 up-front validation).
        if val.len() as u64 > MAX_DATA_SIZE as u64 {
            return Err(Error::Mdb(MdbError::BadValSize));
        }
        let psize = self.psize;
        let (lpg, ki) = *path.last().expect("non-empty path");
        if let Some(frame) = self.dirty.bytes_mut(lpg) {
            // The leaf is already this txn's (LMDB's `MDB_CURRENT` put on a
            // `P_DIRTY` page): one frame access confirms the key and, for a
            // same-size inline value, rewrites it in place (§6.1). A dirty
            // page's ancestors are this txn's pages too — a page is copied
            // only below a touched parent (§5.3) — so the path needs no
            // touch for a write that changes no structure.
            let at = {
                let leaf = LeafMut::from_valid(&mut *frame, psize).map_err(map_page_err)?;
                if leaf.key(ki) != key {
                    return Ok(PathPut::Elsewhere);
                }
                match leaf.inline_value_at(ki) {
                    Ok((off, dsize))
                        if dsize as usize == val.len()
                            && value_is_inline(key.len(), val.len() as u64, psize) =>
                    {
                        Some(off)
                    }
                    _ => None,
                }
            };
            if let Some(off) = at {
                frame[off..off + val.len()].copy_from_slice(val);
                return Ok(PathPut::Kept);
            }
        } else {
            let leaf = match node_view(self.source(), psize, lpg, Some(&self.validated))
                .map_err(map_page_err)?
            {
                NodeView::Leaf(l) => l,
                NodeView::Branch(_) => return Err(Error::Mdb(MdbError::Invalid)),
            };
            if leaf.key(ki) != key {
                return Ok(PathPut::Elsewhere);
            }
        }
        let depth = self.record(tree).depth;
        if let Err(e) = self.put_apply(tree, path, true, key, ValSrc::Val(val), 0) {
            // Mid-mutation failure: poison (TXN-59), as `put_tree` does.
            self.errored = true;
            return Err(e);
        }
        let rec = *self.record(tree);
        if rec.depth == depth {
            return Ok(PathPut::Kept);
        }
        // A replace splits at most one page per level, so the root grew by
        // one level: a branch over the old root's two halves, `path[0]`
        // being the half that holds the entry.
        debug_assert_eq!(rec.depth, depth + 1, "one put grows the root once");
        let frame = self.dirty.bytes(rec.root).expect("new root is dirty");
        let root = BranchRef::new_prevalidated(frame, self.psize).map_err(map_page_err)?;
        let ki = usize::from(root.child_pgno(0) != path[0].0);
        debug_assert_eq!(root.child_pgno(ki), path[0].0, "path[0] is a root child");
        Ok(PathPut::Grown((rec.root, ki)))
    }

    /// Debug-build check for [`put_at_path`](Self::put_at_path): a path the
    /// cursor keeps after a put must equal a fresh descent for the key, which
    /// must be found there.
    #[cfg(debug_assertions)]
    pub(super) fn shadow_check_put_path(&self, tree: TreeId, kept: &[(u64, usize)], key: &[u8]) {
        let (fresh, found) = self
            .search_path(tree, key)
            .expect("shadow re-descent failed on a kept put path");
        assert!(found, "key missing after a cursor put");
        assert_eq!(
            fresh.as_slice(),
            kept,
            "kept cursor path diverges from a fresh descent after a put"
        );
    }
}

/// The write cursor's **semantic** position, used only when no parked stack
/// exists (fresh cursor, or right after a mutation): the next advance
/// re-derives the physical position from it with one seek.
#[derive(Debug, Clone)]
pub(super) enum CurPos {
    /// Never positioned: `next` behaves as `first`, `prev` as `last`.
    Start,
    /// On the entry with this key (set by a mutation): `next` = strictly
    /// greater, `prev` = strictly lower.
    At(Vec<u8>),
    /// This key was just deleted: `next` = first `>=` it (the entry that
    /// followed the deleted one, SPEC 03 §7), `prev` = first `<` it.
    AfterDelete(Vec<u8>),
}

/// One physical step for [`RwCursor::drive`].
pub(super) enum Step<'k> {
    Next,
    Prev,
    First,
    Last,
    Ge(&'k [u8]),
    Gt(&'k [u8]),
    Le(&'k [u8]),
    Lt(&'k [u8]),
}

/// A write cursor: iteration with `put_current`/`del_current`/`put` at the
/// current entry (SPEC 03 §7). It holds the `&mut RwTxn` exclusively, so **at
/// most one cursor exists across any mutation** — cursor fix-up therefore
/// reduces to this cursor's own position (ADR-0004).
///
/// **Stack persistence:** between mutations the cursor persists its
/// root-to-leaf path as a parked `SavedCursor`, so an advance is an O(1)
/// amortized stack step (LMDB's `mc_pg[]`/`mc_ki[]` walk) on that stack in
/// place, not a fresh O(log n) descent; entries are yielded as **borrows** of
/// the txn's frames (lending style — the borrow dies at the next call, which
/// is what makes the in-place mutations safe at compile time). A
/// `del_current`, and a put of the key under the cursor, mutate at the parked
/// path and keep it, repaired in place — across a split for the put (SPEC 03
/// §5.4a / §5.4b, LMDB's in-place cursor fix-up). Any other mutation drops
/// the parked path and records the key (`CurPos`); the next advance re-seeks
/// once.
/// The exclusivity argument is what makes `Cursor::resume_in`'s
/// same-tree-state contract hold: no mutation can happen while a parked stack
/// exists except through this cursor, and each of its mutating methods either
/// repairs the stack to the post-mutation tree or clears it.
pub struct RwCursor<'t, 'env> {
    pub(super) txn: &'t mut RwTxn<'env>,
    /// Which database this cursor iterates/mutates.
    pub(super) db: Database,
    /// Semantic fallback position (authoritative only when `saved` is `None`).
    pub(super) pos: CurPos,
    /// Parked physical path (authoritative when `Some`); cleared by mutations.
    pub(super) saved: Option<SavedCursor>,
    /// The parked path sits on the slot a `del_current` just vacated, which now
    /// holds that entry's successor (SPEC 03 §7 / §5.4a — LMDB's `C_DEL`).
    ///
    /// While set, the cursor is **not** on a live entry: `current_key` reports
    /// `None`, so a second `del_current` or a `put_current` is a no-op.
    /// Only `next` (which settles instead of stepping) and `prev` (which
    /// steps back to the predecessor) may use the path; every other op
    /// re-derives from `pos`.
    pub(super) on_vacated_slot: bool,
    /// Deletes whose *structural* rebalance kept a repaired path instead of
    /// discarding it (SPEC 03 §5.4a repair table) — a test signal that the
    /// repair fires, mirrored by the shadow check. Debug builds only.
    #[cfg(debug_assertions)]
    pub(super) repaired_keeps: u64,
}

impl RwCursor<'_, '_> {
    /// Run one physical step: move the parked path in place (or re-derive
    /// the position from [`CurPos`] with one seek into an empty parked path)
    /// and yield the entry it lands on as borrows of the txn's pages — the
    /// same page views the step read, as LMDB's `mdb_cursor_next` reads the
    /// node its stack points at.
    pub(super) fn drive(&mut self, op: Step<'_>) -> Result<Option<(&[u8], &[u8])>> {
        // In-leaf step (LMDB's `mdb_cursor_next`/`mdb_cursor_prev`, which
        // move `mc_ki` in the page the cursor holds; right after a `C_DEL`
        // delete `next` returns the node at `mc_ki` as is): read the slot
        // straight from the parked path. Leaving the leaf takes the general
        // path below.
        if let Some((pgno, ki)) = self.saved.as_ref().and_then(SavedCursor::entry_pos) {
            let slot = match op {
                Step::Next if self.on_vacated_slot => Some(ki),
                Step::Next => Some(ki + 1),
                Step::Prev => ki.checked_sub(1),
                _ => None,
            };
            if let Some(slot) = slot {
                let tree = self.db.checked_tree(&*self.txn)?;
                if let Some(kv) = tree.entry_in_leaf(pgno, slot).map_err(map_page_err)? {
                    if let Some(s) = self.saved.as_mut() {
                        s.set_leaf_slot(slot);
                    }
                    self.on_vacated_slot = false;
                    return Ok(Some(kv));
                }
            }
        }
        // A post-delete parked path names the vacated slot, which now holds
        // the deleted entry's successor: `next` settles there rather than
        // stepping, and `prev` steps back from it to the predecessor (LMDB's
        // `mdb_cursor_prev` after `C_DEL`). Every other op re-derives from
        // `pos` (SPEC 03 §5.4a: discarding is always a correct repair).
        let settle = self.on_vacated_slot && matches!(op, Step::Next);
        if self.on_vacated_slot && !matches!(op, Step::Next | Step::Prev) {
            self.saved = None;
        }
        self.on_vacated_slot = false;
        let tree = self.db.checked_tree(&*self.txn)?;
        let resumed = self.saved.is_some();
        // The step borrows the parked stack, so it moves where it is parked:
        // no copy of the whole stack in or out (the entry borrows the txn,
        // not the stack, and outlives the cursor).
        let saved = self.saved.get_or_insert_with(SavedCursor::empty);
        let mut c = BCursor::resume_in(tree, saved);
        let r = match op {
            Step::Next => {
                if settle {
                    c.settle()
                } else if resumed {
                    c.next()
                } else {
                    match &self.pos {
                        // A fresh cursor's `next` is `first` (SPEC 03 §4),
                        // which `BCursor::next` already implements.
                        CurPos::Start => c.next(),
                        CurPos::At(k) => c.get_greater_than(k),
                        CurPos::AfterDelete(k) => c.set_range(k),
                    }
                }
            }
            Step::Prev => {
                if resumed {
                    c.prev()
                } else {
                    match &self.pos {
                        // A fresh cursor's `prev` is `last` (SPEC 03 §4).
                        CurPos::Start => c.prev(),
                        CurPos::At(k) => c.get_lower_than(k),
                        // The deleted key's successor is "current", so
                        // `prev` is the entry strictly before the deleted
                        // key (LMDB cursor-delete semantics).
                        CurPos::AfterDelete(k) => c.get_lower_than(k),
                    }
                }
            }
            Step::First => c.first(),
            Step::Last => c.last(),
            Step::Ge(k) => c.get_greater_than_or_equal_to(k),
            Step::Gt(k) => c.get_greater_than(k),
            Step::Le(k) => c.get_lower_than_or_equal_to(k),
            Step::Lt(k) => c.get_lower_than(k),
        };
        let flags = c.park_in();
        match r {
            Ok(entry) => {
                saved.set_flags(flags);
                Ok(entry)
            }
            Err(e) => {
                // A corrupt page: drop the half-moved path (any later op
                // re-derives from `pos`, as after a discard).
                self.saved = None;
                Err(map_page_err(e))
            }
        }
    }

    /// The current entry's key as owned bytes (for a mutation about to
    /// invalidate the borrows), or `None` if the cursor is not on an entry.
    pub(super) fn current_key(&self) -> Result<Option<Vec<u8>>> {
        if self.on_vacated_slot {
            // The parked path names a slot whose entry was just deleted; the
            // cursor is between entries, not on one.
            return Ok(None);
        }
        match &self.saved {
            Some(s) => match s.entry_pos() {
                Some((pgno, ki)) => {
                    let tree = self.db.checked_tree(&*self.txn)?;
                    let (k, _) = tree.entry_at(pgno, ki).map_err(map_page_err)?;
                    Ok(Some(k.to_vec()))
                }
                None => Ok(None),
            },
            None => match &self.pos {
                CurPos::At(k) => Ok(Some(k.clone())),
                _ => Ok(None),
            },
        }
    }

    /// The ordering this cursor's database is sorted by (SPEC 03 §2.0): the
    /// registered comparator of a named database, memcmp otherwise. A caller
    /// that bounds a walk over this cursor must test the bound under this
    /// ordering — a memcmp test over a comparator-ordered seek stops the scan
    /// at an arbitrary point (the read-side `RoRange` carries the same value
    /// for the same reason).
    #[must_use]
    pub fn key_cmp(&self) -> crate::cmp::KeyCmp<'_> {
        self.txn.comparator_for(self.db.sel())
    }

    /// Advance to the next entry (ascending; SPEC 03 §4 `next` semantics over
    /// the writer's view). After a `del_current`, yields the entry that
    /// followed the deleted one (§7). The yielded borrows die at the next
    /// call on this cursor.
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] on a corrupt tree.
    pub fn move_next(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.drive(Step::Next)
    }

    /// Step back to the previous entry (descending; SPEC 03 §4 `prev`
    /// semantics over the writer's view). From a fresh cursor behaves as
    /// `last`; after a `del_current`, yields the entry that preceded the
    /// deleted one.
    ///
    /// # Errors
    ///
    /// As [`RwCursor::move_next`].
    pub fn move_prev(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.drive(Step::Prev)
    }

    /// Position on the first entry (SPEC 03 §4 `first`).
    ///
    /// # Errors
    ///
    /// As [`RwCursor::move_next`].
    pub fn seek_first(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.drive(Step::First)
    }

    /// Position on the last entry (SPEC 03 §4 `last`).
    ///
    /// # Errors
    ///
    /// As [`RwCursor::move_next`].
    pub fn seek_last(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.drive(Step::Last)
    }

    /// Position on the first entry `>= key` (SPEC 03 §4 `set_range`).
    ///
    /// # Errors
    ///
    /// As [`RwCursor::move_next`].
    pub fn seek_ge(&mut self, key: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        self.drive(Step::Ge(key))
    }

    /// Position on the first entry `> key`.
    ///
    /// # Errors
    ///
    /// As [`RwCursor::move_next`].
    pub fn seek_gt(&mut self, key: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        self.drive(Step::Gt(key))
    }

    /// Position on the last entry `<= key`.
    ///
    /// # Errors
    ///
    /// As [`RwCursor::move_next`].
    pub fn seek_le(&mut self, key: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        self.drive(Step::Le(key))
    }

    /// Position on the last entry `< key`.
    ///
    /// # Errors
    ///
    /// As [`RwCursor::move_next`].
    pub fn seek_lt(&mut self, key: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        self.drive(Step::Lt(key))
    }

    /// Rewrite the value of the current entry, keeping the key (`MDB_CURRENT`,
    /// SPEC 03 §7): same-size rewrites happen in place; a different size
    /// deletes and re-inserts (which may split). Returns `false` if the cursor
    /// is not positioned on an entry. Writes at the parked path and keeps it,
    /// following the entry to its half when the rewrite splits the leaf
    /// (SPEC 03 §5.4b), so the next advance needs no seek.
    ///
    /// # Errors
    ///
    /// As [`Database::put`].
    pub fn put_current(&mut self, value: &[u8]) -> Result<bool> {
        let Some(key) = self.current_key()? else {
            return Ok(false);
        };
        let tree = self.txn.ensure_open(&self.db)?;
        if self.put_at_parked(tree, &key, value)? {
            return Ok(true);
        }
        self.txn
            .put_tree(tree, &key, PutFlags::EMPTY, ValSrc::Val(value))
            .map(|_| ())?;
        self.saved = None;
        self.on_vacated_slot = false;
        self.pos = CurPos::At(key);
        Ok(true)
    }

    /// The cursor-position put (SPEC 03 §5.4b): when the cursor has a parked
    /// path on a live entry whose key is byte-equal to `key`, overwrite that
    /// entry at the path — no descent — and keep the path, repaired across a
    /// split, so the next step resumes without a seek either. Returns
    /// `false`, having changed nothing, when the fast path does not apply;
    /// the caller then runs the keyed put.
    ///
    /// Sequential-writes trees take the keyed put: their rightmost-leaf
    /// finger bookkeeping lives on that path.
    pub(super) fn put_at_parked(&mut self, tree: TreeId, key: &[u8], value: &[u8]) -> Result<bool> {
        if self.on_vacated_slot || self.txn.seq_writes_on(tree) {
            return Ok(false);
        }
        let Some(saved) = self.saved.as_mut() else {
            return Ok(false);
        };
        if saved.entry_pos().is_none() {
            return Ok(false);
        }
        // The path stays parked during the put: an error that rejects the put
        // before any mutation (an oversized value, a poisoned txn) leaves the
        // cursor exactly where it was, as the keyed put's error does.
        match self.txn.put_at_path(tree, saved.frames_mut(), key, value)? {
            PathPut::Elsewhere => return Ok(false),
            PathPut::Kept => {}
            // A split kept the path on the entry; only a root grow adds a
            // frame.
            PathPut::Grown(root) => {
                if saved.push_root(root).is_err() {
                    self.saved = None;
                }
            }
        }
        #[cfg(debug_assertions)]
        if let Some(saved) = self.saved.as_mut() {
            self.repaired_keeps += 1;
            self.txn
                .shadow_check_put_path(tree, saved.frames_mut(), key);
        }
        // `pos` is read only while no path is parked (the struct docs), and
        // the path is kept unless it could not take the new root frame. A
        // kept path leaves `pos` alone: like a step, a put through the cursor
        // then copies no key.
        if self.saved.is_some() {
            return Ok(true);
        }
        match &mut self.pos {
            CurPos::At(k) | CurPos::AfterDelete(k) => {
                k.clear();
                k.extend_from_slice(key);
                let k = std::mem::take(k);
                self.pos = CurPos::At(k);
            }
            CurPos::Start => self.pos = CurPos::At(key.to_vec()),
        }
        Ok(true)
    }

    /// General put through the cursor — exactly [`Database::put_with_flags`]
    /// (same flag semantics, SPEC 01 §S1/§S2), leaving the cursor positioned
    /// on `key` (the heed `RwIter::put_current(key, ..)` primitive, which may
    /// write a key other than the one under the cursor).
    ///
    /// # Errors
    ///
    /// As [`Database::put_with_flags`].
    pub fn put(&mut self, flags: PutFlags, key: &[u8], value: &[u8]) -> Result<()> {
        let tree = self.txn.ensure_open(&self.db)?;
        // A plain put of the key under the cursor is an overwrite at the
        // cursor's position (heed's `put_current`); every flag keeps the keyed
        // path, which owns their semantics.
        if flags == PutFlags::EMPTY && self.put_at_parked(tree, key, value)? {
            return Ok(());
        }
        self.txn
            .put_tree(tree, key, flags, ValSrc::Val(value))
            .map(|_| ())?;
        self.saved = None;
        self.on_vacated_slot = false;
        self.pos = CurPos::At(key.to_vec());
        Ok(())
    }

    /// Delete the current entry (SPEC 03 §7). The cursor is left so that a
    /// following [`RwCursor::move_next`] yields the entry that followed the
    /// deleted one. Returns `false` if not positioned.
    ///
    /// # Errors
    ///
    /// As [`Database::delete`].
    pub fn del_current(&mut self) -> Result<bool> {
        // The deleted key is kept for `pos`: it is what `prev`, and any op
        // after a discarded path, re-derives from. It is copied into the
        // buffer the previous `pos` key owned, so a drain allocates once,
        // not once per entry.
        // Same "on an entry" rule as `current_key`.
        if self.on_vacated_slot {
            return Ok(false);
        }
        let key = match self.saved.as_ref() {
            Some(s) => {
                let Some((pgno, ki)) = s.entry_pos() else {
                    return Ok(false);
                };
                let tree = self.db.checked_tree(&*self.txn)?;
                let (k, _) = tree.entry_at(pgno, ki).map_err(map_page_err)?;
                // The parked path is authoritative, so `pos` holds nothing
                // the cursor needs until it is set below.
                let mut buf = match std::mem::replace(&mut self.pos, CurPos::Start) {
                    CurPos::At(b) | CurPos::AfterDelete(b) => b,
                    CurPos::Start => Vec::new(),
                };
                buf.clear();
                buf.extend_from_slice(k);
                buf
            }
            None => match std::mem::replace(&mut self.pos, CurPos::Start) {
                CurPos::At(k) => k,
                other => {
                    self.pos = other;
                    return Ok(false);
                }
            },
        };
        let tree = match self.txn.ensure_open(&self.db) {
            Ok(tree) => tree,
            Err(e) => {
                // Nothing was deleted: the cursor stays on the entry.
                self.pos = CurPos::At(key);
                return Err(e);
            }
        };
        // `pos` is set on every path below.
        let Some(saved) = self.saved.as_mut() else {
            // No parked path (fresh cursor, or the position came from `pos`
            // after an earlier mutation): descend for the key.
            let existed = self.txn.delete_tree(tree, &key)?;
            self.pos = CurPos::AfterDelete(key);
            return Ok(existed);
        };
        // The parked path IS the live root-to-leaf path to this entry, so the
        // delete needs no descent of its own (SPEC 03 §5.4a), and runs on the
        // parked frames in place (LMDB deletes at `mc_pg`/`mc_ki`).
        debug_assert!(
            saved.entry_pos().is_some(),
            "the key was read at the parked entry, so the cursor is on one"
        );
        // The parked leaf slot needs no adjustment: it already names the cell
        // being removed, and removing it shifts the successor down into it.
        let fate = match self.txn.delete_at_path(tree, saved.frames_mut()) {
            Ok(fate) => fate,
            Err(e) => {
                // The txn is poisoned (TXN-59); nothing may resume this path.
                self.saved = None;
                self.pos = CurPos::AfterDelete(key);
                return Err(e);
            }
        };
        match fate {
            PathFate::Invalidated => {
                // The rebalance moved entries in a way the repair table does
                // not follow (an ancestor split, an emptied tree): the parked
                // path may name the wrong page or depth. Discard it — the
                // next op re-seeks from `pos`, which is always a correct
                // repair (§5.4a).
                self.saved = None;
            }
            fate => {
                // The path still descends to the vacated slot: either nothing
                // structural happened (`Unchanged` — only a cell left this
                // leaf), or the borrow/merge repair moved every frame onto
                // the vacated slot (`Kept`), possibly minus a popped root
                // frame (`KeptShrunk`). `ki` names the successor (the cells
                // above it shifted down; a right-sibling borrow/merge appended
                // cells at or after it; a left-sibling one put cells before
                // it and moved `ki` past them), or is one past the last live
                // cell if the tail was deleted — both of which
                // `Cursor::settle` reads, and from which a plain `prev` still
                // yields the predecessor.
                if fate == PathFate::KeptShrunk {
                    saved.drop_root();
                }
                #[cfg(debug_assertions)]
                {
                    if fate != PathFate::Unchanged {
                        self.repaired_keeps += 1;
                    }
                    self.txn
                        .shadow_check_kept_path(tree, saved.frames_mut(), &key);
                }
                self.on_vacated_slot = true;
            }
        }
        self.pos = CurPos::AfterDelete(key);
        Ok(true)
    }
}
