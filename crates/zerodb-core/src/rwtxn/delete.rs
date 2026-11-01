//! Deletes: single keys, ranges, and the borrow/merge rebalance (SPEC 03
//! §7, §10).

use super::*;

/// One `delete_range` leaf step (SPEC 03, delete_range note): entries
/// deleted, the resume key (set when the span ran to its leaf's end), and
/// the deferred-rebalance anchor (set when the leaf kept survivors in front
/// of the span).
pub(super) type LeafSpan = (u64, Option<Vec<u8>>, Option<Vec<u8>>);

/// What a delete's rebalance ([`RwTxn::rebalance`]) did to the root-to-leaf
/// path it was handed (SPEC 03 §5.4a): the parked write cursor keeps, pops,
/// or discards its path based on this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PathFate {
    /// No structural change: the path is valid exactly as it stands.
    Unchanged,
    /// A borrow or merge changed the tree's shape, but every `(pgno, ki)`
    /// frame still names the descent to the vacated slot — the path was
    /// repaired in place (the borrow/merge repair table, SPEC 03 §5.4a).
    Kept,
    /// As [`Kept`](PathFate::Kept), plus a root shrink dropped the tree's top
    /// level: frame 0 must be popped before the path is used.
    KeptShrunk,
    /// The path may name freed pages or stale indices: discard it and re-seek
    /// (always a correct repair, SPEC 03 §5.4a).
    Invalidated,
}

impl PathFate {
    /// Combine this level's fate with the parent recursion's (merge cascades):
    /// any invalidation wins, then a root pop, then a plain keep.
    pub(super) fn and(self, parent: PathFate) -> PathFate {
        use PathFate::{Invalidated, Kept, KeptShrunk, Unchanged};
        match (self, parent) {
            (Invalidated, _) | (_, Invalidated) => Invalidated,
            (KeptShrunk, _) | (_, KeptShrunk) => KeptShrunk,
            (Kept, _) | (_, Kept) => Kept,
            (Unchanged, Unchanged) => Unchanged,
        }
    }
}

impl<'env> RwTxn<'env> {
    pub(super) fn delete_tree(&mut self, tree: TreeId, key: &[u8]) -> Result<bool> {
        self.guard_ok()?;
        // Read-side key leniency (§2.1): an oversized key simply finds
        // nothing (`Ok(false)`); the empty-key `BadValSize` is an API-boundary
        // concern (oracle adapter / heed adapter).
        let mut path = std::mem::take(&mut work_mut!(self).path_buf);
        let res = match self.search_path_into(tree, key, &mut path) {
            // The path is transient here, so its fate is irrelevant.
            Ok(true) => self.delete_at_path(tree, &mut path).map(|_| true),
            other => other,
        };
        work_mut!(self).path_buf = path;
        res
    }

    /// Delete the entry `path` already points at, skipping the descent
    /// `delete_tree` would do (SPEC 03 §5.4a). Returns the path's
    /// [`PathFate`], which tells a parked cursor whether its path survived
    /// (possibly repaired) or must be discarded.
    ///
    /// `path` MUST be the live root-to-leaf path to an existing entry of
    /// `tree`: either fresh from [`search_path`](Self::search_path), or a
    /// cursor's parked path over an unmutated tree (the `Cursor::resume_in`
    /// contract). It is rewritten in place as pages are COWed and as the
    /// rebalance repairs it.
    pub(super) fn delete_at_path(
        &mut self,
        tree: TreeId,
        path: &mut [(u64, usize)],
    ) -> Result<PathFate> {
        self.guard_ok()?;
        // Any delete can restructure the right spine (SPEC 03 §6.6 F3).
        self.finger_invalidate(tree);
        match self.delete_apply(tree, path) {
            Ok(fate) => {
                sat_sub(&mut self.record_mut(tree).entries, 1);
                Ok(fate)
            }
            Err(e) => {
                self.errored = true;
                Err(e)
            }
        }
    }

    /// Returns [`rebalance`](Self::rebalance)'s [`PathFate`].
    pub(super) fn delete_apply(
        &mut self,
        tree: TreeId,
        path: &mut [(u64, usize)],
    ) -> Result<PathFate> {
        self.touch_path(tree, path)?;
        let (lpg, ki) = *path.last().expect("non-empty path");
        let big = {
            let frame = self.dirty.bytes(lpg).expect("leaf touched");
            let leaf = LeafRef::new_prevalidated(frame, self.psize).map_err(map_page_err)?;
            match leaf.value(ki) {
                LeafValue::Inline(_) => None,
                LeafValue::Overflow { head_pgno, dsize } => Some((head_pgno, dsize)),
            }
        };
        if let Some((head, dsize)) = big {
            let n = overflow_page_count(dsize as u64, self.psize);
            self.free_run(head, n);
            sat_sub(&mut self.record_mut(tree).overflow_pages, n);
        }
        {
            let frame = self.dirty.bytes_mut(lpg).expect("leaf touched");
            LeafMut::from_valid(frame, self.psize)
                .map_err(map_page_err)?
                .remove(ki)
                .map_err(map_page_err)?;
        }
        let top = path.len() - 1;
        self.rebalance(tree, path, top, false)
    }

    /// One leaf step of `Database::delete_range` (SPEC 00 row 37; SPEC 03,
    /// delete_range note): descend to `first` — a key the caller just read from this
    /// tree's range, so it exists and is covered by both bounds — splice the
    /// whole covered span of its leaf out in one compaction, and rebalance
    /// that leaf once. Returns the number of entries deleted and, when the
    /// span ran to the leaf's end (covered keys may remain in later leaves),
    /// the last deleted key for the caller to resume strictly after; `None`
    /// means the span stopped at the upper bound and the walk is done. The
    /// third element is the walk's deferred-rebalance anchor, when this leaf
    /// kept survivors in front of the span (see the body).
    pub(super) fn delete_range_leaf(
        &mut self,
        tree: TreeId,
        first: &[u8],
        upper: std::ops::Bound<&[u8]>,
    ) -> Result<LeafSpan> {
        self.guard_ok()?;
        // The splice and its rebalance can restructure the right spine
        // (SPEC 03 §6.6 F3).
        self.finger_invalidate(tree);
        let mut path = std::mem::take(&mut work_mut!(self).path_buf);
        let res = self.delete_range_leaf_inner(tree, first, upper, &mut path);
        work_mut!(self).path_buf = path;
        res
    }

    /// [`delete_range_leaf`](Self::delete_range_leaf) body, split out so the
    /// path buffer is restored on every exit.
    pub(super) fn delete_range_leaf_inner(
        &mut self,
        tree: TreeId,
        first: &[u8],
        upper: std::ops::Bound<&[u8]>,
        path: &mut Path,
    ) -> Result<LeafSpan> {
        use std::ops::Bound;
        let found = self.search_path_into(tree, first, path)?;
        // `first` was read from this txn's own tree with no mutation in
        // between, so the descent lands on it; degrade to "walk done" rather
        // than mutate anything if it somehow does not.
        debug_assert!(found, "delete_range re-descent lost the key it just read");
        let Some(&(lpg, start)) = path.last() else {
            return Ok((0, None, None));
        };
        // Scan the covered span `[start, end)` and collect its overflow runs
        // on the untouched leaf; COW happens once the span is known.
        let (end, bigs, last, anchor) = {
            let cmp = self.tree_comparator(tree);
            let leaf = match node_view(self.source(), self.psize, lpg, Some(&self.validated))
                .map_err(map_page_err)?
            {
                NodeView::Leaf(l) => l,
                NodeView::Branch(_) => return Err(Error::Mdb(MdbError::Invalid)),
            };
            let nkeys = leaf.num_keys();
            let mut bigs: Vec<(u64, u64)> = Vec::new();
            let mut end = start;
            while end < nkeys {
                let key = leaf.key(end);
                // The exact upper-bound test `RoRange` terminates on, under
                // the tree's ordering (SPEC 03 §2.0) — anything else would
                // stop the splice at a different entry than a per-entry
                // `RoRange` walk.
                let covered = match upper {
                    Bound::Unbounded => true,
                    Bound::Included(h) => cmp.compare(key, h) != Ordering::Greater,
                    Bound::Excluded(h) => cmp.compare(key, h) == Ordering::Less,
                };
                if !covered {
                    break;
                }
                if let LeafValue::Overflow { head_pgno, dsize } = leaf.value(end) {
                    bigs.push((head_pgno, overflow_page_count(dsize as u64, self.psize)));
                }
                end += 1;
            }
            if end == start {
                // Unreachable when `found` (the caller's range test proved
                // `first` covered); bail without mutating rather than splice
                // an empty span.
                debug_assert!(!found, "a found first key must be covered");
                return Ok((0, None, None));
            }
            // Only a span that ran to the leaf's end can leave covered keys
            // in later leaves (every key between two leaf boundaries lives in
            // this leaf); a mid-leaf stop hit the upper bound.
            let last = (end == nkeys).then(|| leaf.key(end - 1).to_vec());
            // Uncovered survivors in front of the span (`start > 0`) only
            // exist on the walk's first leaf: every later `first` is the
            // smallest key past the previous leaf's last deleted key, so
            // nothing uncovered can sit before it. When the walk continues
            // past this leaf, its rebalance is deferred to the end of the
            // walk (`anchor` = the survivor just before the span): rebalanced
            // now, a below-threshold boundary leaf would borrow the next
            // leaf's still-covered entries back one separator rewrite at a
            // time, and the walk would re-delete each of them. Deferring is
            // sound: the survivors keep the leaf non-empty (INV-8 holds), and
            // the fill threshold is a delete-time trigger, not a committed
            // invariant (SPEC 03 §11 INV-8).
            let anchor = (start > 0 && last.is_some()).then(|| leaf.key(start - 1).to_vec());
            (end, bigs, last, anchor)
        };
        let run = (end - start) as u64;
        // Mutation phase: a failure past this point leaves partially-applied
        // structure, so it poisons the txn exactly as `delete_at_path` does.
        match self.delete_span_apply(tree, path, start, end, &bigs, anchor.is_none()) {
            Ok(()) => {
                sat_sub(&mut self.record_mut(tree).entries, run);
                Ok((run, last, anchor))
            }
            Err(e) => {
                self.errored = true;
                Err(e)
            }
        }
    }

    /// The delete_range walk's one deferred rebalance (SPEC 03, delete_range
    /// note): the range-start leaf kept its uncovered survivors but may sit
    /// below the fill threshold now that the walk is done. `anchor` is one of
    /// those survivors; re-descend to it and rebalance its (possibly
    /// since-merged) leaf through the ordinary §10 path — by now every
    /// right neighbor is a survivor too, so this borrows or merges once.
    pub(super) fn delete_range_settle(&mut self, tree: TreeId, anchor: &[u8]) -> Result<()> {
        self.guard_ok()?;
        // The deferred rebalance can borrow/merge (SPEC 03 §6.6 F3).
        self.finger_invalidate(tree);
        let mut path = std::mem::take(&mut work_mut!(self).path_buf);
        let res = (|| {
            let found = self.search_path_into(tree, anchor, &mut path)?;
            debug_assert!(found, "deferred-rebalance anchor is a survivor");
            if path.is_empty() {
                return Ok(());
            }
            // The leaf is already dirty from its splice, so this COWs
            // nothing new; `rebalance` requires a touched path.
            self.touch_path(tree, &mut path)?;
            let top = path.len() - 1;
            self.rebalance(tree, &mut path, top, true).map(|_| ())
        })();
        work_mut!(self).path_buf = path;
        if res.is_err() {
            // Same poisoning as the splice: the walk's structure is applied.
            self.errored = true;
        }
        res
    }

    /// Apply one `delete_range` leaf splice — the bulk sibling of
    /// [`delete_apply`](Self::delete_apply): COW the path (§5.3), free the
    /// span's overflow runs (§8), remove cells `[start, end)` in one
    /// compaction, and — unless the walk deferred it — rebalance the leaf
    /// once (§10). The caller adjusts `entries` and poisons the txn on
    /// error. The rebalance's [`PathFate`] is dropped: the walk re-descends
    /// from the last deleted key instead of trusting a repaired path across
    /// leaves.
    pub(super) fn delete_span_apply(
        &mut self,
        tree: TreeId,
        path: &mut [(u64, usize)],
        start: usize,
        end: usize,
        bigs: &[(u64, u64)],
        do_rebalance: bool,
    ) -> Result<()> {
        self.touch_path(tree, path)?;
        let (lpg, _) = *path.last().expect("non-empty path");
        let mut freed = 0u64;
        for &(head, npages) in bigs {
            self.free_run(head, npages);
            freed += npages;
        }
        if freed != 0 {
            sat_sub(&mut self.record_mut(tree).overflow_pages, freed);
        }
        {
            let frame = self.dirty.bytes_mut(lpg).expect("leaf touched");
            LeafMut::from_valid(frame, self.psize)
                .map_err(map_page_err)?
                .remove_span(start, end)
                .map_err(map_page_err)?;
        }
        if !do_rebalance {
            return Ok(());
        }
        let top = path.len() - 1;
        self.rebalance(tree, path, top, true).map(|_| ())
    }

    /// Debug shadow check for a kept cursor path (SPEC 03 §5.4a): after a
    /// delete keeps (or repairs) the parked path, that path must be identical
    /// to a fresh root-to-leaf descent for the deleted key — same `pgno` and
    /// `ki` at every level, with the key absent and its insertion slot the
    /// vacated one — and the vacated slot, when live, must hold a strictly
    /// greater key (the successor). Runs in every debug build (tests, fuzz,
    /// stress), so a repair bug fails loudly instead of silently skipping or
    /// repeating entries in a cursor drain.
    #[cfg(debug_assertions)]
    pub(super) fn shadow_check_kept_path(
        &self,
        tree: TreeId,
        kept: &[(u64, usize)],
        deleted_key: &[u8],
    ) {
        let (fresh, found) = self
            .search_path(tree, deleted_key)
            .expect("shadow re-descent failed on a kept path");
        assert!(!found, "deleted key still present after del_current");
        assert_eq!(
            fresh.as_slice(),
            kept,
            "kept cursor path diverges from a fresh descent for the deleted key"
        );
        let (lpg, ki) = *kept.last().expect("kept path is non-empty");
        let frame = self.dirty.bytes(lpg).expect("kept path leaf is dirty");
        let leaf = LeafRef::new_prevalidated(frame, self.psize).expect("kept leaf decodes");
        if ki < leaf.num_keys() {
            assert_eq!(
                self.tree_comparator(tree)
                    .compare(leaf.key(ki), deleted_key),
                Ordering::Greater,
                "vacated slot does not hold the deleted key's successor"
            );
        }
    }

    /// `(is_leaf, num_keys, used_bytes)` of the dirty page at `pgno`.
    pub(super) fn page_stats(&self, pgno: u64) -> Result<(bool, usize, usize)> {
        let frame = self.dirty.bytes(pgno).expect("page is dirty");
        // A dirty frame is engine-authored (trusted, see `btree::leaf_view`):
        // O(1) structural checks only.
        let page = PageRef::new_trusted_psize(frame, self.psize).map_err(map_page_err)?;
        let body = body_size(self.psize);
        match page.page_type() {
            PageType::Leaf => {
                let l = LeafRef::new_prevalidated(frame, self.psize).map_err(map_page_err)?;
                Ok((true, l.num_keys(), body - l.free_space()))
            }
            PageType::Branch => {
                let b = BranchRef::new_prevalidated(frame, self.psize).map_err(map_page_err)?;
                Ok((false, b.num_keys(), body - b.free_space()))
            }
            _ => Err(Error::Mdb(MdbError::Invalid)),
        }
    }

    /// §10 rebalance at `path[level]` after a delete/merge: root shrink at the
    /// root (§9); otherwise, when below threshold, borrow from (or merge with)
    /// a sibling, recursing upward on merge. `empty_leaf_merges` is
    /// `delete_range`'s walk mode (SPEC 03 §10 empty-page rule): an emptied
    /// leaf takes the merge arm outright instead of borrowing; every other
    /// delete path passes `false` and keeps the default neighbor policy
    /// bit-for-bit (the churn-flatness pins depend on it).
    /// Returns the [`PathFate`] of `path` (SPEC 03 §5.4a): `Unchanged` when
    /// the delete only removed a cell from its leaf; `Kept`/`KeptShrunk` when
    /// a borrow or merge with either sibling changed the shape and every
    /// frame was repaired in place; `Invalidated` for any ancestor split
    /// during a separator rewrite, an emptied tree, an empty sibling, and
    /// anything else this table does not prove.
    pub(super) fn rebalance(
        &mut self,
        tree: TreeId,
        path: &mut [(u64, usize)],
        level: usize,
        empty_leaf_merges: bool,
    ) -> Result<PathFate> {
        let (pgno, _) = path[level];
        let (is_leaf, nkeys, used) = self.page_stats(pgno)?;
        let body = body_size(self.psize);
        if level == 0 {
            // §9 root shrink.
            if is_leaf && nkeys == 0 {
                self.free_page(pgno);
                let rec = self.record_mut(tree);
                rec.root = PGNO_INVALID;
                rec.depth = 0;
                sat_sub(&mut rec.leaf_pages, 1);
                // The tree is empty: the path names a freed page.
                return Ok(PathFate::Invalidated);
            } else if !is_leaf && nkeys == 1 {
                let child = {
                    let frame = self.dirty.bytes(pgno).expect("root is dirty");
                    BranchRef::new_prevalidated(frame, self.psize)
                        .map_err(map_page_err)?
                        .child_pgno(0)
                };
                self.free_page(pgno);
                let rec = self.record_mut(tree);
                rec.root = child;
                rec.depth = rec.depth.saturating_sub(1);
                sat_sub(&mut rec.branch_pages, 1);
                // The surviving child is the root's only (0th) child, which
                // is exactly the path's next frame: popping frame 0 repairs
                // the path (SPEC 03 §5.4a root-shrink row).
                debug_assert_eq!(path[0].1, 0, "single-child root descends via 0");
                debug_assert_eq!(child, path[1].0, "new root is the path's frame 1");
                return Ok(PathFate::KeptShrunk);
            }
            return Ok(PathFate::Unchanged);
        }
        // §10 thresholds: leaf = 25 % fill (FILL_THRESHOLD) or below min_keys;
        // branch = below min_keys (2 children) — its fill threshold is
        // "effectively > 0".
        let below = if is_leaf {
            nkeys < MIN_KEYS_LEAF || used * 1000 < body * FILL_THRESHOLD_PERMILLE as usize
        } else {
            nkeys < MIN_KEYS_BRANCH
        };
        if !below {
            return Ok(PathFate::Unchanged);
        }
        let (ppg, pki) = path[level - 1];
        let parent_nkeys = {
            let frame = self.dirty.bytes(ppg).expect("parent is dirty");
            BranchRef::new_prevalidated(frame, self.psize)
                .map_err(map_page_err)?
                .num_keys()
        };
        if parent_nkeys < 2 {
            // Cannot happen on an INV-8-conforming tree; degrade gracefully.
            debug_assert!(false, "parent branch with < 2 children mid-rebalance");
            return Ok(PathFate::Unchanged);
        }
        // §10 sibling choice: leftmost child pairs with its right neighbor;
        // every other child pairs with its left neighbor.
        let (sib_idx, fromleft) = if pki == 0 {
            (1usize, false)
        } else {
            (pki - 1, true)
        };
        let sib_old = {
            let frame = self.dirty.bytes(ppg).expect("parent is dirty");
            BranchRef::new_prevalidated(frame, self.psize)
                .map_err(map_page_err)?
                .child_pgno(sib_idx)
        };
        let sib = self.touch(sib_old)?;
        if sib != sib_old {
            let frame = self.dirty.bytes_mut(ppg).expect("parent is dirty");
            BranchMut::from_valid(frame, self.psize)
                .map_err(map_page_err)?
                .set_child_pgno(sib_idx, sib);
        }
        let (s_leaf, s_nkeys, s_used) = self.page_stats(sib)?;
        if s_leaf != is_leaf {
            return Err(Error::Mdb(MdbError::Invalid));
        }
        let can_borrow = if is_leaf {
            // Walk mode (§10 empty-page rule): a leaf `delete_range` emptied
            // is never borrowed into. Forcing the merge arm reproduces the
            // tree LMDB's `mdb_rebalance` leaves after *unlinking* an empty
            // page — one surviving page of the pair under the parent — and it
            // always fits (nothing + sibling <= one page). Borrowing here
            // would leave the pair standing and trickle a fully-covered
            // right sibling across, one separator rewrite per entry.
            !(empty_leaf_merges && nkeys == 0)
                && s_nkeys > MIN_KEYS_LEAF
                && s_used * 1000 >= body * FILL_THRESHOLD_PERMILLE as usize
        } else {
            s_nkeys > MIN_KEYS_BRANCH
        };
        if can_borrow {
            let split = self.borrow_entry(tree, path, level, sib, fromleft, is_leaf)?;
            // Borrow repair (SPEC 03 §5.4a repair table). P keeps its pgno
            // and the parent's frame still names P. From the right, the moved
            // entry is appended after P's slot, so the frame is already
            // correct. From the left, the moved entry becomes P's node 0 and
            // every slot shifts up one (LMDB's `mdb_node_move` bumps `mc_ki`
            // of cursors on the destination page). A separator split
            // rewrites ancestors this table does not follow (and a root grow
            // adds a frame), so it invalidates.
            if fromleft {
                path[level].1 += 1;
            }
            Ok(if split {
                PathFate::Invalidated
            } else {
                PathFate::Kept
            })
        } else {
            self.merge_pages(tree, path, level, sib, fromleft, is_leaf)?;
            // Merge repair: right into left, and `merge_pages` leaves the
            // frame on the surviving left page. With the right sibling, P
            // survives and the appended cells land at or after P's slot, so
            // the slot still names the successor; the parent loses child 1
            // only. With the left sibling, P's cells were appended after the
            // sibling's and `merge_pages` shifted the slot by the sibling's
            // count (LMDB's `mdb_page_merge` cursor fix-up). An empty
            // sibling (hostile image) could leave the slot past an empty
            // page, so it invalidates.
            let here = if s_nkeys == 0 {
                PathFate::Invalidated
            } else {
                PathFate::Kept
            };
            let parent = self.rebalance(tree, path, level - 1, empty_leaf_merges)?;
            Ok(here.and(parent))
        }
    }

    /// §10 BORROW (`node_move`): move the boundary entry from the fuller
    /// sibling and rewrite the parent separator. Returns whether the separator
    /// rewrite split an ancestor (the kept-path invalidation signal).
    pub(super) fn borrow_entry(
        &mut self,
        tree: TreeId,
        path: &mut [(u64, usize)],
        level: usize,
        sib: u64,
        fromleft: bool,
        is_leaf: bool,
    ) -> Result<bool> {
        let (pg, _) = path[level];
        let (_, pki) = path[level - 1];
        let psize = self.psize;
        if is_leaf {
            // REB-2: the boundary cell moves verbatim (flags, key, inline
            // value or overflow head) through the txn's scratch buffer — no
            // decode, no owned key/value copies.
            let mut cell = std::mem::take(&mut work_mut!(self).cell_scratch);
            cell.clear();
            let src_idx = {
                let frame = self.dirty.bytes_mut(sib).expect("sibling touched");
                let mut l = LeafMut::from_valid(frame, psize).map_err(map_page_err)?;
                let i = if fromleft { l.num_keys() - 1 } else { 0 };
                cell.extend_from_slice(l.cell_bytes(i).map_err(map_page_err)?);
                l.remove(i).map_err(map_page_err)?;
                i
            };
            debug_assert!(fromleft || src_idx == 0);
            {
                let frame = self.dirty.bytes_mut(pg).expect("page is dirty");
                let mut l = LeafMut::from_valid(frame, psize).map_err(map_page_err)?;
                // From the left the sibling's last entry becomes P's first;
                // from the right the sibling's first becomes P's last.
                let at = if fromleft { 0 } else { l.num_keys() };
                l.insert_cell_bytes(at, &cell).map_err(map_page_err)?;
            }
            // The rewritten separator: from the left, P's (the moved key);
            // from the right, the sibling's (its new first key).
            let (key_pg, sep_idx) = if fromleft { (pg, pki) } else { (sib, pki + 1) };
            cell.clear();
            {
                let frame = self.dirty.bytes(key_pg).expect("page is dirty");
                let l = LeafRef::new_prevalidated(frame, psize).map_err(map_page_err)?;
                cell.extend_from_slice(l.key(0));
            }
            let split = self.update_parent_key(tree, path, level - 1, sep_idx, &cell);
            work_mut!(self).cell_scratch = cell;
            split
        } else if fromleft {
            // Branch borrow from the left sibling: its last child becomes P's
            // new node 0; P's old node 0 gets P's old parent separator; the
            // parent separator becomes the moved child's key.
            let (k_m, c) = {
                let frame = self.dirty.bytes(sib).expect("sibling touched");
                let br = BranchRef::new_prevalidated(frame, psize).map_err(map_page_err)?;
                let i = br.num_keys() - 1;
                (br.key(i).to_vec(), br.child_pgno(i))
            };
            {
                let frame = self.dirty.bytes_mut(sib).expect("sibling touched");
                let mut b = BranchMut::from_valid(frame, psize).map_err(map_page_err)?;
                let i = b.num_keys() - 1;
                b.remove(i).map_err(map_page_err)?;
            }
            let old_sep = {
                let (ppg, _) = path[level - 1];
                let frame = self.dirty.bytes(ppg).expect("parent is dirty");
                BranchRef::new_prevalidated(frame, psize)
                    .map_err(map_page_err)?
                    .key(pki)
                    .to_vec()
            };
            // P is underful (a single child, the branch rebalance trigger).
            // Rebuild it as: node 0 = (empty, c), node 1 = (old_sep, c0).
            let c0 = {
                let frame = self.dirty.bytes(pg).expect("page is dirty");
                let br = BranchRef::new_prevalidated(frame, psize).map_err(map_page_err)?;
                debug_assert_eq!(br.num_keys(), 1, "underful branch has one child");
                br.child_pgno(0)
            };
            {
                let frame = self.dirty.bytes_mut(pg).expect("page is dirty");
                let mut b = BranchMut::from_valid(frame, psize).map_err(map_page_err)?;
                b.remove(0).map_err(map_page_err)?;
                b.insert(0, &[], c).map_err(map_page_err)?;
                b.insert(1, &old_sep, c0).map_err(map_page_err)?;
            }
            self.update_parent_key(tree, path, level - 1, pki, &k_m)
        } else {
            // Branch borrow from the right sibling: its node 0 moves to P's
            // end carrying the sibling's old parent separator; the sibling's
            // node 1 becomes its new (empty-key) node 0 and its key rises to
            // the parent.
            let (c, k1, c1) = {
                let frame = self.dirty.bytes(sib).expect("sibling touched");
                let br = BranchRef::new_prevalidated(frame, psize).map_err(map_page_err)?;
                (br.child_pgno(0), br.key(1).to_vec(), br.child_pgno(1))
            };
            {
                let frame = self.dirty.bytes_mut(sib).expect("sibling touched");
                let mut b = BranchMut::from_valid(frame, psize).map_err(map_page_err)?;
                // Remove at explicit, unshifted indices, HIGHEST FIRST: after
                // `remove(0)` the real-keyed old node 1 would shift into
                // index 0, where the node-0 sentinel rule (empty separator,
                // SPEC 02 §4.1) rejects its key. So: drop old node 1 first (its child
                // is re-inserted as the new sentinel), then the old node 0
                // sentinel, then install the new sentinel.
                b.remove(1).map_err(map_page_err)?; // old node 1 (real key; child c1 survives below)
                b.remove(0).map_err(map_page_err)?; // old node 0 (the empty-key sentinel)
                b.insert(0, &[], c1).map_err(map_page_err)?;
            }
            let old_sep = {
                let (ppg, _) = path[level - 1];
                let frame = self.dirty.bytes(ppg).expect("parent is dirty");
                BranchRef::new_prevalidated(frame, psize)
                    .map_err(map_page_err)?
                    .key(pki + 1)
                    .to_vec()
            };
            {
                let frame = self.dirty.bytes_mut(pg).expect("page is dirty");
                let mut b = BranchMut::from_valid(frame, psize).map_err(map_page_err)?;
                let n = b.num_keys();
                b.insert(n, &old_sep, c).map_err(map_page_err)?;
            }
            self.update_parent_key(tree, path, level - 1, pki + 1, &k1)
        }
    }

    /// Rewrite the parent separator for `child_idx` (never node 0). REB-3:
    /// in place when the new key's padded growth fits the parent's free
    /// space (LMDB's in-place key update); otherwise remove the old node and
    /// re-insert through the generic branch insert, which splits the parent.
    /// The in-place rewrite fits in exactly the cases the re-insert would
    /// not split, so split decisions (and page allocation) are unchanged.
    /// Returns whether the parent split
    /// ([`insert_into_branch`](Self::insert_into_branch)'s flag).
    pub(super) fn update_parent_key(
        &mut self,
        tree: TreeId,
        path: &mut [(u64, usize)],
        parent_level: usize,
        child_idx: usize,
        new_key: &[u8],
    ) -> Result<bool> {
        debug_assert!(child_idx >= 1, "node 0's separator is always empty");
        let (ppg, _) = path[parent_level];
        {
            let frame = self.dirty.bytes_mut(ppg).expect("parent is dirty");
            let fitted = BranchMut::from_valid(frame, self.psize)
                .map_err(map_page_err)?
                .set_key(child_idx, new_key)
                .map_err(map_page_err)?;
            if fitted {
                return Ok(false);
            }
        }
        let child = {
            let frame = self.dirty.bytes(ppg).expect("parent is dirty");
            BranchRef::new_prevalidated(frame, self.psize)
                .map_err(map_page_err)?
                .child_pgno(child_idx)
        };
        {
            let frame = self.dirty.bytes_mut(ppg).expect("parent is dirty");
            BranchMut::from_valid(frame, self.psize)
                .map_err(map_page_err)?
                .remove(child_idx)
                .map_err(map_page_err)?;
        }
        self.insert_into_branch(
            tree,
            path,
            parent_level as isize,
            child_idx,
            new_key.to_vec(),
            child,
            None,
        )
    }

    /// §10 MERGE, always right-into-left: append the right page's entries to
    /// the left page, drop the parent's separator node for the right page,
    /// free the right page. The caller then rebalances the parent.
    pub(super) fn merge_pages(
        &mut self,
        tree: TreeId,
        path: &mut [(u64, usize)],
        level: usize,
        sib: u64,
        fromleft: bool,
        is_leaf: bool,
    ) -> Result<()> {
        let (pg, _) = path[level];
        let (ppg, pki) = path[level - 1];
        let (left, right, right_idx) = if fromleft {
            (sib, pg, pki)
        } else {
            (pg, sib, pki + 1)
        };
        let psize = self.psize;
        // REB-4: the right page's cells are appended to the left page in
        // order, read from a scratch copy of the right page (so only one
        // dirty frame is borrowed at a time) — no owned per-cell copies.
        let mut src = std::mem::take(&mut work_mut!(self).cell_scratch);
        src.clear();
        src.extend_from_slice(self.dirty.bytes(right).expect("right is dirty"));
        // The left page's entry count before the append: where the right
        // page's slot 0 lands.
        let base;
        if is_leaf {
            {
                let r = LeafRef::new_prevalidated(&src, psize).map_err(map_page_err)?;
                let frame = self.dirty.bytes_mut(left).expect("left is dirty");
                let mut l = LeafMut::from_valid(frame, psize).map_err(map_page_err)?;
                base = l.num_keys();
                for j in 0..r.num_keys() {
                    l.insert_cell_bytes(base + j, r.cell_bytes(j, psize).map_err(map_page_err)?)
                        .map_err(map_page_err)?;
                }
            }
            sat_sub(&mut self.record_mut(tree).leaf_pages, 1);
        } else {
            // The right branch's node 0 regains its explicit key: the parent
            // separator being dropped (§10 merge / §6.5 inverse).
            let sep = {
                let frame = self.dirty.bytes(ppg).expect("parent is dirty");
                BranchRef::new_prevalidated(frame, psize)
                    .map_err(map_page_err)?
                    .key(right_idx)
                    .to_vec()
            };
            {
                let r = BranchRef::new_prevalidated(&src, psize).map_err(map_page_err)?;
                let frame = self.dirty.bytes_mut(left).expect("left is dirty");
                let mut b = BranchMut::from_valid(frame, psize).map_err(map_page_err)?;
                base = b.num_keys();
                b.insert(base, &sep, r.child_pgno(0))
                    .map_err(map_page_err)?;
                for j in 1..r.num_keys() {
                    b.insert(base + j, r.key(j), r.child_pgno(j))
                        .map_err(map_page_err)?;
                }
            }
            sat_sub(&mut self.record_mut(tree).branch_pages, 1);
        }
        work_mut!(self).cell_scratch = src;
        {
            let frame = self.dirty.bytes_mut(ppg).expect("parent is dirty");
            BranchMut::from_valid(frame, self.psize)
                .map_err(map_page_err)?
                .remove(right_idx)
                .map_err(map_page_err)?;
        }
        self.free_page(right);
        // Keep the path coherent: the surviving page is the pair's left page.
        // The parent-level recursion reads the parent frame; a cursor keeping
        // the path reads this one, whose slot moves past the left page's own
        // entries when P was the right page.
        path[level].0 = left;
        if fromleft {
            path[level - 1].1 = pki - 1;
            path[level].1 += base;
        }
        Ok(())
    }
}
