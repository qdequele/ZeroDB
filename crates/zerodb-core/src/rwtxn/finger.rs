//! The rightmost-leaf finger (SPEC 03 §6.6): sequential and APPEND puts
//! reuse the cached right-spine descent instead of searching from the root.

use super::*;

/// One tree's **rightmost-leaf finger** (SPEC 03 §6.6): the full
/// root-to-rightmost-leaf descent path as of the tree's last end-of-tree
/// insert. Every branch frame's `ki` is that page's `num_keys - 1` (the right
/// spine); the leaf frame's `ki` is the slot of the establishing insert and is
/// **not** trusted at use time (the insertion slot is recomputed from the live
/// leaf). Valid only while every page on the path is dirty in this txn — so a
/// hit needs no COW — which [`RwTxn::finger_spine`] re-verifies at use time,
/// never assumes from establishment.
///
/// This is PostgreSQL 11 nbtree's rightmost-leaf fastpath (commit `2b272734`)
/// transplanted to the write txn: sequential and APPEND loads (milli's sorted
/// bulk pattern) always target the rightmost leaf, so remembering its descent
/// skips the per-put root-to-leaf search. LMDB has no equivalent technique to
/// mirror here — `mdb_put` initializes a fresh cursor per call and even
/// `MDB_APPEND` re-descends via `mdb_cursor_last` — so this is a deliberate,
/// documented non-LMDB lever (the observable put semantics are unchanged).
pub(super) struct Finger {
    pub(super) tree: TreeId,
    pub(super) path: Path,
}

impl<'env> RwTxn<'env> {
    /// Index of `tree`'s finger in the table, if one is live.
    pub(super) fn finger_pos(&self, tree: TreeId) -> Option<usize> {
        work!(self).fingers.iter().position(|f| f.tree == tree)
    }

    /// Drop `tree`'s finger (SPEC 03 §6.6 F3). Called at the head of every
    /// structural mutation of `tree` that does not itself re-prove the
    /// finger: point deletes, `delete_range`'s splices and settle pass,
    /// `clear`/`drop` — and by the put path on any fast-path miss, whose
    /// descent then re-establishes it ([`RwTxn::finger_consider`]).
    #[inline(always)]
    pub(super) fn finger_invalidate(&mut self, tree: TreeId) {
        // The table is empty for every tree with the setting off (ADR-0015):
        // one load and a branch, the body out of line.
        if !work_mut!(self).fingers.is_empty() {
            self.finger_invalidate_slow(tree);
        }
    }

    #[inline(never)]
    pub(super) fn finger_invalidate_slow(&mut self, tree: TreeId) {
        work_mut!(self).fingers.retain(|f| f.tree != tree);
    }

    /// Ownership-change hook (SPEC 03 §6.6 F3, the load-bearing one): a page
    /// freed into the loose list can be re-served by `allocate` to ANOTHER
    /// tree while staying dirty, so a finger naming it would still pass every
    /// per-frame check — under the wrong tree. Dropping every finger that
    /// names a freed page (`head .. head + n`) makes that reuse hazard
    /// impossible by construction, not by call-site audit. (Normally
    /// unreachable: a finger page is live in its tree, so only an op on that
    /// tree frees it, and every such op invalidates first — but "normally" is
    /// exactly what a refactor breaks.)
    #[inline(always)]
    pub(super) fn finger_forget(&mut self, head: u64, n: u64) {
        // Empty for every tree with the setting off (ADR-0015); see
        // `finger_invalidate`.
        if !work_mut!(self).fingers.is_empty() {
            self.finger_forget_slow(head, n);
        }
    }

    #[inline(never)]
    pub(super) fn finger_forget_slow(&mut self, head: u64, n: u64) {
        work_mut!(self).fingers.retain(|f| {
            f.path
                .iter()
                .all(|&(pg, _)| !(head..head + n).contains(&pg))
        });
    }

    /// Verify a candidate finger path against the live working tree (SPEC 03
    /// §6.6 F1, structural half): every frame is **dirty in this txn**
    /// (checked now, at use time — a hit must need no COW, and a non-dirty
    /// frame may be any committed page), the branch frames form the right
    /// spine (`ki == num_keys - 1`, each child pointer naming the next frame)
    /// down from the record's **current** root, and the last frame is a
    /// non-empty leaf. Returns the leaf's `(pgno, num_keys)`.
    ///
    /// The walk re-proves the path from `rec.root` through live child
    /// pointers, so a `Some` return *is* the tree's current rightmost descent
    /// — byte-for-byte what `rightmost_path_into` would build, minus the
    /// per-level source dispatch. The F3 invalidation hooks keep stale
    /// fingers from lingering (and from ever aliasing a freed-then-reused
    /// page), but a hit's correctness rests on this re-verification, not on
    /// their completeness.
    pub(super) fn finger_spine(
        &self,
        rec: &DBRecord,
        path: &[(u64, usize)],
    ) -> Option<(u64, usize)> {
        if rec.root == PGNO_INVALID
            || path.is_empty()
            || path.len() != rec.depth as usize
            || path[0].0 != rec.root
        {
            return None;
        }
        for lvl in 0..path.len() - 1 {
            let (pg, ki) = path[lvl];
            let frame = self.dirty.bytes(pg)?;
            // A dirty frame is engine-authored: O(1) structural checks only
            // (the `page_stats` convention). A reused frame of another type
            // (leaf, overflow run) fails the type check and misses.
            let br = BranchRef::new_prevalidated(frame, self.psize).ok()?;
            if ki + 1 != br.num_keys() || br.child_pgno(ki) != path[lvl + 1].0 {
                return None;
            }
        }
        let (lpg, _) = *path.last()?;
        let frame = self.dirty.bytes(lpg)?;
        let leaf = LeafRef::new_prevalidated(frame, self.psize).ok()?;
        let n = leaf.num_keys();
        if n == 0 {
            return None;
        }
        Some((lpg, n))
    }

    /// The finger fast path's hit test (SPEC 03 §6.6 F1). A hit needs a live
    /// finger for `tree`, a spine that still verifies ([`RwTxn::finger_spine`]), and `key` sorting **strictly greater** than the
    /// leaf's last key under the tree's comparator — which is APPEND's §6.3
    /// validation and, for a plain put, proof the key is absent (so
    /// `NO_OVERWRITE` cannot fail either): one condition serves every flag
    /// combination. On a hit `path` is loaded with the finger's frames, the
    /// leaf `ki` set to the insertion slot (`num_keys`), and the finger stays
    /// put — a no-split hit only appends a cell to the fingered leaf. On a
    /// miss the finger is dropped: the normal path may restructure the tree,
    /// and its own descent re-establishes the finger (F2).
    pub(super) fn finger_hit(&mut self, tree: TreeId, key: &[u8], path: &mut Path) -> bool {
        let Some(i) = self.finger_pos(tree) else {
            return false;
        };
        let hit = (|| {
            // Cheapest test first: a key that does not sort after the
            // fingered leaf's last key misses without the spine walk, so a
            // random put pays one leaf view and one compare. This read proves
            // nothing on its own (the frame may since belong to another
            // tree); the spine walk below is what makes a hit correct.
            let (lpg, _) = *work_mut!(self).fingers[i].path.last()?;
            let frame = self.dirty.bytes(lpg)?;
            let leaf = LeafRef::new_prevalidated(frame, self.psize).ok()?;
            let n = leaf.num_keys();
            if n == 0
                || self.tree_comparator(tree).compare(key, leaf.key(n - 1)) != Ordering::Greater
            {
                return None;
            }
            let rec = *self.record(tree);
            let (spine_lpg, spine_n) = self.finger_spine(&rec, &work!(self).fingers[i].path)?;
            debug_assert_eq!((spine_lpg, spine_n), (lpg, n));
            Some(n)
        })();
        match hit {
            Some(n) => {
                path.clear();
                path.extend_from_slice(&work_mut!(self).fingers[i].path);
                path.last_mut().expect("finger path is non-empty").1 = n;
                true
            }
            None => {
                work_mut!(self).fingers.swap_remove(i);
                false
            }
        }
    }

    /// Establish `tree`'s finger from a just-completed insert (SPEC 03 §6.6
    /// F2). `path` is the op's final descent — fully touched, so every frame
    /// is dirty — and `before` the tree's record from before the mutation.
    /// The finger is set only when the op provably left `path` as the right
    /// spine: no depth or page count moved (no split — a split relocates the
    /// rightmost leaf and leaves `path`'s frames stale), the inserted slot is
    /// the leaf's last (an end-of-tree insert), and the spine re-verifies
    /// frame by frame. Anything unprovable establishes nothing: the next
    /// qualifying put pays its one descent and tries again. Check order is
    /// cheapest-first so a random put pays two record compares and one leaf
    /// slot check, not the spine walk.
    pub(super) fn finger_consider(
        &mut self,
        tree: TreeId,
        path: &[(u64, usize)],
        before: &DBRecord,
    ) {
        // Only sequential-writes trees reach here, and the GC tree is never
        // one (`seq_writes_on`): `freelist_save`'s rewrites would invalidate
        // a finger at every step.
        debug_assert_ne!(tree, TreeId::Free, "the GC tree is never fingered");
        let rec = *self.record(tree);
        if rec.depth != before.depth
            || rec.leaf_pages != before.leaf_pages
            || rec.branch_pages != before.branch_pages
        {
            return;
        }
        let Some((_, n)) = self.finger_spine(&rec, path) else {
            return;
        };
        let (_, ki) = *path.last().expect("finger_spine rejected an empty path");
        if ki + 1 != n {
            return; // not an end-of-tree insert
        }
        debug_assert!(
            self.finger_pos(tree).is_none(),
            "establishing over a live finger (op entry must have dropped it)"
        );
        work_mut!(self).fingers.push(Finger {
            tree,
            path: path.to_vec(),
        });
    }

    /// Mandated fast-path cross-check (SPEC 03 §6.6 F4), on **every** hit in
    /// every debug build (tests, fuzz, stress): the normal descent for `key`
    /// must land exactly on the finger's frames — same `(pgno, ki)` at every
    /// level, key absent, insertion slot the one the fast path is about to
    /// use. A finger bug fails loudly here instead of corrupting the tree.
    #[cfg(debug_assertions)]
    pub(super) fn finger_shadow_check(&self, tree: TreeId, key: &[u8], path: &[(u64, usize)]) {
        let (fresh, found) = self
            .search_path(tree, key)
            .expect("finger shadow re-descent failed");
        assert!(!found, "finger fast path hit an existing key");
        assert_eq!(
            fresh.as_slice(),
            path,
            "finger path diverges from a fresh descent"
        );
    }

    /// Whether `tree` keeps a rightmost-leaf finger in this txn (ADR-0015).
    /// The GC tree never does (see [`RwTxn::finger_consider`]).
    #[inline]
    pub(super) fn seq_writes_on(&self, tree: TreeId) -> bool {
        match tree {
            TreeId::Main => self.seq_main,
            TreeId::Free => false,
            TreeId::Named(dbi) => self.open.get(dbi).is_some_and(|t| t.seq),
        }
    }

    /// The finger half of [`RwTxn::put_tree_flagged`] (SPEC 03 §6.6,
    /// ADR-0015): `Some(result)` when the put took the fast path, `None` on a
    /// miss (the finger is dropped and the caller runs the normal path, whose
    /// descent re-establishes it). Out of line so that trees with the setting
    /// off keep the put path's shape (the hot path sits at LLVM's inlining
    /// threshold).
    #[inline(never)]
    pub(super) fn put_finger(
        &mut self,
        tree: TreeId,
        key: &[u8],
        flags: PutFlags,
        val: ValSrc<'_>,
        node_flags: u16,
    ) -> Option<Result<ReserveLoc>> {
        let mut path = std::mem::take(&mut work_mut!(self).path_buf);
        if !self.finger_hit(tree, key, &mut path) {
            work_mut!(self).path_buf = path;
            return None;
        }
        #[cfg(debug_assertions)]
        self.finger_shadow_check(tree, key, &path);
        let before = *self.record(tree);
        let ki = path.last().expect("finger path is non-empty").1;
        // The SAME leaf-insert code as the descent paths — entries and
        // overflow_pages bookkeeping, F_BIGDATA runs, RESERVE, and the §6.4
        // end-of-page split all included; only the descent and `touch_path`
        // (every frame is already dirty) are skipped.
        let res = self.insert_into_leaf(
            tree,
            &mut path,
            ki,
            key,
            val,
            flags.contains(PutFlags::APPEND),
            node_flags,
        );
        match &res {
            Ok(_) => {
                sat_add(&mut self.record_mut(tree).entries, 1);
                let after = *self.record(tree);
                if after.depth != before.depth
                    || after.leaf_pages != before.leaf_pages
                    || after.branch_pages != before.branch_pages
                {
                    // The insert split: the fingered leaf is no longer the
                    // rightmost one. Drop the finger; the next end insert
                    // re-descends and re-establishes it (§6.6 F3).
                    self.finger_invalidate(tree);
                }
                #[cfg(debug_assertions)]
                {
                    self.finger_hits += 1;
                }
            }
            Err(_) => {
                // Mid-mutation failure: poison, exactly as the descent path
                // does (TXN-59 clean abort is the only exit).
                self.finger_invalidate(tree);
                self.errored = true;
            }
        }
        work_mut!(self).path_buf = path;
        Some(res)
    }
}
