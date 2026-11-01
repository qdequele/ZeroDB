//! Commit step C1: writing the free-page database (SPEC 05 GC-10..13,
//! GC-20, GC-30..32).

use super::*;

/// Merge two strictly-ascending, mutually-disjoint id slices into one
/// strictly-ascending vec (GC-3/GC-4; the `freelist_save` step-(c) merge of
/// this txn's `freed` with the carried annex remainder, SPEC 05 §2a GC-30).
/// Disjointness holds by provenance — an annex id was free in the base
/// snapshot, a `freed` id was live in it — and is asserted in debug builds.
pub(super) fn merge_sorted_unique(a: &[u64], b: &[u64]) -> Vec<u64> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    merge_sorted_unique_into(a, b, &mut out);
    out
}

/// [`merge_sorted_unique`] into `out`, which is cleared first: the commit
/// keeps one merge buffer across write txns ([`WriterCarry`]).
pub(super) fn merge_sorted_unique_into(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
    out.clear();
    out.reserve(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        if a[i] < b[j] {
            out.push(a[i]);
            i += 1;
        } else {
            debug_assert_ne!(a[i], b[j], "annex/freed overlap (INV-24)");
            out.push(b[j]);
            j += 1;
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    debug_assert!(out.windows(2).all(|w| w[0] < w[1]), "merge not ascending");
}

/// GC-13 regression guard: `freelist_save`'s loop provably terminates; a bug
/// that breaks the fixed point should be a loud panic, not a hang.
pub(super) const FREELIST_SAVE_MAX_ITERS: usize = 64;

impl<'env> RwTxn<'env> {
    /// GC-10 trailing shrink: loose pages that are the highest-numbered pages
    /// of the file are dropped and `next_pgno` rolls back past them, so the
    /// file does not grow by holes at its tail. A loose page may have been
    /// written by a spill before it was freed (SPEC 04 §6.3a); dropping it
    /// then leaves unreferenced bytes past the new high-water, which TXN-70
    /// allows (the file may keep a grown, unreferenced tail).
    pub(super) fn release_trailing_loose(&mut self) {
        if work_mut!(self).loose.is_empty() {
            return;
        }
        let mut set: std::collections::BTreeSet<u64> =
            work_mut!(self).loose.iter().copied().collect();
        while self.next_pgno > self.committed_last_pg + 1 && set.remove(&(self.next_pgno - 1)) {
            debug_assert!(!self.dirty.contains(self.next_pgno - 1));
            self.next_pgno -= 1;
        }
        work_mut!(self).loose = set.into_iter().collect();
    }

    /// Write a PIL under `BE(f)` into the GC tree via the RESERVE path
    /// (GC-11 step (c) / GC-20 rewrite): the engine places the cell (possibly
    /// splitting GC leaves / spilling to an overflow run, GC-5), then the ids
    /// are encoded straight into the dirty frame — no double buffering of a
    /// potentially multi-MB PIL.
    pub(super) fn put_pil(&mut self, f: u64, ids: &[u64]) -> Result<()> {
        let key = gc_key_encode(f);
        let len = pil_size(ids.len());
        let loc = self.put_tree(TreeId::Free, &key, PutFlags::EMPTY, ValSrc::Reserve(len))?;
        pil_encode_into(ids, self.reserved_slot(TreeId::Free, &key, len, loc)?);
        Ok(())
    }

    /// Commit step C1 (SPEC 05 §4 GC-11..13, ADR-0005; SPEC 04 §9): release
    /// trailing loose pages (GC-10), merge the surviving loose
    /// pages into the freed set (GC-9, mirroring the fork's
    /// `mdb_freelist_save` loose merge), then loop to a fixed point: rewrite
    /// every drained entry's remainder / delete the empties (GC-20), and
    /// write the freed set under `BE(writer_txnid)`.
    ///
    /// **Crash safety:** everything here mutates dirty frames and the working
    /// `free_db` record only; nothing reaches disk before C2, so a crash at
    /// any point inside C1 recovers to `N−1` byte-identically (GC-14, REC-6
    /// H0). Between C2 and C5, a page drained from entry `F` may already be
    /// overwritten on disk — safe, because the fallback meta `N−1` still holds
    /// the old `free_db` root whose entry `F` lists that page as free and
    /// whose trees do not reference it (`F ≤ oldest_reader() ≤ N−1`, TXN-62).
    ///
    /// **Allocation restriction (GC-12):** `alloc_mode = GcSave`
    /// for the whole procedure — `allocate` never **reads** the GC tree; it
    /// draws loose pages (including contiguous loose runs), pages from the
    /// **already-loaded drain pool** ([`RwTxn::save_pool_draw`]), or extends.
    /// Pool draws are what keep file growth bounded: the in-save COW of a
    /// committed GC page cannot reuse its own predecessor (the `N−1` meta
    /// still references it, TXN-62), so a total draw ban would extend the
    /// file on every commit, unboundedly under churn. The anti-leak property
    /// a ban would give is provided by the fixed point instead: an entry
    /// touched by a pool draw goes back on the `pending` rewrite set, and the
    /// loop only exits when no rewrite is pending — so the final tree state
    /// never lists a handed-out page.
    ///
    /// **Termination (GC-13):** each iteration that does not exit strictly
    /// shrinks the finite drain pool (a draw), grows `freed` by
    /// COW-obsoleting a **committed** GC page (finitely many), or folds a
    /// loose page into `freed` (each id folds at most once); all three are
    /// bounded, and the previous iteration's PIL value run is loose and is
    /// re-served to the next iteration's allocation (free-before-allocate
    /// ordering inside the put). Annex-pool draws are bounded too, but a
    /// grow of this txn's own entry served from the pool it lists could
    /// spend one draw per iteration; the own-entry size guard in step (c)
    /// rules that cycle out (GC-13 (v)). `FREELIST_SAVE_MAX_ITERS` turns a
    /// regression into a panic.
    pub(super) fn freelist_save(&mut self) -> Result<()> {
        debug_assert!(self.alloc_mode == AllocMode::Normal);
        self.alloc_mode = AllocMode::GcSave;
        debug_assert!(self.save_touched.is_empty());
        // GC-30 carry (SPEC 05 §2a, ADR-0022): the base annex's unconsumed
        // remainder must land in this txn's own entry (annex or tree) — meta
        // `base` is overwritten by txn `base + 2`, so dropping it leaks.
        // The remainder stays a live **in-save pool** (the GC-12 rule,
        // exactly like the drain pool): its ids passed the gate with
        // `F = base.txnid`, so the save's own allocations may consume them —
        // without this, every save's GC-tree ops extend the file while the
        // carried surplus sits unreachable in `freed`, an unbounded ratchet
        // (`churn_parity_general` pins this). The
        // step-(c) placement merges `freed ∪ annex.live()` and re-loops if
        // the put itself drew from the pool, so no persisted set ever lists
        // a handed-out page. The pool is folded into `freed` only after the
        // fixed point (annex arm) or spent into the tree entry (tree arm).
        //
        // GC-32 placement: this txn's own entry goes to the meta annex when
        // the final merged set fits; once the tree arm runs it is sticky for
        // this save (no flip-flop if a later drain rewrite grows `freed`).
        let annex_cap = crate::page::meta_annex_cap(self.psize);
        let mut tree_arm = false;
        let mut annex_mode = false;
        // GC-13 own-entry size guard (tree arm): the page count of this
        // txn's entry `BE(txnid)` as last written (0 = inline), and whether
        // it has shrunk during this save. See step (c).
        let mut own_pages: Option<u64> = None;
        let mut own_shrunk = false;
        // Every ops-drained entry needs its GC-20 rewrite at least once.
        let mut pending: std::collections::BTreeSet<u64> = self.drains.keys().copied().collect();
        // Release-active bound on the *inner* rewrite loop (GC-13 guard):
        // each inner iteration consumes one pending entry,
        // and an entry only re-enters `pending` via an in-save pool draw,
        // which strictly shrinks the finite pool — so total inner iterations
        // across the whole save are bounded by (initial entries) + (total
        // pool ids) + slack. A regression confined to the inner loop panics
        // loudly instead of hanging.
        let inner_budget = pending.len() + self.drains.values().map(Drain::len).sum::<usize>() + 16;
        let mut inner_iters = 0usize;
        // (b) GC-10 trailing shrink of ops-era loose pages, then GC-9: the
        // survivors join the freed set up front (LMDB parity — the fork's
        // freelist_save merges loose pages into the list first). Loose pages
        // *generated by the loop below* (a replaced PIL overflow run) are
        // re-served to later in-save allocations or folded in at the end.
        self.release_trailing_loose();
        let work = work_mut!(self);
        work.freed.extend(std::mem::take(&mut work.loose));
        let mut iters = 0usize;
        loop {
            iters += 1;
            assert!(
                iters <= FREELIST_SAVE_MAX_ITERS,
                "freelist_save failed to reach a fixed point (GC-13)"
            );
            // (a) apply_drains (GC-11a/GC-20): rewrite the remainder of every
            // pending entry; delete fully-drained ones. Never delete a
            // partially-drained entry (leaks the remainder), never leave a
            // drained id behind (double-hand-out). A put/delete here may draw
            // from the pool (re-dirtying entries — merged from `save_touched`)
            // or free committed GC pages (growing `freed`).
            loop {
                pending.extend(std::mem::take(&mut self.save_touched));
                let Some(&f) = pending.iter().next() else {
                    break;
                };
                pending.remove(&f);
                inner_iters += 1;
                assert!(
                    inner_iters <= inner_budget,
                    "freelist_save drain-rewrite loop exceeded its budget (GC-13)"
                );
                // GC-20 rewrite payload: exactly the live remainder — the
                // consumed prefix never reaches the tree.
                let remaining: Vec<u64> = self
                    .drains
                    .get(&f)
                    .map_or(Vec::new(), |d| d.live().to_vec());
                if remaining.is_empty() {
                    self.drains.remove(&f);
                    let existed = self.delete_tree(TreeId::Free, &gc_key_encode(f))?;
                    debug_assert!(existed, "drained GC entry {f} missing from the tree");
                } else {
                    self.put_pil(f, &remaining)?;
                }
            }
            // (c) this txn's own entry: the meta annex when it fits (GC-32),
            // else the GC tree under BE(writer_txnid) as in format v1.
            work_mut!(self).freed.sort_unstable();
            let pre_dedup = work_mut!(self).freed.len();
            work_mut!(self).freed.dedup();
            debug_assert_eq!(
                work_mut!(self).freed.len(),
                pre_dedup,
                "page double-freed (GC-4)"
            );
            let before = work_mut!(self).freed.len();
            let annex_before = self.annex.len();
            let total = before + annex_before;
            if total > 0 {
                if !tree_arm && total <= annex_cap {
                    // GC-32 annex arm: `freed ∪ annex.live()` IS the annex
                    // (merged after the loop; C4 encodes it as `fl_ids`).
                    // The stash writes nothing into the tree, allocates
                    // nothing and frees nothing, so the fixed point below
                    // simply loses the step-(c) feedback edge.
                    annex_mode = true;
                } else {
                    tree_arm = true; // sticky (GC-32)
                    annex_mode = false;
                    let ids = merge_sorted_unique(&work_mut!(self).freed, self.annex.live());
                    // GC-13 own-entry size guard. The entry lists the carried
                    // annex pool and every loose page folded in, so a run
                    // for it taken from those (an annex draw, a loose page)
                    // leaves the pages it accounts for unchanged — the old
                    // run is listed in their place. Near an overflow
                    // page-count boundary that set may admit no exact size:
                    // the shrunk entry frees a tail page, listing it grows
                    // the entry back, and the grow draws a fresh run from
                    // the annex, forever trading annex ids for listed ones
                    // (LMDB never meets this: its put keeps a larger dirty
                    // overflow run in place, which the exact run length of
                    // SPEC 02 §5 forbids). So once the entry has shrunk in
                    // this save, a grow takes every page from outside that
                    // set — the drain pool or the end of the file — which
                    // adds pages to it and so admits the larger size.
                    let pages = gc_value_pages(ids.len(), self.psize);
                    let grow_after_shrink = own_shrunk && own_pages.is_some_and(|p| pages > p);
                    if own_pages.is_some_and(|p| pages < p) {
                        own_shrunk = true;
                    }
                    if grow_after_shrink {
                        // Loose pages are part of the set too (listed at the
                        // fixed point): hold them back for this put.
                        let held = std::mem::take(&mut work_mut!(self).loose);
                        self.save_outside = true;
                        let r = self.put_pil(self.txnid, &ids);
                        self.save_outside = false;
                        work_mut!(self).loose.extend(held);
                        r?;
                    } else {
                        self.put_pil(self.txnid, &ids)?;
                    }
                    own_pages = Some(pages);
                    if work_mut!(self).freed.len() != before || self.annex.len() != annex_before {
                        // The put COW-freed committed GC pages (PIL must
                        // grow) or drew from the annex pool (the written
                        // PIL lists a handed-out page): rewrite next round.
                        continue;
                    }
                }
            }
            // Fixed-point checks: no entry awaits a re-rewrite (anti-leak — a
            // pool draw during the writes above means some tree entry still
            // lists a handed-out page), and no loose page survives unlisted
            // (GC-9/GC-10).
            pending.extend(std::mem::take(&mut self.save_touched));
            if !pending.is_empty() {
                continue;
            }
            self.release_trailing_loose();
            if work_mut!(self).loose.is_empty() {
                break; // true fixed point
            }
            let work = work_mut!(self);
            work.freed.extend(std::mem::take(&mut work.loose));
        }
        // GC-30/GC-32 settle: in the annex arm the persisted annex is the
        // merged set, materialized into `freed` for C4/C6; in the tree arm
        // the pool's ids were written into the tree entry `BE(txnid)`. The
        // pool is spent either way (no allocation runs after C1).
        if annex_mode {
            let work = work_mut!(self);
            merge_sorted_unique_into(&work.freed, self.annex.live(), &mut work.merged);
            std::mem::swap(&mut work.freed, &mut work.merged);
        }
        self.annex = Drain::new(Vec::new());
        self.annex_out = annex_mode;
        Ok(())
    }
}
