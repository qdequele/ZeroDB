//! Commit step C1: writing the free-page database (SPEC 05 GC-10..13,
//! GC-20, GC-30..32).

use std::collections::{BTreeMap, BTreeSet};

use super::*;

/// [`RwTxn::freelist_save`]'s state across fixed-point iterations.
struct SaveLoop {
    /// GC records awaiting their GC-20 rewrite.
    pending: BTreeSet<GcKey>,
    /// Rewrites done so far, and their bound, which release builds enforce
    /// too (GC-13 guard):
    /// each rewrite consumes one pending entry, and an entry only re-enters
    /// `pending` through an in-save pool draw, which strictly shrinks the
    /// finite pool, so the total is bounded by (initial entries) + (total
    /// pool ids) + slack. A bug in the rewrite loop then panics instead of
    /// hanging.
    inner_iters: usize,
    inner_budget: usize,
    /// This txn's own records as last written in the tree arm (GC-11 step
    /// (c)): `seq` → ids. The keys fix the ranges for the rest of the save
    /// (see [`RwTxn::put_own_records`]); the first key is always 0.
    own: BTreeMap<u64, Vec<u64>>,
}

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

/// GC-13 guard: `freelist_save`'s loop provably terminates; a bug that
/// breaks the fixed point should panic, not hang.
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

    /// Write a PIL under `key` into the GC tree via the RESERVE path (GC-11
    /// step (c) / GC-20 rewrite): the engine places the cell (possibly
    /// splitting GC leaves), then the ids are encoded straight into the dirty
    /// frame. A record holds at most [`gc_chunk_cap`] ids, so the cell is
    /// always inline (GC-5).
    pub(super) fn put_pil(&mut self, key: GcKey, ids: &[u64]) -> Result<()> {
        debug_assert!(
            !ids.is_empty() && ids.len() <= gc_chunk_cap(self.psize),
            "GC record of {} ids (GC-5)",
            ids.len()
        );
        debug_assert!(ids[0] >= key.seq, "GC record below its key (INV-29)");
        let key = gc_key_encode(key);
        let len = pil_size(ids.len());
        let loc = self.put_tree(TreeId::Free, &key, PutFlags::EMPTY, ValSrc::Reserve(len))?;
        pil_encode_into(ids, self.reserved_slot(TreeId::Free, &key, len, loc)?);
        Ok(())
    }

    /// Commit step C1 (SPEC 05 §4 GC-11..13, ADR-0005; SPEC 04 §9): release
    /// trailing loose pages (GC-10), merge the surviving loose
    /// pages into the freed set (GC-9, mirroring the fork's
    /// `mdb_freelist_save` loose merge), then loop to a fixed point: rewrite
    /// every drained record's remainder / delete the empties (GC-20), and
    /// write the freed set to the meta annex or as records keyed
    /// `(writer_txnid, seq)` (GC-32).
    ///
    /// **Crash safety:** everything here mutates dirty frames and the working
    /// `free_db` record only; nothing reaches disk before C2, so a crash at
    /// any point inside C1 recovers to `N−1` byte-identically (GC-14, REC-6
    /// H0). Between C2 and C5, a page drained from entry `F` may already be
    /// overwritten on disk — safe, because the fallback meta `N−1` still holds
    /// the old `free_db` root whose entry `F` lists that page as free and
    /// whose trees do not reference it (`F ≤ oldest_reader() ≤ N−1`, TXN-62).
    ///
    /// **Allocation restriction (GC-12):** `alloc_mode = GcSave` for the
    /// whole procedure — `allocate` never **reads** the GC tree; it draws
    /// loose pages, pages from the **already-loaded drain pool**
    /// ([`RwTxn::save_pool_draw`]), the carried annex pool, or extends. Every
    /// request is for one page: GC records are always inline (GC-5).
    /// Pool draws keep file growth bounded: the in-save COW of a committed
    /// GC page cannot reuse its own predecessor (the `N−1` meta still
    /// references it, TXN-62), so without them every commit would extend
    /// the file. An entry touched by a pool draw goes back on the `pending`
    /// rewrite set, and the loop only exits when no rewrite is pending, so
    /// the final tree state never lists a handed-out page.
    ///
    /// **Termination (GC-13):** each iteration that does not exit strictly
    /// shrinks a finite pool (a drain-pool or annex-pool draw), grows `freed`
    /// by COW-obsoleting a **committed** GC page (finitely many), or folds a
    /// loose page into `freed`. Loose pages come only from pages this txn
    /// allocated and then freed in a leaf merge, and a GC leaf merges only
    /// after a record is deleted, which needs a record emptied by pool draws:
    /// so loose folds are bounded by the pool draws too. Nothing else frees
    /// a page: a put into a dirty leaf rewrites it in place or splits it
    /// (one fresh page, nothing freed). `FREELIST_SAVE_MAX_ITERS` turns a
    /// violation into a panic.
    pub(super) fn freelist_save(&mut self) -> Result<()> {
        debug_assert!(self.alloc_mode == AllocMode::Normal);
        self.alloc_mode = AllocMode::GcSave;
        debug_assert!(work!(self).save_touched.is_empty());
        // GC-30 carry (SPEC 05 §2a, ADR-0022): the base annex's unconsumed
        // remainder must land in this txn's own entry (annex or tree) — meta
        // `base` is overwritten by txn `base + 2`, so dropping it leaks.
        // The remainder stays a live **in-save pool**, like the drain pool
        // (GC-12): its ids passed the gate with `F = base.txnid`, so the
        // save's own allocations may consume them instead of extending the
        // file. The step-(c) placement merges `freed ∪ annex.live()` and
        // loops again if the put itself drew from the pool, so no persisted
        // set ever lists a handed-out page. The pool is folded into `freed`
        // only after the fixed point (annex arm) or spent into the tree
        // entry (tree arm).
        //
        // GC-32 placement: this txn's own entry goes to the meta annex when
        // the final merged set fits; once the tree arm runs it is sticky for
        // this save (no flip-flop if a later drain rewrite grows `freed`).
        let annex_cap = crate::page::meta_annex_cap(self.psize);
        let mut tree_arm = false;
        let mut annex_mode = false;
        // Every entry the txn's operations drew from needs its GC-20
        // rewrite at least once.
        let pending: BTreeSet<GcKey> = work!(self).drains.keys().copied().collect();
        let mut st = SaveLoop {
            inner_budget: pending.len()
                + work!(self).drains.values().map(Drain::len).sum::<usize>()
                + 16,
            pending,
            inner_iters: 0,
            own: BTreeMap::new(),
        };
        // (b) GC-10 trailing shrink of the loose pages the txn's operations
        // left, then GC-9: the survivors join the freed set up front (as
        // LMDB's `mdb_freelist_save` merges loose pages first). Loose pages
        // *generated by the loop below* (a GC leaf merged away after a record
        // delete) are re-served to later in-save allocations or folded in at
        // the end.
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
            // (a) apply_drains (GC-11a/GC-20).
            self.rewrite_pending_drains(&mut st)?;
            // (c) this txn's own list: the meta annex when it fits (GC-32),
            // else the GC tree as records keyed (writer_txnid, seq).
            work_mut!(self).freed.sort_unstable();
            let pre_dedup = work_mut!(self).freed.len();
            work_mut!(self).freed.dedup();
            debug_assert_eq!(
                work_mut!(self).freed.len(),
                pre_dedup,
                "page double-freed (GC-4)"
            );
            let before = work_mut!(self).freed.len();
            let annex_before = work!(self).annex.len();
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
                    let work = work!(self);
                    let ids = merge_sorted_unique(&work.freed, work.annex.live());
                    self.put_own_records(&ids, &mut st)?;
                    let work = work!(self);
                    if work.freed.len() != before || work.annex.len() != annex_before {
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
            st.pending
                .extend(std::mem::take(&mut work_mut!(self).save_touched));
            if !st.pending.is_empty() {
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
        // the pool's ids were written into the records `(txnid, seq)`. The
        // pool is spent either way (no allocation runs after C1).
        if annex_mode {
            let work = work_mut!(self);
            merge_sorted_unique_into(&work.freed, work.annex.live(), &mut work.merged);
            std::mem::swap(&mut work.freed, &mut work.merged);
        }
        work_mut!(self).annex = Drain::new(Vec::new());
        self.annex_out = annex_mode;
        Ok(())
    }

    /// Step (a) of [`RwTxn::freelist_save`], apply_drains (GC-11a/GC-20):
    /// rewrite the remainder of every pending record under its own key and
    /// delete fully-drained ones. Never delete a partially-drained record
    /// (leaks the remainder), never leave a drained id behind (double
    /// hand-out). A remainder only lost ids, so it still fits its record's
    /// range and cap (INV-29). A put or delete here may draw from the pool
    /// (re-dirtying records, merged from `save_touched`) or free committed GC
    /// pages (growing `freed`).
    fn rewrite_pending_drains(&mut self, st: &mut SaveLoop) -> Result<()> {
        loop {
            st.pending
                .extend(std::mem::take(&mut work_mut!(self).save_touched));
            let Some(&key) = st.pending.iter().next() else {
                break;
            };
            st.pending.remove(&key);
            st.inner_iters += 1;
            assert!(
                st.inner_iters <= st.inner_budget,
                "freelist_save drain-rewrite loop exceeded its budget (GC-13)"
            );
            // GC-20 rewrite payload: exactly the live remainder — the
            // consumed prefix never reaches the tree.
            let remaining: Vec<u64> = work!(self)
                .drains
                .get(&key)
                .map_or(Vec::new(), |d| d.live().to_vec());
            if remaining.is_empty() {
                work_mut!(self).drains.remove(&key);
                let existed = self.delete_tree(TreeId::Free, &gc_key_encode(key))?;
                debug_assert!(existed, "drained GC record {key:?} missing from the tree");
            } else {
                self.put_pil(key, &remaining)?;
            }
        }
        Ok(())
    }

    /// Step (c), tree arm, of [`RwTxn::freelist_save`]: write this txn's
    /// own list `ids` as records keyed `(txnid, seq)`, each at most
    /// [`gc_chunk_cap`] ids (GC-5, INV-29).
    ///
    /// The first call cuts `ids` into consecutive slices of the cap, keyed 0
    /// for the first and by its lowest id for each later one, so the records
    /// partition the page-id line into ranges `[seq, next seq)`. Later
    /// iterations of the same save keep those ranges: `ids` is split along
    /// them, and only records whose ids changed are rewritten. A range whose
    /// slice grew past the cap is cut again into balanced pieces, each new
    /// piece keyed by its lowest id (inside the range, so the ranges stay a
    /// partition); one that emptied is deleted. When the record keyed 0 is
    /// gone, the lowest remaining one is re-keyed 0 (a delete and a put of
    /// the same ids), so every id that joins the list later has a range.
    fn put_own_records(&mut self, ids: &[u64], st: &mut SaveLoop) -> Result<()> {
        let cap = gc_chunk_cap(self.psize);
        let mut want: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        if st.own.is_empty() {
            for (i, piece) in ids.chunks(cap).enumerate() {
                want.insert(if i == 0 { 0 } else { piece[0] }, piece.to_vec());
            }
        } else {
            let seqs: Vec<u64> = st.own.keys().copied().collect();
            debug_assert_eq!(seqs[0], 0, "own records start at seq 0");
            let mut from = 0;
            for (i, _) in seqs.iter().enumerate() {
                let to = match seqs.get(i + 1) {
                    Some(&next) => from + ids[from..].partition_point(|&p| p < next),
                    None => ids.len(),
                };
                let slice = &ids[from..to];
                from = to;
                if slice.is_empty() {
                    continue;
                }
                let pieces = slice.len().div_ceil(cap);
                let base = slice.len() / pieces;
                let extra = slice.len() % pieces;
                let mut at = 0;
                for j in 0..pieces {
                    let len = base + usize::from(j < extra);
                    let piece = &slice[at..at + len];
                    let seq = if j == 0 { seqs[i] } else { piece[0] };
                    want.insert(seq, piece.to_vec());
                    at += len;
                }
            }
            if !want.contains_key(&0) {
                if let Some((_, low)) = want.pop_first() {
                    want.insert(0, low);
                }
            }
        }
        let txnid = self.txnid;
        let gone: Vec<u64> = st
            .own
            .keys()
            .filter(|seq| !want.contains_key(seq))
            .copied()
            .collect();
        for seq in gone {
            st.own.remove(&seq);
            let existed = self.delete_tree(TreeId::Free, &gc_key_encode(GcKey { txnid, seq }))?;
            debug_assert!(existed, "own GC record {seq} missing from the tree");
        }
        for (seq, piece) in want {
            if st.own.get(&seq) != Some(&piece) {
                self.put_pil(GcKey { txnid, seq }, &piece)?;
                st.own.insert(seq, piece);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::testutil::mem_env;

    const PS: u32 = 4096;

    /// The free tree's records, as `put_own_records` left them.
    fn records(txn: &RwTxn<'_>) -> BTreeMap<GcKey, Vec<u64>> {
        let rec = txn.free_db;
        let mut out = BTreeMap::new();
        if rec.root == PGNO_INVALID {
            return out;
        }
        let tree = Tree::new(txn.source(), txn.psize, rec.root, rec.depth);
        let mut c = tree.cursor();
        let mut e = c.first().unwrap();
        while let Some((k, v)) = e {
            out.insert(gc_key_decode(k).unwrap(), pil_decode(v).unwrap());
            e = c.next().unwrap();
        }
        out
    }

    /// Writes `ids` and checks the tree holds exactly `st.own`, within the
    /// cap and the ranges (INV-29), and lists exactly `ids`.
    fn put_and_check(txn: &mut RwTxn<'_>, st: &mut SaveLoop, ids: &[u64]) -> Vec<u64> {
        txn.put_own_records(ids, st).unwrap();
        let recs = records(txn);
        let own: BTreeMap<GcKey, Vec<u64>> = st
            .own
            .iter()
            .map(|(&seq, v)| {
                (
                    GcKey {
                        txnid: txn.txnid,
                        seq,
                    },
                    v.clone(),
                )
            })
            .collect();
        assert_eq!(recs, own, "tree and bookkeeping disagree");
        let seqs: Vec<u64> = st.own.keys().copied().collect();
        assert_eq!(seqs.first(), Some(&0), "the first record is keyed 0");
        for (i, (seq, v)) in st.own.iter().enumerate() {
            assert!(!v.is_empty() && v.len() <= gc_chunk_cap(PS));
            assert!(v[0] >= *seq);
            if let Some(next) = seqs.get(i + 1) {
                assert!(*v.last().unwrap() < *next, "record {seq} past {next}");
            }
        }
        let all: Vec<u64> = st.own.values().flatten().copied().collect();
        assert_eq!(all, ids);
        seqs
    }

    /// GC-11 step (c): the first write cuts the list into records of `K`;
    /// later writes keep the ranges, split a range that grew past `K` at an
    /// id inside it, delete a range that emptied, and re-key the lowest
    /// record 0 when record 0 is gone, so an id below every key still has a
    /// range.
    #[test]
    fn own_records_keep_their_ranges_across_iterations() {
        let env = mem_env(PS, 1 << 22);
        let mut txn = env.write_txn().unwrap();
        let cap = gc_chunk_cap(PS) as u64;
        let mut st = SaveLoop {
            pending: BTreeSet::new(),
            inner_iters: 0,
            inner_budget: 0,
            own: BTreeMap::new(),
        };
        // 2.5 records' worth of even ids from 100.
        let mut ids: Vec<u64> = (0..cap * 5 / 2).map(|i| 100 + 2 * i).collect();
        let seqs = put_and_check(&mut txn, &mut st, &ids);
        assert_eq!(seqs, vec![0, 100 + 2 * cap, 100 + 4 * cap]);

        // Odd ids join the first range: it holds 2K - 1 ids, cut in two.
        let first_end = 100 + 2 * cap;
        ids.extend((0..cap - 1).map(|i| 101 + 2 * i));
        ids.sort_unstable();
        assert!(ids.iter().filter(|&&p| p < first_end).count() > cap as usize);
        let seqs = put_and_check(&mut txn, &mut st, &ids);
        assert_eq!(seqs.len(), 4, "the grown range split: {seqs:?}");
        assert!(seqs[1] > 0 && seqs[1] < first_end);

        // Every id below the third key goes: records 0 and its split piece
        // are deleted, and the lowest survivor is re-keyed 0.
        let third = seqs[2];
        ids.retain(|&p| p >= third);
        let seqs = put_and_check(&mut txn, &mut st, &ids);
        assert_eq!(seqs.len(), 2);

        // An id below everything lands in record 0, which was full (K ids):
        // it splits again.
        ids.insert(0, 3);
        let seqs = put_and_check(&mut txn, &mut st, &ids);
        assert_eq!(st.own[&0][0], 3);
        assert_eq!(seqs.len(), 3);

        // Emptying the last record deletes it.
        let last = *st.own.keys().last().unwrap();
        ids.retain(|&p| p < last);
        let after = put_and_check(&mut txn, &mut st, &ids);
        assert_eq!(after, seqs[..2]);
        txn.abort();
    }
}
