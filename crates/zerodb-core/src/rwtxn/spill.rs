//! Spilling dirty pages to the file mid-txn past the dirty limit (SPEC 04
//! TXN-68..72), and the frame writes the commit shares with it.

use super::*;

/// Longest run of frames [`RwTxn::write_run`] lists on the stack.
pub(super) const RUN_ON_STACK: usize = 16;

/// One mutating call's worst-case page touches, as LMDB's `need` estimate in
/// `mdb_page_spill` (SPEC 04 TXN-68): the spill trigger's headroom and the
/// smallest spill.
pub(super) const SPILL_NEED: u64 = 64;

impl<'env> RwTxn<'env> {
    /// Bring spilled tree page `pgno` back into a frame at the **same** pgno
    /// (SPEC 04 TXN-72; LMDB `mdb_page_unspill`): its bytes were written by
    /// this txn and already carry its id, so there is no new pgno, no
    /// parent-pointer rewrite and no free.
    #[cold]
    #[inline(never)]
    pub(super) fn unspill(&mut self, pgno: u64) -> Result<u64> {
        if self.dirty.spilled_pages(pgno) != Some(1) {
            // Only tree pages are touched; a run head never is.
            return Err(Error::Mdb(MdbError::Invalid));
        }
        if self.dirty.in_map_mode() {
            // In-place WRITE_MAP (ADR-0021, TXN-45b): the spilled page's
            // bytes already live in the map at `pgno` (they were written
            // there in place and spilling wrote nothing); unspilling is pure
            // re-tracking.
            let frame = self.dirty.unspill_in_place(pgno);
            debug_assert_eq!(
                crate::page::CommonHeader::read(frame).txnid,
                self.txnid,
                "a spilled page carries its txn's id"
            );
            return Ok(pgno);
        }
        let ps = self.psize as usize;
        let base = (pgno as usize)
            .checked_mul(ps)
            .ok_or(Error::Mdb(MdbError::Invalid))?;
        // The `&'env [u8]` map handle, copied out of `self` so the borrow
        // does not alias the `&mut self.dirty` op (as in `touch`). The page
        // was written at a `&mut` boundary before this one, and no borrow into
        // it is live (TXN-39), so reading it now is the TXN-62 contract.
        let map: &[u8] = self.bytes;
        let src = map
            .get(base..base + ps)
            .ok_or(Error::Mdb(MdbError::Invalid))?;
        let frame = self.dirty.unspill_copy(pgno, src);
        debug_assert_eq!(
            crate::page::CommonHeader::read(frame).txnid,
            self.txnid,
            "a spilled page carries its txn's id"
        );
        Ok(pgno)
    }

    /// Write the frames at `pgnos` (ascending) with the commit's page writer:
    /// consecutive frames go out as one vectored write — a frame
    /// at `pgno` covering `n` pages (an overflow run) makes the run
    /// contiguous iff the next frame starts at `pgno + n`. Used by commit C2
    /// and by spilling (SPEC 04 TXN-69). Not yet durable.
    pub(super) fn write_frames(&self, pgnos: &[u64]) -> Result<()> {
        let psize = self.psize;
        // The run being built: `pgnos[first..i]`, contiguous on the file.
        let mut first = 0;
        let mut next_expected: u64 = 0;
        for (i, &pgno) in pgnos.iter().enumerate() {
            // TXN-62: only pages the live meta `N-1` does not reference may be
            // written: beyond the committed high-water (extend / loose), or
            // GC-reclaimed under the oldest-reader gate (freed by
            // `F ≤ oldest ≤ N-1`, hence absent from `N-1`'s trees). Spilling
            // writes a subset of the same pages, earlier (TXN-70). Checked for
            // in-map frames too: their bytes were stored at allocation time
            // (TXN-45b), under exactly this invariant.
            debug_assert!(
                pgno > self.committed_last_pg || work!(self).reclaimed.contains(&pgno),
                "TXN-62 violation: writing page {pgno} referenced by the live snapshot"
            );
            // An in-map frame (ADR-0021) already sits at its final file
            // offset — there is nothing to write (a self-`memcpy` through
            // `write_at_page` would even self-overlap). It still breaks any
            // pending vectored run.
            let (len, in_map) = self.dirty.frame_extent(pgno).expect("pgno has a frame");
            if in_map {
                self.write_run(&pgnos[first..i])?;
                first = i + 1;
                continue;
            }
            if first < i && pgno != next_expected {
                self.write_run(&pgnos[first..i])?;
                first = i;
            }
            if first == i {
                next_expected = pgno;
            }
            next_expected += (len / psize as usize) as u64;
        }
        self.write_run(&pgnos[first..])
    }

    /// Write the heap frames at `run`, which are contiguous on the file, in
    /// one vectored write ([`RwTxn::write_frames`]); nothing for an empty
    /// run. The frame list is built on the stack for a run of up to
    /// [`RUN_ON_STACK`] frames, which covers small commits, so they allocate
    /// nothing here; a longer run collects it into a `Vec`.
    pub(super) fn write_run(&self, run: &[u64]) -> Result<()> {
        let Some(&start) = run.first() else {
            return Ok(());
        };
        let backing = self.env.inner().backing_ref();
        let frame = |pgno: &u64| self.dirty.bytes(*pgno).expect("pgno has a frame");
        if run.len() <= RUN_ON_STACK {
            let mut frames: [&[u8]; RUN_ON_STACK] = [&[]; RUN_ON_STACK];
            for (slot, pgno) in frames.iter_mut().zip(run) {
                *slot = frame(pgno);
            }
            backing.write_pages_at(start, self.psize, &frames[..run.len()])?;
        } else {
            let frames: Vec<&[u8]> = run.iter().map(frame).collect();
            backing.write_pages_at(start, self.psize, &frames)?;
        }
        Ok(())
    }

    /// The spill check (SPEC 04 TXN-68): spill if the dirty set is within
    /// [`SPILL_NEED`] of the limit. Call only where no descent path, view or
    /// frame borrow is live — the start of a mutating entry, or the top of a
    /// multi-step loop that keeps no path across iterations.
    #[inline]
    pub(super) fn maybe_spill(&mut self) -> Result<()> {
        if self.dirty.pages() + SPILL_NEED > self.dirty_limit {
            self.spill()?;
        }
        Ok(())
    }

    /// Spill (SPEC 04 TXN-69, ADR-0017; LMDB `mdb_page_spill`): write at
    /// least `max(SPILL_NEED, limit / 8)` pages of dirty frames to the file —
    /// highest pgnos first, skipping tree roots and finger pages, which are
    /// about to be touched again — and release their frames. A write failure
    /// errors the txn (only abort remains); frames leave the store only after
    /// their write succeeded.
    #[cold]
    #[inline(never)]
    pub(super) fn spill(&mut self) -> Result<()> {
        let want = SPILL_NEED.max(self.dirty_limit / 8);
        let mut keep: Vec<u64> = vec![self.main_db.root, self.free_db.root];
        keep.extend(self.open.values().map(|t| t.rec.root));
        for f in &work_mut!(self).fingers {
            keep.extend(f.path.iter().map(|&(p, _)| p));
        }
        let mut cands: Vec<(u64, u64)> = self
            .dirty
            .frame_extents()
            .filter(|(p, _)| !keep.contains(p))
            .collect();
        cands.sort_unstable_by_key(|&(p, _)| std::cmp::Reverse(p));
        let mut chosen = Vec::new();
        let mut got = 0u64;
        let mut high = self.read_high;
        for (p, n) in cands {
            if got >= want {
                break;
            }
            got += n;
            high = high.max(p + n - 1);
            chosen.push(p);
        }
        if chosen.is_empty() {
            return Ok(());
        }
        chosen.sort_unstable();
        // Under in-place WRITE_MAP (ADR-0021) `write_frames` skips the
        // map-resident frames — their bytes already sit at their final file
        // offsets — so this spill is pure bookkeeping there: the frames are
        // released from tracking and the pgnos marked spilled (TXN-69's
        // observable contract).
        if let Err(e) = self.write_frames(&chosen) {
            self.errored = true;
            return Err(e);
        }
        for p in chosen {
            self.dirty.spill(p);
        }
        // TXN-71: the spill wrote its highest page (in-place WRITE_MAP: the
        // touch already stored it in the fully-backed map), so the file backs
        // every page up to `high`; spilled pages resolve from the map below it.
        self.read_high = high;
        // TXN-71 (ADR-0021): re-derive the cached whole-map read view. The
        // pages just spilled become resolvable through it, and a view taken
        // before those writes must never serve them (under Stacked/Tree
        // Borrows the write invalidates the older borrow). This fresh borrow,
        // at a `&mut` boundary (TXN-39), postdates every write. In-place
        // WRITE_MAP keeps no cached view; see `RwTxn::whole_map`.
        if !self.dirty.in_map_mode() {
            self.bytes = self.env.inner().backing_bytes();
        }
        // TXN-71: a spilled page's map bytes change only at in-place writes
        // of this txn's own dirty frames (TXN-45b) and here, so a fresh
        // memo per spill keeps every memo entry true to the current bytes.
        // The env cache needs no reset: it cannot hold this txn's stamp
        // (only committed txnids are published, all below ours), so a
        // re-read of a spilled page always misses it and revalidates.
        self.validated = ValidatedPages::for_writer(
            self.env.inner().file_trust(),
            Some(self.env.inner().stamp_cache()),
        );
        self.spills += 1;
        Ok(())
    }

    /// How many times this txn has spilled (SPEC 04 TXN-69), even if every
    /// spilled page was brought back since. Test and diagnostics hook.
    #[doc(hidden)]
    #[must_use]
    pub fn spill_count(&self) -> u64 {
        self.spills
    }

    /// Pages currently held in dirty frames (SPEC 04 TXN-68): what the dirty
    /// limit bounds. Test and diagnostics hook.
    #[doc(hidden)]
    #[must_use]
    pub fn dirty_pages(&self) -> u64 {
        self.dirty.pages()
    }

    /// Pgnos currently spilled (SPEC 04 TXN-69). Test and diagnostics hook.
    #[doc(hidden)]
    #[must_use]
    pub fn spilled_pgnos(&self) -> usize {
        self.dirty.spilled_len()
    }

    /// Whether this txn realizes dirty frames **in the writable map**
    /// (ADR-0021, SPEC 04 §6.4 TXN-45b). Test and diagnostics hook: the
    /// WRITE_MAP-parameterized batteries assert the mode is actually active.
    #[doc(hidden)]
    #[must_use]
    pub fn dirty_in_map_mode(&self) -> bool {
        self.dirty.in_map_mode()
    }
}
