//! Page allocation (SPEC 05 GC-15..21): the loose list, GC-tree draws
//! under the oldest-reader gate, the meta annex pool, the cross-record run
//! search, and freeing pages.

use std::ops::ControlFlow;

use super::*;

/// Reject a decoded GC PIL whose ids are not strictly ascending or lie
/// outside `[FIRST_DATA_PGNO, last_pg]` (SPEC 05 GC-3/INV-25/INV-26). The
/// freelist is on-disk data: a corrupt or hostile entry must yield
/// `MdbError::Invalid`, never hand out the meta slots (page 0/1), a live
/// page, or a page beyond the committed file.
pub(crate) fn validate_pil_ids(ids: &[u64], last_pg: u64) -> Result<()> {
    let mut prev: Option<u64> = None;
    for &id in ids {
        if id < FIRST_DATA_PGNO || id > last_pg {
            return Err(Error::Mdb(MdbError::Invalid));
        }
        if let Some(p) = prev {
            if p >= id {
                return Err(Error::Mdb(MdbError::Invalid));
            }
        }
        prev = Some(id);
    }
    Ok(())
}

/// Decode a GC record's PIL and validate it (SPEC 05 GC-18): the freelist
/// is on-disk data and is not trusted. Every id must be a real, committed,
/// non-meta page, strictly ascending (`find_run` and `binary_search` assume
/// sorted ids) and in `[FIRST_DATA_PGNO, last_pg]`; a hostile PIL could
/// otherwise hand out page 0/1 (the meta slots, clobbered at commit C2) or a
/// page past the file.
fn decode_gc_ids(val: &[u8], last_pg: u64) -> Result<Vec<u64>> {
    let ids = pil_decode(val).ok_or(Error::Mdb(MdbError::Invalid))?;
    validate_pil_ids(&ids, last_pg)?;
    Ok(ids)
}

/// Find an `n`-page pick in a sorted-ascending, unique id list (SPEC 05
/// GC-15/16/19): the **smallest id** for `n == 1`, else the head of the
/// **first** contiguous run of length `≥ n`. Because the ids are strictly
/// ascending and unique, `ids[i + n - 1] == ids[i] + n - 1` implies all `n`
/// are consecutive.
pub(super) fn find_run(ids: &[u64], n: u64) -> Option<u64> {
    if n == 1 {
        return ids.first().copied();
    }
    let n = usize::try_from(n).ok()?;
    let mut i = 0;
    while i + n <= ids.len() {
        if ids[i + n - 1] == ids[i] + (n as u64 - 1) {
            return Some(ids[i]);
        }
        i += 1;
    }
    None
}

/// One GC entry's in-memory drain state (GC-20 bookkeeping): the decoded,
/// validated (strictly ascending — `validate_pil_ids`) PIL ids, viewed
/// through a consumed-prefix offset. Only `ids[head..]` is live; the prefix
/// is pages already handed out this txn.
///
/// Why the offset: the deterministic pick is the **smallest** live id (GC-19,
/// and for runs usually the front too), so a plain `Vec::drain(0..n)`
/// memmoves the whole tail on every draw — O(L²) to consume an entry of `L`
/// ids. Advancing `head` makes the dominant front draw O(1) while keeping the
/// live slice sorted-ascending for `find_run`/`binary_search`. (LMDB solves
/// the same problem by keeping its reclaimed list reverse-sorted and cutting
/// the tail.) A mid-slice run removal (rare: `n > 1` whose run does not start
/// at the front) still pays the memmove via `Vec::drain`.
///
/// The consumed prefix is never re-exposed: every read goes through
/// [`Drain::live`], and the GC-20 rewrite at commit encodes exactly the live
/// slice.
#[derive(Debug, Clone)]
pub(super) struct Drain {
    /// Decoded PIL ids, ascending. `..head` consumed, `head..` live.
    pub(super) ids: DrainIds,
    /// Length of the consumed front prefix. Invariant: `head <= ids.len()`.
    pub(super) head: usize,
}

/// A [`Drain`]'s ids: its own, or the base snapshot's annex ids shared
/// without a copy until a mid-slice removal needs one.
#[derive(Debug, Clone)]
pub(super) enum DrainIds {
    Owned(Vec<u64>),
    /// The base meta's annex (SPEC 05 §2a), pinned in the snapshot. Front
    /// draws only advance `head`, so only a mid-slice removal
    /// ([`Drain::take_span`]) copies it.
    Shared(Arc<[u64]>),
}

impl DrainIds {
    pub(super) fn as_slice(&self) -> &[u64] {
        match self {
            DrainIds::Owned(v) => v,
            DrainIds::Shared(a) => a,
        }
    }
}

impl Drain {
    pub(super) fn new(ids: Vec<u64>) -> Self {
        Drain {
            ids: DrainIds::Owned(ids),
            head: 0,
        }
    }

    /// A drain over shared, already validated ids (the base annex).
    pub(super) fn shared(ids: Arc<[u64]>) -> Self {
        Drain {
            ids: DrainIds::Shared(ids),
            head: 0,
        }
    }

    /// The remaining (still-free) ids, sorted ascending.
    pub(super) fn live(&self) -> &[u64] {
        &self.ids.as_slice()[self.head..]
    }

    /// Number of remaining ids.
    pub(super) fn len(&self) -> usize {
        self.ids.as_slice().len() - self.head
    }

    /// Consume the run `start .. start + n`. The caller (a `find_run` hit on
    /// [`Drain::live`]) guarantees the run is present and — since the live
    /// slice is strictly ascending — occupies `n` consecutive positions.
    /// Front runs advance `head` (O(1), the GC-19 common case); mid-slice
    /// runs fall back to `Vec::drain`.
    pub(super) fn take_run(&mut self, start: u64, n: u64) {
        let n = usize::try_from(n).expect("run length fits usize (find_run did)");
        self.take_span(start, n);
    }

    /// Consume the `count` live ids that start at id `first` and occupy
    /// consecutive positions of [`Drain::live`]. [`Drain::take_run`] is the
    /// case where those ids are also consecutive page numbers; the
    /// cross-record run search ([`RwTxn::cross_record_run`]) also takes a
    /// record's share of a run whose other pages come from other records —
    /// still consecutive positions here, because every id of this record
    /// that lies between two of them would lie inside the run too.
    pub(super) fn take_span(&mut self, first: u64, count: usize) {
        let pos = self
            .live()
            .binary_search(&first)
            .expect("picked id present in the remaining set");
        debug_assert!(pos + count <= self.len(), "span past the live ids");
        if pos == 0 {
            self.head += count;
        } else {
            if let DrainIds::Shared(a) = &self.ids {
                self.ids = DrainIds::Owned(a.to_vec());
            }
            let DrainIds::Owned(ids) = &mut self.ids else {
                unreachable!("made owned above")
            };
            ids.drain(self.head + pos..self.head + pos + count);
        }
        debug_assert!(self.head <= self.ids.as_slice().len());
    }
}

/// How many freed-page records the cross-record run search
/// ([`RwTxn::cross_record_run`]) reads per page of the requested run: it
/// reads at most `n * CROSS_RUN_RECORDS_PER_PAGE` records for a run of `n`
/// pages. LMDB's page allocator bounds its own search the same way (it gives
/// up and extends the file after `n * 60` failed searches, one per record it
/// merges), so a heavily fragmented free list costs a bounded amount of work
/// per allocation instead of a walk of the whole free-page database.
pub(super) const CROSS_RUN_RECORDS_PER_PAGE: u64 = 60;

/// A source of the cross-record run search: a GC-tree record by key, with the
/// ids the search read when this txn had not drawn from it yet, or the base
/// meta's annex pool.
pub(super) enum CrossSource {
    Tree { f: u64, fresh: Option<Vec<u64>> },
    Annex,
}

/// The cross-record run search's state for one write txn
/// ([`RwTxn::cross_record_run`]): the free ids of its sources merged into
/// one ascending list, each tagged with the index of its source.
pub(super) struct CrossRunCache {
    /// `(page id, index into sources)`, ascending by id. May still hold ids
    /// another draw arm took since it was built (they are in `reclaimed`).
    pub(super) merged: Vec<(u64, u32)>,
    pub(super) sources: Vec<CrossSource>,
    /// The record budget it was built with.
    pub(super) budget: u64,
    /// Whether the budget stopped the read before the last gated record.
    pub(super) truncated: bool,
}

/// Pages a GC-tree value listing `count` ids occupies outside its leaf cell:
/// 0 when the PIL is stored inline, else its overflow run length (SPEC 02
/// §4.2/§5). GC keys are 8 bytes (SPEC 05 GC-2).
pub(super) fn gc_value_pages(count: usize, psize: u32) -> u64 {
    let dsize = pil_size(count) as u64;
    if value_is_inline(8, dsize, psize) {
        0
    } else {
        overflow_page_count(dsize, psize)
    }
}

/// Allocation restriction state (SPEC 05 GC-12, ADR-0005).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AllocMode {
    /// Normal ops: loose → GC draw → extend (GC-16).
    Normal,
    /// Inside `freelist_save` (commit step C1): **all GC draws barred**; loose
    /// pages (including contiguous loose runs — see the GC-12 note) or extend
    /// only. Set once at `freelist_save` entry and never cleared: the commit
    /// consumes the txn.
    GcSave,
}

impl<'env> RwTxn<'env> {
    /// `allocate(n)` (SPEC 05 GC-16, ADR-0005): loose fast path, then a GC
    /// draw (skipped entirely inside `freelist_save` — GC-12), then file
    /// extend with the GC-17 bound.
    ///
    /// Under in-place `WRITE_MAP` (ADR-0021) every pgno this returns is
    /// immediately realized **in the map** — a store to committed bytes, not
    /// a heap frame — so the TXN-62 check that is a `debug_assert` at C2
    /// (`write_frames`) is enforced here as a **typed release-mode guard**
    /// (ADR-0021): a future allocator bug must surface as an error, never
    /// silently clobber committed data through the map.
    pub(super) fn allocate(&mut self, n: u64) -> Result<u64> {
        let p = self.allocate_raw(n)?;
        if self.dirty.in_map_mode() && !self.in_map_target_ok(p, n) {
            // Only abort remains: the allocator's bookkeeping can no longer
            // be trusted (LMDB `MDB_TXN_ERROR` degradation, TXN-30).
            self.errored = true;
            return Err(Error::Io(std::io::Error::other(format!(
                "TXN-62 violation refused (ADR-0021 M2): allocator returned page {p} (run of {n}) \
                 at or below the committed high-water {} without a reclaim record — writing it \
                 in place would clobber the live snapshot",
                self.committed_last_pg
            ))));
        }
        Ok(p)
    }

    /// The ADR-0021 predicate: every page of the run is either beyond the
    /// committed high-water or recorded as GC-reclaimed under the
    /// oldest-reader gate this txn (`reclaimed` also covers loose re-draws:
    /// a loose page was classified loose by exactly this predicate,
    /// `RwTxn::free_page`/`free_run`).
    pub(super) fn in_map_target_ok(&self, p: u64, n: u64) -> bool {
        // Fast path: a run starting beyond the high-water lies wholly beyond
        // it (pgnos ascend through the run).
        if p > self.committed_last_pg {
            return true;
        }
        (p..p.saturating_add(n))
            .all(|pg| pg > self.committed_last_pg || work!(self).reclaimed.contains(&pg))
    }

    /// [`RwTxn::allocate`] without the ADR-0021 guard (the guard wraps
    /// every return point in one place).
    ///
    /// The hot shape here is the loose pop. The draw arms (`loose_run`,
    /// `save_pool_draw`, `annex_draw`, `gc_reclaim`, `cross_record_run`) are
    /// `#[inline(never)]` so their size never pushes this function past
    /// LLVM's inlining budget.
    pub(super) fn allocate_raw(&mut self, n: u64) -> Result<u64> {
        debug_assert!(n >= 1);
        if n == 1 {
            if let Some(p) = work_mut!(self).loose.pop() {
                return Ok(p);
            }
        } else if self.alloc_mode == AllocMode::GcSave {
            // Loose-run draw, `freelist_save` only (SPEC 05 GC-12 note): the
            // previous loop iteration's PIL value run is loose and must be
            // re-allocatable or the fixed point is unreachable.
            if let Some(start) = self.loose_run(n) {
                return Ok(start);
            }
        }
        match self.alloc_mode {
            AllocMode::Normal => {
                // GC-16 step 2: read the GC tree under the oldest-reader gate.
                if let Some(start) = self.gc_reclaim(n)? {
                    return Ok(start);
                }
                // GC-16 step 2b (SPEC 05 §2a, ADR-0022): the base meta's
                // free-list annex pool, after the tree (the annex's
                // freeing-txn is the newest possible, so tree-first keeps
                // the GC-18/19 oldest-first order).
                if let Some(start) = self.annex_draw(n) {
                    return Ok(start);
                }
                // GC-21: a run no single source holds may still be free
                // across several of them — the tree records the gate admits
                // and the annex pool — as LMDB finds by merging the records
                // it reads before searching.
                if n > 1 {
                    if let Some(start) = self.cross_record_run(n)? {
                        return Ok(start);
                    }
                }
            }
            AllocMode::GcSave => {
                // GC-12 (ADR-0005): never *read* the GC tree
                // in-save, but draws from the already-loaded drain pool are
                // permitted — required for bounded file growth, since the
                // in-save COW of a committed GC page can never reuse its own
                // predecessor (TXN-62: the N−1 meta still references it) and
                // would otherwise extend the file on every commit. Each entry
                // touched here is re-rewritten before the fixed point
                // (`save_touched`), so no rewritten entry lists a handed-out
                // page (the anti-leak property GC-12 exists for).
                if let Some(start) = self.save_pool_draw(n) {
                    return Ok(start);
                }
                // The carried annex pool (SPEC 05 §2a GC-30): gate-checked
                // like every annex draw; the step-(c) placement re-reads
                // `annex.live()` after every put, so a draw here can never
                // leave a handed-out id in the persisted set — the GC-12
                // pool argument. Skipped while `freelist_save` grows its own
                // entry after a shrink (`save_outside`, GC-13).
                if !self.save_outside {
                    if let Some(start) = self.annex_draw(n) {
                        return Ok(start);
                    }
                }
            }
        }
        let mp = map_pages(self.env.inner().map_size(), self.psize);
        if is_map_full(self.next_pgno, n, mp) {
            return Err(Error::Mdb(MdbError::MapFull));
        }
        let p = self.next_pgno;
        self.next_pgno += n;
        Ok(p)
    }

    /// Record the run `start .. start + n`, just drawn from a GC source, in
    /// `reclaimed` (it feeds the TXN-62 checks and the loose classification
    /// in [`RwTxn::free_page`]).
    fn mark_reclaimed(&mut self, start: u64, n: u64) {
        let reclaimed = &mut work_mut!(self).reclaimed;
        for p in start..start + n {
            let first_time = reclaimed.insert(p);
            debug_assert!(first_time, "page {p} reclaimed twice (INV-24)");
        }
    }

    /// Serve a contiguous `n`-page run from the loose list (GcSave only; see
    /// [`RwTxn::allocate`]). A loose run that abuts `next_pgno` may be
    /// completed by extension. Returns the run's head pgno.
    #[inline(never)]
    pub(super) fn loose_run(&mut self, n: u64) -> Option<u64> {
        debug_assert!(self.alloc_mode == AllocMode::GcSave);
        let mp = map_pages(self.env.inner().map_size(), self.psize);
        // Borrows only the `work` field, so `next_pgno` stays usable.
        let loose = &mut work_mut!(self).loose;
        if loose.is_empty() {
            return None;
        }
        loose.sort_unstable();
        debug_assert!(
            loose.windows(2).all(|w| w[0] < w[1]),
            "loose list holds a duplicate pgno"
        );
        let mut i = 0;
        while i < loose.len() {
            // Extent of the maximal consecutive run starting at index i.
            let mut j = i + 1;
            while j < loose.len() && loose[j] == loose[j - 1] + 1 {
                j += 1;
            }
            let len = (j - i) as u64;
            if len >= n {
                let start = loose[i];
                loose.drain(i..i + n as usize);
                return Some(start);
            }
            // Top-of-file completion: the run ends at next_pgno - 1, so
            // extending by (n - len) yields one contiguous run.
            if loose[j - 1] + 1 == self.next_pgno && !is_map_full(self.next_pgno, n - len, mp) {
                let start = loose[i];
                loose.drain(i..j);
                self.next_pgno += n - len;
                return Some(start);
            }
            i = j;
        }
        None
    }

    /// In-save draw from the **already-loaded drain pool** (GC-12;
    /// see [`RwTxn::allocate`]). Deterministic like GC-19: smallest
    /// reclaimable `F` first (`drains` is a `BTreeMap`), smallest id / first
    /// contiguous run within it. Gate compliance is inherited: every pool
    /// entry passed `F ≤ oldest_reader()` when first loaded during ops, and
    /// any reader that pins *after* that pins the published snapshot
    /// `≥ writer_txnid − 1 ≥ F`, from whose trees these pages are absent.
    #[inline(never)]
    pub(super) fn save_pool_draw(&mut self, n: u64) -> Option<u64> {
        debug_assert!(self.alloc_mode == AllocMode::GcSave);
        let mut hit: Option<(u64, u64)> = None;
        for (f, remaining) in &self.drains {
            if let Some(start) = find_run(remaining.live(), n) {
                hit = Some((*f, start));
                break;
            }
        }
        let (f, start) = hit?;
        // Reader-table shadow check: gate compliance is inherited (doc above), but a
        // fresh table re-scan re-proves it at the hand-out moment.
        #[cfg(debug_assertions)]
        self.debug_assert_gate(f);
        let remaining = self.drains.get_mut(&f).expect("pool entry present");
        remaining.take_run(start, n);
        self.mark_reclaimed(start, n);
        self.save_touched.insert(f);
        Some(start)
    }

    /// Allocate step 2b (SPEC 05 §2a GC-31, ADR-0022): draw from the base
    /// meta's free-list annex pool. The pool's freeing-txnid is `base.txnid`
    /// (= `writer_txnid − 1`), so the gate `F ≤ oldest_reader()` admits the
    /// draw exactly when no live reader is pinned **below** the base — a
    /// reader pinned *at* the base is safe (pages freed by `base` left its
    /// trees), and the crash-fallback meta is `base` itself, whose annex
    /// still lists the drawn ids as free (the TXN-62 / GC-12 pool argument
    /// verbatim). Deterministic like GC-19: smallest id / first contiguous
    /// run of the (ascending) pool. Serves both allocation modes: during
    /// ops as step 2b, and inside `freelist_save` as the carried pool
    /// (GC-30 — the step-(c) placement re-reads `annex.live()` after every
    /// tree write, so an in-save draw is always re-accounted).
    #[inline(never)]
    pub(super) fn annex_draw(&mut self, n: u64) -> Option<u64> {
        if self.annex.len() == 0 {
            return None;
        }
        let f = self.base().txnid;
        if f > self.oldest_reader() {
            return None; // GC-31 gate: a reader is pinned below the base.
        }
        let start = find_run(self.annex.live(), n)?;
        // Reader-table shadow check, as in every other draw arm.
        #[cfg(debug_assertions)]
        self.debug_assert_gate(f);
        self.annex.take_run(start, n);
        self.mark_reclaimed(start, n);
        Some(start)
    }

    /// The GC reuse gate (SPEC 05 GC-18, SPEC 04 TXN-20/21): `min(smallest
    /// live reader snapshot txnid, writer_txnid − 1)`. The reader term is the
    /// lock-free reader-table SeqCst scan
    /// ([`crate::env::EnvInner::oldest_live_reader`]); this is its only
    /// consumer. Cached per write txn (TXN-22; ADR-0006): the
    /// first draw scans, later draws reuse — always sound because a stale
    /// gate is only ever *smaller* (readers that release mid-txn are not
    /// reclaimed-against; readers that pin mid-txn pin `≥ commit point =
    /// txnid − 1 ≥` the cache, so the cache never overshoots a new pin).
    pub(super) fn oldest_reader(&mut self) -> u64 {
        if let Some(o) = self.oldest_cache {
            return o;
        }
        let cap = self.txnid - 1;
        let o = match self.env.inner().oldest_live_reader() {
            Some(r) => r.min(cap),
            None => cap,
        };
        self.oldest_cache = Some(o);
        o
    }

    /// Debug shadow tracking of the GC gate (GC never reclaims a page a live
    /// reader can reach): at the moment pages from GC entry `F` are handed out,
    /// re-scan the reader table **fresh** (SeqCst, not the per-txn cache) and
    /// assert no live reader is pinned below `F`. A snapshot `t` references a
    /// page freed by `F` iff `t < F` (the page left `F`'s tree and every
    /// later one), so `F ≤ min(live pins)` is exactly "no live reader can
    /// reach any page being drawn". Catches a gate bug at the reclaim site,
    /// not via downstream corruption. Debug builds only.
    ///
    /// Caveat: this re-scan is itself just a set of SeqCst loads subject to
    /// the same memory-model visibility rules as the real gate — it is a
    /// debug *aid*, not an independent oracle. A pin it fails to observe is
    /// exactly a pin the TXN-20 proof already covers (mid-pin ⇒ `≥ N − 1`),
    /// and a stale read of a just-released slot only makes the assert
    /// stricter (conservative), so it can produce no false confidence and no
    /// false alarm — but it also cannot detect ordering bugs the gate itself
    /// would miss on the same hardware.
    #[cfg(debug_assertions)]
    pub(super) fn debug_assert_gate(&self, f: u64) {
        // `F ≤ writer_txnid − 1`, spelled `<` for clippy (same predicate).
        assert!(
            f < self.txnid,
            "GC drew from entry F={f} > writer_txnid-1={} (TXN-20 seed violated)",
            self.txnid - 1
        );
        if let Some(min_pin) = self.env.inner().oldest_live_reader() {
            assert!(
                f <= min_pin,
                "GC gate violated: drew pages freed by F={f} while a live reader is pinned at \
                 {min_pin} < F — that snapshot still references them (TXN-20/21, ADR-0006)"
            );
        }
    }

    /// GC-16 step 2 (`gc_reclaim`, SPEC 05 GC-18..20): walk GC entries in
    /// ascending freeing-txnid order (BE keys ⇒ forward cursor); for the first
    /// entry `F ≤ oldest_reader()` whose remaining ids satisfy the request
    /// (smallest id for `n == 1` — GC-19; first within-PIL contiguous run for
    /// `n > 1` — GC-15/21), record the drain in `self.drains` (the tree entry
    /// itself is rewritten only at C1, GC-20) and return the head pgno.
    #[inline(never)]
    pub(super) fn gc_reclaim(&mut self, n: u64) -> Result<Option<u64>> {
        debug_assert!(
            self.alloc_mode == AllocMode::Normal,
            "GC draw inside freelist_save (GC-12)"
        );
        if self.free_db.root == PGNO_INVALID {
            return Ok(None);
        }
        let oldest = self.oldest_reader();
        // Scan pass (shared borrows only); the drain mutation happens after
        // the cursor borrow ends.
        struct Pick {
            f: u64,
            fresh: Option<Vec<u64>>,
            start: u64,
        }
        let last_pg = self.committed_last_pg;
        let mut pick: Option<Pick> = None;
        self.visit_gated_records(oldest, |f, drain, val| {
            let (fresh, start) = match drain {
                Some(d) => (None, find_run(d.live(), n)),
                None => {
                    let ids = decode_gc_ids(val, last_pg)?;
                    let start = find_run(&ids, n);
                    (Some(ids), start)
                }
            };
            Ok(match start {
                Some(start) => {
                    pick = Some(Pick { f, fresh, start });
                    ControlFlow::Break(())
                }
                None => ControlFlow::Continue(()),
            })
        })?;
        let Some(Pick { f, fresh, start }) = pick else {
            return Ok(None);
        };
        // Reader-table shadow check: re-scan the live reader table at the hand-out
        // moment (fresh, not the per-txn cache) — see `debug_assert_gate`.
        #[cfg(debug_assertions)]
        self.debug_assert_gate(f);
        let remaining = match fresh {
            Some(ids) => self.drains.entry(f).or_insert(Drain::new(ids)),
            None => self.drains.get_mut(&f).expect("drain entry present"),
        };
        remaining.take_run(start, n);
        self.mark_reclaimed(start, n);
        Ok(Some(start))
    }

    /// Walk the GC tree's records oldest first (BE keys, so a forward
    /// cursor), stopping at the reuse gate: the first `F > oldest` ends the
    /// walk, as no older record remains (GC-18). `visit` gets each record's
    /// `F`, this txn's drain of it if any, and its raw PIL value, and returns
    /// `Break` to stop early.
    fn visit_gated_records(
        &self,
        oldest: u64,
        mut visit: impl FnMut(u64, Option<&Drain>, &[u8]) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        if self.free_db.root == PGNO_INVALID {
            return Ok(());
        }
        let rec = self.free_db;
        let tree = Tree::new(self.source(), self.psize, rec.root, rec.depth);
        let mut cursor = tree.cursor();
        let mut entry = cursor.first().map_err(map_page_err)?;
        while let Some((key, val)) = entry {
            let f = gc_key_decode(key).ok_or(Error::Mdb(MdbError::Invalid))?;
            if f > oldest || visit(f, self.drains.get(&f), val)?.is_break() {
                break;
            }
            entry = cursor.next().map_err(map_page_err)?;
        }
        Ok(())
    }

    /// Allocate step 2c (SPEC 05 GC-16/GC-21): a run of `n > 1` pages whose
    /// pages come from more than one freed-page source, tried only after the
    /// per-record search ([`RwTxn::gc_reclaim`]) and the annex pool
    /// ([`RwTxn::annex_draw`]) found no run inside any single source.
    ///
    /// The sources are the GC-tree records the gate admits (`F ≤
    /// oldest_reader()`), read oldest first, at most
    /// `n * CROSS_RUN_RECORDS_PER_PAGE` of them, then the annex pool when
    /// its own gate (GC-31) admits it. Their live ids are merged into one
    /// sorted list, as LMDB merges the records it reads into its in-memory
    /// free list, and the first run of `n` consecutive ids from the low end
    /// is taken. Each source keeps its own record (GC-20): the run is removed
    /// from every source it came from — tree records through `drains`, the
    /// pool through `annex` — so `freelist_save` rewrites each remainder
    /// under its own key exactly as for a single-record draw. A page is free
    /// in exactly one source (INV-24), so no page is handed out twice, and
    /// every source's share is removed, so none leaks.
    ///
    /// The merged list is built once per txn and kept ([`CrossRunCache`]),
    /// as LMDB keeps its in-memory free list for the whole txn. Ids the
    /// other draw arms take afterwards stay in it until a candidate run
    /// meets them: every draw records its pages in `reclaimed`, so a
    /// candidate is checked against it and a taken id is dropped there.
    ///
    /// Every page drawn passed the same gate as a single-record draw, so no
    /// live snapshot and no crash-fallback meta can reach it (GC-18).
    #[inline(never)]
    pub(super) fn cross_record_run(&mut self, n: u64) -> Result<Option<u64>> {
        debug_assert!(n > 1 && self.alloc_mode == AllocMode::Normal);
        if self.cross_miss.is_some_and(|m| n >= m) {
            return Ok(None);
        }
        let run_len = usize::try_from(n).map_err(|_| Error::Mdb(MdbError::Invalid))?;
        let budget = n.saturating_mul(CROSS_RUN_RECORDS_PER_PAGE);
        let mut cache = match self.cross_cache.take() {
            // A cache that read every gated record, or at least as many as
            // this request may read, serves it.
            Some(c) if !c.truncated || c.budget >= budget => c,
            _ => self.build_cross_cache(budget)?,
        };
        // Search from the low end. A candidate whose ids all are still free
        // is the pick; an id another draw arm took since the cache was built
        // is dropped, and the search resumes at the first window it could
        // have hidden.
        let mut from = 0usize;
        let hit = loop {
            let ids = &cache.merged;
            let Some(at) = (from..ids.len().saturating_sub(run_len - 1))
                .find(|&i| ids[i + run_len - 1].0 == ids[i].0 + (n - 1))
            else {
                break None;
            };
            match (at..at + run_len).find(|&j| work_mut!(self).reclaimed.contains(&ids[j].0)) {
                None => break Some(at),
                Some(j) => {
                    cache.merged.remove(j);
                    from = j.saturating_sub(run_len - 1);
                }
            }
        };
        let Some(at) = hit else {
            if !cache.truncated {
                self.cross_miss = Some(self.cross_miss.map_or(n, |m| m.min(n)));
            }
            self.cross_cache = Some(cache);
            return Ok(None);
        };
        let start = cache.merged[at].0;
        // Each source's share of the run: its first id and its count. A
        // source's ids inside the run are consecutive positions of its own
        // list (`Drain::take_span`).
        let mut shares: Vec<(u32, u64, usize)> = Vec::new();
        for &(p, i) in &cache.merged[at..at + run_len] {
            match shares.iter_mut().find(|s| s.0 == i) {
                Some(s) => s.2 += 1,
                None => shares.push((i, p, 1)),
            }
        }
        cache.merged.drain(at..at + run_len);
        debug_assert!(shares.len() >= 2, "a single-source run was missed upstream");
        for (i, first, count) in shares {
            match &mut cache.sources[i as usize] {
                CrossSource::Tree { f, fresh } => {
                    let f = *f;
                    // Reader-table shadow check, as in every other draw arm.
                    #[cfg(debug_assertions)]
                    self.debug_assert_gate(f);
                    // A record first drawn here enters `drains` with the ids
                    // the cache read; if `gc_reclaim` loaded it meanwhile,
                    // its entry is the live one.
                    let drain = match fresh.take() {
                        Some(ids) => self.drains.entry(f).or_insert(Drain::new(ids)),
                        None => self.drains.get_mut(&f).expect("drain entry present"),
                    };
                    drain.take_span(first, count);
                }
                CrossSource::Annex => {
                    #[cfg(debug_assertions)]
                    self.debug_assert_gate(self.base().txnid);
                    self.annex.take_span(first, count);
                }
            }
        }
        self.cross_cache = Some(cache);
        self.mark_reclaimed(start, n);
        Ok(Some(start))
    }

    /// Build the cross-record run search's merged list
    /// ([`RwTxn::cross_record_run`]): the live ids of the gated GC-tree
    /// records, oldest first and at most `budget` of them, and of the annex
    /// pool when GC-31 admits it, each tagged with its source.
    pub(super) fn build_cross_cache(&mut self, budget: u64) -> Result<CrossRunCache> {
        let oldest = self.oldest_reader();
        let mut sources: Vec<CrossSource> = Vec::new();
        let mut truncated = false;
        let last_pg = self.committed_last_pg;
        let mut read = 0u64;
        self.visit_gated_records(oldest, |f, drain, val| {
            if read == budget {
                truncated = true;
                return Ok(ControlFlow::Break(()));
            }
            read += 1;
            match drain {
                Some(d) if d.len() == 0 => {}
                Some(_) => sources.push(CrossSource::Tree { f, fresh: None }),
                None => {
                    let ids = decode_gc_ids(val, last_pg)?;
                    if !ids.is_empty() {
                        sources.push(CrossSource::Tree {
                            f,
                            fresh: Some(ids),
                        });
                    }
                }
            }
            Ok(ControlFlow::Continue(()))
        })?;
        if self.annex.len() > 0 && self.base().txnid <= oldest {
            sources.push(CrossSource::Annex);
        }
        let mut merged: Vec<(u64, u32)> = Vec::new();
        // Every single source was already searched in full, so a run needs
        // two sources at least; with fewer the list stays empty.
        if sources.len() >= 2 {
            for (i, s) in sources.iter().enumerate() {
                let i = u32::try_from(i).map_err(|_| Error::Mdb(MdbError::Invalid))?;
                let live: &[u64] = match s {
                    CrossSource::Tree {
                        fresh: Some(ids), ..
                    } => ids,
                    CrossSource::Tree { f, fresh: None } => self.drains[f].live(),
                    CrossSource::Annex => self.annex.live(),
                };
                merged.extend(live.iter().map(|&p| (p, i)));
            }
            // The sources' lists are each ascending: the stable sort merges
            // runs.
            merged.sort_by_key(|&(p, _)| p);
            // INV-24: a free page is listed by exactly one source. The
            // records are on-disk data and never trusted (GC-18): a page
            // listed twice would be handed out while still listed free, so a
            // corrupt free list fails the allocation like any other invalid
            // record.
            if merged.windows(2).any(|w| w[0].0 == w[1].0) {
                return Err(Error::Mdb(MdbError::Invalid));
            }
        }
        Ok(CrossRunCache {
            merged,
            sources,
            budget,
            truncated,
        })
    }

    /// Record `pgno` as freed (GC-6), dropping its dirty frame if it has one.
    /// A page this txn allocated (`> committed_last_pg`) — or reclaimed from
    /// the GC DB this txn (drained out of its entry, so no committed snapshot
    /// at-or-after the gate references it) — is loose (GC-7); dropping the
    /// frame here is sound because freeing only happens inside `&mut` ops,
    /// where no borrow into the frame can be live (TXN-39/43).
    pub(super) fn free_page(&mut self, pgno: u64) {
        // SPEC 03 §6.6 F3 ownership hook: a freed page may be re-served to
        // another tree while staying dirty — no finger may keep naming it.
        self.finger_forget(pgno, 1);
        if pgno > self.committed_last_pg || work!(self).reclaimed.contains(&pgno) {
            self.dirty.discard(pgno);
            work_mut!(self).loose.push(pgno);
        } else {
            // A page of the base snapshot. Every dirty or spilled page was
            // allocated by this txn, so it passes the test above (the TXN-62
            // invariant `write_frames` asserts): this one has no frame to
            // drop, and freeing it is a list append, as in LMDB.
            debug_assert!(
                !self.dirty.contains(pgno) && self.dirty.spilled_pages(pgno).is_none(),
                "committed page {pgno} has a dirty or spilled frame"
            );
            work_mut!(self).freed.push(pgno);
        }
    }

    /// Free a whole overflow run (SPEC 03 §8: all `n` pgnos).
    pub(super) fn free_run(&mut self, head: u64, n: u64) {
        // SPEC 03 §6.6 F3 ownership hook, as in `free_page`.
        self.finger_forget(head, n);
        // Run frames are multi-page; `discard` drops them rather than pooling.
        self.dirty.discard(head);
        for p in head..head + n {
            if p > self.committed_last_pg || work_mut!(self).reclaimed.contains(&p) {
                work_mut!(self).loose.push(p);
            } else {
                work_mut!(self).freed.push(p);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_run_picks_deterministically() {
        // GC-19: n == 1 -> smallest id.
        assert_eq!(find_run(&[5, 9, 10, 11], 1), Some(5));
        assert_eq!(find_run(&[], 1), None);
        // GC-15/21: first contiguous run of length >= n.
        assert_eq!(find_run(&[5, 9, 10, 11], 3), Some(9));
        assert_eq!(find_run(&[5, 9, 10, 11], 2), Some(9));
        assert_eq!(find_run(&[5, 9, 10, 11], 4), None);
        // A longer run serves a shorter request from its head.
        assert_eq!(find_run(&[2, 3, 4, 5, 6], 3), Some(2));
        // Gaps split runs.
        assert_eq!(find_run(&[2, 4, 6, 8], 2), None);
    }

    /// GC-19 common case: a front draw advances `head` (no memmove) and the
    /// live view shrinks from the front.
    #[test]
    fn drain_front_pop_advances_head() {
        let mut d = Drain::new(vec![5, 9, 10, 11]);
        d.take_run(5, 1);
        assert_eq!(d.live(), &[9, 10, 11]);
        assert_eq!(
            (d.head, d.ids.as_slice().len()),
            (1, 4),
            "front pop must not memmove"
        );
        // A front *run* draw also advances head, by n.
        d.take_run(9, 2);
        assert_eq!(d.live(), &[11]);
        assert_eq!((d.head, d.ids.as_slice().len()), (3, 4));
        assert_eq!(d.len(), 1);
    }

    /// The rare mid-slice run removal (n > 1, run not at the live front)
    /// falls back to `Vec::drain` and preserves order around the hole.
    #[test]
    fn drain_mid_slice_run_removal() {
        let mut d = Drain::new(vec![2, 6, 7, 8, 12]);
        // Consume the front first so the mid removal happens behind a
        // non-zero head.
        d.take_run(2, 1);
        assert_eq!(d.live(), &[6, 7, 8, 12]);
        let start = find_run(d.live(), 2).unwrap();
        assert_eq!(start, 6);
        // Remove [7, 8] — a run strictly inside the live slice.
        d.take_run(7, 2);
        assert_eq!(d.live(), &[6, 12]);
        assert_eq!(d.len(), 2);
    }

    /// A record's share of a run drawn across records (GC-21): its ids inside
    /// the run interleave with another record's, so they are consecutive
    /// positions of its own list but not consecutive page numbers.
    #[test]
    fn drain_take_span_removes_a_records_share_of_a_cross_record_run() {
        // Records A and B interleave over the run 11..=15.
        let mut a = Drain::new(vec![4, 11, 13, 15, 20]);
        let mut b = Drain::new(vec![12, 14, 30]);
        a.take_span(11, 3);
        b.take_span(12, 2);
        assert_eq!(a.live(), &[4, 20]);
        assert_eq!(b.live(), &[30]);
        // From the front: advances the consumed prefix, as a front run does.
        let mut c = Drain::new(vec![7, 9, 11, 40]);
        c.take_span(7, 3);
        assert_eq!(c.live(), &[40]);
        assert_eq!(
            (c.head, c.ids.as_slice().len()),
            (3, 4),
            "front span must not memmove"
        );
    }

    /// The base annex is drained without a copy until a mid-slice removal
    /// needs one; the shared ids themselves never change.
    #[test]
    fn drain_shared_copies_only_on_a_mid_slice_removal() {
        let base: Arc<[u64]> = Arc::from(&[2u64, 3, 6, 7, 8, 12][..]);
        let mut d = Drain::shared(Arc::clone(&base));
        d.take_run(2, 2);
        assert!(matches!(d.ids, DrainIds::Shared(_)), "front draw shares");
        assert_eq!(d.live(), &[6, 7, 8, 12]);
        d.take_run(7, 2);
        assert!(matches!(d.ids, DrainIds::Owned(_)), "mid-slice draw copies");
        assert_eq!(d.live(), &[6, 12]);
        assert_eq!(d.len(), 2);
        assert_eq!(&*base, &[2, 3, 6, 7, 8, 12]);
    }

    /// Fully drained: the live view is empty however the ids were consumed —
    /// the state `freelist_save` step (a) turns into a tree-entry delete.
    #[test]
    fn drain_fully_drained_is_empty() {
        let mut d = Drain::new(vec![3, 4, 5]);
        d.take_run(3, 2); // front run
        d.take_run(5, 1); // last id
        assert_eq!(d.live(), &[] as &[u64]);
        assert_eq!(d.len(), 0);

        let mut d = Drain::new(vec![3, 4, 5]);
        d.take_run(4, 2); // mid/tail run first
        d.take_run(3, 1);
        assert_eq!(d.live(), &[] as &[u64]);
    }

    /// GC-20 byte-identity guarantee: for a mixed draw sequence, the live
    /// remainder equals what a naive representation (binary_search +
    /// `Vec::drain` on every draw) would encode into the rewritten PIL.
    #[test]
    fn drain_remainder_matches_naive_drain() {
        // Reference: the naive drain-on-every-draw algorithm.
        fn naive_take(ids: &mut Vec<u64>, start: u64, n: u64) {
            let pos = ids.binary_search(&start).unwrap();
            ids.drain(pos..pos + n as usize);
        }
        // Ascending ids with runs and gaps.
        let init: Vec<u64> = vec![2, 3, 4, 5, 8, 9, 10, 14, 15, 20, 21, 22, 23, 30];
        let mut d = Drain::new(init.clone());
        let mut naive = init;
        // Draw with GC-19/GC-18 pick rules (n == 1 -> front; n > 1 -> first
        // run), interleaving sizes so front pops and mid-slice removals mix.
        for n in [1u64, 3, 1, 2, 1, 4, 1, 1, 2] {
            let pick = find_run(d.live(), n);
            assert_eq!(pick, find_run(&naive, n), "pick diverged at n={n}");
            let Some(start) = pick else { continue };
            d.take_run(start, n);
            naive_take(&mut naive, start, n);
            assert_eq!(d.live(), naive.as_slice(), "remainder diverged at n={n}");
        }
        assert_eq!(d.live(), &[] as &[u64], "sequence sized to drain fully");
        assert!(naive.is_empty());
    }
}
