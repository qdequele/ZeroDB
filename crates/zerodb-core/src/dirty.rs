//! The write transaction's dirty-page store (SPEC 04 §6.3, ADR-0004 D1).
//! Milestone 1.4.
//!
//! A [`DirtyStore`] holds a write txn's private, not-yet-committed page copies
//! as **individually stable frames** indexed by pgno:
//!
//! - a **tree page** (leaf/branch) is one `psize`-byte `Box<[u8]>` frame;
//! - an **overflow run** is one contiguous `n * psize`-byte `Box<[u8]>` frame,
//!   keyed by its **head** pgno (interior pages of a run have no entry — runs
//!   are only ever addressed through their head, SPEC 02 §5).
//!
//! **The stability guarantee (TXN-41):** a `Box<[u8]>` never reallocates, and
//! growing/rehashing the `HashMap` index moves only the `Box` handle (a
//! pointer), never the bytes it owns (TXN-44). So a `&'txn [u8]` borrowed from
//! a frame stays valid until the frame itself is removed — which only happens
//! inside a `&mut` operation on the owning txn, when no such borrow can be
//! live (TXN-39, enforced by the borrow checker: reads take `&RwTxn`, writes
//! take `&mut RwTxn`).
//!
//! Frames of pages freed within the txn are dropped at the free point — a
//! `&mut` boundary, so no borrow can alias them (TXN-43's soundness condition
//! is met by construction; the store never drops a frame under a `&self`
//! borrow). This module is pure safe Rust with no I/O; `miri` exercises it
//! (TXN-49).
//!
//! **Frame reuse (PERF-GAP B3/B12; LMDB's `me_dpages` pool).** A freed or
//! discarded one-page frame is not returned to the allocator immediately;
//! [`DirtyStore::discard`] parks it on a bounded `spare` list, and
//! [`DirtyStore::insert_tree_frame`] / [`DirtyStore::insert_copy`] reuse a
//! spare (zero-filling or overwriting it) before falling back to a fresh
//! allocation. This keeps the stability contract intact — spares are still
//! individually boxed and only move to/from `spare` inside `&mut` ops — while
//! avoiding a slow-path `malloc`+`free` of a full page (above glibc's tcache
//! bin) per touched or new page. Run frames (multi-page) are never pooled;
//! they are just dropped. The env hands a store its initial spares at
//! write-txn begin and reclaims them at end (see [`DirtyStore::with_spare`] /
//! [`DirtyStore::reclaimable_frames`]).
//!
//! **Spilling (SPEC 04 §6.3a, ADR-0017; LMDB `mdb_page_spill`).** The store
//! counts the pages its frames hold ([`DirtyStore::pages`]). Past the env's
//! dirty limit the write txn writes some frames to the file and hands them to
//! [`DirtyStore::spill`], which drops the frame and records the pgno as
//! **spilled** with its page count; reads then resolve it from the map, and
//! [`DirtyStore::unspill_copy`] brings a tree page back into a frame at the
//! same pgno when the txn writes it again. A pgno is never both a frame and
//! spilled. Like frame removal, spilling happens only inside `&mut` ops.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

/// Pgno hasher (PERF-GAP issue #9): every page load inside a write txn
/// probes this store first (`Source::Writer`), and the hannoy-build call
/// tree put the default `RandomState` SipHash among the top descent costs.
/// Keys are page numbers authored by the engine itself — never
/// attacker-controlled input — so SipHash's HashDoS resistance buys nothing
/// here. This uses the splitmix64 finalizer (same full-avalanche 3-multiply
/// mixer as `btree::ValidatedPages::mix`), which also mixes the low bits
/// hashbrown's control bytes rely on.
#[derive(Default)]
pub(crate) struct PgnoHasher(u64);

impl Hasher for PgnoHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write_u64(&mut self, n: u64) {
        // splitmix64 finalizer.
        let mut z = n;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        self.0 = z ^ (z >> 31);
    }

    fn write(&mut self, bytes: &[u8]) {
        // Unused for the `u64` keys this store indexes (`u64::hash` calls
        // `write_u64`); kept correct (FNV-1a fold) so any future key shape
        // degrades gracefully instead of panicking.
        for &b in bytes {
            self.0 = (self.0 ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01B3);
        }
    }
}

pub(crate) type PgnoBuildHasher = BuildHasherDefault<PgnoHasher>;

/// Cap on pooled one-page frames — both a store's `spare` list and the
/// env-level pool it draws from (PERF-GAP B3/B12).
///
/// LMDB's `me_dpages` pool has no such cap because LMDB *spills* dirty pages
/// (`mdb_page_spill`), so its dirty set — and therefore its pool — is bounded
/// (`MDB_IDL_UM_MAX`). ZeroDB has no spill: the whole dirty set lives in RAM
/// until commit (SPEC 04), so an uncapped pool would retain a large indexing
/// txn's entire dirty set as spares for the rest of the env's life. 256
/// one-page frames is ≈ 1 MiB at a 4 KiB page size — enough to serve the
/// small, bursty write txns that dominate, cheap to hold onto.
pub(crate) const SPARE_CAP: usize = 256;

/// A write txn's dirty-page frames, indexed by pgno. See the module docs for
/// the stability contract.
#[derive(Debug)]
pub struct DirtyStore {
    psize: u32,
    /// `log2(psize)` (page sizes are powers of two, SPEC 02 §3.2): turns a
    /// frame length into its page count with a shift, not a division, on the
    /// discard/remove path deletes take once per freed page.
    psize_shift: u32,
    frames: HashMap<u64, Box<[u8]>, PgnoBuildHasher>,
    /// Recycled one-page frames (exactly `psize` bytes each), reused by
    /// `insert_tree_frame` / `insert_copy` before allocating (PERF-GAP B3).
    /// Frames only move to/from here inside `&mut` ops, preserving the TXN-41
    /// stability contract; bounded by [`SPARE_CAP`].
    spare: Vec<Box<[u8]>>,
    /// Pages held by `frames` (a run frame counts its `n` pages): the
    /// quantity the dirty limit bounds (SPEC 04 TXN-68).
    pages: u64,
    /// Spilled pgnos (SPEC 04 TXN-69..72) → page count (1 for a tree page,
    /// `n` for an overflow-run head). Disjoint from `frames`.
    spilled: HashMap<u64, u64, PgnoBuildHasher>,
}

impl DirtyStore {
    /// An empty store for pages of size `psize`.
    #[must_use]
    pub fn new(psize: u32) -> DirtyStore {
        debug_assert!(psize.is_power_of_two());
        DirtyStore {
            psize,
            psize_shift: psize.trailing_zeros(),
            frames: HashMap::default(),
            spare: Vec::new(),
            pages: 0,
            spilled: HashMap::default(),
        }
    }

    /// An empty store seeded with a pool of recycled one-page frames (the
    /// env's `me_dpages`-style pool handed in at write-txn begin, PERF-GAP
    /// B12). The `Vec` is adopted as is, O(1): every write txn, empty ones
    /// included, pays for the pool hand-over, so it must not scale with the
    /// pool size (a per-frame filter here made `env/txn/rw_empty_commit` 6×
    /// slower). The pool only ever holds frames of this env's page size.
    #[must_use]
    pub fn with_spare(psize: u32, mut spare: Vec<Box<[u8]>>) -> DirtyStore {
        debug_assert!(spare.iter().all(|f| f.len() == psize as usize));
        spare.truncate(SPARE_CAP);
        debug_assert!(psize.is_power_of_two());
        DirtyStore {
            psize,
            psize_shift: psize.trailing_zeros(),
            frames: HashMap::default(),
            spare,
            pages: 0,
            spilled: HashMap::default(),
        }
    }

    /// Whether `pgno` has a dirty frame (tree page, or overflow-run **head**).
    #[must_use]
    pub fn contains(&self, pgno: u64) -> bool {
        self.frames.contains_key(&pgno)
    }

    /// Number of dirty frames (runs count once).
    #[must_use]
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// Whether the store holds no frames.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Pages held in frames (a run counts its `n` pages; SPEC 04 TXN-68).
    #[must_use]
    pub fn pages(&self) -> u64 {
        self.pages
    }

    /// Pages a frame of `len` bytes holds.
    #[inline]
    fn pages_of(&self, len: usize) -> u64 {
        (len >> self.psize_shift) as u64
    }

    /// Whether any page of this txn is spilled (SPEC 04 §6.3a). The cheap
    /// gate in front of every spilled-page lookup.
    #[must_use]
    #[inline]
    pub fn has_spills(&self) -> bool {
        !self.spilled.is_empty()
    }

    /// The page count of spilled `pgno` (1 for a tree page, `n` for a run
    /// head), or `None` if it is not spilled.
    #[must_use]
    pub fn spilled_pages(&self, pgno: u64) -> Option<u64> {
        if self.spilled.is_empty() {
            return None;
        }
        self.spilled.get(&pgno).copied()
    }

    /// Number of spilled pgnos (runs count once).
    #[must_use]
    pub fn spilled_len(&self) -> usize {
        self.spilled.len()
    }

    /// Every frame as `(pgno, pages)` — the spill candidate set.
    pub fn frame_extents(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        let ps = self.psize as usize;
        self.frames
            .iter()
            .map(move |(&p, f)| (p, (f.len() / ps) as u64))
    }

    /// Record that `pgno`'s frame has been written to the file and release it
    /// (SPEC 04 TXN-69): the frame goes to the spare pool (or is dropped) and
    /// `pgno` becomes spilled. The caller must have written the frame's
    /// current bytes at `pgno` first. A `&mut` op, like [`DirtyStore::discard`].
    pub fn spill(&mut self, pgno: u64) {
        if let Some(frame) = self.frames.remove(&pgno) {
            let n = self.pages_of(frame.len());
            self.pages -= n;
            self.spilled.insert(pgno, n);
            if frame.len() == self.psize as usize && self.spare.len() < SPARE_CAP {
                self.spare.push(frame);
            }
        }
    }

    /// Bring spilled tree page `pgno` back into a frame holding `src` (its
    /// bytes as read from the map) and clear its spilled mark (SPEC 04
    /// TXN-72; LMDB `mdb_page_unspill`). Returns the frame mutably. `src` must
    /// be exactly one page.
    pub fn unspill_copy(&mut self, pgno: u64, src: &[u8]) -> &mut [u8] {
        debug_assert_eq!(
            self.spilled.get(&pgno),
            Some(&1),
            "unspill of a spilled tree page"
        );
        self.spilled.remove(&pgno);
        self.insert_copy(pgno, src)
    }

    /// The frame bytes for `pgno`: exactly `psize` for a tree page, the whole
    /// contiguous `n * psize` run for an overflow head.
    #[must_use]
    pub fn bytes(&self, pgno: u64) -> Option<&[u8]> {
        self.frames.get(&pgno).map(|b| &**b)
    }

    /// Mutable frame bytes for `pgno` (a `&mut self` op; TXN-42 in-place edit).
    #[must_use]
    pub fn bytes_mut(&mut self, pgno: u64) -> Option<&mut [u8]> {
        self.frames.get_mut(&pgno).map(|b| &mut **b)
    }

    /// Insert (or replace) the frame for `pgno`. The frame length must be a
    /// non-zero multiple of `psize` (1 page for tree frames, `n` for runs).
    pub fn insert(&mut self, pgno: u64, frame: Box<[u8]>) {
        debug_assert!(
            !frame.is_empty() && frame.len().is_multiple_of(self.psize as usize),
            "frame length {} must be a positive multiple of psize {}",
            frame.len(),
            self.psize
        );
        debug_assert!(
            !self.spilled.contains_key(&pgno),
            "frame for a spilled pgno"
        );
        self.pages += self.pages_of(frame.len());
        if let Some(old) = self.frames.insert(pgno, frame) {
            self.pages -= self.pages_of(old.len());
        }
    }

    /// Insert a fresh zeroed one-page tree frame for `pgno` and return it
    /// mutably (the caller formats it as a leaf/branch page). Reuses a pooled
    /// spare when one is available, zero-filling it — callers rely on the frame
    /// being all-zero (e.g. `ZeroReserve` regions, TXN-47).
    pub fn insert_tree_frame(&mut self, pgno: u64) -> &mut [u8] {
        let frame = match self.spare.pop() {
            Some(mut f) => {
                debug_assert_eq!(f.len(), self.psize as usize, "spare frame is one page");
                f.fill(0);
                f
            }
            None => vec![0u8; self.psize as usize].into_boxed_slice(),
        };
        debug_assert!(
            !self.spilled.contains_key(&pgno),
            "frame for a spilled pgno"
        );
        self.pages += 1;
        if let Some(old) = self.frames.insert(pgno, frame) {
            self.pages -= self.pages_of(old.len());
        }
        self.frames
            .get_mut(&pgno)
            .map(|b| &mut **b)
            .expect("frame was just inserted")
    }

    /// Insert a one-page COW copy of `src` for `pgno` and return it mutably (the
    /// caller restamps the header). Reuses a pooled spare, overwriting it with
    /// `src`, before allocating (PERF-GAP B3). `src` must be exactly one page.
    pub fn insert_copy(&mut self, pgno: u64, src: &[u8]) -> &mut [u8] {
        debug_assert_eq!(
            src.len(),
            self.psize as usize,
            "insert_copy is one-page only"
        );
        let frame = match self.spare.pop() {
            Some(mut f) => {
                debug_assert_eq!(f.len(), self.psize as usize, "spare frame is one page");
                f.copy_from_slice(src);
                f
            }
            None => src.into(),
        };
        debug_assert!(
            !self.spilled.contains_key(&pgno),
            "frame for a spilled pgno"
        );
        self.pages += 1;
        if let Some(old) = self.frames.insert(pgno, frame) {
            self.pages -= self.pages_of(old.len());
        }
        self.frames
            .get_mut(&pgno)
            .map(|b| &mut **b)
            .expect("frame was just inserted")
    }

    /// Remove and return the frame for `pgno` (freed within the txn, GC-7/8;
    /// or rebound by loose-page reuse). Only called from `&mut` ops (TXN-43).
    pub fn remove(&mut self, pgno: u64) -> Option<Box<[u8]>> {
        // A spilled pgno has no frame; freeing one goes through `discard`,
        // which clears the mark (SPEC 04 TXN-72).
        debug_assert!(
            !self.spilled.contains_key(&pgno),
            "remove of a spilled pgno"
        );
        let f = self.frames.remove(&pgno);
        if let Some(f) = &f {
            self.pages -= self.pages_of(f.len());
        }
        f
    }

    /// Discard the frame for `pgno` (freed within the txn). A one-page frame is
    /// parked on the `spare` pool for reuse if there is room; a run frame
    /// (multi-page) or an overflow past [`SPARE_CAP`] is dropped. Like
    /// [`DirtyStore::remove`], only called from `&mut` ops (TXN-43): the frame
    /// leaves `frames` at a `&mut` boundary, so no borrow can alias it.
    ///
    /// Freeing a **spilled** pgno clears its spilled mark (SPEC 04 TXN-72):
    /// the page returns to the txn as loose, like any page it allocated.
    pub fn discard(&mut self, pgno: u64) {
        if let Some(frame) = self.frames.remove(&pgno) {
            self.pages -= self.pages_of(frame.len());
            if frame.len() == self.psize as usize && self.spare.len() < SPARE_CAP {
                self.spare.push(frame);
            }
        } else if !self.spilled.is_empty() {
            self.spilled.remove(&pgno);
        }
    }

    /// Take up to `cap` one-page frames out of this store — spares first, then
    /// remaining one-page dirty frames — to hand back to the env pool at
    /// end-of-txn (PERF-GAP B12). Called only after the commit pipeline has
    /// finished writing (or on abort, where the frames are discarded garbage):
    /// recycled contents never matter because every reuse zero-fills or fully
    /// overwrites. Run frames are left behind to drop with the store.
    #[must_use]
    pub fn reclaimable_frames(&mut self, cap: usize) -> Vec<Box<[u8]>> {
        // The spare `Vec` moves out whole (O(1)); only this txn's dirty frames
        // are walked, so the hand-back costs O(pages dirtied), like LMDB
        // returning its dirty list to `me_dpages`.
        let ps = self.psize as usize;
        let mut out = std::mem::take(&mut self.spare);
        out.truncate(cap);
        if out.len() < cap && !self.frames.is_empty() {
            self.pages = 0;
            for (_, f) in self.frames.drain() {
                if out.len() >= cap {
                    break;
                }
                if f.len() == ps {
                    out.push(f);
                }
            }
        }
        out
    }

    /// Dirty pgnos in ascending order — the commit write-out order (C2 writes
    /// sequentially by pgno; ADR-0004 D1).
    #[must_use]
    pub fn sorted_pgnos(&self) -> Vec<u64> {
        let mut v: Vec<u64> = self.frames.keys().copied().collect();
        v.sort_unstable();
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_are_stable_across_index_growth() {
        // TXN-44: growing the pgno→frame index must not move frame contents.
        let mut s = DirtyStore::new(4096);
        s.insert(7, vec![0xAB; 4096].into_boxed_slice());
        let addr = s.bytes(7).unwrap().as_ptr() as usize;
        // Force many rehashes.
        for pgno in 100..1100 {
            s.insert(pgno, vec![0u8; 4096].into_boxed_slice());
        }
        assert_eq!(s.bytes(7).unwrap().as_ptr() as usize, addr);
        assert_eq!(s.bytes(7).unwrap()[0], 0xAB);
    }

    #[test]
    fn run_frames_are_contiguous() {
        let mut s = DirtyStore::new(4096);
        s.insert(10, vec![1u8; 3 * 4096].into_boxed_slice());
        let run = s.bytes(10).unwrap();
        assert_eq!(run.len(), 3 * 4096);
        // One contiguous slice spanning the whole run (TXN-41).
        assert!(run.iter().all(|&b| b == 1));
        assert!(s.bytes(11).is_none(), "interior pages have no entry");
    }

    #[test]
    fn sorted_pgnos_ascend() {
        let mut s = DirtyStore::new(4096);
        for pgno in [9u64, 2, 40, 3] {
            s.insert(pgno, vec![0u8; 4096].into_boxed_slice());
        }
        assert_eq!(s.sorted_pgnos(), vec![2, 3, 9, 40]);
    }

    #[test]
    fn discarded_one_page_frame_is_reused_and_zeroed() {
        // A discarded one-page frame is handed back to the next allocation
        // (same buffer, no fresh malloc), and `insert_tree_frame` returns it
        // all-zero even though it last held data (PERF-GAP B3).
        let mut s = DirtyStore::new(4096);
        s.insert_copy(7, &[0xAB; 4096]);
        let addr = s.bytes(7).unwrap().as_ptr() as usize;
        s.discard(7);
        // insert_copy reuses the same buffer, overwriting it.
        let reused = s.insert_copy(8, &[0xCD; 4096]).as_ptr() as usize;
        assert_eq!(reused, addr, "insert_copy reused the discarded frame");
        assert!(s.bytes(8).unwrap().iter().all(|&b| b == 0xCD));
        // Discard again; this time a tree frame must come back zeroed.
        s.discard(8);
        let f = s.insert_tree_frame(9);
        assert_eq!(f.as_ptr() as usize, addr, "insert_tree_frame reused it");
        assert!(f.iter().all(|&b| b == 0), "reused tree frame is zeroed");
    }

    #[test]
    fn run_frames_are_not_pooled() {
        // A discarded multi-page (run) frame is dropped, not parked as a spare:
        // the next one-page allocation must allocate fresh, not hand out a
        // slice of the wrong length.
        let mut s = DirtyStore::new(4096);
        s.insert(10, vec![1u8; 3 * 4096].into_boxed_slice());
        s.discard(10);
        let f = s.insert_tree_frame(11);
        assert_eq!(f.len(), 4096, "one-page frame, not the 3-page run");
    }

    #[test]
    fn spare_pool_respects_cap() {
        // No more than SPARE_CAP frames are ever parked; overflow is dropped.
        let mut s = DirtyStore::new(4096);
        for pgno in 0..(SPARE_CAP as u64 + 50) {
            s.insert_tree_frame(pgno);
        }
        for pgno in 0..(SPARE_CAP as u64 + 50) {
            s.discard(pgno);
        }
        assert_eq!(s.spare.len(), SPARE_CAP, "spare list capped at SPARE_CAP");
        // reclaimable_frames also honors its own cap argument.
        let taken = s.reclaimable_frames(10);
        assert_eq!(taken.len(), 10);
    }

    #[test]
    fn with_spare_seeds_and_reclaim_returns_frames() {
        // A store seeded from the env pool reuses those frames; reclaim then
        // hands one-page frames (spare + dirty) back, capped.
        let seed: Vec<Box<[u8]>> = (0..3).map(|_| vec![9u8; 4096].into_boxed_slice()).collect();
        let seed_addr = seed[2].as_ptr() as usize; // last popped first
        let mut s = DirtyStore::with_spare(4096, seed);
        let f = s.insert_tree_frame(1);
        assert_eq!(f.as_ptr() as usize, seed_addr, "seeded spare reused");
        // Two spares left plus one dirty frame => three reclaimable.
        let back = s.reclaimable_frames(SPARE_CAP);
        assert_eq!(back.len(), 3);
        // The spare Vec is handed over whole: its allocation survives the
        // round trip (no per-frame copy into a fresh Vec).
        let pool_ptr = back.as_ptr() as usize;
        let mut s2 = DirtyStore::with_spare(4096, back);
        let back2 = s2.reclaimable_frames(SPARE_CAP);
        assert_eq!(back2.as_ptr() as usize, pool_ptr);
    }
}
