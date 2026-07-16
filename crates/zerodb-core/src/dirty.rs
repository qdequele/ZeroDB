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

use std::collections::HashMap;

/// A write txn's dirty-page frames, indexed by pgno. See the module docs for
/// the stability contract.
#[derive(Debug)]
pub struct DirtyStore {
    psize: u32,
    frames: HashMap<u64, Box<[u8]>>,
}

impl DirtyStore {
    /// An empty store for pages of size `psize`.
    #[must_use]
    pub fn new(psize: u32) -> DirtyStore {
        DirtyStore {
            psize,
            frames: HashMap::new(),
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
            !frame.is_empty() && frame.len() % self.psize as usize == 0,
            "frame length {} must be a positive multiple of psize {}",
            frame.len(),
            self.psize
        );
        self.frames.insert(pgno, frame);
    }

    /// Insert a fresh zeroed one-page tree frame for `pgno` and return it
    /// mutably (the caller formats it as a leaf/branch page).
    pub fn insert_tree_frame(&mut self, pgno: u64) -> &mut [u8] {
        let frame = vec![0u8; self.psize as usize].into_boxed_slice();
        self.frames.insert(pgno, frame);
        self.frames
            .get_mut(&pgno)
            .map(|b| &mut **b)
            .expect("frame was just inserted")
    }

    /// Remove and return the frame for `pgno` (freed within the txn, GC-7/8;
    /// or rebound by loose-page reuse). Only called from `&mut` ops (TXN-43).
    pub fn remove(&mut self, pgno: u64) -> Option<Box<[u8]>> {
        self.frames.remove(&pgno)
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
}
