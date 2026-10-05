//! Env-wide cache of validated page versions (ADR-0018; SPEC 04 TXN-38).
//!
//! A txn's validated-pages memo ([`crate::btree::ValidatedPages`]) dies with
//! the txn, so a workload of short txns re-walks the same hot pages' cells on
//! every txn. This cache carries the result across txns, keyed by the page's
//! **version**: the pair (pgno, header txnid stamp). ZeroDB stamps every page
//! it writes with the writing txn's id, writes each pgno at most once per
//! commit, and only reuses a pgno from a later txn, so a (pgno, stamp) pair
//! names one immutable byte image for the env's life. A validation recorded
//! for that pair therefore stays true with no invalidation from the writer:
//! a reused page carries a newer stamp and simply misses.
//!
//! Shape: a fixed, direct-mapped table of [`SLOTS`] slots (slot = pgno
//! modulo [`SLOTS`]; a newer version or a colliding pgno overwrites), allocated in
//! [`CHUNK_SLOTS`]-slot chunks on the first publish that lands in each, so an
//! env pays only for the part of the table its hot pages use. Each slot
//! is a tiny seqlock over two words: a lookup that races a publish, or reads
//! a slot mid-overwrite, sees a changed or odd sequence and reports a miss,
//! which revalidates. A hit hands out a zero-check page view, so the
//! protocol must never pair one publish's key with another's stamp; the
//! loom model below checks exactly that.
//!
//! Stated limit: the cache assumes the file changes only through this
//! process's commits (single-process model). Bytes rewritten underneath the env with a
//! stamp they already carried would be trusted from the cache.

use std::sync::OnceLock;

use crate::sync::{fence, AtomicU64, Ordering};

/// Slots in the table: 64 Ki × 32 bytes = 2 MiB per env at most (ADR-0018).
pub(crate) const SLOTS: usize = 1 << 16;

/// Slots per lazily allocated chunk (32 KiB), so a first publish never
/// zero-fills the whole table (measured 2026-09-29 on `env/open/reopen`).
pub(crate) const CHUNK_SLOTS: usize = 1 << 10;

/// Which validated shape an entry vouches for; part of the key, as in the
/// txn memo, so a page validated as a leaf never hits as a
/// branch.
#[derive(Clone, Copy)]
pub(crate) enum StampKind {
    Leaf,
    Branch,
}

/// One seqlock-guarded entry. `seq` is even when the slot is stable and odd
/// while a publisher owns it; `key` is `(pgno | kind tag) + 1`, so a zeroed
/// slot never matches.
#[repr(align(32))]
struct Slot {
    seq: AtomicU64,
    key: AtomicU64,
    stamp: AtomicU64,
}

impl Slot {
    fn empty() -> Slot {
        Slot {
            seq: AtomicU64::new(0),
            key: AtomicU64::new(0),
            stamp: AtomicU64::new(0),
        }
    }
}

/// The env-wide table (ADR-0018). Owned by the env and borrowed by every
/// read txn.
pub struct StampCache {
    chunks: Box<[OnceLock<Box<[Slot]>>]>,
    /// Slots per chunk; a power of two.
    chunk_slots: usize,
    /// Total slots; a power of two.
    len: usize,
}

impl std::fmt::Debug for StampCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StampCache")
            .field("len", &self.len)
            .field(
                "chunks_allocated",
                &self.chunks.iter().filter(|c| c.get().is_some()).count(),
            )
            .finish()
    }
}

impl StampCache {
    /// An empty cache of [`SLOTS`] slots, allocated chunk by chunk.
    pub(crate) fn new() -> StampCache {
        StampCache::with_slots(SLOTS)
    }

    /// An empty cache of `len` slots (a power of two). Small tables are for
    /// the loom model and collision tests.
    pub(crate) fn with_slots(len: usize) -> StampCache {
        assert!(len.is_power_of_two());
        let chunk_slots = len.min(CHUNK_SLOTS);
        StampCache {
            chunks: (0..len / chunk_slots).map(|_| OnceLock::new()).collect(),
            chunk_slots,
            len,
        }
    }

    fn key_of(pgno: u64, kind: StampKind) -> u64 {
        let tag = match kind {
            StampKind::Leaf => 0,
            StampKind::Branch => 1u64 << 63,
        };
        // pgnos are bounded far below bit 63 (`map_size / page_size`), and
        // the `+1` cannot wrap.
        (pgno | tag) + 1
    }

    /// `(chunk, slot within chunk)` for `pgno`: the pgno itself, masked, not
    /// a hash. Pages the file holds side by side (a bulk-loaded tree's
    /// leaves, a sequential writer's) then sit in adjacent slots, so a scan
    /// probes the table almost sequentially. Hashed slots made a full scan
    /// touch one random line and page of the table per leaf: `scan/full/*`
    /// +5 % from cache and TLB misses (perf, 2026-09-30). A pgno is one kind
    /// at a time, so leaf and branch share its slot.
    fn index(&self, pgno: u64) -> (usize, usize) {
        let i = (pgno as usize) & (self.len - 1);
        (i / self.chunk_slots, i & (self.chunk_slots - 1))
    }

    /// `true` iff a publish of exactly `(pgno, kind, stamp)` is the slot's
    /// current, stable content. Any race or collision answers `false`.
    pub(crate) fn contains(&self, pgno: u64, kind: StampKind, stamp: u64) -> bool {
        let key = Self::key_of(pgno, kind);
        let (c, i) = self.index(pgno);
        let Some(chunk) = self.chunks[c].get() else {
            return false;
        };
        let slot = &chunk[i];
        // Ordering: `Acquire` pairs with the publisher's closing `Release`
        // store of the even sequence, so a stable `s1` makes that publish's
        // `key`/`stamp` stores visible to the loads below.
        let s1 = slot.seq.load(Ordering::Acquire);
        if s1 & 1 == 1 {
            return false;
        }
        // Ordering: `Relaxed` data loads; consistency is decided by the
        // sequence re-check after the fence, not by these loads.
        let k = slot.key.load(Ordering::Relaxed);
        let t = slot.stamp.load(Ordering::Relaxed);
        // Ordering: this `Acquire` fence synchronizes with the fence
        // `Release` a publisher issues after taking the slot (odd store) and
        // before its data stores. If either load above read a later
        // publisher's store, that publisher's odd sequence store happens
        // before the re-load below, which then differs from `s1` — the
        // standard seqlock reader (ARM: `dmb ishld` here).
        fence(Ordering::Acquire);
        let s2 = slot.seq.load(Ordering::Relaxed);
        s1 == s2 && k == key && t == stamp
    }

    /// Record that the page version `(pgno, kind, stamp)` passed full
    /// validation. Call only after the validation succeeded. A slot busy
    /// with another publish is skipped (the entry is only an optimization).
    pub(crate) fn publish(&self, pgno: u64, kind: StampKind, stamp: u64) {
        let key = Self::key_of(pgno, kind);
        let (c, i) = self.index(pgno);
        let chunk = self.chunks[c].get_or_init(|| {
            (0..self.chunk_slots)
                .map(|_| Slot::empty())
                .collect::<Box<[Slot]>>()
        });
        let slot = &chunk[i];
        // Ordering: `Relaxed` — only a hint for the CAS below.
        let s = slot.seq.load(Ordering::Relaxed);
        if s & 1 == 1 {
            return;
        }
        // Ordering: `Acquire` on success makes the previous publisher's data
        // stores (released by its closing even store, which this CAS read)
        // happen before ours, so ours land after them in each word's
        // modification order and the slot never ends up mixing the two.
        // Failure: another publisher won; skip.
        if slot
            .seq
            .compare_exchange(s, s + 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        // Ordering: `Release` fence orders the odd sequence store above
        // before the data stores below, for the reader's `Acquire` fence
        // (see `contains`).
        fence(Ordering::Release);
        slot.key.store(key, Ordering::Relaxed);
        slot.stamp.store(stamp, Ordering::Relaxed);
        // Ordering: `Release` publishes the data stores (and the validation
        // that program-order preceded this call) to a reader's `Acquire`
        // load of this even sequence.
        slot.seq.store(s + 2, Ordering::Release);
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn exact_version_hits_other_versions_miss() {
        let c = StampCache::new();
        assert!(!c.contains(7, StampKind::Leaf, 3), "empty cache misses");
        c.publish(7, StampKind::Leaf, 3);
        assert!(c.contains(7, StampKind::Leaf, 3));
        assert!(!c.contains(7, StampKind::Leaf, 4), "newer stamp misses");
        assert!(!c.contains(7, StampKind::Branch, 3), "other kind misses");
        assert!(!c.contains(8, StampKind::Leaf, 3), "other pgno misses");
        c.publish(7, StampKind::Leaf, 4);
        assert!(c.contains(7, StampKind::Leaf, 4));
        assert!(!c.contains(7, StampKind::Leaf, 3), "overwritten version");
    }

    #[test]
    fn chunks_are_allocated_only_where_publishes_land() {
        let c = StampCache::new();
        assert_eq!(c.chunks.len(), SLOTS / CHUNK_SLOTS);
        c.publish(42, StampKind::Branch, 5);
        assert_eq!(c.chunks.iter().filter(|x| x.get().is_some()).count(), 1);
        assert!(c.contains(42, StampKind::Branch, 5));
        for pgno in 0..10_000 {
            c.publish(pgno, StampKind::Leaf, 1);
        }
        for pgno in 0..10_000 {
            // Slot = pgno: 10k consecutive pgnos never collide.
            assert!(c.contains(pgno, StampKind::Leaf, 1), "pgno {pgno}");
            assert!(!c.contains(pgno, StampKind::Leaf, 2));
        }
        assert_eq!(
            c.chunks.iter().filter(|x| x.get().is_some()).count(),
            10_000usize.div_ceil(CHUNK_SLOTS),
            "consecutive pgnos fill consecutive chunks only"
        );
    }

    #[test]
    fn pgnos_one_table_apart_share_a_slot() {
        let c = StampCache::new();
        let far = 7 + SLOTS as u64;
        c.publish(7, StampKind::Leaf, 3);
        c.publish(far, StampKind::Leaf, 3);
        assert!(
            !c.contains(7, StampKind::Leaf, 3),
            "evicted by the collision"
        );
        assert!(c.contains(far, StampKind::Leaf, 3));
        assert!(
            !c.contains(7, StampKind::Leaf, 3),
            "the key word, not just the slot, must match"
        );
    }

    #[test]
    fn colliding_pages_evict_each_other() {
        let c = StampCache::with_slots(1);
        c.publish(1, StampKind::Leaf, 10);
        c.publish(2, StampKind::Branch, 11);
        assert!(!c.contains(1, StampKind::Leaf, 10));
        assert!(c.contains(2, StampKind::Branch, 11));
    }

    #[test]
    fn concurrent_publish_and_lookup_never_mix_entries() {
        // One slot, two publishers of distinct versions, readers checking
        // that every hit is a pair some publisher actually wrote.
        let c = std::sync::Arc::new(StampCache::with_slots(1));
        let mut hs = Vec::new();
        for t in 0..2u64 {
            let c = c.clone();
            hs.push(std::thread::spawn(move || {
                for i in 0..20_000u64 {
                    c.publish(t, StampKind::Leaf, t * 1_000_000 + i);
                }
            }));
        }
        for _ in 0..2 {
            let c = c.clone();
            hs.push(std::thread::spawn(move || {
                for i in 0..20_000u64 {
                    // A mixed pair would be (0, 1_000_000 + i) or
                    // (1, i): never published.
                    assert!(!c.contains(0, StampKind::Leaf, 1_000_000 + i));
                    assert!(!c.contains(1, StampKind::Leaf, i));
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
    }
}

// loom L7 (ADR-0018): the seqlock never lets a lookup pair one publish's key
// with another's stamp. Run via `just loom`.
#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use loom::thread;
    use std::sync::Arc;

    #[test]
    fn loom_stamp_cache_never_mixes_publishes() {
        loom::model(|| {
            let c = Arc::new(StampCache::with_slots(1));
            // Allocate (the single chunk) before spawning so the model covers
            // the slot protocol, not `OnceLock`.
            c.publish(9, StampKind::Leaf, 9);
            let a = {
                let c = c.clone();
                thread::spawn(move || c.publish(1, StampKind::Leaf, 100))
            };
            let b = {
                let c = c.clone();
                thread::spawn(move || c.publish(2, StampKind::Leaf, 200))
            };
            // Mixed pairs were never published and must never hit. Two
            // probes keep the model exhaustive in reasonable time; each
            // covers one direction of a torn read between the publishers.
            assert!(!c.contains(1, StampKind::Leaf, 200));
            assert!(!c.contains(2, StampKind::Leaf, 100));
            a.join().unwrap();
            b.join().unwrap();
            // Quiescent: whichever publish owns the slot is intact.
            let hits = [
                c.contains(9, StampKind::Leaf, 9),
                c.contains(1, StampKind::Leaf, 100),
                c.contains(2, StampKind::Leaf, 200),
            ];
            assert_eq!(hits.iter().filter(|h| **h).count(), 1);
        });
    }
}
