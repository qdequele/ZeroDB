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
//! Shape (ADR-0018 amendment, 2026-10-31): one slot per pgno, slot = pgno
//! modulo [`SLOTS`] (2^23), so every page of a file below 2^23 pages owns its
//! slot and pages never evict each other; in a larger file, pages [`SLOTS`]
//! apart share a slot and the newer publish wins. Slots are allocated where
//! publishes land, in three levels — a top array made at open, directory
//! blocks of [`MID_CHUNKS`] chunks, and chunks of [`CHUNK_SLOTS`] slots — so
//! the table grows with the pages the file uses (8 bytes per page) and a
//! small env pays for one chunk.
//!
//! Each slot is one atomic word holding the stamp, the pgno's bits above the
//! slot index, and the page kind (see [`StampCache::word`]). Within one slot
//! the word determines `(pgno, kind, stamp)` exactly, so a word match is an
//! exact key-and-stamp match. A hit hands out a zero-check page view, so a
//! lookup must never combine one publish's key with another's stamp; with the
//! whole entry in one word, every value a lookup can load is one some publish
//! stored whole, so it cannot. The loom model below checks exactly that.
//!
//! Stated limit: the cache assumes the file changes only through ZeroDB
//! commits, by this process or by another process sharing the lock file
//! (ADR-0023 §5). Such a commit stamps every page it writes with its own
//! txnid, so a page another process rewrote misses. Bytes rewritten
//! underneath the env by anything else, with a stamp they already carried,
//! would be trusted from the cache.

use std::sync::OnceLock;

use crate::sync::{AtomicU64, Ordering};

/// Slots before pgnos wrap: 8 Mi × 8 bytes = 64 MiB per env at most, and no
/// collisions for files up to 32 GiB at 4 KiB pages (ADR-0018 amendment).
pub(crate) const SLOTS: usize = 1 << 23;

/// Slots per lazily allocated chunk (32 KiB), so a first publish never
/// zero-fills more than that (measured on `env/open/reopen`).
pub(crate) const CHUNK_SLOTS: usize = 1 << 12;

/// Chunks per lazily allocated directory block (1.5 KiB, covering 256 Ki
/// pages), so the array made at open stays small.
pub(crate) const MID_CHUNKS: usize = 1 << 6;

/// Bits of a pgno: every pgno is below `MAX_MAP_SIZE / MIN_PAGE_SIZE` =
/// 2^47 / 2^12. A larger one is never cached (a miss, which revalidates).
const PGNO_BITS: u32 = 35;

const _: () = assert!(
    1u64 << PGNO_BITS == crate::page::geometry::MAX_MAP_SIZE / crate::page::MIN_PAGE_SIZE as u64
);

/// Which validated shape an entry vouches for; part of the key, as in the
/// txn memo, so a page validated as a leaf never hits as a
/// branch.
#[derive(Clone, Copy)]
pub(crate) enum StampKind {
    Leaf,
    Branch,
}

/// A run of slots. A slot of `0` is empty: every published word has bit 0
/// set.
type Chunk = Box<[AtomicU64]>;

/// A directory block of lazily allocated chunks.
type Mid = Box<[OnceLock<Chunk>]>;

/// The env-wide table (ADR-0018). Owned by the env and borrowed by every
/// read txn.
pub struct StampCache {
    top: Box<[OnceLock<Mid>]>,
    /// log2 of the slot count.
    bits: u32,
    /// Slots per chunk; a power of two.
    chunk_slots: usize,
    /// Chunks per directory block; a power of two.
    mid_chunks: usize,
}

impl std::fmt::Debug for StampCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StampCache")
            .field("len", &(1usize << self.bits))
            .field("chunks_allocated", &self.chunks_allocated())
            .field("allocated_bytes", &self.allocated_bytes())
            .finish()
    }
}

impl StampCache {
    /// An empty cache of [`SLOTS`] slots, allocated as publishes land.
    pub(crate) fn new() -> StampCache {
        StampCache::with_slots(SLOTS)
    }

    /// An empty cache of `len` slots (a power of two, at most 2^35). Small
    /// tables are for the loom model and collision tests.
    pub(crate) fn with_slots(len: usize) -> StampCache {
        assert!(len.is_power_of_two() && len as u64 <= 1 << PGNO_BITS);
        let chunk_slots = len.min(CHUNK_SLOTS);
        let mid_chunks = (len / chunk_slots).min(MID_CHUNKS);
        StampCache {
            top: (0..len / chunk_slots / mid_chunks)
                .map(|_| OnceLock::new())
                .collect(),
            bits: len.trailing_zeros(),
            chunk_slots,
            mid_chunks,
        }
    }

    /// Chunks allocated so far.
    fn chunks_allocated(&self) -> usize {
        self.top
            .iter()
            .filter_map(OnceLock::get)
            .map(|mid| mid.iter().filter(|c| c.get().is_some()).count())
            .sum()
    }

    /// Heap bytes the table holds now: the top array, the directory blocks
    /// and the chunks allocated so far.
    pub(crate) fn allocated_bytes(&self) -> usize {
        let mids = self.top.iter().filter(|m| m.get().is_some()).count();
        self.top.len() * std::mem::size_of::<OnceLock<Mid>>()
            + mids * self.mid_chunks * std::mem::size_of::<OnceLock<Chunk>>()
            + self.chunks_allocated() * self.chunk_slots * std::mem::size_of::<AtomicU64>()
    }

    /// The slot word for `(pgno, kind, stamp)`, or `None` when the triple is
    /// outside the encodable domain (such a triple is never published, so a
    /// lookup of it misses). Layout, for a table of `2^bits` slots:
    ///
    /// ```text
    /// 63 ... 2 + 35 - bits | ... 2        |   1  | 0
    ///  stamp               | pgno >> bits | kind | 1
    /// ```
    ///
    /// The slot index is `pgno mod 2^bits`, so within one slot the word
    /// gives back the pgno, the kind and the stamp exactly. At 2^23 slots
    /// the stamp has 50 bits: 35 years of commits at a million a second.
    #[inline]
    fn word(&self, pgno: u64, kind: StampKind, stamp: u64) -> Option<u64> {
        let shift = 2 + PGNO_BITS - self.bits;
        if pgno >> PGNO_BITS != 0 || stamp >> (64 - shift) != 0 {
            return None;
        }
        let kind = match kind {
            StampKind::Leaf => 0,
            StampKind::Branch => 1 << 1,
        };
        Some((stamp << shift) | ((pgno >> self.bits) << 2) | kind | 1)
    }

    /// `(directory block, chunk within it, slot within the chunk)` for
    /// `pgno`: the pgno itself, masked, not a hash. Pages the file holds
    /// side by side (a bulk-loaded tree's leaves, a sequential writer's)
    /// then sit in adjacent slots, so a scan probes the table almost
    /// sequentially. Hashed slots make a full scan touch one random line and
    /// page of the table per leaf: `scan/full/*` measured +5 % from cache
    /// and TLB misses. A pgno is one kind at a time, so leaf and branch
    /// share its slot.
    #[inline]
    fn index(&self, pgno: u64) -> (usize, usize, usize) {
        let i = (pgno as usize) & ((1usize << self.bits) - 1);
        let chunk = i / self.chunk_slots;
        (
            chunk / self.mid_chunks,
            chunk & (self.mid_chunks - 1),
            i & (self.chunk_slots - 1),
        )
    }

    /// `true` iff a publish of exactly `(pgno, kind, stamp)` is the slot's
    /// current content. A collision or an unallocated slot answers `false`.
    pub(crate) fn contains(&self, pgno: u64, kind: StampKind, stamp: u64) -> bool {
        let Some(want) = self.word(pgno, kind, stamp) else {
            return false;
        };
        let (t, m, i) = self.index(pgno);
        let Some(chunk) = self.top[t].get().and_then(|mid| mid[m].get()) else {
            return false;
        };
        // Ordering: `Acquire` pairs with the publisher's `Release` store, so
        // the validation (or commit) that preceded the publish happens before
        // this hit. The page bytes are also ordered by the reader's own
        // snapshot pin (TXN-18/19). The word is loaded whole, so coherence
        // needs no ordering at all (ARM: one `ldar`).
        chunk[i].load(Ordering::Acquire) == want
    }

    /// Record that the page version `(pgno, kind, stamp)` passed full
    /// validation. Call only after the validation succeeded. Racing
    /// publishers to one slot leave the last store; every store is a true
    /// fact, so either outcome is sound.
    pub(crate) fn publish(&self, pgno: u64, kind: StampKind, stamp: u64) {
        let Some(word) = self.word(pgno, kind, stamp) else {
            return;
        };
        let (t, m, i) = self.index(pgno);
        let mid = self.top[t].get_or_init(|| {
            (0..self.mid_chunks)
                .map(|_| OnceLock::new())
                .collect::<Mid>()
        });
        let chunk = mid[m].get_or_init(|| {
            (0..self.chunk_slots)
                .map(|_| AtomicU64::new(0))
                .collect::<Chunk>()
        });
        // Ordering: `Release` publishes the validation that program-order
        // preceded this call to a lookup's `Acquire` load of the word.
        chunk[i].store(word, Ordering::Release);
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    fn mids_allocated(c: &StampCache) -> usize {
        c.top.iter().filter(|m| m.get().is_some()).count()
    }

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
        assert_eq!(c.top.len(), SLOTS / CHUNK_SLOTS / MID_CHUNKS);
        assert_eq!(c.chunks_allocated(), 0);
        c.publish(42, StampKind::Branch, 5);
        assert_eq!((mids_allocated(&c), c.chunks_allocated()), (1, 1));
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
            c.chunks_allocated(),
            10_000usize.div_ceil(CHUNK_SLOTS),
            "consecutive pgnos fill consecutive chunks only"
        );
        // The next directory block starts at chunk `MID_CHUNKS`.
        let far = (MID_CHUNKS * CHUNK_SLOTS) as u64;
        assert!(!c.contains(far, StampKind::Leaf, 1), "unallocated misses");
        c.publish(far, StampKind::Leaf, 1);
        assert_eq!((mids_allocated(&c), c.chunks_allocated()), (2, 4));
        assert_eq!(
            c.allocated_bytes(),
            c.top.len() * std::mem::size_of::<OnceLock<Mid>>()
                + 2 * MID_CHUNKS * std::mem::size_of::<OnceLock<Chunk>>()
                + 4 * CHUNK_SLOTS * 8
        );
    }

    #[test]
    fn pages_below_the_cap_never_evict_each_other() {
        let c = StampCache::new();
        // Pgnos in every directory block, including both ends of the table.
        let pgnos: Vec<u64> = (0..SLOTS as u64)
            .step_by(MID_CHUNKS * CHUNK_SLOTS / 2 + 4099)
            .chain([1, 4095, 4096, SLOTS as u64 - 1])
            .collect();
        for &p in &pgnos {
            c.publish(p, StampKind::Leaf, p + 1);
        }
        for &p in &pgnos {
            assert!(c.contains(p, StampKind::Leaf, p + 1), "pgno {p}");
        }
        assert_eq!(mids_allocated(&c), SLOTS / CHUNK_SLOTS / MID_CHUNKS);
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
            "the pgno bits in the word, not just the slot, must match"
        );
    }

    #[test]
    fn the_word_is_exact_at_the_domain_edges() {
        let c = StampCache::new();
        let max_pgno = (1u64 << PGNO_BITS) - 1;
        let max_stamp = (1u64 << 50) - 1;
        for kind in [StampKind::Leaf, StampKind::Branch] {
            c.publish(max_pgno, kind, max_stamp);
            assert!(c.contains(max_pgno, kind, max_stamp));
            assert!(!c.contains(max_pgno, kind, max_stamp - 1));
            // Same slot, the pgno's high bits differ.
            assert!(!c.contains(max_pgno - SLOTS as u64, kind, max_stamp));
            assert!(!c.contains(max_pgno ^ (1 << (PGNO_BITS - 1)), kind, max_stamp));
        }
        assert!(!c.contains(max_pgno, StampKind::Leaf, max_stamp));
        assert!(c.contains(max_pgno, StampKind::Branch, max_stamp));
        let before = c.allocated_bytes();
        // Outside the domain: never published (nothing is allocated), never
        // a hit, and no truncated alias of an in-domain entry.
        c.publish(0, StampKind::Leaf, 1u64 << 50);
        c.publish(1u64 << PGNO_BITS, StampKind::Leaf, 1);
        assert_eq!(c.allocated_bytes(), before, "nothing was published");
        assert!(!c.contains(0, StampKind::Leaf, 1u64 << 50));
        assert!(!c.contains(1u64 << PGNO_BITS, StampKind::Leaf, 1));
        c.publish(0, StampKind::Leaf, 0);
        assert!(c.contains(0, StampKind::Leaf, 0), "stamp 0 is a valid word");
        assert!(!c.contains(0, StampKind::Leaf, 1u64 << 50));
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

// loom L7 (ADR-0018): a lookup never pairs one publish's key with another's
// stamp. Run via `just loom`.
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
            // Mixed pairs were never published and must never hit; each
            // probe covers one direction of a mix between the publishers.
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
