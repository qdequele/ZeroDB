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
//! borrow). This module does no I/O; its heap paths are pure safe Rust, and
//! its one `unsafe` is the sanctioned call of the in-place WRITE_MAP map-slice
//! broker ([`map_mut`]; CLAUDE.md unsafe policy, ADR-0021). `miri` exercises
//! both realizations (TXN-49; `tests/writemap_in_place_miri.rs` over the
//! heap-backed test map).
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
//!
//! **In-map dirty pages (ADR-0021; SPEC 04 §6.4 TXN-45b; `WRITE_MAP`).** When
//! the backing brokers map slices ([`Backing::dirty_in_map`]), a store built
//! with [`DirtyStore::with_spare_in_map`] realizes every new frame **in the
//! writable map** at the frame's own pgno ([`Slot::Map`]) instead of on the
//! heap: the bytes are already at their final file offset, so commit C2 (and
//! spilling) writes nothing for them. The TXN-41 stability contract holds
//! unchanged — the map is mapped once for the env's life (ADR-0004 D4), so a
//! map frame's address never moves — and the borrow discipline is the same:
//! `bytes` ties a shared view to `&self`, `bytes_mut` a mutable view to
//! `&mut self`, so the brokered `&mut` slices are transient and never
//! overlap a live borrow (TXN-39). Writing a frame **at allocation time**
//! instead of commit C2 is safe for the same reason spilling is (TXN-70):
//! the pgno came from this txn's allocator, so TXN-62 guarantees no live
//! snapshot references it. Map frames are never pooled as spares; `remove`
//! copies a map frame out to a heap `Box` (the split path's scratch copy).

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

use crate::env::Backing;

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

/// One dirty frame: where its bytes live.
#[derive(Debug)]
enum Slot {
    /// A heap frame (`psize` bytes for a tree page, `n * psize` for a run) —
    /// the default-mode backing (SPEC 04 §6.3, TXN-45a).
    Heap(Box<[u8]>),
    /// An **in-map** frame of `len` bytes at byte offset `pgno * psize` of
    /// the writable map (ADR-0021, TXN-45b): the bytes live at their final
    /// file offset; only the length is tracked here. Views are brokered from
    /// the backing per access ([`Backing::map_dirty_page`] for `&mut`, the
    /// shared map bytes for `&`).
    Map { len: usize },
}

impl Slot {
    fn len(&self) -> usize {
        match self {
            Slot::Heap(b) => b.len(),
            Slot::Map { len } => *len,
        }
    }
}

/// A write txn's dirty-page frames, indexed by pgno. See the module docs for
/// the stability contract. `'env` is the backing's lifetime (only used in
/// in-map mode, [`DirtyStore::with_spare_in_map`]; heap-mode stores never
/// touch it).
pub struct DirtyStore<'env> {
    psize: u32,
    /// `log2(psize)` (page sizes are powers of two, SPEC 02 §3.2): turns a
    /// frame length into its page count with a shift, not a division, on the
    /// discard/remove path deletes take once per freed page.
    psize_shift: u32,
    frames: HashMap<u64, Slot, PgnoBuildHasher>,
    /// The in-map broker (ADR-0021): `Some` iff this store realizes new
    /// frames in the writable map. A shared reference only — the `&mut`
    /// slices it brokers are created transiently inside `&mut self` methods
    /// and never stored (module docs).
    broker: Option<&'env dyn Backing>,
    /// Recycled one-page frames (exactly `psize` bytes each), reused by
    /// `insert_tree_frame` / `insert_copy` before allocating (PERF-GAP B3).
    /// Frames only move to/from here inside `&mut` ops, preserving the TXN-41
    /// stability contract; bounded by [`SPARE_CAP`]. In-map stores still use
    /// it for the `remove` scratch copy.
    spare: Vec<Box<[u8]>>,
    /// Pages held by `frames` (a run frame counts its `n` pages): the
    /// quantity the dirty limit bounds (SPEC 04 TXN-68).
    pages: u64,
    /// Spilled pgnos (SPEC 04 TXN-69..72) → page count (1 for a tree page,
    /// `n` for an overflow-run head). Disjoint from `frames`.
    spilled: HashMap<u64, u64, PgnoBuildHasher>,
}

impl std::fmt::Debug for DirtyStore<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Manual: `&dyn Backing` has no `Debug`; show the mode instead.
        f.debug_struct("DirtyStore")
            .field("psize", &self.psize)
            .field("in_map", &self.broker.is_some())
            .field("frames", &self.frames.len())
            .field("pages", &self.pages)
            .field("spilled", &self.spilled.len())
            .field("spare", &self.spare.len())
            .finish()
    }
}

/// The brokered mutable map view for the `len`-byte frame at `pgno`
/// (ADR-0021). A free function (not a method) so callers can hold disjoint
/// borrows of the store's other fields; the returned borrow is always
/// immediately re-tied to `&mut DirtyStore` by its caller's signature.
/// Panics only on a store-logic bug: every `Slot::Map` was created through
/// the same broker with the same geometry.
///
/// This is the **sole sanctioned call site** of the `unsafe` map-slice broker
/// (CLAUDE.md unsafe policy, ratified 2026-10-02; ADR-0021 B1): the broker is
/// an `unsafe fn` because it mints `&mut [u8]` from `&self`, and this module
/// discharges its contract. `map_mut` is itself an `unsafe fn` for the same
/// reason — no safe function anywhere may mint `&mut` from a shared borrow.
///
/// # Safety
///
/// Callers are `&mut self` methods of [`DirtyStore`] and must (the brokered
/// contract, discharged jointly with the `SAFETY` block below):
/// re-tie the returned borrow to `&mut self` via their signature, never
/// store it, hold no other view of the region across the call, and only
/// name regions tracked (or being tracked) as this store's `Slot::Map`
/// frames — TXN-62 pgnos of the single live write txn.
// The crate's `unsafe_code` allows outside `page::raw` are exactly this
// module's brokered map-slice path — the WRITE_MAP in-place write the policy
// sanctions for `zerodb-core::dirty` (ADR-0021): this fn plus its five
// `&mut self` callers' one-line `unsafe { map_mut(..) }` calls, and the
// declaration-only allow on `Backing::map_dirty_page` in `env`.
#[allow(unsafe_code)]
// clippy's `mut_from_ref` fires on unsafe fns too (verified clippy 1.97);
// this one's `# Safety` contract is exactly the exclusivity the lint fears.
#[allow(clippy::mut_from_ref)]
unsafe fn map_mut(broker: Option<&dyn Backing>, psize: u32, pgno: u64, len: usize) -> &mut [u8] {
    let b = broker.expect("Slot::Map exists only in an in-map store");
    let pages = (len / psize as usize) as u64;
    // SAFETY (the brokered contract, `Backing::map_dirty_page` /
    // `MmapWritable::slice_mut`; CLAUDE.md invariants for this sanction):
    //  * Single writer (TXN-6): a `DirtyStore` exists only inside the one
    //    live `RwTxn`, which holds the env's writer lock for its whole life;
    //    no other thread can reach a broker of this env while it does.
    //  * The target pgno is referenced by no live snapshot (TXN-62): every
    //    `Slot::Map` is created for a pgno this txn **allocated** — beyond
    //    the committed high-water or GC-reclaimed under the oldest-reader
    //    gate — which the txn additionally re-checks with a typed
    //    release-mode guard before any in-map frame is tracked
    //    (`RwTxn::allocate`, ADR-0021 M2). Readers, nested readers and the
    //    committed trees therefore never dereference the region.
    //  * Exactly one live `&mut` per map region, tied to `&mut DirtyStore`,
    //    never stored: every caller of this function is a `&mut self` method
    //    of the store whose signature re-ties the returned borrow, the store
    //    never retains it (only `Slot::Map { len }` bookkeeping), and no
    //    caller invokes it twice without the previous borrow dying first.
    //  * The whole-map `&[u8]` read view is re-derived after each spill
    //    (`RwTxn::spill`, ADR-0021 B2) and — in in-map mode — re-borrowed
    //    lazily on every access (`RwTxn::whole_map`; the M1 miri finding:
    //    under Stacked Borrows even *copying* a stale view after an
    //    in-place write is UB at the written locations), so no stale shared
    //    borrow is ever created over, or read at, locations an in-place
    //    write touched; shared views of dirty frames (`DirtyStore::bytes`)
    //    likewise re-derive from `broker.bytes()` on every call and are
    //    tied to `&self` (TXN-39/41).
    unsafe {
        b.map_dirty_page(pgno, psize, pages)
            .expect("in-map frame region was brokered at insert and the map never shrinks")
    }
}

impl<'env> DirtyStore<'env> {
    /// An empty store for pages of size `psize`.
    #[must_use]
    pub fn new(psize: u32) -> DirtyStore<'env> {
        DirtyStore::with_spare(psize, Vec::new())
    }

    /// An empty store seeded with a pool of recycled one-page frames (the
    /// env's `me_dpages`-style pool handed in at write-txn begin, PERF-GAP
    /// B12). The `Vec` is adopted as is, O(1): every write txn, empty ones
    /// included, pays for the pool hand-over, so it must not scale with the
    /// pool size (a per-frame filter here made `env/txn/rw_empty_commit` 6×
    /// slower). The pool only ever holds frames of this env's page size.
    #[must_use]
    pub fn with_spare(psize: u32, mut spare: Vec<Box<[u8]>>) -> DirtyStore<'env> {
        debug_assert!(spare.iter().all(|f| f.len() == psize as usize));
        spare.truncate(SPARE_CAP);
        debug_assert!(psize.is_power_of_two());
        DirtyStore {
            psize,
            psize_shift: psize.trailing_zeros(),
            frames: HashMap::default(),
            broker: None,
            spare,
            pages: 0,
            spilled: HashMap::default(),
        }
    }

    /// [`DirtyStore::with_spare`], realizing new frames **in the writable
    /// map** through `broker` (ADR-0021, SPEC 04 §6.4 TXN-45b). The caller
    /// (write-txn begin) selects this only when
    /// [`Backing::dirty_in_map`] is `true`.
    #[must_use]
    pub fn with_spare_in_map(
        psize: u32,
        spare: Vec<Box<[u8]>>,
        broker: &'env dyn Backing,
    ) -> DirtyStore<'env> {
        debug_assert!(broker.dirty_in_map());
        let mut s = DirtyStore::with_spare(psize, spare);
        s.broker = Some(broker);
        s
    }

    /// Whether this store realizes dirty frames in the writable map
    /// (ADR-0021). Commit C2 and spilling write nothing for such frames.
    #[must_use]
    pub fn in_map_mode(&self) -> bool {
        self.broker.is_some()
    }

    /// Whether `pgno`'s frame lives in the map (always `false` in heap mode).
    /// Commit C2/spill skip these frames: their bytes are already at their
    /// final file offset.
    #[must_use]
    pub fn in_map(&self, pgno: u64) -> bool {
        self.broker.is_some() && matches!(self.frames.get(&pgno), Some(Slot::Map { .. }))
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
    /// current bytes at `pgno` first — for an in-map frame (ADR-0021) there
    /// is nothing to write (the bytes already sit at the frame's file
    /// offset), so spilling is pure bookkeeping. A `&mut` op, like
    /// [`DirtyStore::discard`].
    pub fn spill(&mut self, pgno: u64) {
        if let Some(frame) = self.frames.remove(&pgno) {
            let n = self.pages_of(frame.len());
            self.pages -= n;
            self.spilled.insert(pgno, n);
            if let Slot::Heap(frame) = frame {
                if frame.len() == self.psize as usize && self.spare.len() < SPARE_CAP {
                    self.spare.push(frame);
                }
            }
        }
    }

    /// Bring spilled tree page `pgno` back into a frame holding `src` (its
    /// bytes as read from the map) and clear its spilled mark (SPEC 04
    /// TXN-72; LMDB `mdb_page_unspill`). Returns the frame mutably. `src` must
    /// be exactly one page. Heap mode only — an in-map store uses
    /// [`DirtyStore::unspill_in_place`] (the bytes are already in the map;
    /// copying `src` over itself would self-overlap).
    pub fn unspill_copy(&mut self, pgno: u64, src: &[u8]) -> &mut [u8] {
        debug_assert!(self.broker.is_none(), "in-map stores unspill in place");
        debug_assert_eq!(
            self.spilled.get(&pgno),
            Some(&1),
            "unspill of a spilled tree page"
        );
        self.spilled.remove(&pgno);
        self.insert_copy(pgno, src)
    }

    /// In-map variant of [`DirtyStore::unspill_copy`] (ADR-0021; SPEC 04
    /// TXN-72): the spilled tree page's bytes already live in the map at
    /// `pgno` (they were written there in place), so unspilling is pure
    /// re-tracking — no copy. Returns the frame mutably.
    #[allow(unsafe_code)] // sanctioned brokered-slice call (ADR-0021; see `map_mut`)
    pub fn unspill_in_place(&mut self, pgno: u64) -> &mut [u8] {
        debug_assert!(self.broker.is_some(), "in-place unspill needs the broker");
        debug_assert_eq!(
            self.spilled.get(&pgno),
            Some(&1),
            "unspill of a spilled tree page"
        );
        self.spilled.remove(&pgno);
        let len = self.psize as usize;
        self.pages += 1;
        if let Some(old) = self.frames.insert(pgno, Slot::Map { len }) {
            self.pages -= self.pages_of(old.len());
        }
        // SAFETY: `&mut self` op re-tying the borrow via this signature; the
        // region is this store's re-tracked `Slot::Map` frame (a TXN-62 pgno
        // this txn wrote in place before spilling); no other view is live.
        unsafe { map_mut(self.broker, self.psize, pgno, len) }
    }

    /// The frame bytes for `pgno`: exactly `psize` for a tree page, the whole
    /// contiguous `n * psize` run for an overflow head.
    #[must_use]
    pub fn bytes(&self, pgno: u64) -> Option<&[u8]> {
        match self.frames.get(&pgno)? {
            Slot::Heap(b) => Some(&**b),
            Slot::Map { len } => {
                // Shared view of the in-map frame: a plain subslice of the
                // backing's shared map bytes, tied to `&self` like any heap
                // frame view (TXN-39/41).
                let off = (pgno as usize) * self.psize as usize;
                let bytes = self
                    .broker
                    .expect("Slot::Map exists only in an in-map store")
                    .bytes();
                Some(&bytes[off..off + *len])
            }
        }
    }

    /// Mutable frame bytes for `pgno` (a `&mut self` op; TXN-42 in-place edit).
    #[must_use]
    #[allow(unsafe_code)] // sanctioned brokered-slice call (ADR-0021; see `map_mut`)
    pub fn bytes_mut(&mut self, pgno: u64) -> Option<&mut [u8]> {
        match self.frames.get_mut(&pgno)? {
            Slot::Heap(b) => Some(&mut **b),
            // SAFETY: `&mut self` op; the brokered view is re-tied to
            // `&mut self` by this signature, so it dies at the next store op
            // like a heap-frame borrow; the region is a tracked `Slot::Map`
            // frame (TXN-62 pgno) and no other view of it is live.
            Slot::Map { len } => Some(unsafe { map_mut(self.broker, self.psize, pgno, *len) }),
        }
    }

    /// Record the new frame slot for `pgno` and return the displaced slot, if
    /// any, keeping the page count right.
    fn track(&mut self, pgno: u64, slot: Slot) {
        debug_assert!(
            !self.spilled.contains_key(&pgno),
            "frame for a spilled pgno"
        );
        self.pages += self.pages_of(slot.len());
        if let Some(old) = self.frames.insert(pgno, slot) {
            self.pages -= self.pages_of(old.len());
        }
    }

    /// Insert (or replace) the frame for `pgno`. The frame length must be a
    /// non-zero multiple of `psize` (1 page for tree frames, `n` for runs).
    /// Always a heap frame (test/compat path; the engine's run creation goes
    /// through [`DirtyStore::insert_run_frame`]).
    pub fn insert(&mut self, pgno: u64, frame: Box<[u8]>) {
        debug_assert!(
            !frame.is_empty() && frame.len() % self.psize as usize == 0,
            "frame length {} must be a positive multiple of psize {}",
            frame.len(),
            self.psize
        );
        self.track(pgno, Slot::Heap(frame));
    }

    /// Insert a fresh zeroed one-page tree frame for `pgno` and return it
    /// mutably (the caller formats it as a leaf/branch page). Heap mode reuses
    /// a pooled spare when one is available, zero-filling it; in-map mode
    /// (ADR-0021) zero-fills the map page at `pgno` — callers rely on the
    /// frame being all-zero (e.g. `ZeroReserve` regions, TXN-47).
    #[allow(unsafe_code)] // sanctioned brokered-slice call (ADR-0021; see `map_mut`)
    pub fn insert_tree_frame(&mut self, pgno: u64) -> &mut [u8] {
        let ps = self.psize as usize;
        if self.broker.is_some() {
            self.track(pgno, Slot::Map { len: ps });
            // SAFETY: `&mut self` op re-tying the borrow via this signature;
            // the region was just tracked as this store's `Slot::Map` frame
            // at a pgno the txn allocated (TXN-62, re-checked by the
            // caller's release guard); no other view of it is live.
            let frame = unsafe { map_mut(self.broker, self.psize, pgno, ps) };
            frame.fill(0);
            return frame;
        }
        let frame = match self.spare.pop() {
            Some(mut f) => {
                debug_assert_eq!(f.len(), ps, "spare frame is one page");
                f.fill(0);
                f
            }
            None => vec![0u8; ps].into_boxed_slice(),
        };
        self.track(pgno, Slot::Heap(frame));
        self.bytes_mut(pgno).expect("frame was just inserted")
    }

    /// Insert a one-page COW copy of `src` for `pgno` and return it mutably (the
    /// caller restamps the header). Heap mode reuses a pooled spare,
    /// overwriting it with `src`, before allocating (PERF-GAP B3); in-map mode
    /// (ADR-0021) copies `src` straight into the map page at `pgno` — the one
    /// copy COW inherently costs, with no heap frame and no commit write-back.
    /// `src` must be exactly one page and MUST NOT overlap the map page at
    /// `pgno` (it never does: `src` is a committed or spilled page, `pgno` a
    /// fresh allocation of this txn).
    #[allow(unsafe_code)] // sanctioned brokered-slice call (ADR-0021; see `map_mut`)
    pub fn insert_copy(&mut self, pgno: u64, src: &[u8]) -> &mut [u8] {
        let ps = self.psize as usize;
        debug_assert_eq!(src.len(), ps, "insert_copy is one-page only");
        if self.broker.is_some() {
            self.track(pgno, Slot::Map { len: ps });
            // SAFETY: as `insert_tree_frame`; additionally `src` never
            // overlaps the region (a committed/spilled page vs a fresh
            // TXN-62 allocation — caller contract above).
            let frame = unsafe { map_mut(self.broker, self.psize, pgno, ps) };
            frame.copy_from_slice(src);
            return frame;
        }
        let frame = match self.spare.pop() {
            Some(mut f) => {
                debug_assert_eq!(f.len(), ps, "spare frame is one page");
                f.copy_from_slice(src);
                f
            }
            None => src.into(),
        };
        self.track(pgno, Slot::Heap(frame));
        self.bytes_mut(pgno).expect("frame was just inserted")
    }

    /// Insert a fresh zeroed `pages`-page overflow-run frame headed at `pgno`
    /// and return it mutably (the caller writes the run header and payload).
    /// Heap mode allocates the contiguous `pages * psize` box (TXN-41's run
    /// rule); in-map mode (ADR-0021) the run is realized in the map at its
    /// final offset — contiguity is the map's own layout.
    #[allow(unsafe_code)] // sanctioned brokered-slice call (ADR-0021; see `map_mut`)
    pub fn insert_run_frame(&mut self, pgno: u64, pages: u64) -> &mut [u8] {
        let len = (pages as usize) * self.psize as usize;
        debug_assert!(pages > 0, "a run has at least one page");
        if self.broker.is_some() {
            self.track(pgno, Slot::Map { len });
            // SAFETY: as `insert_tree_frame`, for the whole `pages`-page
            // run region (every page of it is a TXN-62 allocation of this
            // txn — the caller's release guard checks each one).
            let frame = unsafe { map_mut(self.broker, self.psize, pgno, len) };
            frame.fill(0);
            return frame;
        }
        self.track(pgno, Slot::Heap(vec![0u8; len].into_boxed_slice()));
        self.bytes_mut(pgno).expect("frame was just inserted")
    }

    /// Remove and return the frame for `pgno` (freed within the txn, GC-7/8;
    /// or taken out as the split path's address-stable scratch). Only called
    /// from `&mut` ops (TXN-43). An in-map frame is **copied out** to a heap
    /// box (ADR-0021): the split path reads the old frame while re-packing
    /// the same pgno, which for a map frame would self-overlap — the copy is
    /// the same scratch copy LMDB's split makes under `MDB_WRITEMAP`.
    pub fn remove(&mut self, pgno: u64) -> Option<Box<[u8]>> {
        // A spilled pgno has no frame; freeing one goes through `discard`,
        // which clears the mark (SPEC 04 TXN-72).
        debug_assert!(
            !self.spilled.contains_key(&pgno),
            "remove of a spilled pgno"
        );
        let f = self.frames.remove(&pgno)?;
        self.pages -= self.pages_of(f.len());
        match f {
            Slot::Heap(b) => Some(b),
            Slot::Map { len } => {
                let off = (pgno as usize) * self.psize as usize;
                let bytes = self
                    .broker
                    .expect("Slot::Map exists only in an in-map store")
                    .bytes();
                let src = &bytes[off..off + len];
                let mut b = match self.spare.pop() {
                    Some(f) if f.len() == len => f,
                    Some(f) => {
                        // Wrong size for this run — put it back and allocate.
                        self.spare.push(f);
                        vec![0u8; len].into_boxed_slice()
                    }
                    None => vec![0u8; len].into_boxed_slice(),
                };
                b.copy_from_slice(src);
                Some(b)
            }
        }
    }

    /// Discard the frame for `pgno` (freed within the txn). A one-page heap
    /// frame is parked on the `spare` pool for reuse if there is room; a run
    /// frame (multi-page), an overflow past [`SPARE_CAP`], or an in-map frame
    /// (whose bytes are not ours to pool) is dropped. Like
    /// [`DirtyStore::remove`], only called from `&mut` ops (TXN-43): the frame
    /// leaves `frames` at a `&mut` boundary, so no borrow can alias it.
    ///
    /// Freeing a **spilled** pgno clears its spilled mark (SPEC 04 TXN-72):
    /// the page returns to the txn as loose, like any page it allocated.
    pub fn discard(&mut self, pgno: u64) {
        if let Some(frame) = self.frames.remove(&pgno) {
            self.pages -= self.pages_of(frame.len());
            if let Slot::Heap(frame) = frame {
                if frame.len() == self.psize as usize && self.spare.len() < SPARE_CAP {
                    self.spare.push(frame);
                }
            }
        } else if !self.spilled.is_empty() {
            self.spilled.remove(&pgno);
        }
    }

    /// Take up to `cap` one-page frames out of this store — spares first, then
    /// remaining one-page dirty heap frames — to hand back to the env pool at
    /// end-of-txn (PERF-GAP B12). Called only after the commit pipeline has
    /// finished writing (or on abort, where the frames are discarded garbage):
    /// recycled contents never matter because every reuse zero-fills or fully
    /// overwrites. Run frames and in-map frames (ADR-0021 — the map's bytes
    /// are not pool material) are left behind to drop with the store.
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
                if let Slot::Heap(f) = f {
                    if f.len() == ps {
                        out.push(f);
                    }
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
