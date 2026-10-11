//! The MVCC reader table and the published-snapshot cell (SPEC 04 §3/§4,
//! TXN-14..22; ADR-0006).
//!
//! This module is the only place a reader and the writer communicate. It
//! contains **no** `unsafe` (ADR-0006 Option B): the load-bearing lock-free
//! protocol is carried entirely by the slot atomics and the `commit_point`
//! atomic; the snapshot cell's mutex only manages the `Arc<Snapshot>`'s
//! lifetime, with critical sections that are all bounded O(1) pointer
//! operations (TXN-18).
//!
//! Concurrency primitives come from [`crate::sync`], so the identical source
//! runs natively (and under miri) and is model-checked under
//! `RUSTFLAGS="--cfg loom"` (`just loom`; the `loom_*` tests at the bottom of
//! this file are the reader-table loom suite, L1–L5 per ADR-0006).
//!
//! Crash-safety note: nothing in this module writes
//! to disk. Every step here is in-process state; a crash at any point between
//! any two operations leaves the durable file exactly as the commit pipeline
//! (SPEC 04 §9) left it, and recovery never consults reader state (ZeroDB is
//! single-process, see docs/DIVERGENCES.md — a crashed process has no
//! surviving readers to respect).

use std::sync::Arc;

use crate::env::Snapshot;
#[cfg(not(loom))]
use crate::shared::ReaderSlot as Slot;
use crate::shared::{owner_generation, owner_pid, owner_word};

/// The local slot type under loom: [`crate::shared::ReaderSlot`]'s fields and
/// accessors over loom's atomics, which loom must see to model the protocol.
/// The shipped build uses `ReaderSlot` itself, whose `std` atomics have the
/// fixed lock-file layout (ADR-0023 §3).
#[cfg(loom)]
struct Slot {
    txnid: AtomicU64,
    owner: crate::sync::AtomicU64,
}

#[cfg(loom)]
impl Slot {
    fn new_free() -> Slot {
        Slot {
            txnid: AtomicU64::new(RDR_FREE),
            owner: crate::sync::AtomicU64::new(0),
        }
    }
    fn txnid(&self) -> &AtomicU64 {
        &self.txnid
    }
    fn owner(&self) -> &crate::sync::AtomicU64 {
        &self.owner
    }
}
use crate::sync::{AtomicU64, Mutex, Ordering};

/// Slot sentinel: unoccupied (SPEC 04 TXN-14).
pub(crate) const RDR_FREE: u64 = u64::MAX;
/// The largest committed txnid an env may carry (open-time bound, SPEC 06
/// REC-1a): the slot word encodes occupancy via the sentinel band at the top
/// of `u64`, so a *pinned* txnid must stay strictly below [`RDR_CLAIMED`] —
/// and the next writer txnid is `meta.txnid + 1`, which must not wrap or
/// enter the band either. A committed txnid above this is unreachable by any
/// real workload (ids grow by one per commit) and therefore marks a corrupt
/// or hostile meta; `open_with_backing` rejects it with `MdbError::Invalid`
/// instead of letting `store_pin`'s TXN-14 assumption break.
///
/// The bound leaves a margin of 2^32 ids below the band, and `RwTxn::new`
/// re-enforces it at runtime (a writer refuses to start once
/// `base.txnid >= MAX_COMMITTED_TXNID`): an accepted file can therefore
/// never *commit its way* into the band, however many transactions follow.
/// (A margin of only a few ids would let a boundary-value hostile meta reach
/// `RDR_CLAIMED` two commits after a successful open.)
pub(crate) const MAX_COMMITTED_TXNID: u64 = RDR_CLAIMED - (1 << 32);

/// Slot sentinel: reserved by a reader that has not yet published a real
/// snapshot txnid (transient, SPEC 04 §4.3).
pub(crate) const RDR_CLAIMED: u64 = u64::MAX - 1;

/// Where the reader slots live: on this process's heap (the default), or in a
/// lock file shared with other processes (ADR-0023). Every operation resolves
/// this once, to a slot slice, and then walks the slice directly.
enum Slots {
    Local(Box<[Slot]>),
    // loom models the in-process protocol only; the shared region holds
    // `std` atomics that loom cannot instrument.
    #[cfg(not(loom))]
    Shared(std::sync::Arc<dyn crate::shared::SharedRegion>),
}

/// The in-process reader table (SPEC 04 §4.1): a fixed array of `max_readers`
/// slots, allocated once at env open and never resized (TXN-14).
///
/// Each slot is one `AtomicU64` carrying both occupancy and the pinned txnid
/// via the sentinel band at the top of `u64` (a real txnid is always
/// `< RDR_CLAIMED`: ids start at 1 and grow by one per commit). One word per
/// slot means every observation of a slot is atomic — a state+txnid two-field
/// design would admit torn observations and need its own inter-field ordering
/// protocol (ADR-0006 D1). Each [`crate::shared::ReaderSlot`] is 64-byte
/// aligned and 128 bytes long, which keeps a pinning reader's store and the
/// writer's scan off each other's cache lines (TXN-14). A table opened with
/// [`ReaderTable::new_shared`] has the same slot layout, but the slots live in
/// a region shared with other processes (ADR-0023 §3) instead of on this heap.
pub(crate) struct ReaderTable {
    slots: Slots,
    /// This process's PID when the slots are shared (ADR-0023 §3): a claim
    /// records it in the slot's owner field and a release clears it. `None`
    /// for a local table, whose owner field is never written.
    owner_pid: Option<u32>,
    /// LMDB `MDB_txninfo::mti_numreaders` parity: the
    /// **high-water** slot count, i.e. `max(claimed slot index) + 1` over the
    /// env's lifetime. See [`ReaderTable::num_readers`] for why this is a
    /// high-water mark and not the live count.
    high_water: AtomicU64,
}

impl ReaderTable {
    /// Allocate the table with every slot `RDR_FREE`.
    pub(crate) fn new(max_readers: u32) -> ReaderTable {
        let slots = (0..max_readers)
            .map(|_| Slot::new_free())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        ReaderTable {
            slots: Slots::Local(slots),
            owner_pid: None,
            high_water: AtomicU64::new(0),
        }
    }

    /// A table whose slots live in a cross-process region (ADR-0023 §3). The
    /// claim, pin and release protocol is the same; claims also record the
    /// owner PID so a later stage can reap a dead process's slots.
    #[cfg(not(loom))]
    pub(crate) fn new_shared(
        region: std::sync::Arc<dyn crate::shared::SharedRegion>,
    ) -> ReaderTable {
        let pid = region.pid();
        ReaderTable {
            slots: Slots::Shared(region),
            owner_pid: Some(pid),
            high_water: AtomicU64::new(0),
        }
    }

    /// Drop this table's reference to the cross-process region (ADR-0023),
    /// leaving an empty local table. Called only from the environment's
    /// teardown, which needs the region released before it fires the close
    /// event (SPEC 04 TXN-53); `&mut self` proves no reader is using the
    /// slots. A no-op for a local table.
    pub(crate) fn release_region(&mut self) {
        #[cfg(not(loom))]
        if matches!(self.slots, Slots::Shared(_)) {
            self.slots = Slots::Local(Box::new([]));
        }
    }

    /// The slot array, wherever it is stored. Resolved once per operation;
    /// callers then walk or index the slice directly.
    #[inline]
    fn slots(&self) -> &[Slot] {
        match &self.slots {
            Slots::Local(s) => s,
            #[cfg(not(loom))]
            Slots::Shared(r) => r.slots(),
        }
    }

    /// Claim a slot (SPEC 04 TXN-15): scan from 0; the first successful
    /// `compare_exchange(RDR_FREE → RDR_CLAIMED)` grants exclusive ownership.
    /// `None` after a full scan means the table is exhausted — the caller maps
    /// it to `MdbError::ReadersFull` (TXN-16; no reaping path exists in a
    /// single-process engine, so the error is immediate).
    ///
    /// There is no ABA hazard in this CAS: the compare value `RDR_FREE` means
    /// "free *now*", and ownership is conferred by the successful exchange
    /// itself — no thread acts on a stale expectation derived from an earlier
    /// read (ADR-0006 D1). No scan hint and no high-water-mark shortcut
    /// (ADR-0006 D2): a stale bound could hide an already-pinned reader from
    /// the writer's scan, which is exactly the missed-pin bug.
    ///
    /// **ADR-0006 D2 still holds** despite the `high_water` counter maintained
    /// below: that counter is written here but read *only* by
    /// [`ReaderTable::num_readers`] (introspection). Neither this scan
    /// nor the writer's [`ReaderTable::oldest`] scan consults it — both still
    /// walk every slot, unconditionally.
    pub(crate) fn claim(&self) -> Option<u32> {
        self.claim_as(self.owner_pid)
    }

    /// [`ReaderTable::claim`] on behalf of `owner` (the PID recorded in the
    /// slot; `None` records none). The table's own claims pass its
    /// `owner_pid`; loom models and unit tests pass other PIDs to play
    /// several processes over one table.
    pub(crate) fn claim_as(&self, owner: Option<u32>) -> Option<u32> {
        for (i, rs) in self.slots().iter().enumerate() {
            let slot = rs.txnid();
            // Relaxed pre-filter: a pure optimization to skip occupied slots
            // without a CAS. Carries no ordering obligation — the CAS below
            // is the sole authority on the slot's state.
            if slot.load(Ordering::Relaxed) != RDR_FREE {
                continue;
            }
            // TXN-15 orderings: `Acquire` on success so this reader's
            // subsequent protocol reads happen-after the claim (pairing with
            // the previous owner's `Release` store of RDR_FREE, TXN-18a — the
            // clean slot handover); `Relaxed` on failure because a lost race
            // carries no data dependency (just try the next slot). The claim
            // publishes nothing another thread must observe yet — only the
            // later pin store (§4.3) is cross-thread-ordered — so no fence or
            // SeqCst is needed here.
            if slot
                .compare_exchange(RDR_FREE, RDR_CLAIMED, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                if let Some(pid) = owner {
                    // Relaxed load: the Acquire CAS above synchronizes with
                    // the previous releaser's Release store of FREE, which
                    // follows its owner-word store. While the txnid is not
                    // FREE only this claimer writes the word: a reaper's CAS
                    // needs a non-zero PID, and the word's PID is 0.
                    let w = rs.owner().load(Ordering::Relaxed);
                    // Release: a later reaper (ADR-0023 §3) loads the word
                    // with Acquire before judging the slot; this store must
                    // be visible no later than the pin that follows it. The
                    // generation increment makes the word unique to this
                    // claim (ADR-0023 amendment 14).
                    rs.owner().store(
                        owner_word(owner_generation(w).wrapping_add(1), pid),
                        Ordering::Release,
                    );
                }
                // `mti_numreaders` parity: LMDB bumps its counter only
                // when the first-fit scan lands *past* the current high-water
                // (`if (i == nr) ti->mti_numreaders = ++nr;`), so the value is
                // monotone. `fetch_max` is the same thing without LMDB's
                // reader mutex. Relaxed: this counter is pure introspection —
                // nothing reads it to make a correctness decision, and it
                // orders no other access. Monotonicity comes from `fetch_max`
                // being a single RMW, not from the ordering.
                self.high_water.fetch_max(i as u64 + 1, Ordering::Relaxed);
                return Some(i as u32);
            }
        }
        None
    }

    /// Publish a pinned txnid into an owned slot (the store half of the
    /// TXN-17 publish-and-verify loop, and the adopt-newer re-store of its
    /// tail — both monotone for a given owner).
    ///
    /// `SeqCst` (TXN-17, ARM weak model): the reader's `store slot` followed
    /// by its `load commit_point` and the writer's `store commit_point`
    /// followed by its `load slot` (TXN-19/20) are two store→load sequences —
    /// the classic StoreLoad hazard that neither `Acquire` nor `Release`
    /// prevents. Placing all four accesses in the single `SeqCst` total order
    /// is what guarantees the two sides cannot both "miss" each other.
    pub(crate) fn store_pin(&self, slot: u32, txnid: u64) {
        debug_assert!(
            txnid < RDR_CLAIMED,
            "a real snapshot txnid never reaches the sentinel band (TXN-14)"
        );
        self.slots()[slot as usize]
            .txnid()
            .store(txnid, Ordering::SeqCst);
    }

    /// Release an owned slot (SPEC 04 TXN-18a): store `RDR_FREE` with
    /// `Release`.
    ///
    /// `Release` justification: it orders the reader's prior page reads
    /// before the slot becomes reusable, pairing with the next claimer's
    /// `Acquire` CAS (TXN-15) for a clean handover. A full fence/SeqCst is
    /// unnecessary — the writer's SeqCst scan may still read the *old*
    /// (non-FREE) txnid of a just-departed reader, but that stale read only
    /// makes `oldest` *smaller* (the writer holds back reclamation it could
    /// have done), never larger, so it can never authorize reclaiming a page
    /// a live reader needs (TXN-20, closing paragraph). Exercised by loom L3.
    pub(crate) fn release(&self, slot: u32) {
        self.release_as(slot, self.owner_pid.is_some());
    }

    /// Release `slot`. `owned` says whether the slot carries an owner PID (a
    /// shared table), in which case the release is three stores (ADR-0023
    /// amendment 7):
    ///
    /// 1. `txnid = CLAIMED` (`Release`): unpins. It orders the owner's page
    ///    reads before the GC gate can stop counting this slot (the TXN-18a
    ///    argument). A death after this store leaves `CLAIMED` plus the dead
    ///    PID, which a reaper frees.
    /// 2. owner word with PID 0 and the generation kept (`Release`):
    ///    disowns. A death after this store leaves `CLAIMED` with no PID, the
    ///    known leak (ADR-0023 §3), which pins nothing. The word is loaded
    ///    `Relaxed` first: its only writer while the owner lives is the owner.
    /// 3. `txnid = FREE` (`Release`): hands the slot over. The next claimer's
    ///    `Acquire` CAS on FREE also sees PID 0 before it stores its own word
    ///    with the generation incremented. A reaper's CAS on the old word can
    ///    then succeed only before step 2, and a reaper acts only on a dead
    ///    owner, which cannot be mid-release.
    ///
    /// A local table (`owned == false`) does step 3 alone, as before
    /// (TXN-18a).
    pub(crate) fn release_as(&self, slot: u32, owned: bool) {
        let rs = &self.slots()[slot as usize];
        if owned {
            rs.txnid().store(RDR_CLAIMED, Ordering::Release);
            let w = rs.owner().load(Ordering::Relaxed);
            rs.owner()
                .store(owner_word(owner_generation(w), 0), Ordering::Release);
        }
        rs.txnid().store(RDR_FREE, Ordering::Release);
    }

    /// This table's owner PID: `Some` for a shared table.
    // Called once the open and claim paths use reaping (ADR-0023 stage 2);
    // exercised only by unit tests until then.
    #[allow(dead_code)]
    pub(crate) fn owner_pid(&self) -> Option<u32> {
        self.owner_pid
    }

    /// The minimum pinned snapshot txnid across all slots, or `None` if no
    /// slot holds a real txnid (SPEC 04 TXN-20 — the reader term of the GC
    /// gate; the `min(_, writer_txnid − 1)` fold lives in the caller,
    /// `RwTxn::oldest_reader`).
    ///
    /// Every load is `SeqCst`, pairing with `store_pin` (TXN-17). The
    /// two-case correctness proof (SPEC 04 §4.5), with the writer in txn `N`:
    ///
    /// 1. *Already-pinned readers (real txnid `v`) are never missed.* The
    ///    reader published `v` with a SeqCst store and this scan loads SeqCst;
    ///    both participate in the single SeqCst total order, and the writer
    ///    additionally holds the write mutex (the only path that advances
    ///    `commit_point`). Any pin visible before the scan is folded into
    ///    `oldest`.
    /// 2. *Skipping a `RDR_CLAIMED` slot is safe.* A mid-pin reader has not
    ///    pinned yet; when it does, it loads `commit_point` — currently
    ///    `≥ N − 1` — and its verify confirms no commit raced, so it ends
    ///    pinned at a snapshot `≥ N − 1`. A page freed by `F ≤ N − 1` left
    ///    `F`'s tree and every later tree, so no snapshot `≥ N − 1 ≥ F`
    ///    references it — the mid-pin reader cannot be reading a page this
    ///    writer reclaims.
    ///
    /// The whole table is scanned, always — max_readers SeqCst loads, at most
    /// once per write txn (TXN-22 caching, ADR-0006 decision 6).
    pub(crate) fn oldest(&self) -> Option<u64> {
        let mut oldest: Option<u64> = None;
        for rs in self.slots() {
            let v = rs.txnid().load(Ordering::SeqCst);
            if v == RDR_FREE || v == RDR_CLAIMED {
                continue;
            }
            oldest = Some(match oldest {
                Some(o) if o <= v => o,
                _ => v,
            });
        }
        oldest
    }

    /// The TXN-17 publish-and-verify loop against a commit-point atomic:
    /// publish `commit_point`'s current value into the owned `slot`, re-read,
    /// and retry until the two agree; returns the verified pin `t`.
    ///
    /// This is the **protocol core**, factored out of [`SnapshotCell::pin`]
    /// so a mutex-free model (the ADR-0006 Option-D/E fallback world) can
    /// drive the *same* code the shipped pin runs — see the mutation-check
    /// record below for why no such loom test is committable with loom 0.7.
    /// All accesses SeqCst; see [`ReaderTable::store_pin`]
    /// for the StoreLoad pairing argument. Liveness: the loop only retries
    /// while a commit is actively racing, and each commit strictly advances
    /// `commit_point`, so it terminates.
    pub(crate) fn publish_and_verify(&self, slot: u32, commit_point: &AtomicU64) -> u64 {
        let mut t = commit_point.load(Ordering::SeqCst);
        loop {
            self.store_pin(slot, t);
            let t2 = commit_point.load(Ordering::SeqCst);
            if t2 == t {
                return t;
            }
            // A commit landed between the two loads; retry with the newer t.
            t = t2;
        }
    }

    /// Pin `slot` against a published-txnid word and resolve the snapshot
    /// for the verified txnid: run [`ReaderTable::publish_and_verify`], call
    /// `resolve(t)`, and retry while it returns `Ok(None)` (the meta slot
    /// for `t` moved on, so a newer txnid is published), at most `limit`
    /// times. `Ok(None)` after `limit` attempts; errors pass through. The
    /// slot keeps the last verified pin either way, which is conservative
    /// for the GC gate (TXN-20); the caller releases it on failure.
    ///
    /// The lock-file reader (`EnvInner::pin_shared`, ADR-0023 §4) and the
    /// loom model of that protocol both run this function.
    // Under loom only the loom models call it (ADR-0023 stage 2).
    #[cfg_attr(loom, allow(dead_code))]
    pub(crate) fn pin_against<S, E>(
        &self,
        slot: u32,
        t_word: &AtomicU64,
        limit: u32,
        mut resolve: impl FnMut(u64) -> Result<Option<S>, E>,
    ) -> Result<Option<S>, E> {
        for _ in 0..limit {
            let t = self.publish_and_verify(slot, t_word);
            if let Some(s) = resolve(t)? {
                return Ok(Some(s));
            }
            #[cfg(not(loom))]
            std::thread::yield_now();
            #[cfg(loom)]
            loom::thread::yield_now();
        }
        Ok(None)
    }

    /// The table's fixed slot count — `max_readers` as configured at open
    /// (TXN-14; `mdb_env_get_maxreaders` / `MDB_envinfo::me_maxreaders`).
    pub(crate) fn capacity(&self) -> u32 {
        self.slots().len() as u32
    }

    /// `MDB_envinfo::me_numreaders` parity.
    ///
    /// **This is a high-water mark, not a live count** — the surprising part,
    /// verified against the fork's `mdb.c` (`mdb_txn_renew0`). LMDB allocates a
    /// reader slot by first-fit scan and only ever *increments*
    /// `mti_numreaders`, when the scan lands past the current high-water.
    /// Ending a read txn clears the slot's `mr_pid` but leaves the counter
    /// alone, so `me_numreaders` is the **maximum number of simultaneously
    /// live readers ever observed** by the env, and it never decreases. LMDB's
    /// own header documents it as "number of reader slots used", which is
    /// misleading; the differential test in
    /// `zerodb-oracle/tests/env_info_differential.rs` observed the real
    /// behavior. ZeroDB reproduces it exactly (LMDB parity) and offers
    /// the genuinely-live count separately as [`ReaderTable::in_use`].
    ///
    /// Logged in `docs/DIVERGENCES.md` (`me_numreaders` semantics; a PROPOSED
    /// candidate for later redefinition, not approved).
    ///
    /// Relaxed load: introspection only, orders nothing (see [`ReaderTable::claim`]).
    pub(crate) fn num_readers(&self) -> u32 {
        self.high_water.load(Ordering::Relaxed) as u32
    }

    /// Slots **currently** occupied — claimed *or* pinned. A ZeroDB extension:
    /// the number LMDB's `me_numreaders` looks like it should be but is not
    /// (see [`ReaderTable::num_readers`]).
    ///
    /// This is an **introspection** read, not part of the pin protocol: the
    /// value is a sample of a concurrently-mutating table and is only
    /// meaningful as a snapshot count. `RDR_CLAIMED` counts as occupied — the
    /// slot is owned. `SeqCst` for consistency with every other read of a slot
    /// word (TXN-17/19/20); the cost is irrelevant on a diagnostic path and
    /// using a weaker ordering here would need its own justification.
    pub(crate) fn in_use(&self) -> u32 {
        self.slots()
            .iter()
            .filter(|rs| rs.txnid().load(Ordering::SeqCst) != RDR_FREE)
            .count() as u32
    }

    /// Free every slot whose owner process is dead (ADR-0023 §3 and
    /// amendment 8); returns how many slots were freed. `reaper` is this
    /// process's PID: its own slots are never probed (a probe cannot see this
    /// process's own lock, and its stale own-PID slots were freed at open).
    /// `is_alive` is probed at most once per PID per scan.
    ///
    /// Per slot: load the owner word `w` (`Acquire`, pairing with the
    /// claimer's `Release` store, so a non-zero PID implies the claim CAS
    /// that preceded it). Skip PID 0 (free, being claimed, or being released:
    /// presumed live). Probe the owner. If it is dead, adopt the slot with
    /// `CAS w -> (generation of w, reaper)` (`AcqRel` on success: it is the
    /// single linearization point that two reapers, or a reaper and a
    /// re-claim, contend on; the `Acquire` half makes the dead owner's last
    /// stores visible before this reaper overwrites the slot). Then run the
    /// owner's release sequence. A reaper that dies after the CAS leaves a
    /// slot carrying its own PID, which the next reaper frees.
    ///
    /// The CAS is free of PID-reuse ABA (ADR-0023 amendment 14): every claim
    /// of the slot increments the generation in the word, so a slot freed and
    /// re-claimed since `w` was loaded, even by a new process with the same
    /// PID, fails the CAS. Wrap-around needs 2^32 claims of one slot while a
    /// reaper is paused between its load and its CAS, which is accepted.
    ///
    /// The scan can miss a slot that dies while it runs, and can see a
    /// released-then-reclaimed slot under its new owner; both only leave a
    /// slot for the next scan, never free a live one.
    ///
    /// # Errors
    ///
    /// The first probe error; slots freed before it stay freed.
    // Called once the open and claim paths use reaping (ADR-0023 stage 2);
    // exercised only by unit tests until then.
    #[allow(dead_code)]
    pub(crate) fn reap_dead(
        &self,
        reaper: u32,
        mut is_alive: impl FnMut(u32) -> std::io::Result<bool>,
    ) -> std::io::Result<u32> {
        let mut verdicts: Vec<(u32, bool)> = Vec::new();
        let mut freed = 0;
        for (i, rs) in self.slots().iter().enumerate() {
            let w = rs.owner().load(Ordering::Acquire);
            let pid = owner_pid(w);
            if pid == 0 || pid == reaper {
                continue;
            }
            let alive = match verdicts.iter().find(|(p, _)| *p == pid) {
                Some(&(_, a)) => a,
                None => {
                    let a = is_alive(pid)?;
                    verdicts.push((pid, a));
                    a
                }
            };
            if alive {
                continue;
            }
            // Relaxed on failure: a lost race (another reaper, or the slot
            // changed owner) carries no data; skip the slot.
            if rs
                .owner()
                .compare_exchange(
                    w,
                    owner_word(owner_generation(w), reaper),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                debug_assert_ne!(
                    rs.txnid().load(Ordering::Relaxed),
                    RDR_FREE,
                    "a slot with an owner PID is never FREE (ADR-0023 amendment 7)"
                );
                self.release_as(i as u32, true);
                freed += 1;
            }
        }
        Ok(freed)
    }

    /// Free every slot that carries this process's PID `me` (ADR-0023
    /// amendment 9). Called once at open, before this process's first claim,
    /// so any such slot was left by a dead process that had the same PID.
    /// Returns how many slots were freed.
    ///
    /// `CAS w -> (generation of w, PID 0)` (`AcqRel`, as in
    /// [`ReaderTable::reap_dead`]) is what excludes another process's reaper.
    /// That reaper may have probed `me` as dead before this process took its
    /// liveness lock; the word has changed, so its pending CAS fails. If this
    /// process later re-claims the slot, the claim increments the generation,
    /// so the reaper's CAS still fails. Then `txnid = FREE` (`Release`).
    /// Dying between the two stores leaves a pinned slot with PID 0 until
    /// the next first-opener wipe: a one-store window, documented in the ADR.
    // Called once the open and claim paths use reaping (ADR-0023 stage 2);
    // exercised only by unit tests until then.
    #[allow(dead_code)]
    pub(crate) fn reap_own_stale(&self, me: u32) -> u32 {
        let mut freed = 0;
        for rs in self.slots() {
            let w = rs.owner().load(Ordering::Acquire);
            if owner_pid(w) == me
                && rs
                    .owner()
                    .compare_exchange(
                        w,
                        owner_word(owner_generation(w), 0),
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                    .is_ok()
            {
                rs.txnid().store(RDR_FREE, Ordering::Release);
                freed += 1;
            }
        }
        freed
    }

    /// A **snapshot of the occupied slots** — the introspection primitive
    /// behind `Env::reader_list` (`mdb_reader_list`).
    ///
    /// Returns `(slot index, pinned txnid)` for every slot that is not
    /// `RDR_FREE`, in slot order; `None` for the txnid means the slot is
    /// `RDR_CLAIMED` — owned by a reader that is mid-pin and has not yet
    /// published a snapshot txnid (SPEC 04 §4.3).
    ///
    /// **The result is inherently stale.** Slots are read one at a time with
    /// no global lock (there is none to take — SPEC 04 §4 is a lock-free
    /// protocol), so this is not a linearizable snapshot of the whole table:
    /// a reader can be born or die between two loads, and the returned vector
    /// may correspond to no single instant. That is the honest and only
    /// possible semantics for lock-free introspection, and it is why nothing
    /// in the engine consults this — the GC gate uses
    /// [`ReaderTable::oldest`], which is correct *because* a stale read there
    /// can only under-estimate (TXN-20).
    ///
    /// `SeqCst` per slot, matching every other slot read (TXN-17/19/20); the
    /// ordering buys nothing here beyond consistency of style, and the cost is
    /// irrelevant on a diagnostic path.
    ///
    /// The third element is the owner PID, `None` for a local table or a slot
    /// whose PID word is 0 (being claimed or released). `Acquire`, as in
    /// [`ReaderTable::reap_dead`].
    pub(crate) fn list(&self) -> Vec<(u32, Option<u64>, Option<u32>)> {
        let mut out = Vec::new();
        for (i, rs) in self.slots().iter().enumerate() {
            let txnid = match rs.txnid().load(Ordering::SeqCst) {
                RDR_FREE => continue,
                RDR_CLAIMED => None,
                t => Some(t),
            };
            let pid = match self.owner_pid {
                None => None,
                Some(_) => Some(owner_pid(rs.owner().load(Ordering::Acquire))).filter(|&p| p != 0),
            };
            out.push((i as u32, txnid, pid));
        }
        out
    }

    /// Raw slot value (tests only).
    #[cfg(test)]
    pub(crate) fn raw(&self, slot: u32) -> u64 {
        self.slots()[slot as usize].txnid().load(Ordering::SeqCst)
    }
}

/// The published-snapshot cell (SPEC 04 TXN-18; ADR-0006 Option B): a
/// `Mutex<Arc<Snapshot>>` whose critical sections are all bounded O(1) pointer
/// operations (the writer's single swap per commit at C6; a reader's clone at
/// pin), never held across I/O, allocation, or tree work — so no reader ever
/// blocks on the write *transaction* (TXN-9), and the LMDB-NOTLS read-open
/// parity bar (one mutex per open) is met. The mirroring `commit_point` atomic carries the entire
/// lock-free pin protocol.
pub(crate) struct SnapshotCell {
    /// The immutable `(txnid, roots)` object readers `Arc`-clone (TXN-18).
    cell: Mutex<Arc<Snapshot>>,
    /// Mirrors the published object's txnid (TXN-17/19). All accesses SeqCst
    /// — see [`ReaderTable::store_pin`] for the StoreLoad pairing argument.
    commit_point: AtomicU64,
}

impl SnapshotCell {
    /// Seed the cell from the snapshot read at env open (the one and only
    /// time a durable meta *page* is read for roots — TXN-18).
    pub(crate) fn new(initial: Arc<Snapshot>) -> SnapshotCell {
        let txnid = initial.txnid;
        SnapshotCell {
            cell: Mutex::new(initial),
            commit_point: AtomicU64::new(txnid),
        }
    }

    /// The current commit point (the live snapshot's txnid, TXN-19).
    ///
    /// SeqCst: participates in the TXN-17/19/20 single total order (StoreLoad
    /// on ARM; SPEC 04 §4.3).
    pub(crate) fn commit_point(&self) -> u64 {
        self.commit_point.load(Ordering::SeqCst)
    }

    /// `Arc`-clone the published snapshot. The clone keeps the
    /// `(txnid, roots)` alive for the caller's life regardless of later
    /// commits. Critical section: one refcount bump (TXN-18).
    pub(crate) fn clone_snapshot(&self) -> Arc<Snapshot> {
        Arc::clone(&self.cell.lock().expect("snapshot cell poisoned"))
    }

    /// Publish a freshly committed snapshot (commit step C6) in the TXN-19
    /// order: (1) swap the `Arc<Snapshot>` into the cell, then (2) store the
    /// commit point `SeqCst`. Object-before-counter guarantees a reader that
    /// observes the new counter can always clone the matching (or newer)
    /// roots — inverting these two steps is the R3 publish-order trap
    /// (ADR-0006), caught by loom L4.
    ///
    /// Called only under the write mutex (C6), so publishes are serialized
    /// and `commit_point` is strictly monotone.
    pub(crate) fn publish(&self, snap: Arc<Snapshot>) {
        let txnid = snap.txnid;
        let old = {
            let mut g = self.cell.lock().expect("snapshot cell poisoned");
            std::mem::replace(&mut *g, snap)
        };
        // SeqCst: the writer half of the TXN-17 StoreLoad pairing (ARM weak
        // memory) — this store and the reader's publish-and-verify loop must
        // share the single SeqCst total order.
        self.commit_point.store(txnid, Ordering::SeqCst);
        // Drop the previous snapshot's Arc only after unlocking: if this was
        // its last reference, deallocation runs here, outside the critical
        // section — keeping the section strictly O(1) pointer ops as TXN-18
        // requires.
        drop(old);
    }

    /// Replace the cached snapshot with `snap` if it is newer (ADR-0023 §4:
    /// a snapshot decoded after another process committed). Only snapshots
    /// validated against the published txnid are ever passed here. Does not
    /// touch `commit_point`, which lock-file environments do not pin against.
    ///
    /// Critical section: one comparison and one pointer swap (TXN-18); the
    /// displaced `Arc` is dropped after the unlock, as in [`Self::publish`].
    pub(crate) fn install_if_newer(&self, snap: Arc<Snapshot>) {
        let old = {
            let mut g = self.cell.lock().expect("snapshot cell poisoned");
            if snap.txnid <= g.txnid {
                return;
            }
            std::mem::replace(&mut *g, snap)
        };
        drop(old);
    }

    /// Pin a snapshot (SPEC 04 TXN-10 steps 1–3): claim a slot, run the
    /// TXN-17 publish-and-verify loop against `commit_point`, then clone the
    /// published object (adopting a newer one if a commit landed between the
    /// verify and the clone — the TXN-17 tail). Returns the pinned snapshot
    /// and the owned slot index; `None` means the table is exhausted
    /// (TXN-16 → `ReadersFull` in the caller).
    ///
    /// The invariant the tail preserves is `slot value ≤ cloned snapshot
    /// txnid`: pinning *older* than what is read is conservative (GC merely
    /// holds back more), while the reverse would let GC reclaim pages the
    /// cloned roots still reference — the unsafe direction (ADR-0006 R3).
    ///
    /// Why the clone can never see roots *older* than the verified `t`: the
    /// writer's swap is sequenced-before its `commit_point.store(t)`
    /// (TXN-19); a load that reads `t` therefore happens-after the swap, and
    /// the reader's subsequent lock of the same mutex is ordered after the
    /// writer's unlock — it observes the swapped (or newer) `Arc`.
    pub(crate) fn pin(&self, table: &ReaderTable) -> Option<(Arc<Snapshot>, u32)> {
        // Step 1 — claim (TXN-15/16).
        let slot = table.claim()?;
        // Step 2 — publish-and-verify (TXN-17): the factored protocol core.
        let t = table.publish_and_verify(slot, &self.commit_point);
        // Step 3 — clone the roots from the published object, never from a
        // durable meta page (TXN-10 step 3).
        let snap = self.clone_snapshot();
        debug_assert!(
            snap.txnid >= t,
            "publish-order inversion: cloned roots {} older than verified commit point {t} \
             (TXN-19 object-before-counter violated)",
            snap.txnid
        );
        if snap.txnid > t {
            // TXN-17 tail: a commit landed between the verify and the clone;
            // adopt the newer object and re-store its txnid — monotone, and
            // still a validly pinned, newer snapshot.
            table.store_pin(slot, snap.txnid);
        }
        Some((snap, slot))
    }
}

// ---------------------------------------------------------------------------
// Native unit tests (run under `cargo test` and miri — the std path).
// ---------------------------------------------------------------------------

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::page::DBRecord;

    fn snap(txnid: u64) -> Arc<Snapshot> {
        Arc::new(Snapshot {
            txnid,
            last_pg: 1,
            main_db: DBRecord::empty(),
            free_db: DBRecord::empty(),
            free_annex: Arc::from(&[][..]),
        })
    }

    #[test]
    fn claim_release_exhaustion_cycle() {
        let t = ReaderTable::new(2);
        let a = t.claim().expect("slot 0");
        let b = t.claim().expect("slot 1");
        assert_ne!(a, b);
        assert_eq!(t.claim(), None, "exhausted table must refuse (TXN-16)");
        t.release(a);
        assert_eq!(t.claim(), Some(a), "released slot is reusable");
        t.release(a);
        t.release(b);
        assert_eq!(t.claim(), Some(0));
    }

    #[test]
    fn zero_capacity_table_is_always_full() {
        // max_readers = 0 is degenerate but must not panic: every read txn
        // fails ReadersFull. The fork instead rejects the open with EINVAL —
        // filed in docs/DIVERGENCES.md (`max_readers(0)` at open); no consumer
        // passes 0.
        let t = ReaderTable::new(0);
        assert_eq!(t.claim(), None);
        assert_eq!(t.oldest(), None);
    }

    #[test]
    fn oldest_skips_sentinels_and_takes_min() {
        let t = ReaderTable::new(4);
        assert_eq!(t.oldest(), None);
        let a = t.claim().unwrap(); // left RDR_CLAIMED — must be skipped
        let b = t.claim().unwrap();
        let c = t.claim().unwrap();
        t.store_pin(b, 9);
        t.store_pin(c, 7);
        assert_eq!(t.raw(a), RDR_CLAIMED);
        assert_eq!(t.oldest(), Some(7), "min of real pins, CLAIMED skipped");
        t.release(c);
        assert_eq!(t.oldest(), Some(9));
        t.release(b);
        t.release(a);
        assert_eq!(t.oldest(), None);
    }

    #[test]
    fn pin_takes_current_snapshot_and_slot_matches() {
        let table = ReaderTable::new(2);
        let cell = SnapshotCell::new(snap(5));
        let (s, slot) = cell.pin(&table).expect("slot available");
        assert_eq!(s.txnid, 5);
        assert_eq!(table.raw(slot), 5, "slot carries the pinned txnid");
        cell.publish(snap(6));
        // The existing pin is untouched by a later publish.
        assert_eq!(table.raw(slot), 5);
        let (s2, slot2) = cell.pin(&table).expect("second slot");
        assert_eq!(s2.txnid, 6);
        assert_eq!(table.raw(slot2), 6);
        table.release(slot);
        table.release(slot2);
    }

    #[test]
    fn pin_exhaustion_returns_none() {
        let table = ReaderTable::new(1);
        let cell = SnapshotCell::new(snap(1));
        let (_s, slot) = cell.pin(&table).unwrap();
        assert!(cell.pin(&table).is_none(), "TXN-16");
        table.release(slot);
        assert!(cell.pin(&table).is_some());
    }

    #[test]
    fn publish_updates_commit_point_and_object_together() {
        let cell = SnapshotCell::new(snap(3));
        assert_eq!(cell.commit_point(), 3);
        assert_eq!(cell.clone_snapshot().txnid, 3);
        cell.publish(snap(4));
        assert_eq!(cell.commit_point(), 4);
        assert_eq!(cell.clone_snapshot().txnid, 4);
    }

    #[test]
    fn shared_claim_records_pid_and_release_clears_it_first() {
        use crate::shared::{HeapRegion, SharedRegion};
        let region = Arc::new(HeapRegion::new(2, 4242, true));
        let t = ReaderTable::new_shared(region.clone());
        assert_eq!(t.capacity(), 2);
        let s = t.claim().expect("slot");
        assert_eq!(
            owner_pid(region.slots()[s as usize].owner().load(Ordering::SeqCst)),
            4242
        );
        assert_eq!(
            region.slots()[s as usize].txnid().load(Ordering::SeqCst),
            RDR_CLAIMED
        );
        t.store_pin(s, 7);
        assert_eq!(t.oldest(), Some(7));
        t.release(s);
        assert_eq!(
            owner_pid(region.slots()[s as usize].owner().load(Ordering::SeqCst)),
            0
        );
        assert_eq!(
            region.slots()[s as usize].txnid().load(Ordering::SeqCst),
            RDR_FREE
        );
        assert_eq!(t.oldest(), None);
    }

    #[test]
    fn shared_oldest_sees_slots_written_by_another_process() {
        use crate::shared::{HeapRegion, SharedRegion};
        let region = Arc::new(HeapRegion::new(3, 1, true));
        // Slot 2 pinned at txnid 5 by "another process" (written directly).
        region.slots()[2]
            .owner()
            .store(owner_word(1, 99), Ordering::SeqCst);
        region.slots()[2].txnid().store(5, Ordering::SeqCst);
        let t = ReaderTable::new_shared(region.clone());
        let mine = t.claim().expect("slot");
        assert_eq!(mine, 0);
        t.store_pin(mine, 9);
        assert_eq!(t.oldest(), Some(5));
    }

    #[test]
    fn shared_exhaustion_returns_none() {
        use crate::shared::HeapRegion;
        let t = ReaderTable::new_shared(Arc::new(HeapRegion::new(1, 1, true)));
        let _a = t.claim().expect("slot");
        assert!(t.claim().is_none());
    }

    fn shared_table(slots: u32, pid: u32) -> (Arc<crate::shared::HeapRegion>, ReaderTable) {
        let region = Arc::new(crate::shared::HeapRegion::new(slots, pid, true));
        let table =
            ReaderTable::new_shared(Arc::clone(&region) as Arc<dyn crate::shared::SharedRegion>);
        (region, table)
    }

    #[test]
    fn release_leaves_a_free_unowned_slot() {
        // The owner's release order is CLAIMED, pid 0, FREE (ADR-0023
        // amendment 7); the end state is a free, unowned slot.
        let (region, t) = shared_table(1, 7);
        let s = t.claim().unwrap();
        t.store_pin(s, 5);
        t.release(s);
        assert_eq!(t.raw(s), RDR_FREE);
        assert_eq!(
            owner_pid(
                crate::shared::SharedRegion::slots(&*region)[0]
                    .owner()
                    .load(Ordering::SeqCst)
            ),
            0
        );
    }

    #[test]
    fn reap_frees_only_dead_processes_slots() {
        let (region, t) = shared_table(4, 1);
        let a = t.claim_as(Some(2)).unwrap(); // process 2, will die
        t.store_pin(a, 5);
        let b = t.claim_as(Some(3)).unwrap(); // process 3, alive
        t.store_pin(b, 6);
        let c = t.claim().unwrap(); // this process (1)
        t.store_pin(c, 7);
        region.kill(2);
        let n = t
            .reap_dead(1, |p| crate::shared::SharedRegion::is_alive(&*region, p))
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(t.raw(a), RDR_FREE);
        assert_eq!(t.raw(b), 6);
        assert_eq!(t.raw(c), 7);
        assert_eq!(t.oldest(), Some(6));
    }

    #[test]
    fn reap_counts_each_dead_slot_once() {
        let (region, t) = shared_table(4, 1);
        for _ in 0..2 {
            let s = t.claim_as(Some(2)).unwrap();
            t.store_pin(s, 5);
        }
        region.kill(2);
        let alive = |p| crate::shared::SharedRegion::is_alive(&*region, p);
        assert_eq!(t.reap_dead(1, alive).unwrap(), 2);
        assert_eq!(t.reap_dead(1, alive).unwrap(), 0);
        assert_eq!(t.oldest(), None);
    }

    #[test]
    fn reap_skips_claimed_slots_without_a_pid() {
        // The known leak (ADR-0023 §3): a claim CAS without its PID store.
        let (_region, t) = shared_table(2, 1);
        let s = t.claim_as(None).unwrap();
        assert_eq!(t.reap_dead(1, |_| Ok(false)).unwrap(), 0);
        assert_eq!(t.raw(s), RDR_CLAIMED);
    }

    #[test]
    fn reap_never_probes_its_own_pid() {
        let (_region, t) = shared_table(2, 1);
        let s = t.claim().unwrap();
        t.store_pin(s, 5);
        let n = t
            .reap_dead(1, |p| {
                assert_ne!(p, 1, "probed own PID");
                Ok(true)
            })
            .unwrap();
        assert_eq!(n, 0);
        assert_eq!(t.raw(s), 5);
    }

    #[test]
    fn reap_probes_each_pid_once_per_scan() {
        let (_region, t) = shared_table(4, 1);
        for _ in 0..3 {
            let s = t.claim_as(Some(9)).unwrap();
            t.store_pin(s, 5);
        }
        let mut probes = 0;
        let n = t
            .reap_dead(1, |_| {
                probes += 1;
                Ok(false)
            })
            .unwrap();
        assert_eq!((n, probes), (3, 1));
    }

    #[test]
    fn reap_propagates_a_probe_error_without_freeing() {
        let (_region, t) = shared_table(2, 1);
        let s = t.claim_as(Some(2)).unwrap();
        t.store_pin(s, 5);
        assert!(t
            .reap_dead(1, |_| Err(std::io::Error::other("probe")))
            .is_err());
        assert_eq!(t.raw(s), 5);
    }

    #[test]
    fn own_pid_stale_slots_are_freed() {
        // A dead predecessor with this PID (1) left two slots; another
        // process (2) holds one.
        let (_region, t) = shared_table(4, 1);
        for v in [5, 6] {
            let s = t.claim_as(Some(1)).unwrap();
            t.store_pin(s, v);
        }
        let other = t.claim_as(Some(2)).unwrap();
        t.store_pin(other, 7);
        assert_eq!(t.reap_own_stale(1), 2);
        assert_eq!(t.oldest(), Some(7));
        assert_eq!(t.list(), vec![(other, Some(7), Some(2))]);
    }

    #[test]
    fn list_reports_the_owner_pid() {
        let (_region, t) = shared_table(3, 4);
        let a = t.claim().unwrap();
        t.store_pin(a, 3);
        let b = t.claim_as(None).unwrap();
        assert_eq!(t.list(), vec![(a, Some(3), Some(4)), (b, None, None)]);
        let local = ReaderTable::new(2);
        let c = local.claim().unwrap();
        local.store_pin(c, 2);
        assert_eq!(local.list(), vec![(c, Some(2), None)]);
    }

    #[test]
    fn pin_against_retries_until_resolved() {
        let t = ReaderTable::new(1);
        let word = AtomicU64::new(4);
        let s = t.claim().unwrap();
        let mut calls = 0;
        let got: Result<Option<u64>, ()> = t.pin_against(s, &word, 8, |v| {
            calls += 1;
            if calls < 3 {
                // Meta slot moved on: a newer T is published.
                word.store(v + 1, Ordering::SeqCst);
                Ok(None)
            } else {
                Ok(Some(v))
            }
        });
        assert_eq!(got, Ok(Some(6)));
        assert_eq!(t.raw(s), 6);
        let none: Result<Option<u64>, ()> = t.pin_against(s, &word, 2, |_| Ok(None));
        assert_eq!(none, Ok(None), "gives up after `limit` attempts");
    }

    #[test]
    fn reap_cannot_free_a_slot_reclaimed_under_a_reused_pid() {
        let (_region, t) = shared_table(1, 1);
        let s = t.claim_as(Some(2)).unwrap();
        t.store_pin(s, 5); // process 2 pinned 5, then died
        let n = t
            .reap_dead(3, |pid| {
                assert_eq!(pid, 2);
                // While the reaper holds its stale load: a new process with
                // PID 2 frees its predecessor's slot and re-claims it.
                assert_eq!(t.reap_own_stale(2), 1);
                let again = t.claim_as(Some(2)).unwrap();
                t.store_pin(again, 6);
                Ok(false) // the reaper's verdict on the old process 2
            })
            .unwrap();
        assert_eq!(
            n, 0,
            "the reaper freed a slot re-claimed under the same PID"
        );
        assert_eq!(t.raw(s), 6);
        assert_eq!(t.list(), vec![(s, Some(6), Some(2))]);
    }

    #[test]
    fn each_claim_bumps_the_generation_and_release_keeps_it() {
        let (region, t) = shared_table(1, 1);
        let word = || {
            crate::shared::SharedRegion::slots(&*region)[0]
                .owner()
                .load(Ordering::SeqCst)
        };
        let s = t.claim().unwrap();
        let g1 = owner_generation(word());
        assert_eq!(owner_pid(word()), 1);
        t.release(s);
        assert_eq!(word(), owner_word(g1, 0));
        t.claim().unwrap();
        assert_eq!(owner_generation(word()), g1.wrapping_add(1));
    }
}

// ---------------------------------------------------------------------------
// The loom suite (the reader table's model-checking gate; ADR-0006 L1–L5). Runs only
// under `just loom` (`RUSTFLAGS="--cfg loom" cargo test -p zerodb-core --lib
// loom_`). Models are deliberately tiny (≤ 2 spawned threads + main) so loom
// explores them exhaustively.
//
// Mutation-check record (ADR-0006):
//
// (a) Inverting the TXN-19 publish order (counter before object) fails
//     L2 + L4 + L5 — the genuinely load-bearing order under Option B is
//     machine-checked.
// (b) Weakening the pin store to `Release` or the scan load to `Relaxed`
//     does NOT fail this suite, for two stacked reasons:
//     (i) in the faithful with-mutex models the weakenings are genuinely
//     safe — the cell mutex creates happens-before chains (an old reader's
//     pin-completing clone-unlock HB every later publish/begin lock HB the
//     scan) that keep old pins visible, while a mid-pin reader lands
//     `≥ N − 1` and is safe to miss (TXN-20 case 2); (ii) loom 0.7 CANNOT
//     detect SC-access weakenings at all — a mutex-free "L2b" model (pin via
//     `publish_and_verify` against a bare commit-point atomic, the ADR-0006
//     Option-D/E fallback world) is not committable because loom reports
//     the Dekker violation even for the correct all-SeqCst code, and a
//     minimal all-SeqCst store-buffer litmus run under loom 0.7 confirms it
//     explores the both-miss outcome C++20 (P0668) and AArch64 RCsc forbid:
//     loom implements the pre-repair SC-access semantics (its SC support is
//     fence-oriented; tokio-rs/loom#180 class). A test that fails on correct
//     code cannot be committed.
// The SeqCst sites are therefore guarded by SPEC 04 TXN-17/20 (normative),
// the per-site justification comments, this record, and ADR-0006 R1: any
// future migration to the Option-D/E lock-free cell MUST bring its own
// StoreLoad verification (an SC-fence reformulation loom can check,
// a different checker, or hardware litmus runs) as a precondition — the
// mutex HB chains that make the weakenings survivable today vanish there.
// ---------------------------------------------------------------------------

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use crate::page::DBRecord;
    use loom::thread;

    fn snap(txnid: u64) -> Arc<Snapshot> {
        Arc::new(Snapshot {
            txnid,
            last_pg: 1,
            main_db: DBRecord::empty(),
            free_db: DBRecord::empty(),
            free_annex: Arc::from(&[][..]),
        })
    }

    /// L1a — two readers race claims on a ONE-slot table: exactly one wins;
    /// the loser observes exhaustion (TXN-15/16). No double-ownership.
    #[test]
    fn loom_l1_claim_race_one_slot() {
        loom::model(|| {
            let t = Arc::new(ReaderTable::new(1));
            let t2 = Arc::clone(&t);
            let other = thread::spawn(move || t2.claim());
            let mine = t.claim();
            let theirs = other.join().unwrap();
            match (mine, theirs) {
                (Some(0), None) | (None, Some(0)) => {}
                bad => panic!("claim race must have exactly one winner, got {bad:?}"),
            }
        });
    }

    /// L1b — two readers race claims on a TWO-slot table: both win, distinct
    /// slots; after release the slot is reclaimable.
    #[test]
    fn loom_l1_claim_race_two_slots_then_reuse() {
        loom::model(|| {
            let t = Arc::new(ReaderTable::new(2));
            let t2 = Arc::clone(&t);
            let other = thread::spawn(move || t2.claim());
            let mine = t.claim().expect("two slots, two claimers");
            let theirs = other.join().unwrap().expect("two slots, two claimers");
            assert_ne!(mine, theirs, "double-ownership of a slot");
            t.release(mine);
            assert_eq!(t.claim(), Some(mine), "released slot reusable");
        });
    }

    /// L2 — the machine-checked TXN-20 two-case proof: one reader pins (full
    /// TXN-17 loop + clone + adopt tail) while the writer publishes commit 6
    /// and then — as the next write txn N = 7 would — computes the GC gate
    /// with seed N − 1 = 6. In every interleaving the reader's final pinned
    /// snapshot must be `≥` the writer's computed `oldest`, else GC could
    /// reclaim pages the reader's roots still reference.
    #[test]
    fn loom_l2_pin_vs_publish_and_scan() {
        loom::model(|| {
            let table = Arc::new(ReaderTable::new(1));
            let cell = Arc::new(SnapshotCell::new(snap(5)));
            let (tr, cr) = (Arc::clone(&table), Arc::clone(&cell));
            let reader = thread::spawn(move || {
                let (s, _slot) = cr.pin(&tr).expect("one slot, one reader");
                s.txnid
            });
            // Writer: commit N = 6 publishes (TXN-19 order inside publish),
            // then the next writer (N = 7) computes oldest with seed 6.
            cell.publish(snap(6));
            let oldest = table.oldest().map_or(6, |r| r.min(6));
            let pinned = reader.join().unwrap();
            assert!(pinned == 5 || pinned == 6, "reader pinned {pinned}");
            assert!(
                pinned >= oldest,
                "GC gate computed oldest={oldest} but a live reader is pinned at {pinned}: \
                 reclaiming pages freed by F ≤ {oldest} would corrupt that snapshot (TXN-20)"
            );
        });
    }

    /// L3 — release vs the writer's scan: a stale (pre-release) read is
    /// conservative only — `oldest` may report the departed reader (holding
    /// reclamation back) but never anything else; after the join the slot is
    /// observably free and reclaimable (TXN-18a / TXN-20 closing argument).
    ///
    /// Note: this is a **one-sided** (never-unsafe) property. The
    /// test cannot distinguish "the scan read a stale pre-release value" from
    /// "the scan simply ran before the release" — both yield `Some(5)` — so
    /// it pins conservatism and post-join visibility, not scan precision.
    #[test]
    fn loom_l3_release_vs_oldest_scan() {
        loom::model(|| {
            let table = Arc::new(ReaderTable::new(1));
            let slot = table.claim().expect("empty table");
            table.store_pin(slot, 5);
            let t2 = Arc::clone(&table);
            let releaser = thread::spawn(move || t2.release(slot));
            let o = table.oldest(); // races the release
            releaser.join().unwrap();
            assert!(
                o == Some(5) || o.is_none(),
                "scan racing a release must read the old pin or FREE, got {o:?}"
            );
            // join() synchronizes: the release is now globally visible.
            assert_eq!(table.oldest(), None);
            assert_eq!(table.claim(), Some(slot), "freed slot reclaimable");
        });
    }

    /// L4 — publish racing the pin retry loop: the reader must end with
    /// `slot value ≤ cloned snapshot txnid` (pinning older than what it reads
    /// is conservative; the reverse is the unsafe direction) and with roots
    /// no older than its verified commit point. A TXN-19 publish-order
    /// inversion (counter before object) fails this test (ADR-0006 R3).
    #[test]
    fn loom_l4_publish_vs_pin_retry() {
        loom::model(|| {
            let table = Arc::new(ReaderTable::new(1));
            let cell = Arc::new(SnapshotCell::new(snap(5)));
            let c2 = Arc::clone(&cell);
            let writer = thread::spawn(move || c2.publish(snap(6)));
            let (s, slot) = cell.pin(&table).expect("one slot, one reader");
            writer.join().unwrap();
            assert!(s.txnid == 5 || s.txnid == 6, "cloned {}", s.txnid);
            // Raw slot read (test-only): the pin the writer's scan would see.
            let raw = table.raw(slot);
            assert!(
                raw <= s.txnid,
                "slot pinned {raw} but cloned roots are {} — GC could reclaim pages \
                 those roots reference (slot ≤ snap invariant, ADR-0006 R3)",
                s.txnid
            );
            table.release(slot);
        });
    }

    /// L5 — slot reuse: reader A pins and releases while reader B pins,
    /// with the commit point advanced to 6 before B starts. B can never
    /// resurrect A's older pin: whichever slot B gets, its pin is the
    /// current commit point (6), and the post-join scan reflects only live
    /// pins.
    #[test]
    fn loom_l5_slot_reuse_no_resurrection() {
        loom::model(|| {
            let table = Arc::new(ReaderTable::new(1));
            let cell = Arc::new(SnapshotCell::new(snap(5)));
            let (ta, ca) = (Arc::clone(&table), Arc::clone(&cell));
            let a = thread::spawn(move || match ca.pin(&ta) {
                Some((s, slot)) => {
                    let t = s.txnid;
                    ta.release(slot);
                    Some(t)
                }
                None => None, // B held the only slot at that moment
            });
            // Advance the commit point, then B pins (program order: B's loop
            // must observe ≥ 6).
            cell.publish(snap(6));
            let b = cell.pin(&table); // may be ReadersFull while A holds
            let a_t = a.join().unwrap();
            assert!(
                a_t.is_some() || b.is_some(),
                "with one slot and one release, at least one pin succeeds"
            );
            if let Some(t) = a_t {
                assert!(t == 5 || t == 6, "A pinned {t}");
            }
            match b {
                Some((s, slot)) => {
                    assert_eq!(s.txnid, 6, "B pins the already-published 6, never A's 5");
                    assert_eq!(table.oldest(), Some(6), "scan sees only the live pin");
                    table.release(slot);
                }
                None => {
                    // A held the slot during B's scan; after the join A has
                    // released — the table must be observably empty.
                    assert_eq!(table.oldest(), None);
                }
            }
        });
    }
}
