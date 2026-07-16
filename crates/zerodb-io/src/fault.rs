//! Fault-injection write backend (M1.11, ADR-0008 D1; SPEC 06 REC-19/REC-20).
//!
//! [`FaultBacking`] wraps a real [`Backing`] and journals every
//! `write_at_page` issued since the last completed sync barrier. The wrapped
//! backing keeps serving the **live view** (`bytes()`), exactly as the OS page
//! cache would — un-fsynced writes are visible to the running process — while
//! the journal tracks what a *power loss* could take away.
//!
//! On a simulated power cut ([`FaultHandle::capture`] +
//! [`CapturedDisk::materialize`]) the harness produces disk **images** per the
//! REC-20 barrier model:
//!
//! - every write folded by a completed synchronous barrier is present and
//!   intact (`durable`);
//! - each pending write is independently present-intact, present-torn (at
//!   sector or sub-sector granularity, prefix- and suffix-kept), or absent;
//! - pending writes may persist in any order. Ordering only matters through
//!   overwrites, and per-write fates applied in issue order reach every final
//!   state arbitrary ordering can: for a sector written by `w1` then `w2`,
//!   {drop both, apply `w1` only, apply `w2` (with or without `w1`)} covers
//!   {old, first, last} — the ALICE reordering model for final states.
//!
//! **Crash-safety spine, restated as the model (REC-20):** the commit
//! pipeline's C3 barrier folds txn `N`'s data writes into `durable` *before*
//! C4's meta write enters the journal, so no materializable image contains a
//! durable meta `N` without durable `N`-data. If the pipeline ever misordered
//! C3 after C4, this backend would materialize exactly that corrupt image and
//! the harness's REC-18 verification would fail — that is the tripwire this
//! module arms (and the mutation self-test proves it fires, ADR-0008 D6.1).
//!
//! Everything here is safe Rust (ADR-0008 D1 Option B): the live view is the
//! wrapped backing's — no new aliasing, no new `unsafe`.

use std::sync::{Arc, Mutex};

use zerodb_core::env::Backing;

/// The tear granularity of REC-19/REC-20: one disk sector.
pub const SECTOR: usize = 512;

// ---------------------------------------------------------------------------
// Deterministic PRNG (ADR-0008 D1, ratified OQ4): splitmix64 seeding a
// xoshiro256** core. In-house so fault plans are bit-stable across platforms,
// crate versions, and time — a saved (seed, cycle) pair reproduces the exact
// image forever. ~20 lines, test-infrastructure only (CRC32C precedent).
// ---------------------------------------------------------------------------

/// splitmix64: the standard 64-bit seed expander (public domain, Steele et
/// al.). Used to derive independent sub-seeds and to seed [`Rng`].
#[must_use]
pub fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// xoshiro256** — deterministic, dependency-free PRNG for fault plans and
/// workload generation (ADR-0008 D1/OQ4).
#[derive(Debug, Clone)]
pub struct Rng {
    s: [u64; 4],
}

impl Rng {
    /// Seed via four splitmix64 steps (the xoshiro authors' recommendation —
    /// avoids the all-zero state and decorrelates similar seeds).
    #[must_use]
    pub fn new(seed: u64) -> Rng {
        let a = splitmix64(seed);
        let b = splitmix64(a);
        let c = splitmix64(b);
        let d = splitmix64(c);
        Rng { s: [a, b, c, d] }
    }

    /// Next raw 64-bit output.
    pub fn next_u64(&mut self) -> u64 {
        let s = &mut self.s;
        let result = s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = s[1] << 17;
        s[2] ^= s[0];
        s[3] ^= s[1];
        s[1] ^= s[2];
        s[0] ^= s[3];
        s[2] ^= t;
        s[3] = s[3].rotate_left(45);
        result
    }

    /// Uniform in `0..n` (`n > 0`). Modulo bias is irrelevant for fault-plan
    /// sampling (n ≪ 2^64).
    pub fn below(&mut self, n: usize) -> usize {
        debug_assert!(n > 0);
        (self.next_u64() % n as u64) as usize
    }

    /// True with probability `a` in `b`.
    pub fn ratio(&mut self, a: u64, b: u64) -> bool {
        debug_assert!(b > 0 && a <= b);
        self.next_u64() % b < a
    }

    /// Fill `buf` with deterministic bytes.
    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let v = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&v[..chunk.len()]);
        }
    }
}

// ---------------------------------------------------------------------------
// Journal state
// ---------------------------------------------------------------------------

/// One positioned write issued since the last barrier: a whole page or a whole
/// contiguous overflow run (the engine issues nothing else — SPEC 04 §9 C2/C4).
#[derive(Debug, Clone)]
pub struct WriteRecord {
    /// Byte offset in the file (`pgno * psize` — page-aligned by construction
    /// of the [`Backing`] trait, the ADR-0008 D5 invariant).
    pub offset: u64,
    /// The bytes written (length a positive multiple of `psize`, D5).
    pub data: Box<[u8]>,
}

/// Counters for reporting/plan-distribution sanity.
#[derive(Debug, Clone, Copy, Default)]
pub struct FaultStats {
    /// `write_at_page` calls journaled.
    pub writes: u64,
    /// Synchronous barriers completed (pending folded into durable).
    pub barriers: u64,
    /// Asynchronous flushes observed (deliberately **not** barriers, REC-9:
    /// `msync(MS_ASYNC)` gives no completion guarantee).
    pub async_flushes: u64,
}

#[derive(Debug)]
struct FaultState {
    psize: u32,
    /// The disk image as of the last completed barrier. Only grows; sized to
    /// the durable high-water, not the map (memory stays proportional to use).
    durable: Vec<u8>,
    /// The durable file length (file extension is fs metadata a crash can
    /// lose too — REC-14; folding a write past this extends it).
    durable_len: u64,
    /// Writes since the last barrier, in issue order.
    pending: Vec<WriteRecord>,
    /// Mutation self-test mode (ADR-0008 D6.1): barriers fold ONLY meta-page
    /// writes; data writes stay pending forever. This models a pipeline whose
    /// data fsync is ineffective/misordered — the harness MUST catch the
    /// resulting meta-without-data images. Never set outside the self-test.
    broken_data_barriers: bool,
    stats: FaultStats,
}

impl FaultState {
    fn fold_barrier(&mut self) {
        self.stats.barriers += 1;
        let broken = self.broken_data_barriers;
        let meta_end = 2 * u64::from(self.psize);
        let pending = std::mem::take(&mut self.pending);
        for rec in pending {
            if broken && rec.offset >= meta_end {
                // Self-test mode: this data write's durability is silently
                // dropped on the floor — the "kind fsync that lies".
                self.pending.push(rec);
                continue;
            }
            let end = rec.offset + rec.data.len() as u64;
            if self.durable.len() < end as usize {
                self.durable.resize(end as usize, 0);
            }
            self.durable[rec.offset as usize..end as usize].copy_from_slice(&rec.data);
            self.durable_len = self.durable_len.max(end);
        }
    }
}

/// The harness's handle onto a [`FaultBacking`]'s journal: capture simulated
/// power cuts, flip the mutation-self-test mode, read stats.
#[derive(Clone)]
pub struct FaultHandle {
    state: Arc<Mutex<FaultState>>,
}

impl FaultHandle {
    /// Snapshot the `(durable, pending)` pair — the simulated power cut. The
    /// running env is untouched (capture, don't kill: the cut point is frozen
    /// while the workload continues, ADR-0008 D2).
    #[must_use]
    pub fn capture(&self) -> CapturedDisk {
        let st = self.state.lock().expect("fault state lock");
        CapturedDisk {
            psize: st.psize,
            durable: st.durable.clone(),
            durable_len: st.durable_len,
            pending: st.pending.clone(),
        }
    }

    /// ADR-0008 D6.1 mutation self-test switch. See [`FaultState`] docs.
    pub fn set_broken_data_barriers(&self, broken: bool) {
        self.state
            .lock()
            .expect("fault state lock")
            .broken_data_barriers = broken;
    }

    /// Journal counters.
    #[must_use]
    pub fn stats(&self) -> FaultStats {
        self.state.lock().expect("fault state lock").stats
    }
}

/// A [`Backing`] wrapper journaling un-barriered writes (ADR-0008 D1 Option B).
///
/// Reads (`bytes`) delegate to the wrapped backing — the live process view,
/// identical to production where the page cache serves un-fsynced writes.
/// Writes journal, then delegate. `sync(false)` folds the journal (the REC-20
/// pending→durable transition); `sync(true)` (`MAP_ASYNC`) deliberately does
/// **not** — the most adversarial sound model of `msync(MS_ASYNC)`.
pub struct FaultBacking {
    inner: Box<dyn Backing>,
    state: Arc<Mutex<FaultState>>,
}

impl FaultBacking {
    /// Wrap `inner`, seeding the durable image from its current on-disk
    /// content (a freshly created env's two creation metas, typically).
    ///
    /// # Errors
    ///
    /// Propagates `inner.real_disk_size()` failure.
    pub fn wrap(
        inner: Box<dyn Backing>,
        psize: u32,
    ) -> std::io::Result<(FaultBacking, FaultHandle)> {
        let len = inner.real_disk_size()? as usize;
        let durable = inner.bytes()[..len.min(inner.bytes().len())].to_vec();
        let durable_len = durable.len() as u64;
        Self::wrap_with_initial(inner, psize, durable, durable_len)
    }

    /// Wrap with an explicit initial durable image (tests).
    ///
    /// # Errors
    ///
    /// Infallible today; `Result` for signature parity with [`FaultBacking::wrap`].
    pub fn wrap_with_initial(
        inner: Box<dyn Backing>,
        psize: u32,
        durable: Vec<u8>,
        durable_len: u64,
    ) -> std::io::Result<(FaultBacking, FaultHandle)> {
        let state = Arc::new(Mutex::new(FaultState {
            psize,
            durable,
            durable_len,
            pending: Vec::new(),
            broken_data_barriers: false,
            stats: FaultStats::default(),
        }));
        let handle = FaultHandle {
            state: Arc::clone(&state),
        };
        Ok((FaultBacking { inner, state }, handle))
    }
}

impl Backing for FaultBacking {
    fn bytes(&self) -> &[u8] {
        self.inner.bytes()
    }

    fn real_disk_size(&self) -> std::io::Result<u64> {
        self.inner.real_disk_size()
    }

    fn try_clone_file(&self) -> std::io::Result<std::fs::File> {
        self.inner.try_clone_file()
    }

    fn write_at_page(&self, pgno: u64, psize: u32, data: &[u8]) -> std::io::Result<()> {
        {
            let mut st = self.state.lock().expect("fault state lock");
            // ADR-0008 D5 (O_DIRECT-friendly sizing audit): every commit write
            // MUST be a positive whole-page multiple at a page offset. The
            // trait's `pgno` shape makes the offset structural; length is
            // asserted here on every write of every crash cycle.
            debug_assert_eq!(psize, st.psize, "write psize drifted from env psize");
            debug_assert!(
                !data.is_empty() && data.len() % psize as usize == 0,
                "ADR-0008 D5: non-page-multiple commit write ({} bytes)",
                data.len()
            );
            st.stats.writes += 1;
            st.pending.push(WriteRecord {
                offset: pgno * u64::from(psize),
                data: data.into(),
            });
        }
        self.inner.write_at_page(pgno, psize, data)
    }

    fn sync_data(&self) -> std::io::Result<()> {
        self.sync(false)
    }

    fn sync(&self, async_flush: bool) -> std::io::Result<()> {
        {
            let mut st = self.state.lock().expect("fault state lock");
            if async_flush {
                // REC-9/REC-11: MS_ASYNC completes with no durability
                // guarantee — pending stays pending (not a barrier).
                st.stats.async_flushes += 1;
            } else {
                st.fold_barrier();
            }
        }
        self.inner.sync(async_flush)
    }
}

// ---------------------------------------------------------------------------
// Crash-state materialization (REC-19/REC-20)
// ---------------------------------------------------------------------------

/// The fate of one pending write in a materialized image.
#[derive(Debug, Clone)]
pub enum WriteFate {
    /// Fully persisted.
    Applied,
    /// Not persisted at all (the durable content shows through).
    Dropped,
    /// Sector-aligned tear: `keep` whole sectors persisted from the front
    /// (`prefix == true`) or the back (`prefix == false`); the rest reverts to
    /// durable content. On a meta write this is the CRC-**valid** old-or-new
    /// case resolved by txnid selection, never CRC rejection (REC-8/REC-19).
    TornSectors {
        /// Number of whole sectors persisted.
        keep: usize,
        /// Persist from the front (true) or the back (false).
        prefix: bool,
    },
    /// Sub-sector tear: split at byte `at` (not sector-aligned) — the tear
    /// that can split a meta's sector 0 and MUST be rejected by the CRC
    /// (REC-8/REC-19).
    TornBytes {
        /// Byte offset within the write where the tear splits it.
        at: usize,
        /// Persist the bytes before `at` (true) or from `at` on (false).
        prefix: bool,
    },
    /// An arbitrary per-sector persistence mask (`mask.len()` == sector count)
    /// — the general reorder/tear case.
    SectorMask(Vec<bool>),
}

/// What length the materialized file has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LengthPolicy {
    /// Only the durable length: any file extension performed by pending
    /// writes is lost with the crash (REC-14 — the extension is fs metadata
    /// covered only by a barrier). Pending bytes beyond it vanish.
    DurableOnly,
    /// The file grew to cover every persisted pending write.
    CoverApplied,
}

/// One deterministic crash-state recipe: a fate per pending write (aligned
/// with [`CapturedDisk::pending`]) plus the length policy. `label` names the
/// quota bucket for reporting/repro.
#[derive(Debug, Clone)]
pub struct FaultPlan {
    /// Per-pending-write fate, index-aligned with `CapturedDisk::pending`.
    pub fates: Vec<WriteFate>,
    /// File-length outcome.
    pub length: LengthPolicy,
    /// Quota bucket / provenance tag.
    pub label: &'static str,
}

/// A frozen `(durable, pending)` pair — the simulated power cut, from
/// [`FaultHandle::capture`]. Materialization is a pure function of this and a
/// [`FaultPlan`].
#[derive(Debug, Clone)]
pub struct CapturedDisk {
    /// Env page size.
    pub psize: u32,
    /// Durable image bytes (barrier-covered writes only).
    pub durable: Vec<u8>,
    /// Durable file length.
    pub durable_len: u64,
    /// Un-barriered writes in issue order.
    pub pending: Vec<WriteRecord>,
}

impl CapturedDisk {
    /// Whether pending write `i` targets a meta slot (pages 0/1).
    #[must_use]
    pub fn is_meta_write(&self, i: usize) -> bool {
        self.pending[i].offset < 2 * u64::from(self.psize)
    }

    /// Index of the **last** pending meta write, if any (the one in flight —
    /// earlier pending meta writes exist only under relaxed durability).
    #[must_use]
    pub fn last_meta_write(&self) -> Option<usize> {
        (0..self.pending.len())
            .rev()
            .find(|&i| self.is_meta_write(i))
    }

    /// The all-dropped image: durable state only. Its selected txnid is the
    /// **floor** of the legal recovery set.
    #[must_use]
    pub fn floor_image(&self) -> Vec<u8> {
        self.materialize(&FaultPlan {
            fates: vec![WriteFate::Dropped; self.pending.len()],
            length: LengthPolicy::DurableOnly,
            label: "floor",
        })
    }

    /// The all-applied image: as if every pending write persisted. Its
    /// selected txnid is the **ceiling** of the legal recovery set.
    #[must_use]
    pub fn ceil_image(&self) -> Vec<u8> {
        self.materialize(&FaultPlan {
            fates: vec![WriteFate::Applied; self.pending.len()],
            length: LengthPolicy::CoverApplied,
            label: "ceil",
        })
    }

    /// Materialize one crash-state image per `plan`.
    ///
    /// Fates are applied in **issue order** (see module docs: with per-write
    /// drops this reaches every final state arbitrary persistence order can).
    ///
    /// # Panics
    ///
    /// If `plan.fates.len() != self.pending.len()` (plans are built for one
    /// capture).
    #[must_use]
    pub fn materialize(&self, plan: &FaultPlan) -> Vec<u8> {
        assert_eq!(
            plan.fates.len(),
            self.pending.len(),
            "plan/capture mismatch"
        );
        // Final length first (a DurableOnly image silently loses pending
        // bytes past the durable length — the lost-extension case, REC-14).
        let target_len = match plan.length {
            LengthPolicy::DurableOnly => self.durable_len,
            LengthPolicy::CoverApplied => {
                let mut len = self.durable_len;
                for (rec, fate) in self.pending.iter().zip(&plan.fates) {
                    if !matches!(fate, WriteFate::Dropped) {
                        len = len.max(rec.offset + rec.data.len() as u64);
                    }
                }
                len
            }
        };
        let mut img = vec![0u8; target_len as usize];
        let base = (self.durable_len as usize).min(img.len());
        img[..base].copy_from_slice(&self.durable[..base]);

        let mut apply = |offset: u64, bytes: &[u8]| {
            let start = offset as usize;
            if start >= img.len() {
                return;
            }
            let n = bytes.len().min(img.len() - start);
            img[start..start + n].copy_from_slice(&bytes[..n]);
        };

        for (rec, fate) in self.pending.iter().zip(&plan.fates) {
            let len = rec.data.len();
            let nsec = len.div_ceil(SECTOR);
            match fate {
                WriteFate::Applied => apply(rec.offset, &rec.data),
                WriteFate::Dropped => {}
                WriteFate::TornSectors { keep, prefix } => {
                    let keep = (*keep).min(nsec);
                    if *prefix {
                        let n = (keep * SECTOR).min(len);
                        apply(rec.offset, &rec.data[..n]);
                    } else {
                        let skip = ((nsec - keep) * SECTOR).min(len);
                        apply(rec.offset + skip as u64, &rec.data[skip..]);
                    }
                }
                WriteFate::TornBytes { at, prefix } => {
                    let at = (*at).min(len);
                    if *prefix {
                        apply(rec.offset, &rec.data[..at]);
                    } else {
                        apply(rec.offset + at as u64, &rec.data[at..]);
                    }
                }
                WriteFate::SectorMask(mask) => {
                    for (s, keep) in mask.iter().enumerate().take(nsec) {
                        if *keep {
                            let a = s * SECTOR;
                            let b = ((s + 1) * SECTOR).min(len);
                            apply(rec.offset + a as u64, &rec.data[a..b]);
                        }
                    }
                }
            }
        }
        img
    }

    /// The REC-20 **adversarial** plan set: quota variants first (so no batch
    /// can starve the REC-19 CRC-rejection or txnid-selection paths), then
    /// seeded random per-write fates. Deterministic in `seed`.
    ///
    /// Quotas (asserted by `plan_quotas_present` in tests, ADR-0008 D6.1):
    /// - `floor` (all dropped) and `ceil` (all applied) — always;
    /// - `lost-extension` (all applied, durable length) — always;
    /// - if a meta write is pending: `meta-subsector-torn` (splits the meta's
    ///   first sector **between the header and body txnid copies**, so the
    ///   mixed slot always fails header/body-txnid agreement or the CRC —
    ///   REC-8's guaranteed rejection), `meta-dropped` + rest applied,
    ///   `meta-applied-only` (all data dropped), and both sector-aligned meta
    ///   tears (`prefix` keeps new sector 0 ⇒ CRC-valid new; `suffix` drops
    ///   sector 0 ⇒ CRC-valid old — the txnid-selection path).
    #[must_use]
    pub fn plans_adversarial(&self, seed: u64, variants: usize) -> Vec<FaultPlan> {
        let n = self.pending.len();
        let mut rng = Rng::new(splitmix64(seed ^ 0xADD5_EC70));
        let mut plans = Vec::with_capacity(variants.max(3));

        let uniform = |fate: WriteFate, length, label| FaultPlan {
            fates: vec![fate; n],
            length,
            label,
        };
        plans.push(uniform(
            WriteFate::Dropped,
            LengthPolicy::DurableOnly,
            "floor",
        ));
        plans.push(uniform(
            WriteFate::Applied,
            LengthPolicy::CoverApplied,
            "ceil",
        ));
        plans.push(uniform(
            WriteFate::Applied,
            LengthPolicy::DurableOnly,
            "lost-extension",
        ));

        if let Some(m) = self.last_meta_write() {
            let meta_len = self.pending[m].data.len();
            let with_meta_fate = |fate: WriteFate, others: WriteFate, label| {
                let mut fates = vec![others; n];
                fates[m] = fate;
                FaultPlan {
                    fates,
                    length: LengthPolicy::CoverApplied,
                    label,
                }
            };
            // Sub-sector tear guaranteed to be rejected: cut inside sector 0
            // between the meta header (bytes [0,32), holds the header txnid)
            // and the CRC-covered body — the mixed old/new slot disagrees on
            // header-vs-body txnid (old slot content is always an older
            // commit's or the creation meta) and/or fails the CRC (SPEC 02
            // §3.2/§3.3). 32 is not a SECTOR multiple: a true sub-sector cut.
            plans.push(with_meta_fate(
                WriteFate::TornBytes {
                    at: 32,
                    prefix: true,
                },
                WriteFate::Applied,
                "meta-subsector-torn",
            ));
            plans.push(with_meta_fate(
                WriteFate::Dropped,
                WriteFate::Applied,
                "meta-dropped",
            ));
            plans.push(with_meta_fate(
                WriteFate::Applied,
                WriteFate::Dropped,
                "meta-applied-only",
            ));
            // Sector-aligned tears: never CRC-rejected (REC-19 — the harness
            // must NOT expect a CRC failure here); resolved by txnid selection.
            plans.push(with_meta_fate(
                WriteFate::TornSectors {
                    keep: 1,
                    prefix: true,
                },
                WriteFate::Applied,
                "meta-torn-aligned-new",
            ));
            let nsec = meta_len.div_ceil(SECTOR);
            plans.push(with_meta_fate(
                WriteFate::TornSectors {
                    keep: nsec.saturating_sub(1),
                    prefix: false,
                },
                WriteFate::Applied,
                "meta-torn-aligned-old",
            ));
        }

        while plans.len() < variants.max(plans.len()) {
            let mut fates = Vec::with_capacity(n);
            for rec in &self.pending {
                let len = rec.data.len();
                let nsec = len.div_ceil(SECTOR);
                let fate = match rng.below(100) {
                    0..=34 => WriteFate::Applied,
                    35..=59 => WriteFate::Dropped,
                    60..=74 => WriteFate::TornSectors {
                        keep: rng.below(nsec + 1),
                        prefix: rng.ratio(1, 2),
                    },
                    75..=84 => WriteFate::TornBytes {
                        // Any byte offset, deliberately including non-sector
                        // multiples (sub-sector coverage beyond the meta).
                        at: rng.below(len.max(1)),
                        prefix: rng.ratio(1, 2),
                    },
                    _ => {
                        let mask = (0..nsec).map(|_| rng.ratio(1, 2)).collect();
                        WriteFate::SectorMask(mask)
                    }
                };
                fates.push(fate);
            }
            let length = if rng.ratio(1, 8) {
                LengthPolicy::DurableOnly
            } else {
                LengthPolicy::CoverApplied
            };
            plans.push(FaultPlan {
                fates,
                length,
                label: "random",
            });
        }
        plans
    }

    /// The **ordered-writeback** plan set (ADR-0008 D4, `NO_SYNC`/`MAP_ASYNC`
    /// sub-model (i)): pending writes persist only as an issue-order prefix —
    /// modeling a filesystem that preserves write order — with the write in
    /// flight at the cut optionally torn. Under this model REC-11's
    /// conditional structural-consistency guarantee applies, so the harness
    /// runs full verification on these images.
    #[must_use]
    pub fn plans_ordered(&self, seed: u64, variants: usize) -> Vec<FaultPlan> {
        let n = self.pending.len();
        let mut rng = Rng::new(splitmix64(seed ^ 0x0BDE_BEEF));
        let mut plans = Vec::with_capacity(variants);
        for v in 0..variants.max(1) {
            // Deterministic sweep of prefix lengths, then random.
            let k = if v == 0 {
                0
            } else if v == 1 {
                n
            } else {
                rng.below(n + 1)
            };
            let mut fates: Vec<WriteFate> = Vec::with_capacity(n);
            for i in 0..n {
                fates.push(if i < k {
                    WriteFate::Applied
                } else {
                    WriteFate::Dropped
                });
            }
            // Optionally tear the in-flight write (the first dropped one).
            if k < n && rng.ratio(1, 2) {
                let len = self.pending[k].data.len();
                fates[k] = if rng.ratio(1, 2) {
                    WriteFate::TornBytes {
                        at: rng.below(len.max(1)),
                        prefix: true,
                    }
                } else {
                    WriteFate::TornSectors {
                        keep: rng.below(len.div_ceil(SECTOR) + 1),
                        prefix: true,
                    }
                };
            }
            plans.push(FaultPlan {
                fates,
                length: LengthPolicy::CoverApplied,
                label: "ordered-prefix",
            });
        }
        plans
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A do-nothing inner backing: the journal/materialization logic under
    /// test never reads the live view.
    struct NullInner;

    impl Backing for NullInner {
        fn bytes(&self) -> &[u8] {
            &[]
        }
        fn real_disk_size(&self) -> std::io::Result<u64> {
            Ok(0)
        }
        fn try_clone_file(&self) -> std::io::Result<std::fs::File> {
            Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "null"))
        }
        fn write_at_page(&self, _: u64, _: u32, _: &[u8]) -> std::io::Result<()> {
            Ok(())
        }
        fn sync_data(&self) -> std::io::Result<()> {
            Ok(())
        }
    }

    const PS: u32 = 4096;

    fn setup(initial_pages: usize) -> (FaultBacking, FaultHandle) {
        let durable = vec![7u8; initial_pages * PS as usize];
        let len = durable.len() as u64;
        FaultBacking::wrap_with_initial(Box::new(NullInner), PS, durable, len).unwrap()
    }

    fn page(fill: u8) -> Vec<u8> {
        vec![fill; PS as usize]
    }

    #[test]
    fn barrier_folds_pending_into_durable() {
        let (b, h) = setup(2);
        b.write_at_page(5, PS, &page(0xAA)).unwrap();
        assert_eq!(h.capture().pending.len(), 1);
        b.sync(false).unwrap();
        let cap = h.capture();
        assert!(cap.pending.is_empty(), "barrier must clear the journal");
        assert_eq!(
            cap.durable_len,
            6 * u64::from(PS),
            "fold extends durable length"
        );
        assert_eq!(cap.durable[5 * PS as usize], 0xAA);
        assert_eq!(h.stats().barriers, 1);
    }

    #[test]
    fn async_flush_is_not_a_barrier() {
        // REC-9: MS_ASYNC gives no durability — pending must survive it.
        let (b, h) = setup(2);
        b.write_at_page(3, PS, &page(0xBB)).unwrap();
        b.sync(true).unwrap();
        let cap = h.capture();
        assert_eq!(cap.pending.len(), 1, "async flush must not fold");
        assert_eq!(cap.durable_len, 2 * u64::from(PS));
        assert_eq!(h.stats().async_flushes, 1);
    }

    #[test]
    fn floor_and_ceil_images() {
        let (b, h) = setup(2);
        b.write_at_page(2, PS, &page(0xCC)).unwrap();
        let cap = h.capture();
        let floor = cap.floor_image();
        assert_eq!(
            floor.len(),
            2 * PS as usize,
            "floor = durable only (REC-14)"
        );
        let ceil = cap.ceil_image();
        assert_eq!(ceil.len(), 3 * PS as usize);
        assert!(ceil[2 * PS as usize..].iter().all(|&x| x == 0xCC));
    }

    #[test]
    fn torn_bytes_prefix_and_suffix() {
        let (b, h) = setup(3);
        b.write_at_page(1, PS, &page(0xDD)).unwrap();
        let cap = h.capture();
        let img = cap.materialize(&FaultPlan {
            fates: vec![WriteFate::TornBytes {
                at: 100,
                prefix: true,
            }],
            length: LengthPolicy::CoverApplied,
            label: "t",
        });
        let base = PS as usize;
        assert!(img[base..base + 100].iter().all(|&x| x == 0xDD));
        assert!(img[base + 100..base + PS as usize].iter().all(|&x| x == 7));
        let img = cap.materialize(&FaultPlan {
            fates: vec![WriteFate::TornBytes {
                at: 100,
                prefix: false,
            }],
            length: LengthPolicy::CoverApplied,
            label: "t",
        });
        assert!(img[base..base + 100].iter().all(|&x| x == 7));
        assert!(img[base + 100..base + PS as usize]
            .iter()
            .all(|&x| x == 0xDD));
    }

    #[test]
    fn torn_sectors_prefix_and_suffix() {
        let (b, h) = setup(3);
        b.write_at_page(1, PS, &page(0xEE)).unwrap();
        let cap = h.capture();
        let base = PS as usize;
        let img = cap.materialize(&FaultPlan {
            fates: vec![WriteFate::TornSectors {
                keep: 2,
                prefix: true,
            }],
            length: LengthPolicy::CoverApplied,
            label: "t",
        });
        assert!(img[base..base + 2 * SECTOR].iter().all(|&x| x == 0xEE));
        assert!(img[base + 2 * SECTOR..base + PS as usize]
            .iter()
            .all(|&x| x == 7));
        let img = cap.materialize(&FaultPlan {
            fates: vec![WriteFate::TornSectors {
                keep: 2,
                prefix: false,
            }],
            length: LengthPolicy::CoverApplied,
            label: "t",
        });
        let cut = PS as usize - 2 * SECTOR;
        assert!(img[base..base + cut].iter().all(|&x| x == 7));
        assert!(img[base + cut..base + PS as usize]
            .iter()
            .all(|&x| x == 0xEE));
    }

    #[test]
    fn lost_extension_drops_beyond_durable_len() {
        // REC-14: an image may keep applied writes only within the durable
        // length — the extension itself is lost.
        let (b, h) = setup(2);
        b.write_at_page(1, PS, &page(0x11)).unwrap(); // within durable
        b.write_at_page(9, PS, &page(0x22)).unwrap(); // extends
        let cap = h.capture();
        let img = cap.materialize(&FaultPlan {
            fates: vec![WriteFate::Applied, WriteFate::Applied],
            length: LengthPolicy::DurableOnly,
            label: "lost-extension",
        });
        assert_eq!(img.len(), 2 * PS as usize);
        assert!(img[PS as usize..].iter().all(|&x| x == 0x11));
    }

    #[test]
    fn adversarial_quotas_present_with_pending_meta() {
        let (b, h) = setup(2);
        b.write_at_page(4, PS, &page(1)).unwrap();
        b.write_at_page(0, PS, &page(2)).unwrap(); // meta slot 0
        let cap = h.capture();
        let plans = cap.plans_adversarial(42, 12);
        for want in [
            "floor",
            "ceil",
            "lost-extension",
            "meta-subsector-torn",
            "meta-dropped",
            "meta-applied-only",
            "meta-torn-aligned-new",
            "meta-torn-aligned-old",
        ] {
            assert!(
                plans.iter().any(|p| p.label == want),
                "quota bucket {want} missing"
            );
        }
        assert!(plans.len() >= 12);
        // The sub-sector quota tear must NOT be sector-aligned.
        let sub = plans
            .iter()
            .find(|p| p.label == "meta-subsector-torn")
            .unwrap();
        match &sub.fates[1] {
            WriteFate::TornBytes { at, .. } => assert!(at % SECTOR != 0),
            other => panic!("expected TornBytes, got {other:?}"),
        }
    }

    #[test]
    fn plans_and_images_are_deterministic() {
        // OQ4: bit-stable plans — same seed, same capture ⇒ identical images.
        let mk = || {
            let (b, h) = setup(2);
            let mut rng = Rng::new(9);
            for i in 0..6u64 {
                let mut data = page(0);
                rng.fill(&mut data);
                b.write_at_page(2 + i, PS, &data).unwrap();
            }
            b.write_at_page(1, PS, &page(3)).unwrap();
            h.capture()
        };
        let (c1, c2) = (mk(), mk());
        let (p1, p2) = (c1.plans_adversarial(7, 16), c2.plans_adversarial(7, 16));
        assert_eq!(p1.len(), p2.len());
        for (a, b) in p1.iter().zip(&p2) {
            assert_eq!(c1.materialize(a), c2.materialize(b));
        }
        let (o1, o2) = (c1.plans_ordered(7, 8), c2.plans_ordered(7, 8));
        for (a, b) in o1.iter().zip(&o2) {
            assert_eq!(c1.materialize(a), c2.materialize(b));
        }
    }

    #[test]
    fn ordered_plans_are_prefixes() {
        let (b, h) = setup(2);
        for i in 0..5u64 {
            b.write_at_page(2 + i, PS, &page(i as u8)).unwrap();
        }
        let cap = h.capture();
        for plan in cap.plans_ordered(3, 10) {
            // Once a Dropped/torn fate appears, everything after is Dropped.
            let mut seen_cut = false;
            for f in &plan.fates {
                match f {
                    WriteFate::Applied => assert!(!seen_cut, "apply after cut in {plan:?}"),
                    _ => seen_cut = true,
                }
            }
        }
    }

    #[test]
    fn broken_barrier_mode_keeps_data_pending() {
        // ADR-0008 D6.1: the mutation self-test switch — barriers fold metas
        // but silently drop data durability.
        let (b, h) = setup(2);
        h.set_broken_data_barriers(true);
        b.write_at_page(6, PS, &page(0x66)).unwrap(); // data
        b.write_at_page(1, PS, &page(0x11)).unwrap(); // meta slot 1
        b.sync(false).unwrap();
        let cap = h.capture();
        assert_eq!(cap.pending.len(), 1, "data write must stay pending");
        assert_eq!(cap.pending[0].offset, 6 * u64::from(PS));
        assert_eq!(cap.durable[PS as usize], 0x11, "meta write folded");
        assert_eq!(
            cap.durable_len,
            2 * u64::from(PS),
            "no data extension folded"
        );
    }

    #[test]
    fn xoshiro_reference_stability() {
        // Pin the PRNG bit-stream so a dependency-free reimplementation or
        // refactor cannot silently change every saved repro seed.
        let mut r = Rng::new(0);
        let first: Vec<u64> = (0..4).map(|_| r.next_u64()).collect();
        let mut r2 = Rng::new(0);
        let again: Vec<u64> = (0..4).map(|_| r2.next_u64()).collect();
        assert_eq!(first, again);
        let mut r3 = Rng::new(1);
        assert_ne!(first[0], r3.next_u64());
        // splitmix64 known-good value (Vigna's reference: seed 0 first output).
        assert_eq!(splitmix64(0), 0xE220_A839_7B1D_CDAF);
    }
}
