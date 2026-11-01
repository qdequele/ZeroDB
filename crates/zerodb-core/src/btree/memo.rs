//! The per-transaction validated-pages memo ([`ValidatedPages`]): which
//! map pages this transaction already validated, so a revisit skips the cell
//! walk.

use super::*;

/// Geometric growth levels of the memo (level `i` holds
/// `MEMO_BASE_SLOTS << (2 * i)` slots): 7 levels ≈ 5.6 M slots ≈ 2.8 M
/// memoized pages at the ½ load-factor gate — a ~45 GB env of 16 K pages
/// touched by ONE txn before the memo saturates and degrades to plain
/// revalidation (correct, just slower).
const MEMO_LEVELS: usize = 7;
/// Slots in level 0 (8 KiB) — sized so short txns allocate once and small.
const MEMO_BASE_SLOTS: usize = 1024;
/// Level-0 inserts whose slot index the memo logs, so that dropping it can
/// clear exactly those slots and hand the zeroed level to the next memo built
/// on the same thread (see [`ValidatedPages`]'s `Drop`). A point read on a
/// tree of a few million keys records about four pages; 32 covers a short
/// transaction of several reads. A memo with more level-0 entries frees its
/// level instead. The log lives in level 0's allocation, after its
/// [`MEMO_BASE_SLOTS`] table slots, so a memo that never reads costs nothing
/// extra to build.
const MEMO_LOG_SLOTS: usize = 32;

/// Table slots of level `li` (its allocation may carry more: level 0's log).
const fn level_slots(li: usize) -> usize {
    MEMO_BASE_SLOTS << (2 * li)
}

thread_local! {
    /// One all-zero level 0, left by the last memo dropped on this thread,
    /// so a short read transaction does not allocate, zero and free 8 KiB.
    /// LMDB avoids per-request setup by letting the caller keep a
    /// transaction and re-arm it
    /// (`mdb_txn_reset` / `mdb_txn_renew`); heed 0.22.1 does not expose that,
    /// so the reuse happens here instead. The level holds no entry when it is
    /// parked (dropping the memo zeroes every slot it wrote), so a new
    /// transaction starts with an empty memo, exactly as with a fresh
    /// allocation. One level per thread bounds the parked memory to 8 KiB per
    /// thread that ever dropped a memo.
    static SPARE_BASE: Cell<Option<Box<[AtomicU64]>>> = const { Cell::new(None) };
}

/// The zeroed allocation of level `li`: the parked level 0 when one is
/// there, else a fresh one. Level 0 carries its insert log after the table.
fn new_level(li: usize) -> Box<[AtomicU64]> {
    let mut len = level_slots(li);
    if li == 0 {
        // `try_with` fails only while the thread's locals are being torn
        // down; allocate then.
        if let Some(level) = SPARE_BASE.try_with(Cell::take).ok().flatten() {
            return level;
        }
        len += MEMO_LOG_SLOTS;
    }
    (0..len)
        .map(|_| AtomicU64::new(0))
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

/// Which validated shape a memo entry vouches for (kind-tagged memo). The kind is
/// part of the memo **key**, so a hit can hand out a fully *trusted* typed
/// view (`new_trusted`, zero checks) while a pgno validated as one kind can
/// never be trusted as the other — the mismatched lookup simply misses and
/// revalidates, failing loudly on the type check.
#[derive(Clone, Copy)]
pub(crate) enum PageKind {
    Leaf,
    Branch,
}

/// Txn-scoped memo of pages whose **cells** have already passed full
/// validation this txn (docs/PERFORMANCE.md, eager page validation).
///
/// `LeafRef::new`/`BranchRef::new` validate every cell — O(`num_keys`) per
/// view construction — which multiplied every descent (get, seek, put
/// search). A page recorded here is re-wrapped via the zero-check
/// `new_trusted` constructors (the memo key carries the page kind).
///
/// Soundness: entries are only recorded for **map-sourced** bytes
/// (`Source::bytes_from_classified`), which are immutable for the owning
/// txn's life; dirty frames never enter the memo.
///
/// Concurrency: milli shares one `RoTxn` across rayon workers, so the probe
/// must not lock (a `Mutex<HashSet>` probe was the second-hottest zerodb
/// frame in the milli indexing profile). Insert-only open addressing over
/// `AtomicU64` slots (`0` = empty, else `key`), in geometrically growing levels published
/// through `OnceLock` — levels are never moved or rehashed, `contains` probes
/// every initialized level, and nothing here can affect correctness: any
/// degradation (stale level counter, saturated last level, racing duplicate
/// insert) is at worst a miss, which revalidates.
pub struct ValidatedPages<'e> {
    levels: [OnceLock<Box<[AtomicU64]>>; MEMO_LEVELS],
    /// Highest level inserts currently target.
    cur: AtomicUsize,
    /// Per-level fill counts: the ½ load-factor gate, and at drop the number
    /// of level-0 slots written (each successful insert counts once). Level
    /// 0's allocation ends with the slot index of its first
    /// [`MEMO_LOG_SLOTS`] inserts, in insert order, read only by `Drop`.
    counts: [AtomicUsize; MEMO_LEVELS],
    /// The env's [`FileTrust`] policy, copied in at txn begin (ADR-0014).
    /// When set, a memo miss on a map page takes the header-checked
    /// `new_prevalidated` view, as dirty frames do, and records the page.
    trust_file: bool,
    /// The env-wide cache of validated page versions (ADR-0018), probed on
    /// a validating memo miss for a map page. Borrowed from the env by a
    /// plain read txn, so opening one touches no shared refcount. `None` for
    /// env-owning `static_read_txn`s and under a trusting policy. Write txns
    /// carry it too (ADR-0018 publish-at-commit): the committer seeds the
    /// cache, so a writer's probe hits the path the previous commit
    /// rewrote. Kept inside the memo so the tree code passes one pointer.
    pub(super) shared: Option<&'e StampCache>,
    /// Whether a validating miss may publish its result into `shared`
    /// (ADR-0018). True for plain read txns; false for
    /// write txns (and the nested read txns sharing their memo): a writer's
    /// miss-arm publish could record a non-final image of its own spilled
    /// page (rewritten in place under the same stamp, SPEC 04 TXN-69/72), and
    /// an aborted writer's publishes would outlive the abort into its reused
    /// txnid (TXN-2). Read only in the out-of-line miss arms.
    pub(super) publish_shared: bool,
}

impl ValidatedPages<'static> {
    #[cfg(test)]
    pub(crate) fn new() -> ValidatedPages<'static> {
        ValidatedPages::for_policy(FileTrust::VALIDATE)
    }

    /// A memo for a txn of an env opened under `policy` (ADR-0014), with no
    /// env-wide cache (write txns, tools, tests).
    pub(crate) fn for_policy(policy: FileTrust) -> ValidatedPages<'static> {
        ValidatedPages::for_reader(policy, None)
    }
}

impl<'e> ValidatedPages<'e> {
    /// A read txn's memo: `policy` (ADR-0014) and the env-wide cache
    /// (ADR-0018). Under a trusting policy the cache is dropped: the trusted
    /// memo-miss arm never probes it.
    pub(crate) fn for_reader(
        policy: FileTrust,
        shared: Option<&'e StampCache>,
    ) -> ValidatedPages<'e> {
        ValidatedPages::build(policy, shared, true)
    }

    /// A write txn's memo (ADR-0018): probes the
    /// env-wide cache — the committer seeds it, so the path the previous
    /// commit rewrote hits — but never publishes from the miss arm (see
    /// `publish_shared`). Under a trusting policy the cache is dropped.
    pub(crate) fn for_writer(
        policy: FileTrust,
        shared: Option<&'e StampCache>,
    ) -> ValidatedPages<'e> {
        ValidatedPages::build(policy, shared, false)
    }

    fn build(
        policy: FileTrust,
        shared: Option<&'e StampCache>,
        publish_shared: bool,
    ) -> ValidatedPages<'e> {
        ValidatedPages {
            levels: std::array::from_fn(|_| OnceLock::new()),
            cur: AtomicUsize::new(0),
            counts: std::array::from_fn(|_| AtomicUsize::new(0)),
            trust_file: policy.is_trusted(),
            shared: if policy.is_trusted() { None } else { shared },
            publish_shared,
        }
    }

    /// `true` when the env was opened with [`FileTrust::trust_contents`]:
    /// a memo miss on a map page then takes the header-checked view instead
    /// of the cell walk (ADR-0014).
    pub(super) fn trusts_file(&self) -> bool {
        self.trust_file
    }

    /// One slot's stored key: `(pgno | kind_tag) + 1`, so `0` stays "empty".
    /// Bit 63 tags the kind; pgnos are bounded far below that
    /// (`map_size / page_size`), and the `+1` cannot wrap.
    fn key_of(pgno: u64, kind: PageKind) -> u64 {
        let tag = match kind {
            PageKind::Leaf => 0,
            PageKind::Branch => 1u64 << 63,
        };
        (pgno | tag) + 1
    }

    /// splitmix64 finalizer — cheap, well-mixed slot index (SipHash was
    /// measurable in the milli profile).
    fn mix(mut z: u64) -> u64 {
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    #[inline(always)]
    pub(super) fn contains(&self, pgno: u64, kind: PageKind) -> bool {
        let key = Self::key_of(pgno, kind);
        // Ordering: `Acquire` here pairs with the `Release` slot publication
        // in `insert`, so a hit happens-after the completed validation that
        // program-order preceded that insert (required on ARM's weak model;
        // the page bytes themselves are immutable and were readable before).
        // A stale `cur` or slot read is only ever a miss → revalidation.
        let top = self.cur.load(Ordering::Acquire).min(MEMO_LEVELS - 1);
        for (li, level) in self.levels.iter().enumerate().take(top + 1) {
            let Some(lvl) = level.get() else { continue };
            let lvl = &lvl[..level_slots(li)];
            let mask = lvl.len() - 1;
            let mut i = (Self::mix(key) as usize) & mask;
            // Bounded probe: the ½ load-factor gate guarantees empty slots,
            // so the first `0` ends this level; the bound is belt-and-braces.
            for _ in 0..lvl.len() {
                match lvl[i].load(Ordering::Acquire) {
                    0 => break,
                    s if s == key => return true,
                    _ => i = (i + 1) & mask,
                }
            }
        }
        false
    }

    pub(super) fn insert(&self, pgno: u64, kind: PageKind) {
        let key = Self::key_of(pgno, kind);
        loop {
            // Ordering: `Acquire` on `cur` sees the latest published level
            // index; `get_or_init` does its own synchronization for the
            // allocation itself.
            let li = self.cur.load(Ordering::Acquire).min(MEMO_LEVELS - 1);
            let alloc = self.levels[li].get_or_init(|| new_level(li));
            let (lvl, log) = alloc.split_at(level_slots(li));
            // ½ load-factor gate: keeps probes short and guarantees empty
            // slots so `contains` terminates on the first `0`.
            if self.counts[li].load(Ordering::Relaxed) * 2 >= lvl.len() {
                if li + 1 < MEMO_LEVELS {
                    // Ordering: `AcqRel` — the winning bump publishes the new
                    // level index; losers reload and retry.
                    let _ =
                        self.cur
                            .compare_exchange(li, li + 1, Ordering::AcqRel, Ordering::Acquire);
                    continue;
                }
                return; // saturated: degrade to revalidation, stay correct
            }
            let mask = lvl.len() - 1;
            let mut i = (Self::mix(key) as usize) & mask;
            for _ in 0..lvl.len() {
                // Ordering: `Release` on success publishes "this page's full
                // validation completed" (all validation reads are
                // program-ordered before this CAS) to `contains`'s `Acquire`
                // loads; `Acquire` on failure so an observed equal key is a
                // completed publication by a racing thread.
                match lvl[i].compare_exchange(0, key, Ordering::Release, Ordering::Acquire) {
                    Ok(_) => {
                        // `Relaxed` suffices: no data is published through
                        // the count or the log. Only `Drop` reads them as
                        // exact, under `&mut self`, and whatever handed it
                        // that exclusive access (a thread join, the last
                        // `Arc` drop) orders every insert before it.
                        let n = self.counts[li].fetch_add(1, Ordering::Relaxed);
                        if let Some(entry) = log.get(n) {
                            // Only level 0 has a log (`log` is empty above it).
                            entry.store(i as u64, Ordering::Relaxed);
                        }
                        return;
                    }
                    Err(s) if s == key => return,
                    Err(_) => i = (i + 1) & mask,
                }
            }
            // Level filled under racing inserts before the gate caught it:
            // advance (or saturate) and retry.
            if li + 1 < MEMO_LEVELS {
                let _ = self
                    .cur
                    .compare_exchange(li, li + 1, Ordering::AcqRel, Ordering::Acquire);
            } else {
                return;
            }
        }
    }
}

impl Drop for ValidatedPages<'_> {
    /// Clear level 0 slot by slot and park it for the next memo on this
    /// thread, when every slot written is logged; otherwise free it.
    ///
    /// Soundness: a memo entry vouches for a page only within one snapshot.
    /// It is keyed by page number and kind, not by the page's stamp, so an
    /// entry carried into a later transaction could vouch for a page that
    /// was freed and rewritten since. The parked level therefore holds no
    /// entry: `counts[0]` is the exact number of level-0 slots written (one
    /// count per successful insert), and when it is at most
    /// `MEMO_LOG_SLOTS` every one of those slots is in the level's log.
    fn drop(&mut self) {
        let Some(mut base) = self.levels[0].take() else {
            return;
        };
        let written = *self.counts[0].get_mut();
        if written > MEMO_LOG_SLOTS {
            return;
        }
        let (table, log) = base.split_at_mut(level_slots(0));
        for logged in &mut log[..written] {
            let slot = std::mem::take(logged.get_mut());
            // A logged index is below `level_slots(0)`: it was a table index.
            *table[usize::try_from(slot).expect("logged slot index")].get_mut() = 0;
        }
        debug_assert!(
            base.iter_mut().all(|slot| *slot.get_mut() == 0),
            "a parked memo level must hold no entry"
        );
        // Replaces (and frees) a level another memo on this thread parked
        // first. Fails only during thread-local teardown, which frees `base`.
        let _ = SPARE_BASE.try_with(|spare| spare.set(Some(base)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Take this thread's parked memo level, if any.
    fn take_spare() -> Option<Box<[AtomicU64]>> {
        SPARE_BASE.with(Cell::take)
    }

    /// A dropped memo parks its level 0 for the next memo on the thread with
    /// every entry cleared: a later transaction must not inherit a page this
    /// one validated (memo entries are keyed by page number, not stamp, so
    /// the page may have been rewritten since). The next memo reuses the
    /// parked level and starts empty.
    #[test]
    fn dropped_memo_parks_an_empty_level_for_the_next() {
        drop(take_spare());
        let first = ValidatedPages::new();
        for pgno in 1..=8 {
            first.insert(pgno, PageKind::Leaf);
            first.insert(pgno + 100, PageKind::Branch);
        }
        assert!(first.contains(3, PageKind::Leaf));
        let level = first.levels[0].get().expect("level 0 allocated").as_ptr();
        drop(first);

        let second = ValidatedPages::new();
        for pgno in 1..=8 {
            assert!(!second.contains(pgno, PageKind::Leaf));
            assert!(!second.contains(pgno + 100, PageKind::Branch));
        }
        second.insert(42, PageKind::Leaf);
        let reused = second.levels[0].get().expect("level 0 allocated");
        assert_eq!(reused.as_ptr(), level, "the parked level is reused");
        assert!(second.contains(42, PageKind::Leaf));
        assert!(!second.contains(3, PageKind::Leaf), "no entry carried over");
        assert_eq!(
            reused[..level_slots(0)]
                .iter()
                .filter(|s| s.load(Ordering::Relaxed) != 0)
                .count(),
            1,
            "only the new transaction's entry is in the level"
        );
    }

    /// A memo that wrote more level-0 slots than it logs frees its level
    /// rather than parking a level it cannot clear slot by slot; a memo that
    /// never allocated parks nothing.
    #[test]
    fn memo_past_the_log_frees_its_level() {
        drop(take_spare());
        let big = ValidatedPages::new();
        for pgno in 0..=MEMO_LOG_SLOTS as u64 {
            big.insert(pgno, PageKind::Leaf);
        }
        drop(big);
        assert!(take_spare().is_none(), "an over-full level is freed");

        drop(ValidatedPages::new());
        assert!(take_spare().is_none(), "an unused memo parks nothing");
    }

    /// Inserts racing from several threads are all logged, so the memo's
    /// drop still parks a level with no entry left in it.
    #[test]
    fn racing_inserts_are_all_cleared_at_drop() {
        drop(take_spare());
        let memo = ValidatedPages::new();
        std::thread::scope(|s| {
            for t in 0..4u64 {
                let memo = &memo;
                s.spawn(move || {
                    for pgno in 0..6 {
                        // Threads 0/1 and 2/3 insert the same keys: racing
                        // duplicates must count once.
                        memo.insert((t / 2) * 1000 + pgno, PageKind::Leaf);
                    }
                });
            }
        });
        assert_eq!(memo.counts[0].load(Ordering::Relaxed), 12);
        drop(memo);
        let parked = take_spare().expect("a level within the log is parked");
        assert!(parked.iter().all(|s| s.load(Ordering::Relaxed) == 0));
    }

    /// The lock-free memo under concurrent insert/contains.
    /// 4 threads × 2,000 keys with heavy overlap (every key inserted by two
    /// threads, both kinds) force level growth (level 0 holds 512 at the ½
    /// gate), CAS races on duplicate keys, and probes racing publications.
    /// Afterwards every inserted key must be a hit under its own kind and a
    /// miss under the other (bit-63 tag). Runs natively and under miri
    /// (miri's weak-memory machinery checks the Acquire/Release pairs); loom
    /// is deliberately not wired: the memo's contract is advisory (any race
    /// outcome is at worst a miss → revalidation), unlike the reader table's.
    #[test]
    fn validated_pages_concurrent_insert_contains() {
        let vp = ValidatedPages::new();
        let n_threads = 4usize;
        // Full size natively (forces two level-growths); small under miri —
        // its weak-memory simulation is ~1000× slower and the orderings it
        // checks are the same at any size. Level growth still triggers with
        // 640 distinct keys > level 0's 512-insert gate.
        #[cfg(not(miri))]
        let per_thread = 2_000u64;
        #[cfg(miri)]
        let per_thread = 320u64;
        std::thread::scope(|s| {
            for t in 0..n_threads {
                let vp = &vp;
                s.spawn(move || {
                    for i in 0..per_thread {
                        // Overlap: thread t and thread (t+1)%4 share keys.
                        let pgno = (t as u64 % 2) * 10_000 + i;
                        let kind = if i % 2 == 0 {
                            PageKind::Leaf
                        } else {
                            PageKind::Branch
                        };
                        vp.insert(pgno, kind);
                        assert!(vp.contains(pgno, kind), "own insert must hit");
                    }
                });
            }
        });
        for pgno in 0..per_thread {
            let (own, other) = if pgno % 2 == 0 {
                (PageKind::Leaf, PageKind::Branch)
            } else {
                (PageKind::Branch, PageKind::Leaf)
            };
            assert!(vp.contains(pgno, own));
            assert!(vp.contains(10_000 + pgno, own));
            assert!(!vp.contains(pgno, other), "kind tag must separate");
            assert!(!vp.contains(10_000 + pgno, other));
        }
        assert!(!vp.contains(999_999, PageKind::Leaf));
    }
}
