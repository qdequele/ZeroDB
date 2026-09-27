//! Custom key comparators (**milestone 2.4**; SPEC 03 §2.0, D-004).
//!
//! LMDB exposes `mdb_set_compare(txn, dbi, MDB_cmp_func*)` — a raw C function
//! pointer installed per-dbi, per-open. heed hides it completely: its
//! `Comparator` trait exists but every `Database` is `DefaultComparator`, and
//! nothing in heed's open path ever reaches `mdb_set_compare`. Phase 1
//! therefore assumed unsigned lexicographic byte order everywhere (SPEC 00
//! row 53, SPEC 03 §2 invariant BT-1). This module lifts that assumption for
//! **named** databases, as a safe Rust trait rather than a function pointer.
//!
//! ## What is and is not comparator-aware
//!
//! | Tree | Comparator | Why |
//! |---|---|---|
//! | A named DB | caller's, or memcmp | the whole point of this milestone |
//! | The main / unnamed DB | **always memcmp** | it doubles as the named-DB catalog (SPEC 02 §6). Catalog keys are DB *names* and catalog values are engine-internal `DBRecord` bytes; re-ordering them under caller-supplied logic would put engine metadata at the mercy of user code, and a panicking or inconsistent comparator would corrupt the catalog rather than one database. See [`ComparatorError::MainDatabase`]. |
//! | The GC / free tree | **always memcmp** | keys are big-endian txnids (SPEC 05); memcmp order *is* numeric order, and the GC gate depends on it. Not reachable from the public API at all. |
//!
//! ## The comparator is NOT persisted — read this
//!
//! Neither LMDB nor ZeroDB stores the comparator in the file. A B+tree is only
//! meaningful under the ordering that built it, so **reopening a database with
//! a different comparator than the one that wrote it silently produces wrong
//! answers**: lookups miss keys that are present, range scans return the wrong
//! subsets, and subsequent writes interleave two incompatible orderings into
//! one tree, permanently corrupting it. Nothing detects this at open. It is a
//! *caller* obligation, exactly as it is under `mdb_set_compare`.
//!
//! ZeroDB does what it can within the format it has:
//!
//! - **In-process mismatch is detected.** A comparator is registered once per
//!   dbi in [`ComparatorRegistry`]; a second registration for the same
//!   database with a different [`Comparator::name`] is refused
//!   ([`ComparatorError::Mismatch`]) instead of silently taking one of them.
//!   This catches the common real bug — two modules in one process disagreeing
//!   about a DB — but says nothing about the previous *run*.
//! - **Cross-open mismatch is NOT detected, and cannot be without a format
//!   change.** The natural fix is a comparator-identity fingerprint in the
//!   per-DB record, checked at open. There is nowhere to put one: `DBRecord`
//!   is exactly 48 bytes with every byte assigned (SPEC 02 §3.1 — `root`,
//!   4 × page/entry counters, `depth`, `flags`, `leaf2_ksize`), and its only
//!   two unassigned *values* (`flags` at offset 42, `leaf2_ksize` at offset
//!   44) are already earmarked for DUPSORT/DUPFIXED in milestone 2.8.
//!   Widening the record or repurposing those fields is an on-disk **format**
//!   decision, which CLAUDE.md rule 6 puts behind an ADR and human approval —
//!   deliberately not taken here. The hazard is therefore **documented, not
//!   mitigated**, and is filed as D-014.
//!
//! ## Requirements on an implementation
//!
//! A [`Comparator`] must be a **total order**: antisymmetric, transitive,
//! total, and *deterministic* — the same pair of byte strings must always
//! compare the same way, for the life of the data, across processes and
//! releases. Violating this does not trigger Rust unsafety (nothing here is
//! `unsafe`; the crate is `#![deny(unsafe_code)]` outside `page::raw`), but it does corrupt
//! the tree in the ordinary sense: a B+tree built under an inconsistent
//! ordering has no correct search path, and `zerodb-tools check` will report
//! INV-5/INV-6 violations.
//!
//! A comparator that **panics** unwinds through the engine. Inside a write
//! transaction that is safe in the RAII sense — the `RwTxn` is dropped and its
//! dirty pages discarded, so the durable file is untouched (SPEC 04 §9: the
//! file changes only at commit) — but the operation is lost. Do not panic.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// A total order over raw key bytes (**milestone 2.4**) — ZeroDB's safe
/// replacement for LMDB's `MDB_cmp_func` C function pointer.
///
/// Implementations must be a deterministic total order; see the [module
/// docs](self) for the full contract and the non-persistence hazard.
///
/// `Send + Sync` because one registration is shared by every transaction on
/// the environment, on any thread (read txns are `Send`, SPEC 00 row 29), and
/// `'static` because it outlives every transaction that uses it.
pub trait Comparator: Send + Sync + 'static {
    /// Compare two keys. Must be a deterministic total order.
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;

    /// A stable identity for this ordering, used to detect *in-process*
    /// disagreement about a database ([`ComparatorError::Mismatch`]).
    ///
    /// Two registrations with the same name are assumed to be the same
    /// ordering, so the name must change whenever the ordering does. Pick
    /// something specific and versioned (`"myapp.facet-key.v2"`), not
    /// `"custom"`. It is **not** written to disk — it cannot rescue a
    /// cross-open mismatch (module docs).
    fn name(&self) -> &str;
}

/// The default ordering: unsigned lexicographic byte comparison, i.e. LMDB's
/// `mdb_cmp_memn` and heed's `DefaultComparator` (SPEC 00 row 53).
///
/// Every database uses this unless a custom comparator is registered, and the
/// main/unnamed DB and the GC tree use it unconditionally.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DefaultComparator;

impl Comparator for DefaultComparator {
    #[inline]
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    fn name(&self) -> &str {
        "memcmp"
    }
}

/// A [`Comparator`] built from a plain closure — the "safe Rust closure" form
/// of `mdb_set_compare`.
///
/// ```
/// use std::cmp::Ordering;
/// use zerodb_core::cmp::{Comparator, FnComparator};
///
/// // Descending byte order.
/// let rev = FnComparator::new("example.reverse.v1", |a: &[u8], b: &[u8]| b.cmp(a));
/// assert_eq!(rev.compare(b"a", b"b"), Ordering::Greater);
/// ```
pub struct FnComparator<F> {
    name: &'static str,
    f: F,
}

impl<F> FnComparator<F>
where
    F: Fn(&[u8], &[u8]) -> Ordering + Send + Sync + 'static,
{
    /// Wrap `f` under the stable identity `name` (see [`Comparator::name`]).
    pub fn new(name: &'static str, f: F) -> FnComparator<F> {
        FnComparator { name, f }
    }
}

impl<F> Comparator for FnComparator<F>
where
    F: Fn(&[u8], &[u8]) -> Ordering + Send + Sync + 'static,
{
    #[inline]
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        (self.f)(a, b)
    }

    fn name(&self) -> &str {
        self.name
    }
}

/// The ordering in force for one tree, in a form cheap enough to sit inside a
/// `Copy` [`crate::btree::Tree`] and be passed down every descent.
///
/// [`KeyCmp::Default`] is a distinct variant rather than
/// `Custom(&DefaultComparator)` so the overwhelmingly common path stays a
/// predictable branch onto an inlined `slice::cmp`, with no vtable call —
/// this type is on the hot path of every `get`, `put`, and cursor step.
#[derive(Clone, Copy)]
pub enum KeyCmp<'a> {
    /// Unsigned lexicographic byte order ([`DefaultComparator`]).
    Default,
    /// A caller-registered comparator, borrowed from the environment's
    /// [`ComparatorRegistry`] (which owns it for the env's whole life).
    Custom(&'a dyn Comparator),
}

/// Unsigned lexicographic byte comparison (the [`KeyCmp::Default`] ordering),
/// with a fast path for equal-length 8- and 4-byte keys.
///
/// Meilisearch and hannoy key the hot trees on fixed-width big-endian integers
/// (u32/u64 document and item ids), so almost every binary-search probe compares
/// two slices of the same length 8 or 4. For equal-length slices, comparing them
/// as big-endian integers is *identical* to memcmp order (the most significant
/// differing byte decides both), so this returns exactly `a.cmp(b)` — but as one
/// integer load and compare each rather than a `memcmp` call (roadmap 7e; the
/// aarch64 hot path showed `bl _memcmp` on every probe, PERF-GAP B13). Every
/// other length pair, including 8-vs-4, falls through to the slice comparison,
/// which never reads past either slice's length.
#[inline]
fn default_cmp_fast(a: &[u8], b: &[u8]) -> Ordering {
    match (a.len(), b.len()) {
        // The `try_into` cannot fail — the arm guard fixes each length — and the
        // fallible branch optimizes out; no bytes outside `a`/`b` are read.
        (8, 8) => u64::from_be_bytes(a.try_into().unwrap())
            .cmp(&u64::from_be_bytes(b.try_into().unwrap())),
        (4, 4) => u32::from_be_bytes(a.try_into().unwrap())
            .cmp(&u32::from_be_bytes(b.try_into().unwrap())),
        _ => a.cmp(b),
    }
}

impl KeyCmp<'_> {
    /// Compare two keys under this ordering.
    #[inline]
    #[must_use]
    pub fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        match self {
            KeyCmp::Default => default_cmp_fast(a, b),
            KeyCmp::Custom(c) => c.compare(a, b),
        }
    }

    /// Whether two keys are the *same key* under this ordering.
    ///
    /// Deliberately not byte equality: a comparator may consider distinct byte
    /// strings equal (a case-folding order, say), and the tree stores at most
    /// one of any comparator-equal group. Every "did I land on the key I asked
    /// for?" test in the engine goes through here — using `==` instead is the
    /// classic way a comparator gets applied to descent but not to the final
    /// hit test.
    #[inline]
    #[must_use]
    pub fn eq(&self, a: &[u8], b: &[u8]) -> bool {
        self.compare(a, b) == Ordering::Equal
    }

    /// Whether this is the default (memcmp) ordering.
    #[must_use]
    pub fn is_default(&self) -> bool {
        matches!(self, KeyCmp::Default)
    }
}

impl std::fmt::Debug for KeyCmp<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyCmp::Default => f.write_str("KeyCmp::Default"),
            KeyCmp::Custom(c) => write!(f, "KeyCmp::Custom({})", c.name()),
        }
    }
}

/// Why registering a comparator was refused (milestone 2.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComparatorError {
    /// A comparator was requested for the main / unnamed database. Refused
    /// unconditionally: that tree is also the named-DB catalog (SPEC 02 §6),
    /// which is engine-internal and must stay memcmp-ordered. Use a named
    /// database.
    MainDatabase,
    /// A *different* comparator is already registered for this database in
    /// this process — the two names disagree. Carries `(registered, requested)`.
    ///
    /// Refused rather than resolved: silently keeping either one would leave
    /// half the process reading the database under an ordering it did not ask
    /// for, which is the exact failure mode this milestone is trying to make
    /// impossible to reach by accident.
    Mismatch {
        /// The [`Comparator::name`] already registered for this database.
        registered: String,
        /// The [`Comparator::name`] of the rejected registration.
        requested: String,
    },
}

impl std::fmt::Display for ComparatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComparatorError::MainDatabase => f.write_str(
                "the main/unnamed database is also the named-DB catalog and always uses \
                 memcmp ordering; register a comparator on a named database instead",
            ),
            ComparatorError::Mismatch {
                registered,
                requested,
            } => write!(
                f,
                "database already has comparator {registered:?} registered in this process; \
                 refusing to also register {requested:?}"
            ),
        }
    }
}

impl std::error::Error for ComparatorError {}

/// The environment's per-database comparator table (milestone 2.4).
///
/// Sized `max_dbs` at env open and indexed by dbi, so a lookup on the read
/// path is an index and a `OnceLock::get` — no lock, no allocation. Each slot
/// is written at most once, which is what lets a lookup hand out a
/// `&'a dyn Comparator` borrowed from the environment: the registry owns the
/// boxed comparator for the env's whole life and never moves or replaces it.
///
/// The `Mutex<HashMap<..>>` beside it is *not* on the read path — it exists
/// only to make the "same dbi, second registration" check atomic against a
/// concurrent registration of the same database, and is touched once per
/// `*_with_comparator` open.
pub struct ComparatorRegistry {
    /// dbi → comparator, written at most once per slot. `None`/unset = memcmp.
    slots: Box<[OnceLock<Box<dyn Comparator>>]>,
    /// Serializes registration so the read-modify-write of a slot is atomic.
    /// Maps dbi → registered name, for the mismatch diagnostic.
    reg_lock: Mutex<HashMap<u32, String>>,
}

impl ComparatorRegistry {
    /// Allocate a registry for `max_dbs` named databases.
    #[must_use]
    pub fn new(max_dbs: u32) -> ComparatorRegistry {
        let slots = (0..max_dbs)
            .map(|_| OnceLock::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        ComparatorRegistry {
            slots,
            reg_lock: Mutex::new(HashMap::new()),
        }
    }

    /// The ordering in force for named database `dbi`.
    ///
    /// An out-of-range dbi (impossible for a handle the engine produced) and
    /// an unregistered one both mean memcmp.
    #[must_use]
    pub fn get(&self, dbi: u32) -> KeyCmp<'_> {
        match self.slots.get(dbi as usize).and_then(OnceLock::get) {
            Some(c) => KeyCmp::Custom(&**c),
            None => KeyCmp::Default,
        }
    }

    /// Register `cmp` for named database `dbi`.
    ///
    /// Idempotent for the same [`Comparator::name`]: re-registering the same
    /// ordering (the normal case — every process opening the DB must pass its
    /// comparator) is `Ok` and keeps the first instance.
    ///
    /// # Errors
    ///
    /// [`ComparatorError::Mismatch`] if a comparator with a *different* name
    /// is already registered for `dbi`.
    pub fn register(&self, dbi: u32, cmp: Box<dyn Comparator>) -> Result<(), ComparatorError> {
        let requested = cmp.name().to_string();
        let mut guard = self.reg_lock.lock().expect("comparator registry poisoned");
        if let Some(registered) = guard.get(&dbi) {
            if registered == &requested {
                return Ok(());
            }
            return Err(ComparatorError::Mismatch {
                registered: registered.clone(),
                requested,
            });
        }
        let Some(slot) = self.slots.get(dbi as usize) else {
            // Unreachable for an engine-issued dbi (< max_dbs). Treating it as
            // "no custom ordering" rather than panicking keeps a corrupt dbi
            // from taking the process down.
            return Ok(());
        };
        // The `reg_lock` guard makes this the only writer, so `set` cannot
        // lose; `let _ =` documents that rather than unwrapping on a value
        // that is not `Debug`.
        let _ = slot.set(cmp);
        guard.insert(dbi, requested);
        Ok(())
    }

    /// Whether any custom comparator is registered on this environment.
    ///
    /// Used to gate features that are still memcmp-only (the compacting copy,
    /// SPEC 03 §2.0) so they refuse loudly instead of silently rebuilding a
    /// tree in the wrong order.
    #[must_use]
    pub fn any_custom(&self) -> bool {
        !self
            .reg_lock
            .lock()
            .expect("comparator registry poisoned")
            .is_empty()
    }
}

impl std::fmt::Debug for ComparatorRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComparatorRegistry")
            .field("max_dbs", &self.slots.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn rev() -> Box<dyn Comparator> {
        Box::new(FnComparator::new("test.reverse", |a: &[u8], b: &[u8]| {
            b.cmp(a)
        }))
    }

    #[test]
    fn default_is_memcmp() {
        let c = KeyCmp::Default;
        assert_eq!(c.compare(b"a", b"b"), Ordering::Less);
        assert!(c.eq(b"xy", b"xy"));
        assert!(c.is_default());
    }

    #[test]
    fn registry_hands_out_registered_comparator() {
        let r = ComparatorRegistry::new(4);
        assert!(r.get(0).is_default());
        assert!(!r.any_custom());
        r.register(0, rev()).unwrap();
        assert_eq!(r.get(0).compare(b"a", b"b"), Ordering::Greater);
        assert!(r.get(1).is_default(), "other dbis are unaffected");
        assert!(r.any_custom());
    }

    #[test]
    fn same_name_registration_is_idempotent() {
        let r = ComparatorRegistry::new(2);
        r.register(0, rev()).unwrap();
        r.register(0, rev())
            .expect("same ordering re-registers cleanly");
        assert_eq!(r.get(0).compare(b"a", b"b"), Ordering::Greater);
    }

    #[test]
    fn conflicting_registration_is_refused() {
        let r = ComparatorRegistry::new(2);
        r.register(0, rev()).unwrap();
        let err = r
            .register(0, Box::new(DefaultComparator))
            .expect_err("a different ordering for the same dbi must be refused");
        assert_eq!(
            err,
            ComparatorError::Mismatch {
                registered: "test.reverse".into(),
                requested: "memcmp".into(),
            }
        );
        // The first registration survives the refusal.
        assert_eq!(r.get(0).compare(b"a", b"b"), Ordering::Greater);
    }

    #[test]
    fn out_of_range_dbi_is_memcmp_not_a_panic() {
        let r = ComparatorRegistry::new(1);
        assert!(r.get(9).is_default());
        r.register(9, rev()).unwrap();
        assert!(r.get(9).is_default());
    }

    // --- roadmap 7e: equal-length integer-compare fast path ---

    /// The 8-/4-byte fast path must return exactly memcmp order on the edges
    /// that a naive integer compare could get wrong.
    #[test]
    fn fast_path_matches_memcmp_on_edges() {
        // Equal keys, both widths.
        assert_eq!(default_cmp_fast(&[0u8; 8], &[0u8; 8]), Ordering::Equal);
        assert_eq!(default_cmp_fast(&[0xAB; 4], &[0xAB; 4]), Ordering::Equal);

        // Differ only in the last (least-significant) byte.
        let a8 = [1, 2, 3, 4, 5, 6, 7, 8];
        let mut b8 = a8;
        b8[7] = 9;
        assert_eq!(default_cmp_fast(&a8, &b8), Ordering::Less);
        assert_eq!(default_cmp_fast(&b8, &a8), Ordering::Greater);

        // 0x00 vs 0xFF boundaries in the most-significant byte: memcmp treats
        // bytes as unsigned, and so must the integer compare.
        assert_eq!(default_cmp_fast(&[0x00u8; 8], &[0xFFu8; 8]), Ordering::Less);
        assert_eq!(
            default_cmp_fast(&[0xFFu8; 4], &[0x00u8; 4]),
            Ordering::Greater
        );
        // High bit set in the leading byte must still be the *larger* value
        // (unsigned), which a signed compare would get backwards.
        assert_eq!(
            default_cmp_fast(&[0x80, 0, 0, 0], &[0x7F, 0xFF, 0xFF, 0xFF]),
            Ordering::Greater
        );

        // Mixed 4-vs-8 lengths fall through to slice order (a prefix is less).
        assert_eq!(
            default_cmp_fast(&[0u8; 4], &[0u8; 8]),
            [0u8; 4].as_slice().cmp([0u8; 8].as_slice())
        );
        assert_eq!(
            default_cmp_fast(&[0xFFu8; 8], &[0xFFu8; 4]),
            [0xFFu8; 8].as_slice().cmp([0xFFu8; 4].as_slice())
        );
    }

    /// Proptest config that also runs under miri: failure persistence writes a
    /// regressions file (it calls `getcwd`, which miri's isolation refuses),
    /// and miri is ~1000× slower, so it gets fewer cases.
    fn prop_config() -> ProptestConfig {
        ProptestConfig {
            failure_persistence: None,
            cases: if cfg!(miri) { 16 } else { 256 },
            ..ProptestConfig::default()
        }
    }

    proptest! {
        #![proptest_config(prop_config())]
        /// For every length pair in 0..=16 and any bytes, the fast compare is
        /// byte-for-byte identical to slice (memcmp) order.
        #[test]
        fn fast_path_equals_slice_cmp_all_lengths(
            a in proptest::collection::vec(any::<u8>(), 0..=16),
            b in proptest::collection::vec(any::<u8>(), 0..=16),
        ) {
            prop_assert_eq!(default_cmp_fast(&a, &b), a.cmp(&b));
            // And through the public choke point, so both call sites benefit.
            prop_assert_eq!(KeyCmp::Default.compare(&a, &b), a.cmp(&b));
        }
    }

    /// A leaf/branch binary search over 8-byte big-endian keys must return the
    /// same Ok/Err positions as `slice::binary_search` on the sorted key list,
    /// confirming the fast path preserves search results end to end.
    #[test]
    fn lookup_over_be_keys_matches_binary_search() {
        use proptest::test_runner::TestRunner;

        let mut runner = TestRunner::new(prop_config());
        runner
            .run(
                &(
                    proptest::collection::btree_set(any::<u64>(), 0..64),
                    any::<u64>(),
                ),
                |(keys, probe)| {
                    let sorted: Vec<[u8; 8]> = keys.iter().map(|k| k.to_be_bytes()).collect();
                    let needle = probe.to_be_bytes();

                    // Reference: linear binary search over the sorted Vec.
                    let expected = sorted.binary_search(&needle);

                    // Under test: the same search driven by KeyCmp::Default,
                    // mirroring `leaf_lookup`'s loop over a sorted array.
                    let mut lo = 0usize;
                    let mut hi = sorted.len();
                    let mut got = Err(sorted.len());
                    while lo < hi {
                        let mid = lo + (hi - lo) / 2;
                        match KeyCmp::Default.compare(&sorted[mid], &needle) {
                            Ordering::Less => lo = mid + 1,
                            Ordering::Greater => hi = mid,
                            Ordering::Equal => {
                                got = Ok(mid);
                                break;
                            }
                        }
                    }
                    if got.is_err() {
                        got = Err(lo);
                    }

                    prop_assert_eq!(got, expected);
                    Ok(())
                },
            )
            .unwrap();
    }
}
