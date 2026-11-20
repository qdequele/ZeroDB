//! Custom key comparators (SPEC 03 §2.0; see docs/LMDB-COMPATIBILITY.md, DatabaseFlags).
//!
//! LMDB installs a comparator per database with `mdb_set_compare`, a raw C
//! function pointer. heed never calls it: every heed `Database` uses
//! `DefaultComparator`, unsigned lexicographic byte order (SPEC 00 row 53,
//! SPEC 03 §2 invariant BT-1). This module offers a custom order for named
//! databases as a safe Rust trait.
//!
//! ## Which trees take a comparator
//!
//! - A named database: the caller's comparator, or memcmp.
//! - The main (unnamed) database: always memcmp. It is also the named-database
//!   catalog (SPEC 02 §6), whose keys are database names and whose values are
//!   `DBRecord` bytes. A faulty caller comparator there would corrupt the
//!   catalog, not one database. See [`ComparatorError::MainDatabase`].
//! - The free-page tree: always memcmp. Its keys are big-endian txnids
//!   (SPEC 05), so memcmp order is numeric order, which GC depends on.
//!
//! ## The comparator is not persisted
//!
//! Neither LMDB nor ZeroDB stores the comparator in the file. Reopening a
//! database with a different comparator than the one that wrote it gives
//! wrong answers without any error: lookups miss keys that are present, range
//! scans return the wrong subsets, and later writes mix two orderings in one
//! tree, corrupting it. Nothing detects this at open. Passing the same
//! comparator on every open is the caller's obligation, as it is with
//! `mdb_set_compare`.
//!
//! - A mismatch within one process is detected. A comparator is registered
//!   once per database in [`ComparatorRegistry`]; a second registration with a
//!   different [`Comparator::name`] is refused
//!   ([`ComparatorError::Mismatch`]).
//! - A mismatch between two opens of the file is not detected. Detecting it
//!   needs a comparator identity in the per-database record, and `DBRecord`
//!   has no spare field (SPEC 02 §3.1), so it would be a format change. See
//!   docs/LMDB-COMPATIBILITY.md (comparator persistence).
//!
//! ## Requirements on an implementation
//!
//! A [`Comparator`] must be a total order and deterministic: the same pair of
//! byte strings must always compare the same way, for the life of the data,
//! across processes and releases. A violation is not memory-unsafe, but it
//! corrupts the tree: a B+tree built under an inconsistent ordering has no
//! correct search path, and `zerodb-tools check` reports INV-5/INV-6
//! violations.
//!
//! A comparator that panics unwinds through the engine. In a write
//! transaction the `RwTxn` is dropped and its dirty pages discarded, so the
//! file is untouched (SPEC 04 §9: the file changes only at commit), but the
//! operation is lost.

use std::cmp::Ordering;
use std::sync::{Mutex, OnceLock};

/// A total order over raw key bytes, the safe counterpart of LMDB's
/// `MDB_cmp_func` C function pointer.
///
/// Implementations must be a deterministic total order; see the [module
/// docs](self) for the contract and for what happens when a database is
/// reopened with a different comparator.
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
    /// `"custom"`. It is not written to disk, so it cannot detect a mismatch
    /// between two opens of the file (module docs).
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

/// A [`Comparator`] built from a closure.
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
/// `Custom(&DefaultComparator)` so that the common case is a predictable
/// branch to an inlined comparison with no vtable call: every `get`, `put`
/// and cursor step compares keys through this type.
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
/// Fixed-width big-endian integer keys (u32 and u64 ids) are common, and a
/// binary search over them compares two slices of the same length 8 or 4 at
/// every probe. For equal-length slices, comparing them as big-endian
/// integers gives memcmp order (the most significant differing byte decides
/// both), so this returns exactly `a.cmp(b)` with one integer load and
/// compare per side instead of a `memcmp` call (see docs/PERFORMANCE.md).
/// Every other length pair, including 8 against 4, takes the slice
/// comparison.
#[inline]
fn default_cmp_fast(a: &[u8], b: &[u8]) -> Ordering {
    match (a.len(), b.len()) {
        // The `try_into` cannot fail: the match arm fixes each length.
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
    /// This is not byte equality: a comparator may consider distinct byte
    /// strings equal (a case-folding order, say), and the tree stores at most
    /// one key of any such group. Code that tests whether a search landed on
    /// the requested key must use this, not `==`.
    #[inline]
    #[must_use]
    pub fn eq(&self, a: &[u8], b: &[u8]) -> bool {
        self.compare(a, b) == Ordering::Equal
    }

    /// Whether this is the default (memcmp) ordering.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn is_default(&self) -> bool {
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

/// Why registering a comparator was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ComparatorError {
    /// A comparator was requested for the main / unnamed database. Refused
    /// unconditionally: that tree is also the named-DB catalog (SPEC 02 §6),
    /// which is engine-internal and must stay memcmp-ordered. Use a named
    /// database.
    #[error(
        "the main/unnamed database is also the named-DB catalog and always uses \
         memcmp ordering; register a comparator on a named database instead"
    )]
    MainDatabase,
    /// A *different* comparator is already registered for this database in
    /// this process — the two names disagree. Carries `(registered, requested)`.
    ///
    /// Keeping either one would leave part of the process reading the
    /// database under an ordering it did not ask for, so the registration is
    /// refused.
    #[error(
        "database already has comparator {registered:?} registered in this process; \
         refusing to also register {requested:?}"
    )]
    Mismatch {
        /// The [`Comparator::name`] already registered for this database.
        registered: String,
        /// The [`Comparator::name`] of the rejected registration.
        requested: String,
    },
}

/// The environment's per-database comparator table.
///
/// Sized `max_dbs` at env open and indexed by dbi, so a lookup is an index
/// and a `OnceLock::get`, with no lock and no allocation. Each slot is
/// written at most once, so a lookup can hand out a `&'a dyn Comparator`
/// borrowed from the environment: the registry owns the boxed comparator for
/// the env's whole life and never moves or replaces it.
///
/// The `Mutex` is taken only by registration, to make the check for an
/// existing comparator and the write of the slot atomic.
pub struct ComparatorRegistry {
    /// dbi → comparator, written at most once per slot. `None`/unset = memcmp.
    slots: Box<[OnceLock<Box<dyn Comparator>>]>,
    /// Serializes registration so the read-modify-write of a slot is atomic.
    reg_lock: Mutex<()>,
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
            reg_lock: Mutex::new(()),
        }
    }

    /// The ordering in force for named database `dbi`.
    ///
    /// An out-of-range dbi (impossible for a handle the engine produced) and
    /// an unregistered one both mean memcmp.
    #[must_use]
    #[inline]
    pub fn get(&self, dbi: u32) -> KeyCmp<'_> {
        match self.slots.get(dbi as usize).and_then(OnceLock::get) {
            Some(c) => KeyCmp::Custom(&**c),
            None => KeyCmp::Default,
        }
    }

    /// Register `cmp` for named database `dbi`.
    ///
    /// Idempotent for the same [`Comparator::name`]: registering the same
    /// ordering again is `Ok` and keeps the first instance. This is the normal
    /// case, since every open of the database passes its comparator.
    ///
    /// # Errors
    ///
    /// [`ComparatorError::Mismatch`] if a comparator with a *different* name
    /// is already registered for `dbi`.
    pub fn register(&self, dbi: u32, cmp: Box<dyn Comparator>) -> Result<(), ComparatorError> {
        let _guard = self.reg_lock.lock().expect("comparator registry poisoned");
        let Some(slot) = self.slots.get(dbi as usize) else {
            // Unreachable for an engine-issued dbi (< max_dbs). Treating it as
            // "no custom ordering" rather than panicking keeps a corrupt dbi
            // from taking the process down.
            return Ok(());
        };
        if let Some(registered) = slot.get() {
            if registered.name() == cmp.name() {
                return Ok(());
            }
            return Err(ComparatorError::Mismatch {
                registered: registered.name().to_string(),
                requested: cmp.name().to_string(),
            });
        }
        // The `reg_lock` guard makes this the only writer, so `set` cannot
        // fail. Its error type is not `Debug`, so it cannot be unwrapped.
        let _ = slot.set(cmp);
        Ok(())
    }

    /// Whether any custom comparator is registered on this environment.
    ///
    /// Features that support only memcmp order (the compacting copy, SPEC 03
    /// §2.0) check this and refuse, since they would otherwise rebuild a tree
    /// in the wrong order.
    #[must_use]
    pub fn any_custom(&self) -> bool {
        self.slots.iter().any(|slot| slot.get().is_some())
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

    #[test]
    fn error_messages_and_sources() {
        use std::error::Error as _;
        let main = ComparatorError::MainDatabase;
        assert_eq!(
            main.to_string(),
            "the main/unnamed database is also the named-DB catalog and always uses \
             memcmp ordering; register a comparator on a named database instead"
        );
        assert!(main.source().is_none());
        let mismatch = ComparatorError::Mismatch {
            registered: "a\"b".to_owned(),
            requested: "c".to_owned(),
        };
        assert_eq!(
            mismatch.to_string(),
            "database already has comparator \"a\\\"b\" registered in this process; \
             refusing to also register \"c\""
        );
        assert!(mismatch.source().is_none());
    }

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
            // The public entry point must agree too.
            prop_assert_eq!(KeyCmp::Default.compare(&a, &b), a.cmp(&b));
        }
    }

    /// A binary search over 8-byte big-endian keys with the fast comparison
    /// must return the same `Ok`/`Err` positions as `slice::binary_search`.
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

                    // Reference: the standard library's binary search.
                    let expected = sorted.binary_search(&needle);

                    // Under test: the same search driven by `KeyCmp::Default`.
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
