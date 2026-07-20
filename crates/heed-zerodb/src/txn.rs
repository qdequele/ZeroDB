//! Transactions — `RoTxn<'e, T>` / `RwTxn<'p>` and the TLS marker types
//! (SPEC 00 rows 2/13/14/15/16/27/28/29; SPEC 04 §3–§5; ADR-0003 §C6/§C7).
//!
//! ## The one internal representation trick
//!
//! heed's `RwTxn` embeds a `RoTxn` (the same C `MDB_txn`, readable) and derefs
//! to it, so `db.get(&wtxn, …)` reads the write txn's uncommitted state. ZeroDB
//! has three *distinct* Rust read sources — a committed [`zerodb::RoTxn`], a
//! nested [`zerodb::NestedRoTxn`] over a write txn, and a [`zerodb::RwTxn`]
//! itself (its dirty-frame view). We unify them behind one enum [`InnerTxn`] so
//! a single `RoTxn` type serves all three, exactly like heed's single `RoTxn`.
//! `RwTxn { txn: RoTxn<WithoutTls> }` (inner always [`InnerTxn::Rw`]) derefs to
//! that `RoTxn`, reproducing heed's read-through-a-write-txn.
//!
//! ## Send (SPEC 04 TXN-13; heed parity)
//!
//! heed declares `unsafe impl Send for RoTxn<WithoutTls>` and `RwTxn: Send`, and
//! both are load-bearing: consumers move `RoTxn<WithoutTls>` (nested readers,
//! `static_read_txn`) onto rayon/async threads, and milli moves `&mut RwTxn`
//! into a rayon `install`/join scope during indexing. We reproduce both. The
//! reader and nested variants are genuinely `Send` (ZeroDB's TXN-13). The
//! `InnerTxn::Rw` variant owns a `zerodb::RwTxn` whose write-mutex guard is
//! `!Send`; asserting it sendable mirrors heed's own bet on its `MDB_txn` — the
//! single-writer txn is handed to a *joined* worker, so the guard is locked and
//! dropped on the owning thread while only mutable access crosses into the
//! rayon-join-ordered worker. `RwTxn` inherits `Send` from its sole
//! `RoTxn<WithoutTls>` field.

use std::marker::PhantomData;
use std::ops::Deref;

use crate::Result;

// ---------------------------------------------------------------------------
// TLS markers (SPEC 04 §4; ADR-0003 §C6)
// ---------------------------------------------------------------------------

/// Read transactions opened with Thread Local Storage (TLS) — `!Send`. A
/// compile-only shim in Phase 1 (SPEC 00 second table: `WithTls` is SHOULD);
/// ZeroDB's read txns are universally NOTLS, so `WithTls` behaves like
/// `WithoutTls` at runtime (ADR-0003 Q5).
#[derive(Debug, PartialEq, Eq)]
pub enum WithTls {}

/// Read transactions opened without TLS — therefore `Send` (SPEC 00 rows 2/29).
/// Every production open uses this (`read_txn_without_tls`).
#[derive(Debug, PartialEq, Eq)]
pub enum WithoutTls {}

/// Read transactions that might have been opened with or without TLS.
/// `RwTxn`s and any `RoTxn` dereference to `&RoTxn<AnyTls>`.
pub enum AnyTls {}

/// Whether TLS must be used when opening transactions.
pub trait TlsUsage {
    /// True if TLS must be used, false otherwise.
    const ENABLED: bool;
}

impl TlsUsage for WithTls {
    const ENABLED: bool = true;
}
impl TlsUsage for WithoutTls {
    const ENABLED: bool = false;
}
impl TlsUsage for AnyTls {
    const ENABLED: bool = false;
}

// ---------------------------------------------------------------------------
// RoTxn
// ---------------------------------------------------------------------------

/// The three ZeroDB read sources unified behind one type. All arms implement
/// `zerodb::TxnRead`; the read API dispatches over them (see `with_read!`).
// The `Rw` variant (an owning write txn) is far larger than the readers, but
// boxing it would break the direct `&zerodb::RwTxn: TxnRead` dispatch (a
// `&Box<_>` is not `&_` in generic position) and add an allocation to every
// write txn; the write path is not size-sensitive, so the layout is deliberate.
#[allow(clippy::large_enum_variant)]
pub(crate) enum InnerTxn<'e> {
    /// A committed-snapshot read txn (`Env::read_txn` / `static_read_txn`).
    Ro(zerodb::RoTxn<'e>),
    /// A nested read child of a write txn (`RwTxn::nested_read_txn`).
    Nested(zerodb::NestedRoTxn<'e>),
    /// The write txn's own dirty-frame view (the `RwTxn` deref target).
    Rw(zerodb::RwTxn<'e>),
}

/// A read-only transaction — the exact `heed::RoTxn<'e, T>` shape (default
/// `AnyTls`). Covariant in `'e` (ADR-0007 D6 for the nested case).
#[repr(transparent)]
pub struct RoTxn<'e, T = AnyTls> {
    pub(crate) inner: InnerTxn<'e>,
    _tls: PhantomData<&'e T>,
}

// SAFETY (SPEC 04 TXN-13; heed parity, see module docs): the reader
// (`InnerTxn::Ro`) and nested (`InnerTxn::Nested`) variants are `Send` by
// ZeroDB's own guarantees (`zerodb::RoTxn` / `zerodb::NestedRoTxn` are `Send`).
// The `InnerTxn::Rw` variant carries a `!Send` write-mutex guard; declaring it
// sendable mirrors heed's identical `unsafe impl Send for RoTxn<WithoutTls>`
// and is sound for the single-writer install/join usage (module docs): the
// guard is locked and dropped on the owning thread; only a mutable borrow
// crosses into a rayon-join-ordered worker.
unsafe impl Send for RoTxn<'_, WithoutTls> {}

impl<'e, T> RoTxn<'e, T> {
    pub(crate) fn from_inner(inner: InnerTxn<'e>) -> RoTxn<'e, T> {
        RoTxn {
            inner,
            _tls: PhantomData,
        }
    }

    /// Commit a read transaction (SPEC 00 — `RoTxn::commit`). For ZeroDB a read
    /// txn commit is a plain release (no cross-process metadata sync to do); it
    /// simply drops the pinned snapshot / reader slot.
    ///
    /// # Errors
    ///
    /// Infallible in ZeroDB; returns [`Result`] for API shape.
    pub fn commit(self) -> Result<()> {
        drop(self);
        Ok(())
    }

    /// This transaction's id (SPEC 00 second table — SHOULD, **landed in
    /// milestone 2.7**; `mdb_txn_id`).
    ///
    /// For a read txn this is the **pinned snapshot's** txnid — the commit
    /// this reader sees, which is also what `Env::reader_list` reports for its
    /// slot (M2.2). For a write txn it is the id the txn *will* publish when
    /// it commits. A nested read txn reports its parent write txn's id, since
    /// that is the state it observes (SPEC 04 §5).
    #[must_use]
    pub fn id(&self) -> usize {
        let id = match &self.inner {
            InnerTxn::Ro(r) => r.txnid(),
            InnerTxn::Nested(n) => n.txnid(),
            InnerTxn::Rw(w) => w.txnid(),
        };
        // heed types this as `usize` (`mdb_txn_id` returns `size_t`); ZeroDB
        // txnids are `u64`. On a 32-bit target this would truncate, but the
        // supported targets (CLAUDE.md: linux-aarch64 primary, linux-x86_64,
        // macOS aarch64) are all 64-bit, so the cast is lossless there.
        id as usize
    }
}

/// `RoTxn<WithTls>` → `RoTxn<AnyTls>` (SPEC 04 §4 deref chain). ZeroDB carries
/// no per-txn TLS state, so the marker change is a pure retag: the transparent
/// layout is identical across `T`, so a reference retag is sound.
impl<'a> Deref for RoTxn<'a, WithTls> {
    type Target = RoTxn<'a, AnyTls>;
    fn deref(&self) -> &Self::Target {
        // SAFETY: `RoTxn<T>` is `#[repr(transparent)]` over `InnerTxn`; the only
        // `T`-dependent field is a ZST `PhantomData`, so all `RoTxn<'a, T>`
        // share one layout and a `&`-retag is valid (mirrors heed's identical
        // `transmute`-based deref).
        unsafe { &*(self as *const RoTxn<'a, WithTls>).cast::<RoTxn<'a, AnyTls>>() }
    }
}

/// `RoTxn<WithoutTls>` → `RoTxn<AnyTls>` (SPEC 04 §4).
impl<'a> Deref for RoTxn<'a, WithoutTls> {
    type Target = RoTxn<'a, AnyTls>;
    fn deref(&self) -> &Self::Target {
        // SAFETY: as above.
        unsafe { &*(self as *const RoTxn<'a, WithoutTls>).cast::<RoTxn<'a, AnyTls>>() }
    }
}

// ---------------------------------------------------------------------------
// RwTxn
// ---------------------------------------------------------------------------

/// A read-write transaction — the exact `heed::RwTxn<'p>` shape. Owns the
/// ZeroDB write txn inside its deref-target `RoTxn`.
///
/// **`Send` (heed parity, milli requirement).** heed declares `RwTxn: Send`, and
/// milli moves `&mut RwTxn` into a rayon `install`/join scope during indexing
/// (`ThreadPoolNoAbort::install`), which needs `RwTxn: Send`. This type is `Send`
/// via the `unsafe impl Send for RoTxn<WithoutTls>` above (its only field). The
/// underlying ZeroDB write-mutex guard is `!Send`, so this asserts what heed
/// asserts for its `MDB_txn`: the single-writer txn is safe to hand to a joined
/// worker (the guard is locked and dropped on the owning thread; only mutable
/// access happens on the joined worker, ordered by the rayon join). `Sync`
/// too, so `&RwTxn` — hence a nested reader — is `Send` (SPEC 04 §5).
pub struct RwTxn<'p> {
    pub(crate) txn: RoTxn<'p, WithoutTls>,
}

impl<'p> RwTxn<'p> {
    pub(crate) fn from_zdb(w: zerodb::RwTxn<'p>) -> RwTxn<'p> {
        RwTxn {
            txn: RoTxn::from_inner(InnerTxn::Rw(w)),
        }
    }

    /// `&zerodb::RwTxn` for read dispatch and nested-txn open.
    pub(crate) fn zdb(&self) -> &zerodb::RwTxn<'p> {
        match &self.txn.inner {
            InnerTxn::Rw(w) => w,
            _ => unreachable!("RwTxn always wraps InnerTxn::Rw"),
        }
    }

    /// `&mut zerodb::RwTxn` for the write API.
    pub(crate) fn zdb_mut(&mut self) -> &mut zerodb::RwTxn<'p> {
        match &mut self.txn.inner {
            InnerTxn::Rw(w) => w,
            _ => unreachable!("RwTxn always wraps InnerTxn::Rw"),
        }
    }

    fn into_zdb(self) -> zerodb::RwTxn<'p> {
        match self.txn.inner {
            InnerTxn::Rw(w) => w,
            _ => unreachable!("RwTxn always wraps InnerTxn::Rw"),
        }
    }

    /// Commit all operations of the transaction (SPEC 00 row 27).
    ///
    /// # Errors
    ///
    /// The commit pipeline's failures (`MapFull`, poisoned env, a live nested
    /// child → `BadTxn`), mapped to [`crate::Error`].
    pub fn commit(self) -> Result<()> {
        self.into_zdb().commit().map_err(Into::into)
    }

    /// Abandon all operations of the transaction (SPEC 00 row 28).
    pub fn abort(self) {
        self.into_zdb().abort();
    }

    /// Open a nested read transaction reading this write txn's uncommitted
    /// state (SPEC 00 row 16, SPEC 04 §5). Equivalent to
    /// [`crate::Env::nested_read_txn`] with this txn as the parent.
    ///
    /// # Errors
    ///
    /// [`crate::MdbError::BadTxn`] if this txn has errored (fork parity).
    pub fn nested_read_txn(&self) -> Result<RoTxn<'_, WithoutTls>> {
        let nested = self.zdb().nested_read_txn().map_err(crate::Error::from)?;
        Ok(RoTxn::from_inner(InnerTxn::Nested(nested)))
    }
}

impl<'p> Deref for RwTxn<'p> {
    type Target = RoTxn<'p, WithoutTls>;
    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}
