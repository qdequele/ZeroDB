//! Environment open/close, meta selection, and the same-process registry
//! (SPEC 02 §3.2, SPEC 06 §1, SPEC 04 §7). Milestone 1.2.
//!
//! This module owns the *behavioral* half of env lifecycle and is deliberately
//! I/O-free so it stays `miri`-clean: the mapped file bytes reach it through the
//! [`Backing`] trait, which the mmap layer (`zerodb-io`) implements for real
//! envs and which tests implement over a plain `Vec<u8>`. All `unsafe` (the
//! mmap) lives behind that trait in `zerodb-io`; this module contains none.
//!
//! What is implemented here (M1.2 scope): reading both meta slots, validating
//! each via the M1.1 predicate ([`crate::page::MetaPage::validate`]), selecting
//! the live snapshot (normal / `PREV_SNAPSHOT`), mapping the SPEC 06 REC error
//! taxonomy onto [`Error`], the process registry with `EnvAlreadyOpened`, the
//! refcounted [`EnvInner`] behind [`Env`] (`Clone`), and deferred close with
//! [`EnvClosingEvent`] (SPEC 04 TXN-52/53). Write txns, the reader table, and
//! the B-tree read path are later milestones and are *not* built here.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::Duration;

use crate::error::{Error, MdbError};
use crate::page::geometry::{is_map_full, map_pages};
use crate::page::{select_meta, MetaChoice, MetaPage, MetaValidity, META_A_PGNO, META_B_PGNO};

/// Read access to a memory-mapped (or, in tests, heap) env file.
///
/// The concrete real implementation is `zerodb_io::MmapBacking`, which confines
/// the mmap `unsafe` and guarantees the map is unmapped before the file fd is
/// closed (SPEC 04 TXN-53). Tests supply a `Vec<u8>`-backed implementation so
/// the selection logic runs under `miri`.
///
/// Implementors must be `Send + Sync`: an [`EnvInner`] is shared across threads
/// through an `Arc` and read txns (later milestones) hand out `Send` borrows.
pub trait Backing: Send + Sync {
    /// The whole mapped region as bytes. Slot 0 is `[0, page_size)`, slot 1 is
    /// `[page_size, 2*page_size)`; data pages follow.
    fn bytes(&self) -> &[u8];

    /// The actual on-disk length of the backing file (`fstat`), for
    /// [`Env::real_disk_size`] (SPEC 00 row 18).
    fn real_disk_size(&self) -> std::io::Result<u64>;

    /// `dup()` the backing data-file descriptor (SPEC 00 row 22,
    /// [`Env::try_clone_inner_file`]). Requires the env to be a single regular
    /// data file (SPEC 02 §8).
    fn try_clone_file(&self) -> std::io::Result<std::fs::File>;
}

/// A cross-thread one-shot signal: fires once, when the last [`EnvInner`]
/// reference is dropped (SPEC 04 TXN-53). Waiters block until then.
#[derive(Debug)]
struct SignalEvent {
    fired: Mutex<bool>,
    cv: Condvar,
}

impl SignalEvent {
    fn new() -> SignalEvent {
        SignalEvent {
            fired: Mutex::new(false),
            cv: Condvar::new(),
        }
    }

    fn signal(&self) {
        let mut g = self.fired.lock().expect("signal mutex poisoned");
        *g = true;
        self.cv.notify_all();
    }

    fn wait(&self) {
        let mut g = self.fired.lock().expect("signal mutex poisoned");
        while !*g {
            g = self.cv.wait(g).expect("signal mutex poisoned");
        }
    }

    /// Wait up to `dur`. Returns `true` if the event has fired.
    fn wait_timeout(&self, dur: Duration) -> bool {
        let g = self.fired.lock().expect("signal mutex poisoned");
        if *g {
            return true;
        }
        let (g2, _res) = self.cv.wait_timeout(g, dur).expect("signal mutex poisoned");
        *g2
    }
}

// ---------------------------------------------------------------------------
// Same-process registry (SPEC 04 TXN-51)
// ---------------------------------------------------------------------------

/// Registry value: the owning env's unique id plus a `Weak` handle. The id lets
/// an [`EnvInner`]'s `Drop` remove only *its own* slot even if a newer env has
/// meanwhile re-registered the same path (the classic registry race).
type RegEntry = (u64, Weak<EnvInner>);

fn registry() -> &'static Mutex<HashMap<PathBuf, RegEntry>> {
    static REG: OnceLock<Mutex<HashMap<PathBuf, RegEntry>>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(HashMap::new()))
}

fn next_env_id() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    // Relaxed: the counter only needs uniqueness, it publishes/acquires no other
    // memory (each fetch_add yields a distinct value on every architecture).
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// EnvInner / Env
// ---------------------------------------------------------------------------

/// The shared, refcounted heart of an environment (SPEC 04 TXN-50). Owns the
/// backing map, the selected snapshot metadata, the registry back-reference,
/// and the close signal. Torn down (map unmapped, fd closed, registry slot
/// cleared, close event fired) only when the last [`Env`] / txn reference drops
/// (TXN-52/53).
pub struct EnvInner {
    /// Unique id for the registry-race guard.
    id: u64,
    /// Canonical directory path — the registry key and [`Env::path`] value.
    path: PathBuf,
    /// The mapped file. `Option` so `Drop` can release it *before* firing the
    /// close event, guaranteeing waiters observe a fully-closed env (TXN-53).
    ///
    /// No `Mutex` is needed: the only mutation is the release in [`EnvInner`]'s
    /// `Drop`, which receives `&mut self` and runs only when the last `Arc`
    /// reference is gone (so no reader can be touching it concurrently). Shared
    /// `&self` readers ([`EnvInner::backing_bytes`], `real_disk_size`, …) take
    /// only immutable references, which is why a `RoTxn` can borrow the mapped
    /// `&[u8]` for its whole life (SPEC 04 TXN-37) with no `unsafe` in this
    /// crate. `Backing: Send + Sync` keeps `EnvInner: Sync`.
    backing: Option<Box<dyn Backing>>,
    /// The DB page size (from the live meta; authoritative — SPEC 02 §3.2).
    page_size: u32,
    /// The runtime map size (SPEC 02 §8): the caller's `map_size` if given, else
    /// the live meta's. Returned by [`Env::info`].
    map_size: u64,
    /// The selected live meta snapshot (higher-txnid, or older under
    /// `PREV_SNAPSHOT`).
    meta: MetaPage,
    /// Whether this env was opened with `PREV_SNAPSHOT` (SPEC 01 §S5).
    prev_snapshot: bool,
    /// Close signal, shared with any outstanding [`EnvClosingEvent`].
    closing: Arc<SignalEvent>,
}

impl std::fmt::Debug for EnvInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvInner")
            .field("id", &self.id)
            .field("path", &self.path)
            .field("page_size", &self.page_size)
            .field("map_size", &self.map_size)
            .field("txnid", &self.meta.txnid)
            .field("prev_snapshot", &self.prev_snapshot)
            .finish_non_exhaustive()
    }
}

impl EnvInner {
    /// The DB page size recorded in the live meta.
    #[must_use]
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// The runtime map size (SPEC 00 row 20, `Env::info().map_size`).
    #[must_use]
    pub fn map_size(&self) -> u64 {
        self.map_size
    }

    /// The txnid of the selected live snapshot.
    #[must_use]
    pub fn txnid(&self) -> u64 {
        self.meta.txnid
    }

    /// The live meta snapshot.
    #[must_use]
    pub fn meta(&self) -> &MetaPage {
        &self.meta
    }

    /// Whether this env was opened on the previous (older) snapshot.
    #[must_use]
    pub fn is_prev_snapshot(&self) -> bool {
        self.prev_snapshot
    }

    /// The canonical directory path (SPEC 00 row 21).
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The actual on-disk file length (SPEC 00 row 18).
    ///
    /// # Errors
    ///
    /// Propagates the `fstat` I/O error.
    pub fn real_disk_size(&self) -> Result<u64, Error> {
        match self.backing.as_ref() {
            Some(b) => Ok(b.real_disk_size()?),
            None => Err(Error::Mdb(MdbError::Invalid)),
        }
    }

    /// The whole mapped env file as bytes, borrowed for as long as this
    /// `EnvInner` is borrowed (SPEC 04 TXN-37). A `RoTxn` holds `&'env Env`,
    /// which keeps the owning `Arc<EnvInner>` (and hence this map) alive for the
    /// txn's life, so the returned slice is valid `'env`. Returns an empty slice
    /// only after close has released the map (never observed by a live txn).
    #[must_use]
    pub fn backing_bytes(&self) -> &[u8] {
        match self.backing.as_ref() {
            Some(b) => b.bytes(),
            None => &[],
        }
    }

    /// `dup()` the data-file fd for raw snapshot streaming (SPEC 00 row 22).
    ///
    /// # Errors
    ///
    /// Propagates the `dup` I/O error.
    pub fn try_clone_inner_file(&self) -> Result<std::fs::File, Error> {
        match self.backing.as_ref() {
            Some(b) => Ok(b.try_clone_file()?),
            None => Err(Error::Mdb(MdbError::Invalid)),
        }
    }

    /// Whether allocating an `n`-page run at `next_pgno` would exceed the map
    /// (SPEC 02 §8, `MdbError::MapFull`). No consumer of the write path exists
    /// yet (M1.4); exposed now so the geometry ceiling is testable at open.
    #[must_use]
    pub fn would_map_full(&self, next_pgno: u64, n: u64) -> bool {
        is_map_full(next_pgno, n, map_pages(self.map_size, self.page_size))
    }
}

impl Drop for EnvInner {
    fn drop(&mut self) {
        // Remove our own registry slot (guarded by id against a re-registered
        // path — see `RegEntry`).
        if let Ok(mut reg) = registry().lock() {
            if let Some((rid, _)) = reg.get(&self.path) {
                if *rid == self.id {
                    reg.remove(&self.path);
                }
            }
        }
        // Release the map (and, inside `MmapBacking`, close the fd) *before*
        // firing the close event, so `EnvClosingEvent::wait` returns only once
        // teardown has actually run (SPEC 04 TXN-53). `Drop` holds `&mut self`
        // (the last `Arc` reference is gone), so taking the backing here races
        // with no reader.
        drop(self.backing.take());
        self.closing.signal();
    }
}

/// A cheap, cloneable environment handle (SPEC 00 row 25, SPEC 04 TXN-50).
///
/// `Clone` bumps the refcount; it never reopens the file. The underlying
/// [`EnvInner`] is torn down only when the last clone (and every outstanding
/// txn, in later milestones) drops.
#[derive(Clone)]
pub struct Env {
    inner: Arc<EnvInner>,
}

impl std::fmt::Debug for Env {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Env").field(&self.inner).finish()
    }
}

impl Env {
    /// Access the shared inner state.
    #[must_use]
    pub fn inner(&self) -> &Arc<EnvInner> {
        &self.inner
    }

    /// The DB page size (SPEC 02 §3.2, authoritative from the live meta).
    #[must_use]
    pub fn page_size(&self) -> u32 {
        self.inner.page_size()
    }

    /// The runtime map size (SPEC 00 row 20).
    #[must_use]
    pub fn map_size(&self) -> u64 {
        self.inner.map_size()
    }

    /// Environment info (SPEC 00 row 20/60). Only `map_size` is populated in
    /// Phase 1 — the sole field any consumer reads (`Env::info().map_size`).
    #[must_use]
    pub fn info(&self) -> EnvInfo {
        EnvInfo {
            map_size: self.inner.map_size(),
        }
    }

    /// The canonical directory path (SPEC 00 row 21).
    #[must_use]
    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    /// The txnid of the live snapshot.
    #[must_use]
    pub fn txnid(&self) -> u64 {
        self.inner.txnid()
    }

    /// Whether opened on the previous snapshot (`PREV_SNAPSHOT`).
    #[must_use]
    pub fn is_prev_snapshot(&self) -> bool {
        self.inner.is_prev_snapshot()
    }

    /// Actual on-disk file size (SPEC 00 row 18).
    ///
    /// # Errors
    ///
    /// Propagates the `fstat` I/O error.
    pub fn real_disk_size(&self) -> Result<u64, Error> {
        self.inner.real_disk_size()
    }

    /// `dup()` the data-file fd (SPEC 00 row 22).
    ///
    /// # Errors
    ///
    /// Propagates the `dup` I/O error.
    pub fn try_clone_inner_file(&self) -> Result<std::fs::File, Error> {
        self.inner.try_clone_inner_file()
    }

    /// Number of strong references to the shared inner (this handle included).
    /// Diagnostic; the deferred-close contract does not depend on it.
    #[must_use]
    pub fn handle_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    /// Consume this handle and return an [`EnvClosingEvent`] that fires when the
    /// **last** reference to the env drops (SPEC 04 TXN-52). Dropping this
    /// handle is part of the close: if it was the last reference, the event has
    /// already fired by the time this returns.
    #[must_use]
    pub fn prepare_for_closing(self) -> EnvClosingEvent {
        let event = EnvClosingEvent {
            signal: Arc::clone(&self.inner.closing),
        };
        // Drop this handle's strong ref; if it was the last, `EnvInner::drop`
        // fires `signal` now.
        drop(self);
        event
    }
}

/// Environment info (SPEC 00 rows 20/60). Mirrors the single field any consumer
/// reads from `mdb_env_info`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvInfo {
    /// The configured/runtime map size, in bytes.
    pub map_size: u64,
}

/// A signal fired once the environment is fully closed (SPEC 04 TXN-53,
/// SPEC 00 rows 23/24). Obtained from [`Env::prepare_for_closing`].
#[derive(Clone)]
pub struct EnvClosingEvent {
    signal: Arc<SignalEvent>,
}

impl std::fmt::Debug for EnvClosingEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvClosingEvent").finish_non_exhaustive()
    }
}

impl EnvClosingEvent {
    /// Block until the environment is fully closed.
    pub fn wait(&self) {
        self.signal.wait();
    }

    /// Block until closed or `dur` elapses. Returns `true` if the env closed.
    #[must_use]
    pub fn wait_timeout(&self, dur: Duration) -> bool {
        self.signal.wait_timeout(dur)
    }
}

// ---------------------------------------------------------------------------
// Open
// ---------------------------------------------------------------------------

/// Open an env over an already-constructed [`Backing`] (SPEC 02 §3.2, SPEC 06
/// §1). This is the I/O-free core of open: the caller (`zerodb-io` / the public
/// `zerodb` crate) has already opened, created-if-new, and mapped the file, and
/// determined `page_size` (from the meta, or the creation option) and the
/// runtime `map_size`.
///
/// Steps: registry dedup (`EnvAlreadyOpened`) → validate both slots → select →
/// map REC taxonomy to [`Error`] → build & register [`EnvInner`].
///
/// `page_size` is the size the caller mapped/validated with; it must be the
/// authoritative page size for the file (the meta's own `page_size` field is
/// re-checked by [`MetaPage::validate`]). The registry key is `canonical_path`;
/// this function does **not** touch the filesystem.
///
/// # Errors
///
/// - [`Error::EnvAlreadyOpened`] if a live handle for `canonical_path` exists.
/// - [`Error::Mdb`]`(`[`MdbError::Invalid`]`)` if neither slot validates, or
///   under the one-valid + `PREV_SNAPSHOT` combination (SPEC 06 REC-2†).
pub fn open_with_backing(
    canonical_path: PathBuf,
    backing: Box<dyn Backing>,
    page_size: u32,
    map_size: u64,
    prev_snapshot: bool,
) -> Result<Env, Error> {
    // Validate the two meta slots from the mapped bytes (SPEC 02 §3.2). A
    // decode error here (bad page size / truncated buffer) means the file is not
    // a usable env → Invalid.
    let bytes = backing.bytes();
    let ps = page_size as usize;
    let slot0 = read_slot(bytes, META_A_PGNO, ps, page_size)?;
    let slot1 = read_slot(bytes, META_B_PGNO, ps, page_size)?;

    // Select the live snapshot (SPEC 02 §3.2 / SPEC 06 REC-2..5).
    let meta = match select_meta(&slot0, &slot1, prev_snapshot) {
        MetaChoice::Both { meta, .. } => meta,
        MetaChoice::OnlyOne { meta, .. } => {
            if prev_snapshot {
                // REC-2† (ratified 2026-07-16): one valid slot + PREV_SNAPSHOT is
                // a hard error — there are not two committed snapshots to pick an
                // older from.
                return Err(Error::Mdb(MdbError::Invalid));
            }
            meta
        }
        // REC-3: both invalid → unrecoverable.
        MetaChoice::None => return Err(Error::Mdb(MdbError::Invalid)),
    };

    // Register under the process registry (SPEC 04 TXN-51). Hold the lock across
    // check + insert so two concurrent opens of one path cannot both succeed.
    let id = next_env_id();
    let closing = Arc::new(SignalEvent::new());
    let inner = Arc::new(EnvInner {
        id,
        path: canonical_path.clone(),
        backing: Some(backing),
        page_size,
        map_size,
        meta,
        prev_snapshot,
        closing,
    });

    {
        let mut reg = registry().lock().expect("registry mutex poisoned");
        if let Some((_, w)) = reg.get(&canonical_path) {
            if w.upgrade().is_some() {
                // A live handle already exists (SPEC 04 TXN-51). `inner` is
                // dropped here, releasing its (freshly built) backing.
                return Err(Error::EnvAlreadyOpened);
            }
        }
        reg.insert(canonical_path, (id, Arc::downgrade(&inner)));
    }

    Ok(Env { inner })
}

/// Validate one meta slot at `pgno` from the mapped bytes.
fn read_slot(
    bytes: &[u8],
    pgno: u64,
    page_size_usize: usize,
    page_size: u32,
) -> Result<MetaValidity, Error> {
    let base = pgno as usize * page_size_usize;
    let end = base
        .checked_add(page_size_usize)
        .ok_or(Error::Mdb(MdbError::Invalid))?;
    if end > bytes.len() {
        // The mapped region does not even cover both meta slots → not an env.
        return Err(Error::Mdb(MdbError::Invalid));
    }
    // `validate` never panics on bad content; a decode error (bad reader psize)
    // is impossible here because `page_size` is validated by the caller, but map
    // any such error to Invalid defensively.
    MetaPage::validate(&bytes[base..end], page_size).map_err(|_| Error::Mdb(MdbError::Invalid))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::MetaPage;

    /// A heap-backed [`Backing`] so the selection logic runs under `miri`
    /// (no mmap, no file I/O).
    struct VecBacking(Vec<u8>);

    impl Backing for VecBacking {
        fn bytes(&self) -> &[u8] {
            &self.0
        }
        fn real_disk_size(&self) -> std::io::Result<u64> {
            Ok(self.0.len() as u64)
        }
        fn try_clone_file(&self) -> std::io::Result<std::fs::File> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "vec backing has no fd",
            ))
        }
    }

    const PS: u32 = 4096;
    const MAP: u64 = 1 << 20;

    /// Build a two-slot file image; each slot carries the given txnid (or is
    /// left as raw zeros when `None`, i.e. never written = invalid).
    fn image(slot0: Option<u64>, slot1: Option<u64>) -> Vec<u8> {
        let ps = PS as usize;
        let mut buf = vec![0u8; 2 * ps];
        if let Some(t) = slot0 {
            let mut m = MetaPage::create(0, PS, MAP);
            m.txnid = t;
            m.encode(&mut buf[0..ps]).unwrap();
        }
        if let Some(t) = slot1 {
            let mut m = MetaPage::create(1, PS, MAP);
            m.txnid = t;
            m.encode(&mut buf[ps..2 * ps]).unwrap();
        }
        buf
    }

    fn unique_path(tag: &str) -> PathBuf {
        let n = next_env_id();
        PathBuf::from(format!("/virtual/{tag}-{n}"))
    }

    fn open(buf: Vec<u8>, prev: bool, path: PathBuf) -> Result<Env, Error> {
        open_with_backing(path, Box::new(VecBacking(buf)), PS, MAP, prev)
    }

    #[test]
    fn selects_higher_txnid() {
        let env = open(image(Some(3), Some(7)), false, unique_path("hi")).unwrap();
        assert_eq!(env.txnid(), 7);
        assert_eq!(env.page_size(), PS);
        assert_eq!(env.map_size(), MAP);
    }

    #[test]
    fn prev_snapshot_selects_lower_txnid() {
        let env = open(image(Some(3), Some(7)), true, unique_path("prev")).unwrap();
        assert_eq!(env.txnid(), 3);
        assert!(env.is_prev_snapshot());
    }

    #[test]
    fn one_valid_slot_wins_without_prev_snapshot() {
        // Slot 1 never written (all zeros) → invalid; slot 0 wins (REC-2).
        let env = open(image(Some(5), None), false, unique_path("one")).unwrap();
        assert_eq!(env.txnid(), 5);
    }

    #[test]
    fn one_valid_slot_with_prev_snapshot_is_invalid() {
        // REC-2†: one valid slot + PREV_SNAPSHOT → Invalid.
        let e = open(image(Some(5), None), true, unique_path("one-prev")).unwrap_err();
        assert!(matches!(e, Error::Mdb(MdbError::Invalid)));
    }

    #[test]
    fn both_invalid_is_invalid() {
        let e = open(image(None, None), false, unique_path("none")).unwrap_err();
        assert!(matches!(e, Error::Mdb(MdbError::Invalid)));
    }

    #[test]
    fn torn_higher_slot_falls_back_to_older() {
        // Slot 1 has the higher txnid but a corrupted CRC region → invalid;
        // open must fall back to the older, intact slot 0 (REC-2 one-valid).
        let mut buf = image(Some(4), Some(9));
        let ps = PS as usize;
        // Corrupt a byte inside slot 1's CRC-covered region [ps, ps+168).
        buf[ps + 100] ^= 0xFF;
        let env = open(buf, false, unique_path("torn")).unwrap();
        assert_eq!(env.txnid(), 4, "must fall back to the older intact slot");
    }

    #[test]
    fn registry_rejects_second_open() {
        let path = unique_path("dup");
        let env = open(image(Some(1), Some(1)), false, path.clone()).unwrap();
        let e = open(image(Some(1), Some(1)), false, path.clone()).unwrap_err();
        assert!(matches!(e, Error::EnvAlreadyOpened));
        drop(env);
        // After the first env drops, the path is free again.
        let _env2 = open(image(Some(1), Some(1)), false, path).unwrap();
    }

    #[test]
    fn clone_shares_inner_and_close_defers() {
        let env = open(image(Some(2), Some(2)), false, unique_path("clone")).unwrap();
        let c = env.clone();
        assert_eq!(env.handle_count(), 2);
        // prepare_for_closing on one handle: the other keeps the env alive, so
        // the event must NOT have fired yet.
        let ev = env.prepare_for_closing();
        assert!(
            !ev.wait_timeout(Duration::from_millis(0)),
            "close must wait for the surviving clone"
        );
        drop(c);
        // Now the last reference is gone; the event fires.
        ev.wait();
        assert!(ev.wait_timeout(Duration::from_millis(0)));
    }

    #[test]
    fn map_full_boundary_via_env() {
        let env = open(image(Some(1), Some(1)), false, unique_path("mapfull")).unwrap();
        // map_size 1 MiB, psize 4096 → 256 pages, valid pgnos 0..255.
        assert!(!env.inner().would_map_full(255, 1));
        assert!(env.inner().would_map_full(256, 1));
    }
}
