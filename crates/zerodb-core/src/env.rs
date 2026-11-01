//! Environment open/close, meta selection, and the same-process registry
//! (SPEC 02 §3.2, SPEC 06 §1, SPEC 04 §7).
//!
//! This module owns the *behavioral* half of env lifecycle and is deliberately
//! I/O-free so it stays `miri`-clean: the mapped file bytes reach it through the
//! [`Backing`] trait, which the mmap layer (`zerodb-io`) implements for real
//! envs and which tests implement over a plain `Vec<u8>`. All `unsafe` (the
//! mmap) lives behind that trait in `zerodb-io`; this module contains none.
//!
//! What is implemented here: reading both meta slots, validating
//! each via the page-layer predicate ([`crate::page::MetaPage::validate`]), selecting
//! the live snapshot (normal / `PREV_SNAPSHOT`), mapping the SPEC 06 REC error
//! taxonomy onto [`Error`], the process registry with `EnvAlreadyOpened`, the
//! refcounted [`EnvInner`] behind [`Env`] (`Clone`), and deferred close with
//! [`EnvClosingEvent`] (SPEC 04 TXN-52/53). The inner also owns the
//! MVCC reader table and the published-snapshot cell (`crate::readers`,
//! SPEC 04 §3/§4, ADR-0006); the write path lives in `crate::rwtxn`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::Duration;

use crate::cmp::{Comparator, ComparatorError, ComparatorRegistry, KeyCmp};
use crate::error::{Error, MdbError};
use crate::page::geometry::{is_map_full, map_pages};
use crate::page::{
    select_meta, DBRecord, FileTrust, MetaChoice, MetaPage, MetaValidity, META_A_PGNO, META_B_PGNO,
};
use crate::readers::{ReaderTable, SnapshotCell};
use crate::stamps::StampCache;

/// `EINVAL`, the error `mdb_env_set_mapsize` returns while the env has a
/// live write txn (`Env::resize`). `zerodb-core` has no `libc`; the value is
/// 22 on both supported targets (Linux and macOS).
#[cfg(any(target_os = "linux", target_os = "macos"))]
const EINVAL: i32 = 22;

/// A consistent (mapped bytes, logical map size) pair (ADR-0023 amendment 15),
/// from one [`Backing::view`] call. No `Debug`: it would print the whole map.
#[derive(Clone, Copy)]
pub struct MapView<'a> {
    /// The mapped bytes. A mapping is at least `map_size` long, and longer
    /// when the file is (the mapping covers the whole file); a heap test
    /// backing may be shorter.
    pub bytes: &'a [u8],
    /// The logical map size: `MapFull` beyond it, and `MapResized` at a txn
    /// begin whose snapshot lies beyond it.
    pub map_size: u64,
}

/// Read (and, for the write path, page-granular write) access to a
/// memory-mapped (or, in tests, heap) env file.
///
/// The concrete real implementation is `zerodb_io::MmapBacking`, which confines
/// the mmap `unsafe` and guarantees the map is unmapped before the file fd is
/// closed (SPEC 04 TXN-53). Tests supply a `Vec<u8>`-backed implementation so
/// the selection logic runs under `miri` (that implementation is read-only —
/// the write methods keep their erroring defaults, so miri tests exercise
/// mutation and in-txn reads, never commit I/O).
///
/// Implementors must be `Send + Sync`: an [`EnvInner`] is shared across threads
/// through an `Arc` and read txns hand out `Send` borrows.
pub trait Backing: Send + Sync {
    /// The current mapped bytes and logical map size, as one consistent pair
    /// (ADR-0023 amendment 15). A mapped backing's bytes are at least as long
    /// as the logical size; a heap test backing models a file and may be
    /// shorter. A backing whose map can grow publishes a new pair without
    /// ever invalidating bytes it lent earlier; a heap backing returns its
    /// fixed pair.
    fn view(&self) -> MapView<'_>;

    /// The whole mapped region as bytes: [`Backing::view`]'s bytes. Slot 0 is
    /// `[0, page_size)`, slot 1 is `[page_size, 2*page_size)`; data pages
    /// follow. The region may extend past the current file length (ADR-0004
    /// D4: the map covers the full `map_size`); callers only dereference pages
    /// a committed snapshot references, which are always within the file
    /// (SPEC 06 REC-14).
    fn bytes(&self) -> &[u8] {
        self.view().bytes
    }

    /// Set the logical map size to `map_size`, adding a mapping of
    /// max(map_size, 2 × current) when the current one is shorter (on
    /// ENOMEM, retry exactly map_size, else Io). Under WRITE_MAP, grows the
    /// file first, only if it is shorter. Caller holds the in-process writer
    /// lock and, with a lock file, the cross-process one (`Env::resize`), or
    /// runs at open before the backing is shared and only for a size inside
    /// the file (a joiner's seed), so publications are serialized and the
    /// file grows only under both writer locks (ADR-0023 amendment 15).
    ///
    /// The default refuses: the backing cannot grow its map.
    ///
    /// # Errors
    ///
    /// `Unsupported` by default; a backing that grows reports its `mmap` or
    /// file-growth error.
    fn grow_map(&self, map_size: u64) -> std::io::Result<()> {
        let _ = map_size;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "backing cannot grow its map",
        ))
    }

    /// Grow the data file to at least `len` bytes; never shrink it. Called
    /// at a `WRITE_MAP` write-txn begin, with both writer locks held, so a
    /// joiner whose mapping extends past the file end has every page below
    /// its logical map size backed before it writes through the map
    /// (ADR-0023 amendment 15, decision 4).
    ///
    /// The default does nothing: a backing that writes with `pwrite` extends
    /// the file as it writes.
    ///
    /// # Errors
    ///
    /// The `fstat` or `set_len` error of a backing that grows its file.
    fn ensure_file_len(&self, len: u64) -> std::io::Result<()> {
        let _ = len;
        Ok(())
    }

    /// The actual on-disk length of the backing file (`fstat`), for
    /// [`Env::real_disk_size`] (SPEC 00 row 18).
    fn real_disk_size(&self) -> std::io::Result<u64>;

    /// Copy meta slot `slot` (0 or 1) into `buf` (`psize` bytes). Readers of
    /// a lock-file environment (ADR-0023 §4) decode metas from this copy,
    /// never from a borrowed map slice: another process may be rewriting the
    /// slot, and a torn copy is caught by its CRC. The default copies from
    /// [`Backing::bytes`] (heap test backings); file backings use `pread`.
    ///
    /// # Errors
    ///
    /// The read error, or `UnexpectedEof` if the slot is outside the backing.
    fn read_meta_copy(&self, slot: u64, psize: u32, buf: &mut [u8]) -> std::io::Result<()> {
        let ps = psize as usize;
        let start = slot as usize * ps;
        let src = self
            .bytes()
            .get(start..start + ps)
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::UnexpectedEof))?;
        buf[..ps].copy_from_slice(src);
        Ok(())
    }

    /// `dup()` the backing data-file descriptor (SPEC 00 row 22,
    /// [`Env::try_clone_inner_file`]). Requires the env to be a single regular
    /// data file (SPEC 02 §8).
    fn try_clone_file(&self) -> std::io::Result<std::fs::File>;

    /// The path the data file was opened at, if the backing is a file
    /// opened by path ([`Env::data_path`]). The default is `None`: test and
    /// fault-injection backings have no file a raw copy may read directly.
    fn data_path(&self) -> Option<&Path> {
        None
    }

    /// Positioned write of `data` starting at page `pgno` (commit step C2/C4,
    /// SPEC 04 §9). `data.len()` is a multiple of `psize` (one page, or a whole
    /// overflow run). Writing past EOF extends the file. Not durable until
    /// [`Backing::sync_data`].
    ///
    /// The default errors with `Unsupported` — read-only backings (the miri
    /// test backing) never commit.
    ///
    /// # Errors
    ///
    /// Propagates the positioned-write I/O error.
    fn write_at_page(&self, pgno: u64, psize: u32, data: &[u8]) -> std::io::Result<()> {
        let _ = (pgno, psize, data);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "this backing is read-only",
        ))
    }

    /// Whether this backing brokers **in-map dirty pages** (ADR-0021,
    /// SPEC 04 §6.4 TXN-45b): `true` only for the writable-map backing
    /// (`EnvFlags::WRITE_MAP`), where [`Backing::map_dirty_page`] returns
    /// slices and the write txn realizes dirty pages directly in the map.
    /// Default `false`: dirty pages are heap frames written at commit C2
    /// (TXN-45a), the LMDB-parity baseline behavior of every other backing.
    fn dirty_in_map(&self) -> bool {
        false
    }

    /// The brokered mutable slice for the `pages`-page region starting at
    /// page `pgno` of the writable map (ADR-0021; SPEC 04 §6.4 TXN-45b).
    /// `None` when the backing does not broker in-map dirty pages
    /// ([`Backing::dirty_in_map`] is `false`) or the region is out of the
    /// map's bounds.
    ///
    /// # Safety
    ///
    /// **The brokered contract (ADR-0021 B1).**
    /// This mints `&mut [u8]` from `&self`; the signature cannot express the
    /// exclusivity it needs, so the obligation is the caller's. The sole
    /// sanctioned caller is the write txn's `DirtyStore` (AGENTS.md unsafe
    /// policy; see `zerodb_io::MmapWritable::slice_mut` for the full
    /// statement). The caller must guarantee:
    ///
    /// - the region belongs to a page/run the active write txn **allocated**
    ///   (TXN-62 — no live snapshot references it);
    /// - there is a single writer (TXN-6);
    /// - at most one `&mut` into the region is live at a time, tied to
    ///   `&mut` on the dirty store and never stored (TXN-39/41/42);
    /// - no outstanding `&[u8]` view of the map is later read at locations
    ///   this slice wrote — the writer re-derives its whole-map read view
    ///   after every spill (TXN-71, ADR-0021 B2).
    // `unsafe fn` *declaration* only (ADR-0021 B1: no safe fn may mint `&mut`
    // from `&self`); the default body is trivially safe and the one unsafe
    // *call* lives in `crate::dirty::map_mut`, the home the AGENTS.md policy
    // sanctions for the WRITE_MAP in-place brokered map-slice write.
    #[allow(unsafe_code)]
    // clippy cannot see that this is an `unsafe fn` whose documented contract
    // covers exactly what `mut_from_ref` fears (the lint fires on unsafe fns
    // too); ADR-0021 B1's substance is the `unsafe fn` itself.
    #[allow(clippy::mut_from_ref)]
    unsafe fn map_dirty_page(&self, pgno: u64, psize: u32, pages: u64) -> Option<&mut [u8]> {
        let _ = (pgno, psize, pages);
        None
    }

    /// Vectored positioned write of **consecutive** frames starting at
    /// `start_pgno` (commit C2 batching, coalesced writes): `frames` are
    /// page-multiple buffers laid out back-to-back on disk from
    /// `start_pgno * psize`.
    ///
    /// The default delegates to one [`Backing::write_at_page`] per frame —
    /// byte-identical semantics, so the fault-injection backing keeps its
    /// per-frame crash journal and the writable-map backing its per-frame
    /// copy. The plain-file backing overrides this with `pwritev` to batch
    /// syscalls.
    ///
    /// # Errors
    ///
    /// Propagates the positioned-write I/O error.
    fn write_pages_at(&self, start_pgno: u64, psize: u32, frames: &[&[u8]]) -> std::io::Result<()> {
        let mut pgno = start_pgno;
        for f in frames {
            self.write_at_page(pgno, psize, f)?;
            pgno += (f.len() / psize as usize) as u64;
        }
        Ok(())
    }

    /// Durability barrier for previously-written pages (commit steps C3/C5;
    /// `File::sync_data` — ADR-0004 D3 as amended by OQ3: std semantics as-is,
    /// `fdatasync` on Linux).
    ///
    /// # Errors
    ///
    /// Propagates the fsync I/O error (the caller poisons the env, REC-13).
    fn sync_data(&self) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "this backing is read-only",
        ))
    }

    /// The durability barrier the commit pipeline invokes at C3/C5
    /// (SPEC 06 REC-9/REC-12). `async_flush` is honored only by the writable-map
    /// backing (`WRITE_MAP` + `MAP_ASYNC` → `msync(MS_ASYNC)`); the default
    /// heap/pwrite backing ignores it and calls [`Backing::sync_data`]
    /// (`fdatasync`). Whether this method is called *at all* is decided by the
    /// caller from the durability flags (`NO_SYNC`/`NO_META_SYNC` skip it,
    /// SPEC 01 §S6); the backing only chooses the *primitive*
    /// (`fdatasync` vs `msync`), never the policy.
    ///
    /// # Errors
    ///
    /// Propagates the fsync/msync I/O error (the caller poisons the env, REC-13).
    fn sync(&self, async_flush: bool) -> std::io::Result<()> {
        let _ = async_flush;
        self.sync_data()
    }

    /// Whether [`Backing::sync_pages`] flushes only the page ranges it is
    /// given (SPEC 06 REC-12, ranged `WRITE_MAP` barrier). `true` only for
    /// the writable-map backing, whose barrier is an `msync` and can be
    /// limited to the file's used prefix. Default `false`: the commit
    /// pipeline then calls [`Backing::sync`] and skips building the ranges.
    fn ranged_sync(&self) -> bool {
        false
    }

    /// The commit barrier limited to `ranges` (SPEC 06 REC-12): make durable
    /// at least every byte of the `pages`-page runs starting at each
    /// `start_pgno` of `ranges` (`(start_pgno, pages)`, ascending and
    /// non-overlapping). The commit pipeline calls it only when
    /// [`Backing::ranged_sync`] is `true`, with the used prefix
    /// `[0, next_pgno)` at C3 and the meta slot it wrote at C5.
    /// `async_flush` has the meaning it has for [`Backing::sync`].
    ///
    /// The default ignores `ranges` and calls [`Backing::sync`], which
    /// flushes a superset.
    ///
    /// # Errors
    ///
    /// Propagates the fsync/msync I/O error (the caller poisons the env, REC-13).
    fn sync_pages(
        &self,
        async_flush: bool,
        psize: u32,
        ranges: &[(u64, u64)],
    ) -> std::io::Result<()> {
        let _ = (psize, ranges);
        self.sync(async_flush)
    }
}

// ---------------------------------------------------------------------------
// Published snapshot + commit hooks (SPEC 04 TXN-18/19, §9; ADR-0004 D3)
// ---------------------------------------------------------------------------

/// An immutable committed snapshot: the roots and geometry of one committed
/// state (SPEC 04 TXN-18). Readers `Arc`-clone the env's published snapshot at
/// begin and never re-read a durable meta page (the slot a pinned txnid lived
/// in is overwritten two commits later, TXN-63).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// The commit point of this snapshot.
    pub txnid: u64,
    /// File high-water as of this commit (`next_pgno - 1`, SPEC 05 GC-15).
    pub last_pg: u64,
    /// The map size this snapshot's meta records (ADR-0023 amendment 15). A
    /// commit persists `max(base.map_size, the committing process's logical
    /// map size)`, so it never lowers the recorded size.
    pub map_size: u64,
    /// Root/stats of the main/catalog DB.
    pub main_db: DBRecord,
    /// Root/stats of the free (GC) DB.
    pub free_db: DBRecord,
    /// This snapshot's meta free-list annex (SPEC 02 §3 format v2, ADR-0022;
    /// SPEC 05 §2a): the pages freed by txn `txnid`, exactly the `fl_ids` of
    /// its meta. Pinned **in the snapshot** — the meta slot `txnid & 1` is
    /// overwritten by txn `txnid + 2` even while this snapshot stays pinned
    /// (TXN-63), so holders (`copy`, the next writer) must never re-read the
    /// slot. `Arc<[u64]>` keeps `Snapshot` cheap to clone.
    pub free_annex: std::sync::Arc<[u64]>,
}

impl Snapshot {
    /// The snapshot a validated meta page describes, with `annex` as the
    /// meta's free-list annex ids (read via [`MetaPage::read_annex`] from the
    /// same validated slot buffer; `annex.len()` must equal `meta.fl_count`).
    #[must_use]
    pub fn from_meta(meta: &MetaPage, annex: Vec<u64>) -> Snapshot {
        debug_assert_eq!(annex.len(), meta.fl_count as usize);
        Snapshot {
            txnid: meta.txnid,
            last_pg: meta.last_pg,
            map_size: meta.map_size,
            main_db: meta.main_db,
            free_db: meta.free_db,
            free_annex: annex.into(),
        }
    }
}

/// A crash-injection point between commit-pipeline steps (SPEC 04 §9 H0–H4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookPoint {
    /// After C1 (`freelist_save`): nothing on disk yet.
    H0,
    /// After C2 (dirty pages written, **not** fsynced).
    H1,
    /// After C3 (`fsync(data)`).
    H2,
    /// After C4 (meta written to slot `N & 1`, **not** fsynced).
    H3,
    /// After C5 (`fsync(meta)`): txn `N` durable.
    H4,
}

/// A commit-pipeline observer (ADR-0004 D3/OQ5: **always compiled**, default
/// absent, so the crash-tested pipeline is byte-for-byte the shipped one).
/// The crash-consistency harnesses install hooks that kill/tear at a chosen
/// [`HookPoint`]; the commit smoke test aborts the process at each point in turn.
pub trait CommitHook: Send + Sync {
    /// Called between commit steps, at `point`. May abort/kill the process.
    fn at(&self, point: HookPoint);
}

/// The single-writer lock (SPEC 04 TXN-6), thread-agnostic by construction:
/// an `occupied` flag under a short-critical-section mutex plus a condvar.
///
/// It deliberately does **not** hand out the `Mutex`'s own `MutexGuard` for
/// the write txn's lifetime: `std::sync::MutexGuard` is `!Send` (and on some
/// platforms unlocking from a foreign thread aborts), while `zerodb::RwTxn`
/// must be genuinely `Send` — heed's `WithoutTls` mode moves write txns
/// across threads, and the txn's drop (commit or abort) may run on any
/// thread. Here the guard only *clears the flag* under a fresh short lock
/// taken on whichever thread drops it, which is sound on every platform.
#[derive(Debug)]
struct WriterLock {
    occupied: Mutex<WriterSlot>,
    cv: Condvar,
}

/// The state behind [`WriterLock`]'s mutex.
#[derive(Debug, Default)]
struct WriterSlot {
    /// `true` while a write txn is live.
    occupied: bool,
    /// Threads parked in [`WriterLock::acquire`]'s wait. The release notifies
    /// only when this is non-zero: on Linux, std's futex `Condvar` makes every
    /// `notify_one` a `futex_wake` system call even with nobody waiting, which
    /// cost one syscall per write txn (`env/txn/rw_empty_commit`).
    waiters: u32,
    /// What one write txn leaves for the next (issue #8): the dirty-frame
    /// pool (LMDB's `me_dpages`), the emptied containers a write txn fills,
    /// and the snapshot the last one published ([`crate::rwtxn::WriterCarry`]).
    /// Kept under the writer lock's own mutex, as LMDB keeps `me_dpages`
    /// under its writer mutex: the carry moves out with the acquire and back
    /// with the release, so it costs no lock operation of its own (a
    /// separate `Mutex` for the frame pool made every write txn, empty ones
    /// included, ~36 % slower). `None` until the first write txn ends.
    carry: Option<Box<crate::rwtxn::WriterCarry>>,
}

/// Ownership of the writer slot; releases it on drop, from any thread.
/// Held by `RwTxn` for its whole life (TXN-6).
pub(crate) struct WriterGuard<'env> {
    lock: &'env WriterLock,
    /// The carry: taken from the slot at acquire, handed to the write txn,
    /// given back at txn end, stored at release.
    carry: Option<Box<crate::rwtxn::WriterCarry>>,
    /// The cross-process writer lock, when this guard also holds it
    /// (ADR-0023 §2; set by [`EnvInner::lock_writer_shared`]). Released
    /// before the in-process slot on drop.
    shared: Option<&'env dyn crate::shared::SharedRegion>,
}

impl WriterGuard<'_> {
    /// The carry taken with the writer slot, allocated on the first write
    /// txn of the env.
    pub(crate) fn carry(&mut self) -> &mut crate::rwtxn::WriterCarry {
        self.carry.get_or_insert_with(Box::default)
    }

    /// Move the carry out, for the new write txn. O(1): the `Box` moves.
    pub(crate) fn take_carry(&mut self) -> Box<crate::rwtxn::WriterCarry> {
        self.carry.take().unwrap_or_default()
    }

    /// The carry to store back into the slot when this guard releases it.
    pub(crate) fn give_back_carry(&mut self, carry: Box<crate::rwtxn::WriterCarry>) {
        self.carry = Some(carry);
    }
}

impl WriterLock {
    fn new() -> WriterLock {
        WriterLock {
            occupied: Mutex::new(WriterSlot::default()),
            cv: Condvar::new(),
        }
    }

    /// Claim the writer slot if it is free; `None` while a write txn of
    /// this env is live. Never waits (`Env::resize`, ADR-0023 amendment 15).
    fn try_acquire(&self) -> Option<WriterGuard<'_>> {
        // Poison recovery as in `acquire`.
        let mut g = self
            .occupied
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if g.occupied {
            return None;
        }
        g.occupied = true;
        let carry = g.carry.take();
        Some(WriterGuard {
            lock: self,
            carry,
            shared: None,
        })
    }

    /// Block until the writer slot is free, then claim it.
    fn acquire(&self) -> WriterGuard<'_> {
        // Poison recovery: the lock guards no data — the dirty set lives in
        // the RwTxn and is dropped during unwind (TXN-60 implicit abort) — so
        // clearing a poison is sound and keeps the env usable after a writer
        // panic. The flag mutex is only ever held for the flag flip below,
        // but the same recovery applies.
        let mut g = self
            .occupied
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while g.occupied {
            // Registered under the mutex before `wait` releases it, so a
            // release that reads `waiters == 0` under the same mutex cannot be
            // racing a thread about to park: no lost wakeup.
            g.waiters += 1;
            g = self
                .cv
                .wait(g)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            g.waiters -= 1;
        }
        g.occupied = true;
        let carry = g.carry.take();
        WriterGuard {
            lock: self,
            carry,
            shared: None,
        }
    }
}

impl Drop for WriterGuard<'_> {
    fn drop(&mut self) {
        // Cross-process lock first, then the in-process slot: a thread of this
        // process woken by the slot release then competes fairly with other
        // processes for the shared lock.
        if let Some(region) = self.shared.take() {
            region.unlock_writer();
        }
        let mut g = self
            .lock
            .occupied
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        g.occupied = false;
        g.carry = self.carry.take();
        // One waiter at most can make progress (single writer), so
        // `notify_one` suffices; drop the flag lock before notify is not
        // required for correctness (the waiter re-checks under the lock).
        // Skipped when nobody is parked, which is the common, uncontended case.
        if g.waiters > 0 {
            self.lock.cv.notify_one();
        }
    }
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

/// Env-level durability / write-mode flags (SPEC 01 Table 1, §S6/§S7).
/// Selected once at open and immutable for the env's life. The commit pipeline
/// reads them to decide which fsync/msync barriers run (SPEC 06 REC-9/REC-12);
/// the backing implementation chooses the *primitive* (`fdatasync` vs `msync`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DurabilityFlags {
    /// `MDB_RDONLY` — env-level read-only: `write_txn`/`force_sync` → `EACCES`
    /// (SPEC 01 Table 1, TXN-8).
    pub read_only: bool,
    /// `MDB_NOSYNC` — skip **both** data (C3) and meta (C5) fsync on commit
    /// (SPEC 01 §S6). Durability restored by `force_sync`.
    pub no_sync: bool,
    /// `MDB_NOMETASYNC` — fsync data (C3) but skip the meta fsync (C5) this
    /// commit (SPEC 01 §S6, REC-10).
    pub no_meta_sync: bool,
    /// `MDB_MAPASYNC` — with `WRITE_MAP`, use `msync(MS_ASYNC)` for the commit
    /// flushes (SPEC 01 §S6, REC-9). No effect without `write_map`.
    pub map_async: bool,
    /// `MDB_WRITEMAP` — writes go through a writable mmap (SPEC 01 §S7,
    /// SPEC 04 §6.4). Recorded here for introspection; the actual writable map
    /// lives in the `zerodb-io` backing.
    pub write_map: bool,
}

/// The `EACCES` error a write attempt on a read-only env returns (SPEC 01
/// Table 1, TXN-8). Matches the fork's `mdb_txn_begin` → `EACCES`, which heed
/// surfaces as `Error::Io(PermissionDenied)` (os error 13) — so ZeroDB returns
/// the identical `Io(PermissionDenied)` for oracle taxonomy parity.
fn eacces_error() -> Error {
    Error::Io(std::io::Error::from_raw_os_error(13))
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
    /// The runtime map size is not stored here: it lives in the backing's
    /// published view with the mapping it fits in (ADR-0023 amendment 15).
    page_size: u32,
    /// The meta selected **at open** (higher-txnid, or older under
    /// `PREV_SNAPSHOT`). Read once to seed the published snapshot (TXN-18);
    /// steady-state reads use [`EnvInner::snapshot`], never this field, because
    /// commits advance past it.
    meta: MetaPage,
    /// Whether this env was opened with `PREV_SNAPSHOT` (SPEC 01 §S5).
    prev_snapshot: bool,
    /// Close signal, shared with any outstanding [`EnvClosingEvent`].
    closing: Arc<SignalEvent>,
    /// The single-writer lock (SPEC 04 TXN-6). Guards no data — the write
    /// txn's state lives in the `RwTxn` — it only serializes writers. A
    /// thread-agnostic flag+condvar lock (see [`WriterLock`]) so the guard —
    /// and with it `RwTxn` — is `Send`.
    write_mutex: WriterLock,
    /// The page buffer a commit encodes its meta into (commit step C4),
    /// reused across commits. Every byte is zero while it sits here; empty
    /// until the first commit. Only the writer touches it, so its lock is
    /// never contended; it is taken only by commits that write a meta, which
    /// keeps it off the begin path and out of the `RwTxn` (an empty write
    /// txn pays nothing for it).
    meta_buf: Mutex<Vec<u8>>,
    /// The published-snapshot cell (SPEC 04 TXN-18; ADR-0006 Option B): the
    /// immutable `Arc<Snapshot>` behind a
    /// bounded-O(1)-critical-section mutex, plus the mirroring SeqCst
    /// `commit_point` atomic that carries the whole lock-free pin protocol
    /// (TXN-17/19/20). Published in the TXN-19 order (swap the object, then
    /// store the commit point).
    snap_cell: SnapshotCell,
    /// Commit-pipeline crash hooks (ADR-0004 D3; default `None` = no-op).
    commit_hook: Mutex<Option<Arc<dyn CommitHook>>>,
    /// REC-13 fsync-gate: set (Release) when a commit fsync fails; checked
    /// (Acquire) at every write-txn begin and commit. A poisoned env still
    /// serves read txns from their pinned snapshots.
    poisoned: AtomicBool,
    /// The MVCC reader table (SPEC 04 §4; ADR-0006): `max_readers` 128-byte
    /// slots, in this process's memory or, in a lock-file environment, in
    /// the shared lock file (ADR-0023 §3). Readers claim/pin/release
    /// slots lock-free; the writer's GC gate scans it (`oldest_live_reader`,
    /// TXN-21).
    reader_table: ReaderTable,
    /// The named-database slot table (SPEC 02 §6.2, ADR-0024): slot ↔ name
    /// and each slot's lifetime. Records resolve from the catalog (TXN-10).
    handles: crate::handles::HandleTable,
    /// Env-level durability / write-mode flags (SPEC 01 §S6/§S7).
    /// Immutable after open; read by the commit pipeline and by `write_txn` /
    /// `force_sync`.
    durability: DurabilityFlags,
    /// Per-named-database key comparators (SPEC 03 §2.0). Empty
    /// unless a caller opened a database through one of the
    /// `*_with_comparator` entry points; the main/catalog tree and the GC tree
    /// are never represented here and are always memcmp.
    ///
    /// **Not persisted** — see `crate::cmp` for the reopen hazard
    /// (docs/DIVERGENCES.md, comparator persistence).
    comparators: ComparatorRegistry,
    /// Catalog capacity (`max_dbs`), immutable after open. Duplicated out of
    /// the `named` registry so read txns can size their dbi-indexed record
    /// table without taking the registry lock.
    max_dbs: u32,
    /// Page-validation policy (ADR-0014), fixed at open. Every txn copies it
    /// into its validated-pages memo at begin.
    file_trust: FileTrust,
    /// Env-wide cache of validated page versions (ADR-0018), borrowed by
    /// every plain read txn. Never consulted under a trusting policy.
    stamp_cache: StampCache,
    /// A write txn's dirty limit in pages (SPEC 04 TXN-68, ADR-0017), fixed
    /// at open: past it the txn spills pages to the file.
    dirty_limit: u64,
    /// Sequential-writes default (ADR-0015), fixed at open: whether a write
    /// txn keeps a rightmost-leaf finger for a tree with no override.
    sequential_writes: bool,
    /// Per-database sequential-writes overrides (ADR-0015): slot 0 is the
    /// main DB, slot `1 + dbi` a named DB. [`SEQ_FOLLOW`] = the env default,
    /// [`SEQ_OFF`] / [`SEQ_ON`] = forced. Runtime state, never persisted.
    sequential_overrides: Box<[AtomicU8]>,
    /// The cross-process region when opened with a lock file (ADR-0023);
    /// `None` keeps the single-process path unchanged.
    shared: Option<Arc<dyn crate::shared::SharedRegion>>,
    /// Whether this process still holds the first-opener election past its
    /// open (ADR-0023 amendment 19): set at open for a `PREV_SNAPSHOT` open
    /// that initialized the lock file, cleared by its first commit, which
    /// finishes the election instead of publishing. Read and written only
    /// at open and by the write-lock holder.
    holds_election: AtomicBool,
    /// A durable commit newer than the published snapshot (amendment 19):
    /// the commit that finished a held election (never published, its
    /// txnid reused by the next commit), or, in a joiner, the newest meta
    /// on disk when it was newer than `T` at open. Reported by
    /// [`Env::info`], [`Env::stat`], [`Env::txnid`] and [`Env::resize`] while
    /// it is newer than the published snapshot, as LMDB reports the newest
    /// meta on disk; cleared once a publish reaches it.
    unpublished: Mutex<Option<Unpublished>>,
    /// Whether [`EnvInner::unpublished`] may hold a value: lets the common
    /// case skip its mutex.
    has_unpublished: AtomicBool,
}

/// A durable commit newer than the published snapshot (ADR-0023
/// amendment 19), kept in [`EnvInner`] for `info`, `stat` and `resize`.
struct Unpublished {
    snap: Arc<Snapshot>,
    /// This process's own unpublished first `PREV_SNAPSHOT` commit, which
    /// [`Env::txnid`] also reports; `false` for a newer meta a joiner found
    /// on disk at open.
    own_commit: bool,
}

/// [`EnvInner`] sequential-writes override: follow the env default.
const SEQ_FOLLOW: u8 = 0;
/// [`EnvInner`] sequential-writes override: forced off.
const SEQ_OFF: u8 = 1;
/// [`EnvInner`] sequential-writes override: forced on.
const SEQ_ON: u8 = 2;

impl std::fmt::Debug for EnvInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvInner")
            .field("id", &self.id)
            .field("path", &self.path)
            .field("page_size", &self.page_size)
            .field("map_size", &self.map_size())
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

    /// The runtime map size (SPEC 00 row 20, `Env::info().map_size`): the
    /// logical size of the backing's current view (ADR-0023 amendment 15).
    #[must_use]
    pub fn map_size(&self) -> u64 {
        self.backing_ref().view().map_size
    }

    /// The txnid of the current live snapshot (the commit point; advances on
    /// every commit, SPEC 04 TXN-19). SeqCst load inside the cell — see
    /// `crate::readers` for the TXN-17/19/20 StoreLoad pairing argument. In a
    /// lock-file environment it is the shared published txnid `T`, so it
    /// includes other processes' commits (ADR-0023 §4).
    ///
    /// One exception (ADR-0023 amendment 19, SPEC 04 TXN-67): in a
    /// `PREV_SNAPSHOT` opener with a lock file, between its first and second
    /// commit, it is the first commit's txnid. That commit is durable but not
    /// published, so no read txn can pin it: read txns still begin at `T`. In
    /// a process that joined, it stays `T` even when [`Env::info`] reports a
    /// newer meta found on disk at open.
    #[must_use]
    pub fn txnid(&self) -> u64 {
        let t = match &self.shared {
            // SeqCst: the same total order as the T stores (TXN-17/19).
            Some(region) => region.published_txnid().load(Ordering::SeqCst),
            None => self.snap_cell.commit_point(),
        };
        // ADR-0023 amendment 19: a `PREV_SNAPSHOT` opener's own unpublished
        // first commit, while it is newer than `T`. A joiner's newer meta on
        // disk is left out: `txnid` stays the published txnid there.
        self.newer_unpublished(t, true).map_or(t, |u| u.txnid)
    }

    /// Whether this process still holds the first-opener election past its
    /// open (ADR-0023 amendment 19).
    pub(crate) fn holds_election(&self) -> bool {
        // Relaxed: set before the env is shared, and afterwards read and
        // written only by the holder of the writer locks, whose
        // acquire/release orders every access.
        self.holds_election.load(Ordering::Relaxed)
    }

    /// The unpublished first commit of a `PREV_SNAPSHOT` opener
    /// (ADR-0023 amendment 19), if its txnid is above `txnid`.
    /// With `own_only`, only this process's own unpublished commit counts,
    /// not a newer meta a joiner found on disk at open.
    fn newer_unpublished(&self, txnid: u64, own_only: bool) -> Option<Arc<Snapshot>> {
        // Acquire: pairs with the Release store made after the cell is
        // filled. The flag only lets this call skip the mutex; the value
        // itself is read under the mutex, which orders it. A stale `false`
        // answers as a call made just before the cell was filled (or just
        // after it was cleared) would, which is a valid order on any memory
        // model, ARM's included.
        if !self.has_unpublished.load(Ordering::Acquire) {
            return None;
        }
        let mut cell = self.unpublished.lock().expect("unpublished cell poisoned");
        match cell.as_ref() {
            Some(u) if u.snap.txnid > txnid => {
                (u.own_commit || !own_only).then(|| Arc::clone(&u.snap))
            }
            Some(_) => {
                // A published snapshot reached it (`T` never decreases), so
                // it can never be newer again.
                *cell = None;
                // Release: under the mutex, after the cell is emptied.
                self.has_unpublished.store(false, Ordering::Release);
                None
            }
            None => None,
        }
    }

    /// `snap`, or the unpublished first commit of a `PREV_SNAPSHOT` opener
    /// when that one is newer (ADR-0023 amendment 19): the newest meta on
    /// disk, which LMDB's `mdb_env_info`, `mdb_env_stat` and
    /// `mdb_env_set_mapsize` read.
    fn or_newer_unpublished(&self, snap: Arc<Snapshot>) -> Arc<Snapshot> {
        self.newer_unpublished(snap.txnid, false).unwrap_or(snap)
    }

    /// Finish a commit while this process holds the election
    /// (ADR-0023 amendment 19). The commit is durable; it is not published,
    /// as LMDB's first `MDB_PREVSNAPSHOT` commit leaves the shared txnid on
    /// the older meta (`OBS_PREV_SNAPSHOT_COMMIT_TXNIDS`): `T` and the
    /// published-snapshot cell keep the older snapshot, so the next writer
    /// begins from it and reuses this txnid (TXN-2). Then the election is
    /// finished, letting the processes that wait in it join.
    ///
    /// # Errors
    ///
    /// The I/O error of [`SharedRegion::finish_open`]. The election stays
    /// held and the next commit tries again.
    ///
    /// [`SharedRegion::finish_open`]: crate::shared::SharedRegion::finish_open
    pub(crate) fn finish_held_election(&self, committed: Arc<Snapshot>) -> Result<(), Error> {
        {
            let mut cell = self.unpublished.lock().expect("unpublished cell poisoned");
            *cell = Some(Unpublished {
                snap: committed,
                own_commit: true,
            });
            // Release: after the cell is filled; see `newer_unpublished`.
            self.has_unpublished.store(true, Ordering::Release);
        }
        let region = self
            .shared
            .as_ref()
            .expect("the election is held only with a lock file");
        region.finish_open().map_err(Error::Io)?;
        // Relaxed: see `holds_election`.
        self.holds_election.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// The meta selected at open, possibly unpublished in a joiner: the
    /// durable meta page this process read once when it opened the
    /// environment (TXN-18), kept only to seed the published snapshot cell.
    /// In a lock-file environment (ADR-0023) a process that *joins* an
    /// environment another process already has open may select an
    /// older meta than the one the first opener is about to publish, or the
    /// shared published txnid `T` may already be ahead of it by the time this
    /// call returns — so this is never current once another writer has run.
    ///
    /// **Do not read roots or txnids from this for anything that must be
    /// correct across processes or across commits** (the main/free-DB roots,
    /// `txnid`, `last_pg`): use [`EnvInner::snapshot`] for the process-local
    /// published view, or [`EnvInner::current_snapshot`] for the one that
    /// also follows another process's commits. This accessor exists only for
    /// open-time/debugging use (geometry recorded at the moment this process
    /// opened, e.g. in [`std::fmt::Debug`]); it goes stale after the first
    /// commit, by this process or any other.
    #[must_use]
    pub fn meta(&self) -> &MetaPage {
        &self.meta
    }

    /// `Arc`-clone the current published snapshot (SPEC 04 TXN-18). The
    /// clone keeps the `(txnid, roots)` alive for the caller's
    /// life regardless of later commits. Critical section: one refcount bump
    /// (ADR-0006 Option B).
    #[must_use]
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.snap_cell.clone_snapshot()
    }

    /// Publish a freshly committed snapshot (commit step C6) in the TXN-19
    /// order: (1) swap the `Arc<Snapshot>` into the cell, then (2) store the
    /// commit point `SeqCst`. Object-before-counter guarantees a reader that
    /// sees the new counter can load the matching (or newer) roots. See
    /// `crate::readers::SnapshotCell::publish` for the ordering comments.
    ///
    /// In a lock-file environment the shared published txnid `T` is stored
    /// last, which is the moment other processes begin to see the commit
    /// (ADR-0023 §4).
    pub(crate) fn publish_snapshot(&self, snap: Arc<Snapshot>) {
        let txnid = snap.txnid;
        self.snap_cell.publish(snap);
        if let Some(region) = &self.shared {
            // ADR-0023 §4: T is stored after the meta write (and its fsync in
            // the durable modes, C4/C5), and after the cache swap (TXN-19
            // object-before-counter). SeqCst pairs with every reader's
            // publish-and-verify and with the writer's SeqCst slot scan,
            // across processes. Only the cross-process writer-lock holder
            // advances T.
            region.published_txnid().store(txnid, Ordering::SeqCst);
        }
    }

    /// The commit's reusable meta-page buffer (commit step C4), locked for
    /// the meta write. The buffer is all zero on entry, or empty before the
    /// first commit; the commit sizes it to the page, encodes into it, writes
    /// it, and clears the prefix it encoded before the guard drops.
    pub(crate) fn meta_buf(&self) -> std::sync::MutexGuard<'_, Vec<u8>> {
        // Poison recovery: a writer that panicked mid-encode may have left
        // bytes behind, so a recovered buffer is discarded (the commit
        // reallocates a zeroed one).
        self.meta_buf.lock().unwrap_or_else(|p| {
            let mut g = p.into_inner();
            g.clear();
            g
        })
    }

    /// Acquire the single-writer lock (SPEC 04 TXN-6/7): blocks until the
    /// current writer finishes; never errors. The returned guard releases
    /// from whatever thread drops it ([`WriterLock`]), so `RwTxn` is `Send`.
    /// The writer-panic poison-recovery policy lives in
    /// [`WriterLock::acquire`].
    pub(crate) fn lock_writer(&self) -> WriterGuard<'_> {
        self.write_mutex.acquire()
    }

    /// Take the cross-process writer lock on top of the in-process one
    /// (ADR-0023 §2); a no-op without a lock file. The guard releases it on
    /// drop, before the in-process slot. A writer that took the lock over
    /// from a dead owner then frees dead processes' reader slots (see
    /// [`EnvInner::lock_writer_region`]).
    ///
    /// # Errors
    ///
    /// The I/O error of the lock call, or of a liveness probe during the
    /// takeover reap. After a failed lock call the guard holds only the
    /// in-process lock. After a failed takeover reap it holds both locks, and
    /// dropping it (as the failing write-txn begin does) releases both, as
    /// LMDB's lock recovery releases the writer mutex when its reader check
    /// fails.
    #[inline]
    pub(crate) fn lock_writer_shared<'a>(
        &'a self,
        guard: &mut WriterGuard<'a>,
    ) -> Result<(), Error> {
        match &self.shared {
            None => Ok(()),
            Some(region) => self.lock_writer_region(region.as_ref(), guard),
        }
    }

    /// The lock-file arm of [`EnvInner::lock_writer_shared`]. Out of line so
    /// the write-txn path without a lock file stays as small as it was
    /// before lock files existed.
    ///
    /// A writer that took the lock over from a dead owner then frees dead
    /// processes' reader slots, as LMDB's writer does when it recovers the
    /// lock from a dead owner on Linux (ADR-0023 amendment 12).
    ///
    /// # Errors
    ///
    /// The I/O error of the lock call, or of a liveness probe during the
    /// takeover reap. In the second case the guard already records the
    /// cross-process lock, so dropping it (as the failing write txn does)
    /// releases the lock, as LMDB's lock recovery releases the writer mutex
    /// when its reader check fails.
    #[inline(never)]
    fn lock_writer_region<'a>(
        &'a self,
        region: &'a dyn crate::shared::SharedRegion,
        guard: &mut WriterGuard<'a>,
    ) -> Result<(), Error> {
        let took_over = region.lock_writer().map_err(Error::Io)?;
        guard.shared = Some(region);
        if took_over {
            self.clear_stale_readers()?;
        }
        Ok(())
    }

    /// Free the reader slots of dead processes (`mdb_reader_check`,
    /// ADR-0023 §3). Returns the number of slots freed (the fork's
    /// `mdb_reader_check` also counts slots, ADR-0023 amendment 12); always 0
    /// without a lock file, where every slot belongs to this process.
    ///
    /// # Errors
    ///
    /// The I/O error of a liveness probe. Nothing has been freed then: every
    /// probe runs before any slot is touched.
    pub(crate) fn clear_stale_readers(&self) -> Result<usize, Error> {
        #[cfg(not(loom))]
        if let Some(region) = &self.shared {
            let n = self
                .reader_table
                .reap_dead(region.pid(), |p| region.is_alive(p))
                .map_err(Error::Io)?;
            return Ok(n as usize);
        }
        Ok(0)
    }

    /// Whether a failed commit fsync poisoned the env (SPEC 06 REC-13).
    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        // Acquire: pairs with the Release in `poison` so a writer that
        // observes the flag also observes everything before the failure.
        self.poisoned.load(Ordering::Acquire)
    }

    /// Poison the env after a failed durability barrier (SPEC 06 REC-13).
    pub(crate) fn poison(&self) {
        // Release: see `is_poisoned`.
        self.poisoned.store(true, Ordering::Release);
    }

    /// Install (or clear) the commit-pipeline crash hook (ADR-0004 D3). Test
    /// infrastructure for the commit smoke test and the crash harness; the
    /// default (`None`) makes every hook site a no-op.
    pub fn set_commit_hook(&self, hook: Option<Arc<dyn CommitHook>>) {
        *self.commit_hook.lock().expect("hook cell poisoned") = hook;
    }

    /// Fire the crash hook at `point`, if one is installed. The `Arc` is
    /// cloned out of the cell before the call so a hook that re-enters the env
    /// cannot deadlock on the cell.
    pub(crate) fn run_hook(&self, point: HookPoint) {
        let hook = self.commit_hook.lock().expect("hook cell poisoned").clone();
        if let Some(h) = hook {
            h.at(point);
        }
    }

    /// The backing for commit I/O (write pages / fsync). Panics only if called
    /// after close — impossible while any `Env`/txn borrow exists.
    pub(crate) fn backing_ref(&self) -> &dyn Backing {
        self.backing
            .as_deref()
            .expect("backing present while the env is open")
    }

    /// Pin a snapshot for a new read txn (SPEC 04 TXN-10 steps 1–3):
    /// claim a reader-table slot, run the TXN-17 SeqCst publish-and-verify
    /// loop against the commit point, and clone the published snapshot
    /// (adopting a newer one if a commit raced the clone — the TXN-17 tail).
    /// Returns the pinned snapshot and the owned slot index; the caller
    /// (`RoTxn`) releases the slot at drop via [`EnvInner::release_reader`].
    ///
    /// In a lock-file environment the commit point is the shared published
    /// txnid `T` and the snapshot comes from [`EnvInner::snapshot_at`]
    /// (ADR-0023 §4).
    ///
    /// # Errors
    ///
    /// [`MdbError::ReadersFull`] when every slot is occupied (TXN-16). In a
    /// lock-file environment also [`MdbError::MapResized`] when the snapshot
    /// at `T` lies beyond this process's logical map size, and
    /// [`MdbError::Invalid`] when it cannot be decoded or lies beyond the
    /// file.
    pub(crate) fn pin_reader(&self) -> Result<(Arc<Snapshot>, u32), Error> {
        // loom builds have no shared reader table (`ReaderTable::new_shared`
        // is not compiled), and the open path refuses a region there.
        #[cfg(not(loom))]
        if let Some(region) = &self.shared {
            return self.pin_shared(region.as_ref());
        }
        self.snap_cell
            .pin(&self.reader_table)
            .ok_or(Error::Mdb(MdbError::ReadersFull))
    }

    /// The lock-file arm of [`EnvInner::pin_reader`]: claim a shared slot,
    /// run TXN-17 against `T`, then resolve the snapshot for the verified
    /// txnid. The slot keeps the verified pin while the meta is read, so a
    /// retry after the slot moved on is conservative (GC holds back, TXN-20).
    #[cfg(not(loom))]
    fn pin_shared(
        &self,
        region: &dyn crate::shared::SharedRegion,
    ) -> Result<(Arc<Snapshot>, u32), Error> {
        let slot = self
            .reader_table
            .claim()
            .ok_or(Error::Mdb(MdbError::ReadersFull))?;
        // TXN-17 publish-and-verify against the shared T, then the snapshot
        // for the verified txnid; `None` means meta slot `t & 1` moved on.
        match self
            .reader_table
            .pin_against(slot, region.published_txnid(), META_RETRY_LIMIT, |t| {
                self.snapshot_at(t)
            }) {
            Ok(Some(snap)) => Ok((snap, slot)),
            Ok(None) => {
                self.reader_table.release(slot);
                Err(Error::Mdb(MdbError::Invalid))
            }
            Err(e) => {
                self.reader_table.release(slot);
                Err(e)
            }
        }
    }

    /// The snapshot for published txnid `t`: the cached one when it matches
    /// (the single-process fast path), else decoded from the data file and
    /// cached. `None` when meta slot `t & 1` has moved on or was torn.
    ///
    /// The decoded snapshot is checked against this process's map and the
    /// current file length before it is used or cached (SPEC 06 REC-1a). The
    /// cache hit skips both checks: a cached snapshot passed them already.
    fn snapshot_at(&self, t: u64) -> Result<Option<Arc<Snapshot>>, Error> {
        let cached = self.snap_cell.clone_snapshot();
        if cached.txnid == t {
            return Ok(Some(cached));
        }
        let backing = self.backing_ref();
        let Some(snap) = read_snapshot_at(backing, self.page_size, t)? else {
            return Ok(None);
        };
        check_snapshot_backed(backing, self.page_size, &snap)?;
        let snap = Arc::new(snap);
        self.snap_cell.install_if_newer(Arc::clone(&snap));
        Ok(Some(snap))
    }

    /// The latest published snapshot. In a lock-file environment it follows
    /// the shared `T`, so it includes other processes' commits (ADR-0023 §4);
    /// otherwise it is [`EnvInner::snapshot`]. The writer's base.
    ///
    /// # Errors
    ///
    /// In a lock-file environment, [`MdbError::MapResized`] when the snapshot
    /// at `T` lies beyond this process's logical map size, and
    /// [`MdbError::Invalid`] when it cannot be decoded or lies beyond the
    /// file.
    #[inline]
    pub(crate) fn current_snapshot(&self) -> Result<Arc<Snapshot>, Error> {
        self.writer_base(None)
    }

    /// [`EnvInner::current_snapshot`] for a writer holding the writer lock,
    /// given the snapshot the previous write txn of this process ended with
    /// (its published one, or its unchanged base; [`crate::rwtxn::WriterCarry`]).
    /// When that is still the latest it is returned as is: no lock of the
    /// snapshot cell and no reference-count change, which a write txn
    /// otherwise pays at every begin and end.
    ///
    /// Published txnids only grow — a commit publishes a txnid once, and a
    /// commit that fails or finishes a held election publishes nothing — so
    /// a cached snapshot whose txnid is the latest published one is the
    /// latest snapshot.
    ///
    /// # Errors
    ///
    /// As [`EnvInner::current_snapshot`].
    #[inline]
    pub(crate) fn writer_base(
        &self,
        cached: Option<Arc<Snapshot>>,
    ) -> Result<Arc<Snapshot>, Error> {
        match &self.shared {
            None => match cached {
                // The commit point is stored by the publishing writer, whose
                // writer-lock release happens-before this writer's acquire,
                // so this load sees the latest publish.
                Some(s) if s.txnid == self.snap_cell.commit_point() => Ok(s),
                _ => Ok(self.snapshot()),
            },
            Some(region) => self.current_snapshot_shared(region.as_ref(), cached),
        }
    }

    /// The lock-file arm of [`EnvInner::writer_base`]: the snapshot at the
    /// shared `T`, `cached` when that is it. Out of line so the default
    /// write-txn path stays small.
    #[inline(never)]
    fn current_snapshot_shared(
        &self,
        region: &dyn crate::shared::SharedRegion,
        cached: Option<Arc<Snapshot>>,
    ) -> Result<Arc<Snapshot>, Error> {
        for _ in 0..META_RETRY_LIMIT {
            // SeqCst: pairs with the committing writer's SeqCst store of T
            // (the same total order as TXN-17/19).
            let t = region.published_txnid().load(Ordering::SeqCst);
            if let Some(s) = cached.as_ref() {
                if s.txnid == t {
                    return Ok(cached.expect("matched above"));
                }
            }
            if let Some(s) = self.snapshot_at(t)? {
                return Ok(s);
            }
            std::thread::yield_now();
        }
        Err(Error::Mdb(MdbError::Invalid))
    }

    /// The newest published snapshot, without the map check: in a lock-file
    /// environment the snapshot committed as `T`, decoded from a `pread`
    /// copy of its meta slot (no map access); otherwise the process-local
    /// cell. For `Env::resize`, which holds both writer locks, so `T` cannot
    /// move while it runs (ADR-0023 amendment 15, step 3).
    ///
    /// # Errors
    ///
    /// [`MdbError::Invalid`] when the published slot never decodes, or its
    /// meta is corrupt; the I/O error of the meta read.
    fn newest_snapshot_unchecked(&self) -> Result<Arc<Snapshot>, Error> {
        let Some(region) = &self.shared else {
            return Ok(self.snapshot());
        };
        for _ in 0..META_RETRY_LIMIT {
            // SeqCst: pairs with the committing writer's SeqCst store of T.
            let t = region.published_txnid().load(Ordering::SeqCst);
            if let Some(s) = read_snapshot_at(self.backing_ref(), self.page_size, t)? {
                return Ok(Arc::new(s));
            }
            std::thread::yield_now();
        }
        Err(Error::Mdb(MdbError::Invalid))
    }

    /// Release a reader-table slot (SPEC 04 TXN-18a: `Release` store of
    /// `RDR_FREE`). Called from `RoTxn::drop`, from whatever thread the
    /// `Send` txn ended up on (TXN-13).
    pub(crate) fn release_reader(&self, slot: u32) {
        self.reader_table.release(slot);
    }

    /// The reader table's fixed slot count (`MDB_envinfo::me_maxreaders`):
    /// `max_readers` as configured at open, or, in a lock-file environment,
    /// the count recorded in the lock file, which a joining process adopts
    /// (ADR-0023 §1 as amended).
    #[must_use]
    pub fn max_readers(&self) -> u32 {
        self.reader_table.capacity()
    }

    /// `MDB_envinfo::me_numreaders` parity: the **high-water** reader-slot
    /// count, which never decreases when a read txn ends, over every
    /// process's readers in a lock-file environment. See
    /// [`crate::readers::ReaderTable::num_readers`] for the fork evidence and
    /// docs/DIVERGENCES.md (`me_numreaders` semantics).
    #[must_use]
    pub fn num_readers(&self) -> u32 {
        self.reader_table.num_readers()
    }

    /// Reader slots **currently** occupied — a ZeroDB extension, and the
    /// number [`EnvInner::num_readers`] misleadingly looks like.
    /// A concurrently-sampled diagnostic count, not a synchronization point.
    #[must_use]
    pub fn live_readers(&self) -> u32 {
        self.reader_table.in_use()
    }

    /// The ordering in force for the database `sel` addresses
    /// (SPEC 03 §2.0).
    ///
    /// [`DbSel::Main`] is **always** [`KeyCmp::Default`]: the main tree is
    /// also the named-DB catalog, whose keys are DB names and whose values are
    /// engine-internal `DBRecord` bytes. A named DB is whatever was registered
    /// for its dbi, defaulting to memcmp.
    #[must_use]
    pub fn comparator_for(&self, sel: crate::rotxn::DbSel) -> KeyCmp<'_> {
        match sel {
            crate::rotxn::DbSel::Main => KeyCmp::Default,
            crate::rotxn::DbSel::Named(dbi) => self.comparators.get(dbi),
        }
    }

    /// Register a comparator for named database `dbi`. Called from
    /// the `*_with_comparator` open paths, never directly by users.
    pub(crate) fn register_comparator(
        &self,
        dbi: u32,
        cmp: Box<dyn Comparator>,
    ) -> Result<(), ComparatorError> {
        self.comparators.register(dbi, cmp)
    }

    /// Whether any custom comparator is registered on this env.
    /// Gates the memcmp-only compacting-copy path (SPEC 03 §2.0).
    #[must_use]
    pub fn has_custom_comparator(&self) -> bool {
        self.comparators.any_custom()
    }

    /// Per-slot reader introspection — `mdb_reader_list`. See
    /// [`Env::reader_list`] for the full contract; this is the raw form.
    #[must_use]
    pub fn reader_list(&self) -> Vec<ReaderEntry> {
        // Sample the commit point *once* so every age in the returned vector
        // is relative to the same reference point. Sampling per entry would
        // make the ages mutually inconsistent for no benefit.
        let now = self.txnid();
        self.reader_table
            .list()
            .into_iter()
            .map(|(slot, txnid, pid)| ReaderEntry {
                slot,
                txnid,
                pid,
                // `saturating_sub`, not a subtraction: a reader can publish a
                // pin *newer* than the commit point we sampled a moment ago
                // (it read a commit that landed in between), which would
                // otherwise underflow. Reporting age 0 for "as new as or newer
                // than the reference point" is the truthful answer.
                age: txnid.map(|t| now.saturating_sub(t)),
            })
            .collect()
    }

    /// The smallest snapshot txnid any live reader has pinned, if any: the
    /// SeqCst full-table scan of SPEC 04 TXN-20 (two-case proof quoted at the
    /// scan site, `crate::readers::ReaderTable::oldest`). The GC gate folds it
    /// with `writer_txnid − 1` (TXN-20/21) and caches it per write txn
    /// (TXN-22; ADR-0006 decision 6).
    #[must_use]
    pub(crate) fn oldest_live_reader(&self) -> Option<u64> {
        self.reader_table.oldest()
    }

    /// The named-database slot table (ADR-0024).
    pub(crate) fn handles(&self) -> &crate::handles::HandleTable {
        &self.handles
    }

    /// Whether a free slot may take another database's name: not when a
    /// custom comparator is registered for it, since that registration is
    /// per slot and permanent (ADR-0024).
    pub(crate) fn slot_reusable(&self, slot: u32) -> bool {
        matches!(self.comparators.get(slot), KeyCmp::Default)
    }

    /// The catalog capacity this env was opened with (`max_dbs`). Every dbi
    /// the registry hands out is below it. Lock-free.
    #[must_use]
    pub(crate) fn max_dbs(&self) -> u32 {
        self.max_dbs
    }

    /// The name for a named-DB dbi index (`DbSel::Named`), if the index is
    /// assigned. Cloned out so no registry lock is held by the caller.
    #[must_use]
    pub(crate) fn named_name(&self, dbi: u32) -> Option<Box<[u8]>> {
        self.handles.name(dbi)
    }

    /// Whether this env was opened on the previous (older) snapshot.
    #[must_use]
    pub fn is_prev_snapshot(&self) -> bool {
        self.prev_snapshot
    }

    /// The env-level durability / write-mode flags (SPEC 01 §S6/§S7).
    #[must_use]
    pub fn durability(&self) -> DurabilityFlags {
        self.durability
    }

    /// The page-validation policy this env was opened with (ADR-0014).
    #[must_use]
    pub fn file_trust(&self) -> FileTrust {
        self.file_trust
    }

    /// The env-wide cache of validated page versions (ADR-0018).
    pub(crate) fn stamp_cache(&self) -> &StampCache {
        &self.stamp_cache
    }

    /// Test/diagnostics hook (ADR-0018; like
    /// [`crate::rwtxn::RwTxn::spills`]): whether the env-wide
    /// validated-pages cache currently holds exactly the page version
    /// `(pgno, kind, stamp)` — `branch` selects the kind half of the key.
    /// Observes only; never part of the stable API.
    #[doc(hidden)]
    #[must_use]
    pub fn stamp_cache_has(&self, pgno: u64, branch: bool, stamp: u64) -> bool {
        let kind = if branch {
            crate::stamps::StampKind::Branch
        } else {
            crate::stamps::StampKind::Leaf
        };
        self.stamp_cache.contains(pgno, kind, stamp)
    }

    /// A write txn's dirty limit in pages (SPEC 04 TXN-68, ADR-0017).
    #[must_use]
    pub fn dirty_limit(&self) -> u64 {
        self.dirty_limit
    }

    /// The env's sequential-writes default (ADR-0015).
    #[must_use]
    pub fn sequential_writes_default(&self) -> bool {
        self.sequential_writes
    }

    /// Whether the main DB (`None`) or named DB `dbi` uses the sequential-
    /// writes fast path: its override if set, else the env default
    /// (ADR-0015).
    #[must_use]
    pub fn sequential_writes_for(&self, dbi: Option<u32>) -> bool {
        let slot = dbi.map_or(0, |d| d as usize + 1);
        // Ordering: `Relaxed` — the value is a performance hint that selects
        // between two paths with identical results, it publishes no other
        // data, and any value a racing read sees is a valid choice.
        match self
            .sequential_overrides
            .get(slot)
            .map_or(SEQ_FOLLOW, |a| a.load(Ordering::Relaxed))
        {
            SEQ_ON => true,
            SEQ_OFF => false,
            _ => self.sequential_writes,
        }
    }

    /// Set or clear (`None`) the sequential-writes override of the main DB
    /// (`dbi = None`) or named DB `dbi` (ADR-0015). Write txns that start
    /// after the call use it; an out-of-range dbi (impossible for a handle
    /// the engine produced) is ignored.
    pub fn set_sequential_writes(&self, dbi: Option<u32>, on: Option<bool>) {
        let slot = dbi.map_or(0, |d| d as usize + 1);
        let v = match on {
            None => SEQ_FOLLOW,
            Some(false) => SEQ_OFF,
            Some(true) => SEQ_ON,
        };
        if let Some(a) = self.sequential_overrides.get(slot) {
            // Ordering: `Relaxed` — see `sequential_writes_for`.
            a.store(v, Ordering::Relaxed);
        }
    }

    /// Whether the env is read-only (`MDB_RDONLY`, SPEC 01 Table 1).
    #[must_use]
    pub fn is_read_only(&self) -> bool {
        self.durability.read_only
    }

    /// Force durability of all prior commits — `mdb_env_sync(env, 1)`
    /// (SPEC 01 §S6). Equivalent to [`EnvInner::sync`]`(true)`, and the only
    /// form heed exposes (SPEC 00 second table).
    ///
    /// # Errors
    ///
    /// See [`EnvInner::sync`].
    pub fn force_sync(&self) -> Result<(), Error> {
        self.sync(true)
    }

    /// Explicit environment sync — full `mdb_env_sync(env, force)` parity
    /// (SPEC 01 §S6). heed exposes only the `force = true` form
    /// ([`EnvInner::force_sync`]); `force = false` is a ZeroDB extension.
    ///
    /// The fork's `mdb_env_sync0` decides three things, reproduced exactly:
    ///
    /// 1. **`MDB_RDONLY` → `EACCES`**, before anything else, for either value
    ///    of `force`.
    /// 2. **Whether to flush at all**: only if `force || !NO_SYNC`. So
    ///    `sync(false)` on a `NO_SYNC` env is a **silent no-op returning
    ///    `Ok(())`** — it does *not* make prior commits durable. This is the
    ///    one behavioral difference between the two `force` values, and the
    ///    reason `force_sync` is the durability-restoring call.
    /// 3. **Which primitive**: under `WRITE_MAP`, `msync(MS_ASYNC)` iff
    ///    `MAP_ASYNC && !force`, else `MS_SYNC`; without `WRITE_MAP`,
    ///    `fdatasync`. I.e. `force` also downgrades `MAP_ASYNC` to a real
    ///    barrier. Expressed here as the `async_flush` argument to
    ///    [`Backing::sync`], which owns the primitive choice.
    ///
    /// A call that does flush always issues the barrier even when nothing is
    /// pending, matching LMDB (which likewise always calls through).
    ///
    /// # Errors
    ///
    /// - [`Error::Io`] (`EACCES`) on a read-only env (SPEC 01 Table 1).
    /// - [`Error::Io`] if the env is already poisoned (SPEC 06 REC-13).
    /// - [`Error::Io`] on an `msync`/`fsync` failure — which also **poisons**
    ///   the env (REC-13), exactly like a failed commit barrier.
    pub fn sync(&self, force: bool) -> Result<(), Error> {
        // (1) RDONLY is rejected before the NO_SYNC test, as in mdb_env_sync0.
        if self.durability.read_only {
            return Err(eacces_error());
        }
        if self.is_poisoned() {
            return Err(Error::Io(std::io::Error::other(
                "environment poisoned by a failed durability barrier (SPEC 06 REC-13)",
            )));
        }
        // (2) `force || !NO_SYNC` — otherwise no flush is issued at all.
        if !force && self.durability.no_sync {
            return Ok(());
        }
        // (3) MS_ASYNC only when MAP_ASYNC is in effect and this is not a
        // forced sync. `Backing::sync` ignores `async_flush` unless the
        // backing is the writable map, matching the `MDB_WRITEMAP` guard.
        let async_flush = !force && self.durability.map_async && self.durability.write_map;
        // Serialize with the writer: a concurrent commit must not interleave its
        // pipeline with an explicit sync. The guard is released on return.
        let _guard = self.lock_writer();
        match self.backing_ref().sync(async_flush) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.poison(); // REC-13
                Err(e.into())
            }
        }
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

    /// The path the data file was opened at (SPEC 00 row 17: a raw copy
    /// clones the file it names, after checking it is still the same file).
    #[must_use]
    pub fn data_path(&self) -> Option<&Path> {
        self.backing.as_ref().and_then(|b| b.data_path())
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
    /// (SPEC 02 §8, `MdbError::MapFull`). Exposed so the geometry ceiling is
    /// testable at the env level; the write path checks `is_map_full` itself.
    #[must_use]
    pub fn would_map_full(&self, next_pgno: u64, n: u64) -> bool {
        is_map_full(next_pgno, n, map_pages(self.map_size(), self.page_size))
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
        // Release the cross-process region (ADR-0023) before the close event
        // too: dropping the last reference closes the lock file, which
        // releases its record locks and this process's claim on its path. A
        // waiter woken by the event may reopen at once, and would otherwise
        // find the lock file still open (`EnvAlreadyOpened`). The reader table
        // holds the only other reference; write txns borrow the region and
        // cannot outlive this `EnvInner`.
        self.reader_table.release_region();
        let shared = self.shared.take();
        // With the reader table's reference released, this is the last one,
        // so the drop below really closes the lock file before the close
        // event fires. (Unit tests keep their own reference to the in-memory
        // test region they open with, so the check is for every other build.)
        #[cfg(not(test))]
        if let Some(region) = &shared {
            debug_assert_eq!(
                Arc::strong_count(region),
                1,
                "the lock-file region outlives its environment"
            );
        }
        drop(shared);
        self.closing.signal();
    }
}

/// A cheap, cloneable environment handle (SPEC 00 row 25, SPEC 04 TXN-50).
///
/// `Clone` bumps the refcount; it never reopens the file. The underlying
/// [`EnvInner`] is torn down only when the last clone (and every outstanding
/// txn) drops.
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

    /// An identity for this environment, equal across every clone of the same
    /// `Env` and distinct from every other open environment (the address of the
    /// shared inner state). Consumers use it to assert that a database handle
    /// and a transaction belong to the same environment, as heed does with its
    /// `MDB_env` pointer.
    #[must_use]
    pub fn ident(&self) -> usize {
        Arc::as_ptr(self.inner()) as *const () as usize
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

    /// Whether this process opened the environment alone: it won the lock
    /// file's first-opener election, so no other process had the environment
    /// open at that moment (ADR-0023 amendment 18). Always `true` without a
    /// lock file, where the environment is single-process.
    ///
    /// The answer describes the open only: another process can join later.
    /// `zerodb-tools load` uses it to refuse a destination that another
    /// process has open.
    #[must_use]
    pub fn opened_exclusively(&self) -> bool {
        self.inner
            .shared
            .as_ref()
            .is_none_or(|r| r.is_initializer())
    }

    /// Whether this environment runs with a lock file shared with other
    /// processes (ADR-0023 amendment 17). `false` when it was opened without
    /// one, and for a read-only environment on read-only media, where no
    /// lock file can be created and the open proceeds without one, as LMDB
    /// does.
    #[must_use]
    pub fn has_lock_file(&self) -> bool {
        self.inner.shared.is_some()
    }

    /// Change the map size, as `mdb_env_set_mapsize` on an open env
    /// (ADR-0023 amendment 15).
    ///
    /// - `0` adopts the map size recorded in the newest committed meta
    ///   (`OBS_SET_MAPSIZE_ZERO_ADOPTS`), the answer to a
    ///   [`MdbError::MapResized`] begin.
    /// - A size below the newest snapshot's data rounds up to exactly
    ///   `(last_pg + 1) * page_size`, silently, as LMDB does.
    /// - A smaller size than the current mapping lowers only the logical
    ///   size: nothing is ever unmapped before the env closes.
    ///
    /// Live read txns do not block it, and every value they borrowed stays
    /// valid (grow-only mappings). In a lock-file environment it takes the
    /// cross-process writer lock, waiting like a write begin, so no commit
    /// lands while it rounds up. LMDB takes no lock here: its
    /// `mdb_env_set_mapsize` returns while another process holds a write txn
    /// (`OBS_SET_MAPSIZE_OTHER_PROCESS_WRITER`; docs/DIVERGENCES.md D-023).
    /// Under `WRITE_MAP` it grows the data file to the new size, never
    /// shrinking it.
    ///
    /// # Errors
    ///
    /// - [`Error::Io`] with raw OS error `EINVAL` (22) while a write txn of
    ///   this env is live (`OBS_SET_MAPSIZE_WITH_WRITE_TXN`).
    /// - [`Error::Io`] (`InvalidInput`) for a requested size above
    ///   [`MAX_MAP_SIZE`](crate::page::geometry::MAX_MAP_SIZE);
    ///   [`MdbError::Invalid`] when `0` adopts a meta size above it (a corrupt
    ///   meta), or the newest meta cannot be read.
    /// - The I/O error of the cross-process lock, of the `mmap`, or of the
    ///   file growth.
    pub fn resize(&self, new_size: u64) -> Result<(), Error> {
        let inner = self.inner();
        // OBS_SET_MAPSIZE_WITH_WRITE_TXN: EINVAL while a write txn is live.
        let mut guard = inner
            .write_mutex
            .try_acquire()
            .ok_or_else(|| Error::Io(std::io::Error::from_raw_os_error(EINVAL)))?;
        // Step 2: the cross-process writer lock (a no-op without a lock
        // file), through the write begin's path, which reaps after a
        // dead-owner takeover. Every error below drops `guard`, releasing
        // both locks.
        inner.lock_writer_shared(&mut guard)?;
        // ADR-0023 amendment 19: the newest meta on disk, as LMDB's
        // `mdb_env_set_mapsize`, including an unpublished first commit.
        let snap = inner.or_newer_unpublished(inner.newest_snapshot_unchecked()?);
        let min = snap
            .last_pg
            .checked_add(1)
            .and_then(|n| n.checked_mul(u64::from(inner.page_size())))
            .ok_or(Error::Mdb(MdbError::Invalid))?;
        // OBS_SET_MAPSIZE_ZERO_ADOPTS: 0 adopts the newest meta's size.
        let (size, from_meta) = if new_size == 0 {
            (snap.map_size, true)
        } else {
            (new_size, false)
        };
        // LMDB rounds up silently.
        let size = size.max(min);
        if size > crate::page::geometry::MAX_MAP_SIZE {
            return Err(if from_meta {
                Error::Mdb(MdbError::Invalid)
            } else {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "map size ({size}) exceeds the supported maximum {}",
                        crate::page::geometry::MAX_MAP_SIZE
                    ),
                ))
            });
        }
        inner.backing_ref().grow_map(size).map_err(Error::Io)?;
        // The guard's drop releases both writer locks.
        drop(guard);
        Ok(())
    }

    /// Environment info (SPEC 00 rows 20/60; `mdb_env_info`). The struct is
    /// complete — see [`EnvInfo`] for the field-by-field
    /// `MDB_envinfo` mapping and which values are format-specific.
    ///
    /// All fields are read from the **live published snapshot** plus the reader
    /// table; no transaction is opened, so this never blocks a writer and never
    /// consumes a reader slot. In a lock-file environment the snapshot follows
    /// the shared published txnid `T` ([`EnvInner::current_snapshot`]), so
    /// `last_pgno`/`last_txnid` reflect another process's commit as soon as it
    /// publishes (`OBS_INFO_SEES_OTHER_COMMIT`, ADR-0023 §4). This call stays
    /// infallible: if the snapshot at `T` cannot be read right now (for
    /// example [`MdbError::MapResized`], when `T` has moved past a commit that
    /// grew the map beyond this process's current mapping), it falls back to
    /// this process's own cached snapshot ([`EnvInner::snapshot`]) rather than
    /// surfacing the error, which can under-report a page/txnid that another
    /// process has already published. That fallback has no dedicated test.
    ///
    /// It can also report a commit that no read txn sees yet, as LMDB's
    /// `mdb_env_info` reports the newest meta on disk (ADR-0023 amendment 19,
    /// SPEC 04 TXN-67): a `PREV_SNAPSHOT` opener's unpublished first commit,
    /// and, in a process that joined, a meta newer than `T` that it found on
    /// disk at open (such an opener's first commit, or another process's
    /// commit between its meta write and its `T` store). Either is reported
    /// until `T` reaches it, and `last_txnid` can then exceed
    /// [`Env::txnid`]. The metas are not re-read per call, so a newer meta
    /// written after this process opened and never published (a writer that
    /// died before storing `T`) is not reported; LMDB's would report it.
    #[must_use]
    pub fn info(&self) -> EnvInfo {
        let snap = self
            .inner
            .current_snapshot()
            .unwrap_or_else(|_| self.inner.snapshot());
        // ADR-0023 amendment 19: an unpublished first `PREV_SNAPSHOT` commit
        // is the newest meta on disk, which LMDB's `mdb_env_info` reports.
        let snap = self.inner.or_newer_unpublished(snap);
        EnvInfo {
            map_size: self.inner.map_size(),
            last_pgno: snap.last_pg,
            last_txnid: snap.txnid,
            max_readers: self.inner.max_readers(),
            num_readers: self.inner.num_readers(),
            live_readers: self.inner.live_readers(),
        }
    }

    /// Environment-level statistics (`mdb_env_stat`): the [`EnvStat`] of the
    /// **main** DB of the live snapshot.
    ///
    /// Like [`Env::info`] this reads the published snapshot directly rather
    /// than opening a read txn, so it neither blocks nor occupies a reader
    /// slot. See [`EnvStat`] for the `MDB_stat` field mapping; the page counts
    /// are ZeroDB-format values (own on-disk format) and must not be compared to LMDB's.
    /// In a lock-file environment this follows the shared published txnid `T`
    /// the same way [`Env::info`] does, so another process's commit is visible
    /// as soon as it publishes (`OBS_STAT_SEES_OTHER_COMMIT`), with the same
    /// fall back to the cached snapshot if the read at `T` fails.
    ///
    /// Like [`Env::info`], it reports an unpublished `PREV_SNAPSHOT` first
    /// commit, or a joiner's newer meta found on disk at open, until `T`
    /// reaches it, so its counts can describe a commit no read txn sees yet
    /// (ADR-0023 amendment 19, SPEC 04 TXN-67).
    #[must_use]
    pub fn stat(&self) -> EnvStat {
        let snap = self
            .inner
            .current_snapshot()
            .unwrap_or_else(|_| self.inner.snapshot());
        // As in `info` (ADR-0023 amendment 19).
        let snap = self.inner.or_newer_unpublished(snap);
        let rec = snap.main_db;
        EnvStat {
            page_size: self.inner.page_size(),
            depth: rec.depth,
            branch_pages: rec.branch_pages,
            leaf_pages: rec.leaf_pages,
            overflow_pages: rec.overflow_pages,
            entries: rec.entries,
        }
    }

    /// List the environment's **occupied** reader slots —
    /// ZeroDB's answer to `mdb_reader_list`, which heed does not expose at all.
    ///
    /// One [`ReaderEntry`] per slot that is currently owned by a read
    /// transaction, in slot order. Free slots are omitted, exactly as
    /// `mdb_reader_list` skips slots with `mr_pid == 0`: presence in the
    /// returned vector *is* the liveness answer, and `max_readers() -
    /// reader_list().len()` is the number of free slots. Each entry gives the
    /// slot index, the pinned snapshot txnid, its age, and, in an environment
    /// opened with a lock file, the owner's PID (LMDB's `pid` column). There
    /// is no thread column: a `RoTxn` is `Send`, so its thread is not a
    /// stable property.
    ///
    /// # This is a sample, not a snapshot
    ///
    /// The reader table is lock-free (SPEC 04 §4): slots are read one at a
    /// time, so **entries can already be stale by the time this returns**, and
    /// the vector may correspond to no single instant — a read txn can end (or
    /// begin) between two slot loads. This is inherent to lock-free
    /// introspection, not an implementation shortcut: the only way to get a
    /// linearizable answer would be to stop the world, which would make the
    /// diagnostic itself a correctness hazard. Treat the result as a
    /// monitoring sample. Nothing in the engine consults it; the GC gate uses
    /// a different scan (TXN-20) whose staleness is provably one-directional.
    ///
    /// Ages are all computed against a single sampled commit point
    /// ([`Env::txnid`]); see [`ReaderEntry::age`].
    #[must_use]
    pub fn reader_list(&self) -> Vec<ReaderEntry> {
        self.inner.reader_list()
    }

    /// Reclaim reader slots abandoned by dead processes —
    /// `mdb_reader_check`. Returns the number of slots freed.
    ///
    /// LMDB's reader table lives in a shared `lock.mdb` mapped by every
    /// process that opens the env, so a process that dies without ending its
    /// read txn leaves a slot pinned forever, stalling GC for everyone else.
    /// `mdb_reader_check` exists to scan those slots and free the ones whose
    /// `mr_pid` no longer names a live process.
    ///
    /// Without a lock file ZeroDB is **single-process** (see
    /// docs/DIVERGENCES.md): the reader table is plain process memory, every
    /// slot is owned by a `RoTxn` in *this* address space, and a `RoTxn`
    /// releases its slot in `Drop` — including while unwinding from a panic.
    /// The only way to leak a slot is to `mem::forget` a live `RoTxn`, which is
    /// exactly as unrecoverable as leaking any other resource and is not what
    /// `mdb_reader_check` is for. If this process dies, the table dies with it,
    /// so this path always returns `0`, which is the correct answer, not a
    /// stub.
    ///
    /// With the lock file on (the default; `zerodb::EnvOpenOptions::lock_file`,
    /// heed's `NO_LOCK` off, ADR-0023), the reader table is shared
    /// across processes, and a process that dies inside a read txn leaves its
    /// slot pinned. This method then probes the owner of every occupied slot
    /// and frees the slots of processes that no longer have the environment
    /// open (ADR-0023 §3). Like `mdb_reader_check`, it counts **slots**, not
    /// processes: one dead process that held two slots counts 2.
    ///
    /// As in LMDB, the engine reaps on its own in only one other place: a
    /// writer that takes over the writer lock from a dead owner (Linux)
    /// frees dead readers' slots too. Beginning a read txn, committing, and
    /// joining an environment that another process has open never reap
    /// another process's slots, and a full reader table fails with
    /// `ReadersFull` without reaping; call this method to recover the slots.
    /// (The first process to open the environment wipes the whole table, as
    /// no other process has it open; and a joining process frees the slots
    /// left under its own PID by a dead predecessor, since no probe can tell
    /// those from its own, ADR-0023 amendment 9.)
    ///
    /// # Errors
    ///
    /// The I/O error of a liveness probe. Nothing has been freed then: every
    /// probe runs before any slot is touched.
    pub fn clear_stale_readers(&self) -> Result<usize, Error> {
        self.inner.clear_stale_readers()
    }

    /// Whether any custom key comparator is registered on this environment
    /// Gates the memcmp-only compacting-copy path
    /// (SPEC 03 §2.0).
    #[must_use]
    pub fn has_custom_comparator(&self) -> bool {
        self.inner.has_custom_comparator()
    }

    /// The canonical directory path (SPEC 00 row 21).
    #[must_use]
    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    /// The txnid of the live snapshot: in a lock-file environment, the
    /// shared published txnid `T`, which includes other processes' commits.
    ///
    /// In a `PREV_SNAPSHOT` opener with a lock file, between its first and
    /// second commit, it is the first commit's txnid, which is durable but
    /// not published: read txns still pin the older snapshot (ADR-0023
    /// amendment 19, SPEC 04 TXN-67). In a process that joined, it stays at
    /// `T` even while [`Env::info`] and [`Env::stat`] report a newer meta.
    #[must_use]
    pub fn txnid(&self) -> u64 {
        self.inner.txnid()
    }

    /// Whether opened on the previous snapshot (`PREV_SNAPSHOT`).
    #[must_use]
    pub fn is_prev_snapshot(&self) -> bool {
        self.inner.is_prev_snapshot()
    }

    /// Whether the env is read-only (`MDB_RDONLY`, SPEC 01 Table 1).
    #[must_use]
    pub fn is_read_only(&self) -> bool {
        self.inner.is_read_only()
    }

    /// The env-level durability / write-mode flags (SPEC 01 §S6/§S7).
    #[must_use]
    pub fn durability(&self) -> DurabilityFlags {
        self.inner.durability()
    }

    /// The page-validation policy this env was opened with (ADR-0014).
    #[must_use]
    pub fn file_trust(&self) -> FileTrust {
        self.inner.file_trust()
    }

    /// Force durability of all prior commits — `mdb_env_sync(env, 1)`
    /// (SPEC 01 §S6). Restores durability under `NO_SYNC` / `NO_META_SYNC` /
    /// `MAP_ASYNC`. The form heed exposes; equivalent to [`Env::sync`]`(true)`.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] (`EACCES`) on a read-only env; [`Error::Io`] on an
    /// `msync`/`fsync` failure (which poisons the env, REC-13).
    pub fn force_sync(&self) -> Result<(), Error> {
        self.inner.force_sync()
    }

    /// Explicit environment sync with full `mdb_env_sync(env, force)` parity
    /// `force = false` is a ZeroDB extension — heed exposes
    /// only [`Env::force_sync`]. See [`EnvInner::sync`] for the exact
    /// three-way semantics (`EACCES`, the `force || !NO_SYNC` gate, and the
    /// `MAP_ASYNC` downgrade).
    ///
    /// # Errors
    ///
    /// See [`EnvInner::sync`].
    pub fn sync(&self, force: bool) -> Result<(), Error> {
        self.inner.sync(force)
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

    /// The path the data file was opened at, or `None` for a backing that
    /// is not a file opened by path. The file at that path may since have
    /// been renamed or replaced: compare it with
    /// [`Env::try_clone_inner_file`] before relying on it.
    #[must_use]
    pub fn data_path(&self) -> Option<&Path> {
        self.inner.data_path()
    }

    /// Number of strong references to the shared inner (this handle included).
    /// Diagnostic; the deferred-close contract does not depend on it.
    #[must_use]
    pub fn handle_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    /// Install (or clear) the commit-pipeline crash hook (ADR-0004 D3). See
    /// [`EnvInner::set_commit_hook`]; test infrastructure (commit smoke test,
    /// crash harness).
    pub fn set_commit_hook(&self, hook: Option<Arc<dyn CommitHook>>) {
        self.inner.set_commit_hook(hook);
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

/// Environment info (SPEC 00 rows 20/60; `mdb_env_info` / `MDB_envinfo`).
/// Every field is populated, though `map_size` is the only one any consumer
/// reads.
///
/// ## LMDB `MDB_envinfo` field mapping
///
/// | `MDB_envinfo` | here | note |
/// |---|---|---|
/// | `me_mapaddr` | *(absent)* | LMDB-structural: the address the map is fixed at under `MDB_FIXEDMAP`. ZeroDB never maps at a fixed address (`MDB_FIXEDMAP` is WON'T, SPEC 01), so the value would always be null and the field is **not exposed** here. The `heed-zerodb` mirror keeps a `map_addr` field for signature parity and reports null. |
/// | `me_mapsize` | [`EnvInfo::map_size`] | identical meaning. |
/// | `me_last_pgno` | [`EnvInfo::last_pgno`] | identical *meaning* (id of the last used page), but the **value is format-specific**: it counts ZeroDB pages of ZeroDB's layout, not LMDB's. Never compare it cross-engine. |
/// | `me_last_txnid` | [`EnvInfo::last_txnid`] | identical meaning and value domain: the id of the last committed txn. Comparable cross-engine. |
/// | `me_maxreaders` | [`EnvInfo::max_readers`] | identical: the reader-table size (configured, or adopted from the lock file by a joining process). |
/// | `me_numreaders` | [`EnvInfo::num_readers`] | identical — **including LMDB's surprise**: it is a *high-water mark*, not a live count (see docs/DIVERGENCES.md). In a lock-file environment it counts every process's readers, as LMDB's shared counter does (`OBS_NUMREADERS_OTHER_PROCESS`): it is derived from the shared reader slots, so it restarts from 0 only when a first opener initializes the lock file. |
/// | *(none)* | [`EnvInfo::live_readers`] | **ZeroDB extension**: the genuinely-live occupied-slot count that `me_numreaders` looks like but is not. |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvInfo {
    /// The configured/runtime map size, in bytes (`me_mapsize`).
    pub map_size: u64,
    /// Id of the last used page (`me_last_pgno`) — the file high-water of the
    /// live snapshot. Format-specific value (see the struct docs).
    pub last_pgno: u64,
    /// Id of the last committed transaction (`me_last_txnid`).
    pub last_txnid: u64,
    /// Reader-table size (`me_maxreaders`): configured, or adopted from the
    /// lock file by a process that joined a running environment.
    pub max_readers: u32,
    /// `me_numreaders`: the **high-water** reader-slot count — the maximum
    /// number of simultaneously live readers ever observed. It does **not**
    /// decrease when a read txn ends; that is LMDB's actual behavior, verified
    /// against the fork and reproduced here. In a lock-file environment it
    /// covers every process's readers, as LMDB's does. For the live count,
    /// use [`EnvInfo::live_readers`].
    pub num_readers: u32,
    /// **ZeroDB extension** (no `MDB_envinfo` counterpart): reader slots
    /// currently occupied, by any process in a lock-file environment. A
    /// concurrently sampled diagnostic count.
    pub live_readers: u32,
}

/// One occupied reader-table slot, as reported by [`Env::reader_list`]
/// (one `MDB_reader` row in `mdb_reader_list` output).
///
/// A **ZeroDB extension**: heed exposes no reader introspection whatsoever, so
/// there is no signature to mirror. Compared to LMDB's row there is no
/// `thread` column, since a `RoTxn` is `Send` and the owning thread is not a
/// stable property worth reporting. The `pid` column is reported in an
/// environment opened with a lock file ([`ReaderEntry::pid`]); without one
/// every reader is in this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReaderEntry {
    /// The slot's index in the reader table, in `0..max_readers`. Stable for
    /// the life of the read txn that owns it, then reusable by another reader.
    pub slot: u32,
    /// The snapshot txnid this reader has pinned — the transaction whose data
    /// it sees, and the value that holds GC back (SPEC 04 TXN-20).
    ///
    /// `None` means the slot is **claimed but not yet pinned**: a read txn has
    /// won the slot's CAS and is inside the publish-and-verify loop (SPEC 04
    /// §4.3), a window of a few instructions. Such a reader constrains nothing
    /// yet, and by the time you act on this value it is certainly pinned.
    pub txnid: Option<u64>,
    /// How many commits the environment has published since the pinned
    /// snapshot: `Env::txnid() - txnid`. `None` exactly when [`ReaderEntry::txnid`]
    /// is.
    ///
    /// This is the number that matters operationally — a large and growing age
    /// identifies the long-lived read transaction that is preventing free-page
    /// reclamation. It is computed against **one** commit point sampled for
    /// the whole [`Env::reader_list`] call, and saturates at 0 rather than
    /// underflowing if a reader pinned a commit that landed after that sample.
    pub age: Option<u64>,
    /// The PID of the process that owns the slot, in an environment opened
    /// with a lock file (ADR-0023 amendment 13). `None` without a lock file
    /// (every reader is in this process) and for a slot being claimed or
    /// released. LMDB's `mdb_reader_list` prints the same column.
    pub pid: Option<u32>,
}

/// Environment-level statistics (`mdb_env_stat` / `MDB_stat` over the **main**
/// DB). The per-database form is
/// [`crate::rotxn::DatabaseStat`] (SPEC 00 row 49).
///
/// ## LMDB `MDB_stat` field mapping
///
/// | `MDB_stat` | here | note |
/// |---|---|---|
/// | `ms_psize` | [`EnvStat::page_size`] | identical meaning; the value is whatever the env was created with (SPEC 02 §0, runtime page-size selection). |
/// | `ms_depth` | [`EnvStat::depth`] | identical meaning (tree height, 0 = empty). The *value* depends on per-page fan-out and therefore on the on-disk format (ZeroDB's own) — comparable in kind, not exactly, cross-engine. |
/// | `ms_branch_pages` | [`EnvStat::branch_pages`] | identical meaning; **format-specific value** (own on-disk format). |
/// | `ms_leaf_pages` | [`EnvStat::leaf_pages`] | identical meaning; **format-specific value** (own on-disk format). |
/// | `ms_overflow_pages` | [`EnvStat::overflow_pages`] | identical meaning; **format-specific value** (own on-disk format). |
/// | `ms_entries` | [`EnvStat::entries`] | identical meaning **and value**: for the env-level stat this is the number of entries in the main DB, which under a named-DB env is the number of named-DB catalog records. Comparable cross-engine. |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvStat {
    /// Database page size in bytes (`ms_psize`).
    pub page_size: u32,
    /// Height of the main tree (`ms_depth`); 0 = empty, 1 = root-is-leaf.
    pub depth: u16,
    /// Branch (internal) pages of the main tree (`ms_branch_pages`).
    pub branch_pages: u64,
    /// Leaf pages of the main tree (`ms_leaf_pages`).
    pub leaf_pages: u64,
    /// Overflow pages of the main tree (`ms_overflow_pages`).
    pub overflow_pages: u64,
    /// Entries in the main tree (`ms_entries`).
    pub entries: u64,
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

/// Upper bound accepted for `max_readers` (SPEC 00 row 5, SPEC 04 TXN-14).
/// The reader table eagerly allocates one cache-padded slot (128 bytes,
/// `ReaderSlot`) per reader — 1 Mi slots is 128 MiB, already far beyond any real
/// configuration; anything larger is treated as a caller bug
/// (`Io(InvalidInput)`) rather than an allocation-failure abort.
pub const MAX_READERS_LIMIT: u32 = 1 << 20;

/// Upper bound accepted for `max_dbs` (SPEC 00 row 4) — same rationale as
/// [`MAX_READERS_LIMIT`] (the comparator registry allocates `max_dbs` slots
/// eagerly).
pub const MAX_DBS_LIMIT: u32 = 1 << 20;

/// Rejects a `max_readers` or `max_dbs` above its limit with
/// `Io(InvalidInput)` (see docs/DIVERGENCES.md).
///
/// Both values size eager allocations: `max_readers` reader slots (in this
/// process, or in the lock file of a lock-file environment, ADR-0023) and
/// `max_dbs` comparator `OnceLock`s. An unbounded value, e.g.
/// `max_readers(u32::MAX)`, would be an allocation-failure abort or a
/// 512 GiB lock file, not an error. The bounds (SPEC 00 rows 4/5, SPEC 04
/// TXN-14) are far above any real deployment (Meilisearch's ceiling is 1024
/// readers / ~30 dbs). The engine calls this before it creates any file, and
/// the core open functions call it again for callers that build a
/// backing themselves.
pub fn check_open_limits(max_readers: u32, max_dbs: u32) -> Result<(), Error> {
    if max_readers > MAX_READERS_LIMIT {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "max_readers ({max_readers}) exceeds the supported maximum {MAX_READERS_LIMIT}"
            ),
        )));
    }
    if max_dbs > MAX_DBS_LIMIT {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("max_dbs ({max_dbs}) exceeds the supported maximum {MAX_DBS_LIMIT}"),
        )));
    }
    Ok(())
}

/// Open an env over an already-constructed [`Backing`] (SPEC 02 §3.2, SPEC 06
/// §1). This is the I/O-free core of open: the caller (`zerodb-io` / the public
/// `zerodb` crate) has already opened, created-if-new, and mapped the file, and
/// determined `page_size` (from the meta, or the creation option). The
/// runtime map size is the backing's: [`Backing::view`] (ADR-0023 amendment
/// 15).
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
// The open parameters are all distinct scalars/flags the caller (zerodb-io / the
// public crate) has already resolved; bundling them into a params struct would
// only add indirection for this single internal entry point.
#[allow(clippy::too_many_arguments)]
pub fn open_with_backing(
    canonical_path: PathBuf,
    backing: Box<dyn Backing>,
    page_size: u32,
    prev_snapshot: bool,
    max_dbs: u32,
    max_readers: u32,
    durability: DurabilityFlags,
) -> Result<Env, Error> {
    open_with_backing_policy(
        canonical_path,
        backing,
        page_size,
        prev_snapshot,
        max_dbs,
        max_readers,
        durability,
        FileTrust::VALIDATE,
        false,
        None,
    )
}

/// LMDB's dirty limit, `MDB_IDL_UM_MAX` = 2^17 pages (SPEC 04 TXN-68).
pub const DEFAULT_DIRTY_LIMIT: u64 = 1 << 17;

/// The smallest dirty limit `max_dirty_bytes` can set, in pages (SPEC 04
/// TXN-68): a spill writes at least 64 pages and keeps roots and finger
/// pages, so a smaller limit would spill on nearly every call.
pub const MIN_DIRTY_LIMIT: u64 = 128;

/// The dirty limit in pages for `max_dirty_bytes` at `page_size` (SPEC 04
/// TXN-68): `None` = LMDB's [`DEFAULT_DIRTY_LIMIT`], otherwise
/// `max(bytes / page_size, MIN_DIRTY_LIMIT)`.
#[must_use]
pub fn dirty_limit_pages(max_dirty_bytes: Option<usize>, page_size: u32) -> u64 {
    match max_dirty_bytes {
        None => DEFAULT_DIRTY_LIMIT,
        Some(b) => (b as u64 / u64::from(page_size.max(1))).max(MIN_DIRTY_LIMIT),
    }
}

/// [`open_with_backing`] with an explicit page-validation policy
/// (ADR-0014), sequential-writes default (ADR-0015) and dirty limit in
/// pages (ADR-0017; `None` = LMDB's [`DEFAULT_DIRTY_LIMIT`]). The limit is
/// taken as given (at least 1): the public option applies its floor through
/// [`dirty_limit_pages`], while the crash harness uses tiny limits here to
/// spill on nearly every call.
/// `(FileTrust::VALIDATE, false, None)` is exactly [`open_with_backing`]; the
/// trusting policy can only be built through the `unsafe`
/// [`FileTrust::trust_contents`], whose contract the caller carries.
///
/// # Errors
///
/// As [`open_with_backing`].
#[allow(clippy::too_many_arguments)]
pub fn open_with_backing_policy(
    canonical_path: PathBuf,
    backing: Box<dyn Backing>,
    page_size: u32,
    prev_snapshot: bool,
    max_dbs: u32,
    max_readers: u32,
    durability: DurabilityFlags,
    file_trust: FileTrust,
    sequential_writes: bool,
    dirty_limit: Option<u64>,
) -> Result<Env, Error> {
    open_with_backing_shared(
        canonical_path,
        backing,
        page_size,
        prev_snapshot,
        max_dbs,
        max_readers,
        durability,
        file_trust,
        sequential_writes,
        dirty_limit,
        None,
    )
}

/// [`open_with_backing_policy`] for an environment coordinated through a
/// cross-process lock-file region (ADR-0023). `shared = None` is exactly
/// [`open_with_backing_policy`].
///
/// With a region:
/// - the reader slots live in the region (its slot count replaces
///   `max_readers`);
/// - the region's initializer (no other process had the environment open)
///   seeds its snapshot from the selected meta, as a single-process open
///   does, and publishes that txnid as `T`;
/// - a joining process seeds its snapshot from the meta slot holding the
///   published `T`, never from the newest valid meta (ADR-0023 amendment 5);
/// - pins, the writer's base and [`EnvInner::txnid`] follow `T`, and the
///   writer also takes the region's cross-process lock.
///
/// The region is moved in, and the caller keeps no other strong reference to
/// it: the environment is then its only holder, so dropping the environment,
/// or an open that fails after building it, releases the region (closes the
/// lock file) before the error or the close event is seen.
///
/// # Errors
///
/// As [`open_with_backing`]; with a region, also [`MdbError::Invalid`] when
/// the meta slot holding `T` never decodes or its snapshot lies beyond the
/// file, and [`MdbError::MapResized`] when that snapshot lies beyond this
/// process's logical map size.
#[allow(clippy::too_many_arguments)]
pub fn open_with_backing_shared(
    canonical_path: PathBuf,
    backing: Box<dyn Backing>,
    page_size: u32,
    prev_snapshot: bool,
    max_dbs: u32,
    max_readers: u32,
    durability: DurabilityFlags,
    file_trust: FileTrust,
    sequential_writes: bool,
    dirty_limit: Option<u64>,
    shared: Option<Arc<dyn crate::shared::SharedRegion>>,
) -> Result<Env, Error> {
    // loom builds model the in-process reader protocol only; the shared reader
    // table is not compiled there, so a region cannot be honored.
    #[cfg(loom)]
    if shared.is_some() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "lock-file environments are not available under loom",
        )));
    }
    check_open_limits(max_readers, max_dbs)?;
    // Validate the two meta slots from the mapped bytes (SPEC 02 §3.2). A
    // decode error here (bad page size / truncated buffer) means the file is not
    // a usable env → Invalid.
    let bytes = backing.bytes();
    let ps = page_size as usize;
    let slot0 = read_slot(bytes, META_A_PGNO, ps, page_size)?;
    let slot1 = read_slot(bytes, META_B_PGNO, ps, page_size)?;

    // Select the live snapshot (SPEC 02 §3.2 / SPEC 06 REC-2..5).
    let (meta, chosen_slot) = match select_meta(&slot0, &slot1, prev_snapshot) {
        MetaChoice::Both { meta, chosen } => (meta, chosen),
        MetaChoice::OnlyOne { meta, chosen } => {
            if prev_snapshot {
                // REC-2†: one valid slot + PREV_SNAPSHOT is
                // a hard error — there are not two committed snapshots to pick an
                // older from.
                return Err(Error::Mdb(MdbError::Invalid));
            }
            (meta, chosen)
        }
        // REC-3: both invalid → unrecoverable.
        MetaChoice::None => return Err(Error::Mdb(MdbError::Invalid)),
    };
    // Format v2 (ADR-0022): read the selected slot's free-list annex ids —
    // the one and only meta read, alongside the roots (TXN-18); the slot is
    // overwritten two commits later, so the ids are pinned in the Snapshot.
    // `fl_count` passed the §3.2 rule-5 bound + CRC; the ids' GC-29 shape is
    // validated by the consumer before any id is handed out (GC-33).
    let annex = MetaPage::read_annex(&bytes[chosen_slot * ps..(chosen_slot + 1) * ps], page_size)
        .ok_or(Error::Mdb(MdbError::Invalid))?;

    // SPEC 06 REC-1a / SPEC 02 §3.2 step 6 (geometry validation): a slot can
    // carry a valid CRC and still name geometry the real file cannot back — a
    // truncated or hostile file. The mapping covers the full `map_size`
    // (ADR-0004 D4), so pages inside the map but past the file end are
    // unbacked: dereferencing them faults (SIGBUS) instead of erroring. Reject
    // at open, with checked arithmetic (a hostile `last_pg` near `u64::MAX`
    // must not wrap the multiply):
    //  - the file must cover `(last_pg + 1) * page_size` bytes;
    //  - each root is `PGNO_INVALID` or within `[FIRST_DATA_PGNO, last_pg]`.
    // Under WRITE_MAP a process alone on the file grew it to `map_size` at
    // map time, so the length check is trivially true there; a joining
    // process does not grow the file at open (ADR-0023 amendment 15), so for
    // it the check is real. The roots check always applies.
    // Also part of REC-1a: the committed txnid itself. The reader table
    // encodes slot occupancy in the top-of-u64 sentinel band (SPEC 04 TXN-14)
    // and the next writer txnid is `meta.txnid + 1`; a txnid at or above the
    // band is unreachable by real commits (one increment per commit) and
    // would alias the RDR_FREE/RDR_CLAIMED sentinels or wrap — corrupt file.
    if meta.txnid > crate::readers::MAX_COMMITTED_TXNID {
        return Err(Error::Mdb(MdbError::Invalid));
    }
    let real = backing.real_disk_size().map_err(Error::Io)?;
    let needed = meta
        .last_pg
        .checked_add(1)
        .and_then(|n| n.checked_mul(u64::from(page_size)))
        .ok_or(Error::Mdb(MdbError::Invalid))?;
    if needed > real {
        return Err(Error::Mdb(MdbError::Invalid));
    }
    for rec in [&meta.main_db, &meta.free_db] {
        if rec.root != crate::page::PGNO_INVALID
            && (rec.root < crate::page::FIRST_DATA_PGNO || rec.root > meta.last_pg)
        {
            return Err(Error::Mdb(MdbError::Invalid));
        }
    }

    let selected = Snapshot::from_meta(&meta, annex);
    let (seed, newer_on_disk) = match &shared {
        // A joining process seeds from the published txnid, never from the
        // newest valid meta: that meta may belong to a commit still in flight
        // or one that failed, whose txnid the next writer reuses (TXN-2;
        // ADR-0023 amendment 5).
        Some(region) if !region.is_initializer() => {
            let seed = seed_from_published(backing.as_ref(), page_size, region.as_ref())?;
            // ADR-0023 amendment 19: a newer meta on disk than `T` (the
            // unpublished first commit of a `PREV_SNAPSHOT` opener, or a
            // commit between its meta write and its `T` store) is what
            // LMDB's `mdb_env_info`/`mdb_env_stat` report
            // (`OBS_PREV_SNAPSHOT_JOINER_INFO_AFTER_FIRST_COMMIT`). Kept
            // for those calls only; reads still use `T`.
            let newer = (selected.txnid > seed.txnid).then(|| Unpublished {
                snap: Arc::new(selected),
                own_commit: false,
            });
            (seed, newer)
        }
        _ => (selected, None),
    };
    let reader_table = match &shared {
        // In shared mode the slot count comes from the region, fixed by the
        // first opener: a joining process adopts it whatever its own
        // `max_readers` asks for, as an LMDB joiner does (ADR-0023 §1 as
        // amended).
        #[cfg(not(loom))]
        Some(region) => ReaderTable::new_shared(Arc::clone(region)),
        // The reader table is sized once at open and never resized (TXN-14).
        _ => ReaderTable::new(max_readers),
    };
    // A joining process frees the slots that carry its own PID (ADR-0023
    // amendment 9): a dead process that had the same PID left them, and no
    // liveness probe can tell them apart from this process's own slots. None
    // of them is live: this runs before this process's first claim, and the
    // lock-file layer refuses a second open of the same lock file within one
    // process, so no other environment handle here shares these slots. An
    // initializer has already wiped every slot.
    #[cfg(not(loom))]
    if let Some(region) = &shared {
        if !region.is_initializer() {
            reader_table.reap_own_stale(region.pid());
        }
    }

    // Register under the process registry (SPEC 04 TXN-51). Hold the lock across
    // check + insert so two concurrent opens of one path cannot both succeed.
    let id = next_env_id();
    let closing = Arc::new(SignalEvent::new());
    // ADR-0023 amendment 19: a `PREV_SNAPSHOT` initializer keeps the
    // election until its first commit, as LMDB does.
    let holds_election = prev_snapshot && shared.as_ref().is_some_and(|r| r.is_initializer());
    // `shared` moves into the `EnvInner` rather than being cloned: from here
    // on the environment (and its reader table) holds every reference this
    // open owns, so an error return below drops `inner` as the last holder
    // and the lock file closes with it (the check in `EnvInner::drop`).
    let inner = Arc::new(EnvInner {
        id,
        path: canonical_path.clone(),
        backing: Some(backing),
        page_size,
        // Seed the published-snapshot cell from the durable meta — the one
        // and only time a meta *page* is read for roots (SPEC 04 TXN-18). A
        // lock-file environment re-reads metas when another process commits
        // (ADR-0023 §4, `EnvInner::snapshot_at`).
        snap_cell: SnapshotCell::new(Arc::new(seed)),
        write_mutex: WriterLock::new(),
        meta_buf: Mutex::new(Vec::new()),
        commit_hook: Mutex::new(None),
        poisoned: AtomicBool::new(false),
        reader_table,
        handles: crate::handles::HandleTable::new(max_dbs),
        durability,
        comparators: ComparatorRegistry::new(max_dbs),
        max_dbs,
        file_trust,
        stamp_cache: StampCache::new(),
        dirty_limit: dirty_limit.unwrap_or(DEFAULT_DIRTY_LIMIT).max(1),
        sequential_writes,
        sequential_overrides: (0..=max_dbs).map(|_| AtomicU8::new(SEQ_FOLLOW)).collect(),
        meta,
        prev_snapshot,
        closing,
        holds_election: AtomicBool::new(holds_election),
        has_unpublished: AtomicBool::new(newer_on_disk.is_some()),
        unpublished: Mutex::new(newer_on_disk),
        shared,
    });

    if let Some(region) = &inner.shared {
        if region.is_initializer() {
            // SeqCst: the same total order as every later T store and
            // every reader's publish-and-verify (SPEC 04 TXN-17/19).
            region
                .published_txnid()
                .store(inner.snapshot().txnid, Ordering::SeqCst);
        }
    }

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

    // ADR-0023 §1: let joiners in only after `T` is seeded and the env is
    // registered. A `PREV_SNAPSHOT` initializer keeps the election until its
    // first commit instead (amendment 19; `RwTxn::commit`).
    if let Some(region) = &inner.shared {
        if !inner.holds_election() {
            region.finish_open().map_err(Error::Io)?;
        }
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

/// The snapshot committed as txnid `t`, decoded from a copy of meta slot
/// `t & 1` (ADR-0023 §4). `Ok(None)` means the slot no longer holds `t`: it
/// was overwritten by `t + 2`, or the copy was torn by a concurrent rewrite.
/// The caller re-reads the published txnid and retries. Geometry is checked
/// as at open (SPEC 06 REC-1a), against the file as it is now.
pub(crate) fn read_snapshot_at(
    backing: &dyn Backing,
    page_size: u32,
    t: u64,
) -> Result<Option<Snapshot>, Error> {
    let mut buf = vec![0u8; page_size as usize];
    backing
        .read_meta_copy(t & 1, page_size, &mut buf)
        .map_err(Error::Io)?;
    let meta = match MetaPage::validate(&buf, page_size) {
        Ok(MetaValidity::Valid(m)) => m,
        // A failed CRC is a torn copy of a slot being rewritten; retry.
        Ok(_) => return Ok(None),
        Err(_) => return Err(Error::Mdb(MdbError::Invalid)),
    };
    if meta.txnid != t {
        return Ok(None);
    }
    if meta.txnid > crate::readers::MAX_COMMITTED_TXNID {
        return Err(Error::Mdb(MdbError::Invalid));
    }
    for rec in [&meta.main_db, &meta.free_db] {
        if rec.root != crate::page::PGNO_INVALID
            && (rec.root < crate::page::FIRST_DATA_PGNO || rec.root > meta.last_pg)
        {
            return Err(Error::Mdb(MdbError::Invalid));
        }
    }
    let annex = MetaPage::read_annex(&buf, page_size).ok_or(Error::Mdb(MdbError::Invalid))?;
    Ok(Some(Snapshot::from_meta(&meta, annex)))
}

/// Bound on re-reads of the published txnid when its meta slot keeps changing
/// underneath (ADR-0023 §4). Each retry means a commit landed or a slot was
/// mid-rewrite; a slot that never decodes is a corrupt file, reported as
/// [`MdbError::Invalid`] once the bound is reached instead of spinning.
// Lower under miri only to keep its runtime bounded; the bounded-retry →
// `Invalid` behavior is identical.
const META_RETRY_LIMIT: u32 = if cfg!(miri) { 64 } else { 10_000 };

/// The seed snapshot of a process joining a lock-file environment: the one
/// committed as the published txnid `T` (ADR-0023 amendment 5).
fn seed_from_published(
    backing: &dyn Backing,
    page_size: u32,
    region: &dyn crate::shared::SharedRegion,
) -> Result<Snapshot, Error> {
    for _ in 0..META_RETRY_LIMIT {
        // SeqCst: pairs with the committing writer's SeqCst store of T.
        let t = region.published_txnid().load(Ordering::SeqCst);
        if let Some(s) = read_snapshot_at(backing, page_size, t)? {
            match check_snapshot_backed(backing, page_size, &s) {
                // Another process committed past this map between the
                // header read that sized it and this seed: grow to the
                // seed and check again (below).
                Err(Error::Mdb(MdbError::MapResized)) => grow_to_seed(backing, page_size, &s)?,
                other => other?,
            }
            check_snapshot_backed(backing, page_size, &s)?;
            return Ok(s);
        }
        std::thread::yield_now();
    }
    Err(Error::Mdb(MdbError::Invalid))
}

/// Grow a joiner's map to its seed's `(last_pg + 1) * page_size`, as LMDB's
/// open rounds a small map up to its data and never fails with
/// `MDB_MAP_RESIZED` (ADR-0023 amendment 15). The seed is never used
/// unchecked: the cached read fast path does no check of its own, so the
/// caller re-checks it against the grown view.
///
/// Runs at open, before the backing is shared, so no writer lock is needed to
/// serialize the publication. It never grows the data file: a seed past the
/// file's end is corrupt (SPEC 06 REC-1a) and is refused before any growth,
/// so under `WRITE_MAP` the file is already long enough.
///
/// # Errors
///
/// [`MdbError::Invalid`] when the seed needs more than
/// [`MAX_MAP_SIZE`](crate::page::geometry::MAX_MAP_SIZE) or more than the
/// file holds. [`MdbError::MapResized`] when the backing cannot grow its map
/// (a heap test backing); the I/O error of a failed growth.
fn grow_to_seed(backing: &dyn Backing, page_size: u32, seed: &Snapshot) -> Result<(), Error> {
    let needed = seed
        .last_pg
        .checked_add(1)
        .and_then(|n| n.checked_mul(u64::from(page_size)))
        .ok_or(Error::Mdb(MdbError::Invalid))?;
    if needed > crate::page::geometry::MAX_MAP_SIZE
        || needed > backing.real_disk_size().map_err(Error::Io)?
    {
        return Err(Error::Mdb(MdbError::Invalid));
    }
    match backing.grow_map(needed) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {
            Err(Error::Mdb(MdbError::MapResized))
        }
        Err(e) => Err(Error::Io(e)),
    }
}

/// Check that a snapshot decoded after open lies inside this process's map
/// and inside the file as it is now (ADR-0023 amendment 15; SPEC 06 REC-1a,
/// the open-time geometry check). Checked arithmetic: a hostile `last_pg`
/// must not wrap the multiply.
///
/// - Past the logical map size: [`MdbError::MapResized`]. Another process
///   with a larger map committed beyond this one's (`OBS_READ_BEGIN_SMALL_MAP`,
///   `OBS_WRITE_BEGIN_SMALL_MAP`). A larger map size in the meta alone passes.
/// - Past the mapped bytes but inside the logical size: [`MdbError::Invalid`];
///   a mapping always covers its logical size, so this is a bug or corruption.
/// - Past the file's real length: [`MdbError::Invalid`] (REC-1a).
fn check_snapshot_backed(
    backing: &dyn Backing,
    page_size: u32,
    snap: &Snapshot,
) -> Result<(), Error> {
    let needed = check_snapshot_in_view(backing.view(), page_size, snap)?;
    if needed > backing.real_disk_size().map_err(Error::Io)? {
        // REC-1a: the file is shorter than last_pg.
        return Err(Error::Mdb(MdbError::Invalid));
    }
    Ok(())
}

/// The map half of [`check_snapshot_backed`], against one view (a consistent
/// pair from one `Acquire` load). Returns the bytes the snapshot needs,
/// `(last_pg + 1) * page_size`.
fn check_snapshot_in_view(
    view: MapView<'_>,
    page_size: u32,
    snap: &Snapshot,
) -> Result<u64, Error> {
    let needed = snap
        .last_pg
        .checked_add(1)
        .and_then(|n| n.checked_mul(u64::from(page_size)))
        .ok_or(Error::Mdb(MdbError::Invalid))?;
    if needed > view.map_size {
        return Err(Error::Mdb(MdbError::MapResized));
    }
    if needed > view.bytes.len() as u64 {
        // A map shorter than its logical size: a bug or corruption.
        return Err(Error::Mdb(MdbError::Invalid));
    }
    Ok(needed)
}

impl EnvInner {
    /// The write-begin check of the writer's base against `view`, the map
    /// view the begin loaded (ADR-0023 amendment 15), also for a base that
    /// came from the cache: a write txn never starts with a base outside its
    /// logical map size, which would otherwise make every allocation
    /// `MapFull`. The file-length half is not repeated: a cached snapshot
    /// passed it when it was decoded, and a process-local one was committed
    /// through this backing.
    ///
    /// # Errors
    ///
    /// [`MdbError::MapResized`] past the logical map size;
    /// [`MdbError::Invalid`] past the mapped bytes.
    pub(crate) fn check_base_in(&self, view: MapView<'_>, base: &Snapshot) -> Result<(), Error> {
        check_snapshot_in_view(view, self.page_size, base).map(|_| ())
    }
}

/// Test-only helpers: a heap-backed [`Backing`] and an in-memory env
/// constructor, so txn/mutation logic runs under `miri` (no mmap, no file
/// I/O). **Not part of the stable API** — used by this crate's tests, the
/// crate's integration tests, and nothing else. The backing is read-only
/// (commit I/O keeps the erroring `Backing` defaults), so in-memory envs
/// exercise mutation and in-txn reads but never the commit pipeline — commits
/// are tested against real files in `crates/zerodb`.
#[doc(hidden)]
pub mod testutil {
    use super::{next_env_id, open_with_backing_policy, Backing, Env, MapView};
    use crate::error::Error;
    use crate::page::FileTrust;
    use crate::page::MetaPage;
    use std::path::PathBuf;

    /// A heap-backed read-only [`Backing`]: the bytes model the file, and the
    /// logical map size is fixed at construction.
    pub struct VecBacking {
        bytes: Vec<u8>,
        map_size: u64,
    }

    impl VecBacking {
        /// A backing over `bytes` (the file's contents) with a logical map
        /// size of `map_size`, which may exceed `bytes.len()`: pages past the
        /// file are never read, as with a real map past the end of the file.
        #[must_use]
        pub fn new(bytes: Vec<u8>, map_size: u64) -> VecBacking {
            VecBacking { bytes, map_size }
        }

        /// The file's contents, for a test that edits them in place.
        pub fn bytes_mut(&mut self) -> &mut [u8] {
            &mut self.bytes
        }
    }

    impl Backing for VecBacking {
        fn view(&self) -> MapView<'_> {
            MapView {
                bytes: &self.bytes,
                map_size: self.map_size,
            }
        }
        fn real_disk_size(&self) -> std::io::Result<u64> {
            Ok(self.bytes.len() as u64)
        }
        fn try_clone_file(&self) -> std::io::Result<std::fs::File> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "vec backing has no fd",
            ))
        }
    }

    /// A fresh in-memory env: both meta slots valid at txnid 0 (SPEC 02 §3.4),
    /// the backing vector sized to `map_size` (as a real map would be,
    /// ADR-0004 D4), registered under a unique virtual path.
    ///
    /// # Panics
    ///
    /// On an invalid `page_size` (test helper).
    #[must_use]
    pub fn mem_env(page_size: u32, map_size: u64) -> Env {
        mem_env_with(page_size, map_size, false)
    }

    /// [`mem_env`] with the sequential-writes default on (ADR-0015), for the
    /// rightmost-leaf finger tests.
    ///
    /// # Panics
    ///
    /// On an invalid `page_size` (test helper).
    #[must_use]
    pub fn mem_env_sequential(page_size: u32, map_size: u64) -> Env {
        mem_env_with(page_size, map_size, true)
    }

    fn mem_env_with(page_size: u32, map_size: u64, sequential_writes: bool) -> Env {
        let ps = page_size as usize;
        let mut buf = vec![0u8; map_size as usize];
        for slot in [0u64, 1] {
            let meta = MetaPage::create(slot, page_size, map_size);
            let base = slot as usize * ps;
            meta.encode(&mut buf[base..base + ps]).expect("valid meta");
        }
        let path = PathBuf::from(format!("/virtual/mem-env-{}", next_env_id()));
        // A generous named-DB capacity for tests (real envs pass the caller's
        // `max_dbs`; SPEC 02 §6); max_readers = 126, the TXN-14
        // default.
        match open_with_backing_policy(
            path,
            Box::new(VecBacking::new(buf, map_size)),
            page_size,
            false,
            128,
            126,
            super::DurabilityFlags::default(),
            FileTrust::VALIDATE,
            sequential_writes,
            None,
        ) {
            Ok(env) => env,
            Err(Error::Io(e)) => panic!("mem_env open failed: {e}"),
            Err(e) => panic!("mem_env open failed: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testutil::VecBacking;
    use super::*;
    use crate::page::MetaPage;

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
        open_with_backing(
            path,
            Box::new(VecBacking::new(buf, MAP)),
            PS,
            prev,
            128,
            126,
            DurabilityFlags::default(),
        )
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

#[cfg(test)]
mod shared_meta_tests {
    use super::testutil::VecBacking;
    use super::*;
    use crate::page::{DBRecord, MetaPage, FORMAT_VERSION, MAGIC};

    const PS: u32 = 4096;

    fn meta(txnid: u64, last_pg: u64) -> MetaPage {
        MetaPage {
            pgno: txnid & 1,
            txnid,
            magic: MAGIC,
            format_version: FORMAT_VERSION,
            page_size: PS,
            env_flags: 0,
            map_size: 1 << 20,
            last_pg,
            free_db: DBRecord::empty(),
            main_db: DBRecord::empty(),
            fl_count: 0,
        }
    }

    /// Two meta slots plus `extra` zeroed data pages.
    fn image(m0: &MetaPage, m1: &MetaPage, extra: usize) -> VecBacking {
        let ps = PS as usize;
        let mut bytes = vec![0u8; ps * (2 + extra)];
        m0.encode_with_annex(&mut bytes[..ps], &[]).unwrap();
        m1.encode_with_annex(&mut bytes[ps..2 * ps], &[]).unwrap();
        VecBacking::new(bytes, 1 << 20)
    }

    #[test]
    fn reads_the_slot_holding_t() {
        let b = image(&meta(4, 2), &meta(5, 3), 2);
        let s = read_snapshot_at(&b, PS, 5)
            .unwrap()
            .expect("slot 1 holds 5");
        assert_eq!((s.txnid, s.last_pg), (5, 3));
        let s = read_snapshot_at(&b, PS, 4)
            .unwrap()
            .expect("slot 0 holds 4");
        assert_eq!(s.txnid, 4);
    }

    #[test]
    fn overwritten_slot_reads_as_none() {
        // Slot 1 was overwritten by txnid 7 while the caller pinned 5.
        let b = image(&meta(6, 2), &meta(7, 3), 2);
        assert!(read_snapshot_at(&b, PS, 5).unwrap().is_none());
    }

    #[test]
    fn torn_slot_reads_as_none() {
        let mut b = image(&meta(4, 2), &meta(5, 3), 2);
        b.bytes_mut()[PS as usize + 100] ^= 0xFF; // corrupt slot 1 → CRC fails
        assert!(read_snapshot_at(&b, PS, 5).unwrap().is_none());
    }

    #[test]
    fn root_beyond_last_pg_is_invalid() {
        let mut m = meta(5, 3);
        m.main_db.root = 50;
        let b = image(&meta(4, 2), &m, 2);
        assert!(matches!(
            read_snapshot_at(&b, PS, 5),
            Err(Error::Mdb(MdbError::Invalid))
        ));
    }
}

// `HeapRegion` exists only in non-loom test builds (loom models the
// in-process protocol, not the shared region).
#[cfg(all(test, not(loom)))]
mod shared_env_tests {
    use super::testutil::VecBacking;
    use super::*;
    use crate::page::{DBRecord, MetaPage, FORMAT_VERSION, MAGIC};
    use crate::readers::RDR_FREE;
    use crate::shared::{HeapRegion, SharedRegion};
    use std::sync::atomic::Ordering as O;

    const PS: u32 = 4096;

    fn meta_with(txnid: u64, last_pg: u64) -> MetaPage {
        MetaPage {
            pgno: txnid & 1,
            txnid,
            magic: MAGIC,
            format_version: FORMAT_VERSION,
            page_size: PS,
            env_flags: 0,
            map_size: 1 << 20,
            last_pg,
            free_db: DBRecord::empty(),
            main_db: DBRecord::empty(),
            fl_count: 0,
        }
    }

    fn meta(txnid: u64) -> MetaPage {
        meta_with(txnid, 3)
    }

    /// A four-page image: meta slots holding `t0` and `t1`, two data pages.
    fn image(t0: u64, t1: u64) -> Vec<u8> {
        let ps = PS as usize;
        let mut bytes = vec![0u8; ps * 4];
        meta(t0).encode_with_annex(&mut bytes[..ps], &[]).unwrap();
        meta(t1)
            .encode_with_annex(&mut bytes[ps..2 * ps], &[])
            .unwrap();
        bytes
    }

    fn backing(t0: u64, t1: u64) -> Box<dyn Backing> {
        Box::new(VecBacking::new(image(t0, t1), 1 << 20))
    }

    fn try_open(b: Box<dyn Backing>, region: Arc<dyn SharedRegion>) -> Result<Env, Error> {
        let dir = std::env::temp_dir().join(format!("zerodb-shared-{}", next_env_id()));
        open_with_backing_shared(
            dir,
            b,
            PS,
            false,
            4,
            4,
            DurabilityFlags::default(),
            crate::page::FileTrust::VALIDATE,
            false,
            None,
            Some(region),
        )
    }

    fn open(b: Box<dyn Backing>, region: Arc<dyn SharedRegion>) -> Env {
        try_open(b, region).expect("open")
    }

    /// A heap backing over [`image`] whose writes and syncs succeed without
    /// storing anything: enough to run the commit pipeline when a test
    /// checks only txnids, `T` and the election, and reads nothing a commit
    /// wrote.
    struct SinkBacking(VecBacking);

    impl Backing for SinkBacking {
        fn view(&self) -> MapView<'_> {
            self.0.view()
        }
        fn real_disk_size(&self) -> std::io::Result<u64> {
            self.0.real_disk_size()
        }
        fn try_clone_file(&self) -> std::io::Result<std::fs::File> {
            self.0.try_clone_file()
        }
        fn write_at_page(&self, _pgno: u64, _psize: u32, _data: &[u8]) -> std::io::Result<()> {
            Ok(())
        }
        fn sync(&self, _async_flush: bool) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// [`open`] with `PREV_SNAPSHOT`, over a [`SinkBacking`] of
    /// `image(t0, t1)`.
    fn open_prev(t0: u64, t1: u64, region: Arc<dyn SharedRegion>) -> Env {
        let dir = std::env::temp_dir().join(format!("zerodb-shared-{}", next_env_id()));
        open_with_backing_shared(
            dir,
            Box::new(SinkBacking(VecBacking::new(image(t0, t1), 1 << 20))),
            PS,
            true,
            4,
            4,
            DurabilityFlags::default(),
            crate::page::FileTrust::VALIDATE,
            false,
            None,
            Some(region),
        )
        .expect("open")
    }

    /// Begin a write txn: its txnid, and whether its base holds `key`.
    fn write_base(env: &Env, key: &[u8]) -> (u64, bool) {
        let w = env.write_txn().unwrap();
        let has = env.main_database().get(&w, key).unwrap().is_some();
        (w.txnid(), has)
    }

    fn put_commit(env: &Env, key: &[u8]) -> Result<u64, Error> {
        let mut w = env.write_txn()?;
        let id = w.txnid();
        env.main_database().put(&mut w, key, b"v")?;
        w.commit()?;
        Ok(id)
    }

    /// ADR-0023 amendment 19 (`OBS_PREV_SNAPSHOT_JOINER_INFO_AFTER_FIRST_COMMIT`):
    /// a joiner that finds a meta newer than `T` on disk reads at `T` but
    /// reports the newer meta in `info`, until a publish reaches it.
    #[test]
    fn joiner_reports_a_newer_meta_on_disk_until_t_reaches_it() {
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(4, O::SeqCst);
        let env = open(backing(4, 5), region.clone());
        assert_eq!(
            env.inner
                .pin_reader()
                .map(|(s, slot)| {
                    env.inner.release_reader(slot);
                    s.txnid
                })
                .unwrap(),
            4
        );
        assert_eq!(env.info().last_txnid, 5);
        assert_eq!(env.inner.txnid(), 4, "txnid stays the published one");
        assert!(env.inner.has_unpublished.load(O::Relaxed));
        region.published_txnid().store(5, O::SeqCst);
        assert_eq!(env.inner.txnid(), 5);
        assert!(
            !env.inner.has_unpublished.load(O::Relaxed),
            "cleared once T reaches it"
        );
        assert_eq!(env.info().last_txnid, 5);
    }

    /// An ordinary joiner whose newest meta is `T` keeps no unpublished
    /// snapshot, so `info` and `txnid` skip the mutex.
    #[test]
    fn joiner_at_the_newest_meta_keeps_nothing_unpublished() {
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(5, O::SeqCst);
        let env = open(backing(4, 5), region.clone());
        assert!(!env.inner.has_unpublished.load(O::Relaxed));
        assert_eq!(env.info().last_txnid, 5);
    }

    #[test]
    fn an_ordinary_initializer_finishes_the_election_at_open() {
        let region = Arc::new(HeapRegion::new(4, 1, true));
        let _env = open(backing(4, 5), region.clone());
        assert_eq!(region.finish_opens.load(O::Relaxed), 1);
    }

    /// ADR-0023 amendment 19: a `PREV_SNAPSHOT` initializer keeps the
    /// election past its open; an unchanged commit keeps it; the first real
    /// commit finishes it and is not published, so readers keep the older
    /// snapshot and the next write txn reuses its txnid on that base.
    #[test]
    fn prev_snapshot_initializer_holds_the_election_until_its_first_commit() {
        let region = Arc::new(HeapRegion::new(4, 1, true));
        let env = open_prev(4, 5, region.clone());
        assert_eq!(region.published_txnid().load(O::SeqCst), 4);
        assert_eq!(region.finish_opens.load(O::Relaxed), 0);
        env.write_txn().unwrap().commit().unwrap();
        assert_eq!(region.finish_opens.load(O::Relaxed), 0, "unchanged commit");

        assert_eq!(put_commit(&env, b"first").unwrap(), 5);
        assert_eq!(region.finish_opens.load(O::Relaxed), 1);
        assert!(!env.inner.holds_election());
        assert_eq!(region.published_txnid().load(O::SeqCst), 4, "T unchanged");
        let r = env.read_txn().unwrap();
        assert_eq!(r.txnid(), 4, "readers keep the older snapshot");
        assert!(env.main_database().get(&r, b"first").unwrap().is_none());
        drop(r);
        assert_eq!(env.inner.txnid(), 5, "names the unpublished commit");
        assert_eq!(env.info().last_txnid, 5);
        assert_eq!(
            write_base(&env, b"first"),
            (5, false),
            "reused on the older base"
        );

        assert_eq!(put_commit(&env, b"second").unwrap(), 5);
        assert_eq!(region.finish_opens.load(O::Relaxed), 1);
        assert_eq!(region.published_txnid().load(O::SeqCst), 5, "published");
        assert_eq!(env.inner.txnid(), 5);
        assert_eq!(env.info().last_txnid, 5);
    }

    /// If finishing the election fails, the commit reports it, the election
    /// stays held and nothing is published; the next commit tries again.
    #[test]
    fn a_failed_election_finish_is_retried_by_the_next_commit() {
        let region = Arc::new(HeapRegion::new(4, 1, true));
        let env = open_prev(4, 5, region.clone());
        region.fail_next_finish_open();
        assert!(matches!(put_commit(&env, b"first"), Err(Error::Io(_))));
        assert_eq!(region.finish_opens.load(O::Relaxed), 0);
        assert!(env.inner.holds_election());
        assert_eq!(region.published_txnid().load(O::SeqCst), 4);
        assert!(!writer_held(&region), "the failed commit releases the lock");
        assert_eq!(put_commit(&env, b"again").unwrap(), 5);
        assert_eq!(region.finish_opens.load(O::Relaxed), 1);
        assert!(!env.inner.holds_election());
        assert_eq!(region.published_txnid().load(O::SeqCst), 4);
    }

    /// A heap backing whose reported file length can change after open, to
    /// model a file another process grew or that is shorter than a meta
    /// claims. Its logical map size is `map_size`.
    struct SizedBacking {
        bytes: Vec<u8>,
        map_size: u64,
        real: Arc<AtomicU64>,
    }

    impl Backing for SizedBacking {
        fn view(&self) -> MapView<'_> {
            MapView {
                bytes: &self.bytes,
                map_size: self.map_size,
            }
        }
        fn real_disk_size(&self) -> std::io::Result<u64> {
            // Relaxed: a test knob set before the call under test, on the
            // same thread.
            Ok(self.real.load(O::Relaxed))
        }
        fn try_clone_file(&self) -> std::io::Result<std::fs::File> {
            Err(std::io::Error::from(std::io::ErrorKind::Unsupported))
        }
    }

    #[test]
    fn initializer_publishes_the_newest_meta_as_t() {
        let region = Arc::new(HeapRegion::new(4, 1, true));
        let env = open(backing(4, 5), region.clone());
        assert_eq!(region.published_txnid().load(O::SeqCst), 5);
        assert_eq!(env.inner.txnid(), 5);
    }

    #[test]
    fn joiner_seeds_from_published_txnid_not_newest_meta() {
        // Slot 1 holds txnid 5, written but not yet published: T is still 4.
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(4, O::SeqCst);
        let env = open(backing(4, 5), region.clone());
        assert_eq!(env.inner.snapshot().txnid, 4);
        let (snap, slot) = env.inner.pin_reader().expect("pin");
        assert_eq!(snap.txnid, 4);
        assert_eq!(region.slots()[slot as usize].txnid().load(O::SeqCst), 4);
        assert_eq!(
            crate::shared::owner_pid(region.slots()[slot as usize].owner().load(O::SeqCst)),
            2
        );
        env.inner.release_reader(slot);
        assert_eq!(
            region.slots()[slot as usize].txnid().load(O::SeqCst),
            RDR_FREE
        );
    }

    #[test]
    fn pin_follows_t_published_by_another_process() {
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(4, O::SeqCst);
        let env = open(backing(4, 5), region.clone());
        // Another process publishes 5.
        region.published_txnid().store(5, O::SeqCst);
        let (snap, slot) = env.inner.pin_reader().expect("pin");
        assert_eq!(snap.txnid, 5);
        env.inner.release_reader(slot);
        // The cache was refreshed: the fast path now serves 5.
        assert_eq!(env.inner.snapshot().txnid, 5);
    }

    #[test]
    fn current_snapshot_follows_t() {
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(4, O::SeqCst);
        let env = open(backing(4, 5), region.clone());
        region.published_txnid().store(5, O::SeqCst);
        assert_eq!(env.inner.current_snapshot().unwrap().txnid, 5);
        assert_eq!(env.inner.txnid(), 5);
    }

    #[test]
    fn shared_readers_full_reports_readers_full() {
        let region = Arc::new(HeapRegion::new(1, 2, false));
        region.published_txnid().store(4, O::SeqCst);
        let env = open(backing(4, 5), region.clone());
        let (_snap, slot) = env.inner.pin_reader().expect("pin");
        assert!(matches!(
            env.inner.pin_reader(),
            Err(Error::Mdb(MdbError::ReadersFull))
        ));
        env.inner.release_reader(slot);
    }

    #[test]
    fn joiner_with_undecodable_published_slot_fails_instead_of_hanging() {
        // T = 5 names slot 1, whose CRC never verifies: the seed retry loop
        // must give up with Invalid rather than spin forever.
        let mut bytes = image(4, 5);
        bytes[PS as usize + 100] ^= 0xFF;
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(5, O::SeqCst);
        assert!(matches!(
            try_open(Box::new(VecBacking::new(bytes, 1 << 20)), region),
            Err(Error::Mdb(MdbError::Invalid))
        ));
    }

    #[test]
    fn pin_of_undecodable_published_slot_fails_and_frees_the_slot() {
        // Open at T = 4 (slot 0 is sound); another process then "publishes" 5,
        // whose slot never decodes. Pin and the writer's base both give up.
        let mut bytes = image(4, 5);
        bytes[PS as usize + 100] ^= 0xFF;
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(4, O::SeqCst);
        let env = open(Box::new(VecBacking::new(bytes, 1 << 20)), region.clone());
        region.published_txnid().store(5, O::SeqCst);
        assert!(matches!(
            env.inner.pin_reader(),
            Err(Error::Mdb(MdbError::Invalid))
        ));
        assert!(matches!(
            env.inner.current_snapshot(),
            Err(Error::Mdb(MdbError::Invalid))
        ));
        for rs in region.slots() {
            assert_eq!(rs.txnid().load(O::SeqCst), RDR_FREE);
            assert_eq!(crate::shared::owner_pid(rs.owner().load(O::SeqCst)), 0);
        }
    }

    /// An image whose slot 0 holds txnid 4 (`last_pg` 3) and slot 1 holds
    /// txnid 7 with `last_pg` 200, `pages` pages long.
    fn grown_image(pages: usize) -> Vec<u8> {
        let ps = PS as usize;
        let mut bytes = vec![0u8; ps * pages];
        meta(4).encode_with_annex(&mut bytes[..ps], &[]).unwrap();
        meta_with(7, 200)
            .encode_with_annex(&mut bytes[ps..2 * ps], &[])
            .unwrap();
        bytes
    }

    #[test]
    fn refresh_rejects_a_snapshot_past_the_file_end() {
        // The map covers page 200, but the file (as now measured) does not
        // (SPEC 06 REC-1a, re-checked when a foreign commit is observed).
        let real = Arc::new(AtomicU64::new(256 * u64::from(PS)));
        let b = Box::new(SizedBacking {
            bytes: grown_image(256),
            map_size: 256 * u64::from(PS),
            real: Arc::clone(&real),
        });
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(4, O::SeqCst);
        let env = open(b, region.clone());
        region.published_txnid().store(7, O::SeqCst);
        // Shrink the reported length below page 200. Relaxed: read back on
        // this same thread.
        real.store(4 * u64::from(PS), O::Relaxed);
        assert!(matches!(
            env.inner.pin_reader(),
            Err(Error::Mdb(MdbError::Invalid))
        ));
        // The fast path never re-measures the file: T = 4 again pins fine.
        region.published_txnid().store(4, O::SeqCst);
        let (snap, slot) = env.inner.pin_reader().expect("cached snapshot");
        assert_eq!(snap.txnid, 4);
        env.inner.release_reader(slot);
    }

    #[test]
    fn refresh_rejects_a_snapshot_past_this_process_map() {
        // The file reaches page 200, but this process mapped only 4 pages: a
        // process with a larger map_size grew it. ADR-0023 amendment 15 maps
        // this case to `MapResized`, as LMDB does (`OBS_READ_BEGIN_SMALL_MAP`,
        // `OBS_WRITE_BEGIN_SMALL_MAP`); before the observation it was
        // `Invalid`.
        let b = Box::new(SizedBacking {
            bytes: grown_image(4),
            map_size: 4 * u64::from(PS),
            real: Arc::new(AtomicU64::new(256 * u64::from(PS))),
        });
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(4, O::SeqCst);
        let env = open(b, region.clone());
        region.published_txnid().store(7, O::SeqCst);
        assert!(matches!(
            env.inner.pin_reader(),
            Err(Error::Mdb(MdbError::MapResized))
        ));
        assert!(matches!(
            env.inner.current_snapshot(),
            Err(Error::Mdb(MdbError::MapResized))
        ));
        // The refused write begin releases the cross-process writer lock.
        assert!(matches!(
            env.write_txn(),
            Err(Error::Mdb(MdbError::MapResized))
        ));
        assert!(!writer_held(&region));
    }

    #[test]
    fn snapshot_past_the_mapped_bytes_inside_the_map_size_is_invalid() {
        // A logical map size the mapping does not cover is a bug or
        // corruption, never `MapResized` (ADR-0023 amendment 15).
        let b = Box::new(SizedBacking {
            bytes: grown_image(4),
            map_size: 256 * u64::from(PS),
            real: Arc::new(AtomicU64::new(256 * u64::from(PS))),
        });
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(4, O::SeqCst);
        let env = open(b, region.clone());
        region.published_txnid().store(7, O::SeqCst);
        assert!(matches!(
            env.inner.pin_reader(),
            Err(Error::Mdb(MdbError::Invalid))
        ));
    }

    #[test]
    fn write_begin_checks_a_cached_base_against_the_map() {
        // The writer's base is the cached snapshot (T is unchanged since
        // open); it still must lie inside the logical map size. Here a
        // 1-page map size models a view that no longer covers it.
        let b = Box::new(VecBacking::new(image(4, 5), u64::from(PS)));
        let region = Arc::new(HeapRegion::new(4, 2, true));
        let env = open(b, region.clone());
        assert!(matches!(
            env.write_txn(),
            Err(Error::Mdb(MdbError::MapResized))
        ));
        assert!(!writer_held(&region), "the refused begin releases the lock");
    }

    fn writer_held(region: &HeapRegion) -> bool {
        *region.writer.lock().expect("test region poisoned")
    }

    /// The grow log of a [`GrowBacking`]: `(map_size, cross-process writer
    /// lock held)` per call.
    type GrowLog = Arc<Mutex<Vec<(u64, bool)>>>;

    /// A heap backing whose logical map size grows through `grow_map`, as
    /// the mmap backings' do (ADR-0023 amendment 15). It records every grow
    /// and whether the cross-process writer lock was held at the time.
    struct GrowBacking {
        bytes: Vec<u8>,
        map_size: AtomicU64,
        real: u64,
        region: Option<Arc<HeapRegion>>,
        grows: GrowLog,
    }

    impl Backing for GrowBacking {
        fn view(&self) -> MapView<'_> {
            MapView {
                bytes: &self.bytes,
                // Relaxed: the tests grow and read on one thread.
                map_size: self.map_size.load(O::Relaxed),
            }
        }
        fn grow_map(&self, map_size: u64) -> std::io::Result<()> {
            let held = self.region.as_deref().is_some_and(writer_held);
            self.grows
                .lock()
                .expect("test grows poisoned")
                .push((map_size, held));
            // Relaxed: as in `view`.
            self.map_size.store(map_size, O::Relaxed);
            Ok(())
        }
        fn real_disk_size(&self) -> std::io::Result<u64> {
            Ok(self.real)
        }
        fn try_clone_file(&self) -> std::io::Result<std::fs::File> {
            Err(std::io::Error::from(std::io::ErrorKind::Unsupported))
        }
    }

    /// A [`GrowBacking`] over [`grown_image`]`(256)` with a `map_pages` map
    /// and a file `real_pages` long; also returns its grow log.
    fn grow_backing(
        map_pages: u64,
        real_pages: u64,
        region: Option<Arc<HeapRegion>>,
    ) -> (Box<dyn Backing>, GrowLog) {
        let grows = Arc::new(Mutex::new(Vec::new()));
        let b = Box::new(GrowBacking {
            bytes: grown_image(256),
            map_size: AtomicU64::new(map_pages * u64::from(PS)),
            real: real_pages * u64::from(PS),
            region,
            grows: Arc::clone(&grows),
        });
        (b, grows)
    }

    #[test]
    fn joiner_grows_its_map_when_t_lies_past_it() {
        // Another process committed txnid 7 (last_pg 200) between this
        // process's header read and its seed: the map from the header is 4
        // pages. LMDB never fails an open this way, so the seed grows the
        // map to the snapshot's minimum and re-checks.
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(7, O::SeqCst);
        let (b, grows) = grow_backing(4, 256, None);
        let env = open(b, region.clone());
        assert_eq!(env.inner.snapshot().txnid, 7);
        assert_eq!(env.inner.map_size(), 201 * u64::from(PS));
        assert_eq!(
            grows.lock().unwrap().as_slice(),
            &[(201 * u64::from(PS), false)]
        );
    }

    #[test]
    fn joiner_never_grows_its_map_past_the_file() {
        // T names last_pg 200, but the file holds only 4 pages: corruption
        // (SPEC 06 REC-1a), reported before any growth.
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(7, O::SeqCst);
        let (b, grows) = grow_backing(4, 4, None);
        assert!(matches!(
            try_open(b, region),
            Err(Error::Mdb(MdbError::Invalid))
        ));
        assert!(grows.lock().unwrap().is_empty());
    }

    #[test]
    fn resize_zero_adopts_t_under_the_cross_process_lock() {
        // Open at T = 4 (4-page map), then another process publishes 7
        // (last_pg 200, meta map size 1 MiB): reads fail with MapResized
        // until `resize(0)` adopts the meta's size, which it does holding
        // the cross-process writer lock (decision 3).
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(4, O::SeqCst);
        let (b, grows) = grow_backing(4, 256, Some(Arc::clone(&region)));
        let env = open(b, region.clone());
        region.published_txnid().store(7, O::SeqCst);
        assert!(matches!(
            env.inner.pin_reader(),
            Err(Error::Mdb(MdbError::MapResized))
        ));
        env.resize(0).expect("resize(0)");
        assert_eq!(grows.lock().unwrap().as_slice(), &[(1 << 20, true)]);
        assert!(!writer_held(&region), "resize releases the lock");
        let (snap, slot) = env.inner.pin_reader().expect("pin after resize");
        assert_eq!(snap.txnid, 7);
        env.inner.release_reader(slot);
        // A size below the newest snapshot rounds up to it.
        env.resize(u64::from(PS)).expect("resize(1 page)");
        assert_eq!(env.inner.map_size(), 201 * u64::from(PS));
    }

    #[test]
    fn resize_with_a_live_write_txn_is_einval() {
        let region = Arc::new(HeapRegion::new(4, 2, true));
        let (b, grows) = grow_backing(256, 256, Some(Arc::clone(&region)));
        let env = open(b, region);
        let w = env.write_txn().expect("write txn");
        match env.resize(1 << 20) {
            Err(Error::Io(e)) => assert_eq!(e.raw_os_error(), Some(22)),
            other => panic!("expected Io(EINVAL), got {other:?}"),
        }
        drop(w);
        assert!(grows.lock().unwrap().is_empty());
    }

    #[test]
    fn resize_above_the_cap_is_refused_by_source() {
        let region = Arc::new(HeapRegion::new(4, 2, true));
        let (b, grows) = grow_backing(256, 256, Some(Arc::clone(&region)));
        let env = open(b, region);
        match env.resize(crate::page::geometry::MAX_MAP_SIZE + 1) {
            Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
            other => panic!("expected Io(InvalidInput), got {other:?}"),
        }
        assert!(grows.lock().unwrap().is_empty());
        drop(env);

        // A meta-supplied size above the cap is a corrupt meta: `Invalid`.
        let ps = PS as usize;
        let mut bytes = grown_image(256);
        let mut huge = meta_with(7, 200);
        huge.map_size = crate::page::geometry::MAX_MAP_SIZE + u64::from(PS);
        huge.encode_with_annex(&mut bytes[ps..2 * ps], &[]).unwrap();
        let region = Arc::new(HeapRegion::new(4, 2, true));
        let grows = Arc::new(Mutex::new(Vec::new()));
        let b = Box::new(GrowBacking {
            bytes,
            map_size: AtomicU64::new(256 * u64::from(PS)),
            real: 256 * u64::from(PS),
            region: Some(Arc::clone(&region)),
            grows: Arc::clone(&grows),
        });
        let env = open(b, region);
        assert!(matches!(env.resize(0), Err(Error::Mdb(MdbError::Invalid))));
        assert!(grows.lock().unwrap().is_empty());
    }

    #[test]
    fn write_txn_holds_the_cross_process_lock_and_builds_on_t() {
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(4, O::SeqCst);
        let env = open(backing(4, 5), region.clone());
        // Another process committed 5 after this one opened.
        region.published_txnid().store(5, O::SeqCst);
        assert!(!writer_held(&region));
        let txn = env.write_txn().expect("write txn");
        assert!(writer_held(&region), "the write txn holds the shared lock");
        assert_eq!(
            txn.txnid(),
            6,
            "the base is the snapshot at T, not the cache"
        );
        drop(txn);
        assert!(!writer_held(&region), "dropping the txn releases it");
    }

    #[test]
    fn writer_guard_releases_the_shared_lock_on_drop() {
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(4, O::SeqCst);
        let env = open(backing(4, 5), region.clone());
        let mut guard = env.inner.lock_writer();
        env.inner.lock_writer_shared(&mut guard).expect("lock");
        assert!(writer_held(&region));
        drop(guard);
        assert!(!writer_held(&region));
        // The in-process guard alone (what `lock_writer` returns before
        // `lock_writer_shared` is called) never takes the shared lock.
        let guard = env.inner.lock_writer();
        assert!(!writer_held(&region));
        drop(guard);
    }

    /// The error path of `lock_writer_shared`: a failed cross-process lock
    /// call fails the write-txn begin with that I/O error, leaves the guard
    /// without the shared lock (so its drop issues no unlock for a lock never
    /// taken), and the failed begin releases the in-process writer lock.
    #[test]
    fn failed_shared_lock_call_fails_the_begin_and_frees_the_writer() {
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(4, O::SeqCst);
        let env = open(backing(4, 5), region.clone());

        let mut guard = env.inner.lock_writer();
        region.fail_next_lock();
        match env.inner.lock_writer_shared(&mut guard) {
            Err(Error::Io(e)) => assert_eq!(e.to_string(), "test lock failure"),
            other => panic!("expected the lock call's Io error, got {other:?}"),
        }
        assert!(guard.shared.is_none(), "the guard recorded a failed lock");
        assert!(!writer_held(&region));
        drop(guard);

        region.fail_next_lock();
        match env.write_txn() {
            Err(Error::Io(e)) => assert_eq!(e.to_string(), "test lock failure"),
            Err(other) => panic!("expected the lock call's Io error, got {other:?}"),
            Ok(_) => panic!("the write txn began without the shared lock"),
        }
        assert!(!writer_held(&region));
        // Would block forever if the failed begin had kept the in-process
        // writer lock.
        let txn = env.write_txn().expect("the writer lock is free again");
        assert!(writer_held(&region));
        drop(txn);
        assert!(!writer_held(&region));
    }

    /// A published meta slot with a bad magic or a bad format version (a
    /// torn copy of a slot being rewritten reads as either) is retried, like
    /// a bad CRC: `read_snapshot_at` answers "re-read T" rather than failing,
    /// and only a slot that never decodes ends in `Invalid` once the retry
    /// bound is reached.
    #[test]
    fn bad_magic_or_version_in_the_published_slot_is_retried() {
        // Offsets of the magic and the format version in a meta page
        // (SPEC 02 §3.1).
        for (offset, what) in [(32usize, "magic"), (36, "format version")] {
            let mut bytes = image(4, 5);
            bytes[PS as usize + offset] ^= 0xFF;
            let verdict = MetaPage::validate(&bytes[PS as usize..2 * PS as usize], PS).unwrap();
            match (offset, &verdict) {
                (32, MetaValidity::BadMagic) | (36, MetaValidity::BadVersion(_)) => {}
                _ => panic!("{what}: unexpected verdict {verdict:?}"),
            }
            let b = VecBacking::new(bytes.clone(), 1 << 20);
            assert!(
                matches!(read_snapshot_at(&b, PS, 5), Ok(None)),
                "{what}: a bad slot must ask for a retry"
            );
            // The sound slot still decodes.
            let s = read_snapshot_at(&b, PS, 4).unwrap().expect("slot 0");
            assert_eq!(s.txnid, 4);

            // A joiner whose published slot stays bad gives up with Invalid.
            let region = Arc::new(HeapRegion::new(4, 2, false));
            region.published_txnid().store(5, O::SeqCst);
            assert!(
                matches!(
                    try_open(Box::new(VecBacking::new(bytes.clone(), 1 << 20)), region),
                    Err(Error::Mdb(MdbError::Invalid))
                ),
                "{what}: a slot that never decodes must end in Invalid"
            );

            // A slot that reads bad for a few copies and then decodes (a
            // rewrite in progress, then finished) is retried until it does:
            // the joiner seeds from T = 5.
            let region = Arc::new(HeapRegion::new(4, 2, false));
            region.published_txnid().store(5, O::SeqCst);
            let bad_reads = Arc::new(std::sync::atomic::AtomicU32::new(3));
            let torn = TornMetaBacking {
                inner: VecBacking::new(image(4, 5), 1 << 20),
                offset,
                bad_reads: Arc::clone(&bad_reads),
            };
            let env = try_open(Box::new(torn), region).expect("retried until the slot decoded");
            assert_eq!(env.inner.snapshot().txnid, 5, "{what}");
            // Relaxed: read back on the thread that ran the open.
            assert_eq!(
                bad_reads.load(O::Relaxed),
                0,
                "{what}: the bad copies were read"
            );
        }
    }

    /// A [`VecBacking`] whose first `bad_reads` meta-slot copies have the
    /// byte at `offset` flipped, as a copy torn by a concurrent rewrite of
    /// the slot would read.
    struct TornMetaBacking {
        inner: VecBacking,
        offset: usize,
        bad_reads: Arc<std::sync::atomic::AtomicU32>,
    }

    impl Backing for TornMetaBacking {
        fn view(&self) -> MapView<'_> {
            self.inner.view()
        }
        fn real_disk_size(&self) -> std::io::Result<u64> {
            self.inner.real_disk_size()
        }
        fn try_clone_file(&self) -> std::io::Result<std::fs::File> {
            self.inner.try_clone_file()
        }
        fn read_meta_copy(&self, slot: u64, psize: u32, buf: &mut [u8]) -> std::io::Result<()> {
            self.inner.read_meta_copy(slot, psize, buf)?;
            // Relaxed: a counter touched only by the test's own thread.
            let left = self.bad_reads.load(O::Relaxed);
            if left > 0 {
                self.bad_reads.store(left - 1, O::Relaxed);
                buf[self.offset] ^= 0xFF;
            }
            Ok(())
        }
    }

    #[test]
    fn cache_newer_than_t_decodes_t_and_keeps_the_cache() {
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(4, O::SeqCst);
        let env = open(backing(4, 5), region.clone());
        region.published_txnid().store(5, O::SeqCst);
        let (snap, slot) = env.inner.pin_reader().expect("pin 5");
        assert_eq!(snap.txnid, 5);
        env.inner.release_reader(slot);
        assert_eq!(env.inner.snapshot().txnid, 5, "cache refreshed to 5");
        // T reads 4 again (as a reader racing this process's own publish,
        // between the cache swap and the T store, would see it).
        region.published_txnid().store(4, O::SeqCst);
        let (snap, slot) = env.inner.pin_reader().expect("pin 4");
        assert_eq!(snap.txnid, 4, "decoded from meta slot 0");
        env.inner.release_reader(slot);
        assert_eq!(
            env.inner.snapshot().txnid,
            5,
            "install_if_newer never downgrades the cache"
        );
    }

    #[test]
    fn single_process_path_is_unchanged() {
        let env = testutil::mem_env(PS, 1 << 20);
        assert!(env.inner.shared.is_none());
        assert_eq!(
            env.inner.current_snapshot().unwrap().txnid,
            env.inner.txnid()
        );
    }

    // Reaping (ADR-0023 §3, amendment 12). The LMDB fork, observed by
    // `zerodb-oracle/tests/lmdb_reaping_observation.rs`, reaps dead readers
    // in `mdb_reader_check` and when a writer takes over a dead writer's lock
    // (Linux), and nowhere else: not at read-txn begin, write commit, a
    // joiner's open or a full table. The tests below pin each of those.

    /// Claim and pin a slot on behalf of process `pid`, as that process would
    /// have before it died.
    fn pin_as(env: &Env, pid: u32, txnid: u64) -> u32 {
        let s = env
            .inner
            .reader_table
            .claim_as(Some(pid))
            .expect("a free slot");
        env.inner.reader_table.store_pin(s, txnid);
        s
    }

    fn listed_pids(env: &Env) -> Vec<Option<u32>> {
        env.reader_list().into_iter().map(|e| e.pid).collect()
    }

    #[test]
    fn clear_stale_readers_frees_a_dead_processes_slots() {
        let region = Arc::new(HeapRegion::new(4, 1, true));
        let env = open(backing(4, 5), region.clone());
        pin_as(&env, 2, 1);
        assert_eq!(env.clear_stale_readers().unwrap(), 0, "process 2 is alive");
        region.kill(2);
        assert_eq!(env.clear_stale_readers().unwrap(), 1);
        assert_eq!(env.clear_stale_readers().unwrap(), 0);
        assert!(env.reader_list().is_empty());
    }

    #[test]
    fn clear_stale_readers_counts_slots_not_processes() {
        // `mdb_reader_check` counts slots (amendment 12).
        let region = Arc::new(HeapRegion::new(4, 1, true));
        let env = open(backing(4, 5), region.clone());
        pin_as(&env, 2, 4);
        pin_as(&env, 2, 5);
        region.kill(2);
        assert_eq!(env.clear_stale_readers().unwrap(), 2);
    }

    #[test]
    fn clear_stale_readers_is_zero_without_a_lock_file() {
        let env = testutil::mem_env(PS, 1 << 20);
        let r = env.read_txn().unwrap();
        assert_eq!(env.clear_stale_readers().unwrap(), 0);
        assert_eq!(listed_pids(&env), vec![None], "no PID without a lock file");
        drop(r);
    }

    #[test]
    fn reader_list_reports_pids_in_a_lock_file_env() {
        let region = Arc::new(HeapRegion::new(4, 7, true));
        let env = open(backing(4, 5), region.clone());
        let r = env.read_txn().unwrap();
        let list = env.reader_list();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].pid, Some(7));
        drop(r);
    }

    #[test]
    fn joiner_frees_slots_left_under_its_own_pid() {
        // A region whose initializer (another process) is alive, and in
        // which a dead predecessor with this process's PID (5) left a pin.
        let region = Arc::new(HeapRegion::new(4, 5, true));
        {
            let env = open(backing(4, 5), region.clone()); // seeds T
            drop(env);
        }
        SharedRegion::slots(&*region)[2]
            .owner()
            .store(crate::shared::owner_word(1, 5), O::SeqCst);
        SharedRegion::slots(&*region)[2].txnid().store(1, O::SeqCst);
        let joiner = HeapRegion::rejoin(&region); // is_initializer false
        let env = open(backing(4, 5), joiner.clone());
        assert!(
            env.reader_list().is_empty(),
            "the stale own-PID slot survived open"
        );
    }

    #[test]
    fn joiner_open_does_not_reap_another_dead_process() {
        let region = Arc::new(HeapRegion::new(4, 1, true));
        let first = open(backing(4, 5), region.clone());
        pin_as(&first, 3, 4);
        drop(first);
        let joiner = HeapRegion::rejoin(&region);
        joiner.kill(3);
        let env = open(backing(4, 5), joiner.clone());
        assert_eq!(listed_pids(&env), vec![Some(3)]);
        assert_eq!(env.clear_stale_readers().unwrap(), 1);
    }

    #[test]
    fn full_table_gives_readers_full_and_does_not_reap() {
        let region = Arc::new(HeapRegion::new(1, 1, true));
        let env = open(backing(4, 5), region.clone());
        pin_as(&env, 2, 1);
        region.kill(2);
        assert!(matches!(
            env.read_txn(),
            Err(Error::Mdb(MdbError::ReadersFull))
        ));
        assert_eq!(env.clear_stale_readers().unwrap(), 1);
        env.read_txn().expect("the reaped slot is free again");
    }

    #[test]
    fn read_txn_begin_does_not_reap() {
        let region = Arc::new(HeapRegion::new(4, 1, true));
        let env = open(backing(4, 5), region.clone());
        pin_as(&env, 2, 4);
        region.kill(2);
        let r = env.read_txn().unwrap();
        assert_eq!(listed_pids(&env), vec![Some(2), Some(1)]);
        drop(r);
        assert_eq!(listed_pids(&env), vec![Some(2)]);
    }

    #[test]
    fn write_txn_without_takeover_does_not_reap() {
        let region = Arc::new(HeapRegion::new(4, 1, true));
        let env = open(backing(4, 5), region.clone());
        pin_as(&env, 2, 4);
        region.kill(2);
        let txn = env.write_txn().unwrap();
        assert_eq!(listed_pids(&env), vec![Some(2)], "begin does not reap");
        // An unchanged commit runs the commit path without I/O (the heap
        // backing has no file to write).
        txn.commit().unwrap();
        assert_eq!(listed_pids(&env), vec![Some(2)], "commit does not reap");
    }

    #[test]
    fn writer_takeover_reaps_dead_readers() {
        let region = Arc::new(HeapRegion::new(4, 1, true));
        let env = open(backing(4, 5), region.clone());
        pin_as(&env, 2, 4);
        pin_as(&env, 3, 4);
        region.kill(2);
        region.simulate_takeover_on_next_lock();
        let txn = env.write_txn().unwrap();
        assert_eq!(
            listed_pids(&env),
            vec![Some(3)],
            "the takeover freed the dead process's slot, and only it"
        );
        drop(txn);
    }

    #[test]
    fn a_failed_probe_fails_clear_stale_readers_and_a_takeover_write_txn() {
        // As LMDB's lock recovery: a failed reader check fails the lock call
        // and releases the writer lock.
        let region = Arc::new(HeapRegion::new(4, 1, true));
        let env = open(backing(4, 5), region.clone());
        pin_as(&env, 2, 4);
        region.kill(2);
        region.set_liveness_probes_fail(true);
        assert!(matches!(env.clear_stale_readers(), Err(Error::Io(_))));
        region.simulate_takeover_on_next_lock();
        assert!(matches!(env.write_txn(), Err(Error::Io(_))));
        assert_eq!(listed_pids(&env), vec![Some(2)], "nothing was freed");
        assert!(!writer_held(&region), "the failed begin released the lock");
        region.set_liveness_probes_fail(false);
        // Would block forever if the failed begin had kept the lock.
        let txn = env.write_txn().expect("the writer lock is free again");
        drop(txn);
    }
}
