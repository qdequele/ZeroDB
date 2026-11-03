//! Memory maps over an env data file (SPEC 02 §8): the read-only [`Mmap`]
//! (default backing) and the writable [`MmapWritable`] (`EnvFlags::WRITE_MAP`),
//! plus [`MapRegions`], the grow-only cell of mappings a backing reads through
//! (ADR-0023).
//!
//! # Aliasing contract
//!
//! A map aliases the file, and the borrow checker cannot see writes made
//! through the fd or through another mapping. Every `unsafe` block below
//! relies on these invariants, which the engine upholds:
//!
//! 1. **The file outlives the map.** The backing keeps its `File` next to the
//!    mapping and drops the mapping first.
//! 2. **Nothing faults past EOF.** Readers dereference only pages a committed
//!    snapshot references, all inside the file (SPEC 06 REC-14). Under
//!    `WRITE_MAP`, the writer grows the file to the logical map size before it
//!    stores through the map (SPEC 04 §6.4), at its begin and before each
//!    growth inside the txn (ADR-0036). A reader that follows another
//!    process's growth raises the logical size only to a committed snapshot's
//!    size, which lies inside the file. No process truncates the file.
//! 3. **One writer, writing only unshared pages.** One write txn at a time
//!    across processes (TXN-6, ADR-0023 §2). It writes only pages no live
//!    snapshot references (TXN-62: fresh pages, or pages freed before the
//!    oldest reader) and the two meta slots, which are never lent out as
//!    borrows (readers use the published snapshot object, TXN-18; a meta is
//!    read back as a `pread` copy whose CRC rejects a torn slot).
//! 4. **No live borrow is rewritten.** A spilling write txn rewrites one of its
//!    own pages only at the start of a later `&mut` call, when no borrow into
//!    it is live (TXN-39), and re-derives its whole-map view after each spill
//!    (TXN-71) and after each growth of the map inside the txn (ADR-0036).
//!
//! These invariants are about file pages, not about one mapping: an older
//! mapping and a newer one show the same page-cache pages, so a store made
//! through either is visible through both, and a flush through the newest
//! mapping writes back stores made through an older one (tested below).
//!
//! So no `&[u8]` that safe code reads is mutated while it is borrowed.

use std::fs::File;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Mutex, PoisonError};

/// A read-only (`PROT_READ`, `MAP_SHARED`) map over an env data file.
///
/// The map may extend past the end of the file: the file grows underneath it
/// through `pwrite`, and a longer map is added beside it on resize
/// ([`MapRegions`]); a mapping is never remapped in place.
pub(crate) struct Mmap {
    inner: memmap2::Mmap,
}

impl Mmap {
    /// Map the first `len` bytes of `file` read-only. `len` must be `> 0` and
    /// may exceed the file length.
    ///
    /// # Errors
    ///
    /// Propagates the `mmap` I/O error.
    pub(crate) fn map(file: &File, len: usize) -> std::io::Result<Mmap> {
        // SAFETY: the module's aliasing contract: the caller keeps `file` open
        // for the map's life (1), only in-file pages are dereferenced (2), and
        // writes through the fd touch only pages no borrow can see (3, 4).
        let inner = unsafe { memmap2::MmapOptions::new().len(len).map(file)? };
        Ok(Mmap { inner })
    }

    /// Advise the kernel that reads are random (`madvise(MADV_RANDOM)`: no
    /// readahead), as LMDB does for `MDB_NORDAHEAD`.
    ///
    /// # Errors
    ///
    /// Propagates the `madvise` I/O error.
    pub(crate) fn advise_random(&self) -> std::io::Result<()> {
        self.inner.advise(memmap2::Advice::Random)
    }

    /// The whole mapped region.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.inner
    }
}

/// A writable (`PROT_READ | PROT_WRITE`, `MAP_SHARED`) map over an env data
/// file, used under `EnvFlags::WRITE_MAP` (SPEC 04 §6.4): the commit path
/// copies pages into it and flushes it with `msync` instead of `pwrite` and
/// `fdatasync`.
///
/// Every page below the logical map size must be inside the file before
/// anything is stored through the map, so a store never faults with `SIGBUS`.
pub(crate) struct MmapWritable {
    inner: memmap2::MmapMut,
}

impl MmapWritable {
    /// As [`Mmap::advise_random`], for the writable map.
    ///
    /// # Errors
    ///
    /// Propagates the `madvise` I/O error.
    pub(crate) fn advise_random(&self) -> std::io::Result<()> {
        self.inner.advise(memmap2::Advice::Random)
    }

    /// Map the first `len` bytes of `file` read/write. `file` must be open
    /// read+write; `len` may exceed the file, but nothing may be stored past
    /// its end.
    ///
    /// # Errors
    ///
    /// Propagates the `mmap` I/O error.
    pub(crate) fn map(file: &File, len: usize) -> std::io::Result<MmapWritable> {
        // SAFETY: the module's aliasing contract (1-4). Stores go only through
        // `write_at` and `slice_mut`, whose callers keep to it: one writer,
        // in-file pages no snapshot references, no live borrow rewritten.
        let inner = unsafe { memmap2::MmapOptions::new().len(len).map_mut(file)? };
        Ok(MmapWritable { inner })
    }

    /// The whole mapped region as read-only bytes.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.inner
    }

    /// Byte length of the mapped region.
    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }

    /// Copy `data` into the map at byte offset `off` (commit steps C2/C4).
    ///
    /// # Safety
    ///
    /// The caller must be the single writer (TXN-6), and no live `&[u8]`
    /// obtained from [`MmapWritable::bytes`] (or any other view of the file)
    /// may be read at `[off, off + data.len())` while this runs: the target
    /// must be a page no snapshot references (TXN-62) or a meta slot.
    ///
    /// # Panics
    ///
    /// If `off + data.len()` exceeds the mapped region.
    pub(crate) unsafe fn write_at(&self, off: usize, data: &[u8]) {
        let end = off + data.len();
        assert!(
            end <= self.inner.len(),
            "writemap write [{off}, {end}) exceeds map len {}",
            self.inner.len()
        );
        // SAFETY: `as_ptr()` is valid for the whole mapping and the range is
        // bounded by the assert. The caller guarantees no reader aliases the
        // target (see `# Safety`). `data` is a caller buffer, not the map, so
        // the ranges do not overlap. The pointer comes from the map's root
        // allocation, never through a `&[u8]` view.
        unsafe {
            let dst = self.inner.as_ptr().cast_mut().add(off);
            std::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len());
        }
    }

    /// A mutable view of `len` bytes of the map at byte offset `off`: the
    /// brokered dirty-page slice of ADR-0021, through which an in-place
    /// `WRITE_MAP` write txn builds a dirty page directly in the map (SPEC 04
    /// §6.4 TXN-45b).
    ///
    /// # Safety
    ///
    /// This mints `&mut [u8]` from `&self`, so exclusivity is the caller's
    /// obligation. The only caller is `zerodb-core::dirty`, which guarantees:
    ///
    /// - the region is one page (or overflow run) the active write txn
    ///   allocated, so no snapshot references it (TXN-62);
    /// - there is one write txn (TXN-6), and at most one live `&mut` into the
    ///   region, tied to `&mut` on the dirty store and never stored
    ///   (TXN-39/41/42);
    /// - no older `&[u8]` view is read at these bytes afterwards: the writer's
    ///   whole-map view is re-derived after every spill (TXN-71).
    ///
    /// # Panics
    ///
    /// If `off + len` exceeds the mapped region.
    #[must_use]
    // The lint fires on `unsafe fn`s too; the `# Safety` contract above covers
    // exactly the aliasing it warns about.
    #[allow(clippy::mut_from_ref)]
    pub(crate) unsafe fn slice_mut(&self, off: usize, len: usize) -> &mut [u8] {
        let end = off.checked_add(len).expect("map slice overflows usize");
        assert!(
            end <= self.inner.len(),
            "writemap dirty slice [{off}, {end}) exceeds map len {}",
            self.inner.len()
        );
        // SAFETY: the range is inside the mapping (assert above) and the
        // pointer comes from the map's root allocation, as in `write_at`.
        // Exclusivity of the region is the caller's contract (`# Safety`).
        unsafe { std::slice::from_raw_parts_mut(self.inner.as_ptr().cast_mut().add(off), len) }
    }

    /// `msync` the whole map: `MS_ASYNC` when `async_flush` (`MAP_ASYNC`,
    /// SPEC 06 REC-9), else the durable `MS_SYNC` barrier (REC-12).
    ///
    /// # Errors
    ///
    /// Propagates the `msync` I/O error.
    pub(crate) fn flush(&self, async_flush: bool) -> std::io::Result<()> {
        if async_flush {
            self.inner.flush_async()
        } else {
            self.inner.flush()
        }
    }

    /// `msync` the `len` bytes at byte offset `off` (the ranged barrier, SPEC
    /// 06 REC-12). The start is rounded down to an OS page boundary, so the
    /// flushed region always contains the requested one.
    ///
    /// # Errors
    ///
    /// `InvalidInput` if the range leaves the mapped region; otherwise
    /// propagates the `msync` I/O error.
    pub(crate) fn flush_range(
        &self,
        off: usize,
        len: usize,
        async_flush: bool,
    ) -> std::io::Result<()> {
        match off.checked_add(len) {
            Some(end) if end <= self.inner.len() => {}
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "msync range at {off} (+{len}) outside the writable map ({} bytes)",
                        self.inner.len()
                    ),
                ))
            }
        }
        if len == 0 {
            return Ok(());
        }
        if async_flush {
            self.inner.flush_async_range(off, len)
        } else {
            self.inner.flush_range(off, len)
        }
    }
}

/// One mmap of the data file (ADR-0023).
pub(crate) enum Mapping {
    /// A read-only map (the default backing).
    Read(Mmap),
    /// A writable map (`WRITE_MAP`).
    Write(MmapWritable),
    /// Heap bytes standing in for a map, so the [`MapRegions`] protocol runs
    /// under miri, which cannot see OS mappings.
    #[cfg(test)]
    Heap(Box<[u8]>),
}

impl Mapping {
    /// Advise the kernel that reads are random (see [`Mmap::advise_random`]).
    pub(crate) fn advise_random(&self) -> std::io::Result<()> {
        match self {
            Mapping::Read(m) => m.advise_random(),
            Mapping::Write(m) => m.advise_random(),
            #[cfg(test)]
            Mapping::Heap(_) => Ok(()),
        }
    }

    /// The whole mapped region.
    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            Mapping::Read(m) => m.bytes(),
            Mapping::Write(m) => m.bytes(),
            #[cfg(test)]
            Mapping::Heap(b) => b,
        }
    }
}

/// A published (mapping, logical map size) pair. Immutable once published.
pub(crate) struct ViewCell {
    mapping: *const Mapping,
    map_size: u64,
}

/// Every mapping and view a [`MapRegions`] ever created, freed only in its
/// `Drop`.
struct Owned {
    mappings: Vec<*mut Mapping>,
    views: Vec<*mut ViewCell>,
}

/// Grow-only mappings (ADR-0023). Every mapping and view is
/// created with `Box::into_raw`, recorded in `owned`, and freed only in Drop.
///
/// `current` points to the newest view. One `Acquire` load of it gives a
/// consistent (mapping, logical map size) pair: the logical size is never
/// stored apart from the mapping it fits in. A new mapping is added only when
/// it is longer than the current one, so mapping lengths never decrease in
/// publication order. A mapping stays mapped until the backing drops, so no
/// borrow of an older mapping is ever invalidated.
pub(crate) struct MapRegions {
    current: AtomicPtr<ViewCell>,
    owned: Mutex<Owned>,
}

// SAFETY: the raw pointers are uniquely owned boxes of `Send + Sync` types
// (`Mmap`/`MmapWritable` are `Send + Sync`, as is `Box<[u8]>`); `owned` is
// mutated only under its mutex and `current` is atomic. Nothing is freed
// before Drop.
unsafe impl Send for MapRegions {}
// SAFETY: as for `Send`. Shared access only loads `current` (atomically) and
// reads immutable, never-freed views and mappings; `update` mutates `owned`
// under the mutex.
unsafe impl Sync for MapRegions {}

// Pins the `Send`/`Sync` argument above: every mapping type the raw pointers
// own must itself be `Send + Sync`, or this fails to compile.
const _: () = {
    fn assert_send_sync<T: Send + Sync>() {}
    let _ = assert_send_sync::<Mapping>;
};

impl MapRegions {
    /// Publishes view 0 over `first`.
    pub(crate) fn new(first: Mapping, map_size: u64) -> MapRegions {
        let mapping = Box::into_raw(Box::new(first));
        let view = Box::into_raw(Box::new(ViewCell {
            mapping: mapping.cast_const(),
            map_size,
        }));
        MapRegions {
            current: AtomicPtr::new(view),
            owned: Mutex::new(Owned {
                mappings: vec![mapping],
                views: vec![view],
            }),
        }
    }

    /// One Acquire load: a consistent (bytes, map_size).
    pub(crate) fn view(&self) -> (&Mapping, u64) {
        // Acquire: pairs with the Release in `publish_locked`; the view and its
        // mapping were fully built before the store.
        let v = self.current.load(Ordering::Acquire);
        // SAFETY: `v` came from `Box::into_raw` in `publish_locked`/`new` and is
        // recorded in `owned`; views and mappings are freed only in Drop
        // (`&mut self`), so both outlive `&self`. No `&mut` to them ever
        // exists after publication.
        let v = unsafe { &*v };
        // SAFETY: as above, for the mapping `v` points to.
        (unsafe { &*v.mapping }, v.map_size)
    }

    /// Publish `map_size`, adding `new_mapping` if given, unconditionally
    /// (the tests' direct path; the engine publishes through
    /// [`MapRegions::update`]).
    #[cfg(test)]
    pub(crate) fn publish(&self, new_mapping: Option<Mapping>, map_size: u64) {
        let mut owned = self.owned.lock().expect("map regions poisoned");
        self.publish_locked(&mut owned, new_mapping, map_size);
    }

    /// Decide and publish a new view under the regions mutex (ADR-0036):
    /// `plan` gets the current mapping's length and logical size, read under
    /// the mutex, and returns the view to publish (a new mapping or `None`
    /// to keep the current one, and the logical size), or `None` to publish
    /// nothing. Holding the mutex across the decision, the `mmap` and the
    /// publication serializes every caller, so a caller that does not hold
    /// the writer lock (a reader following another process's growth) can
    /// never publish over a newer view it did not see.
    ///
    /// # Errors
    ///
    /// The error `plan` returns; nothing is published then.
    pub(crate) fn update(
        &self,
        plan: impl FnOnce(u64, u64) -> std::io::Result<Option<(Option<Mapping>, u64)>>,
    ) -> std::io::Result<()> {
        let mut owned = self.owned.lock().expect("map regions poisoned");
        // Relaxed: under the mutex, which orders this load after the
        // previous publish's store.
        let cur = self.current.load(Ordering::Relaxed);
        // SAFETY: as in `view`: `cur` is a published, never-freed view, and
        // so is the mapping it points to.
        let (len, size) = unsafe { ((*(*cur).mapping).bytes().len() as u64, (*cur).map_size) };
        if let Some((mapping, map_size)) = plan(len, size)? {
            self.publish_locked(&mut owned, mapping, map_size);
        }
        Ok(())
    }

    /// Publish a view with the regions mutex held.
    fn publish_locked(&self, owned: &mut Owned, new_mapping: Option<Mapping>, map_size: u64) {
        let mapping = match new_mapping {
            Some(m) => {
                let p = Box::into_raw(Box::new(m));
                owned.mappings.push(p);
                p.cast_const()
            }
            None => {
                // Relaxed: under the mutex, which orders this load after the
                // previous publish's store.
                let cur = self.current.load(Ordering::Relaxed);
                // SAFETY: as in `view`: `cur` is a published, never-freed view.
                unsafe { (*cur).mapping }
            }
        };
        let v = Box::into_raw(Box::new(ViewCell { mapping, map_size }));
        owned.views.push(v);
        // Release: the view and its mapping are fully constructed first.
        self.current.store(v, Ordering::Release);
    }
}

impl Drop for MapRegions {
    fn drop(&mut self) {
        // A poisoned mutex still holds every pointer: free them anyway, so no
        // mapping outlives the descriptor and Drop never panics mid-unwind.
        let owned = self.owned.get_mut().unwrap_or_else(PoisonError::into_inner);
        for v in owned.views.drain(..) {
            // SAFETY: each pointer came from `Box::into_raw` exactly once and
            // is freed exactly once; `&mut self` proves no borrow is live.
            drop(unsafe { Box::from_raw(v) });
        }
        for m in owned.mappings.drain(..) {
            // SAFETY: as above.
            drop(unsafe { Box::from_raw(m) });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MapRegions, Mapping};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    /// A heap mapping of `len` bytes, every byte `fill`.
    fn heap(len: usize, fill: u8) -> Mapping {
        Mapping::Heap(vec![fill; len].into_boxed_slice())
    }

    /// One thread publishes three growing views while two threads read
    /// through `view()`. Every view a reader loads is consistent: the mapping
    /// covers the logical size, and the last byte of the logical size holds
    /// the generation that mapping was built with. Under miri, the data-race
    /// detector checks the Release/Acquire pairing that makes the mapping's
    /// bytes visible to a reader that loaded its view.
    #[test]
    fn map_regions_publish_is_seen_consistently() {
        // Generations: 64 bytes of 1s (sizes 48, then 64 without a new
        // mapping), 128 bytes of 2s (size 100), 512 bytes of 3s (size 300).
        let regions = Arc::new(MapRegions::new(heap(64, 1), 48));
        let done = Arc::new(AtomicBool::new(false));
        let readers: Vec<_> = (0..2)
            .map(|_| {
                let regions = Arc::clone(&regions);
                let done = Arc::clone(&done);
                std::thread::spawn(move || {
                    let mut last = 0u64;
                    loop {
                        // Acquire: pairs with the Release store of `done`
                        // after the final publish.
                        let finished = done.load(Ordering::Acquire);
                        let (m, size) = regions.view();
                        let bytes = m.bytes();
                        assert!(bytes.len() as u64 >= size, "mapping shorter than its size");
                        assert!(size >= last, "a later load returned an older view");
                        last = size;
                        let tail = bytes[size as usize - 1];
                        let want = match size {
                            48 | 64 => 1,
                            100 => 2,
                            300 => 3,
                            other => panic!("unexpected size {other}"),
                        };
                        assert_eq!(tail, want, "size {size}");
                        if finished {
                            assert_eq!(size, 300, "the final view is visible");
                            return;
                        }
                    }
                })
            })
            .collect();
        regions.publish(None, 64);
        regions.publish(Some(heap(128, 2)), 100);
        regions.publish(Some(heap(512, 3)), 300);
        // Release: orders the publishes before a reader's Acquire of `done`.
        done.store(true, Ordering::Release);
        for r in readers {
            r.join().expect("reader");
        }
        let (m, size) = regions.view();
        assert_eq!((m.bytes().len(), size), (512, 300));
    }
}

/// Two writable mappings of one file (ADR-0036): a store made through the
/// older, shorter mapping must be written back by a flush through the newer
/// one, since commit flushes only through the newest view. Writeback is
/// observed through the process's disk-write counter (`/proc/self/io` on
/// Linux, `proc_pid_rusage` on macOS) and, on macOS, through `mincore`'s
/// modified bits.
#[cfg(all(test, not(miri), any(target_os = "linux", target_os = "macos")))]
mod cross_mapping_flush_tests {
    use super::MmapWritable;
    use std::fs::{File, OpenOptions};

    struct TempFile(std::path::PathBuf);

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn os_page() -> usize {
        // SAFETY: `sysconf` reads a process constant; no memory is touched.
        let n = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        usize::try_from(n).expect("positive page size")
    }

    /// Bytes this process has caused to be written to storage.
    #[cfg(target_os = "linux")]
    fn disk_bytes_written() -> Option<u64> {
        let io = std::fs::read_to_string("/proc/self/io").ok()?;
        io.lines()
            .find_map(|l| l.strip_prefix("write_bytes: "))?
            .trim()
            .parse()
            .ok()
    }

    /// Bytes this process has caused to be written to storage.
    #[cfg(target_os = "macos")]
    fn disk_bytes_written() -> Option<u64> {
        let mut info = std::mem::MaybeUninit::<libc::rusage_info_v2>::zeroed();
        // SAFETY: `info` is a writable `rusage_info_v2`, the struct the
        // `RUSAGE_INFO_V2` flavor fills; the kernel writes nothing else.
        let rc = unsafe {
            libc::proc_pid_rusage(
                libc::getpid(),
                libc::RUSAGE_INFO_V2,
                info.as_mut_ptr().cast::<libc::rusage_info_t>(),
            )
        };
        // SAFETY: on success the kernel initialized the whole struct.
        (rc == 0).then(|| unsafe { info.assume_init() }.ri_diskio_byteswritten)
    }

    /// Whether the OS page at `off` of `map` is dirty (macOS `mincore`).
    #[cfg(target_os = "macos")]
    fn dirty(map: &MmapWritable, off: usize) -> bool {
        let mut vec: libc::c_char = 0;
        // SAFETY: `off` is OS-page aligned and inside the mapping, so the
        // one-page range is mapped; `vec` holds the one status byte.
        let rc =
            unsafe { libc::mincore(map.bytes().as_ptr().add(off).cast_mut().cast(), 1, &mut vec) };
        assert_eq!(rc, 0, "mincore: {}", std::io::Error::last_os_error());
        i32::from(vec) & (libc::MINCORE_MODIFIED | libc::MINCORE_MODIFIED_OTHER) != 0
    }

    #[cfg(target_os = "linux")]
    fn dirty(_: &MmapWritable, _: usize) -> bool {
        // Linux `mincore` reports residency only.
        false
    }

    fn store(map: &MmapWritable, off: usize, len: usize, fill: u8) {
        // SAFETY: single-threaded test, no borrow of the map is live, and
        // the range is inside both the mapping and the file.
        unsafe { map.write_at(off, &vec![fill; len]) };
    }

    /// How a store through `old` is made durable in each case below.
    fn written_back(file: &File, old: &MmapWritable, new: &MmapWritable, how: &str) -> u64 {
        let os = os_page();
        let off = os; // page 1: inside both mappings
        let fill = how.len() as u8;
        store(old, off, os, fill);
        if cfg!(target_os = "macos") {
            assert!(
                dirty(old, off) || dirty(new, off),
                "{how}: the store left the page clean"
            );
        }
        let before = disk_bytes_written().expect("disk-write counter");
        match how {
            "msync-old" => old.flush_range(off, os, false).expect("msync"),
            "msync-new" => new.flush_range(off, os, false).expect("msync"),
            "fdatasync" => file.sync_data().expect("fdatasync"),
            _ => unreachable!(),
        }
        let after = disk_bytes_written().expect("disk-write counter");
        assert!(
            !dirty(old, off) && !dirty(new, off),
            "{how}: page still dirty"
        );
        assert_eq!(
            new.bytes()[off],
            fill,
            "{how}: the newer mapping sees the store"
        );
        after - before
    }

    #[test]
    fn a_flush_through_a_newer_mapping_writes_back_older_stores() {
        let os = os_page();
        let path = TempFile(std::env::current_dir().expect("cwd").join(format!(
            "zerodb-cross-map-{}-{}.dat",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        )));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path.0)
            .expect("create");
        file.set_len(8 * os as u64).expect("set_len");
        // The older mapping covers 2 OS pages, the newer one the whole file,
        // as after a growth.
        let old = MmapWritable::map(&file, 2 * os).expect("map old");
        let new = MmapWritable::map(&file, 8 * os).expect("map new");
        if disk_bytes_written().is_none() {
            eprintln!("no disk-write counter on this system; skipped");
            return;
        }
        // Control: the flush through the mapping that stored.
        let control = written_back(&file, &old, &new, "msync-old");
        eprintln!("msync through the storing mapping: {control} bytes written");
        if control == 0 {
            eprintln!("the counter did not move for the control; skipped");
            return;
        }
        let through_new = written_back(&file, &old, &new, "msync-new");
        eprintln!("msync through the newer mapping: {through_new} bytes written");
        assert!(
            through_new >= os as u64,
            "msync through the newer mapping wrote nothing"
        );
        let fsync = written_back(&file, &old, &new, "fdatasync");
        eprintln!("fdatasync alone: {fsync} bytes written");
        assert!(fsync >= os as u64, "fdatasync wrote nothing");
    }
}
