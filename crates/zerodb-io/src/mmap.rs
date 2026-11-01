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
//!    stores through the map (SPEC 04 §6.4). No process truncates the file.
//! 3. **One writer, writing only unshared pages.** One write txn at a time
//!    across processes (TXN-6, ADR-0023 §2). It writes only pages no live
//!    snapshot references (TXN-62: fresh pages, or pages freed before the
//!    oldest reader) and the two meta slots, which are never lent out as
//!    borrows (readers use the published snapshot object, TXN-18; a meta is
//!    read back as a `pread` copy whose CRC rejects a torn slot).
//! 4. **No live borrow is rewritten.** A spilling write txn rewrites one of its
//!    own pages only at the start of a later `&mut` call, when no borrow into
//!    it is live (TXN-39), and re-derives its whole-map view after each spill
//!    (TXN-71).
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
// reads immutable, never-freed views and mappings; `publish` mutates `owned`
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
        // Acquire: pairs with the Release in `publish`; the view and its
        // mapping were fully built before the store.
        let v = self.current.load(Ordering::Acquire);
        // SAFETY: `v` came from `Box::into_raw` in `publish`/`new` and is
        // recorded in `owned`; views and mappings are freed only in Drop
        // (`&mut self`), so both outlive `&self`. No `&mut` to them ever
        // exists after publication.
        let v = unsafe { &*v };
        // SAFETY: as above, for the mapping `v` points to.
        (unsafe { &*v.mapping }, v.map_size)
    }

    /// Publish `map_size`, adding `new_mapping` if given. Callers hold the
    /// writer lock, so publications are serialized.
    pub(crate) fn publish(&self, new_mapping: Option<Mapping>, map_size: u64) {
        let mut owned = self.owned.lock().expect("map regions poisoned");
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
