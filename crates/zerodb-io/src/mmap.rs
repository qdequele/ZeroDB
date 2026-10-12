//! Memory maps over an env data file (SPEC 02 §8): the read-only [`Mmap`]
//! (default backing) and the writable [`MmapWritable`] (`EnvFlags::WRITE_MAP`).
//!
//! It also holds [`MapRegions`], the grow-only cell of mappings a backing
//! reads through (ADR-0023 amendment 15).
//!
//! This is one of the sanctioned homes for `unsafe` (the unsafe policy:
//! mmap access). The `unsafe` here: [`Mmap::map`] (read map),
//! [`MmapWritable::map`] (writable map), [`MmapWritable::write_at`] (the
//! `memcpy` into the writable map), [`MmapWritable::slice_mut`] (the
//! ADR-0021 brokered dirty-page slice), and in [`MapRegions`] the view and
//! mapping dereferences, the `Box::from_raw` calls in its `Drop`, and its
//! `Send`/`Sync` impls. Each states its safety contract.

use std::fs::File;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Mutex, PoisonError};

/// A read-only memory map over an env data file.
///
/// Exposes the mapped bytes as `&[u8]` and individual pages by page number.
/// The map (ADR-0004 D4) covers the **full `map_size`** — which
/// may extend past the current end-of-file. The file grows underneath the
/// mapping via `pwrite` (commit C2); a mapping is never remapped in place, and
/// `Env::resize` adds a longer one beside it (`MapRegions`, ADR-0023
/// amendment 15).
pub struct Mmap {
    inner: memmap2::Mmap,
}

impl Mmap {
    /// Map the first `len` bytes of `file` read-only (`MAP_SHARED`).
    ///
    /// `len` must be `> 0`. It may exceed the current file length (ADR-0004
    /// D4): accessing a page wholly beyond EOF raises SIGBUS, so the engine
    /// must only dereference pages a committed snapshot references — which are
    /// always within the file, because a meta is fsynced only after the data
    /// it references (SPEC 06 REC-7/REC-14, SPEC 05 GC-28).
    ///
    /// # Errors
    ///
    /// Propagates the `mmap` I/O error.
    pub fn map(file: &File, len: usize) -> std::io::Result<Mmap> {
        // SAFETY: memmap2's `map` is `unsafe` because a memory map aliases the
        // file's contents and the borrow checker cannot see writers through
        // the fd. Our invariants (SPEC 02 §8, SPEC 04 §9, single-process):
        //  * `file` is a live, open regular file for the whole lifetime of the
        //    returned `Mmap` (the caller keeps the `File` alive alongside it —
        //    see `MmapBacking`);
        //  * accesses are confined to pages referenced by some committed
        //    snapshot, all of which lie within the real file length (REC-14),
        //    so no dereference faults past EOF even though `len` may exceed it;
        //  * the env is single-process: no *other* process writes or
        //    truncates the file. Our *own* commit path writes through the fd
        //    (`MmapBacking::write_at_page`), which mutates the mapped memory —
        //    but only at pages **no live snapshot references** (TXN-62: fresh
        //    pages beyond the committed high-water) and at the meta slots
        //    0/1, which are never handed out as borrows (readers clone the
        //    published snapshot *object*, TXN-18, and never read a meta page
        //    after open). A write txn that spills (SPEC 04 §6.3a) writes
        //    the same TXN-62 pages earlier and may read them back through
        //    the map; it rewrites one only at the start of a later `&mut`
        //    call of that txn, when no borrow into it is live (TXN-39). So no
        //    `&[u8]` observable by safe code is ever mutated while borrowed.
        // The map is read-only (`PROT_READ`); we never write through it.
        let inner = unsafe { memmap2::MmapOptions::new().len(len).map(file)? };
        Ok(Mmap { inner })
    }

    /// Tell the kernel page reads will be random (`madvise(MADV_RANDOM)`):
    /// no readahead around each fault. LMDB applies the same advice for
    /// `MDB_NORDAHEAD` (SPEC 01 Table 1).
    ///
    /// # Errors
    ///
    /// Propagates the `madvise` I/O error.
    pub fn advise_random(&self) -> std::io::Result<()> {
        self.inner.advise(memmap2::Advice::Random)
    }

    /// The whole mapped region.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.inner
    }

    /// Byte length of the mapped region.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the map is empty (never true for a valid env — the two meta
    /// slots are always present).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// The `psize` bytes of page `pgno`, or `None` if that page is not fully
    /// within the mapped region.
    #[must_use]
    pub fn page(&self, pgno: u64, psize: u32) -> Option<&[u8]> {
        let ps = psize as usize;
        let base = (pgno as usize).checked_mul(ps)?;
        let end = base.checked_add(ps)?;
        self.inner.get(base..end)
    }
}

/// A **writable** memory map over an env data file (`EnvFlags::WRITE_MAP`,
/// SPEC 01 §S7, SPEC 04 §6.4). This is the second sanctioned home for mmap
/// `unsafe`: unlike [`Mmap`], the commit path writes *through* this map
/// (`write_at_page` = `memcpy` into the map) and flushes it with `msync`
/// (`sync`), replacing the heap-buffer `pwrite`+`fdatasync` primitive of the
/// default backing.
///
/// Every page below the logical `map_size` MUST be inside the file before
/// anything is stored through the map (SPEC 04 §6.4, matching the fork's
/// `ftruncate`-to-mapsize under writemap): the region is then backed by real
/// (sparse) blocks, so a store anywhere in it — including pages beyond the
/// current logical high-water — never faults with `SIGBUS`. The file is grown
/// at open when the process is alone, and otherwise under both writer locks,
/// at `resize` and at every write-txn begin (ADR-0023 amendment 15); a
/// joiner's map may extend past the file's end until its first write txn.
/// A mapping is never remapped in place; `resize` adds a longer one.
pub struct MmapWritable {
    inner: memmap2::MmapMut,
}

impl MmapWritable {
    /// As [`Mmap::advise_random`], for the writable map.
    ///
    /// # Errors
    ///
    /// Propagates the `madvise` I/O error.
    pub fn advise_random(&self) -> std::io::Result<()> {
        self.inner.advise(memmap2::Advice::Random)
    }

    /// Map the first `len` bytes of `file` read/write (`MAP_SHARED`,
    /// `PROT_READ | PROT_WRITE`). `file` must be open read+write. `len` may
    /// exceed the file; nothing may be stored past the file's end (the
    /// writer grows the file to its logical size first, SPEC 04 §6.4).
    ///
    /// # Errors
    ///
    /// Propagates the `mmap` I/O error.
    pub fn map(file: &File, len: usize) -> std::io::Result<MmapWritable> {
        // SAFETY: the same aliasing contract as `Mmap::map`, extended to writes.
        // memmap2's `map_mut` is `unsafe` because the mapping aliases the file
        // and the borrow checker cannot see writers through the fd. Our
        // invariants (SPEC 04 §9 TXN-62, §6.4, single-process, single-writer TXN-6):
        //  * `file` stays open for the whole life of the returned map (the
        //    caller keeps the `File` alongside it — see `WriteMapBacking`);
        //  * every page a store targets is inside the file: stores happen
        //    only inside a write txn, below the logical map size, after that
        //    txn's begin grew the file to the logical size under both writer
        //    locks (SPEC 04 §6.4, ADR-0023 amendment 15) — a store never
        //    faults past EOF. Pages of the mapping past the file's end (a
        //    joiner's map, or a geometric mapping longer than the logical
        //    size) are never touched; the file never shrinks;
        //  * writes go only through `write_at_page` or the brokered
        //    `slice_mut` (ADR-0021, in-place WRITE_MAP), called only by the
        //    single writer holding the write mutex (TXN-6) — at commit C2/C4,
        //    when it spills (SPEC 04 §6.3a), or at dirty-frame
        //    allocation/edit time (TXN-45b) — and only to
        //    pages **no live snapshot references** (TXN-62: fresh pages beyond
        //    the committed high-water, or GC-reclaimed pages under the
        //    oldest-reader gate) and the meta slots 0/1 (never lent out as
        //    borrows — readers use the published snapshot object, TXN-18); a
        //    spilling write txn (SPEC 04 §6.3a) also writes those TXN-62 pages
        //    mid-txn, each only at the start of a `&mut` call when no borrow
        //    into it is live (TXN-39), and re-derives its whole-map read view
        //    after every spill before resolving a spilled page through it
        //    (TXN-71, ADR-0021 B2). So no
        //    `&[u8]` a reader actually dereferences is mutated while borrowed;
        //  * the env is single-process: no other process writes/truncates.
        let inner = unsafe { memmap2::MmapOptions::new().len(len).map_mut(file)? };
        Ok(MmapWritable { inner })
    }

    /// The whole mapped region as read-only bytes (for the [`Backing::bytes`]
    /// read view). See the `map` SAFETY note: the writer only mutates pages no
    /// reader dereferences (TXN-62).
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.inner
    }

    /// Byte length of the mapped region.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the map is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Copy `data` into the map starting at byte offset `off` (commit C2/C4
    /// through `WriteMapBacking::write_at_page`).
    ///
    /// # Panics
    ///
    /// If `off + data.len()` exceeds the mapped region (a caller bug: a page
    /// beyond `map_size` should have been rejected as `MapFull` first).
    pub fn write_at(&self, off: usize, data: &[u8]) {
        let end = off + data.len();
        assert!(
            end <= self.inner.len(),
            "writemap write [{off}, {end}) exceeds map len {}",
            self.inner.len()
        );
        // SAFETY: `as_ptr()` is valid for the whole `[0, len)` mapping. The write
        // is bounded by the assert above. Exclusivity: `write_at` is reached only
        // from the commit pipeline under the single-writer mutex (TXN-6), and
        // only for TXN-62-safe pages (see `map`), so no `&[u8]` a reader
        // dereferences aliases this store. Non-overlapping: `data` is a caller
        // buffer (dirty frame / meta buffer) distinct from the map.
        unsafe {
            let dst = self.inner.as_ptr().cast_mut().add(off);
            std::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len());
        }
    }

    /// A mutable view of `len` bytes of the map starting at byte offset `off`
    /// — the brokered dirty-page slice of ADR-0021 (SPEC 04 §6.4 TXN-45b):
    /// under in-place `WRITE_MAP` a write txn's dirty page is realized
    /// directly in the map at its freshly-COW'd page number, through this
    /// slice.
    ///
    /// # Safety
    ///
    /// **The brokered contract (ADR-0021 B1).**
    /// This mints `&mut [u8]` from `&self`: the signature cannot express the
    /// exclusivity it relies on (two calls could alias), so the function is
    /// `unsafe` and the obligation is the **caller's** — the sole sanctioned
    /// caller is `zerodb-core::dirty` (per the unsafe policy), which must
    /// guarantee, exactly as [`MmapWritable::write_at`]'s commit-path caller
    /// does:
    ///
    /// - the region belongs to one page (or one overflow run) at a pgno the
    ///   active write txn **allocated** (TXN-62: beyond the committed
    ///   high-water, or GC-reclaimed under the oldest-reader gate), so no live
    ///   snapshot — reader, nested reader on an older txn, or the committed
    ///   trees — references it;
    /// - there is exactly one write txn (TXN-6), and at most one live `&mut`
    ///   into the region exists at a time, tied to `&mut` on the dirty store
    ///   and never stored (TXN-39/41/42);
    /// - no live `&[u8]` view covering the region is read at the written
    ///   locations afterwards — the writer's whole-map read view is
    ///   re-derived after every spill, before any spilled page is resolved
    ///   through it (SPEC 04 TXN-71, ADR-0021 B2).
    ///
    /// # Panics
    ///
    /// If `off + len` exceeds the mapped region (caller bug: allocation past
    /// `map_size` must have been rejected as `MapFull` first).
    #[must_use]
    // clippy cannot see that this is an `unsafe fn` whose documented contract
    // covers exactly what `mut_from_ref` fears (the lint fires on unsafe fns
    // too); ADR-0021 B1's substance is the `unsafe fn` itself.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn slice_mut(&self, off: usize, len: usize) -> &mut [u8] {
        let end = off.checked_add(len).expect("map slice overflows usize");
        assert!(
            end <= self.inner.len(),
            "writemap dirty slice [{off}, {end}) exceeds map len {}",
            self.inner.len()
        );
        // SAFETY: `as_ptr()` is valid for the whole `[0, len)` mapping and the
        // range is bounded by the assert above. The pointer is derived from the
        // map's root allocation (never through a `&[u8]` view), so writes
        // through it do not assert exclusivity over any shared view's tag —
        // the same provenance discipline as `write_at`. Exclusivity of the
        // *region* is the caller's contract above (single writer, TXN-62
        // fresh pgno, one live `&mut` per region via the dirty store, read
        // views re-derived after spills); readers never dereference these
        // pages (TXN-62), so no `&[u8]` that safe code actually reads aliases
        // this slice while it is live.
        unsafe { std::slice::from_raw_parts_mut(self.inner.as_ptr().cast_mut().add(off), len) }
    }

    /// `msync` the whole map. `async_flush` selects `MS_ASYNC` (`MAP_ASYNC`,
    /// SPEC 06 REC-9) over the default `MS_SYNC` (durable barrier, REC-12).
    ///
    /// # Errors
    ///
    /// Propagates the `msync` I/O error.
    pub fn flush(&self, async_flush: bool) -> std::io::Result<()> {
        if async_flush {
            self.inner.flush_async()
        } else {
            self.inner.flush()
        }
    }
}

/// One mmap of the data file (ADR-0023 amendment 15).
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

/// Grow-only mappings (ADR-0023 amendment 15). Every mapping and view is
/// created with `Box::into_raw`, recorded in `owned`, and freed only in Drop.
///
/// `current` points to the newest view. One `Acquire` load of it gives a
/// consistent (mapping, logical map size) pair: the logical size is never
/// stored apart from the mapping it fits in. A new mapping is added only when
/// it is longer than the current one, so mapping lengths never decrease in
/// publication order. A mapping stays mapped until the backing drops, so no
/// borrow of an older mapping is ever invalidated.
pub struct MapRegions {
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
