//! Memory maps over an env data file (SPEC 02 §8): the read-only [`Mmap`]
//! (default backing) and the writable [`MmapWritable`] (`EnvFlags::WRITE_MAP`,
//! M1.10).
//!
//! This is one of the sanctioned homes for `unsafe` (CLAUDE.md unsafe policy:
//! mmap access). There are three `unsafe` blocks: [`Mmap::map`] (read map),
//! [`MmapWritable::map`] (writable map), and [`MmapWritable::write_at`] (the
//! `memcpy` into the writable map); each states its safety contract.

use std::fs::File;

/// A read-only memory map over an env data file.
///
/// Exposes the mapped bytes as `&[u8]` and individual pages by page number.
/// From M1.4 on (ADR-0004 D4) the map covers the **full `map_size`** — which
/// may extend past the current end-of-file. The file grows underneath the
/// fixed mapping via `pwrite` (commit C2); no remap ever happens in Phase 1.
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
        // the fd. Our invariants (SPEC 02 §8, SPEC 04 §9, D-001):
        //  * `file` is a live, open regular file for the whole lifetime of the
        //    returned `Mmap` (the caller keeps the `File` alive alongside it —
        //    see `MmapBacking`);
        //  * accesses are confined to pages referenced by some committed
        //    snapshot, all of which lie within the real file length (REC-14),
        //    so no dereference faults past EOF even though `len` may exceed it;
        //  * the env is single-process (D-001): no *other* process writes or
        //    truncates the file. Our *own* commit path writes through the fd
        //    (`MmapBacking::write_at_page`), which mutates the mapped memory —
        //    but only at pages **no live snapshot references** (TXN-62: fresh
        //    pages beyond the committed high-water) and at the meta slots
        //    0/1, which are never handed out as borrows (readers clone the
        //    published snapshot *object*, TXN-18, and never read a meta page
        //    after open). So no `&[u8]` observable by safe code is ever
        //    mutated while borrowed.
        // The map is read-only (`PROT_READ`); we never write through it.
        let inner = unsafe { memmap2::MmapOptions::new().len(len).map(file)? };
        Ok(Mmap { inner })
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
/// The file MUST be `set_len(map_size)` before mapping (SPEC 04 §6.4, matching
/// the fork's `ftruncate`-to-mapsize under writemap): the whole `[0, map_size)`
/// region is then backed by real (sparse) blocks, so a store anywhere in it —
/// including pages beyond the current logical high-water — never faults with
/// `SIGBUS`. ADR-0004 D4 (map once, no remap) holds: the map covers the full
/// `map_size` for the env's life.
pub struct MmapWritable {
    inner: memmap2::MmapMut,
}

impl MmapWritable {
    /// Map the first `len` bytes of `file` read/write (`MAP_SHARED`,
    /// `PROT_READ | PROT_WRITE`). `file` must be open read+write and at least
    /// `len` bytes long (the caller `set_len`s it first, SPEC 04 §6.4).
    ///
    /// # Errors
    ///
    /// Propagates the `mmap` I/O error.
    pub fn map(file: &File, len: usize) -> std::io::Result<MmapWritable> {
        // SAFETY: the same aliasing contract as `Mmap::map`, extended to writes.
        // memmap2's `map_mut` is `unsafe` because the mapping aliases the file
        // and the borrow checker cannot see writers through the fd. Our
        // invariants (SPEC 04 §9 TXN-62, §6.4, D-001, single-writer TXN-6):
        //  * `file` stays open for the whole life of the returned map (the
        //    caller keeps the `File` alongside it — see `WriteMapBacking`);
        //  * the file was `set_len(map_size)` so every page in `[0, len)` is
        //    backed — a store never faults past EOF;
        //  * writes go only through `write_at_page`, called only by the single
        //    writer holding the write mutex (TXN-6) at commit C2/C4, and only to
        //    pages **no live snapshot references** (TXN-62: fresh pages beyond
        //    the committed high-water, or GC-reclaimed pages under the
        //    oldest-reader gate) and the meta slots 0/1 (never lent out as
        //    borrows — readers use the published snapshot object, TXN-18). So no
        //    `&[u8]` a reader actually dereferences is mutated while borrowed;
        //  * the env is single-process (D-001): no other process writes/truncates.
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
