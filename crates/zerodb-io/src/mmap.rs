//! Read-only memory map over an env data file (SPEC 02 §8).
//!
//! This is one of the sanctioned homes for `unsafe` (CLAUDE.md unsafe policy:
//! mmap access). The single `unsafe` block is [`Mmap::map`]; its safety
//! contract is stated there.

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
