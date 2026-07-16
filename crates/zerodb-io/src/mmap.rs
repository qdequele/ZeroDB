//! Read-only memory map over an env data file (SPEC 02 §8).
//!
//! This is one of the sanctioned homes for `unsafe` (CLAUDE.md unsafe policy:
//! mmap access). The single `unsafe` block is [`Mmap::map`]; its safety
//! contract is stated there.

use std::fs::File;

/// A read-only memory map of (a prefix of) an env data file.
///
/// Exposes the mapped bytes as `&[u8]` and individual pages by page number. The
/// map covers exactly `len` bytes, which the caller guarantees is `<=` the file
/// length so no access can fault past end-of-file (SPEC 02 §8; single-process
/// D-001 rules out concurrent truncation).
pub struct Mmap {
    inner: memmap2::Mmap,
}

impl Mmap {
    /// Map the first `len` bytes of `file` read-only.
    ///
    /// `len` must be `> 0` and `<=` the current file length.
    ///
    /// # Errors
    ///
    /// Propagates the `mmap` I/O error.
    pub fn map(file: &File, len: usize) -> std::io::Result<Mmap> {
        // SAFETY: memmap2's `map` is `unsafe` because a memory map aliases the
        // file's contents and the borrow checker cannot see external writers.
        // Our invariants (SPEC 02 §8, D-001 single-process):
        //  * `file` is a live, open regular file for the whole lifetime of the
        //    returned `Mmap` (the caller keeps the `File` alive alongside it —
        //    see `MmapBacking`);
        //  * `len <= file length` (asserted by the caller), so every byte of the
        //    map is backed by a real page and no access faults past EOF;
        //  * the env is single-process (D-001): no other process truncates or
        //    concurrently writes the file, so the mapping stays valid and the
        //    `&[u8]` we hand out is not mutated underneath a reader.
        // The map is read-only (`PROT_READ`), so we never write through it.
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
