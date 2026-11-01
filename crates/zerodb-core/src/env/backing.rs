//! The [`Backing`] trait: how the I/O-free engine reaches the mapped file.

use super::*;

/// A consistent (mapped bytes, logical map size) pair (ADR-0023),
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
/// Implementors must be `Send + Sync`: an `EnvInner` is shared across threads
/// through an `Arc` and read txns hand out `Send` borrows.
pub trait Backing: Send + Sync {
    /// The current mapped bytes and logical map size, as one consistent pair
    /// (ADR-0023). A mapped backing's bytes are at least as long
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
    /// file grows only under both writer locks (ADR-0023).
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
    /// (ADR-0023).
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
    /// **The brokered contract (ADR-0021).**
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
    ///   after every spill (TXN-71, ADR-0021).
    // `unsafe fn` *declaration* only (ADR-0021: no safe fn may mint `&mut`
    // from `&self`); the default body is trivially safe and the one unsafe
    // *call* lives in `crate::dirty::map_mut`, the home the AGENTS.md policy
    // sanctions for the WRITE_MAP in-place brokered map-slice write.
    #[allow(unsafe_code)]
    // clippy cannot see that this is an `unsafe fn` whose documented contract
    // covers exactly what `mut_from_ref` fears (the lint fires on unsafe fns
    // too); ADR-0021's substance is the `unsafe fn` itself.
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
    /// `File::sync_data` — ADR-0004: std semantics as-is,
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
