//! The I/O layer of ZeroDB: maps an env data file, and implements the
//! engine's [`Backing`] over it (positioned `pwrite` and `fdatasync` by
//! default, a writable map and `msync` under `WRITE_MAP`). It also holds the
//! multi-process lock file ([`lockfile`]).
//!
//! This is an internal crate: use the `zerodb` crate, which opens
//! environments through it.
//!
//! The `unsafe` code lives in the map types (`mmap`), the one `pwritev` call
//! (`file`), the lock file's shared-memory primitives ([`lockfile`]), and the
//! test-only heap map (`testmap`); each block states its safety contract.

#![deny(missing_docs)]
#[cfg(feature = "fault")]
pub mod fault;
mod file;
pub mod lockfile;
mod mmap;
#[cfg(feature = "test-backing")]
pub mod testmap;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub use file::probe_page_size;

use mmap::{MapRegions, Mapping, Mmap, MmapWritable};
use zerodb_core::env::{Backing, MapView};
use zerodb_core::error::{Error, MdbError};
use zerodb_core::page::geometry::MAX_MAP_SIZE;
use zerodb_core::page::{MetaPage, MetaValidity};

/// The current view of `regions` as the engine's [`MapView`].
fn map_view(regions: &MapRegions) -> MapView<'_> {
    let (mapping, map_size) = regions.view();
    MapView {
        bytes: mapping.bytes(),
        map_size,
    }
}

/// `n` bytes as a mapping length.
fn map_len(n: u64) -> std::io::Result<usize> {
    usize::try_from(n).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("map size ({n}) exceeds the address space"),
        )
    })
}

/// Publish logical size `map_size` in `regions`.
///
/// When the current mapping is shorter, a new one of
/// `max(map_size, 2 × current length)` bytes is created with `map` and
/// published with it; if that `mmap` fails with `ENOMEM`, it is retried at
/// exactly `map_size`, and a second failure is returned. Otherwise the current
/// mapping is kept and only the logical size changes: nothing is unmapped.
///
/// Callers serialize publications: they hold the writer locks (`resize`), or
/// run at open before the backing is shared (a joiner's seed).
fn grow_regions(
    regions: &MapRegions,
    map_size: u64,
    map: impl Fn(usize) -> std::io::Result<Mapping>,
) -> std::io::Result<()> {
    let current = regions.view().0.bytes().len() as u64;
    if map_size <= current {
        regions.publish(None, map_size);
        return Ok(());
    }
    // Geometric growth bounds the address space all mappings keep: each is
    // at least twice the previous, so together they stay below twice the
    // newest.
    let wanted = map_size.max(current.saturating_mul(2));
    let mapping = match map_len(wanted).and_then(&map) {
        Ok(m) => m,
        // Out of address space (`ENOMEM`), or `wanted` does not fit a
        // `usize`, or `mmap` refused the length with `EINVAL` (which the
        // standard library reports as `InvalidInput`): `map_size` may fit.
        Err(e)
            if wanted > map_size
                && (e.raw_os_error() == Some(libc::ENOMEM)
                    || e.kind() == std::io::ErrorKind::InvalidInput) =>
        {
            map(map_len(map_size)?)?
        }
        Err(e) => return Err(e),
    };
    regions.publish(Some(mapping), map_size);
    Ok(())
}

/// The data file a backing maps, with what both backings do with it alike.
struct DataFile {
    file: File,
    /// The path `file` was opened at, for [`Backing::data_path`].
    path: PathBuf,
    /// `MDB_NORDAHEAD`: every new mapping is advised `MADV_RANDOM`.
    random_access: bool,
}

impl DataFile {
    /// Map the first `len` bytes of the file, writable or read-only.
    fn map(&self, len: usize, writable: bool) -> std::io::Result<Mapping> {
        let mapping = if writable {
            Mapping::Write(MmapWritable::map(&self.file, len)?)
        } else {
            Mapping::Read(Mmap::map(&self.file, len)?)
        };
        if self.random_access {
            mapping.advise_random()?;
        }
        Ok(mapping)
    }

    fn read_meta_copy(&self, slot: u64, psize: u32, buf: &mut [u8]) -> std::io::Result<()> {
        // A kernel copy rather than a map slice (ADR-0023 §4): another process
        // may be rewriting this slot, and borrowing bytes under a concurrent
        // write would be a data race. A torn copy fails its CRC.
        file::read_page_into(&self.file, slot, psize, &mut buf[..psize as usize])
    }
}

/// A [`Backing`] over a memory-mapped env data file.
///
/// It reads through grow-only `MapRegions` holding read-only maps, and
/// commits with `pwrite` and `fdatasync`. Field order is load-bearing for
/// `Drop`: `regions` is declared before `data`, so every mapping is unmapped
/// before the file is closed (TXN-53).
pub struct MmapBacking {
    regions: MapRegions,
    data: DataFile,
}

impl Backing for MmapBacking {
    fn view(&self) -> MapView<'_> {
        map_view(&self.regions)
    }

    fn grow_map(&self, map_size: u64) -> std::io::Result<()> {
        // A read-only map past the file's end is safe: pages there fault only
        // when touched, and readers touch only pages a checked snapshot
        // references, which lie inside the file (SPEC 06 REC-1a).
        grow_regions(&self.regions, map_size, |len| self.data.map(len, false))
    }

    fn real_disk_size(&self) -> std::io::Result<u64> {
        file::real_disk_size(&self.data.file)
    }

    fn try_clone_file(&self) -> std::io::Result<File> {
        self.data.file.try_clone()
    }

    fn data_path(&self) -> Option<&Path> {
        Some(&self.data.path)
    }

    fn read_meta_copy(&self, slot: u64, psize: u32, buf: &mut [u8]) -> std::io::Result<()> {
        self.data.read_meta_copy(slot, psize, buf)
    }

    fn write_at_page(&self, pgno: u64, psize: u32, data: &[u8]) -> std::io::Result<()> {
        // Positioned write through the fd (commit steps C2/C4, SPEC 04 §9).
        // Writing past EOF extends the file, and the shared read map, which
        // already covers the whole map size, sees the new bytes. Readers never
        // see a change: the target is a page no snapshot references (TXN-62)
        // or a meta slot, which is never lent out (see `mmap.rs`).
        file::write_page(&self.data.file, pgno, psize, data)
    }

    fn write_pages_at(&self, start_pgno: u64, psize: u32, frames: &[&[u8]]) -> std::io::Result<()> {
        // One `pwritev` per chunk instead of one `pwrite` per page; the same
        // argument as `write_at_page` keeps readers safe.
        file::write_pages_vectored(&self.data.file, start_pgno, psize, frames)
    }

    fn sync_data(&self) -> std::io::Result<()> {
        // `fdatasync` on Linux, which also flushes the file size needed to
        // read the data back (REC-14); the full-flush path on macOS.
        self.data.file.sync_data()
    }
}

/// A [`Backing`] over a writable memory-mapped env data file
/// (`EnvFlags::WRITE_MAP`, SPEC 01 §S7, SPEC 04 §6.4).
///
/// The commit path copies pages into the map and flushes it with `msync`
/// instead of `pwrite` and `fdatasync`. It also hands out dirty-page slices
/// of the map ([`Backing::map_dirty_page`], ADR-0021), so a write txn builds
/// its new pages in place and commit has nothing left to copy for them, only
/// the `msync` and the meta write. Backings that do not hand out slices stage
/// dirty pages on the heap (TXN-45a).
///
/// Its `MapRegions` hold only writable maps. Field order is load-bearing for
/// `Drop` (`regions` before `data`).
pub struct WriteMapBacking {
    regions: MapRegions,
    data: DataFile,
    /// A lower bound of the data file's length: the last length this process
    /// measured or set. The file never shrinks, so
    /// [`Backing::ensure_file_len`] is one comparison when the file is
    /// already long enough.
    file_len: AtomicU64,
}

impl WriteMapBacking {
    /// The current writable map.
    fn map(&self) -> &MmapWritable {
        match self.regions.view().0 {
            Mapping::Write(m) => m,
            // Construction publishes only writable maps into these regions.
            _ => unreachable!("a WriteMapBacking holds only writable maps"),
        }
    }
}

impl Backing for WriteMapBacking {
    fn view(&self) -> MapView<'_> {
        map_view(&self.regions)
    }

    fn grow_map(&self, map_size: u64) -> std::io::Result<()> {
        // The file first, so every page below the new logical size is backed
        // before the size is published. Only grows.
        self.ensure_file_len(map_size)?;
        grow_regions(&self.regions, map_size, |len| self.data.map(len, true))
    }

    fn ensure_file_len(&self, len: u64) -> std::io::Result<()> {
        // Relaxed: the value is only a lower bound of a length that never
        // decreases, so any value read, however stale, is still a lower
        // bound; a stale one costs one `fstat`. Callers hold both writer
        // locks (or run at open, before the backing is shared), whose
        // mutexes order every update with the next caller's read.
        if self.file_len.load(Ordering::Relaxed) >= len {
            return Ok(());
        }
        // Re-read the length: another process may have grown the file under
        // the writer lock since this one last looked. Grow only if it is
        // shorter, so the file never shrinks under another mapping.
        let real = file::real_disk_size(&self.data.file)?;
        if real < len {
            self.data.file.set_len(len)?;
        }
        // Relaxed: as for the load above.
        self.file_len.fetch_max(real.max(len), Ordering::Relaxed);
        Ok(())
    }

    fn real_disk_size(&self) -> std::io::Result<u64> {
        file::real_disk_size(&self.data.file)
    }

    fn try_clone_file(&self) -> std::io::Result<File> {
        self.data.file.try_clone()
    }

    fn data_path(&self) -> Option<&Path> {
        Some(&self.data.path)
    }

    fn read_meta_copy(&self, slot: u64, psize: u32, buf: &mut [u8]) -> std::io::Result<()> {
        self.data.read_meta_copy(slot, psize, buf)
    }

    fn write_at_page(&self, pgno: u64, psize: u32, data: &[u8]) -> std::io::Result<()> {
        // Commit steps C2/C4 (SPEC 04 §9): copy the page straight into the
        // map. The bytes become durable at the next `sync` (`msync`).
        let off = (pgno as usize).checked_mul(psize as usize).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("page offset overflow: pgno {pgno} * psize {psize}"),
            )
        })?;
        // Typed error instead of the map's own bounds assert: the commit
        // pipeline validates its targets (TXN-62, the GC freelist checks),
        // but a bug or corruption upstream must surface as an I/O error the
        // caller can poison the env on, not a panic mid-commit.
        let end = off.checked_add(data.len()).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "page write overflows usize",
            )
        })?;
        let map = self.map();
        if end > map.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "page write [{off}, {end}) outside the writable map ({} bytes)",
                    map.len()
                ),
            ));
        }
        // SAFETY: `write_at_page` is called only by the single writer (TXN-6)
        // during commit, for a page no snapshot references (TXN-62) or a meta
        // slot, which readers never borrow (TXN-18).
        unsafe { map.write_at(off, data) };
        Ok(())
    }

    fn dirty_in_map(&self) -> bool {
        // This backing hands out dirty-page slices (TXN-45b).
        true
    }

    // The lint fires on `unsafe fn`s too; the trait's contract covers the
    // aliasing it warns about.
    #[allow(clippy::mut_from_ref)]
    unsafe fn map_dirty_page(&self, pgno: u64, psize: u32, pages: u64) -> Option<&mut [u8]> {
        // Out-of-bounds is `None`, not a panic: a page past the map should
        // already have failed allocation as `MapFull`.
        let ps = psize as usize;
        let off = (pgno as usize).checked_mul(ps)?;
        let len = (pages as usize).checked_mul(ps)?;
        let end = off.checked_add(len)?;
        let map = self.map();
        if len == 0 || end > map.len() {
            return None;
        }
        // SAFETY: the caller of this `unsafe fn` upholds `slice_mut`'s
        // contract (single writer, an unshared page, one live `&mut`, views
        // re-derived after each spill), forwarded unchanged.
        Some(unsafe { map.slice_mut(off, len) })
    }

    fn sync_data(&self) -> std::io::Result<()> {
        self.sync(false)
    }

    fn sync(&self, async_flush: bool) -> std::io::Result<()> {
        // SPEC 06 REC-12: `msync(MS_SYNC)` is the durability barrier under
        // WRITE_MAP (or `MS_ASYNC` under MAP_ASYNC). On macOS `msync` alone is
        // not a full barrier, so a synchronous flush additionally `fdatasync`s
        // the data fd (SPEC 01 §S7); an async flush deliberately does neither
        // (relaxed durability, REC-9).
        self.map().flush(async_flush)?;
        if !async_flush {
            self.data.file.sync_data()?;
        }
        Ok(())
    }

    fn ranged_sync(&self) -> bool {
        // SPEC 06 REC-12: the commit barriers `msync` the used prefix and
        // the meta page, not the whole map.
        true
    }

    fn sync_pages(
        &self,
        async_flush: bool,
        psize: u32,
        ranges: &[(u64, u64)],
    ) -> std::io::Result<()> {
        // SPEC 06 REC-12, ranged barrier: one `msync` per byte range, then —
        // for a synchronous flush — the same `fdatasync` of the data fd as
        // the whole-map barrier (`sync` above), which `msync` alone does not
        // replace on macOS. `MAP_ASYNC` issues `MS_ASYNC` and no fdatasync.
        let map = self.map();
        for (off, len) in msync_byte_ranges(psize, ranges)? {
            map.flush_range(off, len, async_flush)?;
        }
        if !async_flush {
            self.data.file.sync_data()?;
        }
        Ok(())
    }
}

/// The largest OS page size ZeroDB targets (64 KiB, ARM distributions).
/// Two page ranges closer than this may share an OS page, which `msync`
/// flushes whole, so [`msync_byte_ranges`] merges them into one call.
const MAX_OS_PAGE: usize = 64 * 1024;

/// Byte ranges `(offset, len)` of the page ranges `(start_pgno, pages)`
/// (ascending, non-overlapping) for the ranged `msync` (SPEC 06 REC-12).
/// Ranges whose gap is under [`MAX_OS_PAGE`] are merged: the merged range
/// contains both, and this avoids flushing an OS page shared by two ranges
/// twice. The output covers every input byte.
///
/// # Errors
///
/// `InvalidInput` if a range's byte offset overflows `usize`.
fn msync_byte_ranges(psize: u32, ranges: &[(u64, u64)]) -> std::io::Result<Vec<(usize, usize)>> {
    let overflow = || {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "msync range overflows usize",
        )
    };
    let ps = psize as usize;
    let mut out: Vec<(usize, usize)> = Vec::with_capacity(ranges.len());
    for &(pgno, pages) in ranges {
        if pages == 0 {
            continue;
        }
        let off = usize::try_from(pgno)
            .ok()
            .and_then(|p| p.checked_mul(ps))
            .ok_or_else(overflow)?;
        let len = usize::try_from(pages)
            .ok()
            .and_then(|n| n.checked_mul(ps))
            .ok_or_else(overflow)?;
        let end = off.checked_add(len).ok_or_else(overflow)?;
        match out.last_mut() {
            Some((start, l)) if off < *start + *l + MAX_OS_PAGE => {
                debug_assert!(off >= *start, "msync ranges must ascend");
                *l = (*l).max(end - *start);
            }
            _ => out.push((off, len)),
        }
    }
    Ok(out)
}

/// The result of opening (or creating) an env data file: the [`Backing`] plus
/// the authoritative page size and whether the file was freshly created. The
/// runtime map size is the backing's: `backing.view().map_size`.
pub struct Opened {
    /// The mmap-backed reader, boxed as the engine's [`Backing`].
    pub backing: Box<dyn Backing>,
    /// The DB page size (from the file, or the requested size for a new env).
    pub page_size: u32,
    /// Whether the file was created by this call.
    pub created: bool,
}

/// Open `data_path`, creating a fresh env there if it does not yet exist or is
/// empty (SPEC 02 §3.4/§3.5, §8). Returns the mapped [`Backing`] and the geometry
/// the engine core needs to select the live meta.
///
/// - `requested_page_size` is used only when **creating**; for an existing file
///   the page size probed from the meta wins (SPEC 02 §3.2).
/// - `requested_map_size` overrides the persisted map size when `Some`; when
///   `None`, the persisted map size (from a valid slot) is used, falling back to
///   `default_map_size` if neither slot is decodable (the file is then invalid
///   and the core will reject it).
///
/// The effective map size is raised to `(last_pg + 1) * page_size` of the
/// newest valid meta when it is smaller, as LMDB does.
///
/// This function performs all filesystem I/O for open; the engine core
/// ([`zerodb_core::env::open_with_backing`]) does the I/O-free selection.
///
/// # Errors
///
/// Propagates I/O errors as [`Error::Io`]. A requested map size above
/// [`MAX_MAP_SIZE`] is `Io(InvalidInput)` and a persisted one is
/// [`MdbError::Invalid`], both before any file is created, grown or mapped.
pub fn open_or_create(
    data_path: &Path,
    requested_page_size: u32,
    requested_map_size: Option<u64>,
    default_map_size: u64,
    read_only: bool,
    write_map: bool,
) -> Result<Opened, Error> {
    open_or_create_with_advice(
        data_path,
        requested_page_size,
        requested_map_size,
        default_map_size,
        read_only,
        write_map,
        false,
        true,
        false,
    )
}

/// [`open_or_create`] with the `MDB_NORDAHEAD` access hint: `random_access`
/// advises the kernel that the map is read at random (`MADV_RANDOM`, no
/// readahead), as LMDB does for that flag. The hint changes no result.
///
/// `grow_file` says whether a `WRITE_MAP` open may grow the data file to the
/// map size: `true` without a lock file and for the lock file's initializer
/// (the only process), `false` for a joiner. A joiner maps the logical size,
/// which may extend past the file's end, and its first write txn grows the
/// file under both writer locks.
///
/// `prev_snapshot` mirrors the engine's `EnvFlags::PREV_SNAPSHOT`: the
/// persisted map size this open adopts
/// (when `requested_map_size` is `None`) and the minimum it rounds a small
/// requested size up to both come from the **older** valid meta generation,
/// not the newest, as the LMDB fork does (observed in the oracle). With only
/// one valid meta, that one is used either way.
///
/// # Errors
///
/// As [`open_or_create`], plus the `madvise` error.
#[allow(clippy::too_many_arguments)]
pub fn open_or_create_with_advice(
    data_path: &Path,
    requested_page_size: u32,
    requested_map_size: Option<u64>,
    default_map_size: u64,
    read_only: bool,
    write_map: bool,
    random_access: bool,
    grow_file: bool,
    prev_snapshot: bool,
) -> Result<Opened, Error> {
    // `WRITE_MAP` needs write access; it is meaningless (and unsupported here)
    // on a read-only env, which maps read-only. LMDB likewise maps a
    // `WRITEMAP|RDONLY` env read-only. So the writable path is taken only when
    // `write_map && !read_only`.
    let write_map = write_map && !read_only;

    // Refuse a map above the cap before any file is created, grown or mapped.
    if let Some(requested) = requested_map_size {
        if requested > MAX_MAP_SIZE {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("map_size ({requested}) exceeds the supported maximum {MAX_MAP_SIZE}"),
            )));
        }
    }

    let exists_nonempty = match std::fs::metadata(data_path) {
        Ok(m) => m.len() > 0,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => return Err(Error::Io(e)),
    };

    if !exists_nonempty {
        if read_only {
            // An RDONLY env cannot create its store (LMDB parity: ENOENT / EACCES).
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "cannot create a read-only environment",
            )));
        }
        let map_size = requested_map_size.unwrap_or(default_map_size);
        let file = file::create_env_file(data_path, requested_page_size, map_size)?;
        let backing = map_backing(
            file,
            data_path,
            map_size,
            write_map,
            random_access,
            grow_file,
        )?;
        return Ok(Opened {
            backing,
            page_size: requested_page_size,
            created: true,
        });
    }

    // Existing file: probe the page size, map it, and read the persisted map
    // size from whichever slot decodes.
    let file = file::open_file(data_path, read_only)?;
    let page_size = file::probe_page_size(&file)?.unwrap_or(requested_page_size);
    let file_len = file::real_disk_size(&file)? as usize;
    if file_len == 0 {
        return Err(Error::Mdb(MdbError::Invalid));
    }
    // SPEC 02 §3.2: an env holds both meta slots in its first `2 * page_size`
    // bytes; any shorter file cannot be a store and is rejected **before the
    // map exists**. Load-bearing, not just tidy: the core reads slot 1 at
    // `[psize, 2*psize)` through the map, and OS pages wholly past EOF fault
    // (SIGBUS) instead of erroring — reachable with a garbage or truncated
    // file whenever `file_len` is at least one OS page short of `2 * psize`.
    // LMDB never faults here because it preads the header before mapping; the
    // parity outcome for both engines is a clean `MDB_INVALID`.
    if file_len < 2 * page_size as usize {
        return Err(Error::Mdb(MdbError::Invalid));
    }
    // Probe the persisted map size from a small head read before mapping, so
    // the mapping can cover the full effective map_size (ADR-0004).
    let head = file::read_head(&file, file_len.min(2 * page_size as usize))?;
    // `PREV_SNAPSHOT` takes its geometry from the older meta generation, as
    // LMDB does; a normal open takes the newest (see the doc comment above).
    let geometry = meta_geometry(&head, page_size, prev_snapshot);
    if let Some((persisted, _)) = geometry {
        // A persisted size above the cap is corrupt.
        if persisted > MAX_MAP_SIZE {
            return Err(Error::Mdb(MdbError::Invalid));
        }
    }
    let mut map_size = requested_map_size
        .or(geometry.map(|(m, _)| m))
        .unwrap_or(default_map_size);
    // A map below the selected generation's data rounds up to exactly
    // `(last_pg + 1) * page_size`, as LMDB does. Only within the file: a meta
    // naming pages past the file end is corrupt and the engine core rejects
    // it (SPEC 06 REC-1a), so never grow a map, or the file, for it.
    if let Some((_, last_pg)) = geometry {
        let min = last_pg
            .checked_add(1)
            .and_then(|n| n.checked_mul(u64::from(page_size)));
        if let Some(min) = min.filter(|&m| m <= file_len as u64) {
            map_size = map_size.max(min);
        }
    }
    let backing = map_backing(
        file,
        data_path,
        map_size,
        write_map,
        random_access,
        grow_file,
    )?;

    Ok(Opened {
        backing,
        page_size,
        created: false,
    })
}

/// Map `file` at the effective `map_size` and box it as a [`Backing`]: the
/// read-only [`MmapBacking`] (default) or the writable [`WriteMapBacking`]
/// (`WRITE_MAP`). The mapping covers `max(file length, map_size)` and is
/// published as view 0 of the backing's grow-only regions, with `map_size` as
/// the logical size. Under `WRITE_MAP`, when
/// `grow_file` is set, the file is first `set_len(map_size)` so every mapped
/// page is backed (SPEC 04 §6.4 — no `SIGBUS` on a store past EOF). A joiner
/// (`grow_file` false) leaves the file alone: its write txns grow it under
/// both writer locks before writing through the map.
fn map_backing(
    file: File,
    path: &Path,
    map_size: u64,
    write_map: bool,
    random_access: bool,
    grow_file: bool,
) -> Result<Box<dyn Backing>, Error> {
    let mut file_len = file::real_disk_size(&file)?;
    let want = map_len(file_len.max(map_size)).map_err(Error::Io)?;
    // Under `WRITE_MAP`, grow the file to cover the whole map so writes
    // anywhere in `[0, map_size)` land in backed (sparse) blocks, not past EOF.
    if write_map && grow_file && file_len < map_size {
        file.set_len(map_size)?;
        file_len = map_size;
    }
    let data = DataFile {
        file,
        path: path.to_path_buf(),
        random_access,
    };
    let regions = MapRegions::new(data.map(want, write_map)?, map_size);
    Ok(if write_map {
        Box::new(WriteMapBacking {
            regions,
            data,
            file_len: AtomicU64::new(file_len),
        })
    } else {
        Box::new(MmapBacking { regions, data })
    })
}

/// The persisted `(map_size, last_pg)` of the newest valid meta slot (the
/// higher txnid; slot 0 on a tie), or, with `prev`, of the older one (the
/// generation `PREV_SNAPSHOT` serves; the LMDB fork reads its open-time map
/// size and minimum from it too, as the oracle observes). With one valid slot,
/// that one either way; `None` if neither decodes.
fn meta_geometry(bytes: &[u8], page_size: u32, prev: bool) -> Option<(u64, u64)> {
    let ps = page_size as usize;
    let valid = [0usize, 1].into_iter().filter_map(|slot| {
        let page = bytes.get(slot * ps..(slot + 1).checked_mul(ps)?)?;
        match MetaPage::validate(page, page_size) {
            Ok(MetaValidity::Valid(m)) => Some((m.txnid, m.map_size, m.last_pg)),
            _ => None,
        }
    });
    let pick = if prev {
        valid.min_by_key(|m| m.0)
    } else {
        valid.reduce(|a, b| if b.0 > a.0 { b } else { a })
    };
    pick.map(|(_, map_size, last_pg)| (map_size, last_pg))
}

#[cfg(test)]
mod tests {
    use super::{msync_byte_ranges, open_or_create, MAX_MAP_SIZE, MAX_OS_PAGE};
    use zerodb_core::error::{Error, MdbError};

    /// SPEC 06 REC-12: the ranged barrier's byte ranges cover every page it
    /// was given; ranges closer than the largest OS page merge into one call.
    #[test]
    fn msync_byte_ranges_cover_every_page() {
        const PS: u32 = 4096;
        let ps = PS as usize;
        let far = (MAX_OS_PAGE / ps) as u64 + 2; // a gap no OS page spans
        let pages = [(0u64, 2u64), (3, 1), (3 + far, 5), (3 + 2 * far, 0)];
        let bytes = msync_byte_ranges(PS, &pages).unwrap();
        assert_eq!(
            bytes,
            vec![(0, 4 * ps), ((3 + far as usize) * ps, 5 * ps)],
            "near ranges merge, far ones stay apart, empty ones vanish"
        );
        for &(p, n) in &pages {
            for pg in p..p + n {
                let (lo, hi) = (pg as usize * ps, (pg as usize + 1) * ps);
                assert!(
                    bytes.iter().any(|&(o, l)| o <= lo && hi <= o + l),
                    "page {pg} not covered by {bytes:?}"
                );
            }
        }
        assert!(msync_byte_ranges(PS, &[(u64::MAX, 1)]).is_err());
    }

    /// The ranged barrier runs on a real writable map, and a range past the
    /// map is an error, not a panic or a silent no-op.
    #[test]
    fn writemap_sync_pages_flushes_ranges_and_rejects_out_of_map() {
        const PS: u32 = 4096;
        const MAP: u64 = 1 << 20;
        let tmp = TempFile::new("ranged-msync");
        let opened = open_or_create(&tmp.0, PS, Some(MAP), MAP, false, true).unwrap();
        let b = opened.backing;
        assert!(b.ranged_sync(), "the writable map flushes ranges");
        b.write_at_page(5, PS, &[0xA5; PS as usize]).unwrap();
        b.sync_pages(false, PS, &[(0, 2), (5, 1)]).unwrap();
        b.sync_pages(true, PS, &[(5, 1)]).unwrap();
        assert_eq!(
            &b.bytes()[5 * PS as usize..6 * PS as usize],
            &[0xA5; PS as usize][..]
        );
        let past = MAP / u64::from(PS);
        let err = b.sync_pages(false, PS, &[(past, 1)]).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    /// A fresh path in the temp dir, removed on drop.
    struct TempFile(std::path::PathBuf);

    impl TempFile {
        fn new(tag: &str) -> TempFile {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let name = format!("zerodb-io-{tag}-{}-{nanos}", std::process::id());
            TempFile(std::env::temp_dir().join(name))
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    /// A persisted meta `map_size` above `MAX_MAP_SIZE` is corrupt: the open
    /// fails with `Invalid` before mapping it, whatever size is requested.
    #[test]
    fn persisted_map_size_above_the_cap_is_invalid() {
        let path = TempFile::new("cap");
        drop(crate::file::create_env_file(&path.0, 4096, MAX_MAP_SIZE + 4096).expect("create"));
        for requested in [None, Some(1 << 20)] {
            match open_or_create(&path.0, 4096, requested, 1 << 20, false, false) {
                Err(Error::Mdb(MdbError::Invalid)) => {}
                Err(e) => panic!("requested {requested:?}: expected Invalid, got {e:?}"),
                Ok(_) => panic!("requested {requested:?}: expected Invalid, got an env"),
            }
        }
    }

    /// `grow_regions`: a size inside the current
    /// mapping only changes the logical size; a larger one adds a mapping of
    /// at least twice the current length, falls back to the exact size on
    /// `ENOMEM`, and returns a second failure.
    #[test]
    fn grow_regions_is_geometric_with_an_enomem_fallback() {
        use super::grow_regions;
        use crate::mmap::{MapRegions, Mapping};
        use std::cell::RefCell;

        let heap = |len: usize| Ok(Mapping::Heap(vec![0u8; len].into_boxed_slice()));
        let regions = MapRegions::new(Mapping::Heap(vec![0u8; 100].into_boxed_slice()), 80);
        let lens = |r: &MapRegions| {
            let (m, size) = r.view();
            (m.bytes().len(), size)
        };

        // Inside the mapping: no new mapping, smaller or larger.
        grow_regions(&regions, 40, |_| panic!("no mapping expected")).unwrap();
        assert_eq!(lens(&regions), (100, 40));
        grow_regions(&regions, 100, |_| panic!("no mapping expected")).unwrap();
        assert_eq!(lens(&regions), (100, 100));

        // Past it: 2x the current mapping.
        let asked = RefCell::new(Vec::new());
        grow_regions(&regions, 150, |len| {
            asked.borrow_mut().push(len);
            heap(len)
        })
        .unwrap();
        assert_eq!(lens(&regions), (200, 150));
        assert_eq!(asked.take(), vec![200]);

        // ENOMEM on the doubled length: retry at exactly the size.
        grow_regions(&regions, 300, |len| {
            asked.borrow_mut().push(len);
            if len == 400 {
                Err(std::io::Error::from_raw_os_error(libc::ENOMEM))
            } else {
                heap(len)
            }
        })
        .unwrap();
        assert_eq!(lens(&regions), (300, 300));
        assert_eq!(asked.take(), vec![400, 300]);

        // A second failure is returned and nothing is published.
        let err = grow_regions(&regions, 1000, |_| {
            Err(std::io::Error::from_raw_os_error(libc::ENOMEM))
        })
        .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ENOMEM));
        assert_eq!(lens(&regions), (300, 300));
    }

    /// A requested `map_size` above the cap is refused before the data file
    /// is created.
    #[test]
    fn requested_map_size_above_the_cap_creates_nothing() {
        let path = TempFile::new("cap-req");
        match open_or_create(&path.0, 4096, Some(MAX_MAP_SIZE + 1), 1 << 20, false, false) {
            Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
            Err(e) => panic!("expected Io(InvalidInput), got {e:?}"),
            Ok(_) => panic!("expected Io(InvalidInput), got an env"),
        }
        assert!(!path.0.exists());
    }
}
