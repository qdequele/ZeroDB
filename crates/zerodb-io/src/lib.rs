//! zerodb-io — the I/O layer: read-only mmap, plain-file helpers, and the
//! [`Backing`] implementation the engine core reads env files through (and
//! commits through: positioned `pwrite` + `sync_data`).
//!
//! This crate is one of the two sanctioned homes for mmap `unsafe` (the
//! repo's unsafe policy). The `unsafe` blocks live in [`mmap`] (the maps and
//! the grow-only `MapRegions` cell, ADR-0023 amendment 15), the single `pwritev`
//! call in [`file`] (the vectored commit write), the test-only in-memory
//! map stand-in (`testmap`, ADR-0021 M1, behind the `test-backing` feature), and
//! the shared-memory primitives in [`lockfile`] (atomic views over the lock
//! file, `fcntl` record locks, the Linux futex; ADR-0023);
//! everything else is safe `std` I/O — including the [`fault`] crash-injection
//! backend (ADR-0008 D1 Option B: it *wraps* a real backing; its only
//! `unsafe` is the mandatory `unsafe fn` forward of the ADR-0021 brokered
//! map slice, which adds no obligation of its own). An io_uring write
//! backend is not implemented yet.

#![deny(missing_docs)]
#[cfg(feature = "fault")]
pub mod fault;
mod file;
pub mod lockfile;
mod mmap;
#[cfg(feature = "test-backing")]
pub mod testmap;

use std::fs::File;
use std::path::Path;

pub use file::{
    create_env_file, open_file, probe_page_size, read_page, real_disk_size, write_page,
};
pub use mmap::{Mmap, MmapWritable};

use mmap::{MapRegions, Mapping};
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

/// A [`Backing`] over a memory-mapped env data file.
///
/// It reads through grow-only `MapRegions` (ADR-0023 amendment 15) holding
/// read-only maps. Field order is load-bearing for `Drop`: `regions` is
/// declared **before** `file` so every mapping is unmapped before the
/// descriptor is closed (SPEC 04 TXN-53 — "the mmap is unmapped before the
/// file is closed").
pub struct MmapBacking {
    regions: MapRegions,
    file: File,
}

impl Backing for MmapBacking {
    fn view(&self) -> MapView<'_> {
        map_view(&self.regions)
    }

    fn real_disk_size(&self) -> std::io::Result<u64> {
        file::real_disk_size(&self.file)
    }

    fn try_clone_file(&self) -> std::io::Result<File> {
        self.file.try_clone()
    }

    fn read_meta_copy(&self, slot: u64, psize: u32, buf: &mut [u8]) -> std::io::Result<()> {
        // A kernel copy rather than a map slice (ADR-0023 §4): another process
        // may be rewriting this slot, and borrowing bytes under a concurrent
        // write would be a data race. A torn copy fails its CRC.
        file::read_page_into(&self.file, slot, psize, &mut buf[..psize as usize])
    }

    fn write_at_page(&self, pgno: u64, psize: u32, data: &[u8]) -> std::io::Result<()> {
        // Positioned write through the fd (commit C2/C4, SPEC 04 §9). Writing
        // past EOF extends the file; the MAP_SHARED read map observes the new
        // bytes without a remap (ADR-0004 D4 — the map already covers the full
        // map_size). Safe against readers because C2 only ever targets pages
        // no live snapshot references (TXN-62) and the meta pages are never
        // lent out as borrows (readers use the published snapshot object,
        // TXN-18) — see the SAFETY discussion in `mmap.rs`.
        file::write_page(&self.file, pgno, psize, data)
    }

    fn write_pages_at(&self, start_pgno: u64, psize: u32, frames: &[&[u8]]) -> std::io::Result<()> {
        // Batched C2 (commit-write coalescing): one pwritev per chunk instead of one
        // pwrite per dirty page. Same TXN-62 safety argument as
        // `write_at_page` — the target pages are unreferenced by any live
        // snapshot, so racing readers cannot observe the writes.
        file::write_pages_vectored(&self.file, start_pgno, psize, frames)
    }

    fn sync_data(&self) -> std::io::Result<()> {
        // ADR-0004 D3 (OQ3): std's `sync_data` semantics as-is —
        // `fdatasync` on Linux (flushes data + the size metadata needed to
        // read it back, REC-14/GC-28), the full-flush path on macOS.
        self.file.sync_data()
    }
}

/// A [`Backing`] over a **writable** memory-mapped env data file
/// (`EnvFlags::WRITE_MAP`, SPEC 01 §S7, SPEC 04 §6.4).
///
/// The commit path writes dirty pages *through the map* (`write_at_page` =
/// `memcpy` into the map) and flushes with `msync` (`sync`) instead of the
/// default heap-buffer `pwrite` + `fdatasync`. Field order is load-bearing for
/// `Drop` (map unmapped before the fd closes, TXN-53).
///
/// **Realization note (SPEC 04 §6.4 TXN-45b, ADR-0021):** this backing offers
/// the brokered dirty-page slice ([`Backing::map_dirty_page`] /
/// [`MmapWritable::slice_mut`]): the engine core realizes a write txn's dirty
/// pages **directly in the map** at their freshly-COW'd page numbers, so
/// commit C2 has no heap→map copy for them — only `msync` (C3/C5) and the
/// meta write (C4, still through `write_at_page`) remain. The map `unsafe`
/// stays in this crate; `zerodb-core` consumes safe slices whose exclusivity
/// contract is documented on [`MmapWritable::slice_mut`]. The heap-staged
/// realization (TXN-45a) remains the behavior of every backing that does not
/// broker slices (the default mode, the miri test backing, the
/// fault-injection backend).
///
/// Its `MapRegions` hold only writable maps; writes go through the current
/// one. Field order is load-bearing for `Drop` (`regions` before `file`).
pub struct WriteMapBacking {
    regions: MapRegions,
    file: File,
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

    fn real_disk_size(&self) -> std::io::Result<u64> {
        file::real_disk_size(&self.file)
    }

    fn try_clone_file(&self) -> std::io::Result<File> {
        self.file.try_clone()
    }

    fn read_meta_copy(&self, slot: u64, psize: u32, buf: &mut [u8]) -> std::io::Result<()> {
        // A kernel copy rather than a map slice (ADR-0023 §4): another process
        // may be rewriting this slot, and borrowing bytes under a concurrent
        // write would be a data race. A torn copy fails its CRC.
        file::read_page_into(&self.file, slot, psize, &mut buf[..psize as usize])
    }

    fn write_at_page(&self, pgno: u64, psize: u32, data: &[u8]) -> std::io::Result<()> {
        // Commit C2/C4 (SPEC 04 §9): copy the dirty frame / meta buffer straight
        // into the writable map at the page's on-disk offset. TXN-62 guarantees
        // the target page is not referenced by any live snapshot (see the
        // `MmapWritable::map` SAFETY note). No `pwrite`; the bytes become durable
        // only at the next `sync` (`msync`).
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
        map.write_at(off, data);
        Ok(())
    }

    fn dirty_in_map(&self) -> bool {
        // ADR-0021: this backing brokers dirty-page slices, so the engine
        // core realizes dirty pages in the map (SPEC 04 §6.4 TXN-45b).
        true
    }

    // clippy cannot see that this is an `unsafe fn` whose documented contract
    // covers exactly what `mut_from_ref` fears (the lint fires on unsafe fns
    // too); ADR-0021 B1's substance is the `unsafe fn` itself.
    #[allow(clippy::mut_from_ref)]
    unsafe fn map_dirty_page(&self, pgno: u64, psize: u32, pages: u64) -> Option<&mut [u8]> {
        // ADR-0021 brokered dirty-page slice. Bounds are typed (`None`), not
        // asserted: a pgno past `map_size` must already have failed
        // allocation as `MapFull`, so a miss here is a caller bug surfaced
        // gently. The exclusivity contract is `MmapWritable::slice_mut`'s.
        let ps = psize as usize;
        let off = (pgno as usize).checked_mul(ps)?;
        let len = (pages as usize).checked_mul(ps)?;
        let end = off.checked_add(len)?;
        let map = self.map();
        if len == 0 || end > map.len() {
            return None;
        }
        // SAFETY: the caller of this `unsafe fn` discharges `slice_mut`'s
        // brokered contract (single writer, TXN-62 region, one live `&mut`,
        // read views re-derived per spill) — forwarded verbatim.
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
            self.file.sync_data()?;
        }
        Ok(())
    }
}

/// The result of opening (or creating) an env data file: the [`Backing`] plus
/// the authoritative page size, the effective runtime map size, and whether the
/// file was freshly created.
pub struct Opened {
    /// The mmap-backed reader, boxed as the engine's [`Backing`].
    pub backing: Box<dyn Backing>,
    /// The DB page size (from the file, or the requested size for a new env).
    pub page_size: u32,
    /// The effective runtime map size (the requested size, else the persisted
    /// one; SPEC 02 §8), raised to `(last_pg + 1) * page_size` when smaller
    /// (ADR-0023 amendment 15). The backing's view carries the same size.
    pub map_size: u64,
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
/// newest valid meta when it is smaller, as LMDB does
/// (`OBS_OPEN_BELOW_LAST_PG = "minsize"`, ADR-0023 amendment 15).
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
    )
}

/// [`open_or_create`] with the `MDB_NORDAHEAD` access hint: `random_access`
/// advises the kernel that the map is read at random (`MADV_RANDOM`, no
/// readahead), as LMDB does for that flag. The hint changes no result.
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
) -> Result<Opened, Error> {
    // `WRITE_MAP` needs write access; it is meaningless (and unsupported here)
    // on a read-only env, which maps read-only. LMDB likewise maps a
    // `WRITEMAP|RDONLY` env read-only. So the writable path is taken only when
    // `write_map && !read_only`.
    let write_map = write_map && !read_only;

    // ADR-0023 amendment 15, decision 6: refuse a map above the cap before
    // any file is created, grown or mapped.
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
        let backing = map_backing(file, map_size, write_map, random_access)?;
        return Ok(Opened {
            backing,
            page_size: requested_page_size,
            map_size,
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
    // the mapping can cover the full effective map_size (ADR-0004 D4).
    let head = file::read_head(&file, file_len.min(2 * page_size as usize))?;
    let newest = newest_meta_geometry(&head, page_size);
    if let Some((persisted, _)) = newest {
        // A persisted size above the cap is corrupt (ADR-0023 amendment 15).
        if persisted > MAX_MAP_SIZE {
            return Err(Error::Mdb(MdbError::Invalid));
        }
    }
    let mut map_size = requested_map_size
        .or(newest.map(|(m, _)| m))
        .unwrap_or(default_map_size);
    // A map below the newest commit's data rounds up to exactly
    // `(last_pg + 1) * page_size`, as LMDB does (`OBS_OPEN_BELOW_LAST_PG =
    // "minsize"`). Only within the file: a meta naming pages past the file
    // end is corrupt, and the engine core rejects it (SPEC 06 REC-1a) —
    // never grow a map, or under WRITE_MAP the file, for it.
    if let Some((_, last_pg)) = newest {
        let min = last_pg
            .checked_add(1)
            .and_then(|n| n.checked_mul(u64::from(page_size)));
        if let Some(min) = min.filter(|&m| m <= file_len as u64) {
            map_size = map_size.max(min);
        }
    }
    let backing = map_backing(file, map_size, write_map, random_access)?;

    Ok(Opened {
        backing,
        page_size,
        map_size,
        created: false,
    })
}

/// Map `file` at the effective `map_size` and box it as a [`Backing`]: the
/// read-only [`MmapBacking`] (default) or the writable [`WriteMapBacking`]
/// (`WRITE_MAP`). The mapping covers `max(file length, map_size)` and is
/// published as view 0 of the backing's grow-only regions, with `map_size` as
/// the logical size (ADR-0023 amendment 15). Under `WRITE_MAP` the file is
/// first `set_len(map_size)` so every mapped page is backed (SPEC 04 §6.4 — no
/// `SIGBUS` on a store past EOF).
fn map_backing(
    file: File,
    map_size: u64,
    write_map: bool,
    random_access: bool,
) -> Result<Box<dyn Backing>, Error> {
    let file_len = file::real_disk_size(&file)? as usize;
    let want = file_len.max(map_size as usize);
    if write_map {
        // Grow the file to cover the whole map so writes anywhere in
        // `[0, map_size)` land in backed (sparse) blocks, not past EOF.
        if (file_len as u64) < map_size {
            file.set_len(map_size)?;
        }
        let mmap = MmapWritable::map(&file, want)?;
        if random_access {
            mmap.advise_random()?;
        }
        let regions = MapRegions::new(Mapping::Write(mmap), map_size);
        Ok(Box::new(WriteMapBacking { regions, file }))
    } else {
        let mmap = Mmap::map(&file, want)?;
        if random_access {
            mmap.advise_random()?;
        }
        let regions = MapRegions::new(Mapping::Read(mmap), map_size);
        Ok(Box::new(MmapBacking { regions, file }))
    }
}

/// The persisted `(map_size, last_pg)` of the newest valid meta slot (the
/// higher-txnid one), or `None` if neither decodes. This is the meta LMDB
/// reads its open-time map size and minimum from.
fn newest_meta_geometry(bytes: &[u8], page_size: u32) -> Option<(u64, u64)> {
    let ps = page_size as usize;
    let mut best: Option<(u64, u64, u64)> = None; // (txnid, map_size, last_pg)
    for slot in [0usize, 1] {
        let base = slot * ps;
        let end = base.checked_add(ps)?;
        if end > bytes.len() {
            continue;
        }
        if let Ok(MetaValidity::Valid(m)) = MetaPage::validate(&bytes[base..end], page_size) {
            match best {
                Some((t, _, _)) if t >= m.txnid => {}
                _ => best = Some((m.txnid, m.map_size, m.last_pg)),
            }
        }
    }
    best.map(|(_, ms, lp)| (ms, lp))
}

#[cfg(test)]
mod tests {
    use super::{open_or_create, MAX_MAP_SIZE};
    use zerodb_core::error::{Error, MdbError};

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
    /// fails with `Invalid` before mapping it, whatever size is requested
    /// (ADR-0023 amendment 15, decision 6).
    #[test]
    fn persisted_map_size_above_the_cap_is_invalid() {
        let path = TempFile::new("cap");
        drop(super::create_env_file(&path.0, 4096, MAX_MAP_SIZE + 4096).expect("create"));
        for requested in [None, Some(1 << 20)] {
            match open_or_create(&path.0, 4096, requested, 1 << 20, false, false) {
                Err(Error::Mdb(MdbError::Invalid)) => {}
                Err(e) => panic!("requested {requested:?}: expected Invalid, got {e:?}"),
                Ok(_) => panic!("requested {requested:?}: expected Invalid, got an env"),
            }
        }
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
