//! zerodb-io — the I/O layer: read-only mmap, plain-file helpers, and the
//! [`Backing`] implementation the engine core reads env files through (and,
//! from M1.4 on, commits through: positioned `pwrite` + `sync_data`).
//!
//! This crate is one of the two sanctioned homes for mmap `unsafe` (CLAUDE.md
//! unsafe policy). The `unsafe` blocks live in [`mmap`] and the single `pwritev`
//! call in [`file`] (PERF-GAP B4); everything else is
//! safe `std` I/O — including the [`fault`] crash-injection backend (M1.11,
//! ADR-0008 D1 Option B: it *wraps* a real backing, adding zero `unsafe`). The
//! io_uring write backend arrives in Phase 3.5.

pub mod fault;
mod file;
mod mmap;

use std::fs::File;
use std::path::Path;

pub use file::{
    create_env_file, open_file, probe_page_size, read_page, real_disk_size, write_page,
};
pub use mmap::{Mmap, MmapWritable};

use zerodb_core::env::Backing;
use zerodb_core::error::{Error, MdbError};
use zerodb_core::page::{MetaPage, MetaValidity};

/// A [`Backing`] over a memory-mapped env data file.
///
/// Field order is load-bearing for `Drop`: `mmap` is declared **before** `file`
/// so the map is unmapped before the descriptor is closed (SPEC 04 TXN-53 — "the
/// mmap is unmapped before the file is closed").
pub struct MmapBacking {
    mmap: Mmap,
    file: File,
}

impl Backing for MmapBacking {
    fn bytes(&self) -> &[u8] {
        self.mmap.bytes()
    }

    fn real_disk_size(&self) -> std::io::Result<u64> {
        file::real_disk_size(&self.file)
    }

    fn try_clone_file(&self) -> std::io::Result<File> {
        self.file.try_clone()
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
        // Batched C2 (PERF-GAP B4): one pwritev per chunk instead of one
        // pwrite per dirty page. Same TXN-62 safety argument as
        // `write_at_page` — the target pages are unreferenced by any live
        // snapshot, so racing readers cannot observe the writes.
        file::write_pages_vectored(&self.file, start_pgno, psize, frames)
    }

    fn sync_data(&self) -> std::io::Result<()> {
        // ADR-0004 D3 as amended (OQ3): std's `sync_data` semantics as-is —
        // `fdatasync` on Linux (flushes data + the size metadata needed to
        // read it back, REC-14/GC-28), the full-flush path on macOS.
        self.file.sync_data()
    }
}

impl MmapBacking {
    /// Access the mapped bytes (used by the engine core through [`Backing`]).
    #[must_use]
    pub fn map(&self) -> &Mmap {
        &self.mmap
    }
}

/// A [`Backing`] over a **writable** memory-mapped env data file
/// (`EnvFlags::WRITE_MAP`, SPEC 01 §S7, SPEC 04 §6.4; M1.10).
///
/// The commit path writes dirty pages *through the map* (`write_at_page` =
/// `memcpy` into the map) and flushes with `msync` (`sync`) instead of the
/// default heap-buffer `pwrite` + `fdatasync`. Field order is load-bearing for
/// `Drop` (map unmapped before the fd closes, TXN-53).
///
/// **Realization note (SPEC 04 §6.4, amended M1.10):** during a write txn the
/// dirty bytes still live in the engine-core heap dirty-page store (so the
/// value-borrow contract, nested-reader reads, and abort-by-drop are byte-for-
/// byte identical to the default mode, and `zerodb-core` needs no map `unsafe`);
/// they are copied into the writable map at commit **C2** via `write_at_page`
/// and made durable by `msync` at C3/C5. This is observably identical to the
/// fork's live-map writes through the heed surface; true zero-copy live-map
/// mutation is a Phase-3 optimization (needs a bench and a `zerodb-io`-brokered
/// map-slice API to keep the map `unsafe` out of `zerodb-core`).
pub struct WriteMapBacking {
    mmap: MmapWritable,
    file: File,
}

impl Backing for WriteMapBacking {
    fn bytes(&self) -> &[u8] {
        self.mmap.bytes()
    }

    fn real_disk_size(&self) -> std::io::Result<u64> {
        file::real_disk_size(&self.file)
    }

    fn try_clone_file(&self) -> std::io::Result<File> {
        self.file.try_clone()
    }

    fn write_at_page(&self, pgno: u64, psize: u32, data: &[u8]) -> std::io::Result<()> {
        // Commit C2/C4 (SPEC 04 §9): copy the dirty frame / meta buffer straight
        // into the writable map at the page's on-disk offset. TXN-62 guarantees
        // the target page is not referenced by any live snapshot (see the
        // `MmapWritable::map` SAFETY note). No `pwrite`; the bytes become durable
        // only at the next `sync` (`msync`).
        let off = (pgno as usize)
            .checked_mul(psize as usize)
            .expect("page offset overflow");
        self.mmap.write_at(off, data);
        Ok(())
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
        self.mmap.flush(async_flush)?;
        if !async_flush {
            self.file.sync_data()?;
        }
        Ok(())
    }
}

impl WriteMapBacking {
    /// Access the writable map (used by the engine core through [`Backing`]).
    #[must_use]
    pub fn map(&self) -> &MmapWritable {
        &self.mmap
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
    /// one; SPEC 02 §8).
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
/// This function performs all filesystem I/O for open; the engine core
/// ([`zerodb_core::env::open_with_backing`]) does the I/O-free selection.
///
/// # Errors
///
/// Propagates I/O errors as [`Error::Io`].
pub fn open_or_create(
    data_path: &Path,
    requested_page_size: u32,
    requested_map_size: Option<u64>,
    default_map_size: u64,
    read_only: bool,
    write_map: bool,
) -> Result<Opened, Error> {
    // `WRITE_MAP` needs write access; it is meaningless (and unsupported here)
    // on a read-only env, which maps read-only. LMDB likewise maps a
    // `WRITEMAP|RDONLY` env read-only. So the writable path is taken only when
    // `write_map && !read_only`.
    let write_map = write_map && !read_only;

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
        let backing = map_backing(file, map_size, write_map)?;
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
    let persisted = persisted_map_size(&head, page_size);
    let map_size = requested_map_size.or(persisted).unwrap_or(default_map_size);
    let backing = map_backing(file, map_size, write_map)?;

    Ok(Opened {
        backing,
        page_size,
        map_size,
        created: false,
    })
}

/// Map `file` at the effective `map_size` and box it as a [`Backing`]: the
/// read-only [`MmapBacking`] (default) or the writable [`WriteMapBacking`]
/// (`WRITE_MAP`). ADR-0004 D4: the map covers the full `map_size` once, no
/// remap. Under `WRITE_MAP` the file is first `set_len(map_size)` so every
/// mapped page is backed (SPEC 04 §6.4 — no `SIGBUS` on a store past EOF).
fn map_backing(file: File, map_size: u64, write_map: bool) -> Result<Box<dyn Backing>, Error> {
    let file_len = file::real_disk_size(&file)? as usize;
    let want = file_len.max(map_size as usize);
    if write_map {
        // Grow the file to cover the whole map so writes anywhere in
        // `[0, map_size)` land in backed (sparse) blocks, not past EOF.
        if (file_len as u64) < map_size {
            file.set_len(map_size)?;
        }
        let mmap = MmapWritable::map(&file, want)?;
        Ok(Box::new(WriteMapBacking { mmap, file }))
    } else {
        let mmap = Mmap::map(&file, want)?;
        Ok(Box::new(MmapBacking { mmap, file }))
    }
}

/// The persisted `map_size` from whichever meta slot validates (prefer the
/// higher-txnid valid slot), or `None` if neither decodes.
fn persisted_map_size(bytes: &[u8], page_size: u32) -> Option<u64> {
    let ps = page_size as usize;
    let mut best: Option<(u64, u64)> = None; // (txnid, map_size)
    for slot in [0usize, 1] {
        let base = slot * ps;
        let end = base.checked_add(ps)?;
        if end > bytes.len() {
            continue;
        }
        if let Ok(MetaValidity::Valid(m)) = MetaPage::validate(&bytes[base..end], page_size) {
            match best {
                Some((t, _)) if t >= m.txnid => {}
                _ => best = Some((m.txnid, m.map_size)),
            }
        }
    }
    best.map(|(_, ms)| ms)
}
