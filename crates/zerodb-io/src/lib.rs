//! zerodb-io — the I/O layer: read-only mmap, plain-file helpers, and the
//! [`Backing`] implementation the engine core reads env files through (and,
//! from M1.4 on, commits through: positioned `pwrite` + `sync_data`).
//!
//! This crate is one of the two sanctioned homes for mmap `unsafe` (CLAUDE.md
//! unsafe policy). The single `unsafe` block is in [`mmap`]; everything else is
//! safe `std` I/O. The io_uring write backend and the fault-injection backend
//! (M1.11) arrive later.

mod file;
mod mmap;

use std::fs::File;
use std::path::Path;

pub use file::{
    create_env_file, open_file, probe_page_size, read_page, real_disk_size, write_page,
};
pub use mmap::Mmap;

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
) -> Result<Opened, Error> {
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
        let file_len = file::real_disk_size(&file)? as usize;
        // ADR-0004 D4 (approved OQ2): map the full map_size once at open —
        // the base address is fixed for the env's life and file growth needs
        // no remap. Pages beyond EOF are never dereferenced (engine
        // discipline; see `Mmap::map`).
        let mmap = Mmap::map(&file, file_len.max(map_size as usize))?;
        return Ok(Opened {
            backing: Box::new(MmapBacking { mmap, file }),
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
    // Probe the persisted map size from a small head read before mapping, so
    // the mapping can cover the full effective map_size (ADR-0004 D4).
    let head = file::read_head(&file, file_len.min(2 * page_size as usize))?;
    let persisted = persisted_map_size(&head, page_size);
    let map_size = requested_map_size.or(persisted).unwrap_or(default_map_size);
    let mmap = Mmap::map(&file, file_len.max(map_size as usize))?;

    Ok(Opened {
        backing: Box::new(MmapBacking { mmap, file }),
        page_size,
        map_size,
        created: false,
    })
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
