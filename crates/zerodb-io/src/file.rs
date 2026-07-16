//! Plain-file helpers for env data files (SPEC 02 §3.4/§3.5, §8).
//!
//! Creation writes both meta slots and fsyncs (SPEC 02 §3.4); page-granular
//! positioned reads/writes use the unix `pwrite`/`pread` family via
//! [`std::os::unix::fs::FileExt`] (no cursor movement, no `libc`). `fstat` for
//! [`real_disk_size`] uses `File::metadata`, and fd duplication uses
//! `File::try_clone`. No `unsafe` lives here.

use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;

use zerodb_core::page::MetaPage;

// Meta body offset of the `page_size` field (SPEC 02 §3), used to probe the
// page size of an existing file before it is decoded.
const OFF_PAGE_SIZE: u64 = 40;

/// Open an existing data file for read (and, unless `read_only`, write).
///
/// # Errors
///
/// Propagates the open I/O error (e.g. `ENOENT`).
pub fn open_file(path: &Path, read_only: bool) -> std::io::Result<File> {
    OpenOptions::new().read(true).write(!read_only).open(path)
}

/// Read one page-worth (`psize` bytes) at `pgno`, positioned (no cursor move).
///
/// # Errors
///
/// Propagates the positioned-read I/O error, or `UnexpectedEof` on a short read.
pub fn read_page(file: &File, pgno: u64, psize: u32) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; psize as usize];
    let off = pgno * psize as u64;
    file.read_exact_at(&mut buf, off)?;
    Ok(buf)
}

/// Write `bytes` at page `pgno` (positioned; `bytes.len()` need not equal
/// `psize` but must fit within the page).
///
/// # Errors
///
/// Propagates the positioned-write I/O error.
pub fn write_page(file: &File, pgno: u64, psize: u32, bytes: &[u8]) -> std::io::Result<()> {
    debug_assert!(bytes.len() <= psize as usize);
    let off = pgno * psize as u64;
    file.write_all_at(bytes, off)
}

/// The actual on-disk file length via `fstat` (SPEC 00 row 18).
///
/// # Errors
///
/// Propagates the `fstat` I/O error.
pub fn real_disk_size(file: &File) -> std::io::Result<u64> {
    Ok(file.metadata()?.len())
}

/// Probe an existing file's page size by reading the `page_size` field of
/// slot 0 (SPEC 02 §3). Returns `Some(ps)` iff the field is a power of two in
/// `[4096, 65536]`, else `None` (the file is torn/foreign; the caller falls
/// back to the requested page size and lets meta validation reject it).
///
/// # Errors
///
/// Propagates a positioned-read I/O error other than a short read (a short read
/// yields `Ok(None)` — the file is too small to be an env).
pub fn probe_page_size(file: &File) -> std::io::Result<Option<u32>> {
    let mut buf = [0u8; 4];
    match file.read_exact_at(&mut buf, OFF_PAGE_SIZE) {
        Ok(()) => {
            let ps = u32::from_le_bytes(buf);
            if (4096..=65536).contains(&ps) && ps.is_power_of_two() {
                Ok(Some(ps))
            } else {
                Ok(None)
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

/// Create a brand-new env data file (SPEC 02 §3.4): allocate `2 * page_size`
/// bytes, write both meta slots as identical empty metas at txnid 0, and fsync.
///
/// The file is created with `create_new` semantics via truncation of a freshly
/// created file: the caller must ensure the file did not previously exist (or
/// was empty). Returns the open file handle (read+write).
///
/// # Errors
///
/// Propagates any create/write/fsync I/O error, or (unexpectedly) a meta encode
/// error wrapped as `InvalidInput`.
pub fn create_env_file(path: &Path, page_size: u32, map_size: u64) -> std::io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    let ps = page_size as usize;
    // Size the file to the two meta slots (no data page yet — SPEC 02 §3.4).
    file.set_len(2 * ps as u64)?;

    // Slot 0 and slot 1: identical empty metas at txnid 0.
    for slot in [0u64, 1] {
        let meta = MetaPage::create(slot, page_size, map_size);
        let mut buf = vec![0u8; ps];
        meta.encode(&mut buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;
        write_page(&file, slot, page_size, &buf)?;
    }

    // fsync so both slots are durable before the env is usable (SPEC 02 §3.4).
    file.sync_all()?;
    Ok(file)
}
