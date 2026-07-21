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

/// Write `bytes` starting at page `pgno` (positioned). `bytes` is one page (or
/// less), or — for an overflow run (M1.4 commit C2) — a whole multiple of
/// `psize` spanning the contiguous run.
///
/// # Errors
///
/// Propagates the positioned-write I/O error.
pub fn write_page(file: &File, pgno: u64, psize: u32, bytes: &[u8]) -> std::io::Result<()> {
    debug_assert!(bytes.len() <= psize as usize || bytes.len() % psize as usize == 0);
    let off = pgno * psize as u64;
    file.write_all_at(bytes, off)
}

/// Vectored positioned write of consecutive page-multiple `frames` laid out
/// back-to-back from `start_pgno * psize` (commit C2 batching, PERF-GAP B4):
/// one `pwritev` per chunk of up to [`MAX_IOV`] frames instead of one syscall
/// per dirty page.
///
/// A short write (rare on regular files) falls back to per-frame
/// [`write_page`] for the whole chunk — positioned rewrites of the same bytes
/// at the same offsets are idempotent, so restarting the chunk is correct.
///
/// # Errors
///
/// Propagates the positioned-write I/O error.
pub fn write_pages_vectored(
    file: &File,
    start_pgno: u64,
    psize: u32,
    frames: &[&[u8]],
) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    /// Conservative iovec cap ≤ `IOV_MAX` on every supported platform
    /// (Linux 1024, macOS 1024; LMDB's `MDB_COMMIT_PAGES` plays the same
    /// role).
    const MAX_IOV: usize = 512;

    let mut off: u64 = start_pgno * psize as u64;
    let mut i = 0usize;
    while i < frames.len() {
        let chunk = &frames[i..(i + MAX_IOV).min(frames.len())];
        let total: usize = chunk.iter().map(|f| f.len()).sum();
        let iovs: Vec<libc::iovec> = chunk
            .iter()
            .map(|f| libc::iovec {
                iov_base: f.as_ptr() as *mut libc::c_void,
                iov_len: f.len(),
            })
            .collect();
        let offset = libc::off_t::try_from(off).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "offset overflow")
        })?;
        // SAFETY: each iovec points at a live `&[u8]` borrow held for the
        // whole call (`chunk` outlives it); `pwritev` only READS the buffers
        // (`iov_base` is `*mut` purely for C signature symmetry); the fd is a
        // valid, open regular file owned by `file`.
        let n = unsafe {
            libc::pwritev(
                file.as_raw_fd(),
                iovs.as_ptr(),
                iovs.len() as libc::c_int,
                offset,
            )
        };
        if n < 0 {
            return Err(std::io::Error::last_os_error());
        }
        if n as usize != total {
            // Short write: restart this chunk with idempotent per-frame
            // positioned writes.
            let mut pg = off / psize as u64;
            for f in chunk {
                write_page(file, pg, psize, f)?;
                pg += (f.len() / psize as usize) as u64;
            }
        }
        off += total as u64;
        i += chunk.len();
    }
    Ok(())
}

/// Read the first `len` bytes of the file (the meta-slot head, used to probe
/// the persisted map size before mapping).
///
/// # Errors
///
/// Propagates the positioned-read I/O error.
pub fn read_head(file: &File, len: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    file.read_exact_at(&mut buf, 0)?;
    Ok(buf)
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
