//! Plain-file helpers for env data files (SPEC 02 §3.4/§3.5, §8).
//!
//! Creation writes both meta slots and fsyncs (SPEC 02 §3.4); page-granular
//! positioned reads/writes use the unix `pwrite`/`pread` family via
//! [`std::os::unix::fs::FileExt`] (no cursor movement, no `libc`). `fstat` for
//! [`real_disk_size`] uses `File::metadata`, and fd duplication uses
//! `File::try_clone`. The one `unsafe` here is the `pwritev` FFI call behind
//! the vectored commit write, SAFETY-commented at the site.

use std::fs::{File, OpenOptions};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;

use zerodb_core::page::MetaPage;

// Meta body offset of the `page_size` field (SPEC 02 §3), used to probe the
// page size of an existing file before it is decoded.
const OFF_PAGE_SIZE: u64 = 40;

/// The byte offset of page `pgno`, checked: a hostile page number (e.g. from
/// a corrupt freelist) must yield a typed error, never a wrapped offset that
/// silently writes/reads elsewhere in the file.
fn page_offset(pgno: u64, psize: u32) -> std::io::Result<u64> {
    pgno.checked_mul(u64::from(psize)).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("page offset overflow: pgno {pgno} * psize {psize}"),
        )
    })
}

/// Open an existing data file for read (and, unless `read_only`, write).
///
/// # Errors
///
/// Propagates the open I/O error (e.g. `ENOENT`).
pub(crate) fn open_file(path: &Path, read_only: bool) -> std::io::Result<File> {
    OpenOptions::new().read(true).write(!read_only).open(path)
}

/// Positioned read of exactly `buf.len()` bytes at page `pgno` (a kernel copy,
/// not a borrowed map slice — ADR-0023 §4): used to decode a meta slot that
/// another process may be rewriting concurrently.
///
/// # Errors
///
/// Propagates the positioned-read I/O error, or `UnexpectedEof` on a short
/// read.
pub(crate) fn read_page_into(
    file: &File,
    pgno: u64,
    psize: u32,
    buf: &mut [u8],
) -> std::io::Result<()> {
    let off = page_offset(pgno, psize)?;
    file.read_exact_at(buf, off)
}

/// Write `bytes` starting at page `pgno` (positioned). `bytes` is one page (or
/// less), or — for an overflow run (commit step C2) — a whole multiple of
/// `psize` spanning the contiguous run.
///
/// # Errors
///
/// Propagates the positioned-write I/O error.
pub(crate) fn write_page(file: &File, pgno: u64, psize: u32, bytes: &[u8]) -> std::io::Result<()> {
    debug_assert!(bytes.len() <= psize as usize || bytes.len().is_multiple_of(psize as usize));
    let off = page_offset(pgno, psize)?;
    file.write_all_at(bytes, off)
}

/// Segments per `pwritev` call, far below `IOV_MAX` (1024 on Linux and
/// macOS). Commit frames are whole pages, so [`MAX_WRITE_BYTES`] binds first
/// (64 pages of 4 KiB); this bound only matters for smaller buffers, and it
/// lets the segments live in stack arrays.
const MAX_SEGS: usize = 64;

/// Bytes per `pwritev` call, unless one page is larger (then one page): 64
/// pages of 4 KiB, LMDB's `MDB_COMMIT_PAGES`.
///
/// The page cache sizes the folios it allocates for a buffered write by the
/// write's length on kernels and filesystems with large-folio support (XFS,
/// and ext4 on recent kernels). A later write of one page into a large folio
/// costs far more kernel time than a write into a small one, so large calls
/// slow down every small commit that follows a bulk one. Smaller calls slow
/// bulk commits instead, through the extra syscalls. 256 KiB, LMDB's choice,
/// keeps most of the small-commit gain for few extra calls (measurements in
/// docs/PERFORMANCE.md).
const MAX_WRITE_BYTES: usize = 256 * 1024;

/// Byte cap for one `pwritev` call at page size `psize`: [`MAX_WRITE_BYTES`],
/// or one page when a page is larger.
fn commit_cap(psize: u32) -> usize {
    MAX_WRITE_BYTES.max(psize as usize)
}

/// Fill `segs` with the next `pwritev` call's segments over consecutive
/// `frames`, resuming at `at` (frame index, byte offset within it): at most
/// `cap` bytes and [`MAX_SEGS`] segments, cutting a frame longer than the
/// remaining room (an overflow run) across calls. Returns the segment count,
/// 0 once every byte has been handed out; concatenated, the calls' segments
/// are exactly the frames' bytes in order.
fn next_write_run<'a>(
    frames: &[&'a [u8]],
    at: &mut (usize, usize),
    cap: usize,
    segs: &mut [&'a [u8]; MAX_SEGS],
) -> usize {
    debug_assert!(cap > 0);
    let (mut n, mut room) = (0usize, cap);
    while room > 0 && n < MAX_SEGS {
        let Some(frame) = frames.get(at.0) else { break };
        let rest = &frame[at.1..];
        let take = rest.len().min(room);
        if take > 0 {
            segs[n] = &rest[..take];
            n += 1;
            room -= take;
        }
        if take == rest.len() {
            *at = (at.0 + 1, 0);
        } else {
            at.1 += take;
        }
    }
    n
}

/// Vectored positioned write of consecutive page-multiple `frames` laid out
/// back-to-back from `start_pgno * psize` (commit C2 batching): one
/// `pwritev` per [`MAX_WRITE_BYTES`] (or per page, if a page is larger)
/// instead of one syscall per dirty page.
///
/// A short write (rare on regular files) falls back to positioned
/// `write_all_at` of each segment of that call — positioned rewrites of the
/// same bytes at the same offsets are idempotent, so restarting the call's
/// range is correct.
///
/// # Errors
///
/// Propagates the positioned-write I/O error.
pub(crate) fn write_pages_vectored(
    file: &File,
    start_pgno: u64,
    psize: u32,
    frames: &[&[u8]],
) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;

    let cap = commit_cap(psize);
    let mut off: u64 = page_offset(start_pgno, psize)?;
    let mut at = (0usize, 0usize);
    let mut segs: [&[u8]; MAX_SEGS] = [&[]; MAX_SEGS];
    let mut iovs = [libc::iovec {
        iov_base: std::ptr::null_mut(),
        iov_len: 0,
    }; MAX_SEGS];
    loop {
        let count = next_write_run(frames, &mut at, cap, &mut segs);
        if count == 0 {
            break;
        }
        let run = &segs[..count];
        let mut total = 0usize;
        for (iov, s) in iovs.iter_mut().zip(run) {
            *iov = libc::iovec {
                iov_base: s.as_ptr() as *mut libc::c_void,
                iov_len: s.len(),
            };
            total += s.len();
        }
        let offset = libc::off_t::try_from(off).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "offset overflow")
        })?;
        // SAFETY: the first `count` iovecs point at live `&[u8]` borrows
        // (`segs`, borrowing `frames`) held for the whole call; `pwritev`
        // only READS the buffers (`iov_base` is `*mut` purely for C
        // signature symmetry); the fd is a valid, open regular file owned by
        // `file`.
        let n = unsafe {
            libc::pwritev(
                file.as_raw_fd(),
                iovs.as_ptr(),
                count as libc::c_int,
                offset,
            )
        };
        if n < 0 {
            return Err(std::io::Error::last_os_error());
        }
        if n as usize != total {
            // Short write: restart this call's range with idempotent
            // positioned writes of each segment.
            let mut seg_off = off;
            for s in run {
                file.write_all_at(s, seg_off)?;
                seg_off += s.len() as u64;
            }
        }
        off += total as u64;
    }
    Ok(())
}

/// Read the first `len` bytes of the file (the meta-slot head, used to probe
/// the persisted map size before mapping).
///
/// # Errors
///
/// Propagates the positioned-read I/O error.
pub(crate) fn read_head(file: &File, len: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    file.read_exact_at(&mut buf, 0)?;
    Ok(buf)
}

/// The actual on-disk file length via `fstat`.
///
/// # Errors
///
/// Propagates the `fstat` I/O error.
pub(crate) fn real_disk_size(file: &File) -> std::io::Result<u64> {
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
/// The file is created with real `create_new` (O_CREAT|O_EXCL) semantics,
/// owner-only (`0o600`); when the path already holds an **empty regular
/// file** (the caller only reaches this for absent-or-empty paths, e.g. a
/// lock file pre-created by the tools) it is opened in place instead, and a
/// symlink or other non-regular file at the path is refused. Returns the open
/// file handle (read+write).
///
/// # Errors
///
/// Propagates any create/write/fsync I/O error, or (unexpectedly) a meta encode
/// error wrapped as `InvalidInput`.
pub(crate) fn create_env_file(path: &Path, page_size: u32, map_size: u64) -> std::io::Result<File> {
    // `create_new` (O_CREAT|O_EXCL): never follows a symlink planted at the
    // data-file path, and `mode(0o600)` keeps the store owner-only (LMDB
    // creates its map 0600 too; a default-mode file leaks data via umask).
    // The caller reaches this with the file absent **or existing-but-empty**
    // (SPEC 02 §3.4/§3.5): for the empty-file case `create_new` fails with
    // `AlreadyExists` and we fall back to opening the existing file — but
    // only after `symlink_metadata` proves it is a plain regular file, so a
    // planted symlink is refused rather than written through.
    let file = match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
    {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            let md = std::fs::symlink_metadata(path)?;
            if !md.file_type().is_file() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "refusing to initialize an env over a non-regular file (symlink?)",
                ));
            }
            // `O_NOFOLLOW` closes the check-then-open window: a symlink
            // swapped in after `symlink_metadata` makes this open fail
            // (`ELOOP`) instead of being followed.
            OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(path)?
        }
        Err(e) => return Err(e),
    };
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
    // …and fsync the parent directory so the file's *directory entry* is
    // durable too (SPEC 02 §3.4 step 4): without this, a crash
    // shortly after creation can lose the name while the content was already
    // durable — a reachable-file guarantee, one fsync per env lifetime.
    fsync_parent_dir(path)?;
    Ok(file)
}

/// fsync the directory containing `path` (see [`create_env_file`]).
///
/// # Errors
///
/// Propagates the directory open/fsync error.
fn fsync_parent_dir(path: &Path) -> std::io::Result<()> {
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    File::open(parent)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::{commit_cap, next_write_run, write_pages_vectored, MAX_SEGS, MAX_WRITE_BYTES};

    /// The commit cap is 256 KiB, LMDB's 64 pages at 4 KiB, unless one page
    /// is larger; a change to the tuned value has to change this test.
    #[test]
    fn commit_cap_is_256_kib_or_one_page() {
        assert_eq!(commit_cap(4096), 256 * 1024);
        assert_eq!(commit_cap(65536), 256 * 1024);
        assert_eq!(commit_cap(1 << 20), 1 << 20);
    }
    use std::os::unix::fs::FileExt;

    /// Every call's segments `next_write_run` hands out, in order.
    fn runs<'a>(frames: &[&'a [u8]], cap: usize) -> Vec<Vec<&'a [u8]>> {
        let (mut at, mut out) = ((0, 0), Vec::new());
        let mut segs: [&[u8]; MAX_SEGS] = [&[]; MAX_SEGS];
        loop {
            let n = next_write_run(frames, &mut at, cap, &mut segs);
            if n == 0 {
                break;
            }
            out.push(segs[..n].to_vec());
        }
        assert_eq!(
            next_write_run(frames, &mut at, cap, &mut segs),
            0,
            "a finished split stays finished"
        );
        out
    }

    /// Distinct bytes per page, so a misplaced or reordered page shows.
    fn page(ps: usize, tag: usize) -> Vec<u8> {
        (0..ps).map(|i| (i * 31 + tag * 7) as u8).collect()
    }

    /// Runs hold at most `cap` bytes and `MAX_SEGS` segments, cut overflow-run
    /// frames across calls, and concatenate back to the frames' bytes.
    #[test]
    fn write_runs_respect_the_byte_and_iovec_caps() {
        const PS: usize = 4096;
        let pages: Vec<Vec<u8>> = (0..30).map(|t| page(PS, t)).collect();
        let overflow: Vec<u8> = pages[2..12].concat(); // a 10-page overflow run
        let mut frames: Vec<&[u8]> = vec![&pages[0], &pages[1], &overflow];
        frames.extend(pages[12..30].iter().map(Vec::as_slice));
        let want: Vec<u8> = frames.concat();

        for cap in [PS, 4 * PS, MAX_WRITE_BYTES, 1 << 30] {
            let got = runs(&frames, cap);
            for r in &got {
                let len: usize = r.iter().map(|s| s.len()).sum();
                assert!(
                    len <= cap && r.len() <= MAX_SEGS,
                    "cap {cap}: run of {len} bytes"
                );
                assert!(r.iter().all(|s| !s.is_empty()), "no empty segment");
            }
            let full = got
                .iter()
                .filter(|r| r.len() == MAX_SEGS || r.iter().map(|s| s.len()).sum::<usize>() == cap);
            assert!(
                full.count() >= got.len() - 1,
                "cap {cap}: only the last call stops short of a limit"
            );
            let flat: Vec<u8> = got
                .iter()
                .flatten()
                .flat_map(|s| s.iter().copied())
                .collect();
            assert_eq!(flat, want, "cap {cap}: bytes reordered or lost");
        }
        assert_eq!(
            runs(&frames, 1 << 30).len(),
            frames.len().div_ceil(MAX_SEGS),
            "a large byte cap leaves the segment cap"
        );
        assert_eq!(
            runs(&frames, MAX_WRITE_BYTES).len(),
            (30 * PS).div_ceil(MAX_WRITE_BYTES),
            "the commit cap writes MAX_WRITE_BYTES per call"
        );
        assert_eq!(
            runs(&frames, PS).len(),
            30,
            "a one-page cap writes page by page"
        );

        // More segments than MAX_SEGS under a large byte cap: split by count.
        let tiny = [0u8; 8];
        let many: Vec<&[u8]> = vec![&tiny[..]; MAX_SEGS + 3];
        let got = runs(&many, 1 << 30);
        assert_eq!(
            got.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![MAX_SEGS, 3]
        );
        assert!(runs(&[], PS).is_empty(), "nothing to write, no call");
    }

    /// The vectored write lands every byte at its page offset, at a page
    /// size below the byte cap (the overflow run spans three calls) and at
    /// 64 KiB.
    #[test]
    fn write_pages_vectored_round_trips() {
        for ps in [4096usize, 65536] {
            let path = std::env::temp_dir()
                .join(format!("zerodb-io-vectored-{ps}-{}", std::process::id()));
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            let pages: Vec<Vec<u8>> = (0..150).map(|t| page(ps, t)).collect();
            let overflow: Vec<u8> = pages[1..147].concat(); // spans several calls
            let frames: Vec<&[u8]> = vec![&pages[0], &overflow, &pages[147], &pages[148]];
            write_pages_vectored(&file, 3, ps as u32, &frames).unwrap();

            let want: Vec<u8> = frames.concat();
            let mut got = vec![0u8; want.len()];
            file.read_exact_at(&mut got, 3 * ps as u64).unwrap();
            assert_eq!(got, want, "page size {ps}");
            assert_eq!(file.metadata().unwrap().len(), (3 * ps + want.len()) as u64);
            drop(file);
            let _ = std::fs::remove_file(&path);
        }
    }
}
