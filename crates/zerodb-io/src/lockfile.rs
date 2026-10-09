//! The shared lock file of a multi-process environment (ADR-0023).
//!
//! Layout, by byte offset (all fields little-endian atomics; the map is
//! page-aligned and every offset below is a multiple of its field's size):
//!
//! | offset | field |
//! |---|---|
//! | 0 | magic `u64` (`LOCK_MAGIC`, stored last by the initializer) |
//! | 8 | lock-format version `u32` |
//! | 12 | data page size `u32` (0 until bound) |
//! | 16 | `max_readers` `u32` |
//! | 64 | published txnid `T` `u64` (own cache line) |
//! | 128 | writer-lock word `u32`: owner PID, bit 31 = waiters (Linux) |
//! | 192 + i·128 | reader slot `i`: txnid `u64` at +0, owner PID `u32` at +8 |
//!
//! Record locks (POSIX `fcntl`, per process; ADR-0023 amendment 2): byte 0 is
//! the first-opener election, byte 1 the macOS writer lock, and byte
//! `LIVENESS_BASE + pid` each process's liveness lock.
//!
//! The `unsafe` here is the sanctioned shared-memory category of the AGENTS.md
//! policy: atomic views over the mapping, `fcntl`, and the Linux `futex`.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use zerodb_core::shared::{SharedRegion, READER_SLOT_FREE};

const LOCK_MAGIC: u64 = u64::from_le_bytes(*b"ZDBLOCK1");
const LOCK_FORMAT: u32 = 1;
const OFF_MAGIC: usize = 0;
const OFF_FORMAT: usize = 8;
const OFF_PAGE_SIZE: usize = 12;
const OFF_MAX_READERS: usize = 16;
const OFF_T: usize = 64;
#[cfg(target_os = "linux")]
const OFF_WRITER: usize = 128;
const OFF_SLOTS: usize = 192;
const SLOT_STRIDE: usize = 128;
/// Record-lock offset of the election byte.
const ELECTION_BYTE: i64 = 0;
/// Record-lock offset of the macOS writer lock.
#[cfg(not(target_os = "linux"))]
const WRITER_BYTE: i64 = 1;
/// Record-lock offset base of the per-PID liveness locks. Far above the
/// mapped size, so PIDs never collide with bytes 0 and 1.
const LIVENESS_BASE: i64 = 1 << 32;
/// Writer-word bit set while a process sleeps on the futex (Linux).
#[cfg(target_os = "linux")]
const WAITERS: u32 = 1 << 31;
/// How long a waiting writer sleeps before checking the owner is alive.
#[cfg(target_os = "linux")]
const OWNER_PROBE: std::time::Duration = std::time::Duration::from_millis(100);

// `flock::l_type` is a `c_short` on Linux and macOS, but the `F_*LCK`
// constants are `c_int` on Linux and `c_short` on macOS. The casts are
// therefore real on one platform and identities on the other.
#[allow(clippy::unnecessary_cast)]
const RDLCK: libc::c_short = libc::F_RDLCK as libc::c_short;
#[allow(clippy::unnecessary_cast)]
const WRLCK: libc::c_short = libc::F_WRLCK as libc::c_short;
#[allow(clippy::unnecessary_cast)]
const UNLCK: libc::c_short = libc::F_UNLCK as libc::c_short;

/// Why [`LockFile::open`] failed.
#[derive(Debug)]
pub enum LockOpenError {
    /// This process already has the lock file open. A second descriptor would
    /// drop the first one's record locks when closed.
    AlreadyOpen,
    /// The lock file was initialized with incompatible parameters.
    Mismatch(String),
    /// An I/O error.
    Io(std::io::Error),
}

impl From<std::io::Error> for LockOpenError {
    fn from(e: std::io::Error) -> Self {
        LockOpenError::Io(e)
    }
}

impl std::fmt::Display for LockOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockOpenError::AlreadyOpen => {
                f.write_str("the lock file is already open in this process")
            }
            LockOpenError::Mismatch(m) => write!(f, "lock file mismatch: {m}"),
            LockOpenError::Io(e) => write!(f, "lock file I/O error: {e}"),
        }
    }
}

impl std::error::Error for LockOpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LockOpenError::Io(e) => Some(e),
            _ => None,
        }
    }
}

/// The lock-file paths this process has open. A poisoned registry is still
/// consistent (insert and remove do not panic midway), so poison is ignored.
fn open_paths() -> MutexGuard<'static, HashSet<PathBuf>> {
    static PATHS: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    PATHS
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// This process's claim on one lock-file path in the registry. Dropping it
/// removes the path, so every error path and every [`LockFile`] drop releases
/// it exactly once.
struct Registration {
    key: PathBuf,
}

impl Registration {
    /// Claim `path`, keyed by its canonical directory so two spellings of one
    /// file share an entry. Fails with `AlreadyOpen` if already claimed.
    fn claim(path: &Path) -> Result<Registration, LockOpenError> {
        let key = registry_key(path)?;
        if !open_paths().insert(key.clone()) {
            return Err(LockOpenError::AlreadyOpen);
        }
        Ok(Registration { key })
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        open_paths().remove(&self.key);
    }
}

/// The canonical path of the directory holding `path`, joined with the file
/// name. Only the directory is canonicalized because the file may not exist
/// yet.
fn registry_key(path: &Path) -> std::io::Result<PathBuf> {
    let name = path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "lock file path has no file name",
        )
    })?;
    let dir = match path.parent() {
        Some(d) if !d.as_os_str().is_empty() => d,
        _ => Path::new("."),
    };
    Ok(dir.canonicalize()?.join(name))
}

/// The mapped lock file. Field order matters for `Drop`: the map is
/// unmapped before the descriptor closes, closing the descriptor releases
/// this process's record locks, and only then is the path released in the
/// registry — so no second descriptor of this file can be opened by this
/// process while the first is still open.
///
/// Sharing one `LockFile` across threads is sound: all shared state is reached
/// through atomics over the mapping or through `fcntl` on the descriptor.
pub struct LockFile {
    map: memmap2::MmapRaw,
    file: File,
    _registration: Registration,
    pid: u32,
    initializer: bool,
    slots: u32,
}

impl LockFile {
    /// Open or create the lock file at `path` and join the environment
    /// (ADR-0023 §1). The first opener (no other process alive) initializes
    /// the file and holds the election byte exclusively until
    /// [`LockFile::finish_open`]; later openers wait for that, then validate.
    ///
    /// # Errors
    ///
    /// [`LockOpenError::AlreadyOpen`] if this process has it open,
    /// [`LockOpenError::Mismatch`] on a format or `max_readers` mismatch, and
    /// I/O errors.
    pub fn open(path: &Path, max_readers: u32) -> Result<Arc<LockFile>, LockOpenError> {
        // Claimed before any descriptor exists: POSIX record locks belong to
        // the process, and closing a second descriptor would drop the first
        // handle's locks (ADR-0023 amendment 2). On every error below the
        // registration (or the `LockFile` owning it) drops and releases it.
        let registration = Registration::claim(path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let initializer = record_lock(&file, libc::F_SETLK, WRLCK, ELECTION_BYTE)?;
        if !initializer {
            // Blocks until the initializer downgrades in `finish_open`.
            record_lock(&file, libc::F_SETLKW, RDLCK, ELECTION_BYTE)?;
        }
        // A joiner reads `max_readers` with `pread` before mapping. It holds
        // the shared election lock, so the initializer has finished writing.
        let slots = if initializer {
            max_readers
        } else {
            read_u32_at(&file, OFF_MAX_READERS)?
        };
        let len = OFF_SLOTS + SLOT_STRIDE * slots as usize;
        if initializer {
            // Truncate to zero first so no field survives from a previous life.
            file.set_len(0)?;
            file.set_len(len as u64)?;
        } else if file.metadata()?.len() < len as u64 {
            return Err(LockOpenError::Mismatch(
                "lock file shorter than its header says".into(),
            ));
        }
        let map = memmap2::MmapOptions::new().len(len).map_raw(&file)?;
        let pid = std::process::id();
        let lock = LockFile {
            map,
            file,
            _registration: registration,
            pid,
            initializer,
            slots,
        };
        if initializer {
            for i in 0..slots {
                // Relaxed: published by the Release store of the magic below,
                // and no other process can map the file before the election
                // byte is downgraded.
                lock.slot_txnid(i)
                    .store(READER_SLOT_FREE, Ordering::Relaxed);
                lock.slot_pid(i).store(0, Ordering::Relaxed);
            }
            // Relaxed: published by the Release store of the magic below.
            lock.u32_at(OFF_FORMAT)
                .store(LOCK_FORMAT, Ordering::Relaxed);
            lock.u32_at(OFF_MAX_READERS).store(slots, Ordering::Relaxed);
            // Release: every field above is visible to a joiner that loads the
            // magic with Acquire.
            lock.u64_at(OFF_MAGIC).store(LOCK_MAGIC, Ordering::Release);
        } else {
            // Acquire: pairs with the initializer's Release store of the
            // magic. The format load is ordered after it, so Relaxed suffices.
            if lock.u64_at(OFF_MAGIC).load(Ordering::Acquire) != LOCK_MAGIC
                || lock.u32_at(OFF_FORMAT).load(Ordering::Relaxed) != LOCK_FORMAT
            {
                return Err(LockOpenError::Mismatch(
                    "not a ZeroDB lock file of this version".into(),
                ));
            }
            if slots != max_readers {
                return Err(LockOpenError::Mismatch(format!(
                    "max_readers {max_readers} differs from the running environment's {slots}"
                )));
            }
        }
        // This process's liveness lock (ADR-0023 §3), held for the handle's life.
        if !record_lock(
            &lock.file,
            libc::F_SETLK,
            WRLCK,
            LIVENESS_BASE + i64::from(pid),
        )? {
            return Err(LockOpenError::Io(std::io::Error::other(
                "liveness lock already held",
            )));
        }
        Ok(Arc::new(lock))
    }

    /// Record the data page size (initializer) or check it (joiner).
    ///
    /// # Errors
    ///
    /// `InvalidData` when the running environment uses another page size.
    pub fn bind_page_size(&self, page_size: u32) -> std::io::Result<()> {
        let cell = self.u32_at(OFF_PAGE_SIZE);
        // AcqRel/Acquire: the bound value is read by joiners after the
        // election downgrade; the CAS makes a re-bind with another value fail.
        match cell.compare_exchange(0, page_size, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => Ok(()),
            Err(cur) if cur == page_size => Ok(()),
            Err(cur) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("page size {page_size} differs from the running environment's {cur}"),
            )),
        }
    }

    /// Let other processes in: the initializer converts its exclusive
    /// election lock to a shared one (atomic for POSIX locks). No-op for a
    /// joiner, which already holds it shared.
    ///
    /// # Errors
    ///
    /// The `fcntl` error.
    pub fn finish_open(&self) -> std::io::Result<()> {
        if self.initializer {
            record_lock(&self.file, libc::F_SETLK, RDLCK, ELECTION_BYTE)?;
        }
        Ok(())
    }

    /// Whether process `pid` currently has this environment open (holds its
    /// liveness lock). For another PID this is immune to reuse: a dead
    /// process's recycled PID holds no lock unless a live process with that PID
    /// has the environment open. For this process's own PID it always returns
    /// `true`, even when the PID was recorded by a dead predecessor that had
    /// the same PID; callers that may see such a stale PID must handle that
    /// case themselves (as `lock_writer` does on Linux).
    ///
    /// # Errors
    ///
    /// The `fcntl` error.
    pub fn is_alive(&self, pid: u32) -> std::io::Result<bool> {
        if pid == self.pid {
            // F_GETLK never reports the caller's own locks.
            return Ok(true);
        }
        probe_locked(&self.file, LIVENESS_BASE + i64::from(pid))
    }

    fn u64_at(&self, off: usize) -> &AtomicU64 {
        debug_assert!(off.is_multiple_of(8) && off + 8 <= self.map.len());
        // SAFETY: the map is page-aligned and `off` is a multiple of 8 within
        // its length, so the pointer is aligned and in bounds. The mapping
        // lives as long as `self` and is never remapped, so the reference
        // cannot outlive it. Every access to these bytes, in this and every
        // other process, goes through atomics, so there is no non-atomic data
        // race.
        unsafe { AtomicU64::from_ptr(self.map.as_mut_ptr().add(off).cast()) }
    }

    fn u32_at(&self, off: usize) -> &AtomicU32 {
        debug_assert!(off.is_multiple_of(4) && off + 4 <= self.map.len());
        // SAFETY: as `u64_at`, for a 4-aligned in-bounds offset.
        unsafe { AtomicU32::from_ptr(self.map.as_mut_ptr().add(off).cast()) }
    }

    #[cfg(target_os = "linux")]
    fn writer_word(&self) -> &AtomicU32 {
        self.u32_at(OFF_WRITER)
    }
}

impl SharedRegion for LockFile {
    fn published_txnid(&self) -> &AtomicU64 {
        self.u64_at(OFF_T)
    }
    fn slot_count(&self) -> u32 {
        self.slots
    }
    fn slot_txnid(&self, i: u32) -> &AtomicU64 {
        assert!(i < self.slots, "reader slot out of range");
        self.u64_at(OFF_SLOTS + i as usize * SLOT_STRIDE)
    }
    fn slot_pid(&self, i: u32) -> &AtomicU32 {
        assert!(i < self.slots, "reader slot out of range");
        self.u32_at(OFF_SLOTS + i as usize * SLOT_STRIDE + 8)
    }
    fn pid(&self) -> u32 {
        self.pid
    }
    fn is_initializer(&self) -> bool {
        self.initializer
    }

    #[cfg(target_os = "linux")]
    fn lock_writer(&self) -> std::io::Result<()> {
        let w = self.writer_word();
        loop {
            // Acquire: synchronizes with the previous owner's Release in
            // `unlock_writer`, so its T store and meta write are visible.
            let cur = match w.compare_exchange(0, self.pid, Ordering::Acquire, Ordering::Relaxed) {
                Ok(_) => return Ok(()),
                Err(cur) => cur,
            };
            let owner = cur & !WAITERS;
            if owner == self.pid {
                // The word names this process, but this process does not hold
                // the lock: the core calls `lock_writer` only while holding its
                // in-process writer lock, so no other thread of this process
                // can own it. The word is therefore stale, left by a dead
                // process that had the same PID. Waiting would never end
                // (`is_alive` reports this PID alive, and other processes see
                // this process's liveness lock), so take it over at once.
                // Acquire as above; the waiters bit is kept so this owner's
                // release wakes the others.
                if w.compare_exchange(
                    cur,
                    self.pid | (cur & WAITERS),
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .is_ok()
                {
                    return Ok(());
                }
                continue; // the word changed; re-read it
            }
            // Relaxed: setting the waiters bit publishes no data; it only
            // tells the owner's release to issue a wake. The futex syscall
            // re-checks the word against `cur | WAITERS` in the kernel.
            if cur & WAITERS == 0
                && w.compare_exchange(cur, cur | WAITERS, Ordering::Relaxed, Ordering::Relaxed)
                    .is_err()
            {
                continue; // the word changed; re-read it
            }
            let timed_out = futex_wait(w, cur | WAITERS, OWNER_PROBE);
            if timed_out && !self.is_alive(owner)? {
                // The owner died holding the lock (ADR-0023 §2): take it over.
                // Relaxed load: the CAS below re-validates the value. Acquire
                // on the CAS as above. The waiters bit is kept so this
                // owner's release wakes the others.
                let now = w.load(Ordering::Relaxed);
                if now & !WAITERS == owner
                    && w.compare_exchange(
                        now,
                        self.pid | (now & WAITERS),
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return Ok(());
                }
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn unlock_writer(&self) {
        // Release: publishes this writer's T store and meta write to the next
        // owner's Acquire.
        let old = self.writer_word().swap(0, Ordering::Release);
        if old & WAITERS != 0 {
            futex_wake_all(self.writer_word());
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn lock_writer(&self) -> std::io::Result<()> {
        // POSIX record locks belong to the process, so any thread may release
        // it, and the kernel drops it if the process dies (ADR-0023 §2).
        record_lock(&self.file, libc::F_SETLKW, WRLCK, WRITER_BYTE).map(|_| ())
    }

    #[cfg(not(target_os = "linux"))]
    fn unlock_writer(&self) {
        // Unlocking one byte this process holds cannot fail in practice. If it
        // did, every other process would stay blocked on the writer lock until
        // this one exits. This runs inside the core's writer-guard `Drop`, so
        // it aborts rather than panicking: exiting releases the lock.
        if let Err(e) = record_lock(&self.file, libc::F_SETLK, UNLCK, WRITER_BYTE) {
            eprintln!("zerodb: cannot release the cross-process writer lock: {e}");
            std::process::abort();
        }
    }
}

/// Set or clear a one-byte POSIX record lock at `start`. Returns `Ok(false)`
/// when a non-blocking `F_SETLK` finds the byte held by another process.
fn record_lock(
    file: &File,
    cmd: libc::c_int,
    kind: libc::c_short,
    start: i64,
) -> std::io::Result<bool> {
    let fl = one_byte_flock(kind, start);
    loop {
        // SAFETY: `file` is an open descriptor for the call's duration and
        // `fl` is a valid, initialized `flock` that outlives the call; the
        // kernel only reads it for F_SETLK / F_SETLKW.
        let r = unsafe { libc::fcntl(file.as_raw_fd(), cmd, &fl as *const libc::flock) };
        if r == 0 {
            return Ok(true);
        }
        let e = std::io::Error::last_os_error();
        match e.raw_os_error() {
            Some(libc::EINTR) => continue,
            // POSIX allows either errno for a conflicting non-blocking lock.
            Some(libc::EAGAIN) | Some(libc::EACCES) if cmd == libc::F_SETLK => return Ok(false),
            _ => return Err(e),
        }
    }
}

/// Whether another process holds a lock on byte `start`. F_GETLK never
/// reports the calling process's own locks.
fn probe_locked(file: &File, start: i64) -> std::io::Result<bool> {
    let mut fl = one_byte_flock(WRLCK, start);
    // SAFETY: `file` is an open descriptor for the call's duration and `fl` is
    // a valid, initialized `flock` that outlives the call; F_GETLK writes the
    // result into it.
    let r = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETLK, &mut fl as *mut libc::flock) };
    if r != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(fl.l_type != UNLCK)
}

/// A `flock` describing the one byte at `start` with lock type `kind`.
fn one_byte_flock(kind: libc::c_short, start: i64) -> libc::flock {
    // SAFETY: `flock` is a plain C struct of integers (plus padding on some
    // platforms), for which all-zero bytes are a valid value.
    let mut fl: libc::flock = unsafe { std::mem::zeroed() };
    fl.l_type = kind;
    fl.l_whence = libc::SEEK_SET as libc::c_short;
    fl.l_start = start;
    fl.l_len = 1;
    fl
}

fn read_u32_at(file: &File, off: usize) -> std::io::Result<u32> {
    use std::os::unix::fs::FileExt;
    let mut b = [0u8; 4];
    file.read_exact_at(&mut b, off as u64)?;
    Ok(u32::from_le_bytes(b))
}

/// Sleep while `word == expected`, at most `timeout`. Returns `true` on timeout.
#[cfg(target_os = "linux")]
fn futex_wait(word: &AtomicU32, expected: u32, timeout: std::time::Duration) -> bool {
    let ts = libc::timespec {
        tv_sec: timeout.as_secs() as libc::time_t,
        tv_nsec: libc::c_long::from(timeout.subsec_nanos() as i32),
    };
    // SAFETY: `word` is a live, 4-aligned u32 in this lock file's MAP_SHARED
    // mapping, which outlives the call. FUTEX_WAIT without the PRIVATE flag
    // keys on the backing file page, so waiters in other processes match.
    // `ts` lives on the stack across the call. The kernel only reads both.
    let r = unsafe {
        libc::syscall(
            libc::SYS_futex,
            word.as_ptr(),
            libc::FUTEX_WAIT,
            expected,
            &ts as *const libc::timespec,
            std::ptr::null::<u32>(),
            0u32,
        )
    };
    r == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ETIMEDOUT)
}

/// Wake every process sleeping on `word`.
#[cfg(target_os = "linux")]
fn futex_wake_all(word: &AtomicU32) {
    // SAFETY: as `futex_wait`; FUTEX_WAKE only uses the address as a key and
    // ignores the timeout, second address and third value arguments.
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            word.as_ptr(),
            libc::FUTEX_WAKE,
            i32::MAX,
            0usize,
            0usize,
            0u32,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    /// A fresh directory per call. The counter keeps parallel tests apart
    /// when the clock (microsecond resolution on macOS) repeats.
    fn tmp() -> std::path::PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        // Relaxed: only uniqueness of the value matters.
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!(
            "zerodb-lock-{}-{}-{}",
            std::process::id(),
            n,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p.join("zerodb.lock")
    }

    #[test]
    fn first_opener_initializes_every_slot_free() {
        let path = tmp();
        let l = LockFile::open(&path, 8).expect("open");
        assert!(l.is_initializer());
        assert_eq!(l.slot_count(), 8);
        for i in 0..8 {
            assert_eq!(l.slot_txnid(i).load(Ordering::SeqCst), READER_SLOT_FREE);
            assert_eq!(l.slot_pid(i).load(Ordering::SeqCst), 0);
        }
        assert_eq!(l.published_txnid().load(Ordering::SeqCst), 0);
        assert_eq!(l.pid(), std::process::id());
        l.bind_page_size(4096).unwrap();
        l.finish_open().unwrap();
    }

    #[test]
    fn second_open_in_process_is_refused_before_any_fd() {
        let path = tmp();
        let a = LockFile::open(&path, 4).expect("open");
        assert!(matches!(
            LockFile::open(&path, 4),
            Err(LockOpenError::AlreadyOpen)
        ));
        // `a` still holds its locks and works.
        a.lock_writer().unwrap();
        a.unlock_writer();
        drop(a);
        // After the first handle is gone, the path can be opened again.
        LockFile::open(&path, 4).expect("reopen");
    }

    #[test]
    fn writer_lock_releases_from_another_thread() {
        let path = tmp();
        let l = LockFile::open(&path, 4).expect("open");
        l.lock_writer().unwrap();
        let l2 = Arc::clone(&l);
        std::thread::spawn(move || l2.unlock_writer())
            .join()
            .unwrap();
        l.lock_writer().unwrap();
        l.unlock_writer();
    }

    #[test]
    fn another_spelling_of_an_open_path_is_refused() {
        let path = tmp();
        let _a = LockFile::open(&path, 4).expect("open");
        let dir = path.parent().unwrap();
        let other = dir.join(".").join("zerodb.lock");
        assert!(matches!(
            LockFile::open(&other, 4),
            Err(LockOpenError::AlreadyOpen)
        ));
    }

    /// A writer word naming this process's PID, as a dead predecessor with
    /// the same PID would leave it, is taken over at once rather than waited
    /// on forever.
    #[cfg(target_os = "linux")]
    #[test]
    fn stale_writer_word_with_own_pid_is_taken_over() {
        let path = tmp();
        let l = LockFile::open(&path, 4).expect("open");
        for stale in [l.pid(), l.pid() | WAITERS] {
            l.writer_word().store(stale, Ordering::SeqCst);
            let start = std::time::Instant::now();
            l.lock_writer().unwrap();
            assert!(
                start.elapsed() < OWNER_PROBE,
                "waited on a stale own-PID word"
            );
            assert_eq!(l.writer_word().load(Ordering::SeqCst) & !WAITERS, l.pid());
            l.unlock_writer();
            assert_eq!(l.writer_word().load(Ordering::SeqCst), 0);
        }
    }

    #[test]
    fn page_size_rebind_with_another_value_fails() {
        let path = tmp();
        let l = LockFile::open(&path, 4).expect("open");
        l.bind_page_size(4096).unwrap();
        assert!(l.bind_page_size(8192).is_err());
    }
}
