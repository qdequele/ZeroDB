//! The shared lock file of a multi-process environment (ADR-0023).
//!
//! Layout, by byte offset (all fields little-endian atomics; the map is
//! page-aligned and every offset below is a multiple of its field's size):
//!
//! | offset | field |
//! |---|---|
//! | 0 | magic `u64`: `LOCK_MAGIC_INIT` while the initializer builds the file, `LOCK_MAGIC` once it is published |
//! | 8 | lock-format version `u32` |
//! | 12 | data page size `u32` (0 until bound) |
//! | 16 | `max_readers` `u32` |
//! | 64 | published txnid `T` `u64` (own cache line) |
//! | 128 | writer-lock word `u32`: owner PID, bit 31 = waiters (Linux) |
//! | 256 + i·128 | reader slot `i`: txnid `u64` at +0, owner `u64` at +8 (PID low 32 bits, claim generation high 32 bits) |
//!
//! Offsets 136–255 are unused: they keep slot 0 off the writer word's 128-byte
//! line pair (ADR-0023 amendment 11).
//!
//! Before it truncates anything, every opener checks that the lock file is
//! not the data file, and the initializer refuses a non-empty file that does
//! not look like a lock file (ADR-0023 amendment 16; see [`LockFile::open`]).
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

use zerodb_core::shared::{ReaderSlot, SharedRegion, READER_SLOT_FREE, READER_SLOT_SIZE};

const LOCK_MAGIC: u64 = u64::from_le_bytes(*b"ZDBLOCK1");
/// The initializing magic: written with `pwrite` right after the initializer
/// truncates the file, and replaced by [`LOCK_MAGIC`] in
/// [`LockFile::finish_open`]. A file carrying it was left by an initializer
/// that died mid-way; the next initializer accepts it, and a joiner treats it
/// as unfinished (ADR-0023 amendment 16).
const LOCK_MAGIC_INIT: u64 = u64::from_le_bytes(*b"ZDBLOCK0");
const LOCK_FORMAT: u32 = 2;
const OFF_MAGIC: usize = 0;
const OFF_FORMAT: usize = 8;
const OFF_PAGE_SIZE: usize = 12;
const OFF_MAX_READERS: usize = 16;
const OFF_T: usize = 64;
#[cfg(target_os = "linux")]
const OFF_WRITER: usize = 128;
const OFF_SLOTS: usize = 256;
const SLOT_STRIDE: usize = 128;
// The slot table is viewed as a `&[ReaderSlot]` (see `LockFile::slots`): the
// stride must be the slot type's size, and the table's offset a multiple of
// its alignment (the map itself is page-aligned).
const _: () = {
    assert!(SLOT_STRIDE == READER_SLOT_SIZE);
    assert!(SLOT_STRIDE == std::mem::size_of::<ReaderSlot>());
    assert!(OFF_SLOTS.is_multiple_of(std::mem::align_of::<ReaderSlot>()));
};
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
    /// The lock file cannot be created or opened for writing because it lies
    /// on a read-only filesystem (`EROFS`). A read-only environment then runs
    /// without a lock file, as LMDB does (ADR-0023 amendment 17,
    /// `OBS_RDONLY_LOCK_READONLY_FS`); any other caller treats it as the
    /// `EROFS` I/O error it is.
    ReadOnlyFs,
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
            LockOpenError::ReadOnlyFs => f.write_str("the lock file is on a read-only filesystem"),
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

/// The lock files this process has open, by [`RegistryKey`]. A poisoned
/// registry is still consistent (insert and remove do not panic midway), so
/// poison is ignored.
fn open_files() -> MutexGuard<'static, HashSet<RegistryKey>> {
    static FILES: OnceLock<Mutex<HashSet<RegistryKey>>> = OnceLock::new();
    FILES
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// How the registry identifies one lock file.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum RegistryKey {
    /// An existing file, by device and inode number. Every name of the file
    /// (another spelling of the path, a symbolic link, a hard link) has the
    /// same identity.
    File { dev: u64, ino: u64 },
    /// A file that does not exist yet, by its canonical directory joined with
    /// its file name. A missing file cannot be another name of a file this
    /// process has open: that file exists for as long as the descriptor is
    /// open.
    Missing(PathBuf),
}

/// This process's claim on one lock file in the registry. Dropping it removes
/// the entry, so every error path and every [`LockFile`] drop releases it
/// exactly once.
struct Registration {
    key: RegistryKey,
}

impl Registration {
    /// Claim the lock file at `path`, keyed by its file identity (see
    /// [`registry_key`]) so that every name of one file shares an entry.
    /// Fails with `AlreadyOpen` if already claimed.
    fn claim(path: &Path) -> Result<Registration, LockOpenError> {
        let key = registry_key(path)?;
        if !open_files().insert(key.clone()) {
            return Err(LockOpenError::AlreadyOpen);
        }
        Ok(Registration { key })
    }

    /// Re-key this claim by the identity of `file`, the descriptor just
    /// opened for it. A claim made while the file did not exist is keyed by
    /// path; once the open has created the file, keying it by identity is
    /// what makes a later hard or symbolic link to it collide. The identity
    /// can also differ from the one `stat`ed in [`Registration::claim`] if
    /// the name was replaced in between.
    ///
    /// Fails with `AlreadyOpen` if another claim of this process already
    /// holds that identity. The caller must then not close `file`: closing it
    /// would drop the other handle's record locks.
    fn rekey(&mut self, file: &File) -> Result<(), LockOpenError> {
        use std::os::unix::fs::MetadataExt;
        let m = file.metadata()?;
        let key = RegistryKey::File {
            dev: m.dev(),
            ino: m.ino(),
        };
        if key == self.key {
            return Ok(());
        }
        let mut files = open_files();
        if !files.insert(key.clone()) {
            return Err(LockOpenError::AlreadyOpen);
        }
        files.remove(&self.key);
        self.key = key;
        Ok(())
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        open_files().remove(&self.key);
    }
}

/// The registry key of the lock file at `path`, computed without opening a
/// descriptor (opening one and closing it again would drop this process's
/// record locks on that file, ADR-0023 amendment 2).
///
/// An existing file is keyed by `(st_dev, st_ino)` from a `stat` that follows
/// symbolic links, so a symbolic link, a hard link or another spelling of a
/// file this process has open is refused. A file that does not exist is keyed
/// by its canonical directory joined with its file name (only the directory
/// can be canonicalized). If another process creates the file between this
/// `stat` and the open, this process still holds no other entry for that
/// inode, because it had no descriptor to it; and once the open has a
/// descriptor, [`Registration::rekey`] re-keys the claim by the file's
/// identity, which also catches a name that was replaced by a link to a
/// file this process already has open.
fn registry_key(path: &Path) -> std::io::Result<RegistryKey> {
    use std::os::unix::fs::MetadataExt;
    match std::fs::metadata(path) {
        Ok(m) => {
            return Ok(RegistryKey::File {
                dev: m.dev(),
                ino: m.ino(),
            })
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
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
    Ok(RegistryKey::Missing(dir.canonicalize()?.join(name)))
}

/// How [`LockFile::open_with`] takes part in the first-opener election.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Election {
    /// Become the initializer, or wait for the initializer and join.
    Wait,
    /// Become the initializer, or give up at once.
    TryExclusive,
}

/// The `max_readers` recorded in the lock file at `path`, if it is a
/// finished ZeroDB lock file (it carries the final magic); `Ok(None)` if the
/// file does not exist or is not finished.
///
/// A process that joins a running environment adopts that value whatever it
/// asks for ([`LockFile::open`]), as LMDB does. A tool that attaches to an
/// environment it did not create reads it here first, so that, if every
/// other process closes before the tool opens, the tool reinitializes the
/// file with the same count. The value can change before the open if every
/// process closes and another reinitializes the file.
///
/// The read uses its own short-lived descriptor, claimed in this process's
/// registry like an open, so it never closes a descriptor of a lock file
/// this process has open (which would drop that handle's record locks).
///
/// # Errors
///
/// [`LockOpenError::AlreadyOpen`] if this process has the lock file open,
/// and I/O errors other than a missing file.
pub fn published_max_readers(path: &Path) -> Result<Option<u32>, LockOpenError> {
    let _registration = Registration::claim(path)?;
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if read_u64_at(&file, OFF_MAGIC)? != LOCK_MAGIC {
        return Ok(None);
    }
    match read_u32_at(&file, OFF_MAX_READERS) {
        Ok(n) => Ok(Some(n)),
        // A new initializer truncated the file between the two reads (every
        // process had closed the environment): the value is unknown again.
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// The mapped lock file. Field order matters for `Drop`: the map is
/// unmapped before the descriptor closes, closing the descriptor releases
/// this process's record locks, and only then is the file's entry removed
/// from the registry (keyed by the file's identity, not its path) — so no
/// second descriptor of this file can be opened by this process while the
/// first is still open.
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
    /// `data_path` is the environment's data file. Two guards keep the
    /// initializer from truncating a file that is not a lock file (ADR-0023
    /// amendment 16):
    ///
    /// - **Same file.** Every opener compares the lock file's identity with
    ///   the data file's (`st_dev`, `st_ino`), so a hard link, or a name that
    ///   a case-insensitive filesystem resolves to the data file, is refused.
    ///   A missing data file cannot be the lock file, and the check is
    ///   skipped. If this open created the lock file and it is still empty,
    ///   the refusal removes it; otherwise the name designates the data file
    ///   and is left alone.
    /// - **Foreign file.** The initializer accepts a file only if it is
    ///   empty, carries a lock magic (final, or the initializing one left by
    ///   an initializer that died), or has an all-zero header (the first 256
    ///   bytes, or the whole file if shorter) *and* a length an initializer
    ///   can leave: 8 (the initializing magic written to an empty file) or
    ///   256 + 128·k for k ≥ 1 (a header plus k reader slots). A power loss
    ///   can keep such a length and lose the block holding the magic. Any
    ///   other file is refused without being modified, including a stale
    ///   LMDB `lock.mdb`. A ZeroDB or LMDB data file always has a non-zero
    ///   header, so this refuses every *initialized* data file; a foreign
    ///   file with a zero header and one of those lengths is accepted and
    ///   overwritten.
    ///
    /// # Errors
    ///
    /// [`LockOpenError::AlreadyOpen`] if this process has it open (under any
    /// name), [`LockOpenError::Mismatch`] on a format mismatch,
    /// [`LockOpenError::ReadOnlyFs`] when the file cannot be created or
    /// opened for writing on a read-only filesystem, an
    /// [`LockOpenError::Io`] of kind `InvalidInput` when a guard above
    /// refuses the file, of kind `TimedOut` when the election retries run out
    /// before any process finishes initializing the file, and other I/O
    /// errors.
    pub fn open(
        path: &Path,
        data_path: &Path,
        max_readers: u32,
    ) -> Result<Arc<LockFile>, LockOpenError> {
        match Self::open_with(path, data_path, max_readers, Election::Wait)? {
            Some(lock) => Ok(lock),
            // `Election::Wait` never gives up on the election.
            None => unreachable!("a waiting open returned without a lock file"),
        }
    }

    /// Open the lock file at `path` only if this process wins the
    /// first-opener election at once; never wait for another opener
    /// (ADR-0023 amendment 18). `Ok(None)` means another process holds the
    /// election byte, that is, has the environment open or is opening it;
    /// the descriptor is closed and nothing in the file is changed.
    ///
    /// On success this process is the initializer, exactly as after
    /// [`LockFile::open`]: the file is validated by the same guards,
    /// truncated and re-initialized, and the election byte stays
    /// write-locked until [`LockFile::finish_open`] or the drop. A caller that
    /// never calls `finish_open` holds every other opener in the election for
    /// the handle's life; dropping it leaves the initializing magic, which the
    /// next initializer accepts.
    ///
    /// # Errors
    ///
    /// As [`LockFile::open`], except that it never times out in the election.
    pub fn try_open_exclusive(
        path: &Path,
        data_path: &Path,
        max_readers: u32,
    ) -> Result<Option<Arc<LockFile>>, LockOpenError> {
        Self::open_with(path, data_path, max_readers, Election::TryExclusive)
    }

    fn open_with(
        path: &Path,
        data_path: &Path,
        max_readers: u32,
        election: Election,
    ) -> Result<Option<Arc<LockFile>>, LockOpenError> {
        // Claimed before any descriptor exists: POSIX record locks belong to
        // the process, and closing a second descriptor would drop the first
        // handle's locks (ADR-0023 amendment 2). On every error below the
        // registration (or the `LockFile` owning it) drops and releases it.
        let mut registration = Registration::claim(path)?;
        let (file, created) = match open_or_create(path) {
            Ok(opened) => opened,
            Err(e) if e.raw_os_error() == Some(libc::EROFS) => {
                return Err(LockOpenError::ReadOnlyFs)
            }
            Err(e) => return Err(e.into()),
        };
        if let Err(e) = registration.rekey(&file) {
            // The descriptor reached a file this process already has open
            // (its name was replaced by a link to it between the `stat` and
            // the open). Closing it would drop that handle's record locks, so
            // the descriptor is leaked instead.
            std::mem::forget(file);
            return Err(e);
        }
        check_not_data_file(&file, created, path, data_path)?;
        /// How often a joiner that finds a half-built lock file retries the
        /// election before failing (ADR-0023 amendment 10).
        const ELECTION_RETRIES: u32 = 100;
        let mut attempt = 0;
        let initializer = loop {
            if record_lock(&file, libc::F_SETLK, WRLCK, ELECTION_BYTE)? {
                break true;
            }
            if election == Election::TryExclusive {
                // Another process holds the byte. This process holds no record
                // lock on the file, so closing the descriptor (with the
                // registration) drops nothing.
                return Ok(None);
            }
            // Blocks until the initializer downgrades in `finish_open`, or
            // exits.
            record_lock(&file, libc::F_SETLKW, RDLCK, ELECTION_BYTE)?;
            // Every other process may have exited while this one waited,
            // including an initializer that died before finishing. POSIX
            // converts this process's shared lock in place, and the upgrade
            // succeeds only when no other process holds the byte. A failed
            // F_SETLK leaves the shared lock held.
            if record_lock(&file, libc::F_SETLK, WRLCK, ELECTION_BYTE)? {
                break true;
            }
            // Another process holds the byte too. If the magic is not set,
            // the initializer died mid-way and the other holder is a joiner
            // in the same position: drop the lock and retry the election so
            // one of them initializes. A set magic usually means the
            // initializer finished, but it may also be left over from a
            // previous run: an initializer may have died before truncating,
            // or every process may have exited while this joiner waited.
            // Joiners that collide then adopt the previous run's table. That
            // is the same state as joining a table whose processes all
            // exited, which the protocol tolerates: `T` is the last published
            // value, dead PIDs are reapable, and a stale writer word is taken
            // over (ADR-0023 amendment 10).
            if read_u64_at(&file, OFF_MAGIC)? == LOCK_MAGIC {
                break false;
            }
            record_lock(&file, libc::F_SETLK, UNLCK, ELECTION_BYTE)?;
            attempt += 1;
            if attempt == ELECTION_RETRIES {
                return Err(LockOpenError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "lock file never finished initializing",
                )));
            }
            // Stagger retries by PID so two joiners do not keep colliding.
            std::thread::sleep(std::time::Duration::from_millis(
                1 + u64::from(std::process::id() % 7),
            ));
        };
        // A joiner reads `max_readers` with `pread` before mapping. It holds
        // the shared election lock, so the initializer has finished writing.
        // It adopts that count whatever it asked for, as an LMDB joiner sizes
        // its reader table from the lock file (`OBS_JOINER_FEWER_MAXREADERS`,
        // `OBS_JOINER_MORE_MAXREADERS`; ADR-0023 §1 as amended). A count no
        // initializer can write means the file is not a usable lock file.
        if initializer {
            check_lock_file_shape(&file, path)?;
        }
        let slots = if initializer {
            // An initializer never shrinks the reader table of a finished
            // lock file left by an earlier run: as LMDB, which sizes its
            // table from the lock file's length and never shortens it, the
            // count is the larger of the requested one and the recorded one
            // (`OBS_SOLE_OPENER_AFTER_LARGER_RUN_MAXREADERS`; ADR-0023 §1 as
            // amended). The slots themselves are wiped below as always.
            previous_run_slots(&file)?.map_or(max_readers, |n| n.max(max_readers))
        } else {
            let n = read_u32_at(&file, OFF_MAX_READERS)?;
            if n == 0 || n > zerodb_core::env::MAX_READERS_LIMIT {
                return Err(LockOpenError::Mismatch(format!(
                    "the lock file records {n} reader slots"
                )));
            }
            n
        };
        let len = OFF_SLOTS + SLOT_STRIDE * slots as usize;
        if initializer {
            // Truncate to zero first so no field survives from a previous
            // life, then mark the file as being initialized before it grows:
            // a process crash from here on leaves an empty file or one that
            // carries a lock magic, both of which the foreign-file guard
            // accepts. The final magic replaces this one in `finish_open`.
            file.set_len(0)?;
            {
                use std::os::unix::fs::FileExt;
                file.write_all_at(&LOCK_MAGIC_INIT.to_le_bytes(), OFF_MAGIC as u64)?;
            }
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
            for rs in lock.slots() {
                // Relaxed: published by the Release store of the magic in
                // `finish_open`, and no other process can map the file before the election
                // byte is downgraded.
                rs.txnid().store(READER_SLOT_FREE, Ordering::Relaxed);
                rs.owner().store(0, Ordering::Relaxed);
            }
            // Relaxed: published by the Release store of the magic in
            // `finish_open`.
            lock.u32_at(OFF_FORMAT)
                .store(LOCK_FORMAT, Ordering::Relaxed);
            lock.u32_at(OFF_MAX_READERS).store(slots, Ordering::Relaxed);
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
        Ok(Some(Arc::new(lock)))
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

    /// Publish the header and let other processes in: the initializer stores
    /// the magic, then converts its exclusive election lock to a shared one
    /// (atomic for POSIX locks). No-op for a joiner, which already holds it
    /// shared.
    ///
    /// # Errors
    ///
    /// The `fcntl` error.
    pub fn finish_open(&self) -> std::io::Result<()> {
        if self.initializer {
            // Release: the header fields stored in `open` and `T`, seeded by
            // the core since, are visible to any joiner that loads the magic
            // with Acquire. A joiner that sees the magic therefore sees a
            // seeded `T` (ADR-0023 amendment 10).
            self.u64_at(OFF_MAGIC).store(LOCK_MAGIC, Ordering::Release);
            record_lock(&self.file, libc::F_SETLK, RDLCK, ELECTION_BYTE)?;
        }
        Ok(())
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
    fn slots(&self) -> &[ReaderSlot] {
        debug_assert!(OFF_SLOTS + SLOT_STRIDE * self.slots as usize <= self.map.len());
        // SAFETY:
        // - Alignment: the map is page-aligned and `OFF_SLOTS` is a multiple
        //   of `ReaderSlot`'s 64-byte alignment (checked at compile time
        //   above), so the base pointer is aligned for `ReaderSlot`.
        // - Length: `open` maps exactly `OFF_SLOTS + SLOT_STRIDE * slots`
        //   bytes, and `SLOT_STRIDE == size_of::<ReaderSlot>()`, so the slice
        //   lies inside the mapping.
        // - Validity: `ReaderSlot` is `repr(C)` with an `AtomicU64` txnid at
        //   +0, an `AtomicU64` owner word at +8 and `AtomicU8` padding; every
        //   bit pattern is a valid value of each.
        // - Lifetime: the slice borrows `self`, and the mapping lives as long
        //   as `self` and is never remapped.
        // - Aliasing: other processes may write any byte of a slot
        //   concurrently. Every field, the padding included, is an atomic
        //   (an `UnsafeCell`), so a shared reference to the slot permits
        //   those writes and there is no non-atomic data race.
        unsafe {
            std::slice::from_raw_parts(
                self.map.as_mut_ptr().add(OFF_SLOTS).cast::<ReaderSlot>(),
                self.slots as usize,
            )
        }
    }
    fn pid(&self) -> u32 {
        self.pid
    }
    fn is_initializer(&self) -> bool {
        self.initializer
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
    fn is_alive(&self, pid: u32) -> std::io::Result<bool> {
        if pid == self.pid {
            // F_GETLK never reports the caller's own locks.
            return Ok(true);
        }
        probe_locked(&self.file, LIVENESS_BASE + i64::from(pid))
    }

    /// Returns `true` from both takeovers (a dead owner, and a stale word
    /// naming this process's PID), `false` from an ordinary acquisition.
    #[cfg(target_os = "linux")]
    fn lock_writer(&self) -> std::io::Result<bool> {
        let w = self.writer_word();
        loop {
            // Acquire: synchronizes with the previous owner's Release in
            // `unlock_writer`, so its T store and meta write are visible.
            let cur = match w.compare_exchange(0, self.pid, Ordering::Acquire, Ordering::Relaxed) {
                Ok(_) => return Ok(false),
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
                    return Ok(true);
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
                    return Ok(true);
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

    /// Always `false`: a POSIX record lock reports no previous owner, so a
    /// takeover from a dead process cannot be told apart.
    #[cfg(not(target_os = "linux"))]
    fn lock_writer(&self) -> std::io::Result<bool> {
        // POSIX record locks belong to the process, so any thread may release
        // it, and the kernel drops it if the process dies (ADR-0023 §2).
        record_lock(&self.file, libc::F_SETLKW, WRLCK, WRITER_BYTE).map(|_| false)
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

fn read_u64_at(file: &File, off: usize) -> std::io::Result<u64> {
    use std::os::unix::fs::FileExt;
    let mut b = [0u8; 8];
    match file.read_exact_at(&mut b, off as u64) {
        Ok(()) => Ok(u64::from_le_bytes(b)),
        // A file shorter than the magic was never initialized.
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(0),
        Err(e) => Err(e),
    }
}

/// The reader-slot count a finished lock file records, if `file` is one (it
/// carries the final magic) and is long enough to hold that many slots.
/// `None` for an empty, unfinished or inconsistent file, or a count outside
/// `1..=MAX_READERS_LIMIT`.
fn previous_run_slots(file: &File) -> std::io::Result<Option<u32>> {
    let len = file.metadata()?.len();
    if len < OFF_SLOTS as u64 || read_u64_at(file, OFF_MAGIC)? != LOCK_MAGIC {
        return Ok(None);
    }
    let n = read_u32_at(file, OFF_MAX_READERS)?;
    let needed = OFF_SLOTS as u64 + SLOT_STRIDE as u64 * u64::from(n);
    if n == 0 || n > zerodb_core::env::MAX_READERS_LIMIT || len < needed {
        return Ok(None);
    }
    Ok(Some(n))
}

/// Open the lock file at `path`, creating it if missing, and report whether
/// this call created it. `create_new` is tried first so the answer is exact;
/// if the file exists, it is opened as is. A file removed between the two
/// attempts is retried a bounded number of times.
fn open_or_create(path: &Path) -> std::io::Result<(File, bool)> {
    const ATTEMPTS: u32 = 8;
    let mut attempt = 0;
    loop {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
        {
            Ok(f) => return Ok((f, true)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }
        match OpenOptions::new().read(true).write(true).open(path) {
            Ok(f) => return Ok((f, false)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && attempt + 1 < ATTEMPTS => {
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// The same-file guard (ADR-0023 amendment 16): fail with `InvalidInput` if
/// `file`, the lock file just opened at `path`, is the data file at
/// `data_path`. A missing data file cannot be the lock file. On refusal, the
/// lock file is removed only if this open `created` it and it is still empty;
/// otherwise the name designates the data file, which must not be removed.
fn check_not_data_file(
    file: &File,
    created: bool,
    path: &Path,
    data_path: &Path,
) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let data = match std::fs::metadata(data_path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    let lock = file.metadata()?;
    if (lock.dev(), lock.ino()) != (data.dev(), data.ino()) {
        return Ok(());
    }
    if created && lock.len() == 0 {
        // Best effort: the refusal below is the result either way.
        let _ = std::fs::remove_file(path);
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!(
            "the lock file {} is the data file {}",
            path.display(),
            data_path.display()
        ),
    ))
}

/// Whether `len` is a length an initializer can leave behind: the
/// initializing magic alone in a truncated file, or the header plus at least
/// one reader slot.
fn is_initializer_length(len: u64) -> bool {
    let magic_only = (OFF_MAGIC + 8) as u64;
    let header = OFF_SLOTS as u64;
    let slot = SLOT_STRIDE as u64;
    len == magic_only || (len >= header + slot && (len - header).is_multiple_of(slot))
}

/// The foreign-file guard (ADR-0023 amendment 16), run by the initializer
/// before it truncates the lock file at `path`: accept an empty file, a file
/// carrying [`LOCK_MAGIC`] or [`LOCK_MAGIC_INIT`], or one whose header (the
/// first `OFF_SLOTS` bytes, or the whole file if shorter) is all zero and
/// whose length an initializer can leave ([`is_initializer_length`]); refuse
/// anything else with `InvalidInput`, leaving it untouched.
fn check_lock_file_shape(file: &File, path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    let len = file.metadata()?.len();
    if len == 0 {
        return Ok(());
    }
    let mut header = vec![0u8; OFF_SLOTS.min(usize::try_from(len).unwrap_or(OFF_SLOTS))];
    file.read_exact_at(&mut header, 0)?;
    let magic = header
        .get(OFF_MAGIC..OFF_MAGIC + 8)
        .and_then(|b| b.try_into().ok())
        .map(u64::from_le_bytes);
    if matches!(magic, Some(LOCK_MAGIC | LOCK_MAGIC_INIT))
        || (is_initializer_length(len) && header.iter().all(|&b| b == 0))
    {
        return Ok(());
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!(
            "the lock file {} holds data that is not a ZeroDB lock file; \
             refusing to overwrite it (remove it if it is not needed)",
            path.display()
        ),
    ))
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

    /// The path of a lock file in a fresh directory, which is removed with
    /// everything in it when this is dropped.
    struct TmpLock {
        path: std::path::PathBuf,
    }

    impl std::ops::Deref for TmpLock {
        type Target = std::path::Path;
        fn deref(&self) -> &std::path::Path {
            &self.path
        }
    }

    impl AsRef<std::path::Path> for TmpLock {
        fn as_ref(&self) -> &std::path::Path {
            &self.path
        }
    }

    impl Drop for TmpLock {
        fn drop(&mut self) {
            if let Some(dir) = self.path.parent() {
                let _ = std::fs::remove_dir_all(dir);
            }
        }
    }

    /// A lock-file path in a fresh directory per call. The counter keeps
    /// parallel tests apart when the clock (microsecond resolution on macOS)
    /// repeats.
    fn tmp() -> TmpLock {
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
        TmpLock {
            path: p.join("zerodb.lock"),
        }
    }

    #[test]
    fn first_opener_initializes_every_slot_free() {
        let path = tmp();
        let l = LockFile::open(&path, &path.with_file_name("zerodb.dat"), 8).expect("open");
        assert!(l.is_initializer());
        assert_eq!(l.slots().len(), 8);
        for i in 0..8 {
            assert_eq!(
                l.slots()[i].txnid().load(Ordering::SeqCst),
                READER_SLOT_FREE
            );
            assert_eq!(l.slots()[i].owner().load(Ordering::SeqCst), 0);
        }
        assert_eq!(l.published_txnid().load(Ordering::SeqCst), 0);
        assert_eq!(l.pid(), std::process::id());
        l.bind_page_size(4096).unwrap();
        l.finish_open().unwrap();
    }

    /// A try-exclusive open with no other opener initializes the file and
    /// leaves the initializing magic; the next opener accepts it.
    #[test]
    fn try_open_exclusive_initializes_without_publishing() {
        let path = tmp();
        let data = path.with_file_name("zerodb.dat");
        let l = LockFile::try_open_exclusive(&path, &data, 8)
            .expect("open")
            .expect("no other opener");
        assert!(l.is_initializer());
        assert_eq!(l.slots().len(), 8);
        assert_eq!(read_u64_at(&l.file, OFF_MAGIC).unwrap(), LOCK_MAGIC_INIT);
        assert!(matches!(
            LockFile::try_open_exclusive(&path, &data, 8),
            Err(LockOpenError::AlreadyOpen)
        ));
        drop(l);
        let l = LockFile::open(&path, &data, 8).expect("reopen");
        assert!(l.is_initializer());
    }

    /// An initializer never shrinks the reader table of a finished lock file
    /// from an earlier run, as LMDB
    /// (`OBS_SOLE_OPENER_AFTER_LARGER_RUN_MAXREADERS`), grows it when asked
    /// for more, and ignores the count of an unfinished file.
    #[test]
    fn initializer_keeps_a_larger_finished_table() {
        let path = tmp();
        let data = path.with_file_name("zerodb.dat");
        let open_finished = |n: u32| {
            let l = LockFile::open(&path, &data, n).unwrap();
            assert!(l.is_initializer());
            l.finish_open().unwrap();
            l.slots().len()
        };
        assert_eq!(open_finished(12), 12);
        assert_eq!(open_finished(4), 12, "kept the larger table");
        assert_eq!(open_finished(20), 20, "grew");
        assert_eq!(open_finished(4), 20);
        // An unfinished file (initializing magic only) records no count, so
        // the next initializer sizes the table as asked.
        drop(
            LockFile::try_open_exclusive(&path, &data, 30)
                .unwrap()
                .unwrap(),
        );
        assert_eq!(open_finished(4), 4, "an unfinished file's count is ignored");
    }

    /// The recorded `max_readers` is read only from a finished lock file, and
    /// never through a second descriptor of one this process has open.
    #[test]
    fn published_max_readers_reads_a_finished_file() {
        let path = tmp();
        let data = path.with_file_name("zerodb.dat");
        assert_eq!(published_max_readers(&path).unwrap(), None, "missing");
        let l = LockFile::try_open_exclusive(&path, &data, 8)
            .unwrap()
            .unwrap();
        assert!(matches!(
            published_max_readers(&path),
            Err(LockOpenError::AlreadyOpen)
        ));
        drop(l);
        assert_eq!(published_max_readers(&path).unwrap(), None, "unfinished");
        let l = LockFile::open(&path, &data, 12).unwrap();
        l.finish_open().unwrap();
        drop(l);
        assert_eq!(published_max_readers(&path).unwrap(), Some(12));
    }

    #[test]
    fn second_open_in_process_is_refused_before_any_fd() {
        let path = tmp();
        let a = LockFile::open(&path, &path.with_file_name("zerodb.dat"), 4).expect("open");
        assert!(matches!(
            LockFile::open(&path, &path.with_file_name("zerodb.dat"), 4),
            Err(LockOpenError::AlreadyOpen)
        ));
        // `a` still holds its locks and works.
        a.lock_writer().unwrap();
        a.unlock_writer();
        drop(a);
        // After the first handle is gone, the path can be opened again.
        LockFile::open(&path, &path.with_file_name("zerodb.dat"), 4).expect("reopen");
    }

    /// On macOS this cannot tell a released POSIX lock from a held one (a
    /// process never conflicts with itself); the two-process test
    /// `writer_lock_released_on_another_thread_admits_another_process` in
    /// `crates/zerodb/tests/multi_process.rs` covers that case.
    #[test]
    fn writer_lock_releases_from_another_thread() {
        let path = tmp();
        let l = LockFile::open(&path, &path.with_file_name("zerodb.dat"), 4).expect("open");
        assert!(
            !l.lock_writer().unwrap(),
            "an uncontended lock is no takeover"
        );
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
        let _a = LockFile::open(&path, &path.with_file_name("zerodb.dat"), 4).expect("open");
        let dir = path.parent().unwrap();
        let other = dir.join(".").join("zerodb.lock");
        assert!(matches!(
            LockFile::open(&other, &other.with_file_name("zerodb.dat"), 4),
            Err(LockOpenError::AlreadyOpen)
        ));
    }

    /// A hard link names the same file under another name. Opening it would
    /// give this process a second descriptor to its own lock file.
    #[test]
    fn a_hard_link_to_an_open_lock_file_is_refused() {
        let path = tmp();
        let _a = LockFile::open(&path, &path.with_file_name("zerodb.dat"), 4).expect("open");
        let link = path.parent().unwrap().join("hard-link.lock");
        std::fs::hard_link(&path, &link).unwrap();
        assert!(matches!(
            LockFile::open(&link, &link.with_file_name("zerodb.dat"), 4),
            Err(LockOpenError::AlreadyOpen)
        ));
    }

    /// The same through a symbolic link.
    #[test]
    fn a_symlink_to_an_open_lock_file_is_refused() {
        let path = tmp();
        let _a = LockFile::open(&path, &path.with_file_name("zerodb.dat"), 4).expect("open");
        let link = path.parent().unwrap().join("sym-link.lock");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(matches!(
            LockFile::open(&link, &link.with_file_name("zerodb.dat"), 4),
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
        let l = LockFile::open(&path, &path.with_file_name("zerodb.dat"), 4).expect("open");
        for stale in [l.pid(), l.pid() | WAITERS] {
            l.writer_word().store(stale, Ordering::SeqCst);
            let start = std::time::Instant::now();
            assert!(l.lock_writer().unwrap(), "reported as a takeover");
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
        let l = LockFile::open(&path, &path.with_file_name("zerodb.dat"), 4).expect("open");
        l.bind_page_size(4096).unwrap();
        assert!(l.bind_page_size(8192).is_err());
    }

    // --- File guards (ADR-0023 amendment 16) ---

    /// The data-file path next to the lock file `path`.
    fn data_of(path: &Path) -> PathBuf {
        path.with_file_name("zerodb.dat")
    }

    fn assert_invalid_input(r: Result<Arc<LockFile>, LockOpenError>) {
        match r {
            Err(LockOpenError::Io(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput, "{e}")
            }
            Err(other) => panic!("expected Io(InvalidInput), got {other:?}"),
            Ok(_) => panic!("expected Io(InvalidInput), got a lock file"),
        }
    }

    /// Asserts `l` is a freshly initialized table of `n` slots and finishes it.
    fn assert_fresh(l: &LockFile, n: usize) {
        assert!(l.is_initializer());
        assert_eq!(l.slots().len(), n);
        for s in l.slots() {
            assert_eq!(s.txnid().load(Ordering::SeqCst), READER_SLOT_FREE);
            assert_eq!(s.owner().load(Ordering::SeqCst), 0);
        }
        assert_eq!(l.published_txnid().load(Ordering::SeqCst), 0);
        assert_eq!(l.u32_at(OFF_PAGE_SIZE).load(Ordering::SeqCst), 0);
        l.finish_open().unwrap();
        assert_eq!(l.u64_at(OFF_MAGIC).load(Ordering::SeqCst), LOCK_MAGIC);
    }

    /// An initializer killed after writing the initializing magic leaves it in
    /// the file; the next opener accepts the file and initializes it.
    #[test]
    fn crashed_initializer_with_provisional_magic_is_reinitialized() {
        let path = tmp();
        let mut bytes = vec![0xA5u8; OFF_SLOTS + 4 * SLOT_STRIDE];
        bytes[..8].copy_from_slice(&LOCK_MAGIC_INIT.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        let l = LockFile::open(&path, &data_of(&path), 4).expect("open");
        assert_fresh(&l, 4);
    }

    /// A lock file whose whole header reads as zero (power loss kept the
    /// length and lost the magic block) is accepted as never initialized
    /// when its length is one an initializer leaves: 8, or the header plus
    /// k ≥ 1 slots, for any k (here not this `max_readers`). The slot area
    /// holds garbage.
    #[test]
    fn all_zero_lock_header_is_reinitialized() {
        for len in [
            8usize,
            OFF_SLOTS + SLOT_STRIDE,
            OFF_SLOTS + 3 * SLOT_STRIDE,
            OFF_SLOTS + 64 * SLOT_STRIDE,
        ] {
            let path = tmp();
            let mut bytes = vec![0u8; len];
            for b in bytes.iter_mut().skip(OFF_SLOTS) {
                *b = 0xA5;
            }
            std::fs::write(&path, &bytes).unwrap();
            let l = LockFile::open(&path, &data_of(&path), 4)
                .unwrap_or_else(|e| panic!("length {len}: {e}"));
            assert_fresh(&l, 4);
        }
    }

    /// An all-zero file whose length no initializer leaves is refused
    /// untouched: each length sits just off an accepted one.
    #[test]
    fn all_zero_file_of_another_length_is_refused() {
        for len in [
            1usize,
            7,
            9,
            OFF_SLOTS,
            OFF_SLOTS + SLOT_STRIDE - 1,
            OFF_SLOTS + SLOT_STRIDE + 1,
            OFF_SLOTS + 3 * SLOT_STRIDE + 17,
        ] {
            let path = tmp();
            let bytes = vec![0u8; len];
            std::fs::write(&path, &bytes).unwrap();
            assert_invalid_input(LockFile::open(&path, &data_of(&path), 4));
            assert_eq!(std::fs::read(&path).unwrap(), bytes, "length {len}");
        }
    }

    /// A non-empty file with neither lock magic and a non-zero header is
    /// refused, and its bytes are left as they were. The inputs include an
    /// LMDB data file's shape (`0xBEEFC0DE` at byte 16) and a stale LMDB lock
    /// file (`0xBEEFC0DE` at byte 0), both with an otherwise zero, otherwise
    /// accepted length.
    #[test]
    fn foreign_file_is_refused_without_truncating() {
        let mut zero_but_one = vec![0u8; 4096];
        zero_but_one[OFF_SLOTS - 1] = 1;
        let mut zdb1 = vec![0u8; 8192];
        zdb1[32..36].copy_from_slice(b"ZDB1");
        let mut lmdb_data = vec![0u8; 8192];
        lmdb_data[16..20].copy_from_slice(&0xBEEF_C0DEu32.to_le_bytes());
        let mut lmdb_lock = vec![0u8; 8192];
        lmdb_lock[0..4].copy_from_slice(&0xBEEF_C0DEu32.to_le_bytes());
        for bytes in [
            b"not a lock file".to_vec(),
            zero_but_one,
            zdb1,
            lmdb_data,
            lmdb_lock,
        ] {
            let path = tmp();
            std::fs::write(&path, &bytes).unwrap();
            match LockFile::open(&path, &data_of(&path), 4) {
                Err(LockOpenError::Io(e)) => {
                    assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
                    assert!(
                        e.to_string().contains(&path.display().to_string()),
                        "the message names the lock file: {e}"
                    );
                }
                Err(other) => panic!("expected Io(InvalidInput), got {other:?}"),
                Ok(_) => panic!("expected Io(InvalidInput), got a lock file"),
            }
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
            // The refusal released the registry entry.
            std::fs::remove_file(&path).unwrap();
            LockFile::open(&path, &data_of(&path), 4).expect("open after removal");
        }
    }

    /// A lock path that is a hard link to the data file is refused before
    /// anything is truncated, and the link is not removed.
    #[test]
    fn lock_file_that_is_the_data_file_is_refused() {
        let path = tmp();
        let data = data_of(&path);
        let bytes = vec![0u8; 8192];
        std::fs::write(&data, &bytes).unwrap();
        std::fs::hard_link(&data, &path).unwrap();
        assert_invalid_input(LockFile::open(&path, &data, 4));
        assert_eq!(std::fs::read(&data).unwrap(), bytes);
        assert!(path.exists(), "the name designates the data file");
    }

    /// When the lock open itself created the file and it turns out to be the
    /// data file (here the data path is a symbolic link to the lock path, the
    /// portable stand-in for a case-insensitive name), the refusal removes the
    /// empty file it created.
    #[test]
    fn a_created_lock_file_that_is_the_data_file_is_removed() {
        let path = tmp();
        let data = data_of(&path);
        std::os::unix::fs::symlink(&path, &data).unwrap();
        assert_invalid_input(LockFile::open(&path, &data, 4));
        assert!(!path.exists(), "the created lock file was left behind");
    }

    /// A missing data file cannot be the lock file: the check is skipped and
    /// the open initializes as usual.
    #[test]
    fn missing_data_file_skips_the_same_file_check() {
        let path = tmp();
        assert!(!data_of(&path).exists());
        let l = LockFile::open(&path, &data_of(&path), 4).expect("open");
        assert_fresh(&l, 4);
    }
}
