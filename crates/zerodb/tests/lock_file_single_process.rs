//! Single-process coverage for the lock-file environment (ADR-0023), on by
//! default.
//!
//! These tests run entirely within one process: they check that the lock
//! file changes nothing observable about reads/writes, that it appears by
//! default and not with `lock_file(false)`, and that the existing
//! `EnvAlreadyOpened` / reopen-after-close behavior is unaffected. True
//! multi-process coordination is covered elsewhere (Task 7+); this file is
//! the "did wiring it up break the default path" smoke test.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use zerodb::{EnvOpenOptions, Error, LOCK_FILE_NAME};

// --- tiny self-cleaning temp dir (no tempfile dep on the allowlist) ---

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> TempDir {
        let pid = std::process::id();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("zerodb-lockfile-{pid}-{nanos}-{seq}"));
        std::fs::create_dir_all(&path).expect("create temp dir");
        TempDir { path }
    }
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn opts() -> EnvOpenOptions {
    let mut o = EnvOpenOptions::new();
    o.map_size(16 << 20).page_size(4096).lock_file(true);
    o
}

#[test]
fn lock_file_env_reads_and_writes_like_the_default() {
    let dir = TempDir::new();
    let env = opts().open(dir.path()).expect("open");
    assert!(dir.path().join(LOCK_FILE_NAME).exists());
    let db = env.main_database();
    let mut w = env.write_txn().unwrap();
    db.put(&mut w, b"k", b"v").unwrap();
    w.commit().unwrap();
    let r = env.read_txn().unwrap();
    assert_eq!(db.get(&r, b"k").unwrap(), Some(&b"v"[..]));
}

/// The lock file is on by default (ADR-0023), as in LMDB.
#[test]
fn default_env_creates_the_lock_file() {
    let dir = TempDir::new();
    let mut o = EnvOpenOptions::new();
    o.map_size(16 << 20);
    assert!(o.get_lock_file());
    let env = o.open(dir.path()).expect("open");
    assert!(dir.path().join(LOCK_FILE_NAME).exists());
    assert!(env.has_lock_file());
}

/// `lock_file(false)` (heed's `NO_LOCK`) opts out: no lock file is created.
#[test]
fn lock_file_false_creates_no_lock_file() {
    let dir = TempDir::new();
    let mut o = EnvOpenOptions::new();
    o.map_size(16 << 20).lock_file(false);
    let env = o.open(dir.path()).expect("open");
    assert!(!dir.path().join(LOCK_FILE_NAME).exists());
    assert!(!env.has_lock_file());
    let mut w = env.write_txn().unwrap();
    env.main_database().put(&mut w, b"k", b"v").unwrap();
    w.commit().unwrap();
    assert!(!dir.path().join(LOCK_FILE_NAME).exists());
}

/// The number of file descriptors this process has open: the entries of
/// `/proc/self/fd` (Linux) or `/dev/fd` (macOS and other Unixes). The
/// directory handle the listing itself holds is counted every time, so
/// differences between two counts are exact.
fn open_fd_count() -> usize {
    let dir = if cfg!(target_os = "linux") {
        "/proc/self/fd"
    } else {
        "/dev/fd"
    };
    std::fs::read_dir(dir).expect("list open fds").count()
}

/// Set in the child process that runs the body of
/// `thousand_sequential_envs_leak_no_descriptors` alone.
const FD_LEAK_CHILD: &str = "ZERODB_FD_LEAK_CHILD";

/// Opening and dropping 1,000 default (lock-file) environments, each in a
/// fresh directory, leaks no file descriptor: the data file, the lock file
/// and anything else an open holds are all closed on drop.
///
/// The body runs in a child process (this test binary, filtered to this
/// test), so the other tests of this binary, running on parallel threads,
/// cannot open or close descriptors between the two counts.
#[test]
fn thousand_sequential_envs_leak_no_descriptors() {
    if std::env::var_os(FD_LEAK_CHILD).is_none() {
        let exe = std::env::current_exe().expect("current test binary");
        let out = std::process::Command::new(exe)
            .args([
                "--exact",
                "thousand_sequential_envs_leak_no_descriptors",
                "--test-threads=1",
                "--nocapture",
            ])
            .env(FD_LEAK_CHILD, "1")
            .output()
            .expect("spawn the fd-leak child");
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            out.status.success(),
            "fd-leak child failed: {}\nstdout:\n{stdout}\nstderr:\n{}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );
        // The filter matched this test and it ran in the child.
        assert!(stdout.contains("1 passed"), "child stdout:\n{stdout}");
        return;
    }

    let root = TempDir::new();
    let before = open_fd_count();
    for i in 0..1000 {
        let dir = root.path().join(format!("env-{i}"));
        std::fs::create_dir(&dir).unwrap();
        let mut o = EnvOpenOptions::new();
        o.map_size(1 << 20);
        let env = o.open(&dir).expect("open");
        assert!(env.has_lock_file());
        drop(env);
        std::fs::remove_dir_all(&dir).unwrap();
    }
    let after = open_fd_count();
    assert!(
        after <= before + 8,
        "open file descriptors grew from {before} to {after} over 1,000 envs"
    );
}

#[test]
fn double_open_in_process_keeps_first_env_working() {
    let dir = TempDir::new();
    let env = opts().open(dir.path()).expect("open");
    assert!(matches!(
        opts().open(dir.path()),
        Err(Error::EnvAlreadyOpened)
    ));
    let db = env.main_database();
    let mut w = env.write_txn().expect("first env still writes");
    db.put(&mut w, b"a", b"1").unwrap();
    w.commit().unwrap();
}

#[test]
fn reopen_after_close_reinitializes() {
    let dir = TempDir::new();
    {
        let env = opts().open(dir.path()).unwrap();
        let mut w = env.write_txn().unwrap();
        env.main_database().put(&mut w, b"x", b"1").unwrap();
        w.commit().unwrap();
    }
    let env = opts().open(dir.path()).unwrap();
    let r = env.read_txn().unwrap();
    assert_eq!(env.main_database().get(&r, b"x").unwrap(), Some(&b"1"[..]));
}

/// `EnvClosingEvent::wait` promises the environment is fully closed (SPEC 04
/// TXN-53), so an immediate reopen must succeed. With a lock file this
/// includes releasing the lock file: the last handle may drop on another
/// thread, and the close event must not fire while that handle's lock file is
/// still open in this process.
#[test]
fn reopen_after_closing_event_wait_succeeds() {
    let dir = TempDir::new();
    for i in 0..200 {
        let env = opts()
            .open(dir.path())
            .unwrap_or_else(|e| panic!("open {i} failed: {e:?}"));
        let clone = env.clone();
        let (go, wait_go) = std::sync::mpsc::channel::<()>();
        let t = std::thread::spawn(move || {
            let r = clone.read_txn().expect("read txn");
            drop(r);
            wait_go.recv().expect("go");
            // The last handle: the environment closes on this thread.
            drop(clone);
        });
        let closing = env.prepare_for_closing();
        go.send(()).expect("send go");
        closing.wait();
        // Reopen before joining, so nothing but the close event orders the
        // reopen after the other thread's teardown.
        let reopened = opts().open(dir.path());
        t.join().expect("thread");
        reopened.unwrap_or_else(|e| panic!("reopen {i} after wait failed: {e:?}"));
    }
    let env = opts().open(dir.path()).expect("final reopen");
    drop(env);
}

/// An open that fails after the lock file was opened releases it, so a later
/// open of the same directory succeeds. Here the data file name is taken by a
/// directory, so opening the data file fails.
#[test]
fn failed_open_after_lock_file_releases_it() {
    let dir = TempDir::new();
    std::fs::create_dir(dir.path().join("taken")).expect("mkdir");
    let mut bad = opts();
    bad.data_file_name("taken");
    assert!(bad.open(dir.path()).is_err(), "a directory is no data file");
    let env = opts().open(dir.path()).expect("open after a failed open");
    let mut w = env.write_txn().unwrap();
    env.main_database().put(&mut w, b"k", b"v").unwrap();
    w.commit().unwrap();
}

/// An open that fails in the engine (the data file is not a ZeroDB file)
/// releases the lock file too.
#[test]
fn failed_engine_open_releases_lock_file() {
    let dir = TempDir::new();
    let data = dir.path().join("garbage.mdb");
    std::fs::write(&data, vec![0xA5u8; 4096 * 4]).expect("write garbage");
    let mut bad = opts();
    bad.data_file_name("garbage.mdb");
    assert!(bad.open(dir.path()).is_err(), "garbage is no env");
    std::fs::remove_file(&data).expect("rm garbage");
    let env = bad.open(dir.path()).expect("open after a failed open");
    let r = env.read_txn().unwrap();
    assert_eq!(env.main_database().get(&r, b"k").unwrap(), None);
}

/// A lock-file name equal to the data-file name would make the lock-file
/// initializer truncate the data file. The open must be rejected before any
/// file is touched, and the data must survive.
#[test]
fn lock_file_name_equal_to_data_file_name_is_rejected() {
    let dir = TempDir::new();
    {
        let mut o = EnvOpenOptions::new();
        o.map_size(16 << 20).page_size(4096);
        let env = o.open(dir.path()).expect("open");
        let mut w = env.write_txn().unwrap();
        env.main_database().put(&mut w, b"keep", b"me").unwrap();
        w.commit().unwrap();
    }
    let mut bad = opts();
    bad.lock_file_name(zerodb::DATA_FILE_NAME);
    match bad.open(dir.path()) {
        Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected Io(InvalidInput), got {other:?}"),
    }
    let mut o = EnvOpenOptions::new();
    o.map_size(16 << 20).page_size(4096);
    let env = o.open(dir.path()).expect("plain reopen");
    let r = env.read_txn().unwrap();
    assert_eq!(
        env.main_database().get(&r, b"keep").unwrap(),
        Some(&b"me"[..])
    );
}

/// An out-of-range `max_readers` or `max_dbs` is rejected before the lock
/// file is created. The lock file is sized from `max_readers` and its
/// initializer writes every slot, so creating it first would allocate up to
/// 512 GiB on disk for `max_readers(u32::MAX)` before the open failed.
#[test]
fn out_of_range_limits_are_rejected_before_the_lock_file_exists() {
    // Both limits are 1 << 20 (SPEC 00 rows 4 and 5).
    let limit: u32 = 1 << 20;
    for (readers, dbs) in [(limit + 1, 8), (126, limit + 1)] {
        let dir = TempDir::new();
        let e = opts()
            .max_readers(readers)
            .max_dbs(dbs)
            .open(dir.path())
            .unwrap_err();
        match e {
            Error::Io(io) => assert_eq!(io.kind(), std::io::ErrorKind::InvalidInput),
            other => panic!("expected Io(InvalidInput), got {other:?}"),
        }
        assert!(
            !dir.path().join(LOCK_FILE_NAME).exists(),
            "the lock file was created before the limits were checked \
             (max_readers {readers}, max_dbs {dbs})"
        );
    }
}

const MIB: usize = 1 << 20;

fn opts_with(lock_file: bool, map_size: Option<usize>) -> EnvOpenOptions {
    let mut o = EnvOpenOptions::new();
    o.page_size(4096).lock_file(lock_file);
    if let Some(m) = map_size {
        o.map_size(m);
    }
    o
}

/// Opening with a map smaller than the committed data rounds the map up to
/// exactly `(last_pg + 1) * page_size`, as LMDB does
/// (`OBS_OPEN_BELOW_LAST_PG = "minsize"`, ADR-0023 amendment 15).
#[test]
fn open_below_last_pg_rounds_up_to_the_minimum() {
    for lock_file in [false, true] {
        let dir = TempDir::new();
        {
            let env = opts_with(lock_file, Some(8 * MIB))
                .open(dir.path())
                .unwrap();
            let db = env.main_database();
            let value = vec![3u8; 4000];
            let mut w = env.write_txn().unwrap();
            for i in 0..400u32 {
                db.put(&mut w, format!("k{i:04}").as_bytes(), &value)
                    .unwrap();
            }
            w.commit().unwrap();
            assert!(
                (env.info().last_pgno + 1) * 4096 > MIB as u64,
                "setup: the data is past 1 MiB"
            );
        }
        let env = opts_with(lock_file, Some(MIB)).open(dir.path()).unwrap();
        let info = env.info();
        assert_eq!(
            info.map_size,
            (info.last_pgno + 1) * 4096,
            "lock_file {lock_file}: the map rounds up to the minimum"
        );
        let r = env.read_txn().unwrap();
        let db = env.main_database();
        for i in [0u32, 199, 399] {
            assert_eq!(
                db.get(&r, format!("k{i:04}").as_bytes())
                    .unwrap()
                    .map(<[u8]>::len),
                Some(4000),
                "lock_file {lock_file}: key {i}"
            );
        }
    }
}

/// A requested map size above `MAX_MAP_SIZE` is refused with
/// `Io(InvalidInput)` before the data file is created (ADR-0023 amendment 15,
/// decision 6).
#[test]
fn open_rejects_a_map_size_above_the_cap() {
    for lock_file in [false, true] {
        let dir = TempDir::new();
        let size = usize::try_from(zerodb::MAX_MAP_SIZE + 4096).unwrap();
        match opts_with(lock_file, Some(size)).open(dir.path()) {
            Err(Error::Io(e)) => assert_eq!(
                e.kind(),
                std::io::ErrorKind::InvalidInput,
                "lock_file {lock_file}"
            ),
            other => panic!(
                "lock_file {lock_file}: expected Io(InvalidInput), got {:?}",
                other.err()
            ),
        }
        assert!(
            !dir.path().join(zerodb::DATA_FILE_NAME).exists(),
            "lock_file {lock_file}: the data file was created"
        );
    }
}

/// A commit never lowers the persisted map size: an environment opened with a
/// smaller map than the meta records keeps the meta's larger size
/// (`OBS_COMMIT_PERSISTS_OWN_MAPSIZE` = false).
#[test]
fn smaller_map_commit_keeps_the_larger_meta_size_in_one_process() {
    for lock_file in [false, true] {
        let dir = TempDir::new();
        {
            let env = opts_with(lock_file, Some(8 * MIB))
                .open(dir.path())
                .unwrap();
            let mut w = env.write_txn().unwrap();
            env.main_database().put(&mut w, b"a", b"1").unwrap();
            w.commit().unwrap();
        }
        {
            let env = opts_with(lock_file, Some(MIB)).open(dir.path()).unwrap();
            assert_eq!(env.info().map_size, MIB as u64, "the data fits 1 MiB");
            let mut w = env.write_txn().unwrap();
            env.main_database().put(&mut w, b"b", b"2").unwrap();
            w.commit().unwrap();
        }
        let env = opts_with(lock_file, None).open(dir.path()).unwrap();
        assert_eq!(
            env.info().map_size,
            8 * MIB as u64,
            "lock_file {lock_file}: the meta keeps 8 MiB"
        );
    }
}

/// Every combination the `resize` tests run over: with and without a lock
/// file, with the default backing and with `WRITE_MAP`.
const RESIZE_MODES: [(bool, bool); 4] =
    [(false, false), (true, false), (false, true), (true, true)];

/// [`opts_with`], with `WRITE_MAP` when `write_map`.
fn resize_opts(lock_file: bool, write_map: bool, map_size: usize) -> EnvOpenOptions {
    let mut o = opts_with(lock_file, Some(map_size));
    if write_map {
        o.flags(zerodb::EnvFlags::WRITE_MAP);
    }
    o
}

/// `resize` while this env has a live write txn fails with `EINVAL`, as
/// `mdb_env_set_mapsize` does (`OBS_SET_MAPSIZE_WITH_WRITE_TXN`, ADR-0023
/// amendment 15). The map is unchanged and the write txn still commits.
#[test]
fn resize_with_a_live_write_txn_is_refused() {
    for (lock_file, write_map) in RESIZE_MODES {
        let dir = TempDir::new();
        let env = resize_opts(lock_file, write_map, MIB)
            .open(dir.path())
            .unwrap();
        let mut w = env.write_txn().unwrap();
        env.main_database().put(&mut w, b"k", b"v").unwrap();
        match env.resize(2 * MIB as u64) {
            Err(Error::Io(e)) => assert_eq!(
                e.raw_os_error(),
                Some(22),
                "lock_file {lock_file}, write_map {write_map}: {e:?}"
            ),
            other => panic!(
                "lock_file {lock_file}, write_map {write_map}: expected Io(EINVAL), got {other:?}"
            ),
        }
        assert_eq!(env.info().map_size, MIB as u64);
        w.commit().unwrap();
        // With the write txn gone, the same call succeeds.
        env.resize(2 * MIB as u64).unwrap();
        assert_eq!(env.info().map_size, 2 * MIB as u64);
    }
}

/// A live read txn does not block `resize` (decision 5, LMDB's return code).
/// Its borrowed value and a whole-map borrow stay valid and readable across
/// the resize, and a write after it can use the larger map.
#[test]
fn resize_with_a_live_read_txn_succeeds_and_the_value_stays_readable() {
    for (lock_file, write_map) in RESIZE_MODES {
        let dir = TempDir::new();
        let env = resize_opts(lock_file, write_map, MIB)
            .open(dir.path())
            .unwrap();
        let db = env.main_database();
        let mut w = env.write_txn().unwrap();
        db.put(&mut w, b"held", &[9u8; 3000]).unwrap();
        w.commit().unwrap();

        let r = env.read_txn().unwrap();
        let value = db.get(&r, b"held").unwrap().expect("held value");
        let whole = env.inner().backing_bytes();
        let head = whole[..64].to_vec();
        env.resize(2 * MIB as u64)
            .unwrap_or_else(|e| panic!("lock_file {lock_file}, write_map {write_map}: {e:?}"));
        assert_eq!(env.info().map_size, 2 * MIB as u64);
        // Both borrows outlived the resize.
        assert_eq!(value, &[9u8; 3000][..]);
        assert_eq!(&whole[..64], &head[..]);
        assert_eq!(db.get(&r, b"held").unwrap(), Some(&[9u8; 3000][..]));
        drop(r);

        // Fill past the old 1 MiB map: no MapFull.
        let mut w = env.write_txn().unwrap();
        for i in 0..300u32 {
            db.put(&mut w, format!("f{i:04}").as_bytes(), &[5u8; 4000])
                .unwrap();
        }
        w.commit().unwrap();
        assert!((env.info().last_pgno + 1) * 4096 > MIB as u64);
        if write_map {
            // WRITE_MAP grew the file to the logical size under the writer
            // locks.
            assert!(env.real_disk_size().unwrap() >= 2 * MIB as u64);
        }
        let r = env.read_txn().unwrap();
        assert_eq!(db.get(&r, b"f0299").unwrap(), Some(&[5u8; 4000][..]));
    }
}

/// `resize` below the committed data rounds up to exactly
/// `(last_pg + 1) * page_size`, as LMDB does silently (ADR-0023 amendment
/// 15, step 4).
#[test]
fn resize_rounds_up_to_the_last_page() {
    for (lock_file, write_map) in RESIZE_MODES {
        let dir = TempDir::new();
        let env = resize_opts(lock_file, write_map, 4 * MIB)
            .open(dir.path())
            .unwrap();
        let db = env.main_database();
        let mut w = env.write_txn().unwrap();
        for i in 0..400u32 {
            db.put(&mut w, format!("k{i:04}").as_bytes(), &[3u8; 4000])
                .unwrap();
        }
        w.commit().unwrap();
        env.resize(4096).unwrap();
        let info = env.info();
        assert_eq!(
            info.map_size,
            (info.last_pgno + 1) * 4096,
            "lock_file {lock_file}, write_map {write_map}"
        );
        let r = env.read_txn().unwrap();
        assert_eq!(db.get(&r, b"k0399").unwrap(), Some(&[3u8; 4000][..]));
        drop(r);
        // New data soon allocates past the rounded map.
        let mut w = env.write_txn().unwrap();
        let full = (0..50u32).any(|i| {
            matches!(
                db.put(&mut w, format!("over{i:02}").as_bytes(), &[1u8; 8000]),
                Err(Error::Mdb(zerodb::MdbError::MapFull))
            )
        });
        assert!(
            full,
            "lock_file {lock_file}, write_map {write_map}: no MapFull"
        );
    }
}

/// A caller-supplied size above `MAX_MAP_SIZE` is `Io(InvalidInput)`; the
/// map is unchanged (ADR-0023 amendment 15, decision 6).
#[test]
fn resize_above_the_cap_is_refused() {
    for (lock_file, write_map) in RESIZE_MODES {
        let dir = TempDir::new();
        let env = resize_opts(lock_file, write_map, MIB)
            .open(dir.path())
            .unwrap();
        match env.resize(zerodb::MAX_MAP_SIZE + 4096) {
            Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
            other => panic!(
                "lock_file {lock_file}, write_map {write_map}: expected Io(InvalidInput), got {other:?}"
            ),
        }
        assert_eq!(env.info().map_size, MIB as u64);
        assert!(env.real_disk_size().unwrap() <= 2 * MIB as u64);
    }
}

// --- Lock-file naming and the file guards (ADR-0023 amendment 16) ---

#[test]
fn lock_file_name_is_derived_from_the_data_file_name() {
    assert_eq!(
        zerodb::derived_lock_file_name("zerodb.dat".as_ref()),
        "zerodb.lock"
    );
    assert_eq!(
        zerodb::derived_lock_file_name("data.mdb".as_ref()),
        "lock.mdb"
    );
    assert_eq!(
        zerodb::derived_lock_file_name("index.db".as_ref()),
        "index.db-lock"
    );
}

/// The options report the derived name until an explicit one is set.
#[test]
fn get_lock_file_name_reports_the_derived_name_until_overridden() {
    let mut o = opts();
    assert_eq!(o.get_lock_file_name(), LOCK_FILE_NAME);
    o.data_file_name(zerodb::HEED_DATA_FILE_NAME);
    assert_eq!(o.get_lock_file_name(), "lock.mdb");
    o.data_file_name("index.db");
    assert_eq!(o.get_lock_file_name(), "index.db-lock");
    o.lock_file_name("mine.lock");
    assert_eq!(o.get_lock_file_name(), "mine.lock");
}

/// Two native envs with different data names in one directory get different
/// lock files. The envs are opened one after the other: this process's env
/// registry is keyed by directory (SPEC 04 TXN-51), so one process cannot
/// hold both at once; two processes can, and then each needs its own lock
/// file.
#[test]
fn two_native_envs_with_different_data_names_get_different_lock_files() {
    let dir = TempDir::new();
    let a = opts()
        .data_file_name("a.db")
        .open(dir.path())
        .expect("open a");
    drop(a);
    let b = opts()
        .data_file_name("b.db")
        .open(dir.path())
        .expect("open b");
    drop(b);
    assert!(dir.path().join("a.db-lock").exists());
    assert!(dir.path().join("b.db-lock").exists());
    assert!(!dir.path().join(LOCK_FILE_NAME).exists());
}

#[test]
fn lock_file_that_is_the_data_file_is_refused_without_truncating() {
    let dir = TempDir::new();
    {
        let env = opts().data_file_name("data.mdb").open(dir.path()).unwrap();
        let mut w = env.write_txn().unwrap();
        env.main_database().put(&mut w, b"k", b"v").unwrap();
        w.commit().unwrap();
    }
    let len = std::fs::metadata(dir.path().join("data.mdb"))
        .unwrap()
        .len();
    std::fs::hard_link(dir.path().join("data.mdb"), dir.path().join("alias")).unwrap();
    let e = opts()
        .data_file_name("data.mdb")
        .lock_file_name("alias")
        .open(dir.path())
        .unwrap_err();
    match e {
        Error::Io(io) => assert_eq!(io.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected Io(InvalidInput), got {other:?}"),
    }
    assert_eq!(
        std::fs::metadata(dir.path().join("data.mdb"))
            .unwrap()
            .len(),
        len
    );
    assert!(
        dir.path().join("alias").exists(),
        "the alias names the data file and must not be unlinked"
    );
}

/// Env 2's derived lock name is env 1's data file (another file, not a link):
/// the foreign-file guard refuses it and the data survives.
#[test]
fn lock_file_named_like_another_envs_data_file_is_refused() {
    let dir = TempDir::new();
    {
        let env = opts()
            .data_file_name("x.db-lock")
            .open(dir.path())
            .expect("env 1");
        let mut w = env.write_txn().unwrap();
        env.main_database().put(&mut w, b"keep", b"me").unwrap();
        w.commit().unwrap();
    }
    let data = dir.path().join("x.db-lock");
    let before = std::fs::read(&data).unwrap();
    match opts().data_file_name("x.db").open(dir.path()) {
        Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected Io(InvalidInput), got {:?}", other.err()),
    }
    assert_eq!(
        std::fs::read(&data).unwrap(),
        before,
        "env 1's data changed"
    );
    let env = opts()
        .data_file_name("x.db-lock")
        .open(dir.path())
        .expect("env 1 reopens");
    let r = env.read_txn().unwrap();
    assert_eq!(
        env.main_database().get(&r, b"keep").unwrap(),
        Some(&b"me"[..])
    );
}

/// Whether `dir` sits on a case-insensitive filesystem (default APFS).
fn case_insensitive(dir: &Path) -> bool {
    std::fs::write(dir.join("case-probe"), b"").unwrap();
    let insensitive = dir.join("CASE-PROBE").exists();
    std::fs::remove_file(dir.join("case-probe")).unwrap();
    insensitive
}

/// On a case-insensitive filesystem a lock name that differs from the data
/// name only by case names the data file. The open is refused and the data
/// is intact. Skipped on a case-sensitive filesystem.
#[test]
fn lock_name_differing_only_by_case_is_refused_on_a_case_insensitive_fs() {
    let dir = TempDir::new();
    if !case_insensitive(dir.path()) {
        eprintln!("SKIPPED: case-sensitive filesystem");
        return;
    }
    {
        let env = opts().open(dir.path()).unwrap();
        let mut w = env.write_txn().unwrap();
        env.main_database().put(&mut w, b"keep", b"me").unwrap();
        w.commit().unwrap();
    }
    let data = dir.path().join(zerodb::DATA_FILE_NAME);
    let before = std::fs::read(&data).unwrap();
    match opts().lock_file_name("ZERODB.DAT").open(dir.path()) {
        Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected Io(InvalidInput), got {:?}", other.err()),
    }
    assert_eq!(std::fs::read(&data).unwrap(), before, "the data changed");
}

/// The same with no data file yet: the lock open creates the file, finds it
/// is the data file's name, and removes what it created, leaving no file.
#[test]
fn lock_name_differing_only_by_case_creates_nothing_on_a_case_insensitive_fs() {
    let dir = TempDir::new();
    if !case_insensitive(dir.path()) {
        eprintln!("SKIPPED: case-sensitive filesystem");
        return;
    }
    match opts().lock_file_name("ZERODB.DAT").open(dir.path()) {
        Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected Io(InvalidInput), got {:?}", other.err()),
    }
    assert!(!dir.path().join(zerodb::DATA_FILE_NAME).exists());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}
