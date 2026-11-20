//! A commit whose page writes fail, on LMDB and on ZeroDB (SPEC 06 REC-23).
//! CONTRIBUTING.md rule 1: observed, not guessed.
//!
//! The one write failure both engines can be given for real, portably and
//! without privileges, is a file-size limit: under `RLIMIT_FSIZE` (with
//! `SIGXFSZ` ignored) a write that would extend the data file past the limit
//! fails with `EFBIG`. A commit that needs new pages past the end of the file
//! therefore fails in its page writes (commit step C2), like a commit on a
//! full device. The limit is process-wide, so each engine runs in a child
//! process (the test binary re-run as `child_entry`), through the heed API on
//! both sides (heed over the LMDB fork, `heed-zerodb` over ZeroDB).
//!
//! The child records, as one line: the error the failed commit returned,
//! what a read transaction then sees, whether a write transaction can begin
//! while the limit holds, whether the same commit succeeds once the limit is
//! lifted, and what a reopen reads. The parent requires both engines to
//! record the same line.
//!
//! Failures of `fsync` and of the meta write cannot be produced this way
//! (the meta slots lie inside the file); for those, LMDB's handling was read
//! in the fork's source and is compared in ADR-0047.

use std::path::{Path, PathBuf};
use std::time::Duration;

use zerodb_oracle::child::{self, ChildGuard};
use zerodb_oracle::tempdir::TempDir;

const ROLE: &str = "ZERODB_IOERR_ROLE";
const DIR: &str = "ZERODB_IOERR_DIR";
const MAP: usize = 64 << 20;
const SEED_KEYS: u32 = 200;
const BIG_KEYS: u32 = 2000;

fn spawn(role: &str, dir: &Path) -> ChildGuard {
    child::spawn_self(
        module_path!(),
        &[(ROLE, role.as_ref()), (DIR, dir.as_os_str())],
    )
}

/// Cap this process's file size at `limit` bytes (soft limit only, so it can
/// be lifted again) and keep `SIGXFSZ` from killing it.
fn limit_file_size(limit: u64) -> libc::rlim_t {
    // SAFETY: plain libc calls on this child process's own resource limits
    // and signal disposition; `rl` is a valid, initialized `rlimit` the
    // calls read and write. The child is single-threaded apart from the
    // test harness, which writes no files.
    unsafe {
        libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
        let mut rl = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        assert_eq!(libc::getrlimit(libc::RLIMIT_FSIZE, &mut rl), 0);
        let old = rl.rlim_cur;
        rl.rlim_cur = limit as libc::rlim_t;
        assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &rl), 0);
        old
    }
}

/// Restore the soft file-size limit `old`.
fn restore_file_size(old: libc::rlim_t) {
    // SAFETY: as in `limit_file_size`; raising the soft limit back to its
    // old value never exceeds the hard limit, which was left unchanged.
    unsafe {
        let mut rl = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        assert_eq!(libc::getrlimit(libc::RLIMIT_FSIZE, &mut rl), 0);
        rl.rlim_cur = old;
        assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &rl), 0);
    }
}

/// The data file of the env in `dir` (LMDB's `data.mdb`, or ZeroDB's).
fn data_file(dir: &Path) -> PathBuf {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            let name = p.file_name().unwrap().to_string_lossy().into_owned();
            !name.contains("lock")
        })
        .max_by_key(|p| std::fs::metadata(p).unwrap().len())
        .expect("a data file")
}

/// The scenario, written once against the heed API and instantiated for
/// both engines.
macro_rules! scenario {
    ($heed:ident, $dir:expr) => {{
        use $heed::types::Bytes;
        use $heed::{Database, Env, EnvOpenOptions, Error};

        fn errno(e: &Error) -> String {
            match e {
                Error::Io(io) => format!("Io({:?})", io.raw_os_error()),
                other => format!("{other:?}"),
            }
        }

        fn open(dir: &Path) -> Env {
            let mut o = EnvOpenOptions::new();
            o.map_size(MAP).max_dbs(4);
            // SAFETY: the directory is a fresh temp dir only this test
            // writes, and each env is opened once at a time.
            unsafe { o.open(dir) }.unwrap()
        }

        fn fill(env: &Env, db: Database<Bytes, Bytes>, n: u32, len: usize) -> Result<(), Error> {
            let mut w = env.write_txn()?;
            for i in 0..n {
                db.put(&mut w, format!("k{i:06}").as_bytes(), &vec![7u8; len])?;
            }
            w.commit()
        }

        fn count(env: &Env, db: Database<Bytes, Bytes>) -> u64 {
            let r = env.read_txn().unwrap();
            db.len(&r).unwrap()
        }

        let dir: &Path = $dir;
        let env = open(dir);
        let mut w = env.write_txn().unwrap();
        let db: Database<Bytes, Bytes> = env.create_database(&mut w, Some("d")).unwrap();
        w.commit().unwrap();
        fill(&env, db, SEED_KEYS, 40).unwrap();
        env.force_sync().unwrap();

        let len = std::fs::metadata(data_file(dir)).unwrap().len();
        let old = limit_file_size(len);
        let failed = fill(&env, db, BIG_KEYS, 500);
        let after_failure = count(&env, db);
        // Can a write transaction begin and commit while the limit holds?
        // A small update rewrites pages inside the file or fails the same
        // way; either is recorded.
        let begin = match env.write_txn() {
            Ok(w) => {
                w.abort();
                "begins".to_string()
            }
            Err(e) => errno(&e),
        };
        restore_file_size(old);
        let retried = fill(&env, db, BIG_KEYS, 500);
        let after_retry = count(&env, db);
        drop(env);
        let env = open(dir);
        let r = env.read_txn().unwrap();
        let db: Database<Bytes, Bytes> = env.open_database(&r, Some("d")).unwrap().unwrap();
        let reopened = db.len(&r).unwrap();
        format!(
            "commit={} reads_after_failure={after_failure} write_txn_under_limit={begin} \
             retry={} reads_after_retry={after_retry} reopen={reopened}",
            failed.as_ref().map_or_else(errno, |()| "Ok".into()),
            retried.as_ref().map_or_else(errno, |()| "Ok".into()),
        )
    }};
}

#[test]
fn child_entry() {
    let (Ok(role), Ok(dir)) = (std::env::var(ROLE), std::env::var(DIR)) else {
        return;
    };
    let dir = PathBuf::from(dir);
    let env_dir = dir.join("env");
    std::fs::create_dir_all(&env_dir).unwrap();
    let line = match role.as_str() {
        "lmdb" => scenario!(heed, &env_dir),
        "zerodb" => scenario!(heed_zerodb, &env_dir),
        other => panic!("unknown role {other}"),
    };
    std::fs::write(dir.join("result"), line).unwrap();
}

fn observe(role: &str) -> String {
    let dir = TempDir::new().unwrap();
    let mut c = spawn(role, dir.path());
    child::wait_success(&mut c, role, Duration::from_secs(120));
    std::fs::read_to_string(dir.path().join("result")).unwrap()
}

#[test]
fn failed_page_writes_in_a_commit_match_lmdb() {
    let lmdb = observe("lmdb");
    let zerodb = observe("zerodb");
    println!("lmdb:   {lmdb}\nzerodb: {zerodb}");
    // The observation itself, pinned: the commit fails with EFBIG and
    // changes nothing, the env stays writable, and the same commit succeeds
    // once the file may grow.
    let efbig = libc::EFBIG;
    let want = format!(
        "commit=Io(Some({efbig})) reads_after_failure={SEED_KEYS} write_txn_under_limit=begins \
         retry=Ok reads_after_retry={} reopen={}",
        BIG_KEYS.max(SEED_KEYS),
        BIG_KEYS.max(SEED_KEYS),
    );
    assert_eq!(lmdb, want, "LMDB's observed behavior changed");
    assert_eq!(zerodb, lmdb, "ZeroDB differs from LMDB");
}
