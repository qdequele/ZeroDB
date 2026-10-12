//! The tools against a live environment (ADR-0023 amendment 18): `stat`,
//! `dump` and `check` attach to an environment another process has open,
//! through its lock file; `load` and `check`'s raw-file fallback refuse one.
//!
//! The test binary re-runs itself as the other process (see `child_entry`).
#![cfg(unix)]

use std::collections::BTreeSet;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use zerodb::{Env, EnvOpenOptions, DATA_FILE_NAME, HEED_DATA_FILE_NAME};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_dir(tag: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let p = std::env::temp_dir().join(format!(
        "zerodb-lock-{tag}-{}-{nanos}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// The child's role.
const ROLE: &str = "ZERODB_TOOLS_LIVE_ROLE";
/// The environment directory the child opens.
const DIR: &str = "ZERODB_TOOLS_LIVE_DIR";
/// A directory outside the environment for the `ready` and `stop` markers,
/// so the environment directory holds only the environment's own files.
const CTL: &str = "ZERODB_TOOLS_LIVE_CTL";
/// The `max_readers` the child opens with. Not the default, so a tool that
/// joins must adopt the running environment's value.
const CHILD_MAX_READERS: u32 = 200;

/// A spawned child that is killed and reaped when dropped, so a failing test
/// never leaves it running.
struct ChildGuard(Child);

impl Deref for ChildGuard {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}

impl DerefMut for ChildGuard {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_child(role: &str, dir: &Path, ctl: &Path) -> ChildGuard {
    let c = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_entry", "--nocapture", "--test-threads=1"])
        .env(ROLE, role)
        .env(DIR, dir)
        .env(CTL, ctl)
        .spawn()
        .expect("spawn child");
    ChildGuard(c)
}

fn wait_for(path: &Path, deadline: Duration) {
    let end = Instant::now() + deadline;
    while !path.exists() {
        assert!(
            Instant::now() < end,
            "timed out waiting for {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Tell the child to exit and wait until it has.
fn stop(mut child: ChildGuard, ctl: &Path) {
    std::fs::write(ctl.join("stop"), b"").unwrap();
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(st) = child.try_wait().unwrap() {
            assert!(st.success(), "child failed: {st}");
            return;
        }
        assert!(Instant::now() < end, "timed out waiting for the child");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// The child process. Does nothing when run as an ordinary test.
#[test]
fn child_entry() {
    let (Ok(role), Ok(dir), Ok(ctl)) =
        (std::env::var(ROLE), std::env::var(DIR), std::env::var(CTL))
    else {
        return;
    };
    let (dir, ctl) = (PathBuf::from(dir), PathBuf::from(ctl));
    match role.as_str() {
        "hold_adapter" => hold_adapter(&dir, &ctl),
        "hold_native" => hold_native(&dir, &ctl),
        other => panic!("unknown role {other}"),
    }
}

/// Wait for the parent's `stop` marker (bounded, so an orphaned child exits).
fn wait_stop(ctl: &Path) {
    wait_for(&ctl.join("stop"), Duration::from_secs(120));
}

/// Open the environment through the heed adapter (`data.mdb`, `lock.mdb`), as
/// Meilisearch does; commit `live`; then hold a read txn and a write txn
/// whose `pending` is never committed.
fn hold_adapter(dir: &Path, ctl: &Path) {
    use heed_zerodb::types::Bytes;
    let mut opts = heed_zerodb::EnvOpenOptions::new();
    // The adapter opens with a lock file by default (ADR-0023 amendment 17).
    opts.map_size(16 << 20).max_readers(CHILD_MAX_READERS);
    // SAFETY: heed signature parity only; the adapter's open performs no
    // unsafe operation.
    let env = unsafe { opts.open(dir) }.expect("open adapter env");
    let mut w = env.write_txn().unwrap();
    let db: heed_zerodb::Database<Bytes, Bytes> = env.create_database(&mut w, None).unwrap();
    db.put(&mut w, b"live", b"committed").unwrap();
    w.commit().unwrap();

    let r = env.read_txn().unwrap();
    assert_eq!(db.get(&r, b"live").unwrap(), Some(&b"committed"[..]));
    let mut w = env.write_txn().unwrap();
    db.put(&mut w, b"pending", b"uncommitted").unwrap();
    std::fs::write(ctl.join("ready"), b"").unwrap();
    wait_stop(ctl);
    drop(w);
    drop(r);
}

/// Open a native environment (`zerodb.dat`, `zerodb.lock`), commit `live`,
/// and hold the environment open.
fn hold_native(dir: &Path, ctl: &Path) {
    let env = EnvOpenOptions::new()
        .map_size(16 << 20)
        .lock_file(true)
        .open(dir)
        .expect("open native env");
    let mut w = env.write_txn().unwrap();
    env.main_database().put(&mut w, b"live", b"1").unwrap();
    w.commit().unwrap();
    std::fs::write(ctl.join("ready"), b"").unwrap();
    wait_stop(ctl);
}

/// Hex-encode as the dump format renders keys.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn entries(dir: &Path) -> BTreeSet<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect()
}

/// `stat`, `dump` and `check` attach to an environment that another process
/// holds open with a read txn and an uncommitted write txn: they read its
/// committed data, not the pending write, and create nothing beside the
/// environment's own two files.
///
/// This replaces `tools_refuse_a_locked_env`, which asserted the opposite
/// (refusal while a `flock` on the data file was held). ADR-0023 amendment
/// 18 retires that `flock` and makes the tools attach as readers.
#[test]
fn tools_attach_to_a_live_env_in_another_process() {
    let dir = tmp_dir("live");
    let ctl = tmp_dir("live-ctl");
    let child = spawn_child("hold_adapter", &dir, &ctl);
    wait_for(&ctl.join("ready"), Duration::from_secs(30));

    let stat = zerodb_tools::commands::cmd_stat(&dir).expect("stat attaches");
    assert!(stat.contains(HEED_DATA_FILE_NAME), "stat output: {stat}");
    let dump = zerodb_tools::commands::cmd_dump(&dir).expect("dump attaches");
    assert!(dump.contains(&hex(b"live")), "dump output: {dump}");
    assert!(
        !dump.contains(&hex(b"pending")),
        "dump shows an uncommitted write: {dump}"
    );
    let (report, clean) = zerodb_tools::commands::cmd_check(&dir).expect("check attaches");
    assert!(clean, "check report: {report}");
    // The attached walk's header, not the raw-file fallback's.
    assert!(report.contains("txnid="), "check report: {report}");

    // The tools' own open joins the child's environment through the lock
    // file: only a lock-file joiner did not open it exclusively.
    {
        let env = zerodb_tools::common::open_ro(&dir, HEED_DATA_FILE_NAME).expect("attach");
        assert!(
            !env.opened_exclusively(),
            "the tools' open did not join the live env through its lock file"
        );
        assert_eq!(env.info().max_readers, CHILD_MAX_READERS);
        let r = zerodb_tools::common::read_txn(&env).unwrap();
        assert_eq!(
            env.main_database().get(&r, b"live").unwrap(),
            Some(&b"committed"[..])
        );
    }

    let expected: BTreeSet<String> = [HEED_DATA_FILE_NAME, "lock.mdb"]
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
    assert_eq!(entries(&dir), expected);
    stop(child, &ctl);
}

/// A tool on a missing env fails cleanly (not a panic). The message names
/// **both** candidate data-file names (ADR-0010), so an operator who pointed
/// the tool at the wrong directory can see what it looked for.
#[test]
fn tools_refuse_a_missing_env() {
    let dir = tmp_dir("missing");
    let err = zerodb_tools::commands::cmd_stat(&dir).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("no zerodb env"), "err: {err}");
    assert!(msg.contains(zerodb::DATA_FILE_NAME), "err: {err}");
    assert!(msg.contains(zerodb::HEED_DATA_FILE_NAME), "err: {err}");
}

/// `load` refuses a destination that another process has open, and leaves
/// that environment as it was.
#[test]
fn load_refuses_a_destination_another_process_has_open() {
    let dir = tmp_dir("load-live");
    let ctl = tmp_dir("load-live-ctl");
    let child = spawn_child("hold_native", &dir, &ctl);
    wait_for(&ctl.join("ready"), Duration::from_secs(30));

    let dump = tmp_dir("load-live-src").join("dump.txt");
    std::fs::write(
        &dump,
        format!(
            "VERSION=3\nformat=bytevalue\ntype=btree\nHEADER=END\n {}\n {}\nDATA=END\n",
            hex(b"loaded"),
            hex(b"x")
        ),
    )
    .unwrap();
    let err = zerodb_tools::commands::cmd_load(&dump, &dir, 4096, 1 << 20)
        .unwrap_err()
        .to_string();
    assert!(err.contains("refusing"), "message: {err}");
    stop(child, &ctl);

    let env: Env = EnvOpenOptions::new().open(&dir).unwrap();
    let r = env.read_txn().unwrap();
    let main = env.main_database();
    assert_eq!(main.get(&r, b"live").unwrap(), Some(&b"1"[..]));
    assert_eq!(main.get(&r, b"loaded").unwrap(), None);
}

/// Overwrite the meta magic of both meta slots, so no open accepts the file.
fn corrupt_both_metas(data: &Path) {
    use std::os::unix::fs::FileExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(data)
        .unwrap();
    let psize = u64::from(
        zerodb_io::probe_page_size(&file)
            .unwrap()
            .expect("a readable page size"),
    );
    for slot in 0..2u64 {
        file.write_all_at(b"XXXX", slot * psize + 32).unwrap();
    }
    file.sync_all().unwrap();
}

/// When the environment does not open, `check` falls back to walking the raw
/// file only if no other process has it open: here one does, so it refuses
/// rather than walking a file that process may be writing.
#[test]
fn check_raw_fallback_refuses_while_the_env_is_live() {
    let dir = tmp_dir("check-live");
    let ctl = tmp_dir("check-live-ctl");
    let child = spawn_child("hold_adapter", &dir, &ctl);
    wait_for(&ctl.join("ready"), Duration::from_secs(30));

    corrupt_both_metas(&dir.join(HEED_DATA_FILE_NAME));
    let err = zerodb_tools::commands::cmd_check(&dir)
        .unwrap_err()
        .to_string();
    assert!(err.contains("in use"), "message: {err}");
    stop(child, &ctl);
}

/// With no other process, `check` on a file that does not open walks the raw
/// file and reports the violations.
#[test]
fn check_raw_fallback_reports_a_closed_corrupt_env() {
    let dir = tmp_dir("check-closed");
    {
        let env: Env = EnvOpenOptions::new().map_size(1 << 20).open(&dir).unwrap();
        let mut w = env.write_txn().unwrap();
        env.main_database().put(&mut w, b"k", b"v").unwrap();
        w.commit().unwrap();
    }
    corrupt_both_metas(&dir.join(DATA_FILE_NAME));
    let (report, clean) = zerodb_tools::commands::cmd_check(&dir).expect("raw walk runs");
    assert!(!clean, "check report: {report}");
    assert!(report.contains("INV-"), "check report: {report}");
}
