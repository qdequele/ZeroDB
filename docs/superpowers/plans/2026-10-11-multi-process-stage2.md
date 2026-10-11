# Multi-process access — Stage 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reclaim the reader slots of dead processes. Do it the way LMDB does, at the points where LMDB does it, and prove the shared reader protocol with loom, two-process tests, a crash harness and a two-process stress run. This must land before stages 1–2 merge (PR #93).

**Architecture:** Reaping stays in `zerodb-core::readers`. It is generic over a liveness predicate, so the same code runs in the shipped build, under miri and under loom. `SharedRegion` gains `is_alive(pid)`, which `zerodb-io::lockfile` answers with an `F_GETLK` probe on the process's liveness byte. A reaper adopts a dead process's slot with one CAS on its PID word, then runs the owner's release sequence. The release sequence now unpins before it disowns, so a death at any point leaves a slot that is either reapable or pins nothing. The lock file also closes the stale-joiner window (re-election after the shared lock is granted, with the magic stored last) and moves the slot table to offset 256.

**Tech Stack:** Rust 2021 (MSRV 1.98), `libc` (`fcntl`), `loom` 0.7 under `cfg(loom)`, `lmdb-master-sys` FFI in `zerodb-oracle` only. No new dependencies.

**Spec:** `docs/adr/0023-multi-process.md`: §3, staging item 2, the Tests section, and the stage-1 amendments, plus the stage-2 amendments that Task 1 adds. Also the carry-forward list in this plan's Global Constraints.

## Global Constraints

- Branch `qdequele/adr-0023-multi-process`, on top of `31ee66d`. Stages 1 and 2 merge together as PR #93 (ADR-0023, resolved question 3). Do not push: the user pushes.
- `unsafe` only in `zerodb-io::lockfile` (the sanctioned shared-memory category) and in `zerodb-oracle` (FFI). `zerodb-core` stays `#![forbid(unsafe_code)]`. Every `unsafe` block carries a `// SAFETY:` comment.
- Every atomic uses an explicit `Ordering` with a comment justifying it. Assume a weak memory model (ARM). StoreLoad edges are SeqCst, with a written proof (ADR-0006 R1).
- No `#[repr(C)]` casts beyond the existing `ReaderSlot` slice view. Lock-file header fields are accessed by explicit byte offset.
- The lock file stays opt-in (`lock_file(true)`, default off). The default single-process path must not change behavior or cost. Stage 3 flips the default.
- LMDB behavior is observed, not guessed (AGENTS.md rule 1). Reap points and the `clear_stale_readers` count come from Task 2's observations. `ReadersFull` does whatever Task 2 observes LMDB doing.
- Never delete, weaken, `#[ignore]` or loosen a test. No new dependency. No `TODO` in code.
- Comments describe what the code does for an outside reader. Spec rule IDs and ADR numbers are fine.
- Commit messages carry **no** `Co-Authored-By` line (the user's global instruction).
- Before calling a task done, run:
  - `cargo fmt --all -- --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - on macOS also `cargo clippy --workspace --all-targets --target x86_64-unknown-linux-gnu --exclude zerodb-oracle -- -D warnings`;
  - plus the task's own tests.
- Tasks touching `zerodb-core::readers` also run:
  - `cargo +nightly miri test -p zerodb-core readers`;
  - `just loom` (with an isolated `CARGO_HOME` in the scratchpad if the shared registry cache misbehaves).
- Carry-forward items from stage 1 that this stage closes:
  - **Stale-joiner election window**, including a failed initializer leaving `T` unseeded: Task 5.
  - **Blind spot for a slot carrying this process's own reused PID:** Tasks 3 and 6.
  - **`writer_lock_releases_from_another_thread` is vacuous on macOS:** Task 7 adds a two-process version.
  - **Slots off the writer word's 128-byte line (offset 256):** Task 5.
  - **Linux aarch64 (Graviton) run of the futex path:** Task 11.

## Review Focus

1. **A reaper races a live process that re-claims the same slot.** P released the slot, then died; Q claimed it, while a reaper still holds P's PID from an earlier read. The reaper must never free Q's slot. Pinned in Task 4 (`loom_l7_reaper_vs_release_and_reclaim`).
2. **Two processes reap the same dead slot at once.** It is freed exactly once, and the two counts sum to the number of dead slots. Pinned in Task 4 (`loom_l6_two_reapers_one_dead_slot`) and Task 3 (`reap_counts_each_dead_slot_once`).
3. **A process joins with the PID of a dead process that left slots behind.** Its own-PID stale slots are freed at open. A probe cannot see them, because this process holds that PID's liveness lock. Slots it claims later are never reaped by itself. Pinned in Task 3 (`own_pid_stale_slots_are_freed`) and Task 6 (`joiner_frees_slots_left_under_its_own_pid`).
4. **Every other process exits while a joiner waits on the election lock**, including an initializer that died before finishing. The joiner initializes (fresh `T`, wiped slots) instead of adopting a half-built file. Pinned in Task 5 (`joiner_whose_initializer_died_reinitializes`, `three_way_race_after_initializer_death`).
5. **A writer is SIGKILLed between its meta write and its `T` store** (hooks H3/H4) while a reader process stays alive. The next writer builds on `T`, never on the orphaned meta, and the reader never sees the dead commit. Pinned in Task 9 (the `mp` crash mechanism).

---

### Task 1: Record the stage-2 refinements in ADR-0023

Planning stage 2 turned up refinements to §3 and §1. They go into the ADR before any code.

**Files:**
- Modify: `docs/adr/0023-multi-process.md`. Add a new section after `## Amendments (stage-1 planning, 2026-10-09)` and before `## Consequences`.

- [ ] **Step 1: Add the section**

```markdown
## Amendments (stage-2 planning, 2026-10-11)

7. **Release unpins before it disowns.** The owner's release is now three
   stores: `txnid = CLAIMED`, then `pid = 0`, then `txnid = FREE`. With the
   §3 order (`pid = 0`, then `FREE`), a process dying between the two stores
   left a *pinned* txnid with `pid == 0`. No reaper could attribute it, and it
   held the GC gate back until the next first-opener wipe. Now a death at any
   point leaves one of two states. `CLAIMED` with the dead PID is reapable.
   `CLAIMED` with `pid == 0` is the known leak, which pins nothing.
   The local (single-process) table keeps its one-store release.
8. **A reaper adopts, then releases.** Reaping a dead process P's slot is
   `CAS pid P → reaper_pid` (the single linearization point of §3), then the
   owner's release sequence (amendment 7). A reaper that dies mid-reap leaves
   a slot carrying its own PID, which the next reaper frees.
9. **Own-PID stale slots are freed at open.** A slot carrying this process's
   PID before this process has claimed anything was left by a dead process
   with the same PID. The liveness probe cannot see that process is dead,
   because this process now holds that PID's liveness lock. A joining process
   frees every such slot during open, before its first claim. It uses
   `CAS pid P → 0` (which excludes a concurrent reaper's adoption), then
   `txnid = FREE`. Dying between those two stores leaks one pinned slot until
   the next wipe; the window is a single store, accepted and documented.
10. **Joiner re-election and magic-last.** The initializer stores the magic in
    `finish_open`, after `T` is seeded, so a joiner that sees the magic also
    sees a seeded `T`. After a joiner is granted the shared election lock, it
    tries the exclusive lock once more. POSIX converts its shared lock in
    place, and the upgrade succeeds only when no other process holds the
    byte, which means every process exited while it waited. If the upgrade
    succeeds, the joiner initializes. A joiner that loses the upgrade and
    finds no magic drops its lock and retries the election, a bounded number
    of times. This happens when an initializer died before finishing while
    another joiner also holds the lock.
11. **Slot table at offset 256, lock-format version 2.** Slot 0 moves off the
    writer word's 128-byte line pair. The version bump makes a stage-1 lock
    file of a still-running process fail validation instead of being misread.
12. **Reap points, as observed in the oracle.** Task 2 of the stage-2 plan
    fills the table below from
    `crates/zerodb-oracle/tests/lmdb_reaping_observation.rs`. ZeroDB reaps at
    exactly these points:

    | Event after a reader process died | LMDB reaps? |
    |---|---|
    | (filled by stage-2 Task 2) | |

13. **`reader_list` reports the owner PID** (`ReaderEntry::pid`). It is
    `None` on the default path, and for a slot being claimed or released.
```

- [ ] **Step 2: Commit**

```bash
git add docs/adr/0023-multi-process.md
git commit -m "ADR-0023: stage-2 planning amendments (release order, adopt-then-release reaping, own-PID slots, re-election, slot offset)"
```

---

### Task 2: Observe where the LMDB fork reaps dead readers

AGENTS.md rule 1: we learn the reap points from the fork, not from memory of `mdb.c`. This task writes an FFI observation test, runs it on macOS and on Linux (the bench server), pins what it sees, and fills amendment 12's table.

**Files:**
- Create: `crates/zerodb-oracle/tests/lmdb_reaping_observation.rs`
- Modify: `docs/adr/0023-multi-process.md` (amendment 12's table)

**Interfaces:**
- Produces: the observation constants `OBS_*`, read by Task 6's decision table and Task 8's differential test:
  - `OBS_READ_BEGIN_REAPS`, `OBS_WRITE_COMMIT_REAPS`, `OBS_JOINER_OPEN_REAPS`, `OBS_FULL_TABLE_REAPS`: `bool`.
  - `OBS_CHECK_COUNTS_SLOTS`: `bool`. `true` means `mdb_reader_check` counts slots; `false` means it counts processes.
  - `OBS_WRITER_TAKEOVER_REAPS`: `bool`, Linux only.

- [ ] **Step 1: Write the test file**

```rust
//! Where the Meilisearch LMDB fork reclaims the reader slots of dead
//! processes (ADR-0023 §3 "When", amendment 12). AGENTS.md rule 1: observed,
//! not guessed. Drives the vendored fork directly through `lmdb-master-sys`
//! (heed exposes no `mdb_reader_list`); the test binary re-runs itself as the
//! reader/writer process that gets SIGKILLed.
//!
//! Each scenario: a child opens the env, begins read txns (or a write txn),
//! signals, and is SIGKILLed; the parent then performs one action and checks
//! whether the dead child's PID is still listed by `mdb_reader_list`. The
//! `OBS_*` constants pin what the fork does; a diff means the fork changed or
//! the pin was wrong — never edit a pin to make ZeroDB pass.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::ptr;
use std::time::{Duration, Instant};

use lmdb_master_sys as ffi;
use zerodb_oracle::tempdir::TempDir;

const MDB_NOTLS: c_uint = 0x0020_0000;
const MDB_RDONLY: c_uint = 0x0002_0000;
const ROLE: &str = "ZERODB_REAP_OBS_ROLE";
const DIR: &str = "ZERODB_REAP_OBS_DIR";

// Pinned observations (filled from the first run on macOS and Linux; see
// the ADR-0023 amendment 12 table).
const OBS_READ_BEGIN_REAPS: bool = false;
const OBS_WRITE_COMMIT_REAPS: bool = false;
const OBS_JOINER_OPEN_REAPS: bool = false;
const OBS_FULL_TABLE_REAPS: bool = false;
const OBS_CHECK_COUNTS_SLOTS: bool = true;
#[cfg(target_os = "linux")]
const OBS_WRITER_TAKEOVER_REAPS: bool = true;

struct Lmdb {
    env: *mut ffi::MDB_env,
}

impl Lmdb {
    fn open(dir: &Path, max_readers: u32) -> Lmdb {
        let path = CString::new(dir.to_str().unwrap()).unwrap();
        let mut env = ptr::null_mut();
        // SAFETY: plain C API calls on a fresh env handle owned by this
        // struct; `path` outlives `mdb_env_open`.
        unsafe {
            assert_eq!(ffi::mdb_env_create(&mut env), 0);
            assert_eq!(ffi::mdb_env_set_maxreaders(env, max_readers), 0);
            assert_eq!(ffi::mdb_env_set_mapsize(env, 1 << 20), 0);
            assert_eq!(ffi::mdb_env_open(env, path.as_ptr(), MDB_NOTLS, 0o644), 0);
        }
        Lmdb { env }
    }

    /// Begin a read txn; the raw return code on failure.
    fn try_read(&self) -> Result<*mut ffi::MDB_txn, c_int> {
        let mut txn = ptr::null_mut();
        // SAFETY: `self.env` is open; `txn` receives a handle we own.
        let rc = unsafe { ffi::mdb_txn_begin(self.env, ptr::null_mut(), MDB_RDONLY, &mut txn) };
        if rc == 0 { Ok(txn) } else { Err(rc) }
    }

    fn abort(txn: *mut ffi::MDB_txn) {
        // SAFETY: `txn` came from `mdb_txn_begin` and is ended exactly once.
        unsafe { ffi::mdb_txn_abort(txn) }
    }

    /// Begin a write txn, put one key, commit.
    fn commit_one(&self) {
        // SAFETY: standard begin/dbi_open/put/commit sequence on an open
        // env; every pointer is live for the duration of its call.
        unsafe {
            let mut txn = ptr::null_mut();
            assert_eq!(ffi::mdb_txn_begin(self.env, ptr::null_mut(), 0, &mut txn), 0);
            let mut dbi = 0;
            assert_eq!(ffi::mdb_dbi_open(txn, ptr::null(), 0, &mut dbi), 0);
            let mut k = ffi::MDB_val { mv_size: 1, mv_data: b"k".as_ptr() as *mut c_void };
            let mut v = ffi::MDB_val { mv_size: 1, mv_data: b"v".as_ptr() as *mut c_void };
            assert_eq!(ffi::mdb_put(txn, dbi, &mut k, &mut v, 0), 0);
            assert_eq!(ffi::mdb_txn_commit(txn), 0);
        }
    }

    /// PIDs listed by `mdb_reader_list` (one per occupied slot).
    fn reader_pids(&self) -> Vec<u32> {
        unsafe extern "C" fn collect(msg: *const c_char, ctx: *mut c_void) -> c_int {
            // SAFETY: `ctx` is the `&mut Vec<String>` passed below, live for
            // the call; `msg` is a NUL-terminated string from LMDB.
            let out = unsafe { &mut *(ctx as *mut Vec<String>) };
            out.push(unsafe { CStr::from_ptr(msg) }.to_string_lossy().into_owned());
            0
        }
        let mut lines: Vec<String> = Vec::new();
        // SAFETY: the callback only touches `lines`, which outlives the call.
        unsafe {
            ffi::mdb_reader_list(self.env, Some(collect), &mut lines as *mut _ as *mut c_void);
        }
        lines
            .iter()
            .flat_map(|l| l.lines())
            .filter_map(|l| l.split_whitespace().next()?.parse::<u32>().ok())
            .collect()
    }

    /// `mdb_reader_check`'s dead count.
    fn reader_check(&self) -> i32 {
        let mut dead: c_int = 0;
        // SAFETY: `dead` outlives the call.
        assert_eq!(unsafe { ffi::mdb_reader_check(self.env, &mut dead) }, 0);
        dead
    }
}

impl Drop for Lmdb {
    fn drop(&mut self) {
        // SAFETY: every txn of this env has ended; closed exactly once.
        unsafe { ffi::mdb_env_close(self.env) }
    }
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn(role: &str, dir: &Path) -> ChildGuard {
    ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "child_entry", "--nocapture", "--test-threads=1"])
            .env(ROLE, role)
            .env(DIR, dir)
            .spawn()
            .unwrap(),
    )
}

fn wait_for(path: &Path) {
    let end = Instant::now() + Duration::from_secs(30);
    while !path.exists() {
        assert!(Instant::now() < end, "timed out waiting for {}", path.display());
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Spawn a child that holds `n` read txns, wait until it holds them, SIGKILL
/// it. Returns its PID.
fn kill_reader_child(dir: &Path, n: u32) -> u32 {
    let mut c = spawn(&format!("hold_readers:{n}"), dir);
    wait_for(&dir.join("held"));
    let pid = c.0.id();
    c.0.kill().unwrap();
    c.0.wait().unwrap();
    std::fs::remove_file(dir.join("held")).unwrap();
    pid
}

#[test]
fn child_entry() {
    let (Ok(role), Ok(dir)) = (std::env::var(ROLE), std::env::var(DIR)) else { return };
    let dir = PathBuf::from(dir);
    let env = Lmdb::open(&dir, 4);
    if let Some(n) = role.strip_prefix("hold_readers:") {
        let txns: Vec<_> = (0..n.parse::<u32>().unwrap()).map(|_| env.try_read().unwrap()).collect();
        std::fs::write(dir.join("held"), b"").unwrap();
        std::thread::sleep(Duration::from_secs(600));
        drop(txns);
    } else if role == "hold_writer" {
        let mut txn = ptr::null_mut();
        // SAFETY: as `commit_one`; the txn is never ended (the process is killed).
        assert_eq!(unsafe { ffi::mdb_txn_begin(env.env, ptr::null_mut(), 0, &mut txn) }, 0);
        std::fs::write(dir.join("held"), b"").unwrap();
        std::thread::sleep(Duration::from_secs(600));
    } else if role == "open_close" {
        let t = env.try_read().unwrap();
        Lmdb::abort(t);
    } else {
        panic!("unknown role {role}");
    }
}

/// The parent keeps the env open throughout (no first-opener wipe), seeds
/// one commit, then lets `f` act after the child's death.
fn scenario(n_readers: u32, max_readers: u32, f: impl FnOnce(&Lmdb, u32) -> bool) -> bool {
    let dir = TempDir::new().unwrap();
    let env = Lmdb::open(dir.path(), max_readers);
    env.commit_one();
    let pid = kill_reader_child(dir.path(), n_readers);
    assert!(env.reader_pids().contains(&pid), "baseline: a dead reader stays listed");
    f(&env, pid)
}

#[test]
fn observe_read_begin() {
    let reaped = scenario(1, 4, |env, pid| {
        Lmdb::abort(env.try_read().unwrap());
        !env.reader_pids().contains(&pid)
    });
    println!("read txn begin reaps: {reaped}");
    assert_eq!(reaped, OBS_READ_BEGIN_REAPS);
}

#[test]
fn observe_write_commit() {
    let reaped = scenario(1, 4, |env, pid| {
        env.commit_one();
        !env.reader_pids().contains(&pid)
    });
    println!("write txn commit reaps: {reaped}");
    assert_eq!(reaped, OBS_WRITE_COMMIT_REAPS);
}

#[test]
fn observe_joiner_open() {
    let reaped = scenario(1, 4, |env, pid| {
        let path = PathBuf::from(env_path(env));
        let mut c = spawn("open_close", &path);
        assert!(c.0.wait().unwrap().success());
        !env.reader_pids().contains(&pid)
    });
    println!("joiner open reaps: {reaped}");
    assert_eq!(reaped, OBS_JOINER_OPEN_REAPS);
}

/// The directory an env was opened at (`mdb_env_get_path`).
fn env_path(env: &Lmdb) -> String {
    let mut p: *const c_char = ptr::null();
    // SAFETY: LMDB returns a pointer into the env's own storage, valid while
    // the env is open; copied out immediately.
    unsafe {
        assert_eq!(ffi::mdb_env_get_path(env.env, &mut p), 0);
        CStr::from_ptr(p).to_string_lossy().into_owned()
    }
}

#[test]
fn observe_full_table() {
    // Two slots: the dead child holds one, the parent holds the other, then
    // asks for a third reader.
    let reaped = scenario(1, 2, |env, _pid| {
        let live = env.try_read().unwrap();
        let next = env.try_read();
        let ok = next.is_ok();
        if let Ok(t) = next {
            Lmdb::abort(t);
        } else {
            assert_eq!(next.unwrap_err(), ffi::MDB_READERS_FULL);
        }
        Lmdb::abort(live);
        ok
    });
    println!("full table reaps and retries: {reaped}");
    assert_eq!(reaped, OBS_FULL_TABLE_REAPS);
}

#[test]
fn observe_reader_check_count() {
    let counted = scenario(2, 4, |env, pid| {
        let n = env.reader_check();
        assert!(!env.reader_pids().contains(&pid), "reader_check frees the dead slots");
        assert_eq!(env.reader_check(), 0, "a second check finds nothing");
        n == 2
    });
    println!("reader_check counts slots (2) rather than processes (1): {counted}");
    assert_eq!(counted, OBS_CHECK_COUNTS_SLOTS);
}

/// Linux only: on macOS the fork's writer mutex is not robust, so a dead
/// writer blocks the next one forever.
#[cfg(target_os = "linux")]
#[test]
fn observe_writer_takeover() {
    let dir = TempDir::new().unwrap();
    let env = Lmdb::open(dir.path(), 4);
    env.commit_one();
    let reader_pid = kill_reader_child(dir.path(), 1);
    let mut w = spawn("hold_writer", dir.path());
    wait_for(&dir.path().join("held"));
    w.0.kill().unwrap();
    w.0.wait().unwrap();
    env.commit_one(); // takes over the dead writer's mutex
    let reaped = !env.reader_pids().contains(&reader_pid);
    println!("writer takeover reaps: {reaped}");
    assert_eq!(reaped, OBS_WRITER_TAKEOVER_REAPS);
}

#[test]
fn first_opener_starts_with_an_empty_table() {
    let dir = TempDir::new().unwrap();
    {
        let env = Lmdb::open(dir.path(), 4);
        env.commit_one();
    }
    // The child is the only process; it dies holding a reader.
    let pid = kill_reader_child(dir.path(), 1);
    let env = Lmdb::open(dir.path(), 4);
    assert!(!env.reader_pids().contains(&pid), "the first opener wipes the table");
}
```

If `mdb_env_get_path`'s signature in `lmdb-master-sys 0.2.6` differs (`*mut *const c_char`), adapt the call; keep the behavior.

- [ ] **Step 2: Run on macOS and record**

Run: `cargo test -p zerodb-oracle --test lmdb_reaping_observation -- --nocapture --test-threads=1`.
For each `observe_*` test, read the printed value. Wherever a pinned `OBS_*` constant disagrees with the observation, set the constant to the observed value. These pins describe LMDB, and this first run is what establishes them. All tests must then pass.

- [ ] **Step 3: Run on Linux x86-64 (bench server)**

Sync the branch to the bench server (`root@62.210.92.143`, scripts in `_side_projects/benches-server`) and run the same command there. The Linux-only `observe_writer_takeover` gets its pin here. If macOS and Linux disagree on a shared observation, make that constant `cfg`-split per OS and say so in the ADR table.

- [ ] **Step 4: Fill ADR amendment 12's table**

Replace the placeholder row with one row per observation:
- read txn begin;
- write txn commit;
- another process opening and closing;
- `ReadersFull` (reap-and-retry or not);
- writer takeover (Linux; on macOS the fork blocks);
- first-opener wipe.

Add a line stating whether `mdb_reader_check` counts slots or processes.

- [ ] **Step 5: Commit**

```bash
git add crates/zerodb-oracle/tests/lmdb_reaping_observation.rs docs/adr/0023-multi-process.md
git commit -m "oracle: observe where the LMDB fork reaps dead readers (ADR-0023 amendment 12)"
```

**Controller note:** after this task, edit Task 6 Step 3's decision table to keep only the rows Task 2 observed as `true`, before dispatching Task 6.

---

### Task 3: Reaping primitives in the reader table

**Files:**
- Modify: `crates/zerodb-core/src/shared.rs`:
  - add `is_alive` to the trait;
  - give `HeapRegion` a dead-PID set.
- Modify: `crates/zerodb-core/src/readers.rs`:
  - add `claim_as` and `release_as`;
  - change the release order;
  - add `reap_dead`, `reap_own_stale` and `pin_against`;
  - make `list` return the PID.
- Modify: `crates/zerodb-core/src/env.rs`:
  - `pin_shared` uses `pin_against`;
  - `reader_list` maps the PID.
  
  Leave everything else for Task 6.
- Test: `mod tests` in `crates/zerodb-core/src/readers.rs`.

**Interfaces:**
- Produces:
  - `SharedRegion::is_alive(&self, pid: u32) -> std::io::Result<bool>`. Every implementor must provide it: `HeapRegion` here, `LockFile` in Task 5.
  - `ReaderTable::claim_as(&self, owner: Option<u32>) -> Option<u32>`.
  - `ReaderTable::release_as(&self, slot: u32, owned: bool)`.
  - `ReaderTable::reap_dead(&self, reaper: u32, is_alive: impl FnMut(u32) -> std::io::Result<bool>) -> std::io::Result<u32>`, which returns the number of slots freed.
  - `ReaderTable::reap_own_stale(&self, me: u32) -> u32`.
  - `ReaderTable::pin_against<S, E>(&self, slot: u32, t_word: &AtomicU64, limit: u32, resolve: impl FnMut(u64) -> Result<Option<S>, E>) -> Result<Option<S>, E>`.
  - `ReaderTable::list(&self) -> Vec<(u32, Option<u64>, Option<u32>)>`, as (slot, txnid, pid).
  - `ReaderTable::owner_pid(&self) -> Option<u32>`.
  - `HeapRegion::kill(&self, pid: u32)`, test only.

- [ ] **Step 1: Write the failing unit tests (append to `mod tests` in `readers.rs`)**

```rust
    fn shared_table(slots: u32, pid: u32) -> (Arc<crate::shared::HeapRegion>, ReaderTable) {
        let region = Arc::new(crate::shared::HeapRegion::new(slots, pid, true));
        let table = ReaderTable::new_shared(Arc::clone(&region) as Arc<dyn crate::shared::SharedRegion>);
        (region, table)
    }

    #[test]
    fn release_unpins_before_it_disowns() {
        // The owner's release order is CLAIMED, pid 0, FREE (ADR-0023
        // amendment 7); the end state is a free, unowned slot.
        let (region, t) = shared_table(1, 7);
        let s = t.claim().unwrap();
        t.store_pin(s, 5);
        t.release(s);
        assert_eq!(t.raw(s), RDR_FREE);
        assert_eq!(region.slots()[0].pid().load(Ordering::SeqCst), 0);
    }

    #[test]
    fn reap_frees_only_dead_processes_slots() {
        let (region, t) = shared_table(4, 1);
        let a = t.claim_as(Some(2)).unwrap(); // process 2, will die
        t.store_pin(a, 5);
        let b = t.claim_as(Some(3)).unwrap(); // process 3, alive
        t.store_pin(b, 6);
        let c = t.claim().unwrap(); // this process (1)
        t.store_pin(c, 7);
        region.kill(2);
        let n = t
            .reap_dead(1, |p| crate::shared::SharedRegion::is_alive(&*region, p))
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(t.raw(a), RDR_FREE);
        assert_eq!(t.raw(b), 6);
        assert_eq!(t.raw(c), 7);
        assert_eq!(t.oldest(), Some(6));
    }

    #[test]
    fn reap_counts_each_dead_slot_once() {
        let (region, t) = shared_table(4, 1);
        for _ in 0..2 {
            let s = t.claim_as(Some(2)).unwrap();
            t.store_pin(s, 5);
        }
        region.kill(2);
        let alive = |p| crate::shared::SharedRegion::is_alive(&*region, p);
        assert_eq!(t.reap_dead(1, alive).unwrap(), 2);
        assert_eq!(t.reap_dead(1, alive).unwrap(), 0);
        assert_eq!(t.oldest(), None);
    }

    #[test]
    fn reap_skips_claimed_slots_without_a_pid() {
        // The known leak (ADR-0023 §3): a claim CAS without its PID store.
        let (_region, t) = shared_table(2, 1);
        let s = t.claim_as(None).unwrap();
        assert_eq!(t.reap_dead(1, |_| Ok(false)).unwrap(), 0);
        assert_eq!(t.raw(s), RDR_CLAIMED);
    }

    #[test]
    fn reap_never_probes_its_own_pid() {
        let (_region, t) = shared_table(2, 1);
        let s = t.claim().unwrap();
        t.store_pin(s, 5);
        let n = t
            .reap_dead(1, |p| {
                assert_ne!(p, 1, "probed own PID");
                Ok(true)
            })
            .unwrap();
        assert_eq!(n, 0);
        assert_eq!(t.raw(s), 5);
    }

    #[test]
    fn reap_probes_each_pid_once_per_scan() {
        let (_region, t) = shared_table(4, 1);
        for _ in 0..3 {
            let s = t.claim_as(Some(9)).unwrap();
            t.store_pin(s, 5);
        }
        let mut probes = 0;
        let n = t
            .reap_dead(1, |_| {
                probes += 1;
                Ok(false)
            })
            .unwrap();
        assert_eq!((n, probes), (3, 1));
    }

    #[test]
    fn reap_propagates_a_probe_error_without_freeing() {
        let (_region, t) = shared_table(2, 1);
        let s = t.claim_as(Some(2)).unwrap();
        t.store_pin(s, 5);
        assert!(t.reap_dead(1, |_| Err(std::io::Error::other("probe"))).is_err());
        assert_eq!(t.raw(s), 5);
    }

    #[test]
    fn own_pid_stale_slots_are_freed() {
        // A dead predecessor with this PID (1) left two slots; another
        // process (2) holds one.
        let (_region, t) = shared_table(4, 1);
        for v in [5, 6] {
            let s = t.claim_as(Some(1)).unwrap();
            t.store_pin(s, v);
        }
        let other = t.claim_as(Some(2)).unwrap();
        t.store_pin(other, 7);
        assert_eq!(t.reap_own_stale(1), 2);
        assert_eq!(t.oldest(), Some(7));
        assert_eq!(t.list(), vec![(other, Some(7), Some(2))]);
    }

    #[test]
    fn list_reports_the_owner_pid() {
        let (_region, t) = shared_table(3, 4);
        let a = t.claim().unwrap();
        t.store_pin(a, 3);
        let b = t.claim_as(None).unwrap();
        assert_eq!(t.list(), vec![(a, Some(3), Some(4)), (b, None, None)]);
        let local = ReaderTable::new(2);
        let c = local.claim().unwrap();
        local.store_pin(c, 2);
        assert_eq!(local.list(), vec![(c, Some(2), None)]);
    }

    #[test]
    fn pin_against_retries_until_resolved() {
        let t = ReaderTable::new(1);
        let word = AtomicU64::new(4);
        let s = t.claim().unwrap();
        let mut calls = 0;
        let got: Result<Option<u64>, ()> = t.pin_against(s, &word, 8, |v| {
            calls += 1;
            if calls < 3 {
                // Meta slot moved on: a newer T is published.
                word.store(v + 1, Ordering::SeqCst);
                Ok(None)
            } else {
                Ok(Some(v))
            }
        });
        assert_eq!(got, Ok(Some(6)));
        assert_eq!(t.raw(s), 6);
        let none: Result<Option<u64>, ()> = t.pin_against(s, &word, 2, |_| Ok(None));
        assert_eq!(none, Ok(None), "gives up after `limit` attempts");
    }
```

- [ ] **Step 2: Run them to verify they fail to compile**

Run: `cargo test -p zerodb-core --lib readers::tests`
Expected: compile errors for `claim_as`, `reap_dead`, `reap_own_stale`, `pin_against`, `HeapRegion::kill` and `is_alive`, and for `list`'s new tuple.

- [ ] **Step 3: Add `is_alive` to `SharedRegion` and the test region (`shared.rs`)**

In the trait, after `is_initializer`:

```rust
    /// Whether process `pid` currently has this environment open (holds its
    /// liveness lock, ADR-0023 §3). Immune to PID reuse for other processes.
    /// For this process's own PID it is always `true`, even when the PID was
    /// recorded by a dead predecessor with the same PID; the reader table
    /// frees those slots at open instead (ADR-0023 amendment 9).
    ///
    /// # Errors
    ///
    /// The I/O error of the underlying probe.
    fn is_alive(&self, pid: u32) -> std::io::Result<bool>;
```

In `HeapRegion`, add a field `dead: std::sync::Mutex<Vec<u32>>` (initialized empty in `new`), and add:

```rust
    /// Mark `pid` dead for [`SharedRegion::is_alive`].
    pub(crate) fn kill(&self, pid: u32) {
        self.dead.lock().expect("test region poisoned").push(pid);
    }
```

and in `impl SharedRegion for HeapRegion`:

```rust
    fn is_alive(&self, pid: u32) -> std::io::Result<bool> {
        Ok(pid == self.pid || !self.dead.lock().expect("test region poisoned").contains(&pid))
    }
```

Other test regions in `crates/zerodb-core/src/env.rs` (search for `impl crate::shared::SharedRegion for` and `impl SharedRegion for`) get `fn is_alive(&self, _pid: u32) -> std::io::Result<bool> { Ok(true) }`.

- [ ] **Step 4: Implement in `readers.rs`**

Replace `claim` with a wrapper over `claim_as`. The body is today's `claim`, with `self.owner_pid` replaced by `owner`:

```rust
    pub(crate) fn claim(&self) -> Option<u32> {
        self.claim_as(self.owner_pid)
    }

    /// [`ReaderTable::claim`] on behalf of `owner` (the PID recorded in the
    /// slot; `None` records none). The table's own claims pass its
    /// `owner_pid`; loom models and unit tests pass other PIDs to play
    /// several processes over one table.
    pub(crate) fn claim_as(&self, owner: Option<u32>) -> Option<u32> {
        // (today's claim body, with `if let Some(pid) = owner`)
    }

    /// This table's owner PID: `Some` for a shared table.
    pub(crate) fn owner_pid(&self) -> Option<u32> {
        self.owner_pid
    }
```

Replace `release` with:

```rust
    pub(crate) fn release(&self, slot: u32) {
        self.release_as(slot, self.owner_pid.is_some());
    }

    /// Release `slot`. `owned` says whether the slot carries an owner PID (a
    /// shared table), in which case the release is three stores (ADR-0023
    /// amendment 7):
    ///
    /// 1. `txnid = CLAIMED` (`Release`): unpins. It orders the owner's page
    ///    reads before the GC gate can stop counting this slot (the TXN-18a
    ///    argument). A death after this store leaves `CLAIMED` plus the dead
    ///    PID, which a reaper frees.
    /// 2. `pid = 0` (`Release`): disowns. A death after this store leaves
    ///    `CLAIMED` with no PID, the known leak (ADR-0023 §3), which pins
    ///    nothing.
    /// 3. `txnid = FREE` (`Release`): hands the slot over. The next claimer's
    ///    `Acquire` CAS on FREE also sees `pid == 0` before it stores its own
    ///    PID, so the PID word's modification order is `old, 0, new`. A
    ///    reaper's `CAS old → reaper` can then succeed only before step 2,
    ///    and a reaper acts only on a dead owner, which cannot be mid-release.
    ///
    /// A local table (`owned == false`) does step 3 alone, as before
    /// (TXN-18a).
    pub(crate) fn release_as(&self, slot: u32, owned: bool) {
        let rs = &self.slots()[slot as usize];
        if owned {
            rs.txnid().store(RDR_CLAIMED, Ordering::Release);
            rs.pid().store(0, Ordering::Release);
        }
        rs.txnid().store(RDR_FREE, Ordering::Release);
    }
```

Add, after `oldest`:

```rust
    /// Free every slot whose owner process is dead (ADR-0023 §3 and
    /// amendment 8); returns how many slots were freed. `reaper` is this
    /// process's PID: its own slots are never probed (a probe cannot see this
    /// process's own lock, and its stale own-PID slots were freed at open).
    /// `is_alive` is probed at most once per PID per scan.
    ///
    /// Per slot: load the PID (`Acquire`, pairing with the claimer's
    /// `Release` PID store, so a non-zero PID implies the claim CAS that
    /// preceded it). Skip PID 0 (free, being claimed, or being released:
    /// presumed live). Probe the owner. If it is dead, adopt the slot with
    /// `CAS pid P → reaper` (`AcqRel` on success: it is the single
    /// linearization point that two reapers, or a reaper and a re-claim,
    /// contend on; the `Acquire` half makes the dead owner's last stores
    /// visible before this reaper overwrites the slot). Then run the owner's
    /// release sequence. A reaper that dies after the CAS leaves a slot
    /// carrying its own PID, which the next reaper frees.
    ///
    /// The scan can miss a slot that dies while it runs, and can see a
    /// released-then-reclaimed slot under its new owner; both only leave a
    /// slot for the next scan, never free a live one.
    ///
    /// # Errors
    ///
    /// The first probe error; slots freed before it stay freed.
    pub(crate) fn reap_dead(
        &self,
        reaper: u32,
        mut is_alive: impl FnMut(u32) -> std::io::Result<bool>,
    ) -> std::io::Result<u32> {
        let mut verdicts: Vec<(u32, bool)> = Vec::new();
        let mut freed = 0;
        for (i, rs) in self.slots().iter().enumerate() {
            let pid = rs.pid().load(Ordering::Acquire);
            if pid == 0 || pid == reaper {
                continue;
            }
            let alive = match verdicts.iter().find(|(p, _)| *p == pid) {
                Some(&(_, a)) => a,
                None => {
                    let a = is_alive(pid)?;
                    verdicts.push((pid, a));
                    a
                }
            };
            if alive {
                continue;
            }
            // Relaxed on failure: a lost race (another reaper, or the slot
            // changed owner) carries no data; skip the slot.
            if rs
                .pid()
                .compare_exchange(pid, reaper, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                debug_assert_ne!(
                    rs.txnid().load(Ordering::Relaxed),
                    RDR_FREE,
                    "a slot with an owner PID is never FREE (ADR-0023 amendment 7)"
                );
                self.release_as(i as u32, true);
                freed += 1;
            }
        }
        Ok(freed)
    }

    /// Free every slot that carries this process's PID `me` (ADR-0023
    /// amendment 9). Called once at open, before this process's first claim,
    /// so any such slot was left by a dead process that had the same PID.
    /// Returns how many slots were freed.
    ///
    /// `CAS pid me → 0` (`AcqRel`, as in [`ReaderTable::reap_dead`]) is what
    /// excludes another process's reaper. That reaper may have probed `me`
    /// as dead before this process took its liveness lock; its pending
    /// `CAS me → reaper` now fails. Then `txnid = FREE` (`Release`).
    /// Dying between the two stores leaves a pinned slot with PID 0 until
    /// the next first-opener wipe: a one-store window, documented in the ADR.
    pub(crate) fn reap_own_stale(&self, me: u32) -> u32 {
        let mut freed = 0;
        for rs in self.slots() {
            if rs.pid().load(Ordering::Acquire) == me
                && rs
                    .pid()
                    .compare_exchange(me, 0, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
            {
                rs.txnid().store(RDR_FREE, Ordering::Release);
                freed += 1;
            }
        }
        freed
    }
```

Add, after `publish_and_verify`:

```rust
    /// Pin `slot` against a published-txnid word and resolve the snapshot
    /// for the verified txnid: run [`ReaderTable::publish_and_verify`], call
    /// `resolve(t)`, and retry while it returns `Ok(None)` (the meta slot
    /// for `t` moved on, so a newer txnid is published), at most `limit`
    /// times. `Ok(None)` after `limit` attempts; errors pass through. The
    /// slot keeps the last verified pin either way, which is conservative
    /// for the GC gate (TXN-20); the caller releases it on failure.
    ///
    /// The lock-file reader (`EnvInner::pin_shared`, ADR-0023 §4) and the
    /// loom model of that protocol both run this function.
    pub(crate) fn pin_against<S, E>(
        &self,
        slot: u32,
        t_word: &AtomicU64,
        limit: u32,
        mut resolve: impl FnMut(u64) -> Result<Option<S>, E>,
    ) -> Result<Option<S>, E> {
        for _ in 0..limit {
            let t = self.publish_and_verify(slot, t_word);
            if let Some(s) = resolve(t)? {
                return Ok(Some(s));
            }
            #[cfg(not(loom))]
            std::thread::yield_now();
            #[cfg(loom)]
            loom::thread::yield_now();
        }
        Ok(None)
    }
```

Change `list` to return the PID:

```rust
    /// ... (keep the existing doc, and add:) The third element is the owner
    /// PID, `None` for a local table or a slot whose PID word is 0 (being
    /// claimed or released). `Acquire`, as in [`ReaderTable::reap_dead`].
    pub(crate) fn list(&self) -> Vec<(u32, Option<u64>, Option<u32>)> {
        let mut out = Vec::new();
        for (i, rs) in self.slots().iter().enumerate() {
            let txnid = match rs.txnid().load(Ordering::SeqCst) {
                RDR_FREE => continue,
                RDR_CLAIMED => None,
                t => Some(t),
            };
            let pid = match self.owner_pid {
                None => None,
                Some(_) => Some(rs.pid().load(Ordering::Acquire)).filter(|&p| p != 0),
            };
            out.push((i as u32, txnid, pid));
        }
        out
    }
```

Fix every existing caller and test of `list()` to the three-tuple. Today that is `EnvInner::reader_list` (map the third element in Task 6; for now `|(slot, txnid, _pid)|`) plus any `list()` assertions in `readers.rs` tests.

- [ ] **Step 5: Route `pin_shared` through `pin_against` (`env.rs`)**

```rust
    #[cfg(not(loom))]
    fn pin_shared(
        &self,
        region: &dyn crate::shared::SharedRegion,
    ) -> Result<(Arc<Snapshot>, u32), Error> {
        let slot = self
            .reader_table
            .claim()
            .ok_or(Error::Mdb(MdbError::ReadersFull))?;
        // TXN-17 publish-and-verify against the shared T, then the snapshot
        // for the verified txnid; `None` means meta slot `t & 1` moved on.
        match self
            .reader_table
            .pin_against(slot, region.published_txnid(), META_RETRY_LIMIT, |t| {
                self.snapshot_at(t)
            }) {
            Ok(Some(snap)) => Ok((snap, slot)),
            Ok(None) => {
                self.reader_table.release(slot);
                Err(Error::Mdb(MdbError::Invalid))
            }
            Err(e) => {
                self.reader_table.release(slot);
                Err(e)
            }
        }
    }
```

If `META_RETRY_LIMIT` is not a `u32`, cast it at the call site.

- [ ] **Step 6: Run the tests and miri**

Run: `cargo test -p zerodb-core --lib` → all pass.
Run: `cargo +nightly miri test -p zerodb-core readers` → pass.
Run: `cargo test -p zerodb --test lock_file_single_process --test multi_process` → pass. The protocol change must not break stage 1.

- [ ] **Step 7: Commit**

```bash
git add crates/zerodb-core/src/shared.rs crates/zerodb-core/src/readers.rs crates/zerodb-core/src/env.rs
git commit -m "zerodb-core: reap dead processes' reader slots; release unpins before it disowns"
```

---

### Task 4: loom models of the shared reader protocol

**Files:**
- Modify: `crates/zerodb-core/src/readers.rs` (`mod loom_tests`)

**Interfaces:**
- Consumes (Task 3): `claim_as`, `release_as`, `reap_dead`, `reap_own_stale`, `pin_against`, `store_pin`, `oldest`, `raw`.

Under loom the local `Slot` already has a loom `pid` word. A model plays several processes over one `ReaderTable::new(n)` through `claim_as(Some(pid))` and `release_as(slot, true)`. A process's death is a loom `AtomicBool` the dying thread sets last (`SeqCst`), and the reaper's `is_alive` reads it (`SeqCst`). This models the kernel releasing the liveness lock only after the process has stopped running. The real probe is a syscall, so it carries at least that ordering.

- [ ] **Step 1: Add the models**

```rust
    use crate::sync::{AtomicU64 as LAtomicU64, Ordering as O};
    use loom::sync::atomic::AtomicBool;

    /// L6 — two reapers race on one dead slot: it is freed exactly once
    /// (the counts sum to 1), and afterwards the slot is FREE, unowned and
    /// claimable (ADR-0023 §3, amendment 8).
    #[test]
    fn loom_l6_two_reapers_one_dead_slot() {
        loom::model(|| {
            let t = Arc::new(ReaderTable::new(1));
            let s = t.claim_as(Some(2)).unwrap();
            t.store_pin(s, 5); // process 2 pinned 5, then died
            let t2 = Arc::clone(&t);
            let r2 = thread::spawn(move || t2.reap_dead(3, |p| Ok(p != 2)).unwrap());
            let n1 = t.reap_dead(4, |p| Ok(p != 2)).unwrap();
            let n2 = r2.join().unwrap();
            assert_eq!(n1 + n2, 1, "one dead slot freed {n1}+{n2} times");
            assert_eq!(t.oldest(), None);
            assert_eq!(t.claim_as(Some(5)), Some(0));
        });
    }

    /// L7 — a reaper that read P's PID races P's release, P's death, and Q's
    /// re-claim of the same slot. The reaper must never free Q's slot: after
    /// the joins, if Q claimed, Q's pin is still in place and Q still owns the
    /// slot.
    #[test]
    fn loom_l7_reaper_vs_release_and_reclaim() {
        loom::model(|| {
            let t = Arc::new(ReaderTable::new(1));
            let p_dead = Arc::new(AtomicBool::new(false));
            let s = t.claim_as(Some(2)).unwrap();
            t.store_pin(s, 5);
            let (tp, pd) = (Arc::clone(&t), Arc::clone(&p_dead));
            let p = thread::spawn(move || {
                tp.release_as(s, true);
                pd.store(true, O::SeqCst); // P exits after releasing
            });
            let tq = Arc::clone(&t);
            let q = thread::spawn(move || {
                let got = tq.claim_as(Some(3));
                if let Some(slot) = got {
                    tq.store_pin(slot, 6);
                }
                got
            });
            let pd = Arc::clone(&p_dead);
            let n = t
                .reap_dead(4, move |pid| Ok(pid != 2 || !pd.load(O::SeqCst)))
                .unwrap();
            p.join().unwrap();
            let q_slot = q.join().unwrap();
            assert_eq!(n, 0, "nothing was ever dead and owned");
            if let Some(slot) = q_slot {
                assert_eq!(t.raw(slot), 6, "a reaper freed Q's live slot");
            }
        });
    }

    /// L8 — claim / pin / release by a live process races a reap scan that
    /// sees no dead owners, against the writer's `oldest()` scan: the scan
    /// never reports a value other than the live pin or nothing, and a
    /// reaper never frees a live slot (ADR-0023 §3, TXN-20).
    #[test]
    fn loom_l8_live_reader_vs_reaper_and_scan() {
        loom::model(|| {
            let t = Arc::new(ReaderTable::new(1));
            let t1 = Arc::clone(&t);
            let reader = thread::spawn(move || {
                let s = t1.claim_as(Some(2)).unwrap();
                t1.store_pin(s, 5);
                t1.release_as(s, true);
            });
            let t2 = Arc::clone(&t);
            let reaper = thread::spawn(move || t2.reap_dead(3, |_| Ok(true)).unwrap());
            let o = t.oldest();
            reader.join().unwrap();
            assert_eq!(reaper.join().unwrap(), 0);
            assert!(o.is_none() || o == Some(5), "scan saw {o:?}");
            assert_eq!(t.oldest(), None);
        });
    }

    /// L9 — publish-and-verify against a shared T, with the meta read
    /// modeled. The writer writes meta slot `N & 1` (a plain data write,
    /// modeled `Relaxed`; it is a `pwrite`), then stores T = N `SeqCst`, then
    /// (as the next writer) computes the GC gate with seed N. The reader runs
    /// `pin_against`, resolving t by reading meta slot `t & 1` and accepting
    /// only a txnid equal to t (ADR-0023 §4, amendment 4). In every
    /// interleaving the reader resolves a snapshot whose txnid equals its
    /// pin, and its pin is `>=` the writer's gate (TXN-20).
    #[test]
    fn loom_l9_pin_against_shared_t() {
        loom::model(|| {
            let t_word = Arc::new(LAtomicU64::new(5));
            let metas = Arc::new([LAtomicU64::new(4), LAtomicU64::new(5)]);
            let table = Arc::new(ReaderTable::new(1));
            let (tr, wr, mr) = (Arc::clone(&table), Arc::clone(&t_word), Arc::clone(&metas));
            let reader = thread::spawn(move || {
                let s = tr.claim_as(Some(2)).unwrap();
                let got: Result<Option<u64>, ()> = tr.pin_against(s, &wr, 4, |t| {
                    // Acquire: a pread after the SeqCst load of T sees the
                    // meta written before T was stored.
                    let m = mr[(t & 1) as usize].load(O::Acquire);
                    Ok((m == t).then_some(t))
                });
                (got.unwrap(), tr.raw(s))
            });
            metas[0].store(6, O::Relaxed); // commit 6 writes meta slot 0
            t_word.store(6, O::SeqCst); // then publishes T
            let gate = table.oldest().map_or(6, |r| r.min(6));
            let (resolved, pinned) = reader.join().unwrap();
            let snap = resolved.expect("4 attempts suffice with one racing commit");
            assert!(snap == 5 || snap == 6, "resolved {snap}");
            assert_eq!(pinned, snap, "pinned {pinned} but resolved {snap}");
            assert!(pinned >= gate, "gate {gate} passed a live pin {pinned}");
        });
    }

    /// L10 — own-PID stale reaping at open races another process's reaper
    /// that already judged the PID dead: the slot is freed exactly once.
    #[test]
    fn loom_l10_own_stale_vs_foreign_reaper() {
        loom::model(|| {
            let t = Arc::new(ReaderTable::new(1));
            let s = t.claim_as(Some(2)).unwrap();
            t.store_pin(s, 5); // a dead predecessor with PID 2
            let t2 = Arc::clone(&t);
            // The foreign reaper probed PID 2 before the new process took
            // its liveness lock, so it believes 2 is dead.
            let foreign = thread::spawn(move || t2.reap_dead(3, |_| Ok(false)).unwrap());
            let own = t.reap_own_stale(2);
            let n = foreign.join().unwrap();
            assert_eq!(own + n, 1, "freed {own}+{n} times");
            assert_eq!(t.oldest(), None);
        });
    }
    /// L11 — PID reuse (ADR-0023 amendment 14): a reaper holding a stale
    /// "dead" verdict on PID 2 races a new process that also has PID 2,
    /// which frees its predecessor's slot and re-claims it. The claim
    /// generation makes the reaper's CAS fail: the new process's pin
    /// survives in every interleaving.
    #[test]
    fn loom_l11_reaper_vs_same_pid_reclaim() {
        loom::model(|| {
            let t = Arc::new(ReaderTable::new(1));
            let s = t.claim_as(Some(2)).unwrap();
            t.store_pin(s, 5); // the old process 2 pinned 5, then died
            let t2 = Arc::clone(&t);
            // The reaper's probe answered "dead" for the old process 2.
            let reaper = thread::spawn(move || t2.reap_dead(3, |_| Ok(false)).unwrap());
            // The new process 2 frees its predecessor's slot, then claims.
            let own = t.reap_own_stale(2);
            let mine = t.claim_as(Some(2));
            if let Some(slot) = mine {
                t.store_pin(slot, 6);
            }
            let reaped = reaper.join().unwrap();
            assert_eq!(own + reaped, 1, "the old slot is freed exactly once");
            if let Some(slot) = mine {
                assert_eq!(t.raw(slot), 6, "a reaper freed the new process's live slot");
            }
        });
    }

```

Update the header comment of `mod loom_tests` (search for "L1–L5"), and the module doc at the top of `readers.rs`, to say L1–L11.

- [ ] **Step 2: Run loom**

Run: `just loom`
Expected: `loom_l1`…`loom_l11` all pass. If the registry cache misbehaves, use `CARGO_HOME=<scratchpad>/cargo-home just loom` (memory: cargo-registry-cache-corrupted).

- [ ] **Step 3: Mutation check (do not commit the mutation)**

Temporarily revert `release_as` to the §3 order: `pid = 0`, then `FREE`, with no `CLAIMED` store. Rerun `just loom`. All models still pass, because amendment 7 is about process death between the stores, which loom does not model. Record this in the commit message.

Next, temporarily change the CAS in `reap_dead` to a plain `rs.pid().store(reaper, …)`. Rerun: `loom_l6` or `loom_l10` must fail. Revert.

Then temporarily drop the generation increment in `claim_as` (store `owner_word(owner_generation(w), pid)`). Rerun: `loom_l11` must fail. Revert.

- [ ] **Step 4: Commit**

```bash
git add crates/zerodb-core/src/readers.rs
git commit -m "zerodb-core: loom models of reaping and of pinning against a shared T (L6-L10)"
```

---

### Task 5: Lock-file liveness, re-election, magic-last, slot offset

**Files:**
- Modify: `crates/zerodb-io/src/lockfile.rs`
- Create: `crates/zerodb-io/tests/lockfile_processes.rs`

**Interfaces:**
- Consumes (Task 3): `SharedRegion::is_alive`.
- Produces:
  - `LockFile` implements `SharedRegion::is_alive`. The inherent `is_alive` moves into the trait impl, unchanged.
  - `LOCK_FORMAT = 2`, `OFF_SLOTS = 256`.
  - `LockFile::finish_open` stores the magic (initializer).

- [ ] **Step 1: Write the failing integration tests**

`crates/zerodb-io/tests/lockfile_processes.rs`:

```rust
//! Multi-process behavior of the lock file's first-opener election
//! (ADR-0023 §1, amendment 10). The test binary re-runs itself as the other
//! processes.

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use zerodb_core::shared::SharedRegion;
use zerodb_io::lockfile::LockFile;

const ROLE: &str = "ZERODB_LOCKFILE_ROLE";
const DIR: &str = "ZERODB_LOCKFILE_DIR";

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> TempDir {
        static N: AtomicU64 = AtomicU64::new(0);
        // Relaxed: only uniqueness matters.
        let p = std::env::temp_dir().join(format!(
            "zerodb-lockproc-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn(role: &str, dir: &Path) -> ChildGuard {
    ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "child_entry", "--nocapture", "--test-threads=1"])
            .env(ROLE, role)
            .env(DIR, dir)
            .spawn()
            .unwrap(),
    )
}

fn wait_for(p: &Path) {
    let end = Instant::now() + Duration::from_secs(30);
    while !p.exists() {
        assert!(Instant::now() < end, "timed out on {}", p.display());
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn child_entry() {
    let (Ok(role), Ok(dir)) = (std::env::var(ROLE), std::env::var(DIR)) else { return };
    let dir = PathBuf::from(dir);
    let path = dir.join("zerodb.lock");
    match role.as_str() {
        // Win the election, never finish: stand in for an initializer that
        // dies (SIGKILL) before seeding T.
        "init_and_hang" => {
            let l = LockFile::open(&path, 8).unwrap();
            assert!(l.is_initializer());
            std::fs::write(dir.join("initializing"), b"").unwrap();
            std::thread::sleep(Duration::from_secs(600));
        }
        // Join (or initialize), finish, report, exit.
        "join_and_report" => {
            let l = LockFile::open(&path, 8).unwrap();
            l.bind_page_size(4096).unwrap();
            l.finish_open().unwrap();
            let name = if l.is_initializer() { "child-init" } else { "child-join" };
            std::fs::write(dir.join(name), b"").unwrap();
            // Hold the environment until the parent has also opened.
            wait_for(&dir.join("parent-open"));
        }
        other => panic!("unknown role {other}"),
    }
}

/// A joiner waits on the election lock while the initializer dies before
/// finishing. With every other process gone, the joiner must initialize
/// rather than adopt a half-built file.
#[test]
fn joiner_whose_initializer_died_reinitializes() {
    let dir = TempDir::new();
    let mut init = spawn("init_and_hang", &dir.0);
    wait_for(&dir.0.join("initializing"));
    let path = dir.0.join("zerodb.lock");
    let opener = std::thread::spawn(move || LockFile::open(&path, 8).map(|l| l.is_initializer()));
    std::thread::sleep(Duration::from_millis(200)); // the joiner is now blocked
    init.0.kill().unwrap();
    init.0.wait().unwrap();
    let became_initializer = opener.join().unwrap().expect("open after the initializer died");
    assert!(became_initializer, "the joiner adopted a half-built lock file");
}

/// Two waiting processes, initializer dies: both opens succeed, exactly one
/// initializes. Repeated, because which one wins the upgrade varies.
#[test]
fn three_way_race_after_initializer_death() {
    for _ in 0..20 {
        let dir = TempDir::new();
        let mut init = spawn("init_and_hang", &dir.0);
        wait_for(&dir.0.join("initializing"));
        let child = spawn("join_and_report", &dir.0);
        let path = dir.0.join("zerodb.lock");
        let parent = std::thread::spawn(move || {
            let l = LockFile::open(&path, 8).expect("parent open");
            l.bind_page_size(4096).unwrap();
            l.finish_open().unwrap();
            l
        });
        std::thread::sleep(Duration::from_millis(100));
        init.0.kill().unwrap();
        init.0.wait().unwrap();
        let l = parent.join().unwrap();
        std::fs::write(dir.0.join("parent-open"), b"").unwrap();
        let mut child = child;
        assert!(child.0.wait().unwrap().success(), "child open failed");
        let child_init = dir.0.join("child-init").exists();
        assert!(
            l.is_initializer() != child_init,
            "exactly one of the two must initialize (parent {}, child {child_init})",
            l.is_initializer()
        );
    }
}

/// A live process is alive; a SIGKILLed one is not (the kernel drops its
/// liveness lock).
#[test]
fn liveness_probe_sees_death() {
    let dir = TempDir::new();
    let path = dir.0.join("zerodb.lock");
    let l = LockFile::open(&path, 8).unwrap();
    l.bind_page_size(4096).unwrap();
    l.finish_open().unwrap();
    let mut c = spawn("join_and_report", &dir.0);
    wait_for(&dir.0.join("child-join"));
    let pid = c.0.id();
    assert!(l.is_alive(pid).unwrap());
    c.0.kill().unwrap();
    c.0.wait().unwrap();
    assert!(!l.is_alive(pid).unwrap());
    assert!(l.is_alive(std::process::id()).unwrap());
}
```

Note: in `liveness_probe_sees_death` the child blocks on `parent-open`, which this test never writes; the SIGKILL ends it.

Run: `cargo test -p zerodb-io --test lockfile_processes`
Expected: `joiner_whose_initializer_died_reinitializes` fails, because the joiner adopts the file without initializing. `three_way_race_after_initializer_death` fails or hangs as well. `liveness_probe_sees_death` fails to compile until `is_alive` is on the trait (it is called through `SharedRegion`).

- [ ] **Step 2: Move the slots and bump the format**

```rust
const LOCK_FORMAT: u32 = 2;
const OFF_SLOTS: usize = 256;
```

Update the module-doc layout table: `| 256 + i·128 | reader slot i … |`. Add a sentence saying offsets 136–255 are unused, to keep slot 0 off the writer word's 128-byte line pair (ADR-0023 amendment 11).

- [ ] **Step 3: Store the magic last, in `finish_open`**

In `open`, delete the initializer's `lock.u64_at(OFF_MAGIC).store(LOCK_MAGIC, Ordering::Release)` and its comment. Keep the `OFF_FORMAT` and `OFF_MAX_READERS` stores; they stay `Relaxed`, published by the magic store. `finish_open` becomes:

```rust
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
```

Update `finish_open`'s doc to say it publishes the header and lets joiners in.

- [ ] **Step 4: Re-election in `open`**

Replace the election lines (from `let initializer = record_lock(...)` up to the `slots` computation) with a bounded loop:

```rust
        /// How often a joiner that finds a half-built lock file retries the
        /// election before failing (ADR-0023 amendment 10).
        const ELECTION_RETRIES: u32 = 100;
        let mut attempt = 0;
        let initializer = loop {
            if record_lock(&file, libc::F_SETLK, WRLCK, ELECTION_BYTE)? {
                break true;
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
            // Another process holds the byte too. If the initializer
            // finished, the magic is set; otherwise it died mid-way and the
            // other holder is a joiner in the same position. Drop the lock
            // and retry the election so one of them initializes.
            if read_u64_at(&file, OFF_MAGIC)? == LOCK_MAGIC {
                break false;
            }
            record_lock(&file, libc::F_SETLK, UNLCK, ELECTION_BYTE)?;
            attempt += 1;
            if attempt == ELECTION_RETRIES {
                return Err(LockOpenError::Mismatch(
                    "lock file never finished initializing".into(),
                ));
            }
            // Stagger retries by PID so two joiners do not keep colliding.
            std::thread::sleep(std::time::Duration::from_millis(
                1 + u64::from(std::process::id() % 7),
            ));
        };
```

Add the helper next to `read_u32_at`:

```rust
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
```

A joiner reads the magic through `pread` before it maps the file. That read is a kernel copy, made under the shared election lock, which the initializer releases only after its `Release` store of the magic in `finish_open`. Lock release and acquisition order the two processes, so this is sound. Keep the existing in-map `Acquire` validation of the magic as the second check.

- [ ] **Step 5: `is_alive` on the trait**

Move the inherent `pub fn is_alive` into `impl SharedRegion for LockFile` as `fn is_alive(&self, pid: u32) -> std::io::Result<bool>`, keeping its body and its doc comment (the trait doc carries the contract). `lock_writer`'s call `self.is_alive(owner)?` resolves to the trait method. If clippy or rustc asks, add `use zerodb_core::shared::SharedRegion as _;`; the trait is already imported.

- [ ] **Step 6: Run the tests**

Run: `cargo test -p zerodb-io` → all pass. That covers the new integration tests and the existing unit tests; `first_opener_initializes_every_slot_free` still passes.
Run: `cargo test -p zerodb --test lock_file_single_process --test multi_process` → pass.
Run clippy for both targets (Global Constraints).

- [ ] **Step 7: Commit**

```bash
git add crates/zerodb-io/src/lockfile.rs crates/zerodb-io/tests/lockfile_processes.rs
git commit -m "zerodb-io: liveness on the region, joiner re-election, magic stored last, slots at 256 (lock format 2)"
```

---

### Task 6: Reap in the environment where LMDB reaps

**Files:**
- Modify: `crates/zerodb-core/src/env.rs`:
  - `clear_stale_readers`;
  - `reader_list` and `ReaderEntry`;
  - own-stale reaping at open;
  - the observed reap points.
- Modify: `crates/zerodb-io/src/lockfile.rs` (only if Task 2 observed reaping at writer takeover)
- Modify: `crates/zerodb-core/src/shared.rs` (only if Task 2 observed reaping at writer takeover)
- Modify: `crates/heed-zerodb/src/env.rs` (the `clear_stale_readers` doc)
- Test: `crates/zerodb-core/src/env.rs` (`mod tests`, using `HeapRegion`)

**Interfaces:**
- Consumes (Task 3): `reap_dead`, `reap_own_stale`, `owner_pid`, `list`. Consumes (Task 5): `SharedRegion::is_alive`.
- Produces:
  - `ReaderEntry::pid: Option<u32>`, a public field.
  - `EnvInner::clear_stale_readers(&self) -> Result<usize, Error>`.
  - `Env::clear_stale_readers` returns the reaped count in a lock-file env.

> **Since this task was written (Task 3, amendment 14):** the slot's PID is the low half of a 64-bit `owner` word (`ReaderSlot::owner()`, helpers `owner_pid` / `owner_generation` / `owner_word` in `shared.rs`), and `reap_dead` snapshots every owner word before probing. `HeapRegion`'s slots are reached through `SharedRegion::slots(&*region)` (the struct has a `slots` field that shadows the trait method). Task 3 left `#[allow(dead_code)]` on `owner_pid`, `reap_dead` and `reap_own_stale`; remove each once this task calls it. `reap_own_stale` runs only at open, where the in-process lock-file registry already refuses a second open of the same lock file, so no live slot of this process can exist then (state this in the call-site comment).

- [ ] **Step 1: Write the failing tests (`env.rs` `mod tests`)**

Find the existing tests that open an env over a `HeapRegion` (search for `HeapRegion::new` in `env.rs`) and reuse their helper. The helper is referred to below as `open_shared(region)`; use whatever name the file already has.

```rust
    #[test]
    fn clear_stale_readers_frees_a_dead_processes_slots() {
        let region = Arc::new(crate::shared::HeapRegion::new(4, 1, true));
        let env = open_shared(Arc::clone(&region));
        let s = env.inner().reader_table_for_tests().claim_as(Some(2)).unwrap();
        env.inner().reader_table_for_tests().store_pin(s, 1);
        assert_eq!(env.clear_stale_readers().unwrap(), 0, "process 2 is alive");
        region.kill(2);
        assert_eq!(env.clear_stale_readers().unwrap(), 1);
        assert_eq!(env.clear_stale_readers().unwrap(), 0);
        assert!(env.reader_list().is_empty());
    }

    #[test]
    fn reader_list_reports_pids_in_a_lock_file_env() {
        let region = Arc::new(crate::shared::HeapRegion::new(4, 7, true));
        let env = open_shared(Arc::clone(&region));
        let r = env.read_txn().unwrap();
        let list = env.reader_list();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].pid, Some(7));
        drop(r);
    }

    #[test]
    fn joiner_frees_slots_left_under_its_own_pid() {
        // A region whose initializer (another process) is alive, and in
        // which a dead predecessor with this process's PID (5) left a pin.
        let region = Arc::new(crate::shared::HeapRegion::new(4, 5, true));
        {
            let env = open_shared(Arc::clone(&region)); // seeds T
            drop(env);
        }
        crate::shared::SharedRegion::slots(&*region)[2]
            .owner()
            .store(crate::shared::owner_word(1, 5), Ordering::SeqCst);
        crate::shared::SharedRegion::slots(&*region)[2].txnid().store(1, Ordering::SeqCst);
        let joiner = crate::shared::HeapRegion::rejoin(&region); // is_initializer false
        let env = open_shared(Arc::clone(&joiner));
        assert!(env.reader_list().is_empty(), "the stale own-PID slot survived open");
    }
```

These tests need two small test-only helpers. Add them if absent, under `#[cfg(all(test, not(loom)))]`:
- `EnvInner::reader_table_for_tests(&self) -> &ReaderTable`;
- `HeapRegion::rejoin(&self) -> Arc<HeapRegion>`. It returns a region that shares the same `t` and `slots`, with `initializer == false`. Implement it by storing the slots and `t` in an `Arc` inside `HeapRegion`, so `slots()` and `published_txnid()` read through it.

If the existing env tests construct `HeapRegion` differently, adapt the helpers; the assertions stay as written.

Run: `cargo test -p zerodb-core --lib env::tests` → the three tests fail.

- [ ] **Step 2: `ReaderEntry::pid`, `reader_list`, `clear_stale_readers`, open-time own-stale reaping**

Add to `ReaderEntry`:

```rust
    /// The PID of the process that owns the slot, in an environment opened
    /// with a lock file (ADR-0023 amendment 13). `None` without a lock file
    /// (every reader is in this process) and for a slot being claimed or
    /// released. LMDB's `mdb_reader_list` prints the same column.
    pub pid: Option<u32>,
```

Rewrite the `ReaderEntry` doc paragraph that says there is no PID column.

In `EnvInner::reader_list`, map `|(slot, txnid, pid)| ReaderEntry { slot, txnid, pid, age: … }`.

Add to `EnvInner`:

```rust
    /// Free the reader slots of dead processes (`mdb_reader_check`,
    /// ADR-0023 §3). Returns the number freed; always 0 without a lock file,
    /// where every slot belongs to this process.
    ///
    /// # Errors
    ///
    /// The I/O error of a liveness probe.
    pub(crate) fn clear_stale_readers(&self) -> Result<usize, Error> {
        #[cfg(not(loom))]
        if let Some(region) = &self.shared {
            let n = self
                .reader_table
                .reap_dead(region.pid(), |p| region.is_alive(p))
                .map_err(Error::Io)?;
            return Ok(n as usize);
        }
        Ok(0)
    }
```

Replace `Env::clear_stale_readers`'s body with `self.inner.clear_stale_readers()`. Rewrite its doc:
- the default path returns 0, and the existing single-process argument stays;
- in a lock-file env it frees dead processes' slots and returns the count;
- the count is slots or processes, as Task 2 observed (`OBS_CHECK_COUNTS_SLOTS`; if processes, count distinct PIDs in `reap_dead`'s caller instead);
- the `# Errors` section now names the probe's I/O error.

Remove the "always returns 0" and "known gap" sentences.

In `open_with_backing_shared`, right after the reader table is built: when a region is present and it is not the initializer, call `reader_table.reap_own_stale(region.pid())`, with a comment citing amendment 9. An initializer wiped every slot already.

- [ ] **Step 3: Reap at the points Task 2 observed**

Task 2 observed (macOS and Linux x86-64, 3 runs each, identical): read txn begin, write txn commit, a joiner's open, and a full table **never** reap; `mdb_reader_check` counts **slots**; writer takeover **does** reap (Linux; the fork's macOS writer mutex is not robust, so there is no takeover there). The one row to implement:

| Observation | Code |
|---|---|
| `OBS_WRITER_TAKEOVER_REAPS` (Linux) | Add `EnvInner::reap_quietly(&self)`, which runs `clear_stale_readers` and ignores a probe error, because a writer must not fail on a janitor probe. Change `SharedRegion::lock_writer` to return `std::io::Result<bool>`, `true` when the lock was taken from a dead owner. The `LockFile` Linux arm returns `true` from both takeover CASes (dead owner, and stale own-PID word) and `false` from the uncontended CAS; macOS returns `false`, because an `fcntl` lock reports no previous owner; `HeapRegion` gets a test switch to return `true` once. In `lock_writer_region`, `if region.lock_writer().map_err(Error::Io)? { self.reap_quietly(); }`. |

`clear_stale_readers` counts slots (no change to `reap_dead`).

Add one test per implemented row to `env.rs` `mod tests`, using `HeapRegion` with a killed PID. Example for `OBS_FULL_TABLE_REAPS`:

```rust
    #[test]
    fn full_table_reaps_and_retries() {
        let region = Arc::new(crate::shared::HeapRegion::new(1, 1, true));
        let env = open_shared(Arc::clone(&region));
        let s = env.inner().reader_table_for_tests().claim_as(Some(2)).unwrap();
        env.inner().reader_table_for_tests().store_pin(s, 1);
        region.kill(2);
        env.read_txn().expect("the dead slot is reaped and reused");
    }
```

For a row that is `false`, add a test asserting the opposite. For example, if `OBS_FULL_TABLE_REAPS` is `false`, the same setup must give `ReadersFull`, and `clear_stale_readers()` then returns 1.

- [ ] **Step 4: heed adapter doc**

In `crates/heed-zerodb/src/env.rs`, update `clear_stale_readers`'s doc to match the engine. The adapter test `clear_stale_readers_through_the_adapter_is_zero_and_harmless` runs without a lock file and stays as is.

- [ ] **Step 5: Run**

Run:
- `cargo test -p zerodb-core --lib`
- `cargo test -p zerodb`
- `cargo test -p heed-zerodb`
- `cargo +nightly miri test -p zerodb-core env::tests`

All must pass. Also run `cargo test -p zerodb --test reader_introspection`: its `clear_stale_readers_is_zero_and_never_disturbs_live_readers` covers the default path and must pass unchanged.

- [ ] **Step 6: Commit**

```bash
git add crates/zerodb-core/src crates/zerodb-io/src/lockfile.rs crates/heed-zerodb/src/env.rs
git commit -m "zerodb: clear_stale_readers reaps dead processes; reap where LMDB reaps; reader_list reports PIDs"
```

---

### Task 7: Two-process tests for reaping and the writer lock

**Files:**
- Modify: `crates/zerodb/tests/multi_process.rs`

**Interfaces:**
- Consumes: `Env::clear_stale_readers`, `Env::reader_list` with `ReaderEntry::pid`, and `Env::info().last_pgno`.

- [ ] **Step 1: Add child roles to `child_entry`'s `match`**

```rust
        "hold_reader" => {
            // Hold a read txn until told to stop, then exit cleanly.
            let r = env.read_txn().unwrap();
            mark(&dir, "child-reading");
            wait_for(&dir.join("child-stop"), Duration::from_secs(60));
            drop(r);
        }
        "hold_readers_and_die" => {
            let a = env.read_txn().unwrap();
            let b = env.read_txn().unwrap();
            mark(&dir, "child-reading");
            std::thread::sleep(Duration::from_secs(600)); // killed by the parent
            drop((a, b));
        }
```

- [ ] **Step 2: Add the tests**

```rust
/// Seed, then rewrite every key `rounds` times. Returns the growth of
/// `last_pgno` across the rewrites.
fn churn_growth(env: &Env, rounds: u32) -> u64 {
    let db = env.main_database();
    let before = env.info().last_pgno;
    for round in 0..rounds {
        let mut w = env.write_txn().unwrap();
        for i in 0..200u32 {
            db.put(&mut w, format!("k{i:04}").as_bytes(), format!("r{round}").repeat(40).as_bytes())
                .unwrap();
        }
        w.commit().unwrap();
    }
    env.info().last_pgno - before
}

#[test]
fn killed_readers_slots_are_reaped_by_clear_stale_readers() {
    let dir = TempDir::new();
    let env = open(dir.path());
    seed(&env);
    let mut c = child("hold_readers_and_die", dir.path());
    wait_for(&dir.path().join("child-reading"), Duration::from_secs(30));
    let pid = c.id();
    c.kill().unwrap();
    c.wait().unwrap();
    let listed = env.reader_list();
    assert_eq!(listed.iter().filter(|e| e.pid == Some(pid)).count(), 2, "{listed:?}");
    // Whether LMDB counts slots or processes is pinned by
    // lmdb_reaping_observation.rs (OBS_CHECK_COUNTS_SLOTS); the expected
    // value below matches it.
    assert_eq!(env.clear_stale_readers().unwrap(), 2);
    assert!(env.reader_list().is_empty());
    assert_eq!(env.clear_stale_readers().unwrap(), 0);
}

#[test]
fn reaping_never_touches_a_live_process() {
    let dir = TempDir::new();
    let env = open(dir.path());
    seed(&env);
    let mut c = child("hold_reader", dir.path());
    wait_for(&dir.path().join("child-reading"), Duration::from_secs(30));
    assert_eq!(env.clear_stale_readers().unwrap(), 0);
    assert_eq!(env.reader_list().iter().filter(|e| e.pid == Some(c.id())).count(), 1);
    mark(dir.path(), "child-stop");
    assert!(c.wait().unwrap().success());
}

#[test]
fn reaped_slot_lets_the_writer_reuse_pages() {
    let dir = TempDir::new();
    let env = open(dir.path());
    seed(&env);
    let mut c = child("hold_readers_and_die", dir.path());
    wait_for(&dir.path().join("child-reading"), Duration::from_secs(30));
    c.kill().unwrap();
    c.wait().unwrap();
    // The dead reader pins the seed snapshot: every rewrite allocates fresh
    // pages.
    let pinned = churn_growth(&env, 20);
    env.clear_stale_readers().unwrap();
    churn_growth(&env, 2); // let the freelist catch up
    let freed = churn_growth(&env, 20);
    assert!(
        freed * 4 < pinned,
        "pages are not reused after reaping: growth {freed} vs {pinned} while pinned"
    );
}

/// The two-process form of the zerodb-io unit test
/// `writer_lock_releases_from_another_thread`. On macOS that unit test
/// cannot tell a released POSIX lock from a held one (a process never
/// conflicts with itself); another process can.
#[test]
fn writer_lock_released_on_another_thread_admits_another_process() {
    let dir = TempDir::new();
    let env = open(dir.path());
    let db = env.main_database();
    let mut w = env.write_txn().unwrap();
    db.put(&mut w, b"parent", b"1").unwrap();
    let mut c = child("write_when_free", dir.path());
    wait_for(&dir.path().join("child-waiting"), Duration::from_secs(30));
    std::thread::sleep(Duration::from_millis(300));
    assert!(!dir.path().join("child-got-lock").exists(), "lock not held");
    std::thread::spawn(move || w.commit().unwrap()).join().unwrap();
    assert!(c.wait().unwrap().success());
    let r = env.read_txn().unwrap();
    assert_eq!(db.get(&r, b"child").unwrap(), Some(&b"2"[..]));
}
```

Add the real takeover-reap test (Task 6 wired it; LMDB reaps at writer takeover on Linux, ADR-0023 amendment 12):

```rust
/// Linux only: the futex writer lock reports a dead owner's takeover, and
/// the new writer then reaps dead readers, as the LMDB fork does. On macOS
/// the `fcntl` writer lock cannot tell a takeover from a normal acquire.
#[cfg(target_os = "linux")]
#[test]
fn writer_takeover_reaps_a_dead_readers_slot() {
    let dir = TempDir::new();
    let env = open(dir.path());
    seed(&env);
    let mut r = child("hold_readers_and_die", dir.path());
    wait_for(&dir.path().join("child-reading"), Duration::from_secs(30));
    let reader_pid = r.id();
    r.kill().unwrap();
    r.wait().unwrap();
    let mut w = child("hold_and_die", dir.path());
    wait_for(&dir.path().join("child-holds-lock"), Duration::from_secs(30));
    w.kill().unwrap();
    w.wait().unwrap();
    assert!(env.reader_list().iter().any(|e| e.pid == Some(reader_pid)));
    drop(env.write_txn().expect("take over the dead writer's lock"));
    assert!(
        !env.reader_list().iter().any(|e| e.pid == Some(reader_pid)),
        "the takeover did not reap the dead reader"
    );
}
```

Also add the "never reaps" counterpart: after `hold_readers_and_die` is killed, a plain `env.write_txn()` + commit with no dead writer leaves the dead reader listed (`assert!(env.reader_list().iter().any(|e| e.pid == Some(pid)))`), on every OS.

The expected `clear_stale_readers()` count for the 2-reader child is **2**: LMDB counts slots (`OBS_CHECK_COUNTS_SLOTS = true`).

If the moved `RwTxn` does not compile (`'static` bound on `thread::spawn`), use `std::thread::scope(|s| { s.spawn(move || w.commit().unwrap()); })`.

Add a doc line on the zerodb-io unit test `writer_lock_releases_from_another_thread`, pointing to this test for the macOS case. Leave the unit test itself unchanged.

- [ ] **Step 3: Run**

Run: `cargo test -p zerodb --test multi_process` → all pass. Run it 5 times in a row (`for i in 1 2 3 4 5; do cargo test -p zerodb --test multi_process || break; done`) on macOS, and again on the bench server.

- [ ] **Step 4: Commit**

```bash
git add crates/zerodb/tests/multi_process.rs crates/zerodb-io/src/lockfile.rs
git commit -m "tests: reap a SIGKILLed reader process; writer lock released on another thread admits another process"
```

---

### Task 8: Differential tests: dead reader and dead writer, ZeroDB vs LMDB

**Files:**
- Create: `crates/zerodb-oracle/tests/multi_process_differential.rs`

**Interfaces:**
- Consumes:
  - Task 2's `Lmdb` FFI helper shape;
  - zerodb's `EnvOpenOptions::lock_file`, `Env::clear_stale_readers` and `Env::reader_list`.

Copy the `Lmdb` struct and its helpers from Task 2 into this file; test files do not share code. Extend the child roles to both engines: `zdb_hold_readers:<n>`, `zdb_hold_writer`, `lmdb_hold_readers:<n>`, `lmdb_hold_writer`.

- [ ] **Step 1: Write the tests**

```rust
/// One dead reader process holding 2 read txns: after the kill, both
/// engines list it, `clear_stale_readers` returns the same count, and a
/// second call returns 0.
#[test]
fn killed_reader_then_clear_stale_readers_matches_lmdb() {
    let lmdb = {
        let dir = TempDir::new().unwrap();
        let env = Lmdb::open(dir.path(), 8);
        env.commit_one();
        let pid = kill_child("lmdb_hold_readers:2", dir.path());
        let listed = env.reader_pids().iter().filter(|&&p| p == pid).count();
        (listed, env.reader_check(), env.reader_check())
    };
    let zdb = {
        let dir = TempDir::new().unwrap();
        let env = zdb_open(dir.path());
        zdb_commit_one(&env);
        let pid = kill_child("zdb_hold_readers:2", dir.path());
        let listed = env.reader_list().iter().filter(|e| e.pid == Some(pid)).count();
        (
            listed,
            env.clear_stale_readers().unwrap() as i32,
            env.clear_stale_readers().unwrap() as i32,
        )
    };
    assert_eq!(zdb, lmdb, "(listed, first check, second check)");
}

/// Linux only (the fork's macOS writer mutex is not robust): a SIGKILLed
/// writer's lock is taken over, its uncommitted write is absent, and a dead
/// reader is reaped or not exactly as LMDB does at takeover.
#[cfg(target_os = "linux")]
#[test]
fn killed_writer_takeover_matches_lmdb() {
    let run_lmdb = || {
        let dir = TempDir::new().unwrap();
        let env = Lmdb::open(dir.path(), 8);
        env.commit_one();
        let rpid = kill_child("lmdb_hold_readers:1", dir.path());
        kill_child("lmdb_hold_writer", dir.path());
        env.commit_one();
        (env.reader_pids().contains(&rpid), env.has_key(b"doomed"))
    };
    let run_zdb = || {
        let dir = TempDir::new().unwrap();
        let env = zdb_open(dir.path());
        zdb_commit_one(&env);
        let rpid = kill_child("zdb_hold_readers:1", dir.path());
        kill_child("zdb_hold_writer", dir.path());
        zdb_commit_one(&env);
        let r = env.read_txn().unwrap();
        (
            env.reader_list().iter().any(|e| e.pid == Some(rpid)),
            env.main_database().get(&r, b"doomed").unwrap().is_some(),
        )
    };
    assert_eq!(run_zdb(), run_lmdb(), "(dead reader still listed, doomed key present)");
}
```

Helpers to write in the file:
- `kill_child(role, dir) -> u32`: spawn, wait for the `held` marker, SIGKILL, wait, remove the marker, return the PID.
- `zdb_open(dir)`: `zerodb::EnvOpenOptions` with `map_size(1 << 20)`, `max_readers(8)`, `lock_file(true)`.
- `zdb_commit_one(env)`: puts `k` → `v`.
- `Lmdb::has_key(&self, k) -> bool`: an FFI `mdb_get` in a read txn.
- In the `*_hold_writer` roles, the child puts `doomed` before signaling.

- [ ] **Step 2: Run on macOS and on the bench server**

Run: `cargo test -p zerodb-oracle --test multi_process_differential -- --test-threads=1`
Expected: pass on both. A failure is a ZeroDB bug unless it's recorded in `docs/DIVERGENCES.md` (rule 1). Report it to the controller; do not edit an expectation.

- [ ] **Step 3: Commit**

```bash
git add crates/zerodb-oracle/tests/multi_process_differential.rs
git commit -m "oracle: multi-process differential for dead readers and dead writers (ADR-0023)"
```

---

### Task 9: Crash harness: kill a writer mid-commit while a reader process is live

**Files:**
- Create: `crates/zerodb-oracle/src/crash/multiproc.rs`
- Modify: `crates/zerodb-oracle/src/crash/mod.rs` (`pub mod multiproc;`)
- Modify: `crates/zerodb-oracle/src/crash/sigkill.rs` (`child_run` takes a `lock_file: bool`)
- Modify: `crates/zerodb-oracle/src/bin/crash-harness.rs`:
  - `--mechanism mp`;
  - hidden `--crash-child-lock`;
  - hidden `--crash-reader`.
- Modify: `justfile`: add `crash-test-mp`, and add 50 `mp` cycles to `crash-test-quick`.

**Interfaces:**
- Consumes: `sigkill::child_run`, `read_sidecar`, the replay `Exec`, `compare_env_world`, `check_image`, `gen_spec` and `MAX_ROUNDS`. Make the sigkill private helpers `pub(crate)` where needed (`read_sidecar`, `Kill`, `kill_arg`, `Sidecar`).
- Produces: `pub fn run_mp_cycle(seed: u64, opts: &SigkillOpts) -> CutReport`.

**The cycle:**
1. Create a temp dir. Spawn the **writer** child, which is `crash-harness --crash-child-lock <seed> <dir> <kill>`. This is `child_run` with `lock_file(true)` added to its options. When `lock_file` is on, `child_run` does two things after its env opens and before the workload. It writes `writer-open`. Then it waits up to 30 s for `reader-up`, and on a timeout it marks `ABANDON no reader` and exits 0. Without this wait, a hook kill at commit 1 could fire before any reader exists. The parent waits for `writer-open`. If the writer exits first (killed during env creation), it treats the cycle as the creation window, as sigkill does.
2. Spawn the **reader** child, `crash-harness --crash-reader <dir> <page_size>`. It:
   - opens with `lock_file(true)` and the spec's page size and map size;
   - writes `reader-up`;
   - loops until `reader-stop` exists. Each iteration begins a read txn, walks every named database and the main database with a cursor, and checks that iteration succeeds and that keys are strictly ascending.

   On any error it writes `reader-fail <msg>` and exits 1. When stopped it exits 0.
3. Kill the writer as `run_sigkill_cycle` does: a hook abort, or an async SIGKILL whose delay starts once `reader-up` exists.
4. With the reader still alive, the **parent** opens the env with `lock_file(true)`, as a joiner. Let `r = env.txnid()`. The legal set differs from single-process recovery, because the joiner seeds from the published `T`, which the dead writer stores after H4:
   - `Kill::Hook { commit, point }` → `r == commit - 1` for every point 0..=4.
   - `Kill::AsyncMs(_)` → `acked <= r && r <= acked + 1`.
   - `side.done` → `r == acked`.
5. `compare_env_world(&env, world_at_r)` must pass.
6. The parent commits one marker write txn: put `__mp_marker` into the main database. That txn takes over the dead writer's lock (Linux futex; on macOS the kernel released it) and overwrites any orphaned meta. Read the marker back.
7. Write `reader-stop`. The reader must exit 0.
8. Drop the parent env. Check the image with `check_image`. Then reopen without a lock file and assert that `txnid() == r + 1` and that the marker is present.

- [ ] **Step 1: Write `multiproc.rs`**

Implement `run_mp_cycle` following steps 1–8. Reuse `run_sigkill_cycle`'s structure verbatim where it applies:
- the kill choice from the seeded `Rng`;
- the sidecar read;
- the drift and abandon checks;
- the replay `Exec`;
- the `violation` closure, with the tag `[mp]` and the repro hint `crash-harness --repro mp:{seed}`.

Steps 4 and 8 replace the single-process legal set and reopen.

Add the reader body, `pub fn reader_run(dir: &Path, page_size: u32, map_size: usize) -> !`, as described in step 2. For the database walk, open each named database the workload creates. `gen_spec(seed)` knows the names: reuse whatever accessor `workload.rs` exposes, or walk the main-database catalog entries as names.

The creation window (the writer killed before `writer-open`) is a designed outcome, as in sigkill: count the cycle verified without spawning a reader.

- [ ] **Step 2: Wire the binary**

In `crash-harness.rs`:
- add `Mechanism::Mp`, parsed from `--mechanism mp`;
- add the hidden `--crash-child-lock <seed> <dir> <kill>`, which calls `child_run(seed, dir, kill, true)`;
- add the hidden `--crash-reader <dir> <page_size> <map_size>`, which calls `multiproc::reader_run`;
- accept `mp:SEED` in `--repro`;
- in the dispatch, `Mechanism::Mp` runs `run_mp_cycle` for each cycle seed.

`--mechanism both` is unchanged; `mp` is opt-in. Existing `child_run` call sites pass `false`.

- [ ] **Step 3: justfile**

```make
# Multi-process crash cycles: a writer killed mid-commit while a reader
# process stays live (ADR-0023).
crash-test-mp CYCLES='500':
    cargo run -p zerodb-oracle --bin crash-harness -- --mechanism mp --cycles {{CYCLES}}
```

And extend `crash-test-quick`:

```make
crash-test-quick:
    cargo run -p zerodb-oracle --bin crash-harness -- --cycles 200
    cargo run -p zerodb-oracle --bin crash-harness -- --mechanism mp --cycles 50
```

- [ ] **Step 4: Run**

Run: `just crash-test-mp 500` on macOS, then on the bench server.
Expected: 0 violations. Some cycles may be abandoned; report the count.
Run: `just crash-test-quick` → passes.

- [ ] **Step 5: Commit**

```bash
git add crates/zerodb-oracle/src crates/zerodb-oracle/src/bin/crash-harness.rs justfile
git commit -m "crash harness: mp mechanism, writer killed mid-commit while a reader process is live"
```

---

### Task 10: Two-process stress test

**Files:**
- Create: `crates/zerodb/tests/multi_process_stress.rs`
- Modify: `justfile` (`stress` also runs it)

**The workload:**
- Two peer processes open one env with `lock_file(true)` and `max_readers(64)`: the test process plus one child re-exec.
- Each process runs 3 reader threads and 1 writer thread for `ZERODB_STRESS_SECS` (default 5).
- A third "reaped reader" child is spawned and SIGKILLed every 500 ms while holding 2 read txns.

The writer thread loops write txns until the deadline. Each txn:
- rewrites 1–64 random keys of a 2,000-key space (`k{i:05}`), with value `{key}:{gen}:{pad}`, where `gen` is the txn's generation;
- sets `__sum` to the wrapping sum, over all keys, of `hash(key, value)`. It recomputes the sum incrementally: read the old value, subtract its hash, add the new one.

Each reader thread loops read txns. In each one it:
- walks the whole database;
- recomputes the sum and asserts it equals `__sum` in the same snapshot;
- asserts that every value's key prefix matches its key.

The test process also calls `env.clear_stale_readers()` every 100 ms. It asserts that, at the end, `reader_list()` lists no PID other than the two peers.

A mismatch, panic or engine error in any thread fails the run. The child reports failure through its exit status.

At the end:
- both processes join their threads;
- the child exits 0;
- the parent checks the image with `zerodb::check::check_image` after dropping its env;
- the parent asserts that the total commits from both processes is at least 10. This guards against a run where the writers never interleave.

- [ ] **Step 1: Write the test**

Write `multi_process_stress.rs`. Reuse `multi_process.rs`'s `TempDir`, `ChildGuard`, `child`/`wait_for`/`mark` and child-entry pattern, copied, since test files do not share modules. Use one `#[test] fn stress_two_processes()` and one `#[test] fn child_entry()` with the roles `peer` and `reaped_reader`. Use the `ZERODB_STRESS_SECS` parsing from `reader_stress.rs`.

For the hash, use `std::hash::DefaultHasher`. Its keys are fixed within one Rust version, and both processes run the same binary, so they agree. Use `wrapping_add`/`wrapping_sub` on `u64`.

The peer writes its commit count to `peer-commits` before it exits. The parent reads that file.

- [ ] **Step 2: justfile**

```make
stress:
    ZERODB_STRESS_SECS=180 cargo test -p zerodb --test reader_stress -- --nocapture stress_
    ZERODB_STRESS_SECS=180 cargo test -p zerodb --test multi_process_stress -- --nocapture stress_
```

- [ ] **Step 3: Run**

Run: `cargo test -p zerodb --test multi_process_stress` (5 s) → pass.
Run: `ZERODB_STRESS_SECS=60 cargo test -p zerodb --test multi_process_stress -- --nocapture` on macOS → pass.

- [ ] **Step 4: Commit**

```bash
git add crates/zerodb/tests/multi_process_stress.rs justfile
git commit -m "tests: two-process reader/writer stress with a reaped reader process"
```

---

### Task 11: Full gate, platform runs, perf check, docs

**Files:**
- Modify: `benches/results/2026-10-11-multi-process-stage1.md` (add a "stage 2" section) and `benches/results/perf-ledger.jsonl`
- Modify: the PR #93 body (scratchpad `pr-body.md`, then `gh pr edit 93 --body-file …`)
- Modify: the memory file `adr-0023-stage-carry-forward.md`

- [ ] **Step 1: The AGENTS.md gate on macOS**

Run all of:
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo clippy --workspace --all-targets --target x86_64-unknown-linux-gnu --exclude zerodb-oracle -- -D warnings`
- `cargo test --workspace`
- `cargo +nightly miri test -p zerodb-core`
- `just loom`
- `just fuzz-quick`
- `just crash-test-quick`

Every one must pass.

- [ ] **Step 2: Linux x86-64 on the bench server**

Run natively on `root@62.210.92.143`:
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- `cargo test -p zerodb --test multi_process` 5 times
- `just crash-test-mp 2000`
- `just stress` (180 s each)

- [ ] **Step 3: Linux aarch64 (Graviton)**

If the AWS CLI works (`aws sts get-caller-identity`), start the Graviton host (memory: aws-graviton-bench-host), run Step 2's list there, and stop it afterwards. If the session has expired, ask the user to run `! aws sso login` and wait. If that is not possible, record the run as not done in the PR body. Do not claim aarch64 coverage beyond CI's.

- [ ] **Step 4: Perf check of the release change**

Amendment 7 adds one store to the lock-file release. Run the stage-1 three-sided driver on the bench server (`/srv/zerodb-bench/jobs/t8-mp/job3.sh`; check that it targets this HEAD) for `env/txn/*`, `get/*` and `commit/batch/n1`: LMDB, ZeroDB `main`, and this branch with the lock file off and on.

Append a "Stage 2" section to the report, with the table and one sentence per rung that moved. Append the ledger lines in the same schema as the stage-1 lines, including all rungs. The default path (lock file off) must stay flat against `main`. A regression there is a blocker; report it.

- [ ] **Step 5: PR body and memory**

Update `pr-body.md`:
- stage 2 moves from "in progress" to a description of what it adds: reaping, the release order, own-PID slots, re-election, the slot offset, `reader_list` PIDs, and the observed LMDB reap points;
- its tests: loom L6–L10, oracle observation and differential, the `mp` crash mechanism, and the stress run;
- the platform runs;
- the perf rows;
- "Known limitations" trimmed to what remains for stage 3.

Run `gh pr edit 93 --body-file <scratchpad>/pr-body.md`. Keep the PR a draft unless the user says otherwise.

In `memory/adr-0023-stage-carry-forward.md`, mark stage 2 done, with the date and HEAD, and keep the stage-3 list. Add anything this stage deferred.

- [ ] **Step 6: Commit**

```bash
git add benches/results
git commit -m "bench: ADR-0023 stage 2 perf check (release order) in the report and ledger"
```
