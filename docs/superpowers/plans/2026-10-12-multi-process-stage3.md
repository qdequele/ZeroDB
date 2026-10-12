# Multi-process stage 3 (parity, map growth, docs, default flip) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the ADR-0023 lock file ZeroDB's default, at LMDB parity, with `NO_LOCK` as the opt-out. This includes cross-process map growth done LMDB's way (`MapResized` plus `resize`).

**Architecture:** Stages 1–2 (PR #93, branch `qdequele/adr-0023-multi-process`) built the lock file, the cross-process writer lock, the shared published txnid `T` and reader reaping, all opt-in. Stage 3 works in this order:
- observe every remaining LMDB behavior first (Task 1);
- design the remap mechanism in a spike and write ADR amendments for the user to approve (Task 2, **approval gate**);
- implement map growth (Tasks 3–4);
- close the remaining parity items (Tasks 5–9);
- bring the lock-on cost back to the ADR's stated budget (Task 10);
- update the docs (Task 11), flip the default (Task 12), then run the gate (Task 13).

**Tech Stack:** Rust 2021 workspace. `libc` `fcntl`/`flock`/`futex`, `memmap2`. Differential oracle against C LMDB (`lmdb-master-sys 0.2.6`, Meilisearch's fork), loom, miri, criterion.

**Spec:** `docs/adr/0023-multi-process.md` (§1 `NO_LOCK` and read-only, §5 map growth and per-process state, "Staging" item 3, "Tests", "SPEC and divergences"), plus amendments 7–14. Facts gathered for this plan:
- `scratchpad/stage3-inventory.md`, copied into the SDD workspace at setup;
- the default-flip spike: 763 passed, 10 failed, all 10 tests asserting that no lock file exists.

## Global Constraints

- AGENTS.md rules 1–7 apply. LMDB behavior is observed in `crates/zerodb-oracle`, never guessed. Never weaken, delete or `#[ignore]` a test. The 10 tests that assert "no lock file" change because ADR-0023 §6/"Tests" says so; list each one in the PR body (rule 2).
- `unsafe` stays in the AGENTS.md allowlist. Any new `unsafe` (e.g. a remap) must first be named in an ADR amendment the user approved in Task 2, and AGENTS.md's unsafe policy must be amended in the same change.
- No new dependency.
- Every atomic carries an explicit `Ordering` and a comment; assume ARM.
- **Disk guard:** run every `cargo test` covering more than one test file as `bash -c 'ulimit -f 4194304; …'` (2 GiB per file). A spike once wrote a 512 GiB lock file.
- Perf claims use three columns (LMDB / ZeroDB `main` / ZeroDB after), CGU=1 and 5 interleaved rounds on the bench server (`root@62.210.92.143`, driver `/srv/zerodb-bench/jobs/s2-perf/job.sh`).
- Native lock name `zerodb.lock`; heed lock name `lock.mdb`. These were approved in the ADR's open question 1.
- No `Co-Authored-By` lines. Never push; the user pushes.
- Clippy also on `--target x86_64-unknown-linux-gnu --exclude zerodb-oracle` (macOS clippy skips Linux-only modules).
- Comments and docs are written for an outside reader (ADR numbers and rule IDs are fine).

## Review Focus

1. **A read-only consumer opening while a writer process runs** (meilitool-style: READ_ONLY over a live Meilisearch dir). It must attach as a reader through `lock.mdb` and see committed data, never create a second lock file or truncate anything. Pinned in Task 6 (`tools_attach_to_a_live_env_in_another_process`) and Task 8 (`read_only_env_joins_the_existing_lock_file`).
2. **The data file and the lock file are the same file** (case-insensitive APFS, hard link, or a user-chosen name). The open must fail before anything is truncated. Pinned in Task 5 (`lock_file_that_is_the_data_file_is_refused_without_truncating`).
3. **A process whose map is smaller than the file another process grew.** It must get `MapResized` (not `Invalid`), recover through `resize`, and never read past its map. Pinned in Tasks 3–4.
4. **An app that keeps a read txn open and calls `resize`.** It must get an error, and the txn's borrowed bytes must stay valid. Pinned in Task 4 (`resize_with_a_live_txn_is_refused`).
5. **Many short-lived envs** (the proptests and fuzz open one env per case). With the default flipped they must not leak lock files, descriptors or POSIX locks. Pinned in Task 12 (`thousand_sequential_envs_leak_no_descriptors`).

---

### Task 1: Observe the remaining LMDB behaviors (oracle, two processes)

The riskiest assumptions are what LMDB does at map growth and at the other parity points. Every later task's expected values come from here (rule 1).

**Files:**
- Create: `crates/zerodb-oracle/tests/lmdb_stage3_observation.rs`
- Reference pattern: `crates/zerodb-oracle/tests/lmdb_reaping_observation.rs` (FFI, self-re-exec child, `OBS_*` constants pinned and asserted)

**Interfaces:**
- Produces: `OBS_*` constants that later tasks copy as their expected values:
  - `OBS_READ_BEGIN_SMALL_MAP: i32`: the rc of `mdb_txn_begin(RDONLY)` in process A (map 1 MiB) after process B (map 8 MiB) committed past A's map. Expected guess `MDB_MAP_RESIZED`, to be observed.
  - `OBS_WRITE_BEGIN_SMALL_MAP: i32`: the same for a write txn.
  - `OBS_BIGGER_META_MAPSIZE_ALONE_RESIZES: bool`: does a begin fail when only the meta's `mm_mapsize` is larger and `last_pg` still fits A's map? The guess from reading `mdb_txn_renew0` is false: only `next_pgno > maxpg` triggers it.
  - `OBS_SET_MAPSIZE_ZERO_ADOPTS: u64`: A's `mdb_env_info().me_mapsize` after `mdb_env_set_mapsize(env, 0)` with no txn open.
  - `OBS_SET_MAPSIZE_WITH_WRITE_TXN: i32`: rc of `mdb_env_set_mapsize` while A holds a write txn. Guess `EINVAL`.
  - `OBS_COMMIT_PERSISTS_OWN_MAPSIZE: bool`: after A (1 MiB, resized to 8 MiB) and B (8 MiB) alternate, does a small commit by a process opened with 2 MiB write 2 MiB into the meta?
  - `OBS_OPEN_SMALLER_THAN_META: u64`: `me_mapsize` of a process opening with 1 MiB a file whose meta says 8 MiB.
  - `OBS_PREV_SNAPSHOT_JOINER: i32`: rc of opening with `MDB_PREVSNAPSHOT` while another process has the env open. Guess `EAGAIN`.
  - `OBS_RDONLY_EXISTING_LOCK: i32`: rc of an `MDB_RDONLY` open while a writer process holds the env.
  - `OBS_RDONLY_LOCK_UNWRITABLE: i32`: rc of an `MDB_RDONLY` open when `lock.mdb` exists but is mode 0444 (skip and record -1 when running as root).
  - `OBS_NOLOCK_CREATES_LOCK_FILE: bool`: does `lock.mdb` appear after a `MDB_NOLOCK` open of a fresh dir?
  - `OBS_INFO_SEES_OTHER_COMMIT: bool`: after B commits, does A's `mdb_env_info().me_last_txnid` show B's txnid with no txn begun in A?
  - `OBS_STAT_SEES_OTHER_COMMIT: bool`: the same for `mdb_env_stat().ms_entries`.

- [ ] **Step 1:** Write one `#[test]` per constant, using the child pattern from `lmdb_reaping_observation.rs`. Each test asserts the value it observes equals its `OBS_*` constant. Start every constant at the guess above.
- [ ] **Step 2:** Run with `cargo test -p zerodb-oracle --test lmdb_stage3_observation -- --test-threads=1` on macOS. Where a guess is wrong, change the constant to the observed value (this is the observation itself, not weakening a test) and record each change in the report.
- [ ] **Step 3:** Run the same on the bench server (Linux x86-64) with the sync script. The constants must match on both. If they differ, use `cfg(target_os)` constants and say so in the report.
- [ ] **Step 4:** Commit `oracle: observe LMDB map growth, PREV_SNAPSHOT, read-only, NOLOCK and cross-process info (ADR-0023 stage 3)`.

### Task 2: Remap design spike and ADR amendments 15–18 (APPROVAL GATE)

**Files:**
- Modify: `docs/adr/0023-multi-process.md` (append "Amendments (stage-3 planning, 2026-10-12)", items 15–18)
- Spike code: in a throwaway worktree, never committed. The numbers go into the amendment.

**Interfaces:**
- Consumes: the Task 1 constants.
- Produces, as ADR text that Tasks 3–12 implement:
  - **15. Map growth.** The begin check (per Task 1), the `MapResized` error, and the engine's `Env::resize(new_size: u64) -> Result<()>` semantics (0 = adopt the meta's size; round up to `(last_pg+1)*page_size`; refused while any txn of this env is live). Also the adapter's `unsafe fn resize(&self, new_size: usize) -> heed::Result<()>`, mirroring heed 0.22.1 `env.rs:647`, and the **remap mechanism**. The spike measures at least these two candidates, with `ro_begin_abort` and `rw_empty_commit` on the bench server, lock on:
    - (a) the backing behind a lock-free generation flag checked in the existing publish-and-verify begin, with remap only after the writer lock is held and no pinned in-process slot is found;
    - (b) an `Arc` of the mapping cloned per txn.

    It picks the cheaper one that keeps every borrowed `&[u8]` valid, and names any new `unsafe` and where it lives.
  - **16. Lock-file name derivation.** `zerodb.dat` → `zerodb.lock`, `data.mdb` → `lock.mdb`, any other data name → `<data_file_name>-lock` (LMDB's `MDB_NOSUBDIR` convention). An explicit `lock_file_name` still overrides. Before the initializer truncates, the lock file's `(dev, ino)` is compared with the data file's; if they are equal, the open fails with `Io(InvalidInput)`.
  - **17. `NO_LOCK` and read-only.** The engine's opt-out is `lock_file(false)` (default `true`). heed's `NO_LOCK` maps to it, and the adapter's `Env::flags()` reports it. The adapter's stage-1 `lock_file()` setter is removed (it never shipped). READ_ONLY behavior follows `OBS_RDONLY_*`.
  - **18. Tools attach.** stat/dump/check open as ordinary READ_ONLY readers through the derived lock name. The data-file `flock` in `zerodb-tools/src/lock.rs` is removed for the read tools. load and migrate keep refusing a non-empty destination.
- [ ] **Step 1:** Spike both remap candidates (opus) and record per-begin ns, lock on and off, plus a correctness sketch of each.
- [ ] **Step 2:** Write amendments 15–18 with the numbers.
- [ ] **Step 3:** Commit `docs: ADR-0023 amendments 15–18 (map growth, lock name, NO_LOCK, tools) for approval`.
- [ ] **Step 4: STOP.** The controller asks the user to approve amendments 15–18 (AGENTS.md rule 5). After approval, the controller revises Tasks 3–4 of this plan with the chosen mechanism's exact code and commits the revision before dispatching Task 3.

### Task 3: `MapResized` at txn begin (after approval)

**Files:**
- Modify: `crates/zerodb-core/src/error.rs` (add `MdbError::MapResized`, code `-30785`, LMDB's `MDB_MAP_RESIZED`); `crates/zerodb-core/src/env.rs` `check_snapshot_backed` (~2219–2240). Return `MapResized` where Task 1 says LMDB does, and keep `Invalid` when the *file* is shorter than `last_pg` (corruption).
- Modify: `crates/heed-zerodb/src/error.rs` (map the engine's `MapResized` to the existing adapter variant at :117).
- Test: `crates/zerodb/tests/multi_process.rs`, and `crates/zerodb-oracle/tests/multi_process_differential.rs` (a tuple on both engines).

- [ ] **Step 1:** Write `small_map_process_gets_map_resized_after_a_bigger_process_grows_the_file`. The child opens with 8 MiB and writes 4 MiB of values; the parent was opened with 1 MiB before that. The parent's `read_txn()` returns `Err(Error::Mdb(MdbError::MapResized))` if `OBS_READ_BEGIN_SMALL_MAP` is `MDB_MAP_RESIZED`; `write_txn()` follows `OBS_WRITE_BEGIN_SMALL_MAP`.
- [ ] **Step 2:** Run it and check it fails with `Invalid`.
- [ ] **Step 3:** Implement per amendment 15.
- [ ] **Step 4:** Add the differential tuple `(read_begin_rc, write_begin_rc)`, equal on both engines.
- [ ] **Step 5:** Commit.

### Task 4: `Env::resize` and the adapter's `unsafe fn resize` (after approval; exact code added by the Task 2 revision)

**Files:**
- `crates/zerodb-core/src/env.rs`, `crates/zerodb-io/src/{lib,mmap}.rs`, `crates/zerodb/src/lib.rs`, `crates/heed-zerodb/src/env.rs`, AGENTS.md (if amendment 15 adds `unsafe`), `docs/COMPATIBILITY.md:27,79,152`.
- Tests:
  - `resize_zero_adopts_the_grown_map_and_reads_succeed` (two processes);
  - `resize_with_a_live_txn_is_refused` (single process; a read txn held across the call keeps its value readable afterwards);
  - `resize_rounds_up_to_the_last_page` (per LMDB's silent round-up);
  - the adapter `unsafe fn resize` rejects a size that is not a multiple of the OS page size with `Io(InvalidInput)` (heed 0.22.1 does the same);
  - a differential tuple for `OBS_SET_MAPSIZE_ZERO_ADOPTS` and `OBS_SET_MAPSIZE_WITH_WRITE_TXN`;
  - a loom model if amendment 15 picks a lock-free generation flag (resize racing a reader begin: either the reader fails its begin or the remap waits/fails; never both proceed).
- [ ] Steps: RED test, implement per amendment 15, GREEN, miri on any non-mmap logic, loom, commit.

### Task 5: Lock-file name derivation and same-file guard

**Files:**
- Modify: `crates/zerodb/src/lib.rs`:
  - change `lock_file_name: OsString` (:204, default :230) to `Option<OsString>` (`None` = derived);
  - `get_lock_file_name()` returns the derived name;
  - the validation at :509–531 compares against the derived name.
- Modify: `crates/zerodb-io/src/lockfile.rs` `LockFile::open`: take the data file's path; before `set_len(0)` (:341), `fstat` both and fail with `LockOpenError::Io(InvalidInput)` if `(dev, ino)` match.
- Modify: `crates/heed-zerodb/src/env.rs:314–316` (stop setting `lock.mdb` explicitly; derivation gives it).
- Test: `crates/zerodb/tests/lock_file_single_process.rs`.

**Interfaces:**
- Produces: `pub fn derived_lock_file_name(data_file_name: &OsStr) -> OsString` in `crates/zerodb/src/lib.rs`, used by zerodb-tools in Task 6.

- [ ] **Step 1:** Write the failing tests:

```rust
#[test]
fn lock_file_name_is_derived_from_the_data_file_name() {
    assert_eq!(zerodb::derived_lock_file_name("zerodb.dat".as_ref()), "zerodb.lock");
    assert_eq!(zerodb::derived_lock_file_name("data.mdb".as_ref()), "lock.mdb");
    assert_eq!(zerodb::derived_lock_file_name("index.db".as_ref()), "index.db-lock");
}

#[test]
fn two_native_envs_with_different_data_names_get_different_lock_files() {
    let dir = TempDir::new();
    let a = opts().data_file_name("a.db").open(dir.path()).expect("open a");
    let b = opts().data_file_name("b.db").open(dir.path()).expect("open b");
    assert!(dir.path().join("a.db-lock").exists());
    assert!(dir.path().join("b.db-lock").exists());
    drop((a, b));
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
    let len = std::fs::metadata(dir.path().join("data.mdb")).unwrap().len();
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
    assert_eq!(std::fs::metadata(dir.path().join("data.mdb")).unwrap().len(), len);
}
```

  In the hard-link test, the first open leaves a derived `lock.mdb` behind. That's fine: the second open names `alias` explicitly, and `alias` is the data file's inode.
- [ ] **Step 2:** Run them and check they fail.
- [ ] **Step 3:** Implement. The `(dev, ino)` check needs the data file to exist. When it doesn't exist yet, it cannot be the lock file, so skip the check.
- [ ] **Step 4:** Run `cargo test -p zerodb --test lock_file_single_process --test multi_process` and `-p zerodb-io`.
- [ ] **Step 5:** Commit `zerodb: derive the lock-file name from the data-file name and refuse a lock file that is the data file (ADR-0023 amendment 16)`.

### Task 6: zerodb-tools attach to a live environment

**Files:**
- Modify: `crates/zerodb-tools/src/{lock,common,commands,lib,migrate}.rs`. Read tools stop taking the data-file `flock` and open with the lock file on, using `zerodb::derived_lock_file_name`. load/migrate keep their empty-destination refusal and drop the `flock` only if amendment 18 says so.
- Modify: `docs/TOOLS.md`; `AGENTS.md` (the `zerodb-tools` `libc::flock` bullet, removed or narrowed to what remains).
- Rewrite: `crates/zerodb-tools/tests/live_env_refusal.rs::tools_refuse_a_locked_env`. Rename it to `tools_attach_to_a_live_env_in_another_process` and call out the rule-2 change in the PR. `tools_refuse_a_missing_env` stays.

- [ ] **Step 1:** Write the new test. A child process (self-re-exec via an env var, like `crates/zerodb/tests/multi_process.rs:81 child`) opens the env through the heed adapter (`data.mdb`), commits key `live`, signals readiness with a marker file, and holds a read txn open. The parent runs `stat`, `dump` and `check` and asserts they succeed, that `dump` contains `live`, and that the directory holds only `data.mdb` and `lock.mdb`.
- [ ] **Step 2:** Run it and check it fails with the "locked" error.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `cargo test -p zerodb-tools` under the disk guard.
- [ ] **Step 5:** Commit `zerodb-tools: attach to a live environment through its lock file (ADR-0023 amendment 18)`.

### Task 7: `PREV_SNAPSHOT` in a second process

**Files:**
- Modify: `crates/zerodb/src/lib.rs` open (after `LockFile::open`): if `PREV_SNAPSHOT` is set and this process did not win the election (`LockFile::is_initializer()`), drop the lock file and return the error from `OBS_PREV_SNAPSHOT_JOINER`. If it is `EAGAIN`: `Error::Io(io::Error::from_raw_os_error(libc::EAGAIN))`, or the engine's existing mapping of an LMDB errno; follow how `flag_semantics.rs` maps EAGAIN today.
- Test: `crates/zerodb/tests/multi_process.rs::prev_snapshot_open_fails_while_another_process_has_the_env`, plus a differential tuple.
- Docs: SPEC 01 §S5, SPEC 04 TXN-66 (in Task 11).
- [ ] Steps: RED, implement, GREEN, commit `zerodb: PREV_SNAPSHOT requires exclusive use of the lock file, as in LMDB`.

### Task 8: `NO_LOCK`, read-only, and heed adapter flags

**Files:**
- Modify: `crates/heed-zerodb/src/env.rs`:
  - remove the `lock_file` field and setter (:81, 104, 121, 217);
  - pass `opts.lock_file(!self.flags.contains(EnvFlags::NO_LOCK))`;
  - remove `NO_LOCK` from the "dropped" comment (:326);
  - make `Env::flags()` (:407–430) report `NO_LOCK` when the env was opened with it.
- Modify: `crates/zerodb-io/src/lockfile.rs:270–275` only if `OBS_RDONLY_*` shows LMDB opening the lock file differently for RDONLY.
- Test:
  - `crates/heed-zerodb/tests/env_file_naming.rs`: `no_lock_creates_no_lock_file`, `flags_report_no_lock`;
  - `crates/zerodb/tests/multi_process.rs::read_only_env_joins_the_existing_lock_file`. A child writer holds the env; the parent opens READ_ONLY, reads the child's commit, and `reader_list` shows the parent's PID.
- [ ] Steps: RED, implement, GREEN, commit `heed-zerodb: honor NO_LOCK; read-only envs join the lock file (ADR-0023 amendment 17)`.

### Task 9: Cross-process `info()`/`stat()` and the joiner's `meta()`

**Files:**
- Modify: `crates/zerodb-core/src/env.rs` `info()` (:1411) and `stat()` (:1431). When a shared region exists, read from the snapshot at the published `T` (`current_snapshot()`) if `OBS_INFO_SEES_OTHER_COMMIT` / `OBS_STAT_SEES_OTHER_COMMIT` are true; the signatures stay infallible. If `current_snapshot()` fails (e.g. `MapResized`), fall back to the cached snapshot and document it.
- Audit `EnvInner::meta()` (:701) callers; none may read roots or txnids from it in a joiner. Fix any that do, and rename it or document it as "the meta selected at open, possibly unpublished in a joiner".
- Test: `crates/zerodb/tests/multi_process.rs::info_and_stat_see_another_processes_commit`, plus a differential tuple.
- [ ] Steps: RED, implement, GREEN, commit.

### Task 10: Close the lock-on cost to the ADR's budget

The ADR's "Costs and risks" accepts one shared load and one slot store per txn begin, plus one uncontended CAS per write txn. Stage 1 measured +15 ns (read) and +20 ns (empty write) for that; stage 2 is at +17 ns and +45 ns.

**Files:** profile-driven, expected in `crates/zerodb-core/src/{env,readers}.rs`, `crates/zerodb-io/src/lockfile.rs`.

- [ ] **Step 1:** On the bench server, `perf record` the `env/txn/rw_empty_commit` rung, lock off vs lock on, at this branch's HEAD. Attribute the extra cycles (the three-store release, the owner-word claim, the takeover check, the T publish).
- [ ] **Step 2:** Remove what is not in the ADR's budget. Each change copies LMDB's technique or states why (AGENTS rule 7).
- [ ] **Step 3:** Run the three-column bench (LMDB / `main` / HEAD), lock on and lock off. **Target:** lock-on overhead vs `main` at or below stage 1's (+15 ns `ro_begin_abort`, +20 ns `rw_empty_commit`), within bench-ab noise; lock off flat. If the target can't be met, the report states what remains and why. The controller records a ruling, and the user sees it before the flip.
- [ ] **Step 4:** Append a "Stage 3" section to `benches/results/2026-10-11-multi-process-stage1.md` and two lines to `perf-ledger.jsonl`. Commit.

### Task 11: SPEC, DIVERGENCES, COMPATIBILITY, CHANGELOG, ADR reconciliation

**Files and edits** (the line numbers are from the inventory at a6aef66; never renumber rule IDs, and retire rules explicitly):
- SPEC 04:
  - the header (:20) and §0 (:43): multi-process with a lock file by default; `NO_LOCK` keeps the single-process path;
  - the §2 title and TXN-6 (:91–96): the two-layer writer lock;
  - TXN-7 (:114): the cross-process wait;
  - TXN-9 (:122): a note on shared `T`;
  - the §4 intro (:172–176), TXN-17/19 (shared `T`);
  - TXN-18 (:280, meta re-read at `T`);
  - TXN-38 (`MapResized`), TXN-45b (validated-pages cache addendum), TXN-50/51/53 (lock region, registry, lock-fd teardown), TXN-56 (:984, base rebuilt at `T`), TXN-66 (§10, :1059, Task 7).
- SPEC 00: row 7 (:73); the `clear_stale_readers` row (:140); `NO_LOCK` moved out of the WON'T flags (:156); a `resize` row.
- SPEC 01: the `MDB_NOLOCK` row (:64) from WON'T to MUST; §S5 (:279–281); :447.
- SPEC 02 (:687–688): "a lock file is created unless `NO_LOCK`"; §8 naming per amendment 16.
- SPEC 06: a REC-1a (:48–60) addendum for the per-snapshot check and `MapResized`.
- New lock-file invariants get fresh IDs (`LCK-1…`) in a new SPEC 04 section: election, magic Release/Acquire, slot layout and owner word, the three-store release, the reap invariants A and B, and name derivation.
- DIVERGENCES: retire D-001; update D-012 (:25); D-016 (:29) no longer lists `NO_LOCK` as a no-op.
- `docs/COMPATIBILITY.md` :22, 27, 65, 77, 79, 144, 148, 152.
- CHANGELOG: an Unreleased entry covering:
  - multi-process by default, `NO_LOCK`, and `resize`/`MapResized`;
  - `clear_stale_readers` returns a count;
  - `ReaderEntry::pid`, a new public field on an exhaustive struct;
  - the lock-file name derivation;
  - tools attaching to live envs;
  - the limits checked before the lock file.
- The ADR-0023 §3 body reconciled with amendments 7–14 (release order; slots at 256 + i·128); amendment 7's leak cost restated; amendment 9's sentence on why CLAIMED can't come first; status "Accepted, implemented".
- ADR-0006 D1: `ReaderSlot`, not CachePadded.
- Stale notes:
  - `stamps.rs` "single process";
  - the `zerodb-core/src/error.rs` `ReadersFull` doc;
  - `env.rs:1479–1500`;
  - `heed-zerodb/src/env.rs:35`;
  - `zerodb/src/lib.rs:78` and :400–404 ("off by default");
  - `zerodb-tools/src/lib.rs:16,169`.
- Parked doc nits:
  - the `registry_key` race doc;
  - the `LockFile` struct doc "path";
  - the readers.rs `reap_dead` line wrap;
  - `observe_write_commit`'s "begin and commit together" comment.
- Documented limitations: `fork()` after open, PID namespaces, macOS writer takeover not observed, and `Env::sync` taking only the in-process lock (if Task 1 shows parity).
- [ ] Steps: edit, then run `cargo test --doc --workspace` and a link check (`grep -rn "D-001" docs crates` shows only historical CHANGELOG lines). Commit `docs: ADR-0023 stage 3 SPEC, DIVERGENCES, COMPATIBILITY and CHANGELOG`.

### Task 12: Flip the default, plus stage-2 test debt

**Files:**
- `crates/zerodb/src/lib.rs:229` (`lock_file: true`) and its docs.
- Update, per ADR "Tests" and rule 2 (each listed in the PR body):
  - `heed-zerodb/tests/env_file_naming.rs`, 4 tests: the expected listing becomes `["data.mdb", "lock.mdb"]`;
  - `zerodb/tests/data_file_naming.rs`, 4 tests: plus the derived lock name;
  - `zerodb/tests/lock_file_single_process.rs::default_env_creates_no_lock_file`: rename to `default_env_creates_the_lock_file`, and add `lock_file_false_creates_no_lock_file`;
  - `zerodb-oracle/tests/env_lifecycle_differential.rs:298`: `["data.mdb", "lock.mdb"]` on both engines.
- `crates/zerodb-oracle/benches/engine_comparison/backend.rs:723`: the "no lock" column sets `lock_file(false)` explicitly; the default column is now lock on.
- New test, `crates/zerodb/tests/lock_file_single_process.rs::thousand_sequential_envs_leak_no_descriptors`. Open and drop 1,000 envs in fresh dirs. Afterwards the count of `/dev/fd` entries (macOS) or `/proc/self/fd` entries (Linux) is within 8 of the count before.
- Stage-2 test debt:
  - `init_and_hang` exits when its parent dies (poll `getppid()`);
  - the three-way election test confirms the joiners blocked before the kill;
  - the "child open failed" message is fixed on a `wait_for` timeout;
  - the 4× heuristic in `reaped_slot_lets_the_writer_reuse_pages` is replaced with an exact free-page assertion;
  - the 300 ms negative check in `writer_lock_released_on_another_thread…` is made robust (a marker the child writes after it starts waiting);
  - the mp crash harness reader checks values, and mp cycles get their own summary label;
  - `tmp()` cleans up; the max_readers child uses a `ChildGuard`;
  - tests for the `lock_writer_shared` error path, and for BadMagic/BadVersion → retry;
  - a `debug_assert` in `EnvInner::drop` that the region `Arc` strong count is 1;
  - remove the unused `crossbeam-utils` workspace dep if it is still unused.
- [ ] Steps: the flip, then update the tests, then `cargo test --workspace` under the disk guard (expect 0 failures), then commit in 2–3 logical commits.

### Task 13: Gate, platforms, perf

- [ ] **macOS:** fmt, clippy (host and Linux target), `cargo test --workspace` (disk guard), miri, `just loom`, `just fuzz-quick`, `just crash-test-quick`, `just crash-test-mp 500`.
- [ ] **Bench server, Linux x86-64:** clippy, workspace tests, `multi_process` ×5, `just stress`, `crash-test-mp 2000`, and the Task 1 observation test.
- [ ] **Linux aarch64:** a Graviton host if the user provides one; otherwise CI only, stated in the PR.
- [ ] **Consumer gate:** `scripts/consumer.sh` suites (milli, hannoy) with the flipped default.
- [ ] **Perf:** the full three-column ladder, default (lock on) and `NO_LOCK`, in the report and ledger.
- [ ] **PR:** the stacked PR body (base `qdequele/adr-0023-multi-process`). It lists the rule-2 test changes, the perf, the platforms and the limitations. The user pushes.
