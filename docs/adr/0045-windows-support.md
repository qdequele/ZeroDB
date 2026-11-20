# ADR-0045: Windows support

- Status: Draft, awaiting approval (no Windows code is written; the crates
  stop at a `compile_error!` on any non-Unix target until this is approved
  and implemented)
- Related issue: #183
- Date: 2026-11-20

## Context

Meilisearch publishes binaries for Linux, macOS and Windows, and heed links
LMDB on all three. ZeroDB runs on Unix only (the platform table in
`docs/USING.md#platform-support`), so it cannot replace LMDB in a Meilisearch
release until Windows works. It also leaves the oracle blind to Windows: LMDB
uses different primitives there, and its behavior is observed only on Linux
and macOS today.

This ADR is about correctness-critical code: the commit's durability barrier
(SPEC 06), the cross-process writer lock and reader table (SPEC 04 §12,
ADR-0023) and the map (SPEC 02 §8). CONTRIBUTING rule 5 requires approval
before any of it is written.

### What ZeroDB assumes today

Every Unix-only dependency of the shipped crates, by area. A Windows build
fails on each of these (about sixty errors in `zerodb-io` alone, which the
`compile_error!` in `zerodb-core` now hides behind one message).

| Area | Today (Unix) | Where |
|---|---|---|
| Read map | `mmap(PROT_READ, MAP_SHARED)` of the logical map size, **longer than the file**: the file grows under the map by `pwrite`, and pages past the end are never touched (SPEC 06 REC-1a). A resize adds a longer mapping beside the old ones (`MapRegions`, ADR-0023 amendment 15) | `zerodb-io::mmap`, `lib.rs` (`grow_map`) |
| Writable map (`WRITE_MAP`) | `mmap(PROT_READ\|PROT_WRITE)`; the writer extends the file to the map size before storing through it (SPEC 04 §6.4); the file never shrinks (D-024) | `zerodb-io::mmap`, `lib.rs` |
| Commit writes | `pwrite`, and `pwritev` for runs of pages | `zerodb-io::file` |
| Durability barrier | `File::sync_data` (`fdatasync`; `F_FULLFSYNC` on macOS); under `WRITE_MAP`, ranged `msync` then `sync_data` (SPEC 06 REC-12) | `zerodb-io::lib` |
| Lock file | Mapped `MAP_SHARED`; atomics over the mapping | `zerodb-io::lockfile` |
| First-opener election | `fcntl` exclusive lock on byte 0, downgraded to shared | `lockfile` (LCK-1, LCK-2) |
| Cross-process writer lock | Linux: a PID word with a futex; macOS: `fcntl` lock on byte 1 | `lockfile` (LCK-4) |
| Liveness | `fcntl` lock on byte `2^32 + pid`, probed with `F_GETLK`; immune to PID reuse | `lockfile` (LCK-5) |
| One descriptor per lock file per process | A registry keyed by `(st_dev, st_ino)`, because closing any descriptor drops all of a process's POSIX locks on the file | `lockfile` (`Registration`) |
| Environment copy | `copy_file_range` (Linux), `fclonefileat` (macOS), `O_NOFOLLOW`, `st_dev` checks, Unix permissions | `zerodb::copy` |
| Tools | `FileExt::read_at`, a best-effort `flock` to refuse a live environment | `zerodb-tools` |
| heed adapter | `sysconf(_SC_PAGESIZE)` for the OS page size | `heed-zerodb::env` |
| Errors | `libc::EAGAIN`, `libc::EROFS` raw OS errors | `zerodb::lib` |

### What Windows changes

- **A file mapping cannot be longer than the file** unless the section is
  created with `SEC_RESERVE` through the native API. `CreateFileMapping` with
  a size above the file length either extends the file to that size (on a
  writable handle) or fails (on a read-only one). ZeroDB's read map is
  deliberately longer than the file, so the mapping design does not port as
  is. memmap2 uses `CreateFileMapping` and has no `SEC_RESERVE` option.
- **A mapped file cannot shrink** (`SetEndOfFile` fails with
  `ERROR_USER_MAPPED_FILE`). ZeroDB never shrinks the file, so this matches.
  Extending a mapped file is allowed.
- **View offsets must be multiples of the allocation granularity** (64 KiB),
  not of the 4 KiB page size. `MapRegions` always maps from offset 0, so
  this matters only for any future partial view.
- **Positioned writes:** `std::os::windows::fs::FileExt::seek_write` writes
  at an offset (it also moves the handle's file pointer, which ZeroDB never
  uses). There is no `pwritev`: `WriteFileGather` needs unbuffered,
  overlapped, sector-aligned I/O. Runs of pages become one write per page,
  or one write from a gathered buffer.
- **Durability:** `File::sync_data` and `sync_all` are `FlushFileBuffers` in
  std, which writes the file's data and metadata and flushes the device
  cache. `FlushViewOfFile` (memmap2's `flush`) writes dirty map pages but,
  like `msync` on macOS, is not a durability barrier by itself; it must be
  followed by `FlushFileBuffers`.
- **Byte-range locks** (`LockFileEx`) belong to a handle in a process, are
  released when the handle closes or the process ends, and can be unlocked
  from any thread of the owning process. Unlike POSIX locks, closing another
  handle to the same file does not drop them. There is no `F_GETLK`: the
  only probe is a non-blocking lock attempt (`LOCKFILE_FAIL_IMMEDIATELY`).
  The locks are mandatory for `ReadFile`/`WriteFile` on the locked range
  (not for mapped access), and the documentation says the release after a
  process dies happens after a delay that "depends upon available system
  resources".
- **Process IDs** are 32-bit and reused quickly, as on Unix. A process
  object stays alive while any handle to it is open.
- **Kernel objects** (mutexes, events, semaphores) can be named and shared
  across processes. A named mutex whose owner dies is reported to the next
  waiter as `WAIT_ABANDONED`, but it must be released by the thread that
  acquired it.

### What LMDB does on Windows

Read from the vendored fork (`lmdb-master-sys 0.2.6`, `mdb.c`, the `_WIN32`
branches); not yet observed by the oracle, which does not run on Windows.

- **Map:** `NtCreateSection(..., SEC_RESERVE, fd)` and
  `NtMapViewOfSection(..., MEM_RESERVE)` from `ntdll`, loaded at runtime with
  `GetProcAddress`. With the default build the file grows incrementally
  under a reserved view of the whole map size; the `MDB_FIXEDSIZE` build
  allocates the file to the map size up front instead.
- **Writes and durability:** data pages are written with overlapped
  `WriteFile` through a second handle opened with `FILE_FLAG_WRITE_THROUGH`,
  so a commit does not call `FlushFileBuffers`; the meta page goes through a
  third write-through handle. `MDB_FDATASYNC` is `FlushFileBuffers` and
  `MDB_MSYNC` is `FlushViewOfFile`, used by `mdb_env_sync`. Under
  `MDB_WRITEMAP`, a synchronous commit still writes pages with `WriteFile`
  rather than flushing the map, "as this is faster than
  FlushViewOfFile/FlushFileBuffers" (the source's comment).
- **Lock file election:** `LockFile` (exclusive) on byte 0, falling back to
  a shared `LockFileEx`; the winner downgrades by taking the shared lock and
  then unlocking the exclusive one.
- **Writer and reader-table mutexes:** named mutexes
  `Global\MDB<hash>w` and `...r`, the hash taken from the lock file's volume
  serial number and file index, with a NULL DACL. An abandoned mutex
  (`WAIT_ABANDONED`) is LMDB's `EOWNERDEAD`: the next writer runs
  `mdb_reader_check` and reaps dead readers, as on Linux.
- **Liveness:** `mdb_reader_pid` takes no lock on Windows. It checks a PID
  with `OpenProcess` and `WaitForSingleObject(h, 0)`, so a recycled PID
  makes a dead reader look alive (its slot is not reaped until the PID is
  free again).

## Options

The options differ in three places: the map, the cross-process writer lock
and the commit's write and barrier primitives. The rest is mechanical:
`LockFileEx` in place of `fcntl` for the election and liveness bytes, file
IDs in place of `(dev, ino)`, `seek_write`/`seek_read` in place of
`pwrite`/`pread`, `GetSystemInfo` in place of `sysconf`.

### Map: how a mapped file grows

#### Option M1 — `SEC_RESERVE` section, as LMDB

Create the section with `NtCreateSection(SEC_RESERVE)` over the data file
and map a view of the whole map size with `MEM_RESERVE`. The file grows by
positioned writes underneath, as on Unix, and `MapRegions` keeps working
unchanged (a resize maps a new, longer view beside the old one).

- Keeps SPEC 02 §8 and REC-1a as they are: one design on every platform.
- Uses `ntdll` entry points that Microsoft documents for drivers
  (`ZwCreateSection`, `ZwMapViewOfSection`), not in the Win32 API. LMDB has
  shipped on them for years, so the risk is known, but they are outside the
  stable surface. Bindings: `windows-sys` exposes them under its `Wdk`
  features, or they are loaded with `GetProcAddress` as LMDB does.
- Unknown: whether writes through a write-through or ordinary handle become
  visible in a `SEC_RESERVE` view of another process without remapping, for
  pages past the file's length at the time the view was made. LMDB's design
  relies on it; ZeroDB's multi-process tests would check it.

#### Option M2 — Remap when the file outgrows the view

Map with `CreateFileMapping` at the current file length (through memmap2),
and add a longer mapping to `MapRegions` whenever a commit grows the file
past it, before the commit publishes `T`. A reader in another process that
sees a `T` past its mapping maps again, as it does today for
`MapResized` (TXN-73).

- Win32 API only, memmap2 keeps doing the mapping.
- A mapping per growth step: a file growing page by page would create one
  per commit. Growth would have to come in chunks (for example, double up to
  a cap), which changes the file length ZeroDB reports and keeps (SPEC 02
  §8, D-024) and needs its own design.
- Every reader's fast path gains a check of `T` against its mapped length.

#### Option M3 — Allocate the file to the map size at open

As LMDB's `MDB_FIXEDSIZE`: extend the file to `map_size` at open (sparse,
with `FSCTL_SET_SPARSE`, so disk blocks are allocated only when written),
then map it whole with `CreateFileMapping`.

- Simplest map code; no growth of a mapped file at all.
- The file's length becomes the map size: a 1 TiB `map_size` shows a 1 TiB
  file. Sparse files depend on NTFS or ReFS, and copying such a file with
  a tool that does not keep holes writes the zeros. `zerodb-tools stat` and
  every size the engine reports from the file length change meaning on one
  platform.
- A larger `map_size` from another process (`Env::resize`) extends the file
  while it is mapped elsewhere, which is allowed.

**Recommendation: M1**, behind a spike first (CONTRIBUTING rule 6): two
processes, one writing pages past the end of the file through a positioned
write, the other reading them through a `SEC_RESERVE` view made before the
growth; then the same under `WRITE_MAP`. If the spike fails, M3, whose cost
is a reporting difference rather than a design change.

### Cross-process writer lock

#### Option W1 — `LockFileEx` on byte 1, as ZeroDB on macOS

An exclusive lock, waited for with a blocking `LockFileEx`, released with
`UnlockFileEx` from any thread.

- Smallest change: the macOS code path with different calls.
- Inherits D-028: the lock reports no previous owner, so a writer that
  follows a dead writer does not reap, while LMDB on Windows does
  (`WAIT_ABANDONED`). One system call per acquire and per release.
- The release after a process death is delayed by an unspecified time, so a
  waiting writer can wait longer than on Unix after a crash.

#### Option W2 — Named mutex, as LMDB

A named mutex keyed by the lock file's identity; `WAIT_ABANDONED` reports
a dead owner, and the takeover reaps, as LMDB does.

- Parity with LMDB's takeover.
- A mutex must be released by the thread that took it. A ZeroDB `RwTxn` is
  `Send` and may commit or drop on another thread (ADR-0023 §2 rejected the
  robust pthread mutex for this reason). A release on the wrong thread fails
  with `ERROR_NOT_OWNER`.
- Named objects live in a session namespace: `Global\` needs
  `SeCreateGlobalPrivilege` to create, which a service has and an ordinary
  user process may not; `Local\` does not cross sessions (a service and a
  desktop process would not see each other's lock). LMDB uses `Global\`.

#### Option W3 — The Linux PID word, with a timed wait

The writer-lock word in the mapped lock file, as on Linux: `CAS 0 → pid`
to acquire, a swap to 0 (`Release`) to release, from any thread. A
contended waiter sleeps on a named auto-reset event (or polls with a short
sleep), at most 100 ms at a time, and after each timeout probes the owner's
liveness byte; a dead owner is taken over by CAS, and the takeover reports
itself, so the next writer reaps as on Linux.

- Same protocol and the same SPEC rules (LCK-4) as Linux, including the
  takeover-reaps behavior that LMDB has on Windows; no system call when
  uncontended; releasable from any thread.
- The wake-up primitive is new: Windows has no cross-process futex
  (`WaitOnAddress` works within one process only). A named event brings the
  namespace question of W2; polling has no kernel object but adds up to one
  sleep interval of latency to a contended handover.
- Liveness relies on the `LockFileEx` probe, whose release after death is
  delayed: a takeover can come later than on Linux, never earlier (a live
  owner always holds its byte).

**Recommendation: W3**, with polling (1 ms doubling to 100 ms, then the
liveness probe) in the first implementation, and a named event only if
contended handover latency matters in the benchmark ladder. It keeps one
writer-lock protocol for Linux and Windows and avoids D-028 there.

### Reader slots, liveness and cleanup by process ID

Unchanged in protocol (SPEC 04 LCK-3 to LCK-11): the owner word keeps the
PID and the claim generation. Each process holds an exclusive `LockFileEx`
lock on byte `2^32 + pid` from open to close. "Is P alive?" becomes a
non-blocking exclusive `LockFileEx` attempt on P's byte: failure with
`ERROR_LOCK_VIOLATION` means alive; success means dead, and the probe
unlocks at once. A probe of this process's own PID still answers alive
without a call, as today.

This keeps ZeroDB's immunity to PID reuse, which LMDB's `OpenProcess`
check lacks: a dead reader whose PID was recycled is reaped by ZeroDB and
not by LMDB. That is a second Windows behavior difference to record if
the oracle confirms it. The probe can take a lock for a moment, so two
probes of one dead PID can race; the loser sees "alive" and skips a slot it
would have reaped, which is the safe direction.

The one-descriptor registry stays, keyed by the file ID
(`GetFileInformationByHandle`: volume serial number and file index), to
keep `EnvAlreadyOpened` (TXN-51). Its POSIX reason (closing a descriptor
drops the process's locks) does not apply on Windows.

### Commit writes and the durability barrier

#### Option D1 — `seek_write` and `File::sync_data`

Positioned writes of each page (or a gathered buffer per run), and the two
barriers C3 and C5 as `FlushFileBuffers` through `File::sync_data`. Under
`WRITE_MAP`, `FlushViewOfFile` of the ranges, then `FlushFileBuffers`.

- The commit pipeline (ADR-0004, SPEC 06) is unchanged: only the backing's
  primitives differ, which the `Backing` trait already isolates.
- `FlushFileBuffers` flushes the whole file and the device cache, twice per
  commit; this is the slow path LMDB avoids.

#### Option D2 — Write-through handles, as LMDB

Write data pages and the meta through handles opened with
`FILE_FLAG_WRITE_THROUGH`, and skip `FlushFileBuffers` on a synchronous
commit.

- What LMDB does, likely faster on Windows.
- Changes the barrier structure of SPEC 06 (C3 and C5 become properties of
  the writes, not separate calls), the fault-injection backing that models
  them (ADR-0008), and the crash harness's assumptions. That is a durability
  change of its own.

**Recommendation: D1** first, so the crash harness and SPEC 06 apply
unchanged; D2 as a separate, measured ADR if Windows commit latency matters
to Meilisearch.

## Decision

Proposed: M1 (after a spike), W3 with polling, D1, and the reader-table
protocol unchanged with a `LockFileEx` liveness probe. Awaiting the
maintainer's approval; nothing is implemented. Until then the crates stop
at the `compile_error!` in `zerodb-core` on any non-Unix target.

## Consequences

### New dependency

`windows-sys` (Microsoft's raw bindings, no runtime code), for the section
and view calls, `LockFileEx`/`UnlockFileEx`, `GetFileInformationByHandle`,
`GetSystemInfo`, and the error codes. It would be a target-specific
dependency of `zerodb-io` and `heed-zerodb` (`[target.'cfg(windows)'.dependencies]`).
CONTRIBUTING requires an ADR for a new dependency; this is it. `libc` stays
Unix-only.

### `unsafe` added to `zerodb-io`

The policy amendment this needs, in CONTRIBUTING's terms:

- **Map** (`zerodb-io::mmap`, already sanctioned for mmap access): the
  `NtCreateSection`/`NtMapViewOfSection` calls and `NtUnmapViewOfSection`
  in `Drop` (Option M1), with the same aliasing contract as today's maps.
  The views are wrapped in the existing `Mmap`/`MmapWritable` types so
  `MapRegions` and the page casting above them are unchanged. Option M3
  would need none of this: memmap2 maps the file.
- **Lock file** (`zerodb-io::lockfile`, already sanctioned for record
  locks and atomics over the mapping): `LockFileEx` and `UnlockFileEx` with
  an `OVERLAPPED` offset, replacing `fcntl`; a named event if W3 uses one.
  The atomic views over the mapped lock file are unchanged.
- **File identity and system queries:** `GetFileInformationByHandle` and
  `GetSystemInfo`, each a query writing into a caller-owned struct.
- **heed adapter:** `GetSystemInfo` in place of the one `sysconf`.

Every block keeps the `// SAFETY:` rule. No `unsafe` is needed for writes
(`seek_write` is safe) or for the barrier (`File::sync_data` is safe).

### SPEC and documentation

- SPEC 02 §8 (map and file growth on Windows), SPEC 04 §12 (LCK-1, LCK-2,
  LCK-4, LCK-5 gain a Windows form), SPEC 06 REC-12 (`FlushViewOfFile`
  plus `FlushFileBuffers`).
- `docs/USING.md#platform-support`: Windows moves to "tested, supported",
  with a "what differs on Windows" table like macOS's.
- `docs/LMDB-COMPATIBILITY.md`: any Windows-only difference the oracle finds
  (the PID-reuse case above is the expected one).

### Tests and CI

- A Windows CI job (`windows-2025`) running the same suites as the macOS
  job: clippy, `cargo test --workspace`, the `unique-txns` adapter tests and
  the crash smoke. The oracle builds the LMDB fork with MSVC, which
  `lmdb-master-sys` supports, so the differential tests then observe LMDB on
  Windows too.
- The multi-process tests use Unix APIs today (`parent_id`, symlinks, Unix
  permissions, `hdiutil`); each needs a Windows form or a stated reason to
  be Unix-only. The crash harness's `mp` mechanism kills a child process,
  which `Child::kill` does with `TerminateProcess`.
- New tests: the M1 spike as a permanent multi-process test; a writer
  takeover after `TerminateProcess` that reaps (W3); a liveness probe
  against a recycled PID.

### Order of work

1. The M1 spike (map growth across two processes), alone.
2. Single process (`NO_LOCK`): map, positioned writes, barriers, the heed
   adapter, the CI job with the multi-process tests excluded.
3. The lock file: election, liveness, writer lock, reader slots; the
   multi-process tests and the oracle's multi-process observations.
4. `zerodb-tools` and the environment copy (block cloning on ReFS through
   `FSCTL_DUPLICATE_EXTENTS_TO_FILE` is an optional later fast path; the
   streaming copy works everywhere).

## Open questions for human review

1. Is Windows a release requirement for the first Meilisearch integration,
   or can the first integration ship Linux and macOS only with Meilisearch
   keeping LMDB on Windows?
2. M1 relies on `ntdll` section calls outside the Win32 API, as LMDB does.
   Acceptable, or is M3's file-length difference preferred to avoid them?
3. W3 polling versus a named event: is up to 100 ms of extra handover
   latency between processes under contention acceptable for a first
   version?
4. D1's two `FlushFileBuffers` per commit: acceptable for a first version,
   with D2 as a later, measured change?
5. Windows on aarch64 (Snapdragon laptops): in scope with x86-64, or later?
