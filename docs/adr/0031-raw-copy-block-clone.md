# ADR-0030: Raw environment copy by filesystem block clone

- Status: Draft
- Related issue: #64
- Date: 2026-10-20

## Context

The non-compacting ("raw") copy, `CopyToFile::copy_to_file(path,
CompactionOption::Disabled)` (`crates/zerodb/src/copy.rs`), is LMDB's
`mdb_env_copy2` without `MDB_CP_COMPACT`. It opens its own read
transaction, pinning snapshot `T`. It writes two meta pages that it builds for
`T`, then copies pages `2..=last_pg` straight from the map, as LMDB's
`mdb_env_copyfd` does. It is at parity with LMDB: `maint/copy/raw` measures
0.94× (`docs/PERF-GAP-VS-LMDB.md`). Its consistency argument does not exclude
the writer (ADR-0009, module docs of `copy.rs`):

- a page reachable from `T` cannot be reused while this reader is the oldest
  (SPEC 04 TXN-20/21), so it is never overwritten during the copy;
- a page that was already free at `T` may be reused and copied torn, which is
  harmless: the copy's freelist lists it as free.

Copy-on-write filesystems can clone a file by sharing its blocks: `clonefile`
on APFS, `FICLONE`/`copy_file_range` on XFS, btrfs and bcachefs, and block
cloning on ZFS. Rust's `std::fs::copy` already uses them: on Apple platforms
it calls `fclonefileat` and falls back to `fcopyfile`. On Linux it calls
`copy_file_range` in chunks of up to 1 GiB, which the kernel turns into a
reflink when both files are on the same reflink-capable filesystem (Linux
5.3 and later). Calling any of these directly would need FFI (`unsafe`) in
`zerodb`, which the `unsafe` policy in `AGENTS.md` does not allow. Through
`std` it needs none.

Measured on macOS arm64 (APFS, 95 % full disk, host load 5–8), a 1.37 GiB
env, 3 rounds, `crates/zerodb-oracle/examples/reflink_copy_spike.rs`:

| path form, raw | LMDB (heed 0.22.1) | ZeroDB main | clone under read pin |
|---|---|---|---|
| round 0 | 4.74 s | 4.36 s | 0.035 s |
| round 1 | 2.36 s | 2.25 s | 0.040 s |
| round 2 | 30.8 s | 16.1 s | 0.102 s |

## Options

### Option A: clone under the read pin, then fix up (no writer pause)

Keep the current read transaction. Clone the whole data file with
`std::fs::copy` into a fresh 0700 staging directory, because `fclonefileat`
refuses an existing destination. Overwrite pages 0 and 1 of the clone with the
two meta pages built for `T` (the bytes written today). `set_len` the clone to
`(last_pg + 1) × page_size`. Rename it over the destination.

The consistency argument is the one the current copy relies on, unchanged.
Every page the copy's metas reach was immutable for the whole clone because of
the pin, so the clone does not need to be atomic. Free pages may be torn,
which is harmless. The live metas, the file tail and any later commit are
overwritten or trimmed. The clone reads through the page cache. On APFS, bytes
written with `pwrite` and not yet fsynced, and a `WRITE_MAP | NO_SYNC`
commit that was never synced, were both captured (spike coherence checks).
There is no fsync ordering argument and no writer pause.

Cons:

- `std::fs::copy` does not report whether it cloned. On a non-CoW filesystem
  it falls back to a full copy of the **whole file**, not only `0..=last_pg`.
  Under `WRITE_MAP` the file is `set_len(map_size)`, so the fallback could
  copy far more than today. A guard is needed: take this path only when the
  file length is at most `(last_pg + 1) × page_size` plus a small slack, and
  use today's streaming copy otherwise.
- It applies only to the path form. `copy_to_open_file` (heed's
  `copy_to_file(&mut File)`) cannot clone into an open handle on macOS without
  FFI. On Linux, `std::io::copy` from `(&src).take(len)` into the handle
  becomes `copy_file_range` (in-kernel copy, a reflink on the same CoW
  filesystem) and keeps the same pin argument. That is a separate, Linux-only
  change that needs a Linux A/B.
- Progress callbacks: the clone is one step (`done` jumps to `total`).
- The copy's on-disk allocation is shared with the source until either is
  rewritten. Disk-usage tools report this differently from LMDB's copy, but the
  file content is byte-identical.

### Option B: pause the writer (or `force_sync`) and clone the live file

This is the approach the issue proposes. The clone must then be atomic with
respect to the commit sequence, and the copy keeps the live meta pair instead
of metas for `T`. Linux `copy_file_range` is chunked and therefore never atomic
over the whole file. A wrong argument here ships a silently corrupt snapshot.
It also blocks writers, which LMDB's copy does not do beyond the meta read.
Option A gets the same speed without either risk.

### Option C: do nothing

The raw copy is already at LMDB parity. Meilisearch's only raw copy
(`index-scheduler/src/scheduler/enterprise_edition/s3.rs`) uses the open-file
form into `tempfile::tempfile()`, which is usually on another filesystem,
and then reads the whole file again into a tarball for S3. Its snapshots and
compaction routes use the compacting copy. Option A therefore speeds up no
current Meilisearch path.

## Decision

Pending maintainer review. The recommendation is Option A if a path-form raw
copy on a CoW filesystem is a supported use worth a beyond-LMDB fast path,
otherwise Option C. Option B is rejected.

## Consequences (Option A)

- `copy.rs`: a clone branch in `copy_to_file_with_progress` for
  `CompactionOption::Disabled`, behind the file-length guard. The output is
  byte-identical to today's copy of a quiescent env, which a test pins against
  the streaming path. A concurrent-writer test opens the clone and checks it
  equals the snapshot at `T`.
- SPEC 00 row 17 and the `copy.rs` module docs describe the clone path and why
  its consistency argument is the reader pin.
- No new dependency and no `unsafe`.

## Open questions for human review

1. Is a path-form raw copy a use worth optimizing, given that no consumer uses
   one today?
2. Should the Linux open-file `copy_file_range` variant be pursued, given that it
   needs a Linux A/B?
