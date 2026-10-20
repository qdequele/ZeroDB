# ADR-0032: Raw environment copy from the data file: clone or in-kernel copy

- Status: Proposed
- Related issue: #64
- Date: 2026-10-20

## Context

The non-compacting ("raw") copy, `CopyToFile::copy_to_file(path,
CompactionOption::Disabled)` and `copy_to_open_file(&mut File, Disabled)`
(`crates/zerodb/src/copy.rs`), is LMDB's `mdb_env_copy2` / `mdb_env_copyfd2`
without `MDB_CP_COMPACT`. It opens its own read transaction, pinning
snapshot `T`. It writes two meta pages built for `T`, then copies pages
`2..=last_pg` straight from the map, as LMDB's `mdb_env_copyfd` does. It is at
parity with LMDB. Its consistency argument does not exclude the writer
(ADR-0009, module docs of `copy.rs`):

- a page reachable from `T` cannot be reused while this reader is the oldest
  (SPEC 04 TXN-20/21), so it is never overwritten during the copy;
- a page that was already free at `T` may be reused and copied torn, which is
  harmless: the copy's free list lists it as free.

Copy-on-write filesystems can clone a file by sharing its blocks: `clonefile`
on APFS; `FICLONE`, or `copy_file_range` on the same filesystem, on XFS,
btrfs and bcachefs. Rust's `std` already uses them: `std::fs::copy` calls
`fclonefileat` on Apple platforms (falling back to `fcopyfile`), and
`std::io::copy` from one `File` to another calls `copy_file_range` on Linux,
which the kernel turns into a reflink when both files are on the same
reflink-capable filesystem and into an in-kernel copy otherwise. Calling any
of these directly would need FFI (`unsafe`) in `zerodb`, which the `unsafe`
policy in `AGENTS.md` does not allow. Through `std` it needs none.

The spike (macOS arm64, APFS, a 1.37 GiB env, a raw copy to a path) measured
the streaming copy at 2.3–16 s and a clone under the read pin at 35–100 ms;
LMDB took 2.4–31 s on the same busy machine.

The maintainer decided on 2026-10-20 to do this work (issue #64).

## Options

### Option A: read the data file under the read pin, then fix up (no writer pause)

Keep the current read transaction. Take the image's bytes from the data file
instead of the map, then overwrite the image's two meta pages with the ones
built for `T` (the bytes written today) and make its length exactly
`(last_pg + 1) × page_size`:

- **Linux, both forms, when the destination is on the data file's own
  XFS, btrfs or bcachefs filesystem**: `std::io::copy` of the file's first
  `(last_pg + 1) × page_size` bytes (`File::take`), in the streaming copy's
  progress chunks. std performs it with `copy_file_range`, which these
  filesystems turn into a reflink. It never copies more bytes than the
  streaming copy and reports progress as before. "Same filesystem" is the
  same device number (`st_dev`) for the data file and the destination's
  directory (or the open file); the filesystem type is that of the deepest
  mount point containing the data file in `/proc/self/mountinfo`. Anything
  else streams: on ext4 the in-kernel copy was measured about twice as slow
  as streaming from the map (below).
- **macOS, a copy to a path with no progress callback**: `std::fs::copy` of
  the whole file into a fresh owner-only staging directory (`fclonefileat`
  refuses an existing destination; the directory keeps anything else from
  being placed at that path), trimmed with `set_len`, renamed over the
  destination.
- **Everything else** (macOS into an open handle or with a progress callback,
  other Unixes, test backings, a data path that no longer names the env's
  file): today's streaming copy.

**Why the bytes read are `T`'s.** Every page reachable from `T` was written
before `T` was published: with `pwrite` in the default mode, through the
shared map under `WRITE_MAP`. While the copy's read transaction is open no
such page is rewritten, in place or otherwise: commit writes and spills
target only pages no live snapshot references (SPEC 04 TXN-62, TXN-70), and
the `WRITE_MAP` in-place writes obey the same rule ("Reader safety is
TXN-62's, applied earlier", SPEC 04 §6.4, ADR-0021). On Linux and macOS a
file's page cache is the memory a shared mapping maps (a unified buffer
cache), so `read`, `copy_file_range` and the clone see those bytes whether
or not they have reached the disk — the default mode's readers already rely
on the reverse direction (bytes written with `pwrite` are seen through the
map). Reflink and clone paths write dirty cached pages back before sharing
blocks. The clone or copy need not be atomic: Linux `copy_file_range` works
in chunks, and pages free at `T` may change between chunks, which is
harmless as above. The live meta pages, any page past `last_pg`, and any
later commit are overwritten or trimmed. There is no fsync-ordering argument
and no writer pause.

The same argument covers the byte-copy fallbacks (`fcopyfile` on a non-APFS
or cross-volume destination; `copy_file_range`'s in-kernel copy or std's
`sendfile` / read-write fallback on Linux): they too read the file through
the page cache while the pin holds. So the routes do not need to detect
whether a real clone happened to be correct. They do need it to be cheap:
`std::fs::copy` copies the **whole** file when it cannot clone, and under
`WRITE_MAP` the file is as long as the map. The macOS clone is therefore
attempted only when the file is at most a quarter longer than the image, so
a fallback copies at most 25 % more bytes than streaming; a `WRITE_MAP` env
on macOS keeps streaming. Linux copies only the image's bytes and needs no
such guard.

**Which file is read.** The backing records the path it opened the data file
at (`Env::data_path`). The copy opens that path afresh — its own file
offset, since `std::io::copy` uses and moves the offset, and a `dup` of the
env's descriptor would share it with any concurrent copy — and uses it only
when its device and inode equal the env's own descriptor's; the macOS clone,
which `std::fs::copy` opens by path, re-checks the path afterwards. A data
file renamed or replaced since the open falls back to streaming. Opening and
closing a second descriptor releases no lock: the POSIX record locks are on
the lock file, never the data file.

Durability and crash safety are unchanged: like LMDB's `mdb_env_copy`, no
route syncs the copy; the caller fsyncs it if they need it durable. The
destination is unchanged until the final rename on the path form, as today
(a crash leaves at most a staging entry beside it, as today); the
open-handle form writes straight into the handle, as today. The source is
only read.

ARM: the change adds no atomics and no shared memory between threads. That
the bytes a reader sees through the page cache are the ones the writer
stored before publishing `T` rests on the same publication of `T` (the
writer's release, the reader's acquire when it pins `T`) that today's
map-reading copy relies on; the kernel then copies from the same physical
pages.

Cons:

- The clone has no intermediate progress, so the progress form
  (`copy_to_file_with_progress`, a ZeroDB extension) never clones; it
  streams on macOS and copies in chunks on Linux.
- A clone's blocks are shared with the source until either is rewritten.
  Disk-usage tools account for this differently from LMDB's copy; the file
  content is the same.
- On XFS and btrfs the reflink takes the source inode's locks for its
  duration, so a writer's `pwrite`, or a `WRITE_MAP` page fault, can wait
  for it. That is a kernel-side wait of the clone's duration (milliseconds
  where the streaming copy takes seconds), not a pause the copy imposes.
- Linux decides by filesystem type, read from `/proc/self/mountinfo`
  (without `/proc` it streams). An XFS formatted without reflinks (the
  default since xfsprogs 5.1, 2019) would get the in-kernel copy, which on
  ext4 measured slower than streaming.

### Option B: pause the writer (or `force_sync`) and clone the live file

This is the approach the issue first proposed. The clone must then be atomic
with respect to the commit sequence, and the copy keeps the live meta pair
instead of metas for `T`. Linux `copy_file_range` is chunked and therefore
never atomic over the whole file. A wrong argument here ships a silently
corrupt snapshot. It also blocks writers, which LMDB's copy does not do beyond
the meta read. Option A gets the same speed without either risk.

### Option C: do nothing

The raw copy is already at LMDB parity. Meilisearch v1.55's only raw copy
(its S3 snapshot) uses the open-handle form into a temporary file and then
reads that file into a tarball; its regular snapshots use the compacting
copy, which this does not touch. Without Option A's Linux open-handle route,
no current Meilisearch path gets faster.

## Decision

Option A, as described (proposed; the maintainer approves at merge). Option B
is rejected.

## Consequences

- `crates/zerodb/src/copy.rs`: the routes above, chosen per platform and
  form; every route's output is byte-identical to the streaming copy's for a
  quiet env, which a test pins. Tests force each route (including the Linux
  file copy, which on macOS is a read/write loop through the page cache) and
  check that the copy opens at exactly the pinned snapshot under a
  concurrent writer, with and without `WRITE_MAP` and `NO_SYNC`, while the
  live file grows past the snapshot.
- `zerodb-core`: `Backing::data_path` (default `None`) and `Env::data_path`;
  `zerodb-io`'s file backings record the path they opened.
- SPEC 00 row 17 and the `copy.rs` module docs describe the routes and why
  their consistency argument is the reader pin.
- No new dependency and no `unsafe`.
- Measurement: `crates/zerodb-oracle/examples/raw_copy_bench.rs` (LMDB and
  ZeroDB, both forms, medians of rounds alternating the engines).

## Measurements (2026-10-20)

macOS arm64, APFS, internal SSD, host load 20–190 (indicative only): a
3.2 GiB env, 5 rounds; and small envs, 9 rounds. ZeroDB before = `main`
at `2013c13`; LMDB is heed 0.22.1 in the same run.

| raw copy to a path | LMDB | ZeroDB before | ZeroDB after |
|---|---:|---:|---:|
| 3.2 GiB env | 20.4 s / 23.1 s | 20.3 s | 0.19 s |
| 253 MiB env | 55 ms / 85 ms | 68 ms | 4.3 ms |
| 63 MiB env | 18 ms / 11 ms | 26 ms | 1.6 ms |
| 15 MiB env | 3 ms / 3 ms | 3.1 ms | 0.9 ms |

(LMDB is given as measured in the before run / the after run.) The
open-file form is unchanged on macOS (20.9 s before, 21.6 s after, LMDB
17.6 s and 24.3 s).

Linux (OrbStack VM on the same Mac, kernel 7.2, a 1.6 GiB env, loop-mounted
filesystems, 3–5 rounds; a shared VM, so read the ext4 rows as a direction):

| filesystem, form | LMDB | ZeroDB before | ZeroDB, file copy everywhere |
|---|---:|---:|---:|
| XFS (reflink), path | 6.3 s / 2.5 s | 7.7 s | 0.002 s |
| XFS (reflink), open file | 1.4 s / 2.1 s | 2.0 s | 0.001 s |
| btrfs, path | 16.0 s / 2.2 s | 13.0 s | 0.0007 s |
| btrfs, open file | 2.5 s / 2.6 s | 2.0 s | 0.000 s |
| ext4, path (3 passes) | 1.3–2.0 s | 1.5–2.2 s | 2.8–4.6 s |
| ext4, open file (3 passes) | 1.2–1.5 s | 1.4–1.7 s | 2.2–3.4 s |

The ext4 rows are why Linux takes the file copy only on XFS, btrfs and
bcachefs; with that rule ext4 streams as before.

Still to measure on a quiet Linux host (Graviton): the same example on XFS
and on ext4, before and after, both forms:

```text
cargo run --release -p zerodb-oracle --example raw_copy_bench -- 4096 5 [dest-dir]
```

built at `main` and at this change, with the env and `dest-dir` on XFS,
then on ext4 (ext4 should be flat), then with `dest-dir` on another
filesystem (should be flat).

## Open questions for human review

1. The macOS clone is attempted only when the data file is at most a
   quarter longer than the image. Is 25 % the right bound?
2. Should `copy_to_file_with_progress` clone too, reporting only 0 and the
   total? It is a ZeroDB extension; today it streams on macOS so that a
   progress bar moves.
3. Linux picks the route from the filesystem type in `/proc/self/mountinfo`.
   Acceptable, or should the Linux route wait for a way to ask for a clone
   explicitly (which needs FFI, outside the `unsafe` policy)?
