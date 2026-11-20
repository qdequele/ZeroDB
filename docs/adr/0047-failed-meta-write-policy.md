# ADR-0047: What the environment does after a failed meta write or commit barrier

- Status: Accepted (Option A), 2026-11-20. Implemented with #176.
  Open questions 2 and 3 keep REC-13's answers: read transactions keep
  working on a poisoned env, and a failed `Env::force_sync` keeps poisoning.
- Related issue: #176 (I/O-error injection)
- Date: 2026-11-20

## Context

Issue #176 added system-call failure injection to the fault backend
(`zerodb-io::fault`, `IoFault`) and tests that fail every write and sync of a
commit, of a spill, of the `WRITE_MAP` ranged `msync`, of file growth and of
`copy_to_file` (SPEC 06 REC-23). Most of the behavior they pin is sound:

- a failed page write (C2) or spill write aborts or errors the transaction,
  leaves the last commit intact on disk and in every crash image, and the
  environment keeps working. LMDB behaves the same, observed with a real
  `EFBIG` on both engines (`io_error_lmdb_differential`);
- a failed data or meta barrier (C3, C5, the `WRITE_MAP` ranged `msync`, an
  explicit `force_sync`) poisons the environment (SPEC 06 REC-13): every later
  write transaction fails until a reopen;
- a failed copy leaves neither a destination nor a staging file.

Two cases are not sound.

**1. A failed meta write (C4) leaves the environment writable.** The commit
returns the error and the transaction aborts, but nothing is poisoned. When
the write was short and the part that reached the file holds the whole encoded
meta (header and free-list annex, a few hundred bytes of a 4 KiB page), the
slot now holds a valid meta `N` for a commit that reported failure. Its data
is in the file too (C3 succeeded). The next writer in the same process reuses
txnid `N` (SPEC 04 TXN-2) on the snapshot `N−1`, so it may allocate and
overwrite the pages meta `N` references (they are free in `N−1`'s view,
TXN-62). If that writer is interrupted after its C2 and before its own C4 (a
failed C3, a kill, a power loss), the file's newest valid meta is `N` and its
pages are gone. Reproduced in-process with the fault backend: a half-page
short write of the meta, then a different transaction whose C3 fails; the
file then reports INV-18 and a run of INV-22 leaks from `zerodb-tools check`,
with no power loss involved.

How likely: a meta write overwrites an allocated page, so `ENOSPC` there needs
a copy-on-write or thin-provisioned volume (btrfs, ZFS, reflinked XFS, thin
LVM, some network block devices); a partial write before `EIO` needs a failing
device. Rare, but a device that is failing is exactly when the engine must not
turn an error into corruption.

**2. A failed C5 leaves the failed commit's meta in the file.** REC-13
poisons only the failing process (ADR-0023 §2). Its meta `N` stays in the
page cache, readable by a reopen (a commit that reported failure becomes
visible), and, in a lock-file environment, other processes keep committing
from the published `N−1` and reuse txnid `N` (REC-13). By the same reasoning as
case 1 a writer in another process can then reach the same corrupt state if it
is interrupted between its C2 and C4. This is reasoned from the code, not
tested: the tests run one process.

**LMDB** (the vendored fork, read for understanding, not transliterated):

- a failed or short meta `pwrite` (`mdb_env_write_meta`) rewrites the slot's
  previous meta fields through the non-sync descriptor, so the page cache no
  longer holds the new meta, then sets `MDB_FATAL_ERROR`: every later
  `mdb_txn_begin`, read or write, returns `MDB_PANIC` until the environment is
  closed. Under `WRITE_MAP` the meta is stored in the map and a failed meta
  `msync` sets `MDB_FATAL_ERROR` without a rewrite;
- a failed explicit `mdb_env_sync` returns the error and marks nothing;
- a failed page flush or data sync in `mdb_txn_commit` aborts the transaction
  and leaves the environment usable (ZeroDB's REC-13 is stricter for the data
  sync, a recorded strengthening).

LMDB's meta write is also its meta barrier (`O_DSYNC` descriptor), so ZeroDB's
C4 and C5 together correspond to LMDB's one meta write. ADR-0019 (accepted,
implementation parked) already approved LMDB's scrub followed by poisoning for
its `O_DSYNC` meta write (its OQ3); `main` still issues two `fdatasync`s and
has neither the scrub nor the poisoning on C4.

The policy after a failed `fsync` itself (REC-13) is not reopened here: on
Linux a failed `fsync` may drop the dirty pages and clear the error, so a
retry that succeeds proves nothing, and refusing further writes until a reopen
is the only safe answer. The injection tests confirm REC-13 holds on every
barrier, including the `WRITE_MAP` ranged `msync` and an explicit sync.

## Options

### Option A — scrub and poison on any failed C4 or C5 (LMDB's handling)

Before C4, the writer keeps the slot's current bytes (the meta of `N−2`, read
with `read_meta_copy`; one page, carried like `meta_buf`). If C4 or C5 fails,
it writes those bytes back to the slot (through the descriptor; under
`WRITE_MAP` into the map, then a ranged `msync` attempt), ignores the result
of that write, and poisons the environment (REC-13 extended to C4).

- Fixes case 1: the process takes no more writes, and the slot no longer
  holds meta `N` in the page cache, so a reopen reads `N−1`, as the failed
  commit reported.
- Narrows case 2: other processes find the old meta in the slot. If the scrub
  itself fails (the device refuses every write), meta `N` may remain;
  Option C closes that.
- Read transactions keep working on a poisoned env, as REC-13 has them.
  LMDB refuses them (`MDB_PANIC`); keeping them is a strengthening for
  readers, to record in LMDB-COMPATIBILITY.md if adopted.
- Cost: one page kept per writer and one `pread` per commit (or the meta the
  slot held, which the writer already decoded at open or at its last commit:
  no I/O). Nothing on the success path changes on disk.

### Option B — poison on a failed C4, no scrub

- Fixes case 1 in-process: no later writer in this process, and a reopen
  selects meta `N` (consistent: its data was made durable at C3), so the next
  writer uses `N+1`. The commit that reported failure is then visible after a
  reopen.
- Leaves case 2 as it is.
- Smallest change: one `poison()` call.

### Option C — Option A, plus a poison visible to every process

Publish the poison in the lock file's shared region (ADR-0023), so that every
process refuses writes until all of them reopen. Closes case 2 even when the
scrub fails. It changes the shared region (a reader-table and concurrency
change), so it would need its own ADR; listed for completeness.

### Option D — status quo

Keep the known defect, documented in SPEC 06 (conflict 5).

## Recommendation

**Option A.** It is LMDB's handling, the direction ADR-0019 OQ3 already
approved, and it closes the reproduced in-process corruption with a change
confined to the commit pipeline's failure path. Option C can follow if
multi-process deployments make case 2 matter.

## Decision

Option A. The maintainer approved it on 2026-11-20, with REC-13's existing
answers to open questions 2 and 3.

The writer copies the target slot with `Backing::read_meta_copy` (one
`pread` of one page per commit, into a buffer carried with the writer) rather
than keeping the older meta from an earlier commit: in a lock-file
environment another process may have written that slot since. The restore
writes the copy back, then attempts the meta barrier again when the commit
would have synced its meta (`sync_meta`), ignores both results, and poisons.
The added `pread` was not benchmarked; it is a page-cache read next to the
commit's two barriers.

## Consequences

- `commit_pipeline` (C4, C5): keep the slot's previous bytes; on failure,
  restore them, then `poison()`. `Backing` needs no new method
  (`write_at_page` and `sync_pages` cover the restore).
- SPEC 04 §8.1 (error table: a failed meta write also poisons), TXN-60;
  SPEC 06 REC-13 (C4 joins the poisoning steps; the scrub), REC-23 (the C4 row:
  poisoned; a reopen reads `N−1`); conflict 5 retired.
- Tests in `io_error_injection.rs` that change, all toward stricter
  expectations: the `MetaWrite` cases expect a poisoned env and a reopen at the
  previous commit; the `MetaSync` cases expect a reopen at the previous commit
  (the scrub succeeds in the fault backend). New regression test: a short
  meta write, then a second transaction whose C3 fails, then the file must
  check clean and open at the last acknowledged commit (the reproduction
  above): `failed_meta_write_then_interrupted_writer_keeps_the_file_consistent`,
  in default mode and in-place `WRITE_MAP`.
- The fault backend models the scrub as one more write: a pending write that
  a crash may keep or lose, so crash images may still recover `N`
  (consistent) or `N−1`.
- ADR-0019's future `O_DSYNC` meta write reuses the same scrub path.

## Open questions for human review

1. Option A, B, C or D?
2. After a failed meta write, should read transactions keep working (REC-13
   today) or fail as LMDB's `MDB_PANIC` does?
3. Should `Env::force_sync`'s failure keep poisoning (REC-13 today)? LMDB's
   `mdb_env_sync` returns the error without marking the env fatal; ZeroDB's
   stricter answer follows from the same `fsync` reasoning, and is recorded
   in REC-13.
