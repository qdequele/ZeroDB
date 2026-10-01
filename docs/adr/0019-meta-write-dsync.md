# ADR-0019: Durable meta write through an O_DSYNC descriptor (one barrier per commit)

- Status: Accepted (approved by Quentin 2026-10-01: Option A on **all
  platforms**, macOS included, and LMDB's failed-write scrub adopted; OQ4 and
  OQ5 left to the implementation and its bench)
- Milestone: Phase 3 (performance — durable-commit cost; perf roadmap Phase B
  "do what LMDB does")
- Date: 2026-10-01

## Context

A default-mode ZeroDB commit issues **two** `fdatasync`s: C3 (data) and C5
(meta), per SPEC 04 TXN-61 / SPEC 06 REC-7. LMDB issues **one**: it fsyncs the
data (`mdb_env_sync0`, mdb.c:4246), then writes the meta through a second
descriptor on the same file opened `O_WRONLY|O_DSYNC|O_CLOEXEC` (`me_mfd`,
`MDB_O_META`, mdb.c:4871) — the write returns only when that page is durable,
so no separate meta fsync exists ("Avoids a separate fdatasync() call",
mdb.c:4485).

Measured (2026-10-01, cited not re-measured):

- strace, rust-storage-bench YCSB B `--fsync`, 20 s, 1M keys, Linux: LMDB
  opens `data.mdb` twice (`O_RDWR|O_CREAT`, then `O_WRONLY|O_DSYNC|O_CLOEXEC`)
  and does 10,274 `fdatasync` over ~10.3k durable commits (≈1/commit), the
  meta going through the O_DSYNC fd. ZeroDB: 21,107 `fdatasync` over ~10.5k
  commits (≈2/commit).
- Public suite, Graviton4, YCSB B fsync 10M × 100 B: local NVMe ZeroDB 1.00×
  LMDB; **EBS io2 0.91×**, write p99 1.51 ms vs 1.10 ms.
- Meilisearch under a 3 GiB cap on the same EBS host: ZeroDB issues
  1.41× (NVMe) to 1.53× (io2) LMDB's write IOs per small commit for the same
  bytes (uncapped only 1.03–1.04×).

Why one O_DSYNC write beats write+fdatasync: it is one syscall instead of two,
and the kernel can satisfy a synchronized single-page write with an FUA write
where the device supports it, instead of a full device cache flush. Where the
device lacks FUA the kernel emulates with a flush, so the gain is
device-dependent — EBS must be measured, not assumed.

LMDB details that constrain the design (vendored fork, clean-room read):

- `me_mfd` is opened at env open only when `!(MDB_RDONLY|MDB_WRITEMAP)`, and
  **even under** NOSYNC/NOMETASYNC "in case these get reset" (mdb.c:5784-91).
- `mdb_env_write_meta` routes the meta write:
  `mfd = (flags & (MDB_NOSYNC|MDB_NOMETASYNC)) ? me_fd : me_mfd` (mdb.c:4488)
  — the relaxed modes write the meta through the plain fd, unsynced.
- WRITEMAP writes the meta through the map + `msync`; no `me_mfd` involved.
- On a failed meta write LMDB rewrites the *old* meta content through the
  non-sync fd to scrub the new bytes from the page cache, then sets
  `MDB_FATAL_ERROR` (mdb.c:4505-27).
- macOS asymmetry: LMDB's *data* barrier is `fcntl(F_FULLFSYNC)`
  (`MDB_FDATASYNC`, mdb.c:171) but its *meta* fd is plain `O_DSYNC`
  (`MDB_DSYNC`, mdb.c:541-45) — which on macOS does not force the device
  cache. LMDB's macOS meta durability is therefore weaker than its data
  barrier; ZeroDB today uses std `sync_data` (full-flush path on macOS,
  ADR-0004 OQ3) for both.
- LMDB writes only the tail of the `MDB_meta` struct (< 120 bytes, one
  sector), not a full page; ZeroDB's C4 writes the full `psize` meta page.

libmdbx (**unverified** — no vendored source in this tree): believed to open a
dedicated `dsync_fd` for meta writes the same way, plus a steady/weak-meta
scheme ZeroDB already declined (SPEC 06 REC-10 amendment). Taken as prior art
only for "two fds on one file is normal practice"; nothing below depends on it.

## Options

### Option A — second fd with O_DSYNC for the meta write (LMDB's scheme)

At env open, when the env is writable and not WRITE_MAP, open the data file a
second time `O_WRONLY|O_DSYNC|O_CLOEXEC` (Rust `File` adds CLOEXEC by default
— fork/exec hygiene is free; positioned writes, so the fd offset is unused).
C4 writes the meta page through this fd; C5's separate `fdatasync` disappears
in default mode. `NO_META_SYNC`/`NO_SYNC` route the meta through the plain fd
exactly as today (LMDB's mfd routing), so their windows (REC-9/REC-10) are
unchanged; the dsync fd is still opened (parity with the fork's
"in case these get reset"; also keeps `Env::sync(force)` free to restore
durability through the plain-fd `fdatasync` as now).

- **Crash ordering:** REC-7 intact — C3 (fdatasync data) still completes
  before the meta write begins; the meta write returning *implies* meta
  durability, so the old H3→H4 window ("meta written, not yet durable")
  ceases to exist in default mode. During the O_DSYNC write a power cut
  leaves the slot absent/torn/intact exactly as REC-6 H3 describes; after
  return the state is REC-6 H4. Strictly fewer reachable crash states.
  One semantic difference from `fdatasync`: O_DSYNC synchronizes **that
  write only**, not other pending writes on the file — safe here because C3
  has already drained everything else in every mode that uses the dsync fd.
  The meta pages (pgno 0/1) never extend the file, so no size-metadata
  concern.
- **Platforms:** Linux-native. macOS defines O_DSYNC but it does not flush
  the device cache — matching LMDB would *weaken* today's macOS meta barrier
  (std `sync_data` = full flush). Proposal: use the O_DSYNC fd on Linux
  (both arches); on macOS keep C5's `sync_data` (dev platform, strictly
  stronger, not consumer-visible — spec note, no DIVERGENCES entry).
- **Flags/read-only/copy:** READ_ONLY envs and WRITE_MAP envs open no dsync
  fd (LMDB parity; WRITE_MAP keeps msync C3/C5 per REC-12, untouched).
  `copy`/compaction write fresh files through their own descriptors
  (LMDB's `MDB_O_COPY` has no DSYNC) — untouched.
- **Error handling:** a failed/short O_DSYNC meta write = failed C4+C5 →
  poison the env (REC-13, unchanged policy; already stronger than LMDB's
  FATAL_ERROR). LMDB additionally scrubs the page cache by rewriting the old
  meta through the plain fd so an un-durable new meta cannot be read back
  later; ZeroDB has the same exposure today on a failed C5 (reopen before
  power loss may see `N` although commit returned Err). Adopt-or-not is OQ3.
- **Cost:** one extra fd per writable env for its lifetime; one syscall per
  durable commit saved; FUA-capable devices skip a full cache flush.

### Option B — `pwritev2(RWF_DSYNC)` on the existing fd

Per-write O_DSYNC (Linux ≥ 4.7), no second fd; `zerodb-io::file` already
carries the one sanctioned `pwritev` unsafe, so this is one more libc call in
the same module. Needs a runtime fallback (EOPNOTSUPP/ENOSYS, and all of
macOS) to write + `sync_data` — i.e. both code paths exist forever and the
crash harness must model both. Same durability argument as A. Rejected as the
primary: A is what LMDB does (perf-levers-mirror-LMDB rule), works on every
Linux kernel and filesystem the same way, and avoids a second semantics
branch; B remains a credible fallback-free *internal* implementation detail
to revisit only if the second fd proves problematic.

### Option C — status quo (two fdatasyncs)

Correct and simple; measured cost: ~2× the barrier syscalls per durable
commit and 0.91× LMDB on EBS io2 with 1.4× the write p99. Keeping it means
accepting a permanent, explained gap on the exact workload (small durable
commits on flush-expensive devices) the consumer cares about.

### Option D — sector-0-only meta write (refinement of A, not standalone)

ZeroDB's meta CRC region `[0,168)` + CRC at 168 sit in sector 0 and the page
tail is zeros (SPEC 02 §3, REC-8), so C4 could write 512 bytes instead of a
full `psize` page (LMDB writes < 120 bytes). Cuts the FUA write from up to
64 KiB to one sector. Not required for parity; REC-8's tear taxonomy is
unchanged (sector 0 is already the only sector that ever changes). Fold into
A only if the bench shows page size mattering; otherwise a later ledger entry.

## Decision (proposed)

**Option A**, default-on — this is LMDB parity (the project rule: gaps vs
LMDB ship with parity as the default; no opt-in flag, no consumer-visible
behavior change, crash guarantees identical or strictly tighter).

Human decisions (2026-10-01):
- **OQ1/OQ2:** Option A on all platforms. On macOS the meta write also goes
  through the O_DSYNC fd, as LMDB does, which makes the macOS meta barrier
  weaker than today's `sync_data` (O_DSYNC there does not force the device
  cache). macOS is a development platform, not a durability target; SPEC 06
  states the macOS meta guarantee plainly.
- **OQ3:** adopt LMDB's scrub. On a failed or short durable meta write, rewrite
  the slot's previous meta bytes through the plain fd before poisoning the env
  (REC-13), so a reopen before power loss cannot read back the unacknowledged
  meta.
- **OQ4** (Option D, sector-0-only write) and **OQ5** (where the dsync fd
  lives) are left to the implementation, decided by its bench and review.

Mechanism sketch (for review, not implementation): `Backing` gains
`write_page_durable(pgno, psize, data)` with a default impl of
`write_at_page` + `sync_data` (WriteMapBacking and any backend without the
feature keep today's exact behavior); `MmapBacking` overrides it with the
dsync-fd pwrite; `commit_pipeline` C4 calls it when `sync_meta`, else
`write_at_page` as now, and C5's barrier call is dropped where C4 was durable.

## Crash-testing impact

- `FaultBacking` must model a durable-on-return write: journal it, then fold
  **only that write** into the durable image (fold-self, not the barrier's
  fold-all — pending writes from other sources must stay losable, or the
  NO_SYNC/NO_META_SYNC windows would be silently modeled away). A capture
  *during* the call materializes {absent, torn, intact} as for any pending
  write; after return it is durable.
- Mutation self-test (ADR-0008 D6.1): add a `broken_dsync` mode that demotes
  the durable write to a plain write and prove REC-18 trips — the tripwire
  that guards this ADR's ordering claim.
- Hooks: H0–H2 unchanged. In default mode H3 now fires after a *durable*
  meta write, so its REC-6 row tightens to "recovers to `N`" (H3 ≡ H4);
  the old `{N−1, N}` outcome at H3 remains fully exercised by mechanism 2
  (fault capture during the C4 write) and by H3 under NO_META_SYNC/NO_SYNC,
  where C4 stays a plain write. No test is weakened — the H3 assertion gets
  *stricter* in default mode.

SPEC/DIVERGENCES edits the implementation would make (exact list):

- SPEC 04 §9 TXN-61: C4 row — "write meta via the meta-sync fd (durable on
  return) in default mode / via the plain fd under NO_META_SYNC/NO_SYNC";
  C5 row — "meta barrier: satisfied by C4's synchronized write where the
  backend provides one; otherwise `fsync(meta)` as today; skipped under
  NO_META_SYNC/NO_SYNC". H3/H4 wording per above.
- SPEC 06: REC-6 H3 row (default-mode outcome `N`), REC-7 (note the meta
  write itself is the barrier in default mode; ordering claim unchanged),
  REC-9 table meta column, REC-13 (failed durable meta write ⇒ poison),
  REC-17 hook list note. REC-10/REC-12 untouched.
- SPEC 01 §S6: the fd-routing note (`me_mfd` equivalent) gains ZeroDB's
  realization.
- DIVERGENCES.md: **no entry** — this is parity, not a divergence; the macOS
  stronger-barrier choice is an internal durability strengthening documented
  in SPEC 06 (same category as REC-13's poisoning).

## Bench plan (three columns: LMDB / ZeroDB-before / ZeroDB-after)

1. x86 bench server, engine-ladder `commit/sync/*` rungs (local SATA SSD +
   mdraid — a weak referee for flush cost, screening only).
2. Graviton4 hosts, rust-storage-bench YCSB B `--fsync` (10M × 100 B):
   local NVMe (expect ≥ 1.00× held) and EBS io2 (target: close the 0.91×
   gap and the 1.51 ms vs 1.10 ms write p99). Record whether io2 honors FUA
   (gain may be flush-emulated — report either way).
3. Meilisearch replay script with device IO counters (iostat), 3 GiB cap,
   NVMe + io2: write IOs per small commit, currently 1.41–1.53× LMDB.
4. strace syscall census as in Context: expect ≈1 `fdatasync`/durable commit.
5. Full gate: crash-test-quick + the long crash run (both mechanisms, all
   durability modes), fuzz-quick, workspace tests, miri.

## Consequences

Easier: durable-commit cost matches LMDB's by construction; the fault model
gains the primitive any future io_uring backend (FUA/`IORING_FSYNC`) needs.
Harder: two fds on one file (lifetime tied to `EnvInner`, after-mmap drop
order irrelevant since it is not the mapped fd); one more `Backing` method;
platform-conditional C5. No on-disk format change; no API change.

## Open questions for human review

1. **Approve Option A, default-on, Linux-gated?** (STOP zone: this merges
   C4/C5 in default mode.)
2. **macOS:** keep the stronger `sync_data` meta barrier (recommended), or
   match LMDB's plain O_DSYNC fd there too for exact syscall parity on a
   non-durability-target platform?
3. **Failed-write scrub:** adopt LMDB's rewrite-old-meta-through-the-plain-fd
   on a failed durable meta write, closing the "reopen before power loss sees
   an unacknowledged `N`" window that poisoning alone leaves (pre-existing
   today), or keep poison-only and document?
4. **Option D** (sector-0-only meta write): fold into this change if the
   io2 bench shows page-size sensitivity, or keep the full-page write?
5. Where should the dsync fd live — `MmapBacking` (proposed) or a separate
   `MetaWriter` seam so the future io_uring backend shares it?
