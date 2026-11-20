# Power-loss harness

The commit protocol promises that a power cut never loses an acknowledged
commit in a durable mode and never leaves a partial commit visible (SPEC 06
REC-6, REC-9, REC-10). The other crash tests check this against a model of
the disk (the image mechanism) or by killing the process (the SIGKILL
mechanisms). A killed process leaves the kernel page cache intact, so those
tests never lose a write the file system accepted but the device had not made
durable. This harness does: it runs the workload on a real file system on a
Linux block device, and checks the states a power cut can leave on that
device.

## How it works

1. The file system (ext4, XFS) is created on a
   [`dm-log-writes`](https://docs.kernel.org/admin-guide/device-mapper/log-writes.html)
   device. That device-mapper target passes every write to a data device and
   records it on a log device: a FUA write when it completes, any other write
   when a flush that covers it completes (just before that flush's entry),
   and every flush.
2. `power-loss workload` runs a seeded workload in that file system: one
   large commit of multi-page values, the crash harness's seeded rounds
   (overflow values, a multi-MB value on some seeds, a clear of thousands of
   pages), 16 small commits per round, optionally a small dirty limit so large
   commits spill before they commit, and a compaction (`copy_to_file` with
   `CompactionOption::Enabled`) after round 3, with the copy and its
   directories synced. After each commit returns, the workload inserts a mark
   into the log (`dmsetup message <dev> 0 mark zdb-C-<txnid>`), so every log
   position is tied to the commits acknowledged before it.
3. `power-loss verify` reads the log and rebuilds the device on a third loop
   device, entry by entry. At each cut point, by default right after each
   flush or FUA entry, the rebuilt device holds every write the device
   received before that point and none after it: what a power cut leaves once
   the device has honoured the flush. `scripts/power-loss/hook.sh` puts a
   throwaway dm-snapshot over the rebuilt device and mounts it, so the file
   system replays its journal into the snapshot and the rebuilt device stays
   as logged.
4. On each mounted state, the verifier asserts:
   - the env opens (with page validation on);
   - the recovered txnid is at least the last acknowledged commit (default and
     `WRITE_MAP`) or the one before it (`NO_META_SYNC`, REC-10), and at most
     the commit in flight at the cut;
   - `check_image` passes on the data file, and so does
     `zerodb-tools check`;
   - the contents equal the model's committed state for exactly the recovered
     txnid, so no later commit is partly visible;
   - at the end of the log, after a clean unmount, recovery selects the last
     acknowledged commit;
   - once the compaction's mark is in the log, the compacted copy opens,
     passes the same checks and equals the state of the snapshot it copied.

   Under `NO_META_SYNC`, a state that recovers below the commit in flight and
   fails the walk or the data comparison is the reclaim-clobber window that
   REC-10 documents and that LMDB shares. It is counted and printed as a stale
   fallback, not failed, as the image mechanism does.

The modes are default, `WRITE_MAP` and `NO_META_SYNC`. `NO_SYNC` and
`MAP_ASYNC` promise structural safety only when the file system keeps write
order (REC-11), which a power cut does not, so this harness does not run them.

## Why dm-log-writes

Three ways to drop unflushed writes below the file system were considered.

- **`dm-log-writes` with replay** (chosen). The log is a complete,
  deterministic record of what reached the block layer and when the file
  system asked for durability. One workload run yields every flush point as a
  cut, replayed incrementally, so hundreds of cuts per run cost one mount
  each, and any failing cut can be replayed again from the same log. Marks
  placed by the workload tie cut points to acknowledged commits exactly, with
  no clock or race involved. xfstests uses the same target for its power-cut
  tests (generic/482). The replayer is written in Rust in this repository
  (`crates/zerodb-oracle/src/crash/powerloss.rs`), from the format the kernel
  documents, so it needs no tool built from another project and its parser
  and replay logic are unit-tested on any platform.
- **`dm-flakey`** drops writes after a chosen moment, and with `drop_writes`
  behaves like a device that stopped. Each cut needs its own workload run, the
  cut position is a wall-clock guess rather than a log position, and it
  cannot keep "everything before this flush, nothing after". It finds fewer
  states per minute and does not reproduce one exactly.
- **A VM whose virtual disk is cut** (qemu with `cache=unsafe`, killed hard)
  tests the whole stack, but each cut costs a VM boot, cut points are random,
  and the hosted CI runners do not offer nested virtualization reliably. It
  is the more realistic test for one cut and the less useful one for many.

### What a log prefix does not model

A replayed prefix keeps writes in the order they completed. A real device
with a volatile cache may also persist a later write and lose an earlier one
between two flushes, or tear a sector. The image mechanism of the crash
harness covers reordering and tearing against its model of the disk; this
harness covers the real file systems and block layer, without reordering.
Running with `EVERY=N` adds cut points between flushes (prefixes of the
writes a flush would have covered).

## Running it

It needs Linux, root, loop devices, the `log-writes` and `snapshot`
device-mapper targets, and `mkfs.ext4` / `mkfs.xfs`.
`scripts/power-loss/probe.sh` checks all of them. On Ubuntu the two targets
ship in `linux-modules-extra-$(uname -r)`, and XFS needs `xfsprogs`.

```sh
sudo scripts/power-loss/run.sh                       # or: just power-loss
sudo FS=xfs MODES=nometasync SEEDS=7 MAX_POINTS=50 scripts/power-loss/run.sh
```

The defaults run ext4 and XFS, all three modes, with and without spills, on
seeds 7 and 24, checking every flush. Settings are listed at the top of
`run.sh`. Each run uses about 2 GiB of sparse images under
`target/power-loss`. On a violation the verifier prints the run, the log
entry and the marks at the cut; re-run that one combination with the same
seed to reproduce it, since the workload and the log it produces are
deterministic for a given seed up to the file system's own allocation.

## In CI

The `power-loss` job of `.github/workflows/ci.yml` runs nightly on the x86-64
and aarch64 hosted runners (three seeds, at most 250 cut points per run) and
on pull requests that change the harness or `crates/zerodb-io`, on x86-64
only (one seed, at most 40 cut points per run). The hosted Ubuntu runners
provide passwordless `sudo` and loop devices, and the job installs
`linux-modules-extra` for the device-mapper targets. The job's probe step
fails with the missing piece named if a runner image stops providing them.
