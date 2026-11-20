# Hardening runs: the 24-hour soak and the page-size matrix

Two runs need hardware that GitHub's hosted runners do not give: a fuzz,
crash and stress soak longer than the 6-hour job cap, and the test suite on
a kernel whose OS page is not 4 KiB. Both are scripted; this page says which
machine to use, how to run them, and how to record the result. The README
lists both as open until a recorded run exists.

| Run | Command | Length | Machine |
|---|---|---|---|
| Soak | `just soak` (`scripts/soak.sh`) | 24 h per architecture | dedicated, ≥ 16 CPUs, x86-64 and aarch64 |
| Page-size matrix | `just page-size-matrix` (`scripts/page-size-matrix.sh`) | about 1.5 h | aarch64 with a 64 KiB-page kernel |

Run the aarch64 soak on the 64 KiB-page machine, after the matrix: that one
machine then covers both open items for aarch64.

## Why the OS page size matters

The database page size (`psize`, 4 to 64 KiB, SPEC 02 §0) is independent of
the OS page size, but several paths work in OS pages:

- `mmap` maps whole OS pages. A read in an OS page that lies wholly past the
  end of the file faults (`SIGBUS`); a read in the last, partly backed OS page
  returns zeros past the end. With a 64 KiB OS page and 4 KiB DB pages, up to
  15 DB pages share one OS page, and the file's end usually falls inside one.
- The ranged `msync` of the `WRITE_MAP` commit (SPEC 06 REC-12) must start on
  an OS-page boundary. memmap2 rounds the start down; `zerodb-io` merges page
  ranges closer than 64 KiB so one OS page is not flushed twice
  (`msync_byte_ranges`).
- `madvise(MADV_RANDOM)` (`NO_READ_AHEAD`) applies to whole OS pages.
- The heed adapter checks that `map_size` is a multiple of the OS page size,
  as LMDB does, and gives a new store the OS page size as its page size.
- The lock file is mapped too, never longer than the file, so its last OS
  page is the only partly backed one.

On a 4 KiB-page kernel the DB page is never smaller than the OS page, so the
first two points are never exercised with several DB pages per OS page. That
is the case a 64 KiB kernel, or a 16 KiB one, reaches.

`crates/zerodb/tests/os_page_size.rs` exercises each of these paths at DB
pages of 4, 16 and 64 KiB in every backing (default, `NO_READ_AHEAD`,
`WRITE_MAP`, `WRITE_MAP` with `NO_READ_AHEAD` and with `MAP_ASYNC`), and
checks that a run on a host whose OS page is larger than the DB page did reach
file lengths ending mid-OS-page. It runs in the normal `cargo test` suite on
every host; it only proves something new on a host whose OS page is not
4 KiB.

## The page-size matrix

### Machine

Any aarch64 Linux host whose `getconf PAGESIZE` prints `65536`. Two ways to
get one:

- **RHEL 9 for ARM, or a rebuild (Rocky, AlmaLinux).** RHEL 9 boots a 4 KiB
  kernel by default and ships a 64 KiB one as the `kernel-64k` package:

  ```bash
  sudo dnf install -y kernel-64k
  sudo grubby --set-default "$(ls /boot/vmlinuz-*64k* | tail -1)"
  sudo reboot
  ```

- **Ubuntu 22.04 or 24.04 for arm64**, with the 64 KiB kernel flavor:

  ```bash
  sudo apt-get install -y linux-generic-64k
  ```

  then select that kernel in GRUB (or the cloud image's boot configuration)
  and reboot.

On AWS, a Graviton instance (`c7g`, `m7g`, `c8g`; `.2xlarge` or larger) with
either image works. Apple silicon cannot host a 64 KiB-page guest: its CPUs
offer only 4 KiB and 16 KiB translation granules, so a Linux VM on a Mac is a
16 KiB data point at best.

Whatever the machine, check before running, and stop if it prints anything
else:

```bash
getconf PAGESIZE
```

### Setup

```bash
# A C toolchain (the oracle builds LMDB), git, and rustup.
sudo dnf install -y gcc git            # or: sudo apt-get install -y build-essential git
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"
rustup toolchain install nightly
cargo install cargo-fuzz --version 0.13.2 --locked
cargo install just --version 1.58.0 --locked
git clone https://github.com/qdequele/ZeroDB.git && cd ZeroDB
```

Check out the commit to test (the release candidate, or `main`).

### Run

```bash
just page-size-matrix
```

It runs, in order, recording each step and continuing past a failure:

1. `cargo test --workspace`, the whole suite. On this host the heed adapter's
   default page size is 64 KiB, and the OS-page-size battery runs its 4 KiB
   and 16 KiB cases with several DB pages per OS page.
2. The crash harness at DB pages of 4, 16 and 64 KiB
   (`ZERODB_CRASH_PAGE_SIZE`): 2,000 single-process and 200 multi-process
   cycles each.
3. The two reader/writer stress tests, 180 s each.
4. The fuzz gate: `diff_ops` native and through the heed adapter, and the two
   decoder targets, 600 s each. LMDB uses 64 KiB pages here, so the
   differential compares a 4 KiB native ZeroDB, and a 64 KiB adapter ZeroDB,
   with a 64 KiB LMDB.

`CRASH_CYCLES`, `CRASH_MP_CYCLES`, `STRESS_SECS` and `FUZZ_SECS` change the
amounts; the header of `scripts/page-size-matrix.sh` lists them. Everything
lands in `target/page-size-matrix/<UTC timestamp>/`, with `summary.md` on top.

## The soak

### Machine

A machine that does nothing else for 24 hours, with at least 16 CPUs and
32 GB of memory, on local disk (not a network filesystem). Run once on
x86-64 and once on aarch64. Any Linux distribution with a C toolchain works;
the setup is the one above. A laptop works for a rehearsal, not for the
record: sleep, thermal throttling and other work make the counts
incomparable.

### Run

Rehearse for a few minutes first, to catch a setup problem before the long
run:

```bash
HOURS=0.1 CRASH_ROUND_CYCLES=200 scripts/soak.sh
```

Then, from a session that survives logging out (`tmux`, `screen`, or
`systemd-run --user`):

```bash
just soak
```

For 24 hours, in parallel:

- `diff_ops` native against LMDB, in libFuzzer fork mode on half the CPUs;
- `diff_ops` through the heed adapter on an eighth of them;
- `fuzz_page_decode` and `fuzz_image_open`, one worker each;
- the crash harness on a quarter of the CPUs, in rounds of 10,000
  single-process and 500 multi-process cycles, cycling the DB page size through the seed's own (4 or 8 KiB),
  16 KiB and 64 KiB;
- the two reader/writer stress tests, in rounds of up to 30 minutes.

Fuzzing keeps going after a finding, so a 24-hour run reports every distinct
failing input rather than stopping at the first. The output directory,
`target/soak/<UTC timestamp>/`, holds `summary.md` (host, commit, sequence
counts, verdict), every log, each fuzz job's corpus and artifacts, and the
crash harness's repro artifacts. Copy it off the machine before releasing
it.

### A finding

- A fuzz artifact: `just fuzz-decode <artifact>` prints the operation
  sequence; replay it with the fuzz binary, as in CI.
- A crash-harness violation: the log line carries the repro command,
  including the page size when it was pinned
  (`ZERODB_CRASH_PAGE_SIZE=65536 crash-harness --repro image:<seed>`).
- A stress failure: the panic message in `stress/stress.log` names the test.

Open an issue per distinct finding with the artifact attached, and record it
in the PROVENANCE entry below whatever its state.

## Recording a run

1. Copy the run's `summary.md` to `docs/hardening-runs/` as
   `<date>-<arch>-<os page>-<soak|page-size-matrix>.md`. Keep the logs and
   artifacts somewhere durable and link them if they are public.
2. In `docs/PROVENANCE.md`, move the item from "What has not been verified"
   to "What has been verified", with the date, the machine, the commit, the
   sequence counts and the findings, linking the summary.
3. When both a 24-hour soak per architecture and a 64 KiB-kernel matrix that
   passed are recorded, remove the two items from the README status
   paragraph and from its "Not verified so far" list, and drop the
   64K-kernel caveat from "What 0.x promises" in
   [RELEASING.md](RELEASING.md).

## Making it recurring

[RELEASING.md](RELEASING.md#hardening-runs) proposes both runs as a release
step. A self-hosted runner would make the matrix automatic; the
[release step](RELEASING.md#hardening-runs) says what that would take. Nothing
here enables one.
