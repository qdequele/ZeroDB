# AGENTS.md — working on ZeroDB

Guide for anyone changing this repository, human or AI agent. It states the
rules the project holds itself to; `CONTRIBUTING.md` covers the mechanics of
sending a change.

## What this project is

ZeroDB is a transactional, memory-mapped embedded key-value engine written in
pure Rust. It is a drop-in replacement for LMDB **at the API of `heed`** (the
Rust LMDB wrapper Meilisearch uses), not at the file-format level. Its first
consumers are Meilisearch's indexing and search core (`milli`) and its HNSW
vector index (`hannoy`).

Default behavior matches LMDB. Capabilities LMDB lacks, or that heed hides, ship
behind explicit options (for example `max_dirty_bytes`, `sequential_writes`,
trusted-file mode) so the default stays a faithful drop-in.

## Rules

1. **LMDB behavior is observed, not guessed.** The reference is the Meilisearch
   LMDB fork (`mdb.master.nested-rtxns`, vendored in `lmdb-master-sys 0.2.6` /
   heed 0.22.1 — what Meilisearch actually runs; stock LMDB lacks nested read
   transactions). To learn what LMDB does, write a differential test in
   `crates/zerodb-oracle` and run it. A behavior difference is a ZeroDB bug
   unless it is recorded and approved in `docs/DIVERGENCES.md`. Never change an
   oracle expectation to make ZeroDB pass.
2. **Never delete, weaken, `#[ignore]` or loosen a test to make it pass.** If a
   test looks wrong, say so in the PR and let a maintainer decide.
3. **The spec describes the engine.** `docs/SPEC/*.md` is the reference for
   formats and algorithms. Read the relevant file before changing an area, and
   update it in the same change when behavior changes. When code and spec
   disagree: a deliberate change (accepted ADR, approved divergence) means the
   spec is updated; anything else is a bug to fix or report. Rule IDs such as
   `TXN-41` or `GC-16` are cited from code, tests and `zerodb-tools check`
   output — never renumber them; retire a rule explicitly instead.
4. **Clean-room.** Reading LMDB or libmdbx C source to understand an algorithm
   is fine; transliterating C code is not. Implement from the spec.
5. **Design decisions get an ADR** (`docs/adr/`, template `0000-template.md`,
   index `docs/DECISIONS.md`): any on-disk format choice, concurrency protocol,
   fsync ordering or public API shape. For format, durability, GC, reader-table
   and other correctness-critical changes, the ADR is written and approved
   before the implementation.
6. **Test the riskiest assumption first.** Before a change that touches the
   on-disk format or spans more than three crates, land the smallest change
   that tests its riskiest assumption. Do not bundle unrelated improvements
   into a format change. For a feature no consumer needs yet, build a spike
   that reveals the blast radius before a staged implementation. (Learned the
   hard way: a first stage of duplicate-key support grew to 4,330 lines before
   review found it silently broke compaction and dump/load; a spike would have
   shown that in a few hundred lines.)
7. **Performance claims need numbers.** Paste a criterion diff (or a
   `benches/results/` report) comparing LMDB, ZeroDB before and ZeroDB after.
   Speedups copy LMDB's own technique for the path where possible; a novel
   technique needs a stated reason.

## `unsafe` policy

`unsafe` is allowed only in:

- mmap access and page casting (`zerodb-core::page`, `zerodb-io`);
- the in-place `WRITE_MAP` path in `zerodb-core::dirty` (ADR-0021), detailed
  below;
- FFI inside `zerodb-oracle`;
- the minimal API-shape `unsafe` in `heed-zerodb` that heed's pointer model
  requires: `Send` impls, TLS-marker retags, the lifetime-erased write cursor,
  the `ReservedSpace` uninitialized view, and one `sysconf` for the OS page
  size — nothing beyond what the mirrored heed surface forces;
- `zerodb-tools`: one `libc::flock` (`src/lock.rs`, the guard against opening a
  live environment) and the `migrate-lmdb` feature's heed `open`
  (`src/migrate.rs`). These two are in use and awaiting maintainer review.

The in-place `WRITE_MAP` exception covers only calling `zerodb-io`'s `unsafe`
map-slice broker to realize a dirty page in the writable map at a freshly
copied-on-write page number. Its `// SAFETY:` comments must state: a single
writer (TXN-6); the target page is referenced by no live snapshot (TXN-62);
exactly one live `&mut` per map region, tied to `&mut DirtyStore` and never
stored; and no stale whole-map borrow aliases an in-place write (in-place
transactions keep no cached whole-map view; heap modes re-derive theirs after
each spill). The broker is an `unsafe fn`, never a safe function minting `&mut`
from `&self`. The exception also covers the `Backing::map_dirty_page`
`unsafe fn` declaration in `zerodb-core::env` and the
`#[allow(clippy::mut_from_ref)]` on the broker, each with its exclusivity
contract inline.

`zerodb-core::readers` (the reader table) is lock-free with plain atomics and
contains no `unsafe`.

Every `unsafe` block carries a `// SAFETY:` comment stating the invariants it
relies on. No `#[repr(C)]` casts of possibly unaligned data — use explicit
offsets and `read_unaligned`. Every atomic uses an explicit `Ordering` with a
comment justifying it; assume a weak memory model (ARM), never "works on x86".

## Target platforms

linux-aarch64 (AWS Graviton 3/4) is primary; linux-x86_64 and macOS aarch64 are
for development. The database page size is chosen at runtime (4 KiB–64 KiB) and
is independent of the OS page size; never assume 4 KiB OS pages (ARM
distributions often use 64 KiB).

## Checks

Run before calling a change done:

```
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo +nightly miri test -p zerodb-core   # non-mmap logic must pass miri
just fuzz-quick                           # differential fuzz against LMDB
just crash-test-quick                     # crash-consistency smoke
just loom                                 # reader-table model check, when touching readers or nested txns
just stress                               # 180 s reader/writer stress, before an integration gate
```

CI (`.github/workflows/ci.yml`) runs fmt, clippy, test and fuzz-quick on x86-64
and aarch64 for every PR, and miri, loom and the crash smoke on x86-64; nightly
it runs the long fuzz, the full crash run and the stress test on both
architectures. macOS clippy skips Linux-only modules, so also lint the
`x86_64-unknown-linux-gnu` target when a change is lint-sensitive.

## Repo map

- `crates/zerodb-core` — pages, B+tree, transactions, free-page GC, reader
  table. No I/O policy.
- `crates/zerodb-io` — mmap reads, `pwrite`/`pwritev` commit backends, fsync
  strategies, and the fault-injection backend used by the crash tests.
- `crates/zerodb` — public engine API (heed-shaped).
- `crates/heed-zerodb` — heed 0.22.1 API surface over the engine.
- `crates/heed-shim` — a crate literally named `heed` re-exporting
  `heed-zerodb`; the `[patch.crates-io]` target consumers point at. Excluded
  from the workspace so it never collides with the oracle's real `heed`.
- `crates/zerodb-tools` — `stat`, `dump`, `load`, `check`, `migrate-from-lmdb`.
  The optional `migrate-lmdb` feature links C LMDB through heed.
- `crates/zerodb-oracle` — differential harness against C LMDB, plus the
  criterion benchmark ladder (`benches/engine_comparison/`).
- `fuzz/` — differential and decoder fuzz targets.
- `benches/results/` — dated benchmark reports and `perf-ledger.jsonl`, the
  record of every performance experiment (kept or reverted).
- `scripts/` — benchmark A/B, profiling and consumer-suite runners.
- `docs/SPEC/` — format and algorithm spec. `docs/adr/` — design decisions,
  indexed in `docs/DECISIONS.md`. `docs/DIVERGENCES.md` — approved behavior
  differences from LMDB. `docs/PERF-GAP-VS-LMDB.md` — performance inventory.

## Style

Rust 2021, MSRV pinned in the workspace `Cargo.toml`. Errors mirror heed's
error taxonomy. Public items are documented. No `TODO` in merged code — open an
issue instead.

No new dependency without an ADR. Current allowlist: `memmap2`, `libc`,
`thiserror`, `crossbeam-utils`, `rand` (allowed, currently unused), `proptest`,
`arbitrary`, `criterion`; the heed-surface re-exports `heed-traits`,
`heed-types` and `byteorder` in `heed-zerodb` only (ADR-0003); `loom` under
`cfg(loom)` in `zerodb-core` only (ADR-0006); `libfuzzer-sys` in `fuzz/` only
(ADR-0001). `tempfile` is a dev-dependency of `heed-zerodb` without an ADR and
awaits a maintainer decision (`crates/zerodb/tests` hand-roll a `TempDir` in 31
files to avoid it).

Write comments and docs so an outside reader can follow them: describe what
something is rather than citing internal shorthand. Spec rule IDs and ADR
numbers are fine — they point to public documents.

## Tracking work

Planned and open work lives in GitHub issues. Completed work is recorded in
`CHANGELOG.md`, in the PR description, and — for performance experiments — in
`benches/results/perf-ledger.jsonl`.
