# Contributing to ZeroDB

ZeroDB reimplements LMDB's engine in Rust behind heed's API. Its value rests
on behaving exactly like LMDB, so the rules below are short and strict. They
apply to every change, whoever or whatever writes it. To find your way around
the code first, read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Rules

1. **LMDB behavior is observed, not guessed.** The reference is the Meilisearch
   LMDB fork (`mdb.master.nested-rtxns`, vendored in `lmdb-master-sys 0.2.6` /
   heed 0.22.1), which is what Meilisearch runs; stock LMDB lacks nested read
   transactions. To learn what LMDB does, write a differential test in
   `crates/zerodb-oracle` and run it. A behavior difference is a ZeroDB bug
   unless it is recorded and approved in
   [docs/LMDB-COMPATIBILITY.md](docs/LMDB-COMPATIBILITY.md#behavior-differences).
   Never change an oracle expectation to make ZeroDB pass.
2. **Never delete, weaken, `#[ignore]` or loosen a test to make it pass.** If a
   test looks wrong, say so in the PR and let a maintainer decide.
3. **The spec describes the engine.** [docs/SPEC/](docs/SPEC/) is the reference
   for formats and algorithms. Read the relevant file before changing an area,
   and update it in the same change when behavior changes. When code and spec
   disagree, a deliberate change (an accepted ADR, an approved divergence)
   means the spec is updated; anything else is a bug to fix or report. Rule IDs
   such as `TXN-41` or `GC-16` are cited from code, tests and
   `zerodb-tools check` output: never renumber them; retire a rule explicitly
   instead.
4. **Clean room.** Reading LMDB or libmdbx C source to understand an algorithm
   is fine; transliterating C code is not. Implement from the spec.
5. **Design decisions get an ADR** (`docs/adr/`, template `0000-template.md`,
   index [docs/DECISIONS.md](docs/DECISIONS.md)): any on-disk format choice,
   concurrency protocol, fsync ordering or public API shape. For format,
   durability, GC, reader-table and other correctness-critical changes, the
   ADR is written and approved before the implementation.
6. **Test the riskiest assumption first.** Before a change that touches the
   on-disk format or spans more than three crates, land the smallest change
   that tests its riskiest assumption. Do not bundle unrelated improvements
   into a format change. For a feature no consumer needs yet, build a spike
   that shows the blast radius before a staged implementation: a spike shows
   in a few hundred lines what a staged implementation may only reveal after
   thousands.
7. **Performance claims need numbers.** Paste a criterion diff (or a
   `benches/results/` report) comparing LMDB, ZeroDB before and ZeroDB after,
   with the machine named; `just bench-ab` produces exactly that
   ([docs/PERFORMANCE.md](docs/PERFORMANCE.md#judging-one-change)). Speedups
   copy LMDB's own technique for the path where possible; a novel technique
   needs a stated reason.

Default behavior matches LMDB. Capabilities LMDB lacks, or that heed hides,
ship behind explicit options (such as `max_dirty_bytes`, `sequential_writes`
or trusted-file mode), so the default stays a faithful drop-in.

## `unsafe` policy

`unsafe` is allowed only in:

- mmap access and page casting (`zerodb-core::page`, `zerodb-io`),
  including the grow-only mapping cell (`zerodb-io::mmap::MapRegions`): an
  `AtomicPtr` to views and mappings freed only on drop — the view and mapping
  dereferences, the `Box::from_raw` calls in its `Drop` — with its
  `Send`/`Sync` impls (ADR-0023 amendment 15);
- shared-memory synchronization primitives in `zerodb-io::lockfile`
  (ADR-0023): atomic views over the mapped lock file
  (`AtomicU64::from_ptr` / `AtomicU32::from_ptr`, and one
  `slice::from_raw_parts` viewing the reader-slot table as
  `&[zerodb_core::shared::ReaderSlot]`), the Linux `futex` syscall, and
  `fcntl` record locks. They are exposed to `zerodb-core` only
  through the safe `SharedRegion` trait;
- the in-place `WRITE_MAP` path in `zerodb-core::dirty` (ADR-0021), detailed
  below;
- the CRC32C hardware path in `zerodb-core::page::crc32c` (ADR-0031): the one
  call of the `#[target_feature(enable = "crc")]` checksum function, guarded
  by runtime detection of that aarch64 CPU feature, and the same call in that
  module's test comparing it with the table path;
- FFI inside `zerodb-oracle`;
- the minimal API-shape `unsafe` in `heed-zerodb` that heed's pointer model
  requires: `Send` impls, TLS-marker retags, the lifetime-erased write cursor,
  the `ReservedSpace` uninitialized view, one `sysconf` for the OS page
  size, and the `unsafe fn resize` signature heed declares (ADR-0023
  amendment 15; its body holds no `unsafe` block) — nothing beyond what the
  mirrored heed surface forces;
- `zerodb-tools`: the `migrate-lmdb` feature's heed `open`
  (`src/migrate.rs`), in use and awaiting maintainer review;
- `benches/kv-compare` (a standalone crate outside the workspace): the two
  heed `open` calls, the `FileTrust::trust_contents` call, one `getrusage`
  and one `sync`, proposed in ADR-0033 and awaiting maintainer review.

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

linux-aarch64 (AWS Graviton 3 and 4) is the primary target; linux-x86_64 and
macOS aarch64 are for development. The database page size is chosen at
runtime (4 KiB to 64 KiB) and is independent of the OS page size: never assume
4 KiB OS pages, since ARM distributions often use 64 KiB.

## Before you open a PR

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo +nightly miri test -p zerodb-core   # non-mmap logic must pass miri
just fuzz-quick                           # differential fuzz against LMDB
just crash-test-quick                     # crash-consistency smoke
just loom                                 # reader-table model check, when touching readers or nested txns
just stress                               # 180 s reader/writer stress, before an integration gate
```

CI (`.github/workflows/ci.yml`) runs fmt, clippy, tests and the differential
fuzz on x86-64 and aarch64 for every PR, and miri, loom and the crash smoke on
x86-64; nightly it runs the long fuzz, the full crash run and the stress test
on both architectures. It also builds the consumer crates on the workspace
`rust-version` (the MSRV is for consumers; developing ZeroDB itself needs a
current stable toolchain for the bench and fuzz tooling). macOS clippy skips
Linux-only modules, so also lint the `x86_64-unknown-linux-gnu` target when a
change is lint-sensitive.

For changes that could affect Meilisearch or hannoy, also run the consumer gate
([docs/maintainers/CONSUMER-GATE.md](docs/maintainers/CONSUMER-GATE.md)).

## Style

- Rust 2021, with the MSRV pinned in the workspace `Cargo.toml`. Errors mirror
  heed's error taxonomy. Public items are documented. No `TODO` in merged
  code: open an issue instead.
- Write comments and docs so an outside reader can follow them. A comment
  states what something is, or why it is done this way, not the history of how
  it got there; measurements belong in `benches/results/` and the ADRs.
  Spec rule IDs and ADR numbers are fine, since they point to public
  documents.
- **No new dependency without an ADR.** The current allowlist: `memmap2`,
  `libc`, `thiserror`, `rand` (allowed, currently unused), `proptest`,
  `arbitrary`, `criterion`; the heed-surface re-exports `heed-traits`,
  `heed-types` and `byteorder` in `heed-zerodb` only (ADR-0003); `loom` under
  `cfg(loom)` in `zerodb-core` only (ADR-0006); `libfuzzer-sys` in `fuzz/` only
  (ADR-0001). `tempfile` is a dev-dependency of `heed-zerodb` without an ADR
  and awaits a maintainer decision.

## Where things are

| | |
|---|---|
| How the engine works | [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) |
| Format and algorithm specification | [docs/SPEC/](docs/SPEC/) |
| Design decisions | [docs/DECISIONS.md](docs/DECISIONS.md) |
| Docs index | [docs/README.md](docs/README.md) |
| Planned and open work | GitHub issues |
| Release history | [CHANGELOG.md](CHANGELOG.md) |
| Releasing | [docs/maintainers/RELEASING.md](docs/maintainers/RELEASING.md) |
| Security | [SECURITY.md](SECURITY.md) |

Completed work is recorded in `CHANGELOG.md` and in the PR description, and
every performance experiment, kept or reverted, in
`benches/results/perf-ledger.jsonl` (`just perf-ledger`).

## Reporting a bug

Open a GitHub issue with the ZeroDB commit, the consumer (Meilisearch or hannoy
version, or the native API), the platform and page size, and a reproduction.
For a suspected behavior difference from LMDB, the ideal report is an operation
sequence the oracle can replay. Report security issues as described in
[SECURITY.md](SECURITY.md).

## License

By contributing you agree that your contributions are licensed under the
project's dual MIT / Apache-2.0 license.
