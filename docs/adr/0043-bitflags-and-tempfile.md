# ADR-0043: Adopt `bitflags` for the flag types and `tempfile` for test directories

- Status: Proposed (awaiting maintainer decision; no code changed)
- Related issue: none
- Date: 2026-11-12

## Context

CONTRIBUTING.md allows no new dependency without an ADR. Two places in the
workspace re-implement what a small, widely used crate provides.

**Flag types.** Three flag sets are written by hand:

| Type | Where | Lines |
|---|---|---:|
| `EnvFlags`, `DatabaseFlags`, `PutFlags` (heed adapter) | `crates/heed-zerodb/src/flags.rs`, a local `flags!` macro and its three uses | 166 (the whole file) |
| `EnvFlags` (native API) | `crates/zerodb/src/lib.rs` | 76 |
| `PutFlags` (engine) | `crates/zerodb-core/src/rwtxn/insert.rs` | 27 |

heed 0.22.1 defines its flag types with `bitflags!` (`bitflags = "2.9.4"` in
its manifest, `src/mdb/lmdb_flags.rs`), so every method `bitflags` generates
is part of heed's public API. The adapter's macro reproduces only part of it.
[LMDB-COMPATIBILITY.md](../LMDB-COMPATIBILITY.md#envflags) lists the rest as
unsupported: `from_bits_retain`, `from_name`, `iter`, `iter_names`, `set`,
`toggle`, `difference`, `symmetric_difference`, `complement`,
`Extend`/`FromIterator`, hex and binary formatting, and `serde`. `Debug`
prints the numeric value where heed prints the flag names. Meilisearch and
hannoy do not call the missing methods, but a heed user that does would not
compile on ZeroDB.

**Temporary directories in tests.** `tempfile` is already a dev-dependency of
`heed-zerodb`, used by eight of its test files and one unit-test module. It
was added without an ADR, and CONTRIBUTING.md records that it awaits a
decision. The other crates do not use it. Seven files define their own
temporary directory or file helper instead:

- `crates/zerodb-oracle/src/tempdir.rs` (44 lines, used by the oracle's tests)
- `crates/zerodb/tests/common/mod.rs`
- `crates/zerodb-tools/tests/common/mod.rs`
- `crates/zerodb/src/copy_tests.rs`
- `crates/zerodb-io/tests/lockfile_processes.rs`
- `crates/zerodb-io/src/lib.rs` (unit tests)
- `crates/zerodb-io/src/lockfile.rs` (unit tests)

## Options

### Option A — Keep both as they are

No change. The adapter's flag surface stays a subset of heed's, recorded in
LMDB-COMPATIBILITY.md. The `tempfile` dev-dependency of `heed-zerodb` still
needs a decision of its own: approve it there, or replace it with a helper.

- For: no new dependency in the published crates. The hand-written types are
  short and have no known bugs.
- Against: a known gap in the heed API stays open, and there are seven
  temporary-directory helpers to keep correct (unique names, removal on drop).

### Option B — `bitflags` in `heed-zerodb` only, `tempfile` as a workspace dev-dependency

The adapter defines its three flag types with `bitflags!`, with the same
derives as heed, so its flag API is heed's by construction. The native
`zerodb::EnvFlags` and the engine's `PutFlags` stay hand-written. `tempfile`
becomes a workspace dev-dependency and replaces the seven helpers.

- For: closes the API gap where it matters, in the drop-in crate. The
  crates published on crates.io (`zerodb`, `zerodb-core`, `zerodb-io`) gain no
  dependency.
- Against: two styles of flag type remain in the workspace.

### Option C — `bitflags` in all three crates, `tempfile` as a workspace dev-dependency

As Option B, and the native `EnvFlags` and the engine's `PutFlags` also use
`bitflags!`.

- For: one style, and about 270 hand-written lines replaced by three
  declarations and their doc comments.
- Against: `bitflags` becomes a dependency of the published `zerodb-core` and
  `zerodb`, and its types become part of `zerodb`'s public API, so a
  `bitflags` major version bump is a breaking change for `zerodb`. The native
  API's method names change where they differ today (for example the engine's
  `PutFlags::EMPTY` constant against `bitflags`' `empty()`).

### Costs common to B and C

- **Dependency count.** `bitflags` 2.x has no required dependencies. It and
  `tempfile` are already in the workspace `Cargo.lock` (`bitflags` 2.13.2
  through the oracle's real `heed`, `proptest` and `rustix`; `tempfile` 3.27.0
  through `heed-zerodb`), so the lock file does not grow. A consumer that
  patches `heed` with the adapter already resolves `bitflags`, because heed
  0.22.1 depends on it. `tempfile` is a dev-dependency and never reaches a
  consumer.
- **MSRV.** `bitflags` 2.13.2 declares Rust 1.56 and `tempfile` 3.27.0 declares
  Rust 1.63. The workspace MSRV is 1.98.
- **Compile time.** Not measured. It should be measured on a clean build of
  `heed-zerodb` before and after, and recorded here.
- **Crash safety and ARM.** None: flag types are plain integers, and
  `tempfile` is used by tests only.
- **Behavior.** `Debug` output of the adapter's flags changes from a number to
  flag names, which is heed's output. Bit values do not change.

## Decision

Not decided. The author's recommendation is Option B: it fixes the one
user-visible problem, the gap in heed's API, without adding a dependency to
the published engine crates.

## Consequences

If Option B or C is accepted:

- CONTRIBUTING.md's dependency allowlist gains `bitflags` (with the crates
  allowed to use it) and `tempfile` as a dev-dependency, and loses the
  sentence about `tempfile` awaiting a decision.
- LMDB-COMPATIBILITY.md's `EnvFlags` table changes the bitflags rows from
  Unsupported and Emulated to Same, except `serde`, which stays unsupported
  unless the `serde` feature is wired through.
- A test compares the adapter's flag API with heed's: bit values, `Debug`
  output, and iteration.
- The oracle's `tempdir` module and the six other helpers are removed in the
  same change, or in a follow-up that changes tests only.

## Open questions for human review

1. Option A, B or C?
2. Should the `serde` feature of the adapter's flag types follow heed's
   (`bitflags/serde`)? Today the adapter's `serde` feature does nothing.
3. Two of the seven helpers name a temporary file, not a directory
   (`TempFile` in `zerodb-io/src/lib.rs`, `TmpLock` in
   `zerodb-io/src/lockfile.rs`). Replace them too, or leave them?
