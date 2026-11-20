# ADR-0048: heed 0.23 support in the heed adapter

- Status: Proposed, awaiting maintainer approval. Nothing is implemented in
  `main`.
- Related issue: follow-up of #179 (consumer gate); PR qdequele/ZeroDB#191
- Date: 2026-11-20

## Context

`crates/heed-zerodb` is a package named `heed` at version 0.22.1, and consumers
use it through `[patch.crates-io]` (ADR-0003). Cargo applies a patch only when
the replacement's version satisfies the consumer's requirement. A package at
0.22.1 does not satisfy `heed = "0.23"`.

Meilisearch v1.56.0 and later, hannoy v0.3.0, arroy v0.9.0 and cellulite
v0.4.0 depend on heed 0.23.0 from crates.io. On Meilisearch v1.57.0, the
current `[patch]` is ignored and `lmdb-master-sys` stays in the build, so the
consumer gate cannot move past Meilisearch v1.55.1 (`CONSUMER-GATE.md`).
v1.57.0 resolves one heed, 0.23.0 from crates.io, with no git patch of its
own. Meilisearch's fork (ADR-0027) is no longer used.

### What heed 0.23.0 changes

heed 0.23.0 depends on the same `lmdb-master-sys 0.2.6`, `heed-traits 0.20.0`
and `heed-types 0.21.0` as 0.22.1. The LMDB under it is therefore the same
reference fork (CONTRIBUTING rule 1). No LMDB behavior changes, only the Rust
API. The public API diff of the two crates' sources:

- **Opening databases moves into a closure.**
  - `Env::open_database`, `Env::create_database` and `Env::database_options`
    are removed.
  - They are replaced by `Env::open_databases_and_commit(rtxn, f)` and
    `Env::create_databases_and_commit(wtxn, f)`. Each takes the transaction by
    value, holds a per-environment lock while `f` runs, commits the
    transaction, and returns `CommitOrAbort<D::Committed, A>`.
  - `f` receives a new `DatabaseOpener<'e, T>`, which has `database_options`,
    `open_database` and `create_database`.
  - The lock serializes database opening, as in Meilisearch's fork
    (ADR-0027), but without a "unique transaction" type.
- **New public types.**
  - `CommitOrAbort<T, A>`: an enum with `commit`, `abort`, `is_abort`,
    `unwrap_commit` and `unwrap_abort`, plus `From` conversions with `Result`.
  - `OnCommit`: a trait with `type Committed; fn on_commit(self, &CommitToken)`.
    It is implemented for `()`, `Option`, `Result`, arrays, tuples of up to 12
    elements, and `Database`.
  - `CommitToken`: the environment's identity. Its constructor is private.
  - Meilisearch implements `OnCommit` for its own types: 9 `OnCommit` and 8
    `CommitToken` mentions in v1.57.0's `crates/`.
- **`Database` gains a lifetime.**
  - The type becomes `Database<'t, KC, DC, C, CDUP>`.
  - A handle returned inside the closure is tied to the opener. `on_commit`
    turns it into `Database<'static, …>` after checking that the token and
    the handle come from the same environment.
  - `DatabaseOpenOptions::open` and `create` return `Database<'e, …>`.
  - The `remap_*` methods and `lazily_decode_data` take `self` instead of
    `&self` and keep the `CDUP` parameter, which 0.22.1 drops.
- **Commits.** `RoTxn::commit` becomes crate-private: a read transaction
  commits only through `open_databases_and_commit`. `RwTxn::commit` stays
  public.
- **Statistics.**
  - `Env::stat() -> EnvStat` becomes `Env::unnamed_database_stat() -> DatabaseStat`.
  - `EnvStat` and `Env::non_free_pages_size` are removed.
  - `DatabaseStat::non_free_page_size` is added: plain arithmetic, the same
    as the fork's.
- Everything else differs only in documentation: iterators, flags,
  `EnvOpenOptions`, and the database read and write methods.

The adapter needs no new engine capability for any of this. Commit-on-open
already follows LMDB's handle rules (ADR-0024), and the environment identity
check already exists (`Database::env_ident`).

### Spike (what was tested)

To test the riskiest assumption first (CONTRIBUTING rule 6), I copied the
adapter outside the repository, set its version to 0.23.0, and made the API
changes above. That added about 300 lines across `env.rs`, `database.rs`,
`txn.rs`, `lib.rs` and a new `commit.rs`, which holds `CommitOrAbort`,
`OnCommit` and `CommitToken`. The spike also dropped the `unique-txns` module
and the tests.

Then I patched it into Meilisearch v1.57.0 with
`[patch.crates-io] heed = { path = … }`:

- `cargo tree -i lmdb-master-sys` finds C LMDB in neither `milli` nor
  `index-scheduler`. heed 0.23.0 resolves to the adapter for milli, hannoy
  0.3.0, arroy 0.9.0 and cellulite 0.4.0.
- `cargo test -p milli -p index-scheduler` compiles with no Meilisearch source
  change. It passes on 2026-11-20 on macOS aarch64 (rustc 1.98.1, from the
  consumer's toolchain file): 5 test binaries, 448 passed, 0 failed,
  4 ignored. v1.55.1 gives the same counts on the 0.22.1 adapter, because the
  two releases have the same tests in those crates; what changed between
  them is the migration to heed 0.23.
- hannoy v0.3.0 (`7d26c3c6`), patched the same way, also loses
  `lmdb-master-sys`. Its suite, with `--no-fail-fast` on the stable toolchain
  (hannoy pins Rust 1.90), gives 50 passed, 10 failed, 1 ignored.
  - All ten failures are `insta` snapshots that record hannoy version 0.2.0
    while the tag builds 0.3.0. This is the stale-snapshot problem v0.1.7 has
    (`CONSUMER-GATE.md`).
  - Stock LMDB fails the same ten, plus three that fail with `EINVAL` at open
    on macOS.
  - So hannoy v0.3.0 cannot go in the gate as it stands, on either engine.

Packaging, also tested:

- **Cargo rejects two workspace members with the same name**: "two packages
  named `heed` in this workspace". A 0.23.0 package therefore cannot sit next
  to the 0.22.1 one as a workspace member.
- **A git `[patch]` stops resolving when a repository holds two `heed`
  packages.** Even with the second one excluded from the workspace, cargo
  reports "patch for `heed` … resolved to more than one candidate (found
  versions: 0.22.1, 0.23.0)". It resolves again once the patch line names the
  version, for example `heed = { git = "…", version = "0.23" }`, and each line
  then picks its own package.

## Options

### Option A — A second, out-of-workspace `heed` 0.23.0 package sharing the adapter's source

Add `crates/heed-zerodb-0.23/`. Its manifest names the package `heed` at
version 0.23.0, excluded from the workspace. Its `[lib] path` points at
`crates/heed-zerodb/src/lib.rs`. A build script (no dependency) emits
`cfg(heed_0_23)` from `CARGO_PKG_VERSION`. The shared source selects the 0.23
API under that cfg, as it already selects the fork's API under `unique-txns`.

- Both consumer lines keep working: Meilisearch v1.55 on 0.22.1 with
  `unique-txns`, and v1.56 and later on 0.23.0. One engine, one source tree.
- A feature cannot select the API instead of the build script. Consumers set
  `default-features = false` on heed, and a `[patch]` cannot enable features
  (ADR-0027).
- **Breaking for git-patch consumers.** Every `[patch.crates-io] heed = { git = … }`
  line must add `version = "0.22"` or `"0.23"` once the repository holds two
  `heed` packages. This is a one-line documentation change in `USING.md` and
  `README.md`, but every existing consumer manifest has to change. Path
  patches, like the ones `scripts/patch-heed.py` writes, are unaffected.
- The 0.23 package is outside `cargo test --workspace` and clippy. CI and the
  CONTRIBUTING check list need a `--manifest-path` run for it. The adapter
  tests would need a 0.23 variant for the opening and commit paths (most read
  and write tests do not open databases differently).
- Three API surfaces in one source: 0.22.1, the fork, and 0.23. Each one adds
  cfg branches to `env.rs`, `database.rs` and `txn.rs`.
- The oracle keeps linking real heed `=0.22.1` (ADR-0001) against the 0.22.1
  package. Its reference LMDB is the same one 0.23.0 uses.

### Option B — Move the adapter to heed 0.23.0 only

Bump `crates/heed-zerodb` to 0.23.0 and replace the 0.22.1 API with the 0.23
one. Retire `unique-txns` (ADR-0027) and move the oracle to real heed
`=0.23.0` (amending ADR-0001: same `lmdb-master-sys 0.2.6`, so the same
reference behavior).

- One API, no cfg branching, no `version` key in patch lines. The workspace
  stays as it is.
- Drops Meilisearch v1.55 and earlier, hannoy 0.2 and earlier, and every other
  heed 0.22 consumer. They stay on the last ZeroDB release with the 0.22.1
  adapter.
- A large mechanical port of the repository's own callers:
  - `create_database`: 279 sites in 85 files
  - `open_database`: 126 sites in 43 files
  - `.stat()`: 152 sites in 49 files
  - `non_free_pages_size`: 30 sites in 13 files

  These sit across the adapter tests, the oracle harness and examples,
  `zerodb-tools`, the fuzz targets and `benches/kv-compare`. Each needs review
  because test code changes; no expectation changes.
- The consumer gate moves to Meilisearch v1.57.0 in the same change. hannoy
  has no green 0.23 release to pin yet (see the spike).

### Option C — A long-lived branch or a separate repository for the 0.23 line

- No packaging conflict.
- Every engine fix has to be carried to two lines.
- Rejected: the engine is the shared, risky part, and splitting it doubles the
  release and gate work.

## Decision

Pending maintainer review. Recommendation: **Option A now, Option B later.**

- **Option A now** unblocks the consumer gate on current Meilisearch without
  dropping v1.55 or porting about 600 call sites in one change. It changes no
  test expectation and no engine behavior.
- **Option B later**, as its own decision, once no consumer needs heed 0.22.
  At that point the 0.22.1 and fork surfaces are deleted, and the 0.23 package
  moves back into the workspace.

If the `version` key in every git patch line is not acceptable, Option B is
the alternative. It costs the port, and loses Meilisearch v1.55 on new ZeroDB
releases.

## Consequences (Option A)

- **New**:
  - `crates/heed-zerodb-0.23/Cargo.toml` and its `build.rs`.
  - `cfg(heed_0_23)` branches in the adapter: `DatabaseOpener`, the two
    `*_and_commit` methods, the commit types, the `Database` lifetime,
    `unnamed_database_stat`, the `remap_*` signatures, and a crate-private
    `RoTxn::commit`.
  - `unique-txns` has no meaning under `heed_0_23`. Enabling both is a
    compile error.
- **Tests**:
  - A 0.23 variant of the adapter tests that open or create databases, and of
    the env-identity panic tests (`on_commit` with another environment's
    token).
  - Differential coverage stays on the 0.22.1 surface, since the engine calls
    under both surfaces are the same.
  - A test that the per-environment open lock serializes two concurrent
    `*_and_commit` calls, as heed's does.
- **Scripts**:
  - `scripts/patch-heed.py` and `scripts/consumer.sh` choose the adapter
    directory from the heed version in the consumer's `Cargo.lock`.
  - The consumer-gate CI matrix adds Meilisearch at a 0.23 release
    (v1.57.0) and keeps v1.55.1 and hannoy v0.2.0. hannoy v0.3.0 joins once
    a hannoy release fixes its snapshots.
- **Docs**:
  - `USING.md` and `README.md` show patch lines with `version`.
  - `LMDB-COMPATIBILITY.md` lists heed 0.23 coverage.
  - `CONTRIBUTING.md` adds the `--manifest-path` checks.
  - `RELEASING.md` keeps the 0.23 package at 0.23.0, as it keeps the 0.22.1
    one at its heed version.
- **CI**: fmt, clippy and test for the 0.23 package by `--manifest-path`.

## Open questions for human review

1. Is requiring `version = "0.22"` / `"0.23"` in git patch lines acceptable,
   or should support for heed 0.22 end instead (Option B)?
2. Should the per-environment open lock be reproduced? ZeroDB does not need it
   for safety (ADR-0027). heed 0.23 holds it while the closure runs, and a
   caller could rely on that ordering. The recommendation is to reproduce it,
   as for the fork.
3. Should the 0.23 surface get its own differential tests against real heed
   0.23.0, or is coverage of the shared engine calls through the 0.22.1
   surface enough?
