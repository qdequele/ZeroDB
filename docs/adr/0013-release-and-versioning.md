# ADR-0013: Release and versioning policy for the 0.x line

- Status: Proposed (agent-drafted 2026-09-09 for the first release; awaiting human approval)
- Milestone: first release (v0.1.0)
- Date: 2026-09-09

## Context

ZeroDB is about to be proposed to the Meilisearch team as a drop-in for
heed/LMDB. That requires something a consumer can pin: a version, a statement of
what it promises, and a repeatable way to cut the next one. Today every engine
crate is `0.0.1`, nothing is tagged, and there is no changelog or release
workflow.

Forces:

- **Two version lines coexist.** `heed-zerodb` and `heed-shim` carry heed's
  version (`0.22.1`) because cargo's `[patch.crates-io]` requires the
  replacement to satisfy the consumer's `heed = "0.22.1"` requirement
  (ADR-0003, `crates/heed-shim/Cargo.toml`). That number cannot be ZeroDB's
  version. The engine crates (`zerodb-core`, `zerodb-io`, `zerodb`,
  `zerodb-tools`, `zerodb-oracle`) can carry a real ZeroDB version.
- **crates.io is not usable for the adapter.** `heed-shim` is literally named
  `heed`, which is taken; `heed-zerodb` is consumed only through the shim.
  Publishing the engine crates alone would give users a `zerodb` they cannot
  plug into Meilisearch without the git shim anyway, so publishing buys little
  for 0.x and adds a second, semver-enforced surface to maintain.
- **The frozen contract is heed's.** PLAN.md ground rule 2: no heed-mirrored
  signature changes in Phases 0–2. ZeroDB-only extensions (`Env::sync(force)`,
  `reader_list`, `live_readers`, `EnvOpenOptions::page_size`,
  `copy_to_file_with_progress`, `Comparator`) are the only API that can move.
- **The on-disk format is versioned** (`FORMAT_VERSION = 1`,
  `crates/zerodb-core/src/page/mod.rs`) and a mismatch is refused at open
  (SPEC 02 §3, SPEC 06). D-002 sanctions no LMDB file compatibility; migration
  is logical (`zerodb-tools dump`/`load`, `migrate-from-lmdb`).
- **Phase 1's exit criteria are not all met** (PLAN §1.14: 24 h fuzz soak,
  Graviton 4K/64K bench, CI). The first release must say so rather than imply
  production readiness.

## Options

### Option A — git tags + GitHub releases, no crates.io (0.x)

`vX.Y.Z` tags on `main`; a release workflow runs the gate, builds
`zerodb-tools` for linux x86-64, linux aarch64 and macOS aarch64, and publishes
a GitHub release whose notes are the CHANGELOG section. Consumers pin the tag
through the shim: `[patch.crates-io] heed = { git =
"https://github.com/qdequele/ZeroDB", tag = "vX.Y.Z" }` — cargo finds the crate
named `heed` in that repository (`crates/heed-shim`) by name; a `package =`
rename inside `[patch]` does not work (ADR-0003, verified M1.13).

- Pros: one surface to maintain; honest about maturity; the artefact people
  actually need (the tools binary) is delivered; nothing to yank later.
- Cons: no `cargo add zerodb`; docs.rs pages do not exist.

### Option B — Option A plus `cargo publish` of the engine crates

Publish `zerodb-core`, `zerodb-io`, `zerodb`, `zerodb-tools` at the same
version; keep `heed-zerodb`/`heed-shim`/`zerodb-oracle` `publish = false`.

- Pros: docs.rs, `cargo install zerodb-tools`, discoverability.
- Cons: semver becomes enforceable on `zerodb`'s native API before it has a
  second consumer; every path dependency needs a `version =`; publishing is
  irreversible (yank only). The native `zerodb` API has exactly one consumer
  today (the adapter) and no stability commitment has been designed.

### Option C — tag only

A tag and a changelog, no binaries.

- Cons: `zerodb-tools` is the one thing an operator needs without a Rust
  toolchain (offline `check`, `dump`, `load` of a Meilisearch data dir).

## Decision

**Option A** for the whole 0.x line. Revisit publishing (Option B) when either a
second native-API consumer exists or Meilisearch adopts the shim officially and
wants a crates.io dependency; that revisit is a new ADR.

Versioning rules while 0.x:

1. **One version for the engine crates.** `zerodb-core`, `zerodb-io`, `zerodb`,
   `zerodb-tools`, `zerodb-oracle` share `X.Y.Z`; the tag is `vX.Y.Z`; the
   release workflow refuses a tag that differs from `crates/zerodb`'s version.
   `heed-zerodb` and `heed-shim` stay at the heed line they mirror.
2. **Minor bump (0.Y)**: any change to a ZeroDB extension API, any
   `FORMAT_VERSION` bump, any MSRV bump, any new sanctioned divergence. A
   `FORMAT_VERSION` bump is itself an ADR (CLAUDE.md rule 6) and the CHANGELOG
   entry's first line states it with the dump/load migration path.
3. **Patch bump (0.Y.Z)**: bug fixes, performance, docs, tooling, with the
   on-disk format and every API unchanged.
4. **heed-mirrored signatures never change** (ground rule 2). If heed itself
   moves to 0.23, that is a new adapter line, a new ADR, and at least a minor
   bump.
5. **What a release promises**: files written by vX.Y.Z open unchanged in every
   later release with the same `FORMAT_VERSION`; the full CLAUDE.md gate and the
   consumer gate (docs/CONSUMER-GATE.md) passed on the release commit, with the
   Meilisearch and hannoy refs named in the CHANGELOG; known gaps are listed in
   the release notes, never implied away.
6. **MSRV** is the workspace `rust-version`; it is checked in CI and bumped
   only in a minor release.

## Consequences

- New files: `CHANGELOG.md` (Keep a Changelog shape; the release workflow
  extracts the tagged section), `.github/workflows/release.yml`,
  `docs/RELEASING.md`, `SECURITY.md`, `CONTRIBUTING.md`, `docs/COMPATIBILITY.md`
  (the heed/LMDB coverage matrix a release is measured against).
- Engine crates bump `0.0.1 → 0.1.0`; `heed-zerodb`, `heed-shim`, and
  `zerodb-oracle` get `publish = false` so a stray `cargo publish` cannot
  create the second surface this ADR declines.
- `zerodb-tools --version` prints the crate version and `FORMAT_VERSION`, so a
  support request can name both.
- README drops "not published to crates.io; API may still move" in favour of a
  pointer to this policy.

## Open questions for human review

1. Is the "no crates.io for 0.x" call acceptable, or is docs.rs presence wanted
   from day one (Option B)?
2. ~~Should `v0.1.0` wait for the 24 h fuzz soak?~~ **Answered 2026-09-09
   (Quentin, chat): ship 0.1 with the 24 h soak, the Graviton bench and the
   64K-page kernel listed as known gaps in the release notes.** Also answered:
   the release is a git tag + GitHub release with prebuilt `zerodb-tools`
   (Option A), no crates.io — pending only the formal ratification of this ADR.
3. Tag signing: plain annotated tags, or require signed tags from a maintainer
   key?
