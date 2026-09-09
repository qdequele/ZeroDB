# ADR-0013: Release and versioning policy for the 0.x line

- Status: Proposed (agent-drafted 2026-09-09 for the first release; Q1 and Q2
  answered by Quentin the same day in chat — publish the engine crates to
  crates.io, ship 0.1 with the open Phase 1 items as known gaps; Q3 defaults to
  plain annotated tags unless a maintainer objects before tagging)
- Milestone: first release (v0.1.0)
- Date: 2026-09-09 (revised the same day: Option A → Option B after Q1)

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
- **crates.io cannot carry the drop-in path.** `heed-shim` is literally named
  `heed`, which is taken; cargo requires a `[patch]` replacement to share the
  patched crate's *name*, so the shim can only ever be consumed from git.
  `heed-zerodb`'s version is heed's line, and crates.io versions are immutable:
  a fix to the adapter could not be re-released under the `0.22.1` the patch
  needs. crates.io is therefore useful for the **engine** crates (docs.rs,
  `cargo add zerodb`, `cargo install zerodb-tools`), not for the adapter.
- **The frozen contract is heed's.** PLAN.md ground rule 2: no heed-mirrored
  signature changes in Phases 0–2. ZeroDB-only extensions (`Env::sync(force)`,
  `reader_list`, `live_readers`, `EnvOpenOptions::page_size`,
  `copy_to_file_with_progress`, `Comparator`) are the only API that can move.
- **The native `zerodb` API has one consumer** (the adapter) and no designed
  stability commitment; a few internal types leak through public signatures
  (`TxnRead`'s associated types, `EnvStream`). Publishing makes that surface
  semver-relevant.
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
- Cons: no `cargo add zerodb`, no `cargo install zerodb-tools`; docs.rs pages
  do not exist.

### Option B — Option A plus `cargo publish` of the engine crates

Publish `zerodb-core`, `zerodb-io`, `zerodb`, `zerodb-tools` at the same
version; keep `heed-zerodb`, `heed-shim` and `zerodb-oracle` `publish = false`.

- Pros: docs.rs, `cargo install zerodb-tools`, discoverability; the native API
  becomes something a second consumer can depend on.
- Cons: semver becomes enforceable on `zerodb`'s native API before it has a
  second consumer; every path dependency needs a `version =`; publishing is
  irreversible (yank only). A crates.io token has to live in CI.

### Option C — tag only

A tag and a changelog, no binaries.

- Cons: `zerodb-tools` is the one thing an operator needs without a Rust
  toolchain (offline `check`, `dump`, `load` of a Meilisearch data dir).

## Decision

**Option B** (Quentin, 2026-09-09): the release is a git tag, a GitHub release
with prebuilt `zerodb-tools`, **and** a crates.io publish of the four engine
crates. The heed drop-in path stays the git `[patch]` on the shim — crates.io
cannot carry it (see Context).

Versioning rules while 0.x:

1. **One version for the engine crates.** `zerodb-core`, `zerodb-io`, `zerodb`,
   `zerodb-tools`, `zerodb-oracle` share `X.Y.Z`; the tag is `vX.Y.Z`; the
   release workflow refuses a tag that differs from `crates/zerodb`'s version.
   `heed-zerodb` and `heed-shim` stay at the heed line they mirror.
2. **Minor bump (0.Y)**: any change to a ZeroDB extension API, any breaking
   change to the native `zerodb` API (cargo treats `0.Y.*` as one compatible
   line, so this is what crates.io consumers rely on), any `FORMAT_VERSION`
   bump, any MSRV bump, any new sanctioned divergence. A `FORMAT_VERSION` bump
   is itself an ADR (CLAUDE.md rule 6) and the CHANGELOG entry's first line
   states it with the dump/load migration path.
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

Publishing rules:

7. **Published**: `zerodb-core`, `zerodb-io`, `zerodb`, `zerodb-tools`.
   **Never published**: `heed-zerodb` (heed's version line; immutable versions
   would block re-releasing a fix under `0.22.1`), `heed-shim` (the name
   `heed` is taken), `zerodb-oracle` (links C LMDB; test harness). Each carries
   `publish = false` so a stray `cargo publish --workspace` cannot include it.
8. **Internal dependencies are pinned exactly** (`zerodb-core = { path,
   version = "=X.Y.Z" }`). The four crates release together and share
   internals; an exact pin keeps a consumer from resolving `zerodb 0.1.0`
   against `zerodb-core 0.1.3`.
9. **Publishing is the last job of the release workflow**, after the GitHub
   release exists: `cargo publish --workspace` with the `CARGO_REGISTRY_TOKEN`
   repository secret (a crates.io token scoped to *publish-new* and
   *publish-update* on these crates). If it fails, the tag and the GitHub
   release stand and the same command is run from the tag by a maintainer.
   Once the crates exist on crates.io, switching to crates.io Trusted
   Publishing (GitHub OIDC, no long-lived token) is a workflow-only change and
   needs no new ADR.
10. **Every PR runs `cargo publish --workspace --dry-run --no-verify`** so a
    manifest that cannot be published never reaches a tag.
11. **docs.rs** builds each crate with default features; `zerodb-io`'s `fault`
    feature (crash-injection backend, test infrastructure) is not documented
    there.

## Consequences

- New files: `CHANGELOG.md` (Keep a Changelog shape; the release workflow
  extracts the tagged section), `.github/workflows/release.yml`,
  `docs/RELEASING.md`, `SECURITY.md`, `CONTRIBUTING.md`, `docs/COMPATIBILITY.md`
  (the heed/LMDB coverage matrix a release is measured against).
- Engine crates bump `0.0.1 → 0.1.0`; `heed-zerodb`, `heed-shim`, and
  `zerodb-oracle` carry `publish = false`.
- The four published manifests carry the crates.io metadata (`repository`,
  `authors`, `readme`, `keywords`, `categories`) and exact-pinned internal
  dependencies. `SECURITY.md`'s "the maintainer listed in `Cargo.toml`" is now
  true.
- The native `zerodb` API as it stands at 0.1.0 — including the internal types
  that leak through `TxnRead` and `EnvStream` — is the 0.1 line's public
  surface; cleaning those up is a 0.2 change, not a patch.
- `zerodb-tools --version` prints the crate version and `FORMAT_VERSION`, so a
  support request can name both; `cargo install zerodb-tools` works.
- README, `docs/RELEASING.md` and `docs/TOOLS.md` describe the crates.io path;
  the drop-in instructions are unchanged (git patch on the shim at a tag).

## Open questions for human review

1. ~~Is the "no crates.io for 0.x" call acceptable, or is docs.rs presence
   wanted from day one (Option B)?~~ **Answered 2026-09-09 (Quentin, chat):
   publish to crates.io (Option B); the token is set as a repository secret by
   the maintainer.**
2. ~~Should `v0.1.0` wait for the 24 h fuzz soak?~~ **Answered 2026-09-09
   (Quentin, chat): ship 0.1 with the 24 h soak, the Graviton bench and the
   64K-page kernel listed as known gaps in the release notes.**
3. Tag signing: plain annotated tags, or require signed tags from a maintainer
   key? **Default if unanswered: plain annotated tags** (the workflow's
   `--verify-tag` checks existence, not a signature).
