# Releasing ZeroDB

Policy: ADR-0013 (release and versioning). Short version: 0.x releases are git
tags with a GitHub release, prebuilt `zerodb-tools` binaries, and a crates.io
publish of the four engine crates (`zerodb-core`, `zerodb-io`, `zerodb`,
`zerodb-tools`). The heed adapter is **not** published — cargo needs a crate
*named* `heed` for the patch, so heed consumers depend on the repo through
`[patch.crates-io]` on `crates/heed-shim` at a tag.

## One-time setup

- A crates.io API token scoped to *publish-new* and *publish-update* (limit it
  to the four crate names once they exist), stored as the repository secret
  `CARGO_REGISTRY_TOKEN`. The release workflow's last job uses it. Rotate it
  like any credential; never paste it into a workflow file or a log.
- The first publish of a brand-new crate name can also be done from a
  maintainer's machine with the same token (`cargo login`, then the command in
  "If crates.io publish fails" below); after that, prefer the workflow.

## Before tagging

1. **Gate, all of it** (CLAUDE.md "Commands"), on the release commit:

   ```sh
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   cargo +nightly miri test -p zerodb-core
   just loom
   just fuzz-quick
   just crash-test-quick
   just stress
   ```

2. **Consumer gate** (docs/CONSUMER-GATE.md) against the Meilisearch and
   hannoy refs named in the CHANGELOG entry:

   ```sh
   MEILISEARCH_SRC=… just consumer-suites
   MEILISEARCH_SRC=… just consumer-bench
   HANNOY_SRC=…      just hannoy-suites
   HANNOY_SRC=…      just hannoy-bench
   ```

   Copy the comparison tables into `benches/results/<date>-….md` and cite them
   from the CHANGELOG entry. State the machine. Laptop numbers are indicative;
   say so.

3. **Truth pass on the docs.** README status paragraph, `docs/COMPATIBILITY.md`
   (every heed item and LMDB feature with its status), `docs/DIVERGENCES.md`
   (nothing PROPOSED that the release depends on), `docs/DECISIONS.md` matching
   each ADR's own status line, `PROGRESS.md` entry for the release.

4. **Versions.** Bump `version` in every engine crate (`zerodb-core`,
   `zerodb-io`, `zerodb`, `zerodb-tools`, `zerodb-oracle`) to the same value.
   `heed-zerodb` and `heed-shim` stay at the heed line they mirror (0.22.1) —
   that number is what makes the `[patch]` resolve, it is not ZeroDB's version.
   The release workflow refuses a tag that does not equal `crates/zerodb`'s
   version.

5. **CHANGELOG.md.** Add a `## [X.Y.Z] - YYYY-MM-DD` section (Keep a Changelog
   shape). The release workflow extracts exactly that section as the GitHub
   release notes and refuses a tag without one. Include: what changed, the
   on-disk `format_version` and whether files from the previous release open
   unchanged, the Meilisearch/hannoy refs tested, the bench table pointer, the
   known gaps.

6. **On-disk format.** If `FORMAT_VERSION` (`crates/zerodb-core/src/page/mod.rs`)
   changed since the last release, the entry must say so in its first line and
   give the migration path (`zerodb-tools dump` on the old binary, `load` on the
   new one). A format bump is an ADR (CLAUDE.md rule 6) and a minor-version bump
   at least while 0.x.

7. **Publish dry run** on the release commit (CI does the manifest-only
   variant on every PR; this one also builds each packaged crate):

   ```sh
   cargo publish --workspace --dry-run
   ```

   It must print exactly four `Uploading` lines (`zerodb-core`, `zerodb-io`,
   `zerodb`, `zerodb-tools`, each "aborting upload due to dry run"). The
   internal dependencies are exact pins (`version = "=X.Y.Z"`), so the version
   bump in step 4 must also update those pins in `zerodb-io`, `zerodb` and
   `zerodb-tools`.

## Tagging

```sh
git checkout main && git pull
git tag -a vX.Y.Z -m "ZeroDB vX.Y.Z"
git push origin vX.Y.Z
```

`.github/workflows/release.yml` then runs the gate on x86-64 and aarch64,
builds `zerodb-tools` for `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`
and `aarch64-apple-darwin`, and creates the GitHub release with the CHANGELOG
section as notes and the tarballs plus SHA-256 sums as assets. Binaries are
built with default features only; `migrate-from-lmdb` links C LMDB and stays a
from-source feature. Last, the `publish` job runs `cargo publish --workspace
--locked`, which uploads the four engine crates in dependency order and skips
the `publish = false` members.

### If crates.io publish fails

The tag and the GitHub release stand; crates.io is the only thing missing. Fix
the cause (an expired token, a crates.io outage, a name squatted between the
dry run and the tag) and publish from the tag by hand:

```sh
git checkout vX.Y.Z
cargo publish --workspace --locked
```

If some crates already went out, `--workspace` will refuse to re-upload them;
resume with the remaining ones in dependency order (`cargo publish -p zerodb-io`,
then `-p zerodb`, then `-p zerodb-tools`). A version that went out wrong cannot
be replaced, only yanked (`cargo yank`); the fix is a patch release.

## After tagging

- Check the release page renders the notes and lists six assets.
- Check crates.io shows the four crates at the new version and docs.rs built
  them (`https://docs.rs/zerodb/X.Y.Z`); a docs.rs failure is fixed in a
  patch release, not by re-publishing.
- Update the pinned tag in `docs/CONSUMER-GATE.md` examples if the consumer
  pins moved.
- Open the next CHANGELOG section as `## [Unreleased]`.

## What 0.x promises (from ADR-0013)

- **API**: may change between minor versions; a change to a heed-mirrored
  signature is never made (that surface is heed's), only ZeroDB extensions and
  the native `zerodb` API move. cargo treats `0.Y.*` as one compatible line, so
  a patch release never breaks a crates.io consumer's build.
- **On-disk format**: files written by a release open unchanged in every later
  release with the same `FORMAT_VERSION`; a bump is announced in the CHANGELOG
  with a dump/load path.
- **Platforms**: linux-aarch64, linux-x86_64, macOS aarch64; DB page size is
  independent of the OS page size, but a 64K-page kernel has not been exercised
  in CI yet (see the release notes' known gaps).
- **MSRV**: the workspace `rust-version` is what a **consumer** needs to build
  the library crates and `zerodb-tools` (checked in CI without
  dev-dependencies). Developing ZeroDB itself — running the oracle, benches and
  fuzzers — needs a current stable toolchain. A bump is a CHANGELOG entry.
