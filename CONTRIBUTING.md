# Contributing to ZeroDB

ZeroDB is a clean-room, spec-first re-implementation of LMDB's engine behind
heed's API. The rules that keep it honest are short and non-negotiable; they
live in [`CLAUDE.md`](CLAUDE.md) and apply to humans and agents alike. The
ones you will hit first:

- **The oracle decides LMDB semantics.** Never guess what LMDB does: write a
  differential test in `crates/zerodb-oracle` against the Meilisearch LMDB fork
  and observe. A behaviour difference is a ZeroDB bug unless it is a signed-off
  entry in [`docs/DIVERGENCES.md`](docs/DIVERGENCES.md).
- **Never weaken a test to make it pass.** If a test looks wrong, say so in the
  PR and let a maintainer decide.
- **Spec before code.** [`docs/SPEC/`](docs/SPEC/) is the source of truth; if
  your change clarifies behaviour, update the spec in the same PR.
- **Clean room.** Reading LMDB or libmdbx source to understand an algorithm is
  fine; transliterating C is not.
- **`unsafe` only where the policy allows it**, always with a `// SAFETY:`
  comment; atomics always with an explicit `Ordering` and a justification.
- **Significant decisions are ADRs** (`docs/adr/`, indexed in
  `docs/DECISIONS.md`): on-disk format, concurrency protocols, fsync ordering,
  public API shape.

## Before you open a PR

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo +nightly miri test -p zerodb-core   # when touching zerodb-core
just loom                                 # when touching readers / nested txns
just fuzz-quick                           # 10-minute differential fuzz vs real LMDB
just crash-test-quick                     # when touching the write or commit path
```

CI runs the same battery on x86-64 and aarch64, plus a build of the consumer
crates on the workspace `rust-version` (the MSRV is for consumers; developing
ZeroDB itself needs a current stable toolchain because of the bench and fuzz
tooling). For changes that could affect
Meilisearch or hannoy, run the consumer gate too
([`docs/CONSUMER-GATE.md`](docs/CONSUMER-GATE.md)).

Performance claims need a criterion or consumer-bench diff in the PR
description, with the machine named.

## Where things are

| | |
|---|---|
| Roadmap | [`PLAN.md`](PLAN.md) |
| Engineering log | [`PROGRESS.md`](PROGRESS.md) (append-only) |
| Docs map | [`docs/README.md`](docs/README.md) |
| Releasing | [`docs/RELEASING.md`](docs/RELEASING.md) |
| Security | [`SECURITY.md`](SECURITY.md) |

## Reporting a bug

Open a GitHub issue with the ZeroDB commit, the consumer (Meilisearch/hannoy
version, or native API), the platform and page size, and a reproduction. For a
suspected behaviour difference from LMDB, the ideal report is an op sequence
the oracle can replay. Security issues: see `SECURITY.md`.

## License

By contributing you agree that your contributions are licensed under the
project's dual MIT / Apache-2.0 license.
