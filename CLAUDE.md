# ZeroDB — pure-Rust LMDB-parity storage engine for Meilisearch & hannoy

Read `PLAN.md` for the full roadmap. This file is the standing law of the repo.

## What this project is

A transactional embedded KV engine, drop-in for LMDB **at the heed API level**
(not file-format level). Phase 1 = strict LMDB behavior parity. Phase 2 = expose
LMDB features heed hides. Phase 3 = Meilisearch/hannoy-specific improvements.
Consumers: Meilisearch (milli) and hannoy (HNSW vector index).

## Non-negotiable rules

1. **The oracle is the Meilisearch LMDB fork** (`mdb.master.nested-rtxns`, as
   vendored in `lmdb-master-sys 0.2.6` / heed 0.22.1 — what Meilisearch
   actually runs; NOT stock LMDB, which lacks nested read txns). Never guess
   LMDB semantics — write a differential test in `crates/zerodb-oracle` and
   observe. Surprising LMDB behavior is
   replicated in Phase 1 and logged in `docs/DIVERGENCES.md` as a Phase 3
   candidate. NEVER change oracle expectations to make zerodb pass; divergences
   are zerodb bugs unless a maintainer signs off in DIVERGENCES.md.
2. **Never delete, weaken, `#[ignore]`, or loosen a test to make it pass.**
   If a test seems wrong, stop and explain in the PR/summary; a human decides.
3. **Spec before code.** `docs/SPEC/*.md` is the source of truth. Read the
   relevant spec file before touching an area. If implementation clarifies
   behavior, update the spec **in the same change**. If code and spec disagree,
   the spec wins until a human amends it.
4. **Clean-room.** Reading LMDB/libmdbx C source to understand algorithms is
   fine; transliterating C code is forbidden. Implement from the SPEC docs.
5. **One milestone per session.** Start by restating the milestone's acceptance
   criteria from PLAN.md. Do not start work on the next milestone. End the
   session by running the checks below and reporting results verbatim.
6. **ADRs for significant decisions** (`docs/adr/`, template `0000-template.md`):
   any on-disk format choice, concurrency protocol, fsync ordering, public API
   shape. For milestones 0.5, 1.4, 1.5, 1.8, 1.9, 1.11 and all of Phase 3:
   write the ADR first, get human approval, then implement. Milestone 0.5
   (heed integration strategy) is ADR-only — nothing is implemented until a
   human approves it.
7. **Scope rule.** Before a milestone that touches the on-disk format or spans
   more than 3 crates, land the smallest change that tests the riskiest
   assumption first. Do NOT bundle orthogonal improvements into a format change
   because "it's the cheap moment to do it" — that reasoning is how a stage
   becomes 4,000 lines. If a milestone has **no consumer**, prefer a spike over
   a staged implementation: build the minimum that reveals the blast radius,
   then decide whether to continue.
   *(Added 2026-07-20 from M2.8a: stage A of DUPSORT — a feature no consumer
   uses — reached 4,330 insertions across 46 files, was reviewed, and was
   parked. A spike would have surfaced the same finding, that a dup-aware
   cursor silently breaks compaction and dump/load, in a few hundred lines.)*

## unsafe policy

`unsafe` is permitted ONLY in: mmap access and page casting (`zerodb-core::page`,
`zerodb-io`), the reader table (`zerodb-core::readers`), FFI inside
`zerodb-oracle`, and the minimal API-shape unsafe in `heed-zerodb` that heed's
pointer model inherently requires (Send impls, TLS-marker retags, the
lifetime-erased write cursor, the `ReservedSpace` uninit view, and one
`sysconf` for the D-006 boundary — nothing beyond what the mirrored heed
surface forces; ratified 2026-07-17, M1.13). **In use but not yet ratified:**
`zerodb-tools` carries one `libc::flock` (`src/lock.rs`, the live-env guard)
and the `migrate-lmdb` feature's heed `open` (`src/migrate.rs`), both shipped
with M1.12 and self-flagged there; a human must either sanction them here or
ask for their removal (recorded 2026-09-09). `zerodb-core::readers` is listed
above but contains no `unsafe` — the lock-free protocol is plain atomics.
Every unsafe block requires a `// SAFETY:` comment stating the
invariants relied on. No `#[repr(C)]` casts of possibly-unaligned data — use
explicit offsets + `read_unaligned`. All atomics use explicit `Ordering` with a
comment justifying it; assume ARM (weak memory model), never "works on x86".

## Target platforms

linux-aarch64 (AWS Graviton 3/4) is primary; linux-x86_64 and macOS aarch64 for
dev. DB page size is runtime-chosen (4K–64K) and independent of the OS page
size; never assume 4K OS pages (ARM distros often use 64K).

## Commands (run before declaring any milestone done)

```
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo +nightly miri test -p zerodb-core # non-mmap logic must pass miri
just fuzz-quick                          # 10 min differential fuzz
just crash-test-quick                    # crash-consistency smoke (from M1.11 on)
just loom                                # reader-table model check (ADR-0006) when touching readers/nested
just stress                              # 180 s reader/writer stress — mandatory before an integration gate
```

The same battery runs in `.github/workflows/ci.yml` (fmt/clippy/test/miri/loom
and fuzz-quick + crash-test-quick per PR on x86-64 and aarch64; the long fuzz,
full crash run and stress nightly).

Performance claims require a `cargo bench` criterion diff pasted in the summary.

## Repo map

- `crates/zerodb-core` — pages, B+tree, txns, GC, reader table. No I/O policy.
- `crates/zerodb-io` — mmap reads; pwrite / io_uring write backends; fsync
  strategies; fault-injection write backend for crash testing (M1.11).
- `crates/zerodb` — public engine API (heed-shaped).
- `crates/heed-zerodb` — heed backend adapter.
- `crates/heed-shim` — a crate literally named `heed` re-exporting heed-zerodb;
  the `[patch.crates-io]` target consumers point at. Excluded from the
  workspace so it never collides with the oracle's real `heed`.
- `crates/zerodb-tools` — `stat`, `dump`, `load`, `check`, `migrate-from-lmdb`.
- `crates/zerodb-oracle` — differential harness vs C LMDB (only place linking C).
- `docs/SPEC/` — format & algorithm spec (source of truth).
- `docs/adr/` — decisions, indexed in `docs/DECISIONS.md`.
  `docs/DIVERGENCES.md` — sanctioned behavior diffs.

## Agents

Use the subagents in `.claude/agents/`:
- `critical-implementer` (Fable 5, high effort) — milestones 1.4, 1.5, 1.8,
  1.9, 1.11, 3.1 and anything touching commit ordering, GC, the reader table,
  or nested read txns.
- `implementer` (Opus 4.8) — all other implementation milestones.
- `test-writer` (Sonnet 5) — oracle tests, proptests, fuzz targets, bench code.
- `spec-reviewer` (Fable 5) — adversarial PR review against SPEC; run it on
  every critical-path PR before requesting human review.
- `explorer` (Haiku) — codebase/docs lookups; never lets it write code.

## Style

Rust 2021+, MSRV pinned in workspace Cargo.toml. No new dependencies without an
ADR (current allowlist: memmap2, libc, thiserror, crossbeam-utils, rand
[allowlisted, currently unused], proptest, arbitrary, criterion; io-uring
behind a feature [not yet present]; plus the heed-surface re-exports
heed-traits / heed-types / byteorder in `heed-zerodb` only (ADR-0003), loom
under `cfg(loom)` in `zerodb-core` only (ADR-0006), libfuzzer-sys in `fuzz/`
only (ADR-0001). `tempfile` is a dev-dependency of `heed-zerodb` without an
ADR — pending a human call, since `crates/zerodb/tests` hand-roll a `TempDir`
in 19 files to avoid exactly that dependency.) Errors mirror heed's
error taxonomy. Public items documented. No `TODO` left in merged code — file it
in PLAN.md progress notes instead.

## Progress tracking

Append one line per completed milestone to `PROGRESS.md`:
`M1.3 done 2026-07-15 — notes: cursor set_range edge case on empty db, see ADR-0007`.
Read PROGRESS.md at session start to know where the project stands.
