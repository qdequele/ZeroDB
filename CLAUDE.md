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

## unsafe policy

`unsafe` is permitted ONLY in: mmap access and page casting (`zerodb-core::page`,
`zerodb-io`), the reader table (`zerodb-core::readers`), FFI inside
`zerodb-oracle`, and the minimal API-shape unsafe in `heed-zerodb` that heed's
pointer model inherently requires (Send impls, TLS-marker retags, the
lifetime-erased write cursor — nothing beyond what the mirrored heed surface
forces; ratified 2026-07-17, M1.13). Every unsafe block requires a `// SAFETY:` comment stating the
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
cargo miri test -p zerodb-core          # non-mmap logic must pass miri
just fuzz-quick                          # 10 min differential fuzz
just crash-test-quick                    # crash-consistency smoke (from M1.11 on)
```

Performance claims require a `cargo bench` criterion diff pasted in the summary.

## Repo map

- `crates/zerodb-core` — pages, B+tree, txns, GC, reader table. No I/O policy.
- `crates/zerodb-io` — mmap reads; pwrite / io_uring write backends; fsync
  strategies; fault-injection write backend for crash testing (M1.11).
- `crates/zerodb` — public engine API (heed-shaped).
- `crates/heed-zerodb` — heed backend adapter.
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
ADR (current allowlist: memmap2, libc, thiserror, crossbeam-utils, rand,
proptest, arbitrary, criterion; io-uring behind a feature). Errors mirror heed's
error taxonomy. Public items documented. No `TODO` left in merged code — file it
in PLAN.md progress notes instead.

## Progress tracking

Append one line per completed milestone to `PROGRESS.md`:
`M1.3 done 2026-07-15 — notes: cursor set_range edge case on empty db, see ADR-0007`.
Read PROGRESS.md at session start to know where the project stands.
