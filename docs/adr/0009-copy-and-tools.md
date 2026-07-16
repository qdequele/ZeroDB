# ADR-0009: `copy_to_file` design and the `zerodb-tools` shape (M1.12)

- Status: Proposed (agent-authored 2026-07-17; awaiting human ratification)
- Milestone: 1.12 (Tools and migration)
- Date: 2026-07-17

## Context

PLAN §1.12 requires a compacting/raw environment copy (`Env::copy_to_file`
parity, SPEC 00 rows 17/59 — Meilisearch snapshots use it in both
`CompactionOption` modes) and a `zerodb-tools` suite (`stat`, `dump`, `load`,
`check`, `migrate-from-lmdb`). Two design questions needed recording as they
touch a **public API shape** (CLAUDE.md rule 6): how the copy is realized, and
how the tools guard against a live env with no cross-process protocol (D-001).

## Decision — `copy_to_file`

**Public shape.** `zerodb::CopyToFile::copy_to_file(&self, path, CompactionOption)`
— an **extension trait** in the `zerodb` crate, not an inherent method, because
`Env` lives in the I/O-free, `miri`-clean `zerodb-core` and the copy does file
I/O. It produces a **single `zerodb.dat`-format file** (to open the copy, place
it as `<dir>/zerodb.dat`). heed's inherent `Env::copy_to_file(&mut File, …)` is
mapped onto this trait by the M1.13 adapter.

**Snapshot.** `copy_to_file` opens its **own** internal `RoTxn` (SPEC 00 row 17:
the caller cannot supply one) and holds it for the whole copy, pinning snapshot
`T`.

**`Disabled` (raw).** Copy the snapshot's data pages `[FIRST_DATA_PGNO, last_pg]`
verbatim from the map, then synthesize **two fresh meta slots** from `T`
(`main_db`/`free_db`/`last_pg`/`map_size`), so the copy opens at exactly `T` even
if the live env has committed newer metas since the txn began. The freelist is
preserved (matches `mdb_env_copy2` without `MDB_CP_COMPACT`).

**`Enabled` (compact).** Read every live entry of every DB under `T` and rebuild
a fresh, densely-packed image via `builder::build_multi_db_image` (the M1.3
bottom-up bulk builder, promoted to multi-DB here). No free pages survive.

**Correctness under a concurrent writer.** Every page reachable from `T` is
immutable while this reader is the oldest (the GC gate, SPEC 04 TXN-20/21), so a
raw copy never tears a live page. Pages already free at `T` may be concurrently
reused, but their bytes are never read as live data (the copy's freelist lists
them free; they are overwritten on the next reuse), so a torn free page is
harmless. No write mutex is held (matching LMDB), so a large copy does not stall
writers. Verified by the oracle differential
(`copy_to_file_differential.rs`): the copies' logical content is identical to
heed's for both options (dump-equality, not byte-equality — formats differ,
D-002). `copy_to_file` is **not** added to the fuzz op model (it takes its own
internal txn and emits a file, not an in-env state change); the direct
differential tests suffice.

## Decision — tools liveness guard

D-001 means there is **no cross-process reader protocol**. The tools therefore
**require the env to be closed** and detect liveness only **best-effort**: a
non-blocking advisory `flock(LOCK_EX)` on `<dir>/zerodb.dat` (`crate::lock`);
if it is held the tool refuses with a clear "locked" error. This detects another
tool invocation (or any process that flocks the file), **not** the engine itself
(the engine takes no flock) — the documented contract is "run tools offline".

`std` has no stable file-locking API and `fs2`/`fs4` are not on the allowlist,
so the guard is one `libc::flock` FFI call (SAFETY-commented) in
`zerodb-tools::lock`. **This expands the CLAUDE.md unsafe policy** (which lists
only `zerodb-core::{page,readers}`, `zerodb-io`, and `zerodb-oracle`) to
`zerodb-tools` — flagged for a human to record in CLAUDE.md. `zerodb-core`
remains `#![forbid(unsafe_code)]`.

## Decision — dump format

A **logical**, `mdb_dump`-shaped text format (`crate::dump_format`): `VERSION=3`,
then one block per DB (`format=bytevalue`, optional `database=<hex-name>`,
`type=btree`, `HEADER=END`, hex `key`/`value` record lines, `DATA=END`). It
**omits physical geometry** (`mapsize`/`maxreaders`/`db_pagesize`) that real
`mdb_dump` includes, so a dump compares **byte-identically across engines and
page sizes** — logical content is engine-independent, physical layout is not
(D-002). Names are hex-encoded so a name with an embedded `0x00`/newline (valid
in zerodb, D-008) round-trips. `migrate-from-lmdb` links C LMDB behind the
off-by-default `migrate-lmdb` feature (ADR-0001 M1.12 amendment).

## Consequences

- New public surface: `zerodb::{CompactionOption, CopyToFile}`;
  `builder::{build_multi_db_image, NamedDbData}`;
  `rotxn::{collect_entries_flagged, named_databases}`; `RoTxn::{snapshot,
  map_bytes}`; `Cursor::current_flags`.
- `zerodb-tools` becomes a lib + bin; gains `libc` (flock) and, behind
  `migrate-lmdb`, `heed`.
- Human review items: the ADR-0001 amendment ratification line; the CLAUDE.md
  unsafe-policy expansion for `zerodb-tools` flock; the copy's concurrent-writer
  argument.
