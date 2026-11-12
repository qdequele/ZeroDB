# ADR index

ADRs are historical records. Older ones cite milestone numbers of the original
roadmap (such as 1.4 or 3.1, grouped into Phases 0–3) and the original project
plan and engineering log, which are no longer in the tree. Planned work is
tracked in GitHub issues. To propose a decision, copy
[`adr/0000-template.md`](adr/0000-template.md) and add a row to the right
group below.

The groups follow each ADR's own status line; the Status column repeats it in
short.

## Shipped

Approved or accepted, and in `main`.

| # | Title | Status | Area |
|---|-------|--------|------|
| 0001 | [Oracle crate links heed =0.22.1 (Meilisearch LMDB fork)](adr/0001-oracle-links-heed.md) | Approved | Testing |
| 0002 | [On-disk format principles](adr/0002-on-disk-format.md) | Approved | On-disk format |
| 0003 | [heed integration strategy (standalone heed-zerodb crate)](adr/0003-heed-integration-strategy.md) | Approved | heed integration |
| 0004 | [Write path — single-writer RwTxn, COW dirty store, commit pipeline](adr/0004-write-path.md) | Approved | Write path |
| 0005 | [GC implementation — in-txn structures, free-list save, reclaim wiring](adr/0005-gc.md) | Approved | Page reclamation |
| 0006 | [MVCC reader table](adr/0006-reader-table.md) | Approved | Concurrency |
| 0007 | [Nested read transactions over a write txn](adr/0007-nested-read-txns.md) | Approved | Concurrency |
| 0008 | [Crash harness — fault-injection write backend](adr/0008-crash-harness.md) | Approved | Testing |
| 0010 | [Env data-file name (`zerodb.dat` vs heed's `data.mdb`)](adr/0010-env-file-naming.md) | Approved; implemented | heed integration |
| 0014 | [Opt-in trusted-file mode](adr/0014-trusted-file-mode.md) | Accepted | Performance (reads) |
| 0015 | [Opt-in sequential-writes fast path](adr/0015-sequential-writes-option.md) | Accepted | Performance (writes) |
| 0017 | [Bounded dirty-page memory in large write transactions (spilling)](adr/0017-bounded-dirty-memory.md) | Accepted | Memory |
| 0018 | [Env-wide validated-pages cache across transactions](adr/0018-cross-txn-validation-cache.md) | Accepted | Performance (reads) |
| 0021 | [True in-place WRITE_MAP](adr/0021-writemap-in-place.md) | Accepted, implemented | Performance (writes) |
| 0022 | [Meta free-list annex (format v2)](adr/0022-meta-freelist-annex.md) | Accepted, implemented | On-disk format, page reclamation |
| 0023 | [Multi-process access: lock file, shared reader table, cross-process writer lock](adr/0023-multi-process.md) | Accepted, implemented | Concurrency |
| 0024 | [Database handle lifetime: stale handles after abort and remove](adr/0024-database-handle-lifetime.md) | Accepted (Option A), implemented | API, LMDB parity |
| 0026 | [Ranged `msync` for `WRITE_MAP` commits](adr/0026-writemap-ranged-msync.md) | Accepted (Option B), implemented | Durability |
| 0027 | [The API of Meilisearch's heed fork, behind a feature](adr/0027-heed-fork-unique-txns.md) | Accepted (Option B), implemented | API, consumer compatibility |
| 0028 | [Delete rebalance moves cells page to page, as LMDB](adr/0028-delete-rebalance-node-move.md) | Accepted (Option A), implemented | Performance (deletes), B+tree |
| 0029 | [Free-page run search across freed-page records](adr/0029-free-page-run-search.md) | Accepted (Option C), implemented | Page reclamation, LMDB parity |
| 0030 | [A copy-on-write copies only a page's used bytes, as LMDB](adr/0030-cow-copies-used-bytes.md) | Accepted (Option A), implemented | Performance (writes), bytes on disk |
| 0031 | [Hardware CRC32C for the meta checksum](adr/0031-hardware-crc32c.md) | Accepted (Option B), implemented; amends the `unsafe` policy | Performance (commit), `unsafe` policy |
| 0034 | [Trust page contents by default, as LMDB does](adr/0034-trusted-pages-by-default.md) | Accepted | Page validation, LMDB parity, `unsafe` policy |
| 0037 | [Bounded free-page records](adr/0037-bounded-free-page-records.md) | Accepted (Option C), implemented | On-disk format, page reclamation |

## Shipped, approval not recorded

In `main`, but the ADR does not record the maintainer's approval. CONTRIBUTING rule 5 asks for that approval before the implementation; these four are the known exceptions.

| # | Title | Status | Area |
|---|-------|--------|------|
| 0009 | [`copy_to_file` design and the `zerodb-tools` shape](adr/0009-copy-and-tools.md) | Implemented; approval not recorded | Tools |
| 0013 | [Release and versioning policy for the 0.x line](adr/0013-release-and-versioning.md) | Followed for 0.2.0; approval not recorded | Release |
| 0032 | [Raw environment copy from the data file: clone or in-kernel copy](adr/0032-raw-copy-block-clone.md) | Implemented; approval not recorded | Env copy, beyond LMDB |
| 0033 | [A standalone benchmark crate comparing ZeroDB with other key-value stores](adr/0033-kv-compare-bench.md) | Implemented; approval not recorded | Benchmarks, dependencies |

## Accepted, not implemented or parked

The decision stands, but the code is not in `main`.

| # | Title | Status | Area |
|---|-------|--------|------|
| 0011 | [DUPSORT / DUPFIXED](adr/0011-dupsort.md) | Approved; implementation parked, never merged | Features |
| 0019 | [Durable meta write through an O_DSYNC descriptor](adr/0019-meta-write-dsync.md) | Accepted; implementation parked (no measured gain) | Durability |
| 0020 | [Shrink the 32-byte common page header](adr/0020-compact-page-header.md) | Spike approved; format change pending its numbers | On-disk format |

## Draft or proposed

Awaiting a decision. Nothing here is implemented in `main`.

| # | Title | Status | Area |
|---|-------|--------|------|
| 0012 | [Prefetch and access hints (`will_need` / `advise`)](adr/0012-prefetch-access-hints.md) | Draft | Features |
| 0025 | [Nested write transactions](adr/0025-nested-write-txns.md) | Draft, awaiting approval (#71) | Concurrency |
| 0035 | [One atomic word for the in-process writer lock](adr/0035-writer-lock-cell.md) | Proposed | Concurrency, performance (commit), `unsafe` policy |
| 0038 | [Bounded relaxed durability: steady and weak metas, a durable watermark](adr/0038-bounded-relaxed-durability.md) | Proposed | Durability, page reclamation, on-disk format |
| 0039 | [Group commit: start the next write transaction during the previous commit's sync](adr/0039-group-commit.md) | Proposed (recommends not building it now) | Durability, concurrency, beyond LMDB |
| 0041 | [A new tree page is not zero-filled when its memory is reused, as LMDB](adr/0041-fresh-page-zero-fill.md) | Proposed | Performance (writes), bytes on disk |
| 0042 | [Streaming and incremental environment export](adr/0042-streaming-and-incremental-export.md) | Draft, awaiting approval | Env copy, beyond LMDB |
| 0043 | [Adopt `bitflags` for the flag types and `tempfile` for test directories](adr/0043-bitflags-and-tempfile.md) | Proposed | Dependencies, heed API |

## Measured and not adopted

Tried, measured, and left out.

| # | Title | Status | Area |
|---|-------|--------|------|
| 0016 | [Lazy (read-time) cell validation](adr/0016-lazy-validation.md) | Measured, not adopted (parked) | Performance (reads) |

## Reserved numbers

Two numbers are missing above because their ADRs were written on branches
that are not merged into `main`. The numbers are not reused.

| # | Title | Status on its branch | Branch |
|---|-------|--------|------|
| 0036 | Grow the memory map on demand, behind an option | Accepted and implemented on the branch; not merged | `qdequele/issue-33-map-growth-adr-4640ce` |
| 0040 | Refuse unknown persisted flag bits at open | Accepted, not implemented; not merged | `qdequele/silly-torvalds-ba6e41` |
