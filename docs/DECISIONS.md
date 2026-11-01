# ADR index

ADRs are historical records. Older ones cite milestone numbers of the original
roadmap (such as 1.4 or 3.1, grouped into Phases 0–3) and the original project
plan and engineering log, which are no longer in the tree. Planned work is
tracked in GitHub issues. To propose a decision, copy
[`adr/0000-template.md`](adr/0000-template.md) and add a row below.

| # | Title | Status | Area |
|---|-------|--------|------|
| 0000 | [Template](adr/0000-template.md) | — | — |
| 0001 | [Oracle crate links heed =0.22.1 (Meilisearch LMDB fork)](adr/0001-oracle-links-heed.md) | Approved | Testing |
| 0002 | [On-disk format principles](adr/0002-on-disk-format.md) | Approved | On-disk format |
| 0003 | [heed integration strategy (standalone heed-zerodb crate)](adr/0003-heed-integration-strategy.md) | Approved | heed integration |
| 0004 | [Write path — single-writer RwTxn, COW dirty store, commit pipeline](adr/0004-write-path.md) | Approved | Write path |
| 0005 | [GC implementation — in-txn structures, free-list save, reclaim wiring](adr/0005-gc.md) | Approved | Page reclamation |
| 0006 | [MVCC reader table](adr/0006-reader-table.md) | Approved | Concurrency |
| 0007 | [Nested read transactions over a write txn](adr/0007-nested-read-txns.md) | Approved | Concurrency |
| 0008 | [Crash harness — fault-injection write backend](adr/0008-crash-harness.md) | Approved | Testing |
| 0009 | [`copy_to_file` design and the `zerodb-tools` shape](adr/0009-copy-and-tools.md) | Implemented; approval not recorded | Tools |
| 0010 | [Env data-file name (`zerodb.dat` vs heed's `data.mdb`)](adr/0010-env-file-naming.md) | Approved; implemented | heed integration |
| 0011 | [DUPSORT / DUPFIXED](adr/0011-dupsort.md) | Approved; implementation parked, never merged | Features |
| 0012 | [Prefetch and access hints (`will_need` / `advise`)](adr/0012-prefetch-access-hints.md) | Draft | Features |
| 0013 | [Release and versioning policy for the 0.x line](adr/0013-release-and-versioning.md) | Followed for 0.2.0; approval not recorded | Release |
| 0014 | [Opt-in trusted-file mode](adr/0014-trusted-file-mode.md) | Accepted | Performance (reads) |
| 0015 | [Opt-in sequential-writes fast path](adr/0015-sequential-writes-option.md) | Accepted | Performance (writes) |
| 0016 | [Lazy (read-time) cell validation](adr/0016-lazy-validation.md) | Measured, not adopted (parked) | Performance (reads) |
| 0017 | [Bounded dirty-page memory in large write transactions (spilling)](adr/0017-bounded-dirty-memory.md) | Accepted | Memory |
| 0018 | [Env-wide validated-pages cache across transactions](adr/0018-cross-txn-validation-cache.md) | Accepted | Performance (reads) |
| 0019 | [Durable meta write through an O_DSYNC descriptor](adr/0019-meta-write-dsync.md) | Accepted; implementation parked (no measured gain) | Durability |
| 0020 | [Shrink the 32-byte common page header](adr/0020-compact-page-header.md) | Spike approved; format change pending its numbers | On-disk format |
| 0021 | [True in-place WRITE_MAP](adr/0021-writemap-in-place.md) | Accepted, implemented | Performance (writes) |
| 0022 | [Meta free-list annex (format v2)](adr/0022-meta-freelist-annex.md) | Accepted, implemented | On-disk format, page reclamation |
| 0023 | [Multi-process access: lock file, shared reader table, cross-process writer lock](adr/0023-multi-process.md) | Accepted, implemented | Concurrency |
| 0024 | [Database handle lifetime: stale handles after abort and remove](adr/0024-database-handle-lifetime.md) | Accepted (Option A), implemented | API, LMDB parity |
| 0025 | [Nested write transactions](adr/0025-nested-write-txns.md) | Draft, awaiting approval (#71) | Concurrency |
| 0026 | [Ranged `msync` for `WRITE_MAP` commits](adr/0026-writemap-ranged-msync.md) | Accepted (Option B), implemented | Durability |
| 0027 | [The API of Meilisearch's heed fork, behind a feature](adr/0027-heed-fork-unique-txns.md) | Accepted (Option B), implemented | API, consumer compatibility |
| 0028 | [Delete rebalance moves cells page to page, as LMDB](adr/0028-delete-rebalance-node-move.md) | Accepted (Option A), implemented | Performance (deletes), B+tree |
| 0029 | [Free-page run search across freed-page records](adr/0029-free-page-run-search.md) | Accepted (Option C), implemented | Page reclamation, LMDB parity |
| 0030 | [A copy-on-write copies only a page's used bytes, as LMDB](adr/0030-cow-copies-used-bytes.md) | Accepted (Option A), implemented | Performance (writes), bytes on disk |
| 0031 | [Hardware CRC32C for the meta checksum](adr/0031-hardware-crc32c.md) | Accepted (Option B), implemented; amends the `unsafe` policy | Performance (commit), `unsafe` policy |
| 0032 | [Raw environment copy from the data file: clone or in-kernel copy](adr/0032-raw-copy-block-clone.md) | Implemented; approval not recorded | Env copy, beyond LMDB |
| 0033 | [A standalone benchmark crate comparing ZeroDB with other key-value stores](adr/0033-kv-compare-bench.md) | Implemented; approval not recorded | Benchmarks, dependencies |
| 0034 | [Trust page contents by default behind the heed API, as LMDB does](adr/0034-trusted-pages-by-default.md) | Proposed | Page validation, LMDB parity, `unsafe` policy |
