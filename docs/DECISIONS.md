# ADR index

The Milestone column and the ADR bodies refer to the project's original
roadmap (milestones such as 1.4 or 3.1, grouped into Phases 0–3). ADRs are
historical records: those written before October 2026 also cite `CLAUDE.md`
(the project rules, now [`AGENTS.md`](../AGENTS.md), where the ADR rule became
rule 5 and the scope rule rule 6), `PLAN.md` (that roadmap), `PROGRESS.md` (the
engineering log) and the AI-agent roles then used for implementation and
review. Those files are no longer in the tree; git history keeps them. Planned
work is now tracked in GitHub issues.

| # | Title | Status | Milestone |
|---|-------|--------|-----------|
| 0000 | Template | — | — |
| 0001 | [Oracle crate links heed =0.22.1 (Meilisearch LMDB fork)](adr/0001-oracle-links-heed.md) | Approved | 0.3 |
| 0002 | [On-disk format principles](adr/0002-on-disk-format.md) | Approved | 0.4 |
| 0003 | [heed integration strategy (standalone heed-zerodb crate)](adr/0003-heed-integration-strategy.md) | Approved | 0.5 |
| 0004 | [Write path — single-writer RwTxn, COW dirty store, commit pipeline](adr/0004-write-path.md) | Approved | 1.4 |
| 0005 | [GC implementation — in-txn structures, freelist_save at C1, reclaim wiring](adr/0005-gc.md) | Approved | 1.5 |
| 0006 | [MVCC reader table — slot table, publish cell, oldest-reader, loom/stress plan](adr/0006-reader-table.md) | Approved | 1.8 |
| 0007 | [Nested read txns — safe borrow-based `Send` child, child_count, oracle plan](adr/0007-nested-read-txns.md) | Approved | 1.9 |
| 0008 | [Crash harness — fault-injection write backend, two-mechanism crash cycles, crash-harness binary](adr/0008-crash-harness.md) | Approved | 1.11 |
| 0009 | [copy_to_file design + zerodb-tools shape (compact/raw copy, flock guard, logical dump format)](adr/0009-copy-and-tools.md) | Proposed | 1.12 |
| 0010 | [Env data-file naming — adapter presents `data.mdb`, core keeps `zerodb.dat` (D-012)](adr/0010-env-file-naming.md) | Approved (implemented 2026-07-20) | 1.14 gate remainder |
| 0011 | [DUPSORT / DUPFIXED — sub-page→sub-tree encoding, LEAF2 packing, dup comparator + persistence (D-014 tie-in), sub-cursor model, 2.8a–d staging](adr/0011-dupsort.md) | Approved (2.8 parked 2026-07-20; stage A never merged or pushed — local work only) | 2.8 |
| 0012 | [Prefetch / access hints — safe slice-level `RoTxn::will_need` over `memmap2::advise_range`, extension-trait surfacing, replaces hannoy's hand-rolled madvise](adr/0012-prefetch-access-hints.md) | Draft | 3.7 |
| 0013 | [Release and versioning policy for the 0.x line — git tags + GitHub releases with prebuilt zerodb-tools + crates.io publish of the four engine crates (heed adapter stays git-only), one version for the engine crates, exact internal pins, format/MSRV bump rules](adr/0013-release-and-versioning.md) | Proposed (agent-drafted 2026-09-09; Q1 and Q2 answered by the maintainer the same day: publish to crates.io, ship 0.1 with the open Phase 1 items listed as gaps; Q3 tag signing defaults to plain annotated tags) | first release (v0.1.0) |
| 0014 | [Opt-in trusted-file mode — `unsafe` open option that skips page validation on the read path (LMDB parity), default unchanged](adr/0014-trusted-file-mode.md) | Accepted | Phase 3 (PERF-GAP A2(b), issue #21) |
| 0015 | [Opt-in sequential-writes fast path — env option plus per-database override for the rightmost-leaf finger, default off](adr/0015-sequential-writes-option.md) | Accepted | Phase 3 (PERF-GAP roadmap #6) |
| 0016 | [Lazy (read-time) cell validation on first sight — full walk on second sight; read txns, leaf pages first](adr/0016-lazy-validation.md) | Measured, not adopted (parked) | Phase 3 (PERF-GAP A2(b), issue #21) |
| 0017 | [Bounded dirty-page memory in large write transactions (spilling, LMDB `mdb_page_spill`)](adr/0017-bounded-dirty-memory.md) | Accepted | Phase 3 (PERF-GAP C2, issue #3) |
| 0018 | [Env-wide validated-pages cache across transactions, keyed by (pgno, writer txnid stamp)](adr/0018-cross-txn-validation-cache.md) | Accepted | Phase 3 (follow-up of ADR-0016) |
| 0019 | [Durable meta write through an O_DSYNC descriptor — one barrier per durable commit, as LMDB's `me_mfd`](adr/0019-meta-write-dsync.md) | Accepted (all platforms; LMDB's failed-write scrub adopted); implementation parked 2026-10-01 — PR #86 closed: 2→1 fdatasync per durable commit, no measured latency gain | Phase 3 (durable-commit cost) |
| 0020 | [Shrink the 32-byte common page header (24 bytes, keeping pgno and the writer stamp)](adr/0020-compact-page-header.md) | Spike approved; format change pending its numbers. Spike run 2026-10-01 on the local, unpushed branch `qdequele/zerodb-header24-spike` (ca34d35, no PR); its results are recorded only in that branch's copy of the ADR, not on main | Phase 3 (density) |
| 0021 | [True in-place WRITE_MAP — dirty pages in the map, no heap staging, no commit write-back](adr/0021-writemap-in-place.md) | Accepted (2026-10-02, maintainer); spike, then production hardening pass, merged 2026-10-05 as PR #88 (8b066a6) — in-place is the `WRITE_MAP` behaviour. Pre-spike, ZeroDB-writemap was 0.42–0.53× of LMDB-writemap; ranged msync (issue #45) and the ADR's open questions remain | Phase 3 (PERF-GAP B5, issue #13) |
| 0022 | [Meta free-list annex — the per-commit freed PIL rides inside the (CRC-covered) meta page; GC tree becomes the spill/cold path; FORMAT_VERSION 2](adr/0022-meta-freelist-annex.md) | Accepted (2026-10-05, maintainer; format v2 ratified); merged as PR #89 (d155fe6) | perf lever B12 (free-list save); feeds the planned GC redesign for huge write transactions (roadmap item 3.1) |
