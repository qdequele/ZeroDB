# ADR index

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
| 0011 | [DUPSORT / DUPFIXED — sub-page→sub-tree encoding, LEAF2 packing, dup comparator + persistence (D-014 tie-in), sub-cursor model, 2.8a–d staging](adr/0011-dupsort.md) | Approved (2.8 parked 2026-07-20; stage A in stash) | 2.8 |
| 0012 | [Prefetch / access hints — safe slice-level `RoTxn::will_need` over `memmap2::advise_range`, extension-trait surfacing, replaces hannoy's hand-rolled madvise](adr/0012-prefetch-access-hints.md) | Draft | 3.7 |
