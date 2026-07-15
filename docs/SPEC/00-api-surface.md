# SPEC 00 — API surface contract (Phase 0.1 deliverable)

Status: TO BE WRITTEN in milestone 0.1.

Instructions for the agent writing this document:
1. Clone meilisearch/meilisearch, nnethercott/hannoy, meilisearch/arroy and
   meilisearch/heed at pinned commits (record the SHAs here).
2. Enumerate every heed item each consumer uses (grep + read call sites):
   EnvOpenOptions flags, Env methods, RoTxn/RwTxn, Database methods, iterator/
   cursor variants, put flags, codecs, error variants matched on.
   Do not miss: `Env::real_disk_size`, `Env::non_free_pages_size` (milli reads
   LMDB's freelist DB for disk reporting — needs a native zerodb equivalent,
   MUST priority), `Env` clone semantics, the same-process env registry,
   `EnvClosingEvent`, and whether any consumer uses nested txns (decides the
   1.9 scope).
3. Produce the contract table:

| heed item | Used by | LMDB primitive | Notes / edge semantics | Phase 1 milestone |
|-----------|---------|----------------|------------------------|-------------------|

4. Anything heed exposes but no consumer uses: list in a second table, mark
   priority SHOULD (Phase 2) or WON'T.
5. This table is the Phase 1 definition of done, and the input to the 0.5
   heed-integration-strategy ADR (fork vs backend feature vs adapter crate).
