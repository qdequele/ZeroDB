# Consumer gate — Meilisearch on ZeroDB

Two questions this repo has to keep answering:

1. **Does the drop-in work?** Take the current Meilisearch, swap LMDB for ZeroDB
   through the heed shim with **zero source changes**, and run Meilisearch's own
   test suites.
2. **How does Meilisearch perform on each engine?** Same binary build, same
   workloads, once linked against the LMDB fork and once against ZeroDB.

Both are driven by [`scripts/consumer.sh`](../scripts/consumer.sh)
(`just consumer-check`, `just consumer-suites`, `just consumer-bench`).

## How the swap works

Meilisearch depends on `heed = "0.22.1"`. The script appends this to the
Meilisearch `Cargo.toml` (between marker comments, removed again afterwards):

```toml
[patch.crates-io]
heed = { path = "<zerodb>/crates/heed-shim" }
```

`heed-shim` is a crate literally named `heed` at version 0.22.1 that re-exports
`heed-zerodb`, the 1:1 re-implementation of heed's surface over the ZeroDB
engine (ADR-0003). That is the only patch form cargo honours — a `package =`
rename inside `[patch]` is silently ignored. milli, arroy, hannoy, cellulite and
index-scheduler all see the same `heed` type names and compile unchanged.

After patching, the script asserts that `lmdb-master-sys` (the C LMDB fork) has
**vanished** from the dependency tree of the crate under test. A green build that
still links LMDB would prove nothing.

## Drop-in check and suites

```sh
# fastest: shared clone from a local checkout, at the pinned ref
MEILISEARCH_SRC=~/Projects/Meilisearch/meilisearch just consumer-check
MEILISEARCH_SRC=~/Projects/Meilisearch/meilisearch just consumer-suites
FULL=1 MEILISEARCH_SRC=... scripts/consumer.sh suites   # + meilisearch --lib, meilisearch-auth
MEILISEARCH_REF=main scripts/consumer.sh suites         # any ref; default v1.53.1
```

`suites` runs `cargo test -p milli -p index-scheduler` on ZeroDB. index-scheduler
is the load-bearing one: it exercises the task queue's untyped databases, the
index registry's open/close churn with `EnvClosingEvent`, `max_readers`,
`max_dbs`, and `static_read_txn` in async handlers. No test in either crate
asserts LMDB-internal geometry, so a failure is a real behavioural difference.

Results so far are logged in [`PROGRESS.md`](../PROGRESS.md) (search for
"CONSUMER SUITES"). Known blind spot: Meilisearch ships no compaction or
snapshot-restore round-trip test, which is how the `data.mdb` naming issue
(D-012) stayed hidden behind a green suite; those paths are covered by
`crates/heed-zerodb/tests/env_file_naming.rs` instead.

## Benchmarks: Meilisearch on LMDB vs ZeroDB

```sh
MEILISEARCH_SRC=~/Projects/Meilisearch/meilisearch just consumer-bench
WORKLOADS="workloads/movies.json workloads/hackernews-add-new-documents.json" ROUNDS=2 scripts/consumer.sh bench
```

The script builds `meilisearch` in release mode twice — stock (LMDB) and patched
(ZeroDB) — into `target/consumer/bin/`, then runs Meilisearch's own
integration benchmarks, `cargo xtask bench --no-dashboard --binary-path …`, on
each binary with the same workloads. Those benchmarks spawn the real server,
replay HTTP workloads, and collect per-span timings through the logs route
(see Meilisearch's `BENCHMARKS.md`). Each workload already repeats
`run_count` times (10 for `movies.json`); `ROUNDS` repeats the whole
LMDB-then-ZeroDB sequence, flipping the order on even rounds so thermal drift
is not attributed to one engine.

Reports land in `target/consumer/reports/{lmdb,zerodb}/round-N/` as JSON
Lines, one span per line. [`scripts/bench-compare.py`](../scripts/bench-compare.py)
prints, per workload, the median and minimum run total on each engine and the
dominant spans side by side, with the ZeroDB/LMDB ratio (below 1.00x means
ZeroDB is faster).

Caveats that apply to any number these produce:

- A laptop run is indicative. The representative numbers are Graviton + EBS
  gp3, the production target — see PLAN.md §1.14 and `docs/PERF-GAP-VS-LMDB.md`.
- The workloads include HTTP overhead and the indexer's own CPU work; the
  storage engine is one span family among many. Read the per-span table, not
  just the total.
- Allocator: Meilisearch builds with its production allocator either way, so
  unlike the in-repo criterion bench (issue #67) this comparison is
  allocator-fair.
- Workload assets are downloaded once into `target/consumer/assets/` (the
  movies dataset is ~100 MB; the hackernews ones are larger).
- The runner spawns the server on port 7700. If something local holds it (a
  Docker Meilisearch, say), pass `MEILI_PORT=7799`; the script retargets the
  runner's client URLs for the run and restores the file afterwards.
