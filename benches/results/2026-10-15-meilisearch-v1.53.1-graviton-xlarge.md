# Meilisearch v1.53.1 and hannoy on ZeroDB, Graviton4 c8g.xlarge

- **Date:** 2026-10-15.
- **ZeroDB:** `main` at `bee8865` (lock file on by default, lock file rebuilt in place) plus the consumer-script fixes on a feature branch (`256cbc0`). The engine and the heed shim are those of `main`.
- **Consumers:** Meilisearch v1.53.1, built with stable Rust 1.99 because its toolchain pin (1.91.1) is below ZeroDB's MSRV. hannoy v0.2.0 for the test suite, v0.1.7-nested-rtxns for the benchmarks, because v0.2.0's bench target does not compile (it resolves `rand` 0.10, where `fill` moved to another trait).
- **Why not Meilisearch v1.55.0:** it builds against Meilisearch's heed fork, whose API ZeroDB does not provide yet (issue #102).

## Host

- AWS Graviton4 `c8g.xlarge`: 4 vCPU, 8 GiB, Ubuntu 24.04 arm64, eu-west-1, 150 GiB gp3 root volume, ext4. Launched for this run and terminated after it.
- A 16 GiB swap file and `CARGO_BUILD_JOBS=2`, so the Meilisearch test build fits in 8 GiB. The swap was used during the session (about 3 GiB swapped out in total), most likely by the test builds; the counters cover the whole session and cannot be attributed to one step.
- Earlier Meilisearch figures came from an x86-64 server with more memory (`2026-09-28-evening-bench-server-linux-x86.md`, `2026-10-05-meta-annex-real-case.md`), so the ratios below are not directly comparable to them.

## Drop-in: test suites on ZeroDB

`scripts/consumer.sh check`, then `FULL=1 scripts/consumer.sh suites`, then `scripts/hannoy.sh suites`. Each build passes the check that `lmdb-master-sys` (the C LMDB fork) is absent from the dependency tree.

| suite | passed | failed | ignored |
|---|---:|---:|---:|
| milli unit tests | 271 | 0 | 3 |
| milli integration tests | 92 | 0 | 0 |
| index-scheduler | 80 | 0 | 0 |
| meilisearch-auth | 4 | 0 | 0 |
| meilisearch `--lib` | 12 | 0 | 2 |
| hannoy v0.2.0 unit tests | 44 | 0 | 1 |
| hannoy v0.2.0 doc tests | 15 | 0 | 0 |

All the ignored tests are ignored upstream. milli's `filter_depth`, which ran out of memory on an 8 GiB host on either engine before, passed with the swap file.

## Meilisearch benchmarks

`ROUNDS=3 scripts/consumer.sh bench` with `workloads/movies.json`, `workloads/search/movies.json` and `workloads/hackernews-add-new-documents.json`. Each round runs both engines, and even rounds flip the order.

The headline figure is the median server-side time of a run, the `::meta::total` span, as in the earlier reports. `scripts/bench-compare.py` also prints a sum of every span's self time per run; spans that run at the same time on different threads are all counted, so that sum overstates the time a run takes and is given here only for reference.

| workload | runs per engine | LMDB | ZeroDB | ZeroDB ÷ LMDB | self-time sum ratio |
|---|---:|---:|---:|---:|---:|
| movies indexing | 30 | 4.346 s | 4.436 s | **1.02×** | 0.99× |
| movies search | 30 | 9.6 ms | 9.5 ms | **0.99×** | 0.99× |
| hackernews incremental additions | 9 | 45.666 s | 46.880 s | **1.03×** | 1.15× |

Hackernews additions, self-time sums per run and round (seconds):

| | round 1 | round 2 | round 3 |
|---|---|---|---|
| LMDB | 84.3, 60.3, 57.1 | 60.3, 61.3, 59.2 | 57.9, 60.9, 65.5 |
| ZeroDB | 79.1, 68.8, 61.8 | 69.8, 63.0, 73.4 | 69.9, 69.0, 69.5 |

Hackernews additions, the spans that move (median inclusive time per run):

| span | LMDB | ZeroDB | ratio |
|---|---:|---:|---:|
| `indexing::merge::merge_scan_and_send_docids` | 7.939 s | 16.370 s | 2.06× |
| `indexing::merge::merge_and_send_docids` | 7.502 s | 15.943 s | 2.13× |
| `indexing::documents::extract` | 23.933 s | 31.981 s | 1.34× |
| `indexing::scheduler::commit` | 9.918 s | 9.170 s | 0.92× |
| `indexing::post_processing::post_process` | 8.429 s | 5.402 s | 0.64× |

- Movies indexing and search are at parity, as on the x86 server.
- **Hackernews incremental additions take 3% longer on ZeroDB** (1.2 s per run). The x86 server measured this workload at 0.99× on 2026-10-05, on the same span.
- Inside it, the two merge spans take twice as long on ZeroDB. They read the existing posting lists back from the database while merging the new ones. They run in parallel with other work, so they raise `indexing::write_db::all` by 5 s (1.19×) rather than the whole run, and ZeroDB gets 3.7 s back in commit and post-processing.
- Three things differ from the 0.99× run: this host has 8 GiB against a larger x86 server; the lock file is now on by default; and the architecture is aarch64. ZeroDB's peak memory on this workload was already slightly above LMDB's (`docs/PERF-GAP-VS-LMDB.md`), and an 8 GiB host leaves little page cache. This run cannot separate these causes.

## hannoy benchmarks

`HANNOY_REF=v0.1.7-nested-rtxns ROUNDS=2 scripts/hannoy.sh bench`, divan medians.

| bench | LMDB | ZeroDB | ZeroDB ÷ LMDB |
|---|---:|---:|---:|
| `build_hnsw` 512 | 1.411 s | 1.325 s | 0.94× |
| `build_hnsw` 768 | 1.618 s | 1.544 s | 0.95× |
| `build_hnsw` 1536 | 2.263 s | 2.206 s | 0.97× |
| `search_hnsw` 512 | 1.65 ms | 1.55 ms | 0.94× |
| `search_hnsw` 768 | 1.95 ms | 1.84 ms | 0.94× |
| `search_hnsw` 1536 | 2.60 ms | 2.17 ms | 0.83× |

ZeroDB is ahead on every hannoy bench, as on the x86 server (build 0.84–0.89×, search 0.95×).

## Script fixes this run needed

All in `scripts/` on a feature branch:

- The heed patch goes inside a consumer's existing `[patch.crates-io]` section, and every git source of heed is patched too (both needed for Meilisearch v1.55.0, which still fails on the API gap above).
- A consumer without a `[workspace]` table (hannoy v0.2.0) gets an empty one for both builds, so cargo does not take it for a member of the ZeroDB workspace.
- Each benchmarked Meilisearch server starts only once the port is free. The xtask runner does not wait for the previous server to exit, and on this host the next server twice failed with "Address already in use".
