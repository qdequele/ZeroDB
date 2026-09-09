# Meilisearch v1.53.1 on LMDB vs ZeroDB — movies workloads, macOS laptop, 2026-09-09

Produced by `scripts/consumer.sh bench` (see docs/CONSUMER-GATE.md). Indicative
only: the production target is Graviton + EBS gp3.

- Meilisearch: v1.53.1 (577f7af28), heed 0.22.1 → heed-shim (ZeroDB 4b4ff2a + uncommitted consumer-gate scripts); zero source changes; `lmdb-master-sys` absent from the ZeroDB binary's dependency tree.
- Machine: Apple M1 Pro, 16 GB, macOS 27.0. Both binaries `cargo build --release -p meilisearch`, same toolchain (rust-toolchain.toml 1.91.1), Meilisearch's production allocator.
- Workloads: `workloads/movies.json` (settings + ~32k documents indexing, run_count 10) and `workloads/search/movies.json` (run_count 10). One round, LMDB first then ZeroDB. Server on port 7799.
- Per-span timings collected by `cargo xtask bench --no-dashboard` through the logs route; `total (self time)` = sum of every span's self time per run; median over the 10 runs.

```text
== movies.json  (runs: lmdb 10, zerodb 10)
   total (self time)                            lmdb    4.486 s   zerodb    4.503 s   ratio  1.00x   (min 4.072 s / 3.977 s)
   span (median time over runs)                        lmdb         zerodb  ratio
   ::meta::total                                   2.886 s      2.922 s    1.01x
   indexing::scheduler::process_batch              2.818 s      2.842 s    1.01x
   indexing::scheduler::apply_index_operation      2.736 s      2.724 s    1.00x
   indexing::write_db::all                         2.583 s      2.565 s    0.99x
   indexing::documents::extract                    2.092 s      2.072 s    0.99x
   indexing::merge::merge_scan_and_send_docids     1.067 s      1.098 s    1.03x
   indexing::merge::merge_and_send_docids          1.017 s      1.051 s    1.03x
   indexing::documents::merge::word_pair_proxim    867.3 ms     900.1 ms   1.04x
   indexing::documents::extract::word_pair_prox    540.3 ms     527.8 ms   0.98x
   indexing::documents::extract::docids_extract    538.2 ms     464.5 ms   0.86x
   indexing::documents::extract::word_docids       412.6 ms     393.9 ms   0.95x
   indexing::documents::merge::word_position_do    102.6 ms      99.9 ms   0.97x

== search-movies.json  (runs: lmdb 10, zerodb 10)
   total (self time)                            lmdb     16.0 ms  zerodb     16.5 ms  ratio  1.03x   (min 8.8 ms / 9.4 ms)
   span (median time over runs)                        lmdb         zerodb  ratio
   ::meta::total                                    16.9 ms      17.4 ms   1.03x
   search::main::execute_search                      4.4 ms       4.3 ms   0.99x
   search::bucket_sort::bucket_sort                  3.6 ms       3.5 ms   0.97x
   search::graph_based::next_bucket                  1.5 ms       1.5 ms   0.99x
   search::graph_based::start_iteration              1.2 ms       1.2 ms   0.96x
   search::universe::resolve_universe                0.6 ms       0.6 ms   1.03x
   search::position::build_edges                     0.5 ms       0.5 ms   1.01x
   search::query::located_query_terms_from_toke      0.2 ms       0.2 ms   1.00x
   search::words::resolve_condition                  0.2 ms       0.2 ms   1.02x
   search::fid::build_edges                          0.2 ms       0.2 ms   1.15x
   search::universe::filtered_universe               0.2 ms       0.0 ms   0.17x
   search::proximity::resolve_condition              0.1 ms       0.1 ms   1.19x

ratio < 1.00x means ZeroDB is faster; spans are summed across calls per run.
```

Reading: the whole indexing pipeline (extract + merge + write) is at parity, 1.00x on the total and 0.99x on `indexing::write_db::all`, the phase that actually touches the storage engine. Search is 1.03x on a 16 ms total, inside run-to-run noise (min 8.8 vs 9.4 ms). Per-span ratios above 1.03x on sub-millisecond search spans are noise, not signal.

Next: the same run with `WORKLOADS="workloads/hackernews-add-new-documents.json ..." ROUNDS=2` (1M-document scale, alternated order), then on Graviton.
