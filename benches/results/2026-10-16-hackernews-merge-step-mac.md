# Meilisearch hackernews additions: the merge step, on a 16 GiB Mac

- **Date:** 2026-10-16.
- **Question:** on the 8 GiB Graviton4 host (`2026-10-15-meilisearch-v1.53.1-graviton-xlarge.md`), Meilisearch v1.53.1's two merge spans took about twice as long on ZeroDB as on LMDB while the whole batch took 1.03× as long. Is that the engine (the lock file, aarch64, the read path) or the host?
- **ZeroDB:** `main` at `bee8865`, lock file on (the default), the consumer-script fixes of #104.
- **Consumer:** Meilisearch v1.53.1, built with stable Rust; same build recipe as the Graviton run.

## Host

Apple Silicon Mac, 16 GiB, macOS, local SSD. Not a quiet benchmark host: the ratios are indicative. The difference with the Graviton result (0.7× against 2×) is far larger than the noise of such a host.

## Real workload: `hackernews-add-new-documents`

`scripts/consumer.sh bench` with `WORKLOADS=workloads/hackernews-add-new-documents.json ROUNDS=2` (2 rounds of 3 runs per engine, the order flipped in the second round). Median inclusive time per span over the 6 runs, ZeroDB ÷ LMDB.

| span | LMDB | ZeroDB | ratio |
|---|---:|---:|---:|
| `::meta::total` (the whole batch) | 22.586 s | 20.191 s | 0.89× |
| `indexing::write_db::all` | 16.119 s | 14.803 s | 0.92× |
| `indexing::documents::extract` | 14.522 s | 12.171 s | 0.84× |
| `indexing::merge::merge_scan_and_send_docids` | 7.799 s | 5.541 s | 0.71× |
| `indexing::merge::merge_and_send_docids` | 7.344 s | 5.291 s | 0.72× |
| `indexing::post_processing::post_process` | 3.955 s | 3.074 s | 0.78× |
| `indexing::scheduler::commit` | 2.219 s | 1.946 s | 0.88× |

On the Graviton host the two merge spans were 2.06× and 2.13×. Here they are 0.71× and 0.72×, with the lock file on and on aarch64, so neither the lock file nor the architecture explains the Graviton result.

## Synthetic: `merge_census`

`crates/zerodb-oracle/examples/merge_census.rs` reproduces the merge step's shape: `threads` workers, each with its own read transaction, read every key of their share in key order, read every byte of the value, build the merged value and send it over a bounded channel to one writer that puts it into an open write transaction. Values follow posting-list sizes (most 8–128 bytes, 1 in 64 of 8–32 KiB, 1 in 4096 of 64–576 KiB).

| setup | LMDB merge | ZeroDB merge | notes |
|---|---:|---:|---|
| Mac, 1M keys, 1 key in 4 merged, 4 threads | 468–481 ms | 505–529 ms | ZeroDB's gets 108 ms against 135–141 ms; puts 240 ms against 225 ms; reads alone 40 ms against 49 ms |
| same, `NO_LOCK` | — | 505–552 ms | the lock file makes no difference |
| Mac, 2M keys, every key merged | — | parity to 1.13× | ZeroDB peak RSS 2456–2535 MiB against 2200–2308 MiB |
| Linux container (aarch64, 4 CPUs, memory capped at 2 GiB, no swap) | — | 1.05–1.07× | ZeroDB's gets about 1.2× slower than LMDB's on Linux |

No setup reproduces a 2× merge step, even with memory capped at 2 GiB.

## Reading

The merge step on ZeroDB is not slower than on LMDB in itself: it is faster on the Mac, and within 7% in the synthetic shape on Linux under a 2 GiB cap. What differs on the Graviton host is the host: 8 GiB of memory, a gp3 network volume, and swap used during the session. ZeroDB's peak memory on this workload is a little above LMDB's (`docs/PERF-GAP-VS-LMDB.md`, peak RSS row), which leaves less room for the page cache on 8 GiB and would send more of the merge step's reads to the slow volume. That is the most likely cause; it is not proven.

Next step: measure where ZeroDB's extra memory goes in a large write transaction, against LMDB, and confirm with a memory-capped Linux run of the real workload.

## Reproduce

```
WORKLOADS=workloads/hackernews-add-new-documents.json ROUNDS=2 \
  MEILISEARCH_REF=v1.53.1 RUSTUP_TOOLCHAIN=stable scripts/consumer.sh bench

cargo run --release -p zerodb-oracle --example merge_census -- lmdb 1000000 4 4
cargo run --release -p zerodb-oracle --example merge_census -- zerodb 1000000 4 4
```
