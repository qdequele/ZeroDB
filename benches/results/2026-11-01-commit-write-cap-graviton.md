# Commit writes capped at 256 KiB per call (issue #135) — Graviton4, 2026-10-29 to 2026-10-31

Hosts: c8g.xlarge (Graviton4 Neoverse-V2, 4 vCPU, 8 GiB), Ubuntu 24.04, kernel 7.0, ext4 on gp3.
LMDB = the Meilisearch fork through heed 0.22.1 (for Meilisearch: its own heed fork on stock LMDB).

This report collects three runs made while the change was being decided. The raw ladder and profile data of the
2026-10-29 runs were lost with their hosts; their figures below are the ones recorded in issue #135 at the time.
The consumer gate's raw reports were kept.

## The change

A commit writes each run of contiguous dirty pages with `pwritev`. ZeroDB sent up to 512 pages per call, 2 MiB at
4 KiB pages. It now sends at most 256 KiB per call (one page when a page is larger), LMDB's 64 pages
(`MDB_COMMIT_PAGES`) at 4 KiB. A multi-page overflow value can be cut across calls. The iovec array now lives on the
stack (64 segments) instead of a `Vec` per call.

Crash safety does not change: the pages a commit writes are referenced by no snapshot until the meta page is written
after them, so more calls change nothing a reader or a crash can observe.

## Why: large page-cache folios

On kernels and filesystems that cache files in large folios (XFS, and ext4 on recent kernels), a buffered write's
length sets the folio size the page cache allocates for it. A bulk commit written in 2 MiB calls leaves the file in
large folios, and every later commit that rewrites one 4 KiB page inside one pays far more kernel time in
`ext4_block_write_begin`.

10 million 16-byte keys loaded in one transaction, 100-byte values, then single-put `NO_SYNC` commits (the shape of
rust-storage-bench's `a_nosync`), 2026-10-29, `perf stat`:

| | write p50 | kernel cycles per commit |
|---|---:|---:|
| LMDB | 7.6–7.9 µs | 16–17k |
| ZeroDB, 2 MiB per call (before) | 12.1–12.6 µs | 24–27k |
| ZeroDB, 256 KiB per call (after) | 8.7–8.8 µs | ~14k |
| ZeroDB, 16 KiB per call | 7.5 µs | 12–13k |
| ZeroDB, one page per call | 7.2–7.3 µs | ~11k |

`ext4_block_write_begin` was 32 % of ZeroDB's write-path cycles before, against 18 % for LMDB. Bytes written to the
device are the same in every column. Reads and commits on a small tree did not move.

## Ladder, 2026-10-29 (main before #138, #137, #8 and #13 merged)

`scripts/bench-ab.sh`, 3 interleaved rounds, ZeroDB time ÷ LMDB time.

| cap | small commits | bulk large-value writes |
|---|---|---|
| 16 KiB (not kept) | `del/random/n1` 1.58× → ~1.00× | `put/val/v4k` 1.57×, `put/val/v2page` 1.63×, `commit/batch/n10k` 1.22× its old time |
| 256 KiB (kept) | `del/random/n1` 1.58× → 1.20× | `put/val/v4k` 0.99× → 1.10×, `put/val/v2page` 1.10× its old time; most rungs flat |

The bulk large-value rungs are the cost of the cap: they write fresh multi-page values in one commit on a small
file, so more calls is all they see.

## Meilisearch v1.55.0 consumer gate, 2026-10-31

`scripts/consumer.sh`-style run with three Meilisearch binaries: stock LMDB, ZeroDB `main` at `53b924c`, and the same
tree with the 256 KiB cap. 3 rounds, run order rotated per round. Built with rustc 1.98.1 through the
`heed-shim-unique-txns` shim. `::meta::total` is the median inclusive time of the whole workload.

| workload | LMDB | ZeroDB before | ZeroDB after | after ÷ before | after ÷ LMDB |
|---|---:|---:|---:|---:|---:|
| hackernews additions, `::meta::total` | 47.51 s | 47.38 s | 43.70 s | 0.92 | 0.92 |
| … fastest run, total self time | 59.15 s | 59.04 s | 53.46 s | 0.91 | 0.90 |
| … `indexing::merge::merge_and_send_docids` | 13.55 s | 15.77 s | 7.64 s | 0.48 | 0.56 |
| … `indexing::scheduler::commit` | 10.14 s | 9.58 s | 9.40 s | 0.98 | 0.93 |
| movies indexing, `::meta::total` | 4.30 s | 4.31 s | 4.30 s | 1.00 | 1.00 |
| movies search, `::meta::total` | 9.7 ms | 9.7 ms | 9.7 ms | 1.00 | 1.00 |

Bulk indexing does not regress; incremental additions to a large index get faster. The merge step updates
entries of an index that earlier, bulk commits wrote, the case the cap targets. It is likely also the cause of the
merge spans that read 2.1× LMDB on this host size on 2026-10-15
(`2026-10-15-meilisearch-v1.53.1-graviton-xlarge.md`), first put down to the host's memory.

One span moved the other way: movies `indexing::documents::extract::docids_extract` 1.03 s → 1.26 s, with the
workload total flat.

## Not measured

- hannoy: the same gate ran its divan benches, but its results were lost when the host's safety shutdown fired
  before they were copied.
- The ladder on current `main` (the 2026-10-29 run predates four write-path merges).
- XFS, and an ext4 kernel without large folios, where no change is expected.
