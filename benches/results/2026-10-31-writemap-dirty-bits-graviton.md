# WRITE_MAP bulk writes: dirty-page lookup without the hash index (Graviton4, 2026-10-31)

Host: c8g.xlarge (Graviton4, 4 vCPU), Ubuntu 24.04, 60 GiB gp3 root, eu-west-1. Base: main `0adf868`.
Candidate: main + the frame-bits change (uncommitted at the time of the run).

## Why

The #13 bulk-rung profile (`put/order/seq`, `commit/batch/n10k` under `WRITE_MAP`) put 15–17 % of
cycles in finding a write txn's dirty pages: `DirtyStore::bytes` from the descent
(`search_path_into` → `Source::bytes_from_classified`), `bytes_mut`, and the backing's map-view call.
Most of that time was cache misses in the hash probe. LMDB's `mdb_page_get` skips its dirty-list
search under `MDB_WRITEMAP`, because a dirty page then lives at `map + pgno * psize`. ZeroDB still
has to tell dirty pages from file pages: file pages are validated and memoized, and dirty frames
must not be memoized.

## Change (option 2: a cheaper dirty-page test)

- `DirtyStore` keeps two bits per pgno, mirroring its frame index: no frame, a one-page frame in the
  map at the pgno itself, or any other frame (an overflow-run head or a heap frame). They live in 4 KiB
  chunks of 16,384 pages, each allocated on first use. An in-map store builds the bits once it holds
  64 frames, so a small write txn pays nothing. Every change to the index updates them, and debug
  builds check every answer against the index.
- `bytes`, `bytes_mut`, `contains` and `in_map` answer a one-page map frame from the bits without
  probing the index.
- The write txn's page source resolves such a frame by slicing its own whole-map view, which in
  in-map mode is the same mapping. That also skips the backing call that `DirtyStore::bytes` made on
  every lookup.
- Validation is unchanged: dirty frames are still classified as dirty (header checks only, never
  memoized).
- Heap mode (no `WRITE_MAP`) never builds the bits.

## Option 1 measured for comparison (not proposed)

A throwaway variant read every page of a trusted (`FileTrust::trust_contents`) in-map writer straight
from the map, with no dirty test at all, which is LMDB's model. Under the trust contract that is sound,
but it changes how trusted files are validated, so it would need an ADR-0014 amendment. Its marginal
gain over option 2 (trusted vs not, same binary) is 1.7 % on seq, 0.8 % on rand and 3.4 % on n10k,
which is not worth an ADR now.

## Census (`writemap_bulk_census`, mean of 3 interleaved rounds of the min over 40 fresh envs, ns/put)

`WRITE_MAP | NO_SYNC`, 4 KiB pages. `-T` = trusted file. Each ratio is against LMDB `WRITE_MAP`.

| shape | LMDB | before | before -T | after (opt2) | after -T | opt1 | opt1 -T |
|---|---:|---:|---:|---:|---:|---:|---:|
| seq 50k | 175.9 | 185.8 (1.06×) | 184.7 (1.05×) | 164.1 (0.93×) | 163.3 (0.93×) | 160.6 (0.91×) | 157.9 (0.90×) |
| rand 50k | 349.0 | 367.8 (1.05×) | 368.3 (1.06×) | 360.3 (1.03×) | 359.7 (1.03×) | 359.0 (1.03×) | 356.2 (1.02×) |
| v4k 10k | 1339.4 | 1523.5 (1.14×) | 1502.1 (1.12×) | 1524.0 (1.14×) | 1497.2 (1.12×) | 1524.8 (1.14×) | 1502.5 (1.12×) |
| n10k (5 × 10k) | 176.2 | 185.7 (1.05×) | 184.7 (1.05×) | 168.6 (0.96×) | 168.2 (0.95×) | 165.5 (0.94×) | 159.9 (0.91×) |

The round-to-round spread is under 1 % everywhere.

## Profile (`perf record -g`, census seq, 60 runs)

| symbol | before | after |
|---|---:|---:|
| `RwTxn::search_path_into` (self) | 22.1 % | 30.8 % (the bit test is inlined here) |
| `DirtyStore::bytes` | 14.5 % | — (inlined into the descent) |
| `DirtyStore::bytes_mut` | 3.8 % | 2.5 % |
| `WriteMapBacking::map_dirty_page` | 2.6 % | below the top entries |

Descent plus lookup goes from 36.6 % to 30.8 % of a shorter run.

## Ladder A/B (`scripts/bench-ab.sh`, 3 interleaved rounds)

With `ZERODB_BENCH_WRITE_MAP=1`: **improved 7, regressed 0, flat 4, unreliable 0.**

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | verdict |
|---|---:|---:|---:|---:|---:|---|
| `commit/batch/n1` | 1.314 ms | 3.739 ms | 3.733 ms | 2.85× → 2.84× | 0.998 | flat |
| `commit/batch/n100` | 4.565 ms | 5.519 ms | 5.506 ms | 1.21× → 1.21× | 0.999 | flat |
| `commit/batch/n10k` | 11.220 ms | 12.508 ms | 11.210 ms | 1.11× → **1.00×** | 0.896 | faster |
| `put/api/plain` | 11.128 ms | 12.477 ms | 11.016 ms | 1.12× → **0.99×** | 0.890 | faster |
| `put/api/reserved` | 11.232 ms | 14.559 ms | 12.543 ms | 1.30× → **1.12×** | 0.866 | faster |
| `put/order/append` | 6.308 ms | 9.797 ms | 8.676 ms | 1.55× → **1.38×** | 0.886 | faster |
| `put/order/rand` | 28.798 ms | 32.119 ms | 30.730 ms | 1.12× → **1.07×** | 0.957 | faster |
| `put/order/seq` | 11.152 ms | 12.481 ms | 11.027 ms | 1.12× → **0.99×** | 0.886 | faster |
| `put/val/v256` | 2.103 ms | 2.389 ms | 2.159 ms | 1.14× → **1.03×** | 0.904 | faster |
| `put/val/v4k` | 13.605 ms | 15.426 ms | 15.302 ms | 1.13× → 1.12× | 0.992 | flat |
| `put/val/v8` | 1.375 ms | 1.457 ms | 1.470 ms | 1.06× → 1.07× | 1.008 | flat |

Control without `WRITE_MAP` (heap mode never builds the bits): **flat 9 of 9**, every rung within
±1.7 % (`put/order/seq` 1.013, `commit/batch/n10k` 1.003, `put/order/append` 1.017).

## What is left

- `put/val/v4k` (1.12×): a 4 KiB value is a two-page overflow run, and the in-map run frame is
  zero-filled before the value is copied in (`DirtyStore::insert_run_frame`). LMDB doesn't zero it.
  The lookup change doesn't touch this; it is a separate lever. The `put_reserved` zero guarantee
  (TXN-47) has to be kept.
- `commit/batch/n1` (2.84×) and the small-commit gap belong to #8 and the validated-pages work.
- `put/order/append` (1.38×) is the #138 work.
