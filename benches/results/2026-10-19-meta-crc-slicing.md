# Meta CRC on the commit path: slicing-by-16

> **Rebased 2026-10-20.** `main` took its own slicing-by-8 table in #117
> while this work was in review, so the slicing-by-16 commit measured here
> was dropped when the branch was rebased; `main`'s slicing-by-8 is the
> software path. The hardware CRC32C (ADR-0031, numbered 0029 when these
> runs were made) and the `del/random/n1` rung were kept. Commit hashes in
> this report are from the pre-rebase branch and are not in `main`'s
> history.
>
> This report stays as the record of the profile (78 % of a single-delete
> commit in the meta CRC), the comparison with LMDB's technique and the
> slicing-by-16 measurements.

- **Date:** 2026-10-19.
- **Question:** on crud-bench, ZeroDB deletes (one delete per write transaction) ran at about half of LMDB's throughput while creates and updates were near parity. A profile put most of the commit in `MetaPage::encode_with_annex`. What does that cost, and can it be removed without changing the on-disk format?
- **ZeroDB:** `main` at `f98cd08` (before) and `9f26931` (after). crud-bench was run on the delete-rebalance branch (ADR-0028, `0bd9bd7`), before and after the same change.
- **Host:** Apple M-series Mac, 16 KiB OS pages, macOS. Not a quiet host: other sessions were benchmarking at the same time (load 4–11 on 8 cores). Every A/B below interleaves its sides and measures LMDB in both halves as the drift control.

## Where the time went

Every commit writes a meta page whose CRC32C covers bytes `[0, 172)` and the free-list annex ids (SPEC 02 §3.3, ADR-0022). The annex is the commit's freed-page list when it fits in the meta page: up to `(psize − 176) / 8` ids, 2,026 at 16 KiB.

Repro: 200,000 random 26-byte keys with 1,500-byte values, 16 KiB pages, `WRITE_MAP | MAP_ASYNC | NO_SYNC`, trusted-file mode, then every key deleted in shuffled order, one delete per write transaction. The annex length, read from the newest meta slot every 5,000 commits, stayed between 0 and 1,994 ids and was above 1,000 most of the time. It fills as the shrinking tree frees pages, spills to the free-list tree when full, and fills again.

`sample` profile of the delete phase, before the change (main thread, 4 s):

| frame | samples | share of commit |
|---|---:|---:|
| `RwTxn::commit` (all) | 2,107 | 100 % |
| `MetaPage::encode_with_annex` self time | 1,642 | 78 % |
| `freelist_save` → `put_pil` | 158 | 7 % |
| `merge_sorted_unique` | 75 | 4 % |
| writer unlock (`fcntl`) | 50 | 2 % |
| `write_at_page` (meta copy into the map) | 45 | 2 % |

The self time is the CRC: the table implementation consumed one byte per step through a chain of dependent table lookups, 0.39 GB/s, so 42 µs for a full 16 KiB page. Writing the ids and zeroing the page are memcpy/memset speed and did not register.

## What LMDB does

LMDB's meta page has no checksum. Its commit writes only the meta fields that changed, about 100 bytes, in place in the map under `WRITE_MAP` or with one `pwrite` otherwise. The freed-page list goes into the free-list tree as one record per transaction (`mdb_freelist_save`). So LMDB pays nothing for this step. The checksum is a deliberate ZeroDB difference (ADR-0002 §D1: torn-meta detection), and covering the annex with it is format v2 (ADR-0022), so the technique to borrow is not LMDB's. The goal is to make the checksum cheap enough not to matter.

## Change

`crc32c` folds sixteen bytes per step ("slicing-by-16"): sixteen 256-entry tables (16 KiB, built at compile time), where table `k` gives a byte's contribution followed by `k` zero bytes. One step does sixteen independent lookups and XORs instead of a chain of sixteen dependent ones. The trailing `len % 16` bytes use the byte-wise step. Same polynomial, same output, so no format change, no `unsafe`, and no new dependency.

| CRC32C of 16 KiB | time | throughput |
|---|---:|---:|
| byte at a time (before) | 42.3 µs | 0.39 GB/s |
| slicing-by-8 (tried) | 8.8 µs | 1.87 GB/s |
| slicing-by-16 (kept) | 5.7 µs | 2.89 GB/s |

Slicing-by-16 was about 1 µs per commit better than by-8 end to end on the repro (17.6 vs 18.6 µs), so it was kept despite its larger tables. Also tried and dropped: writing the annex ids with one bulk copy and zeroing only the bytes the encoder does not write. They measured the same as the CRC change alone (17.0 vs 17.1 µs), so they were left out.

## Results

**Repro** (5 interleaved runs, median µs per single-delete commit):

| LMDB | ZeroDB before | ZeroDB after |
|---:|---:|---:|
| 8.1 | 36.1 | 17.1 |

**Criterion, new rung `del/random/n1`** (50,000 keys with 1.5 KiB values emptied in shuffled order, one delete per transaction; `scripts/bench-ab.sh`, 5 rounds):

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `del/random/n1` | 1.239 s | 1.620 s | 1.238 s | 1.31× → **1.00×** | 0.771 (±0.051) | 1.002 | faster |

A first 3-round run gave 0.800 (±0.058) with LMDB drift 1.033, over the 3 % gate. The same rung with 256-byte values (an earlier draft) was flat at 0.936 (±0.115), because a tree of ~900 pages never fills the annex.

**Criterion, standard rungs** (3 rounds, load 11):

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `commit/batch/n1` | 18.732 ms | 25.517 ms | 23.233 ms | 1.36× → 1.24× | 0.882 (±0.270) | 0.992 | flat |
| `commit/batch/n100` | 7.594 ms | 8.360 ms | 7.681 ms | 1.10× → 1.01× | 0.932 (±0.194) | 0.972 | flat |
| `commit/batch/n10k` | 12.488 ms | 9.952 ms | 10.127 ms | 0.80× → 0.81× | 0.996 (±0.043) | 0.990 | flat |
| `commit/sync/n1` | 836.106 ms | 1.650 s | 1.651 s | 1.97× → 1.98× | 1.001 (±0.049) | 1.003 | flat |
| `commit/sync/n100` | 987.542 ms | 1.728 s | 1.709 s | 1.75× → 1.73× | 1.010 (±0.195) | 1.011 | flat |
| `commit/sync/n1_map16g` | 845.751 ms | 1.750 s | 1.653 s | 2.07× → 1.95× | 0.991 (±0.063) | 1.059 | drift |
| `del/churn/reinsert` | 10.210 ms | 17.869 ms | 18.180 ms | 1.75× → 1.78× | 0.987 (±0.080) | 1.002 | flat |
| `env/txn/ro_begin_abort` | 6.281 ms | 315.858 µs | 318.533 µs | 0.05× → 0.05× | 1.008 (±0.030) | 0.980 | flat |
| `env/txn/rw_empty_commit` | 599.585 µs | 989.187 µs | 985.065 µs | 1.65× → 1.64× | 0.982 (±0.030) | 0.969 | drift |

No regression. These rungs keep the annex short, so the CRC was never their cost.

**crud-bench** (500,000 `string26` keys, 128 clients, 48 threads, `--optimised`: `WRITE_MAP | MAP_ASYNC | NO_SYNC`, trusted-file mode; 3 rounds, medians in ops/s; ADR-0028 branch before and after this change):

| metric | LMDB (before run) | LMDB (after run) | ZeroDB before | ZeroDB after | after ÷ before | ZeroDB/LMDB before → after |
|---|---:|---:|---:|---:|---:|---:|
| Create | 62,442 | 63,278 | 59,177 | 59,103 | 1.00× | 0.95× → 0.93× |
| Update | 62,379 | 62,298 | 61,195 | 60,961 | 1.00× | 0.98× → 0.98× |
| Delete | 64,801 | 64,306 | 33,336 | 58,941 | **1.77×** | 0.51× → **0.92×** |

Per round, ZeroDB deletes were 33,503 / 33,336 / 33,309 before and 56,647 / 58,955 / 58,941 after. The read and scan rows of the same runs moved in both directions by up to ±15 %, as much as LMDB's own (batch_read_100: LMDB 3,774 vs 5,493). That is host noise: the change does not touch the single-process read path.

## Scorecard

| | |
|---|---|
| **Faster** | Single-delete commits on a large tree 2.1× faster (repro); `del/random/n1` 1.31× → 1.00× LMDB; crud-bench deletes 0.51× → 0.92× LMDB. Standard rungs flat. |
| **Behavior** | Identical. The CRC function returns the same value for every input (bit-at-a-time reference test at every length 0–300 from every block offset, on 16 KiB, and with the stream split at every byte; RFC 3720 vectors unchanged). Files are byte-identical to before; no format, API or LMDB-observable change. `cargo test --workspace` passes; differential fuzz `diff_ops` 56,389 runs in 10 min, `fuzz_image_open` 6.3 M runs in 2 min, no finding. |
| **Safe** | No `unsafe`. Table indexes are masked to 8 bits, so every lookup is in bounds by construction. Miri passes on all `zerodb-core` tests. |
| **Durable** | Commit ordering and barriers unchanged. Crash smoke passes: 229 single-process cycles (15 image cuts, 47 SIGKILL) and 57 multi-process cycles. |
| **Secure** | Torn-meta and hostile-meta detection unchanged: same CRC, same coverage, and the annex count is still bounds-checked before the CRC reads it. `fuzz_image_open` covers hostile meta pages. |

## What is left

- After the change the CRC is ~5 µs of a 17 µs single-delete commit against LMDB's 8 µs. ADR-0002 §D1 plans hardware CRC32C (the ARMv8 `crc32c` instructions, also on Graviton). That would cut the CRC by another 5–10×. It needs `std::arch` intrinsics, which the `unsafe` policy does not yet allow outside its listed modules, so it would need its own decision.
- The rest of the gap to LMDB is the free-list save (`put_pil`, `merge_sorted_unique`), the writer lock's `fcntl` pair on macOS, and the 16 KiB meta copy (LMDB writes ~100 bytes). Each is a few percent of the commit.
