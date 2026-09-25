# Engine comparison ladder — LMDB fork vs ZeroDB, 2026-09-10 (macOS, indicative)

First run of the rung-per-mechanism ladder
(`crates/zerodb-oracle/benches/engine_comparison/`, `just bench`). Reading
guide, and what each rung isolates: [`docs/BENCH-MAP.md`](../../docs/BENCH-MAP.md).

**Ratio = ZeroDB ÷ LMDB; above 1.00 means ZeroDB is slower.** `~` marks rungs
where the two engines are within a standard deviation of each other — not a
finding whatever the ratio says. `⚠` marks a rung whose ratio moved more than
0.15 against its ladder family's base rung; that is where a mechanism announces
itself.

| | |
|---|---|
| Machine | Apple M1 Pro, macOS 27.0, laptop SSD |
| Toolchain | rustc 1.97.0, `--release` (criterion bench profile) |
| Engines | `heed` =0.22.1 (Meilisearch fork, `lmdb-master-sys` 0.2.6) vs `heed-zerodb` @ this tree |
| Page size | 16 KiB (OS page size), pinned identically on both |
| Tier | default (no `get/size/n1m`, no `concurrent` suite) |
| Command | `just bench` then `just bench-report --md` |

**These numbers are indicative, not a referee.** A laptop isolates CPU,
allocator and memcpy plus a local-SSD fsync. Everything in `commit/sync/*`,
`maint/*` and `env/open/create` is dominated by barrier cost, which on the
deployed target (linux-aarch64 Graviton + EBS gp3) is a network round-trip with
completely different economics. Those rows must be re-run there before anything
is claimed from them.

## What this run says

**ZeroDB is at or ahead of parity on the read path.** Every `get/*` rung except
the overflow one (§below) sits at 0.93–1.21×, most of them inside the noise
band; `seek/*` is 0.89–0.93× — ZeroDB faster at every probe pattern — and
`scan/full/fwd` is 1.11×. Transaction setup is not
close: `env/txn/ro_begin_abort` is **0.04×** and `env/txn/rw_empty_commit`
**0.12×** — ZeroDB begins and ends transactions roughly an order of magnitude
faster than the fork, which is the lock-free reader table (ADR-0006) paying off.

**The gaps cluster in four places**, and the ladder names the mechanism for
each:

1. **Deletion — `del/*` at 1.6–2.7×, the largest steady-state gap.** It is not
   concentrated in `churn/reinsert` (1.64×), so this is *not* primarily a
   freelist/GC finding: `bulk/half` (2.02×) and `bulk/all` (1.97×) are worse,
   which puts the cost in the per-key delete and rebalance path itself.
   `del/range/half` at **2.74×** is the sharpest rung here (LMDB 8.7 ms vs
   ZeroDB 23.7 ms for the same span).

   **Followed up the same day — see PERF-GAP `B8`/`B8a` for the full
   diagnosis.** Ablation puts 75 % of the gap in the removal + rebalance step
   (116 ns vs 471 ns per delete, 4.1×): `rebalance` → `borrow_entry`
   round-trips every moved entry through a full `remove_cell` + `insert_cell`
   page rewrite, so 25 K deletes issue 63 542 cell removals and 44 041 cell
   insertions where one each is needed. Separately, `RwCursor::del_current`
   re-descends twice per entry (`B8a`), which is why the cursor delete loop is
   4.2× rather than 2.7×.

2. **Env creation — `env/open/create` at 10.85×, and fully explained.** ZeroDB
   does **two unconditional `fsync`s** per env creation — the data file and then
   the parent directory (`zerodb-io/src/file.rs:245-250`, SPEC 02 §3.4 step 4,
   issue #46) — where LMDB does neither. On macOS Rust's `sync_all` is
   `F_FULLFSYNC`, ~2.5 ms each on this SSD, which accounts for essentially the
   whole 5.5 ms-per-env gap. This is a deliberate durability choice (without it
   a crash just after creation can lose the *name* of an already-durable file),
   it is **once per environment lifetime**, and the rung amplifies it 20× by
   construction. Not a regression; recorded so nobody re-derives it.
   `env/open/reopen` — the same code path minus creation — is **0.41×**.

3. **Copy / compaction — `maint/copy/raw` 2.77×, `maint/copy/compact` 4.72×.**
   Meilisearch calls this on every snapshot, so it is user-visible latency. The
   `compact ÷ raw` step (+1.95) says the rebuild, not the I/O, is the expensive
   half. `copy_raw` is also still the buffered path flagged in the 2026-09-09
   review (a 1× env image in RAM plus a single `fs::write`), which the streaming
   work of PERF-GAP C1 never reached.

4. **Durable commit — `commit/sync/n1` 1.92×, `commit/sync/n100` 1.64×.** The
   long-standing ~1.8× laptop figure, reproduced. This is the row PERF-GAP **B4**
   (vectored/coalesced writes) was built for and the row that has never been
   measured on its actual target. Unchanged conclusion: it needs EBS.

**Two rungs where ZeroDB looks much faster but the run cannot support it.**
`put/val/v4k` (0.48×) and `put/val/v2page` (0.41×) both fall inside the noise
band — the `heavy` profile takes only 10 samples and these rungs rebuild a large
environment per iteration. If the overflow write path really is ~2× LMDB's, that
is worth knowing; it needs a longer run to say so. Note the read side of the
same shape goes the other way: `get/val/v2page` is **1.41×**, the one clear
regression inside the otherwise-flat `get/val` family.

**Smaller, real, low absolute cost:** `scan/full/rev` 1.47× (reverse iteration
only; forward is 1.11×), `scan/prefix/bucket` 1.50× and `scan/range/*` ~1.30×
(positioned descent plus per-step bound test), `scan/meta/len` 2.88× — that last
one is 3.2 ns vs 9.2 ns per call, so the ratio is real and the cost is not.

**Confirmed non-issues.** `put/api/reserved` is 0.99× against LMDB and matches
`put/api/plain`, so the `MDB_RESERVE` work of PERF-GAP B6 / issue #10 has closed
that gap completely. `mixed/rw/8dbs` — the milli-shaped rung, reads and writes
interleaved through one write txn across eight named DBs — is **0.98×**.
`get/db/named ÷ get/db/root` is 1.00, i.e. named-DB resolution now costs nothing
measurable: PERF-GAP **A1** is done and stays done.

## Full table

| rung | LMDB | ZeroDB | ratio | vs family base |
|---|---:|---:|---:|---:|
| `commit/batch/n10k` | 15.024 ms | 16.858 ms | **1.12×** | +0.00 |
| `commit/batch/n1` | 20.514 ms | 32.599 ms | **1.59×** | +0.47 ⚠ |
| `commit/batch/n100` | 8.959 ms | 10.729 ms | **1.20×** | +0.08 |
| `commit/sync/n100` | 1.232 s | 2.015 s | **1.64×** | +0.00 |
| `commit/sync/n1` | 1.028 s | 1.978 s | **1.92×** | +0.29 ⚠ |
| `del/bulk/half` | 11.346 ms | 22.878 ms | **2.02×** | +0.00 |
| `del/bulk/all` | 20.494 ms | 40.374 ms | **1.97×** | -0.05 |
| `del/churn/reinsert` | 13.148 ms | 21.546 ms | **1.64×** | +0.00 |
| `del/clear/all` | 2.110 ms | 2.608 ms | **1.24×** *(noise)* | +0.00 |
| `del/range/half` | 8.672 ms | 23.748 ms | **2.74×** | +0.00 |
| `env/open/reopen` | 2.299 ms | 952.899 µs | **0.41×** | +0.00 |
| `env/open/create` | 10.156 ms | 110.171 ms | **10.85×** | +10.43 ⚠ |
| `env/txn/ro_begin_abort` | 6.370 ms | 245.903 µs | **0.04×** | +0.00 |
| `env/txn/rw_empty_commit` | 603.272 µs | 75.293 µs | **0.12×** | +0.09 |
| `get/access/hot` | 1.352 ms | 1.569 ms | **1.16×** | +0.00 |
| `get/access/miss` | 1.185 ms | 1.433 ms | **1.21×** | +0.05 |
| `get/access/rand` | 3.025 ms | 3.284 ms | **1.09×** | -0.08 |
| `get/access/seq` | 1.799 ms | 1.913 ms | **1.06×** *(noise)* | -0.10 |
| `get/db/root` | 3.313 ms | 3.473 ms | **1.05×** *(noise)* | +0.00 |
| `get/db/named` | 3.312 ms | 3.465 ms | **1.05×** *(noise)* | -0.00 |
| `get/db/named_x8` | 2.933 ms | 3.278 ms | **1.12×** | +0.07 |
| `get/key/k8` | 3.138 ms | 3.310 ms | **1.05×** | +0.00 |
| `get/key/k128` | 5.158 ms | 5.078 ms | **0.98×** *(noise)* | -0.07 |
| `get/key/k32` | 4.348 ms | 4.260 ms | **0.98×** *(noise)* | -0.08 |
| `get/size/n1k` | 1.402 ms | 1.420 ms | **1.01×** *(noise)* | +0.00 |
| `get/size/n50k` | 3.012 ms | 3.296 ms | **1.09×** | +0.08 |
| `get/val/v8` | 1.887 ms | 1.784 ms | **0.95×** | +0.00 |
| `get/val/v256` | 2.032 ms | 1.889 ms | **0.93×** | -0.02 |
| `get/val/v2page` | 1.880 ms | 2.650 ms | **1.41×** | +0.46 ⚠ |
| `get/val/v4k` | 3.379 ms | 3.191 ms | **0.94×** *(noise)* | -0.00 |
| `maint/copy/raw` | 7.398 ms | 20.466 ms | **2.77×** | +0.00 |
| `maint/copy/compact` | 6.272 ms | 29.588 ms | **4.72×** | +1.95 ⚠ |
| `mixed/rw/8dbs` | 42.646 ms | 41.652 ms | **0.98×** *(noise)* | +0.00 |
| `put/api/plain` | 15.724 ms | 15.725 ms | **1.00×** *(noise)* | +0.00 |
| `put/api/reserved` | 16.936 ms | 16.825 ms | **0.99×** *(noise)* | -0.01 |
| `put/order/append` | 12.029 ms | 14.375 ms | **1.19×** *(noise)* | +0.00 |
| `put/order/rand` | 30.943 ms | 32.506 ms | **1.05×** *(noise)* | -0.14 |
| `put/order/seq` | 14.400 ms | 16.051 ms | **1.11×** | -0.08 |
| `put/over/same_size` | 4.860 ms | 5.709 ms | **1.17×** | +0.00 |
| `put/over/grow` | 10.212 ms | 10.069 ms | **0.99×** *(noise)* | -0.19 ⚠ |
| `put/val/v8` | 1.850 ms | 2.780 ms | **1.50×** | +0.00 |
| `put/val/v256` | 2.887 ms | 3.628 ms | **1.26×** | -0.25 ⚠ |
| `put/val/v2page` | 62.160 ms | 25.688 ms | **0.41×** *(noise)* | -1.09 ⚠ |
| `put/val/v4k` | 39.451 ms | 19.116 ms | **0.48×** *(noise)* | -1.02 ⚠ |
| `scan/edge/first_last` | 1.130 ms | 1.202 ms | **1.06×** | +0.00 |
| `scan/full/fwd` | 1.002 ms | 1.110 ms | **1.11×** | +0.00 |
| `scan/full/rev` | 714.640 µs | 1.053 ms | **1.47×** | +0.37 ⚠ |
| `scan/meta/len` | 32.001 µs | 92.251 µs | **2.88×** | +0.00 |
| `scan/prefix/bucket` | 4.144 µs | 6.234 µs | **1.50×** | +0.00 |
| `scan/range/1pct` | 7.861 µs | 10.176 µs | **1.29×** | +0.00 |
| `scan/range/10pct` | 68.216 µs | 89.037 µs | **1.31×** | +0.01 |
| `seek/ge/seq` | 2.952 ms | 2.695 ms | **0.91×** | +0.00 |
| `seek/ge/gap` | 3.787 ms | 3.354 ms | **0.89×** | -0.03 |
| `seek/ge/rand` | 3.569 ms | 3.311 ms | **0.93×** | +0.01 |

## Reproducing

```bash
just bench                 # the whole ladder (~45 min on this machine)
just bench del             # one suite
just bench-report          # the table above
just bench-report --md     # ...as Markdown
just bench-long            # + the 1M-entry depth rung and the concurrent suite
```
