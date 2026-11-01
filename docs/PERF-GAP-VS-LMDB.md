# ZeroDB performance vs LMDB

This page says where ZeroDB stands against LMDB today, which techniques closed
each gap, what was tried and dropped, and what is still open. It is a summary:
every number below links to the report that measured it.

ZeroDB is a drop-in for LMDB at the level of **heed**, the Rust LMDB wrapper.
Its two consumers are Meilisearch, whose indexing and search engine **milli**
does nearly all its storage work through heed, and **hannoy**, an HNSW
vector-search index built on heed. The comparison point is the LMDB fork
Meilisearch actually ships (`lmdb-master-sys 0.2.6`, heed 0.22.1).

## How it is measured

**Ratio = ZeroDB time ÷ LMDB time. Below 1.00, ZeroDB is faster.** Where a
source reports throughput (ops/s), both figures are given and the ratio is
LMDB ops/s ÷ ZeroDB ops/s, which is the same time ratio.

- **Microbench ladder** — criterion benches in
  `crates/zerodb-oracle/benches/engine_comparison/`, run with `just bench` and
  read with `just bench-report`. Each rung isolates one mechanism, so a jump
  between adjacent rungs names that mechanism's cost.
  [`BENCH-MAP.md`](BENCH-MAP.md) says what each rung isolates.
- **Bench server** — the reference machine for the ladder: Intel Xeon
  E3-1230 v2 (x86-64, 4C/8T, turbo off), SATA SSD RAID 1, Debian 12, 4 KiB
  pages. Rungs are read at codegen-units 16 (the default, hannoy's build) and
  at codegen-units 1 (Meilisearch's release build); the build is named where
  it matters.
- **Graviton4 NVMe** — AWS `m8gd.xlarge` (aarch64, local NVMe), the target
  hardware, for YCSB runs.
- **YCSB** — [rust-storage-bench](https://github.com/marvin-j97/rust-storage-bench)
  with a `zerodb` backend next to its heed one: 10M items, one txn per
  operation, usually under a 2 GB memory cap so the data does not fit in RAM.
- **Consumers end to end** — Meilisearch (`cargo xtask bench` workloads,
  through `scripts/consumer.sh`) and hannoy v0.1.7 (its divan benches), each
  built once against LMDB and once against ZeroDB. The recorded Meilisearch
  numbers are on v1.53.1. The gate now defaults to v1.55.0, which builds
  against Meilisearch's heed fork through the `unique-txns` shim (ADR-0027);
  no v1.55 benchmark is recorded yet.

Raw numbers live in [`benches/results/*.md`](../benches/results/), one report
per run. Every lever ever tried, kept or reverted, is one line in
[`benches/results/perf-ledger.jsonl`](../benches/results/perf-ledger.jsonl).

## Where ZeroDB stands today

Default options unless stated. "Opt-in" rows use the trusted-file policy
(ADR-0014) or the sequential-writes option (ADR-0015), which consumers do not
set today. Source keys:

- **[E]** [`2026-09-28-evening-bench-server-linux-x86.md`](../benches/results/2026-09-28-evening-bench-server-linux-x86.md) — full ladder, bench server, CGU16, plus consumers
- **[V]** [`2026-09-30-validation-cache.md`](../benches/results/2026-09-30-validation-cache.md) — read rungs after the env-wide validation cache, bench server, CGU1
- **[M]** [`2026-10-05-meta-annex-real-case.md`](../benches/results/2026-10-05-meta-annex-real-case.md) — YCSB on Graviton4 NVMe and Meilisearch, current main
- **[S]** [`2026-09-30-bounded-dirty-memory.md`](../benches/results/2026-09-30-bounded-dirty-memory.md) — YCSB under a 2 GB cap after spilling, bench server
- **[P]** [`2026-09-30-public-suite-nvme.md`](../benches/results/2026-09-30-public-suite-nvme.md) — the fjall-3 public suite, Graviton4 NVMe, indicative (2 reps)
- **[L]** the perf ledger line for the change
- **[G]** [`2026-10-13-multi-process-stage3-graviton.md`](../benches/results/2026-10-13-multi-process-stage3-graviton.md) — full ladder on Graviton4 (c8g, EBS io2, ext4, CGU1) with the lock file on (the default since ADR-0023) and off (`NO_LOCK`)
- **[C]** [`2026-10-15-meilisearch-v1.53.1-graviton-xlarge.md`](../benches/results/2026-10-15-meilisearch-v1.53.1-graviton-xlarge.md) — Meilisearch v1.53.1 and hannoy on Graviton4 `c8g.xlarge` (8 GiB, gp3), lock file on
- **[D]** [`2026-10-18-delete-rebalance-node-move-mac.md`](../benches/results/2026-10-18-delete-rebalance-node-move-mac.md) — `del/*` rungs and crud-bench before/after ADR-0028, Apple M-series laptop (busy host; indicative)
- **[G2]** [`2026-10-15-lock-file-in-place-graviton.md`](../benches/results/2026-10-15-lock-file-in-place-graviton.md) — `env/*` and a sample of other rungs on Graviton4 (c8g, EBS gp3, ext4, CGU1) after the lock file is rebuilt in place (ADR-0023 amendment 20)
- **[Q]** [`2026-10-31-delete-path-cursor.md`](../benches/results/2026-10-31-delete-path-cursor.md) — `^del/` three-column A/B on Graviton4 after the issue #137 delete-path levers, plus macOS profiles and retired-instruction counts
- **[X]** [`2026-10-19-crud-scan-count-and-batch-delete.md`](../benches/results/2026-10-19-crud-scan-count-and-batch-delete.md) — crud-bench `count()`, `start(5000) limit(100)` and `batch_delete_1000` (repro and crud-bench on an M-series laptop, indicative), plus the `^(scan|get|seek)/` gate on Graviton4 in both policies
- **[SC]** [`2026-10-31-stamp-cache-graviton.md`](../benches/results/2026-10-31-stamp-cache-graviton.md) — the validated-pages cache sized to the file: `ycsb_a_census` 10M keys and the `^(get/|scan/|seek/|env/)` ladder on Graviton4
- **[BP]** [`2026-10-31-build-profiles-and-scans-graviton.md`](../benches/results/2026-10-31-build-profiles-and-scans-graviton.md) — the default-tier ladder under four build profiles, and `^(scan|seek)/` validating vs trusted, Graviton4 `c8g.xlarge`, `main` at `0adf868` (before the validated-pages cache was sized to the file)
- **[W]** [`2026-10-31-writer-carry-graviton.md`](../benches/results/2026-10-31-writer-carry-graviton.md) — write txns reuse the previous one's containers (#8): `commit_census` under `perf stat` and the `^(commit/|env/txn/|put/|del/)` ladder, Graviton4
- **[WM]** [`2026-10-31-writemap-dirty-bits-graviton.md`](../benches/results/2026-10-31-writemap-dirty-bits-graviton.md) — bulk-write rungs with `WRITE_MAP` before/after the per-page dirty bits, Graviton4
- **[WC]** [`2026-11-01-commit-write-cap-graviton.md`](../benches/results/2026-11-01-commit-write-cap-graviton.md) — commit writes capped at 256 KiB per `pwritev` (#135): 10M-key single-put commits, the ladder, and Meilisearch v1.55.0, Graviton4

Rungs that [V] re-measured are quoted from [V]; the rest come from [E], which
predates the cache, so read paths in [E] are slightly pessimistic.

The rungs changed since these sources were run were re-measured only on an
Apple M-series laptop (perf ledger, 2026-10-17). Checking database handle
lifetimes (ADR-0024) made named-database gets 2–8 % slower and read-txn begin
6 % slower; packing branch pages on ascending inserts made gets for missing
keys 26 % faster. Both are described below.

The lock file (ADR-0023) is on by default since stage 3. [E], [V], [M], [S]
and [P] predate it and measure ZeroDB without one. [G] measures both: with
the lock file off every rung is flat against the release before it; with it
on, five rungs regress, and their rows below give both figures.

### Point reads

| What | Ratio | Source |
|---|---:|---|
| Hot key, every page cached (`get/access/hot`) | 0.82× | E |
| Random gets, named DB, 8-byte keys (`get/access/rand`, `get/db/named`, `get/key/k8`) | 0.94–0.95× | V |
| 1M-key tree (`get/size/n1m`) | 1.08× | V (1 round) |
| Wider keys (`get/key/k32`, `k128`) | 1.33×, 1.26× | E |
| Values in overflow pages (`get/val/v4k`, `v2page`) | 1.33×, 1.32× | E |
| … opt-in trusted mode, both engines trusting the file | 0.91×, 0.89× | L (2026-09-29) |
| One read txn + one get, 1M keys (`short_txn_census`) | 1,008 vs 649 ns (1.55×) | V |

### Scans and seeks

Graviton4, default (validating) policy and opt-in trusted mode, same binary
[BP]. Measured before the validated-pages cache was sized to the file, which
then made `scan/full/big` 13 % faster [SC].

| What | Default | Trusted | Source |
|---|---:|---:|---|
| Seeks (`seek/ge/rand`, `seek/ge/seq`) | 0.92×, 0.93× | 0.89×, 0.85× | BP |
| Seek into gaps (`seek/ge/gap`) | 1.06× | 1.01× | BP |
| Full scan (`scan/full/fwd`, `scan/full/rev`) | 1.47× | 1.09×, 1.05× | BP |
| Full scan over a 300 MB tree (`scan/full/big`) | 1.60× | 1.03× | BP |
| Range scans 1 % / 10 % (`scan/range/1pct`, `10pct`) | 0.85×, 0.82× | 0.70×, 0.69× | BP |
| Paged listing (`scan/skip_take/*`) | 0.47× | 0.24× | BP |
| Prefix bucket (`scan/prefix/bucket`) | 1.27× | 1.08× | BP |
| First/last (`scan/edge/first_last`) | 1.41× | 1.42× | BP |
| `len` of a DB (`scan/meta/len`) | 1.12× (7.5 vs 6.7 ns; moves ±40 % with code layout) | 1.15× | BP |

### Writes

| What | Ratio | Source |
|---|---:|---|
| Small puts, sequential (`put/val/v8`, `put/order/seq`, `put/api/plain`) | 0.83–0.87× | E |
| … opt-in sequential writes | 0.66–0.77× | E |
| Reserved puts, milli's document path (`put/api/reserved`) | 0.96× | E |
| Random-order puts (`put/order/rand`) | 0.93× | E |
| APPEND puts (`put/order/append`) | 1.21× | [report](../benches/results/2026-10-31-write-cursor-split-append-graviton.md) |
| Rewriting every value through the write cursor (`put/cursor/rewrite`) | 1.37× | [report](../benches/results/2026-10-31-write-cursor-split-append-graviton.md) |
| Bulk puts with `WRITE_MAP` (`put/order/seq`, `put/api/plain`, `commit/batch/n10k`) | 0.99–1.00× | WM |
| … other rungs with `WRITE_MAP` (`put/order/rand`, `put/api/reserved`, `put/val/v4k`) | 1.07×, 1.12×, 1.12× | WM |
| Overwrites that grow a value (`put/over/grow`) | 1.22× | E |
| Puts drawing pages from a large free-list entry (`put/gc/drain_big`) | 1.14× | E |
| 4 KiB values (`put/val/v4k`) | 0.99× | E |
| Mixed reads/writes over 8 DBs (`mixed/rw/8dbs`) | 0.92× | E |

### Deletes

| What | Ratio | Source |
|---|---:|---|
| Per-key deletes (`del/bulk/half`, `del/bulk/all`) | 1.35–1.36× | Q |
| Delete + re-insert churn (`del/churn/reinsert`) | 1.28× | Q |
| 1,000 random deletes per txn (`del/batch/k1000`) | 1.26× | Q |
| Draining through the write cursor (`del/cursor/drain`) | 1.66× | Q |
| Draining backward through the write cursor (`del/cursor/rev`) | 1.50× | Q |
| `delete_range` (`del/range/half`) | 0.52× | Q |
| One delete per txn, 48 threads (crud-bench, 16 KiB pages) | ~2.1× (~0.48× the throughput) | D |
| `clear` (`del/clear/all`) | 1.43× | Q |

### Commits

| What | Ratio | Source |
|---|---:|---|
| One-put no-sync commit (`commit/batch/n1`) | 1.17× on Graviton4 (an older bench-server run read 1.60×) | W |
| One-put no-sync commit, per commit (`commit_census`) | 5.17 vs 4.25 µs; 5.74k vs 2.64k user cycles | W |
| 100 / 10k puts per commit (`commit/batch/n100`, `n10k`) | 1.08×, 0.90× | E |
| Empty write txn (`env/txn/rw_empty_commit`) | 2.46× (E); Graviton4: 3.52× lock file on [W], 2.80× `NO_LOCK` [G] | E, G, W |
| Read txn begin/abort (`env/txn/ro_begin_abort`) | 0.56× (E); Graviton4: 0.73× lock file on (+6.8 ns per txn), 0.66× `NO_LOCK` | E, G |
| Writer with 0–4 concurrent readers (`concurrent/writer/*`) | 1.15–1.18× | E |
| Durable commits (`commit/sync/*`, SATA fsync-bound) | 0.97–1.03× (noise) | E |
| YCSB B durable, Graviton4 NVMe | 258k vs 238k ops/s (0.92×) | M |
| YCSB A no-sync, Graviton4 NVMe | 211k vs 256k ops/s (1.21×) | M |
| YCSB B no-sync, Graviton4 NVMe | 355k vs 490k ops/s (1.38×) | M |
| YCSB A / B no-sync, both engines `WRITE_MAP` | 408k vs 545k (1.34×) / 847k vs 1,199k (1.42×) | M |

### Large transactions and memory

| What | Result | Source |
|---|---|---|
| YCSB C: 10M-key unsorted load in one txn, 2 GB cap | completes; 539 MiB peak anonymous memory vs LMDB's 520 MiB (was killed by the cap) | S |
| YCSB A / B / C after that load, 2 GB cap, no sync | 1.32× / 1.45× / 1.16× (221k vs 292k, 434k vs 628k, 269k vs 311k ops/s) | S |
| File size after the unsorted 10M load | 2,322 vs 2,155 MiB (+8 %, page fill under random inserts) | S |
| Meilisearch hackernews (1M docs) replay, peak RSS | 4.02–4.27 vs 3.91–4.10 GB | E |

### Copies, stats and tools

| What | Ratio | Source |
|---|---:|---|
| Raw env copy (`maint/copy/raw`) | 0.94× | E |
| Compacting copy (`maint/copy/compact`) | 0.99× | E |
| `non_free_pages_size` (`env/stat/non_free`) | 0.41× (L); Graviton4: 0.48× lock file on (+7 ns per call), 0.44× `NO_LOCK` | L (2026-09-29, CGU1), G |
| Reopen an env (`env/open/reopen`) | 0.96× lock file on (36.4 µs per open, LMDB 37.8 µs; was 9.65× before the lock file was rebuilt in place); 0.49× `NO_LOCK` | G2, G |
| Create an env (`env/open/create`) | 10.50× lock file on (+155 µs per create); 9.04× `NO_LOCK`, by design (see [Deliberate differences](#deliberate-differences)) | G |

### Consumers end to end

| Workload | Ratio | Source |
|---|---:|---|
| Meilisearch indexing, movies | 0.99× (E), 1.01× (M), 1.02× (C) | E, M, C |
| Meilisearch indexing, hackernews 1M docs | 0.99× | E |
| Meilisearch hackernews incremental additions | 0.99× (M); 1.03× on Graviton4 with 8 GiB, merge spans 2.1× (C); 0.89× on a 16 GiB Mac, merge spans 0.71× (`benches/results/2026-10-16-hackernews-merge-step-mac.md`). v1.55.0 on Graviton4 8 GiB: 1.00× before the 256 KiB write cap, 0.92× after, merge spans 1.16× → 0.56× [WC] | M, C, WC |
| Meilisearch search, movies | 0.95× (E), 0.94× (M), 0.99× (C) | E, M, C |
| Meilisearch search, hackernews | 1.01× | E |
| hannoy build, 512 / 768 / 1536 dims | 0.84× / 0.84× / 0.89× (E); 0.94× / 0.95× / 0.97× (C) | E, C |
| hannoy search, 512 / 768 / 1536 dims | 0.95× (E); 0.94× / 0.94× / 0.83× (C) | E, C |
| Public suite, Graviton4: YCSB B fsync / 4 KB values / YCSB A no-sync | 1.00× / 0.90× / 2.01× | P |

The consumers run at parity. The gaps that remain are in micro-operations they
use little (cursor drains, tiny commits, empty txns) and in write-heavy no-sync
YCSB, where per-commit CPU is ZeroDB's weakest path.

## What closed each gap

Each row names the LMDB technique it copies, or says "beyond LMDB". Numbers
are bench server ratios, before → after, unless stated.

### Read path

| Technique | What changed | Before → after | Pointer |
|---|---|---|---|
| **Named-DB record re-resolved on every read** | Each read on a named DB took a lock, cloned the name and descended the catalog, roughly doubling the work per `get`. A read txn now resolves each DB once into a lock-free dbi-indexed table (LMDB's `txn->mt_dbs[dbi]`). | `scan/meta/len` 2.46× → 1.01× | 37eb96e, f82922a |
| **Eager page validation** | ZeroDB checks every cell of a map page before trusting it, so a corrupt file gives a typed error, never undefined behaviour. That walk now runs once per page per txn: a lock-free, kind-tagged memo makes later views cost two header reads, and pages the engine wrote itself are trusted by construction. | with the rest of the July 2026 batch: milli indexing 2.68× → 1.12× (laptop) | 37eb96e |
| **Env-wide validated-pages cache** | Read txns share one cache keyed by (page number, kind, txn id stamped in the header), so short txns stop re-validating hot pages; since 2026-10-01 each commit seeds it. | read txn + get 1,984 → 1,008 ns; random gets 1.16× → 0.94×; `get/size/n1m` 1.48× → 1.08× [V]. Sized to the file since 2026-10-31 (one slot per page): 10M-key random get p50 1.32–1.36 → 1.23–1.24 µs, LMDB 0.96–1.08, Graviton4 [SC] | ADR-0018 |
| **Trusted-file mode** (opt-in) | An `unsafe` open policy, `FileTrust::trust_contents()`, reads pages without the cell walk and skips the overflow header read, as LMDB does. Default stays validating. | trusted `get/val/v4k` 1.29× → 0.91× [L] | ADR-0014 |
| **`validate_page_size` on every page load** | The page size is checked once at env open. | — | 37eb96e |
| **Byte-copy field reads** | Page fields were read by bounds-checked byte indexing; now `read_unaligned` at explicit offsets, behind one per-view bounds proof (LMDB reads its structs in place). | — | 7848e30, #20 |
| **Allocation per get** | Cursors keep their path in a fixed 32-frame inline stack (LMDB's `CURSOR_STACK`); a descent resolves each page's bytes once per level, not twice. | — | #19 |
| **Hot paths above LLVM's inlining threshold** | Raising LLVM's threshold alone sped hot paths up 8–38 %. The nine helpers it then inlined are now `#[inline(always)]`; cold arms stay `#[inline(never)]` so the hot shape does not move. | CGU1: `get/access/hot` 1.37× → 1.11×, `scan/edge/first_last` 2.22× → 1.56×; 37 rungs faster, none slower | eb450ac |
| **Page views read only the flags** | Views decoded the whole header to test one field. | `del/bulk/half` 2.14× → 1.83×, `put/val/v8` 1.00× → 0.89× | 01dcc27 |
| **Cursor-free point get** | A `get` built a full cursor and compared the key twice; it now walks root to leaf and takes exactness from the leaf search (`mdb_node_search`). | `get/access/hot` 1.11× → 0.94× | 79f37ed |
| **Default key compare** | Equal-length 8- or 4-byte keys compare as one big-endian integer instead of a `memcmp` call; same order (beyond LMDB). Kept because Meilisearch's and hannoy's hot trees use 4- and 8-byte ids. | `get/access/hot` 1.00× → 0.80×; `get/key/k128` +4–5 % | ef61096 |
| **Cursor leaf memoization** | A cursor keeps its current leaf instead of reloading it per step. | scan 29× → 2.18× (July 2026, laptop) | — |
| **Scan step and trusted-leaf memo** | Under the trusting policy every leaf a scan visited was recorded in the read txn's validated-pages memo, which grew until each probe missed through every level; trusted leaves are no longer recorded (branches are). A step within a leaf reads the entry from the leaf view in hand, as `mdb_cursor_next` does. | `scan/full/fwd` 2.25× → 1.45× (validating), 2.05× → 1.02× (trusted), Graviton4 [X] | ADR-0014 amendment 2026-10-19 |
| **`NO_READ_AHEAD` honored** | The flag was a no-op, so each page fault read a whole readahead window; it now advises the map `MADV_RANDOM`, as LMDB does. | bigger-than-RAM YCSB: ~10 GB read in 60 s for a 1.5 GB DB → 7–11× less | 93b2740, [report](../benches/results/2026-09-29-ycsb-bigger-than-ram.md) |

### Write path and commits

| Technique | What changed | Before → after | Pointer |
|---|---|---|---|
| **`RwCursor` re-seek** | The write cursor tracked its position by key, re-descending and copying each pair three times per step. It now parks and resumes its path, with no allocation per step. A `del_current`, and a put of the current key, mutate at the parked path and keep it (SPEC 03 §5.4a/§5.4b); since #138 a put that splits keeps it too, repaired to the half holding the entry as `mdb_page_split` fixes up its cursor. Each step moves the parked path in place (it was copied in and out, 512 bytes each way) and yields the entry from the page it read; a same-size rewrite on a page the txn owns checks the key and writes in one page access with no copy-on-write pass, as LMDB's `MDB_CURRENT` put on a `P_DIRTY` page. **Residual:** any op but `next`/`prev` after a delete still re-seeks once; each entry still costs two pgno→frame lookups (step, put) where LMDB holds page pointers. | `put/cursor/rewrite` 5.47× → 2.49× (macOS arm64, 2026-10-19), then 2.50× → 1.37× and `del/cursor/drain` 2.62× → 2.15× (Graviton4, 2026-10-31) [[report](../benches/results/2026-10-31-write-cursor-split-append-graviton.md)] | #7, #138 |
| **Splits without materializing the page** | Appends split onto a fresh right page; other splits pack both halves from cells borrowed from the old page. | — | 3c6a064 |
| **One `pwrite` per dirty page** | Contiguous dirty pages go out in `pwritev` batches, as `mdb_page_flush` does. Its target, EBS gp3, is still unmeasured. | — | 3c6a064 |
| **2 MiB commit writes left large page-cache folios** | Batches were up to 512 pages per call. On kernels that cache files in large folios, a bulk commit then left the file in 2 MiB folios and each later one-page rewrite cost far more kernel time. A call now carries at most 256 KiB, LMDB's 64 pages (`MDB_COMMIT_PAGES`). **Cost:** bulk fresh large values, `put/val/v4k` 0.99× → 1.10× (ladder before the October 31 merges). | 10M-key single-put no-sync commit p50 12.1–12.6 → 8.7–8.8 µs (LMDB 7.6–7.9); `del/random/n1` 1.58× → 1.20×; Meilisearch hackernews additions 1.00× → 0.92× (Graviton4, [WC]) | #135 |
| **Adapter `ReservedSpace`** | `put_reserved` (milli's document path) filled a zeroed heap buffer, then copied it; the caller now writes straight into the page slot, as with `MDB_RESERVE`. The cursor variant still stages; it is to be built in place, as LMDB does (#10, low priority). | `put/api/reserved` 0.96× today [E] | — |
| **Writer slot without a syscall** | Releasing a write txn always issued a `futex_wake`; it now wakes only when a writer waits, like LMDB's pthread writer mutex. | `env/txn/rw_empty_commit` 5.93× → 2.19× | 7c02a5c |
| **dbi-indexed write-txn table** | The write txn's open-DB table was a SipHash map. | `put/val/v8` 1.28× → 1.09× | 4fc1c9e |
| **Reused descent path, in-place APPEND check** | One path buffer per write txn instead of a `Vec` per op; APPEND compares the last key in the page instead of copying it. | `put/val/v8` 1.08× → 1.00×, `put/order/append` 1.32× → 1.19× | 2142ab2, 2ee1d25 |
| **APPEND without a second leaf view or a no-op COW pass** | The append slot is one past the last cell the rightmost descent read (LMDB bumps `mc_ki` after `mdb_cursor_last`); when every page on that descent is already the txn's own frame, the copy-on-write pass is skipped, as `mdb_page_touch` skips `P_DIRTY` pages. **Residual (Mac profile):** zeroing each new split page (SPEC 04 TXN-47; LMDB reuses `me_dpages` without zeroing) ~7 %, an O(1) header re-check per page view, and two pgno→frame lookups per level (view, touch) on keyed puts. | `put/order/append` 1.26× → 1.21× (Graviton4, 2026-10-31) | #138 |
| **Dirty frame pool** | Page buffers are reused within and across write txns, capped at 256 (LMDB's `me_dpages`). The empty-txn cost was accepted by the maintainer. | `put/gc/drain_big` 1.58× → 1.17×, `commit/batch/n1` 2.05× → 1.93×; `rw_empty_commit` 2.19× → 2.41× | 10c98a1 |
| **Write-txn containers carried across txns** | Each write txn built its dirty-page and freed-page indexes, page lists, descent path and open-table slots empty and freed them at the end. One carry on the env's writer slot now holds them and each txn clears them in place, as LMDB reuses `me_txn0`; capacity caps keep one large txn from slowing the small ones after it. The begin path also reuses the cached snapshot and drains the meta page's freed-page list without copying it. | single-put commit 19 → 5 heap allocations, 6.68k → 5.74k user cycles (LMDB 2.64k); `commit/batch/n1` 1.26× → 1.17× (Graviton4, [W]) | #8, PR #144 |
| **O(1) free-list front draw** | Each page drawn from a free-list entry memmoved the rest; a consumed-prefix offset makes it O(1), file byte-identical. | `put/gc/drain_big` 2.21× → 1.68× | 37b03c9 |
| **Free-page runs across records** | A multi-page value searched one freed-page record at a time, so a run split across two records was missed and the file grew. When no single record fits, the allocator now merges the records the reuse gate allows, oldest first and bounded as LMDB bounds its free list, and keeps the merged list for the txn. | file size under multi-page rewrites 2.43× → 1.10× LMDB's (`free_run_churn`, macOS, 400 commits) | ADR-0029, PR #121 |
| **Spilling** | Past LMDB's dirty limit (131,072 pages; `max_dirty_bytes`), a write txn writes its highest-numbered dirty pages to the file and drops them, as `mdb_page_spill` does. | YCSB C loads instead of being killed; YCSB A 0.58 → 0.76, B 0.31 → 0.69 of LMDB's throughput [S]; ladder flat | ADR-0017 |
| **In-place `WRITE_MAP`** (opt-in) | Under `WRITE_MAP`, dirty pages live in the writable map at their final offset: no heap copy, no write-back, as `MDB_WRITEMAP`. | spike, macOS arm64, single-put no-sync commits: 29.8–43.5 → 8.6–11.1 µs | ADR-0021, PR #88 |
| **`WRITE_MAP` dirty-page test from per-page bits** (opt-in mode) | Every level of a descent asks whether its page is already dirty; on a large in-map write txn that hash probe missed the cache (15–17 % of a bulk insert). Two bits per page now answer it once the txn holds 64 frames. LMDB skips the question under `MDB_WRITEMAP`; ZeroDB still needs the answer because map pages are validated and dirty frames are not. | with `WRITE_MAP`: `put/order/seq` 1.12× → 0.99×, `commit/batch/n10k` 1.11× → 1.00×, `put/api/plain` 1.12× → 0.99× (Graviton4, [WM]) | #13, PR #145 |
| **Meta free-list annex** | A commit's freed-page list goes into its own meta page when it fits, instead of a free-list tree put per commit (beyond LMDB). On-disk format 2. | `commit/batch/n1` 2.04× → 1.60×; durable YCSB B on Graviton4 239k → 258k ops/s; Meilisearch flat [M] | ADR-0022, PR #89 |
| **Branch splits on ascending inserts** | A full branch page took a middle split even on an end insert, so ascending loads left every branch page half full. It now splits at the end, as LMDB does. SPEC 03. | 4 KiB pages, 20k ascending keys: 949 → 480 branch pages (LMDB 558); writes flat; `get/access/miss` −26 %, `get/access/hot` +5.5 % (laptop) [L] | #111 |
| **Leaf end-insert split** | A plain put at the end of a full leaf now moves the old last entry to the new page with it, as LMDB does, so ascending loads build LMDB's leaf count. The cell is removed in place from the top of the left page's cell heap, so nothing else moves. | measured rungs flat, `get/access/hot` −4.5 % (laptop) [L] | #113 |
| **Sequential-writes option** (opt-in) | A per-tree rightmost-leaf finger lets ascending puts skip the descent (PostgreSQL's nbtree fast path; LMDB has none). Off by default: random puts are 3–4 % slower. | `put/val/v8` 0.83× → 0.66× [E] | ADR-0015 |

### Deletes

| Technique | What changed | Before → after | Pointer |
|---|---|---|---|
| **Cursor delete keeps its position (the `del_current` re-descent)** | `del_current` re-descended to delete, then again for the next step, so a cursor drain cost 28 % more than deleting by key. It now deletes at the path it holds and settles on the successor (LMDB's `C_DEL`), and keeps the path across leftmost-pairing rebalances (`mdb_cursor_del0`). SPEC 03 §5.4a; pinned by `tests/cursor_delete_position.rs`. | `del/cursor/drain` 3.38× → 2.46× (laptop), then 2.91× → 2.63× | c4ab096, 17fdb24 |
| **Cursor delete steps in place; left-sibling repair; `clear` free list** | After a delete, `next`/`prev` inside the leaf read the slot from the kept path (LMDB's `mdb_cursor_next`/`prev` after `C_DEL`) instead of rebuilding a cursor, and no key is allocated per delete. A borrow from or merge with the left sibling keeps the path (`mdb_node_move`/`mdb_page_merge` fix-ups). Removing a cell fixes the pointers in one pass (`mdb_node_del`). `clear` frees base-snapshot pages with a list append, reserves per branch (`mdb_midl_need`) and checks duplicates with a bitmap. SPEC 03 §5.4a. | `del/cursor/drain` 2.41× → 1.66×, `del/cursor/rev` 3.06× → 1.50×, `del/clear/all` 1.73× → 1.43×, `del/bulk/half` 1.41× → 1.35× (Graviton4, [Q]) | #137 |
| **`delete_range` as one leaf walk** | Collected every key, then point-deleted each; now splices covered cells leaf by leaf and rebalances each leaf once (beyond LMDB, which has no range delete). | `del/range/half` 2.63× → 0.92× | 3512269 |
| **Rebalance moves cells page to page** | A borrow decoded the moved entry into owned key/value buffers, re-encoded it, and rewrote the parent separator as remove + re-insert; a merge did the same per entry. Cells now move verbatim through a per-txn scratch buffer and separators are rewritten in place, as LMDB's node move and key update do. Tree shape unchanged. ADR-0028, SPEC 03 §10.1. | `del/bulk/half` 2.46× → 1.30×, `del/cursor/drain` 2.59× → 1.50×, `del/range/half` 0.67× → 0.24× (laptop, [D]) | ADR-0028 |
| **`clear` frees leaves unread** | Without overflow pages, leaf page numbers are freed from their parents without loading the leaves (LMDB's `mdb_drop0`). | `del/clear/all` 12.73× → 1.78× | c60a31e |

### Copies, stats and tools

| Technique | What changed | Before → after | Pointer |
|---|---|---|---|
| **The `non_free_pages_size` item** | Meilisearch calls it before every register write txn. It decoded the whole free list; it now sums per-DB page counts from the catalog, as heed does over LMDB, and is correct under `WRITE_MAP`. | 317 µs → 148 ns (0.41×) | f98374a, c82e5c8 |
| **Compaction memory (compaction RAM, bounded-memory compaction)** | Compaction, the compacting copy and `zerodb-tools load` held about twice the env size in RAM. The rebuild now streams: peak memory is O(tree depth × page size) plus the write buffers, and the destination appears by atomic rename. `load` also streams its input (#63). | — | ADR-0009 |
| **Env copies write each byte once** | Copies were staged then copied again, the raw copy built the image in RAM, the compacting copy wrote one page per syscall. Now raw writes straight from the map and compact goes through 1 MiB buffers handed to a writer thread (`mdb_env_copyfd1`/`2`). | `maint/copy/raw` 2.53× → 0.93×, `maint/copy/compact` 3.31× → 0.98× | 2f479d2, 7dc3d1c |

## Tried and not adopted

| Idea | Why not | Pointer |
|---|---|---|
| Lazy validation: check only the cells a lookup touches | 1M-key tree −23 %, but sequential and missing-key gets +11–23 %; the env-wide cache later removed most of the repeat cost it targeted | ADR-0016 |
| Durable meta write through an `O_DSYNC` descriptor | 2 → 1 `fdatasync` per commit, but no measured gain on the reachable io2 host | ADR-0019, PR #86 |
| Used-portion COW copy (skip a page's free gap) | `commit/batch/n1` 1.95× → 1.93×, all rungs flat; the frame pool had already removed the cost, and stale bytes would reach the file | 2026-10-02, branch never merged |
| `#[cold]` arms plus `#[inline]` hints | flat at CGU16, `put/val/v8` +8.8 % at CGU1; LLVM still declined | ledger, 994a05c |
| Dirty-store arena or sorted map | the dirty-store probe never cleared the profile's noise bar; the frame pool took the useful part | ledger 2026-07-22; issues #4 and #5 closed |
| Cached parent branch in scans | 2–4 % on `scan/*`, under the noise bar | ledger, b7fa519 |
| Fused cell-removal loop (LMDB's loop shape) | slower, 23.8 vs 21.3 ms: `copy_within` beats per-element indexing | ledger 2026-09-10 |
| Inline path stack on the write path | zero-filling a 520-byte struct per op cost more: `put/val/v8` +8 % | ledger, 9eb19d4 |
| Exact one-page slice on every page load | `get/access/hot` +20 % at CGU1 (code layout) | ledger, a55ae35 |
| GC last-hit fast path | flat (0.977) after the O(1) front draw | ledger, 02277b7 |
| Branch-free leaf cell validation | `scan/full/fwd` +5 %: the early-exit loop is perfectly predicted on valid pages | ledger, 8490125 |
| Direct cursor step | flat; the redundant checks were already optimized away | ledger, 2162d26 |
| One-pass overflow resolver | flat; the cost is the cache miss on the overflow header, which only trusted mode skips | ledger, 2b18e13 |
| Writers publishing into the validation cache; hashed cache slots | `put/val/v8` +12 %; `scan/full/*` +5 % | ADR-0018 §Implementation |
| Spilled-page lookup inside the page resolver | read rungs +3–13 %; spilled pages now resolve through the ordinary map path | [S] |
| Branchless page search (`select_unpredictable` in leaf and branch search, #16) | gets 8–25 % slower on Apple M-series (`get/key/k128` +25 %); retry only with a Graviton A/B | ledger, 17acfb4 |
| Integer compare for any keys of at most 8 bytes, not only equal-length ones (#15) | gets and seeks flat, `scan/prefix/bucket` +18 %; equal-length 4- and 8-byte keys already take the fast path | ledger, e1ee099 |
| Leaf end-insert split by repacking both pages | `put/order/seq` +10.5 %; replaced by moving the one cell in place (#113) | ledger, 43a9e45 |
| Rightmost-leaf finger on by default | random and mixed puts +3–5 %; shipped as the opt-in sequential-writes option | ledger, c7c8b24; ADR-0015 |

## What remains

Measured gaps first, then ideas without a measurement yet. Issues are on
[GitHub](https://github.com/qdequele/ZeroDB/issues).

**Measured**

- **Single-page writes after a bulk commit** (#135) — commit writes are now
  capped at 256 KiB per `pwritev` call, as LMDB's 64 pages: on the 10M-key
  no-sync workload, write p50 went from 12.1–12.6 µs to 8.7–8.8 µs, against
  LMDB's 7.6–7.9 µs [WC]. A 16 KiB cap reached 7.5 µs but made bulk
  large-value writes 1.6× slower. Owed: the public `a_nosync` suite (0.60×
  LMDB's throughput before) and the hannoy benches after the cap, the ladder
  on current `main`, and XFS.
- **Per-commit CPU** (#146) — `commit/batch/n1` 1.17×, empty write txn
  3.52× on Graviton4 [W]; a single-put no-sync commit costs 5.74k user
  cycles against LMDB's 2.64k. What is left is CPU per commit, not I/O.
  Write txns already reuse the previous one's containers (#8). Still open:
  the writer slot's in-process mutex costs two read-modify-write atomics per
  acquire and release on top of the lock file's word (about 8 per empty
  write txn, LMDB 2; removing it needs an owned lock cell, an ADR-level
  change); re-encoding the meta page's free-list annex
  (`MetaPage::encode_with_annex`, ADR-0022), ~78 % of commit in crud-bench's
  one-delete-per-txn phase, which keeps the annex full; the free-list tree
  record that small txns decode and rewrite after a large delete (LMDB
  keeps its reclaimed list, `me_pghead`, across txns); the snapshot `Arc`
  published per commit; the `pwritev` iovec list.
- **Scans** (#147) — by default full scans are 1.47–1.60× and prefix
  buckets 1.27×; in trusted mode 1.03–1.09× [BP], so the default-mode gap is
  page validation (16–19 % of a scan, including out-of-line atomics in
  `ValidatedPages::insert`), and the trusted residual is two calls per entry
  against LMDB's one. `scan/edge/first_last` is 1.41× in both modes: every
  call re-resolves the tree from the root. Range scans, paged listings and
  point seeks (`seek/ge/rand`, `seq`) are faster than LMDB; seeks into gaps
  (`seek/ge/gap`) trail at 1.06× by default and 1.01× trusted. Cache-line prefetch (#17) and branchless page
  search (#16) were closed: the latter slowed gets down.
- **Point gets and the build profile** (#147) — LTO makes point gets 3–10 %
  faster with LMDB flat, from small `zerodb-core` accessors the heed adapter
  calls per operation; the candidate `#[inline]` sites are listed in [BP].
  One codegen unit (Meilisearch's build) gets almost none of it, and makes
  `put/cursor/rewrite` 19 % and `del/cursor/drain` 9 % slower, cause not
  found. Otherwise the profile moves both engines alike: ZeroDB ÷ LMDB stays
  at 1.12–1.13 across all four profiles.
- **Write cursor and APPEND** (#148) — `put/cursor/rewrite` 1.37×,
  `put/order/append` 1.21×. Each cursor rewrite still costs two pgno→frame
  lookups where LMDB's cursor holds page pointers; APPEND zero-fills each
  new split page (SPEC 04 TXN-47), which LMDB does not. #141 left two small
  unexplained regressions (`put/gc/ovf_across` +4.2 %, `del/bulk/all`
  +3.1 %) whose Graviton re-run is owed.
- **Deletes** — per-key deletes 1.35×, cursor drains 1.50–1.66×, `clear`
  1.43× on Graviton4 [Q]. The memmoves of a delete and its borrow now match
  LMDB's byte for byte; what remains is bookkeeping around them (page
  lookups by pgno where LMDB's cursor holds page pointers, O(1) view checks,
  the descent; #148). About 30 µs of `clear`'s gap is the commit's fixed cost
  (#146). On macOS's 16 KiB pages the ladder's delete fixture is one level
  deeper than LMDB's (10-byte branch cells, ADR-0002), so Mac ratios read
  higher. Workloads with one delete per transaction are bound by commit
  instead (#146).
- **Overflow values** — 1.3× by default, because the overflow header is read
  and checked; with both engines trusting the file it is 0.89–0.91×.
- **Wider keys** — `get/key/k32`/`k128` 1.26–1.33× [E]. Extending the integer
  compare to all keys of at most 8 bytes (#15) was tried and not adopted.
- **`WRITE_MAP`** (#13) — bulk puts are at parity since the per-page dirty
  bits [WM]. Still above LMDB with `WRITE_MAP`: `put/order/append` 1.38×,
  `put/api/reserved` 1.12×, `put/order/rand` 1.07×, and `put/val/v4k` 1.12×,
  where in-map overflow runs are zero-filled before the value is copied in,
  which LMDB does not do. No-sync YCSB was last measured at 1.34–1.42× LMDB's
  own `WRITE_MAP` [M], before ranged `msync` and the dirty bits; a re-run is
  owed. Durable `WRITE_MAP` commits on macOS are 1.87–1.91× LMDB [L] because
  ZeroDB issues a second `fdatasync` after the meta write, which LMDB does
  not. Open: prefaulting fresh pages, and whether Meilisearch should set
  `WRITE_MAP`. Preallocating the file was tried and dropped (#42): LMDB does
  not preallocate.
- **Short read txns** — one read txn + one get is 1.55× [V]. The memo level
  and the named-DB table are now reused across txns on a thread (#25,
  `env/txn/ro_get_one` −26 % on macOS); a Linux re-measure is pending.
- **Page fill under random inserts** — the unsorted 10M load makes an 8 %
  larger file than LMDB's [S]. No issue yet.
- **Free-page runs** — since ADR-0029 the residual is small: under steady
  multi-page rewrites ZeroDB's file still grows ~4 % between 200 and 400
  commits, LMDB's 0.6 %. Coalescing leftovers on disk (ADR-0029 option B)
  would close it; not planned.
- **`zerodb-tools load` memory** — streams the dump since #63: peak memory
  footprint ~156 MiB for a 0.7 GB or a 1.4 GB dump (was 1.2 / 2.4 GB), against
  ~1 MiB for LMDB's `mdb_load`, which commits every 100 records where `load`
  commits every 64 MiB (a 4 MiB batch measured 20 MiB but +50 % wall time).

**Not yet measured**

- The cursor reserved put (#10): no consumer profile names it.
- Durable commits on EBS gp3, where per-syscall latency should favour the
  coalesced writes: still owed. On SATA and Graviton4 NVMe durable commits
  are at parity or better.
- A 24-byte page header instead of 32 (ADR-0020): a spike ran on an
  unmerged branch, but the measurement that decides it is not recorded; an
  on-disk format change that needs approval.
- Consumer-facing APIs beyond LMDB: group commit (#36), relaxed durability
  (#35), prefetch hints (#57), map warm-up (#61), automatic map growth (#33),
  balanced ranges for parallel scans (#55), reader parking (#32). Batched
  lookups (#53) and single-descent get-merge-put (#52) were closed as not
  planned. Reflink copies (#64) and a parallel `check` (#65; a 2.1 GB env
  checks in 0.17 s instead of 0.42 s on an 8-core Mac, warm cache) are done;
  LMDB has neither.

## Deliberate differences

- **Meta CRC32C** (ADR-0002) — one checksum per commit and meta read; crash
  recovery depends on it to detect torn writes. LMDB has none. It covers the
  meta free-list annex (up to a page of ids). On aarch64 it uses the ARMv8
  `crc32c` instructions (ADR-0031): about 2 µs at a full 16 KiB page on the
  Mac, 0.75 µs on Graviton4; elsewhere the slicing-by-8 table, about 9 µs
  (`benches/results/2026-10-19-meta-crc-hardware.md`).
- **Page validation by default** — a corrupt page gives a typed error, never
  undefined behaviour. Removing it is opt-in only, through trusted-file mode
  (ADR-0014).
- **Env creation fsyncs** — creating an env syncs the file and its parent
  directory, so a crash cannot lose the name of a durable file (issue #46).
  Paid once per env lifetime; it is why `env/open/create` is 8.55× [E]
  (9.04× on Graviton4 without a lock file [G]).
- **Lock file on by default** (ADR-0023) — multi-process access costs, on
  Graviton4 [G], +29.8 ns per write commit, +6.8 ns per read txn begin/abort
  and +7 ns per `stat`. `NO_LOCK` (`lock_file(false)` natively) is flat.
  Env open first cost +155 µs per create and 17 → 347 µs per reopen, because
  the first opener truncated the lock file and rebuilt it. Since ADR-0023
  amendment 20 it rebuilds the file in place, as LMDB does, and reopen is at
  parity with LMDB: 36.4 µs per open against LMDB's 37.8 µs (issue #94,
  [G2]).

## Deliberate — keep, do not optimize

During the broader performance investigation, several well-known
storage-engine techniques were evaluated for ZeroDB's design and explicitly
rejected. Each entry states the technique and the specific reason it does not
fit ZeroDB, so a future contributor does not re-propose one of these and
re-derive the same conclusion from scratch.

- **`O_DIRECT` commit writes** — a Linux flag that makes writes bypass the OS
  page cache. Incompatible with ZeroDB's read path, which maps the file with
  `MAP_SHARED` and reads straight out of the page cache: Linux does not
  guarantee invalidating already-mapped pages when a file is written with
  `O_DIRECT`, so readers could see stale data. The "double buffering" problem
  `O_DIRECT` normally avoids — one copy in the application's write buffer, one
  in the page cache — doesn't exist here, because the mmap'd pages are the
  same page-cache pages the write call fills.
- **Bε-trees / buffered writes** — buffering writes in internal nodes and
  flushing them down in batches (as in BetrFS/TokuDB) to turn random writes
  into sequential ones. This would force every read to merge in pending
  buffered writes, destroying the zero-copy reads mmap currently gives and
  adding read latency. `milli`'s workload is read-bound, so this trade is
  wrong for ZeroDB's main consumer.
- **Per-page compression** — the two dominant value types ZeroDB stores don't
  compress well: Roaring-bitmap-encoded document ID sets and float32 vectors
  for `hannoy`'s HNSW index. It would also require a buffer pool (a scratch
  area to decompress into) that ZeroDB doesn't have yet.
- **A hash-table sub-database type** — an alternative to the B+tree, for O(1)
  lookup. The actual cost of walking down the tree has already been sharply
  reduced by other read-path fixes landed in this repo (per-transaction
  caching of resolved records and validated pages). A hash table would make
  the real remaining costs — page faults and memory locality — worse, and
  would lose ordered iteration, which both `milli` and `hannoy` rely on for
  range scans.
- **Bloom filter sidecars** — a probabilistic structure some engines use to
  skip lookups for keys that don't exist. `milli` already gates word
  existence with its own FSTs (finite-state transducers used for fast
  prefix/existence lookups) before ever touching the store, so a bloom filter
  would be redundant — and keeping one transactionally consistent under MVCC
  costs more than it saves.
- **Pointer swizzling** — replacing on-disk page IDs with in-memory pointers
  once a page is loaded, used by buffer-pool engines like LeanStore's
  `vmcache`. ZeroDB's mapped pages are immutable, shared, and already have
  stable addresses via mmap — exactly the property swizzling exists to
  provide, and ZeroDB gets it for free from the OS.
- **`MDB_FIXEDMAP`** — an LMDB flag that maps the database at a fixed virtual
  address instead of letting the OS choose. Experimental and unstable
  upstream, fights address-space-layout randomization, doesn't handle file
  resizes cleanly, and no consumer needs it.

## Former item codes

Older ADRs, specs, bench reports and the perf ledger cite this page by item
code. Each maps to the entry above.

| Code | Entry | Code | Entry |
|---|---|---|---|
| A1 | Named-DB record re-resolved on every read | B9 | Env creation fsyncs |
| A2 | Eager page validation; (b) lazy validation, not adopted; (c) env-wide validated-pages cache | B10, B24 | Env copies write each byte once |
| A3 | Byte-copy field reads | B11 | Writer slot without a syscall |
| A4 | `validate_page_size` on every page load | B12 | Per-commit CPU (single-put commit census) |
| A5 | Scans: branch levels re-resolved per cursor step | B13 | Hot paths above LLVM's inlining threshold |
| A6 | Allocation per get | B14 | dbi-indexed write-txn table |
| A7 | Custom comparators (#14, closed: neither milli nor hannoy registers one, and the default order has its own inlined path) | B15 | Reused descent path and in-place APPEND check |
| A8 | Eager page validation (lock-free memo) | B16 | Page views read only the flags |
| B1 | `RwCursor` re-seek | B17 | Cursor-free point get |
| B2 | Splits without materializing the page | B18 | Dirty frame pool |
| B3 | Dirty-store arena or sorted map (not adopted) | B19 | Default key compare |
| B4 | One `pwrite` per dirty page | B20 | The `non_free_pages_size` item |
| B5 | In-place `WRITE_MAP` | B21 | `clear` frees leaves unread |
| B6 | Adapter `ReservedSpace` | B22, B8a | Cursor delete keeps its position |
| B7 | O(1) free-list front draw; meta free-list annex; free-list bookkeeping (#29) | B23 | `delete_range` as one leaf walk |
| B8 | Delete rebalance | C1, C3 | Compaction RAM |
| C2 | Spilling | D | Deliberate differences |

Sections A, B and C were the read path, the write path and peak memory.
