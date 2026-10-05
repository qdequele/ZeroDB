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
- **Consumers end to end** — Meilisearch v1.53.1 (`cargo xtask bench`
  workloads, through `scripts/consumer.sh`) and hannoy v0.1.7 (its divan
  benches), each built once against LMDB and once against ZeroDB.

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

Rungs that [V] re-measured are quoted from [V]; the rest come from [E], which
predates the cache, so read paths in [E] are slightly pessimistic.

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

| What | Ratio | Source |
|---|---:|---|
| Seeks (`seek/ge/rand`, `seek/ge/seq`) | 0.95× | V |
| Seek into gaps (`seek/ge/gap`) | 1.32× | E |
| Full scan (`scan/full/fwd`) | 1.49× | V |
| Range scan 1 % (`scan/range/1pct`) | 1.33× | V |
| Prefix bucket (`scan/prefix/bucket`) | 1.76× | V |
| First/last (`scan/edge/first_last`) | 1.66× | E |
| `len` of a DB (`scan/meta/len`) | 1.00× | E |
| Range scans, opt-in trusted mode | 1.19–1.21× | E |

### Writes

| What | Ratio | Source |
|---|---:|---|
| Small puts, sequential (`put/val/v8`, `put/order/seq`, `put/api/plain`) | 0.83–0.87× | E |
| … opt-in sequential writes | 0.66–0.77× | E |
| Reserved puts, milli's document path (`put/api/reserved`) | 0.96× | E |
| Random-order puts (`put/order/rand`) | 0.93× | E |
| APPEND puts (`put/order/append`) | 1.15× | E |
| Overwrites that grow a value (`put/over/grow`) | 1.22× | E |
| Puts drawing pages from a large free-list entry (`put/gc/drain_big`) | 1.14× | E |
| 4 KiB values (`put/val/v4k`) | 0.99× | E |
| Mixed reads/writes over 8 DBs (`mixed/rw/8dbs`) | 0.92× | E |

### Deletes

| What | Ratio | Source |
|---|---:|---|
| Per-key deletes (`del/bulk/half`, `del/bulk/all`) | 1.75× | E |
| Delete + re-insert churn (`del/churn/reinsert`) | 1.40× | E |
| Draining through the write cursor (`del/cursor/drain`) | 2.72× | E |
| `delete_range` (`del/range/half`) | 0.95× | E |
| `clear` (`del/clear/all`) | 1.84× | E |

### Commits

| What | Ratio | Source |
|---|---:|---|
| One-put no-sync commit (`commit/batch/n1`) | 1.60× | ADR-0022 (CGU1, 5 rounds) |
| 100 / 10k puts per commit (`commit/batch/n100`, `n10k`) | 1.08×, 0.90× | E |
| Empty write txn (`env/txn/rw_empty_commit`) | 2.46× | E |
| Read txn begin/abort (`env/txn/ro_begin_abort`) | 0.56× | E |
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
| `non_free_pages_size` (`env/stat/non_free`) | 0.41× | L (2026-09-29, CGU1) |
| Reopen an env (`env/open/reopen`) | 0.34× | E |
| Create an env (`env/open/create`) | 8.55×, by design (see [Deliberate differences](#deliberate-differences)) | E |

### Consumers end to end

| Workload | Ratio | Source |
|---|---:|---|
| Meilisearch indexing, movies | 0.99× (E), 1.01× (M) | E, M |
| Meilisearch indexing, hackernews 1M docs | 0.99× | E |
| Meilisearch hackernews incremental additions | 0.99× | M |
| Meilisearch search, movies | 0.95× (E), 0.94× (M) | E, M |
| Meilisearch search, hackernews | 1.01× | E |
| hannoy build, 512 / 768 / 1536 dims | 0.84× / 0.84× / 0.89× | E |
| hannoy search, 512 / 768 / 1536 dims | 0.95× | E |
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
| **Env-wide validated-pages cache** | Read txns share one cache keyed by (page number, kind, txn id stamped in the header), so short txns stop re-validating hot pages; since 2026-10-01 each commit seeds it. | read txn + get 1,984 → 1,008 ns; random gets 1.16× → 0.94×; `get/size/n1m` 1.48× → 1.08× [V] | ADR-0018 |
| **Trusted-file mode** (opt-in) | An `unsafe` open policy, `FileTrust::trust_contents()`, reads pages without the cell walk and skips the overflow header read, as LMDB does. Default stays validating. | trusted `get/val/v4k` 1.29× → 0.91× [L] | ADR-0014 |
| **`validate_page_size` on every page load** | The page size is checked once at env open. | — | 37eb96e |
| **Byte-copy field reads** | Page fields were read by bounds-checked byte indexing; now `read_unaligned` at explicit offsets, behind one per-view bounds proof (LMDB reads its structs in place). | — | 7848e30, #20 |
| **Allocation per get** | Cursors keep their path in a fixed 32-frame inline stack (LMDB's `CURSOR_STACK`); a descent resolves each page's bytes once per level, not twice. | — | #19 |
| **Hot paths above LLVM's inlining threshold** | Raising LLVM's threshold alone sped hot paths up 8–38 %. The nine helpers it then inlined are now `#[inline(always)]`; cold arms stay `#[inline(never)]` so the hot shape does not move. | CGU1: `get/access/hot` 1.37× → 1.11×, `scan/edge/first_last` 2.22× → 1.56×; 37 rungs faster, none slower | eb450ac |
| **Page views read only the flags** | Views decoded the whole header to test one field. | `del/bulk/half` 2.14× → 1.83×, `put/val/v8` 1.00× → 0.89× | 01dcc27 |
| **Cursor-free point get** | A `get` built a full cursor and compared the key twice; it now walks root to leaf and takes exactness from the leaf search (`mdb_node_search`). | `get/access/hot` 1.11× → 0.94× | 79f37ed |
| **Default key compare** | Equal-length 8- or 4-byte keys compare as one big-endian integer instead of a `memcmp` call; same order (beyond LMDB). Kept because Meilisearch's and hannoy's hot trees use 4- and 8-byte ids. | `get/access/hot` 1.00× → 0.80×; `get/key/k128` +4–5 % | ef61096 |
| **Cursor leaf memoization** | A cursor keeps its current leaf instead of reloading it per step. | scan 29× → 2.18× (July 2026, laptop) | — |
| **`NO_READ_AHEAD` honored** | The flag was a no-op, so each page fault read a whole readahead window; it now advises the map `MADV_RANDOM`, as LMDB does. | bigger-than-RAM YCSB: ~10 GB read in 60 s for a 1.5 GB DB → 7–11× less | 93b2740, [report](../benches/results/2026-09-29-ycsb-bigger-than-ram.md) |

### Write path and commits

| Technique | What changed | Before → after | Pointer |
|---|---|---|---|
| **`RwCursor` re-seek** | The write cursor tracked its position by key, re-descending and copying each pair three times per step. It now parks and resumes its path, with no allocation per step. **Residual:** `put_current` and cursor `put` still re-seek once per mutation, where LMDB fixes the cursor up in place. | — | #7 |
| **Splits without materializing the page** | Appends split onto a fresh right page; other splits pack both halves from cells borrowed from the old page. | — | 3c6a064 |
| **One `pwrite` per dirty page** | Contiguous dirty pages go out in `pwritev` batches, as `mdb_page_flush` does. Its target, EBS gp3, is still unmeasured. | — | 3c6a064 |
| **Adapter `ReservedSpace`** | `put_reserved` (milli's document path) filled a zeroed heap buffer, then copied it; the caller now writes straight into the page slot, as with `MDB_RESERVE`. The cursor variant still stages (#10, divergence D-015). | `put/api/reserved` 0.96× today [E] | — |
| **Writer slot without a syscall** | Releasing a write txn always issued a `futex_wake`; it now wakes only when a writer waits, like LMDB's pthread writer mutex. | `env/txn/rw_empty_commit` 5.93× → 2.19× | 7c02a5c |
| **dbi-indexed write-txn table** | The write txn's open-DB table was a SipHash map. | `put/val/v8` 1.28× → 1.09× | 4fc1c9e |
| **Reused descent path, in-place APPEND check** | One path buffer per write txn instead of a `Vec` per op; APPEND compares the last key in the page instead of copying it. | `put/val/v8` 1.08× → 1.00×, `put/order/append` 1.32× → 1.19× | 2142ab2, 2ee1d25 |
| **Dirty frame pool** | Page buffers are reused within and across write txns, capped at 256 (LMDB's `me_dpages`). The empty-txn cost was accepted by the maintainer. | `put/gc/drain_big` 1.58× → 1.17×, `commit/batch/n1` 2.05× → 1.93×; `rw_empty_commit` 2.19× → 2.41× | 10c98a1 |
| **O(1) free-list front draw** | Each page drawn from a free-list entry memmoved the rest; a consumed-prefix offset makes it O(1), file byte-identical. | `put/gc/drain_big` 2.21× → 1.68× | 37b03c9 |
| **Spilling** | Past LMDB's dirty limit (131,072 pages; `max_dirty_bytes`), a write txn writes its highest-numbered dirty pages to the file and drops them, as `mdb_page_spill` does. | YCSB C loads instead of being killed; YCSB A 0.58 → 0.76, B 0.31 → 0.69 of LMDB's throughput [S]; ladder flat | ADR-0017 |
| **In-place `WRITE_MAP`** (opt-in) | Under `WRITE_MAP`, dirty pages live in the writable map at their final offset: no heap copy, no write-back, as `MDB_WRITEMAP`. | spike, macOS arm64, single-put no-sync commits: 29.8–43.5 → 8.6–11.1 µs | ADR-0021, PR #88 |
| **Meta free-list annex** | A commit's freed-page list goes into its own meta page when it fits, instead of a free-list tree put per commit (beyond LMDB). On-disk format 2. | `commit/batch/n1` 2.04× → 1.60×; durable YCSB B on Graviton4 239k → 258k ops/s; Meilisearch flat [M] | ADR-0022, PR #89 |
| **Sequential-writes option** (opt-in) | A per-tree rightmost-leaf finger lets ascending puts skip the descent (PostgreSQL's nbtree fast path; LMDB has none). Off by default: random puts are 3–4 % slower. | `put/val/v8` 0.83× → 0.66× [E] | ADR-0015 |

### Deletes

| Technique | What changed | Before → after | Pointer |
|---|---|---|---|
| **Cursor delete keeps its position (the `del_current` re-descent)** | `del_current` re-descended to delete, then again for the next step, so a cursor drain cost 28 % more than deleting by key. It now deletes at the path it holds and settles on the successor (LMDB's `C_DEL`), and keeps the path across leftmost-pairing rebalances (`mdb_cursor_del0`). SPEC 03 §5.4a; pinned by `tests/cursor_delete_position.rs`. | `del/cursor/drain` 3.38× → 2.46× (laptop), then 2.91× → 2.63× | c4ab096, 17fdb24 |
| **`delete_range` as one leaf walk** | Collected every key, then point-deleted each; now splices covered cells leaf by leaf and rebalances each leaf once (beyond LMDB, which has no range delete). | `del/range/half` 2.63× → 0.92× | 3512269 |
| **`clear` frees leaves unread** | Without overflow pages, leaf page numbers are freed from their parents without loading the leaves (LMDB's `mdb_drop0`). | `del/clear/all` 12.73× → 1.78× | c60a31e |

### Copies, stats and tools

| Technique | What changed | Before → after | Pointer |
|---|---|---|---|
| **The `non_free_pages_size` item** | Meilisearch calls it before every register write txn. It decoded the whole free list; it now sums per-DB page counts from the catalog, as heed does over LMDB, and is correct under `WRITE_MAP`. | 317 µs → 148 ns (0.41×) | f98374a, c82e5c8 |
| **Compaction memory (compaction RAM, bounded-memory compaction)** | Compaction, the compacting copy and `zerodb-tools load` held about twice the env size in RAM. The rebuild now streams: peak memory is O(tree depth × page size) plus the write buffers, and the destination appears by atomic rename. `load` still parses its input in memory (#63). | — | ADR-0009 |
| **Env copies write each byte once** | Copies were staged then copied again, the raw copy built the image in RAM, the compacting copy wrote one page per syscall. Now raw writes straight from the map and compact goes through 1 MiB buffers handed to a writer thread (`mdb_env_copyfd1`/`2`). | `maint/copy/raw` 2.53× → 0.93×, `maint/copy/compact` 3.31× → 0.98× | 2f479d2, 7dc3d1c |

## Tried and not adopted

| Idea | Why not | Pointer |
|---|---|---|
| Lazy validation: check only the cells a lookup touches | 1M-key tree −23 %, but sequential and missing-key gets +11–23 %; the env-wide cache later removed most of the repeat cost it targeted | ADR-0016 |
| Durable meta write through an `O_DSYNC` descriptor | 2 → 1 `fdatasync` per commit, but no measured gain on the reachable io2 host | ADR-0019, PR #86 |
| Used-portion COW copy (skip a page's free gap) | `commit/batch/n1` 1.95× → 1.93×, all rungs flat; the frame pool had already removed the cost, and stale bytes would reach the file | 2026-10-02, branch never merged |
| `#[cold]` arms plus `#[inline]` hints | flat at CGU16, `put/val/v8` +8.8 % at CGU1; LLVM still declined | ledger, 994a05c |
| Dirty-store arena or sorted map | the dirty-store probe never cleared the profile's noise bar; the frame pool took the useful part | ledger 2026-07-22; issue #5 closed, #4 open |
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
| Rightmost-leaf finger on by default | random and mixed puts +3–5 %; shipped as the opt-in sequential-writes option | ledger, c7c8b24; ADR-0015 |

## What remains

Measured gaps first, then ideas without a measurement yet. Issues are on
[GitHub](https://github.com/qdequele/ZeroDB/issues).

**Measured**

- **Delete rebalance** — per-key deletes 1.75×, cursor drains 2.72× [E]. A
  borrow moves one entry through a full remove + insert round trip, so
  rebalancing fires on almost every delete near the fill threshold (2.54 cell
  removals per delete where one is needed). LMDB runs the same algorithm but
  moves node bytes page to page. Fix: move several entries per borrow, or a
  direct move primitive; needs an ADR. On Meilisearch's delete-heavy
  settings change this showed as 1.07× end to end
  ([`2026-09-10-meilisearch-delete-heavy-macos.md`](../benches/results/2026-09-10-meilisearch-delete-heavy-macos.md)).
  No issue yet. A cursor delete that rebalances outside the leftmost pairing
  still re-descends.
- **Per-commit CPU** — `commit/batch/n1` 1.60×, empty write txn 2.46×, and
  YCSB no-sync on Graviton4 1.21–1.38× [M]. What is left is CPU per commit
  (page allocation, meta encoding, txn setup), not I/O. Related: reuse
  write-txn containers (#8), free-list bookkeeping (#29, measure first; only
  matters under GC churn), per-page allocation and zero-fill (#4, partly done
  by the frame pool).
- **Scans** — 1.3–1.8× by default; range scans 1.2× in trusted mode. A cursor keeps only its leaf;
  branch levels are re-resolved per cursor step (a memo hit, so cheap; one
  try to cache the parent read flat). Ideas: cache-line prefetch (#17),
  branchless page search (#16).
- **Overflow values** — 1.3× by default, because the overflow header is read
  and checked; with both engines trusting the file it is 0.89–0.91×.
- **Wider keys** — `get/key/k32`/`k128` 1.26–1.33× [E]; extending the integer
  compare to other key shapes is #15.
- **`WRITE_MAP` follow-ups** (#13) — ZeroDB's in-place `WRITE_MAP` still
  trails LMDB's by 1.34–1.42× on no-sync YCSB [M]. Open: ranged `msync` of
  only the changed pages (#45), prefault / preallocation of the map (#42), the
  Graviton A/B, and whether Meilisearch should set `WRITE_MAP`.
- **Short read txns** — one read txn + one get is 1.55× [V]; reusing read txns
  is #25.
- **Page fill under random inserts** — the unsorted 10M load makes an 8 %
  larger file than LMDB's [S]. No issue yet.
- **`zerodb-tools load` memory** — parses the whole dump and reads the built
  file back for its check (#63).

**Not yet measured**

- Custom comparators pay a virtual call per key comparison (#14; milli sets
  one comparator).
- Write cursor re-seek after `put_current` (#7) and the cursor reserved put
  (#10): no consumer profile names them.
- Durable commits on EBS gp3, where per-syscall latency should favour the
  coalesced writes: still owed. On SATA and Graviton4 NVMe durable commits
  are at parity or better.
- A 24-byte page header instead of 32 (ADR-0020): a spike ran on an
  unmerged branch, but the measurement that decides it is not recorded; an
  on-disk format change that needs approval.
- Consumer-facing APIs beyond LMDB: batched lookups (#53), single-descent
  get-merge-put (#52), group commit (#36), relaxed durability (#35), prefetch
  hints (#57), map warm-up (#61), reflink copies (#64), parallel `check`
  (#65).

## Deliberate differences

- **Meta CRC32C** (ADR-0002) — one checksum per commit and meta read; crash
  recovery depends on it to detect torn writes. LMDB has none.
- **Page validation by default** — a corrupt page gives a typed error, never
  undefined behaviour. Removing it is opt-in only, through trusted-file mode
  (ADR-0014).
- **Env creation fsyncs** — creating an env syncs the file and its parent
  directory, so a crash cannot lose the name of a durable file (issue #46).
  Paid once per env lifetime; it is why `env/open/create` is 8.55×.

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
| A7 | Custom comparators (#14) | B15 | Reused descent path and in-place APPEND check |
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
