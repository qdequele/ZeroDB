# Performance

This page covers how ZeroDB compares with LMDB, which techniques closed each gap,
what was tried and dropped, and what is still open. It also explains how the
benchmarks work. Every number links to the report that measured it.

The comparison point is the LMDB build Meilisearch ships (`lmdb-master-sys
0.2.6`, under heed 0.22.1).

## Summary

- **Real workloads run at parity.** Meilisearch indexing and search, and hannoy
  builds and searches, are within a few percent of LMDB in either direction,
  on x86-64 and on Graviton4.
- **Point reads, seeks and small sequential writes are as fast or faster.**
- **The gaps are in micro-operations those consumers use little:** full scans,
  cursor drains, tiny and empty commits, and write-heavy no-sync YCSB. In each
  case the remaining cost is CPU per operation, not I/O.
- **Some costs are deliberate:** page validation, the meta-page checksum, and
  syncs at env creation. Each buys a safety property LMDB lacks, and the
  validation can be turned off with an opt-in.

## How it is measured

**Ratio = ZeroDB time ÷ LMDB time. Below 1.00, ZeroDB is faster.** Where a
source reports throughput, the ratio is LMDB ops/s ÷ ZeroDB ops/s, the same
time ratio.

- **The benchmark ladder:** criterion benchmarks in
  `crates/zerodb-oracle/benches/engine_comparison/`, run with `just bench` and
  read with `just bench-report`. Each rung isolates one mechanism; see
  [The benchmark ladder](#benchmark-ladder) below.
- **Machines:**
  - The reference ladder machine: an x86-64 Xeon E3-1230 v2 (turbo off, SATA
    SSD, Debian 12, 4 KiB pages). Rungs are read at `codegen-units = 16` (the
    default, and hannoy's build) and at 1 (Meilisearch's release build).
  - AWS Graviton4 (`m8gd`/`c8g`, aarch64), the target hardware.
- **YCSB:** [rust-storage-bench](https://github.com/marvin-j97/rust-storage-bench)
  with a `zerodb` backend next to its heed one: 10M items, one txn per
  operation, usually under a 2 GB memory cap so the data does not fit in RAM.
- **Consumers end to end:** Meilisearch's `cargo xtask bench` workloads and
  hannoy's own benchmarks, each built once against LMDB and once against
  ZeroDB ([maintainers/CONSUMER-GATE.md](maintainers/CONSUMER-GATE.md)).

Reports are in [`benches/results/`](../benches/results/) (index in its README).
Every experiment ever tried, kept or reverted, is one line in
[`perf-ledger.jsonl`](../benches/results/perf-ledger.jsonl).

## Where ZeroDB stands

Default options unless stated. "Trusted mode" is the opt-in that skips page
validation (ADR-0014). Source keys:

| Key | Report |
|---|---|
| E | [full ladder and consumers, x86-64, CGU16](../benches/results/archive/2026-09-28-evening-bench-server-linux-x86.md) |
| V | [read rungs after the validated-pages cache, x86-64, CGU1](../benches/results/archive/2026-09-30-validation-cache.md) |
| M | [YCSB on Graviton4 NVMe, and Meilisearch](../benches/results/archive/2026-10-05-meta-annex-real-case.md) |
| S | [YCSB under a 2 GB cap after spilling](../benches/results/archive/2026-09-30-bounded-dirty-memory.md) |
| P | [public YCSB suite, Graviton4 NVMe (indicative)](../benches/results/archive/2026-09-30-public-suite-nvme.md) |
| G | [full ladder on Graviton4, lock file on and off](../benches/results/archive/2026-10-13-multi-process-stage3-graviton.md) |
| G2 | [env rungs after the lock file is rebuilt in place, Graviton4](../benches/results/archive/2026-10-15-lock-file-in-place-graviton.md) |
| C | [Meilisearch v1.53.1 and hannoy on Graviton4](../benches/results/2026-10-15-meilisearch-v1.53.1-graviton-xlarge.md) |
| D | [delete rungs and crud-bench, laptop (indicative)](../benches/results/archive/2026-10-18-delete-rebalance-node-move-mac.md) |
| Q | [delete path, three-column A/B on Graviton4](../benches/results/archive/2026-10-31-delete-path-cursor.md) |
| X | [scans and batch deletes, Graviton4](../benches/results/archive/2026-10-19-crud-scan-count-and-batch-delete.md) |
| SC | [validated-pages cache sized to the file, Graviton4](../benches/results/archive/2026-10-31-stamp-cache-graviton.md) |
| L | the perf ledger line for the change |

E, V, M, S and P predate the multi-process lock file, which has been on by
default since ADR-0023. G measures both settings: with the lock file off every
rung is flat, and with it on five rungs regress; their rows give both figures.

### Point reads

| What | Ratio | Source |
|---|---:|---|
| Hot key, every page cached (`get/access/hot`) | 0.82× | E |
| Random gets, named database, 8-byte keys (`get/access/rand`, `get/db/named`, `get/key/k8`) | 0.94–0.95× | V |
| 1M-key tree (`get/size/n1m`) | 1.08× | V |
| Wider keys (`get/key/k32`, `k128`) | 1.33×, 1.26× | E |
| Values in overflow pages (`get/val/v4k`, `v2page`) | 1.33×, 1.32× | E |
| … trusted mode, both engines trusting the file | 0.91×, 0.89× | L |
| One read txn plus one get, 1M keys | 1,008 vs 649 ns (1.55×) | V |

### Scans and seeks

| What | Ratio | Source |
|---|---:|---|
| Seeks (`seek/ge/rand`, `seek/ge/seq`) | 0.95× | V |
| Seeks into gaps (`seek/ge/gap`) | 1.32× | E |
| Full scan (`scan/full/fwd`) | 1.49× | V |
| Range scan, 1 % (`scan/range/1pct`) | 1.33× | V |
| Prefix bucket (`scan/prefix/bucket`) | 1.76× | V |
| First and last (`scan/edge/first_last`) | 1.66× | E |
| Database `len` (`scan/meta/len`) | 1.00× | E |
| Full scan, trusted mode, Graviton4 (`scan/full/fwd`, `rev`) | 1.02×, 1.06× | X |

### Writes

| What | Ratio | Source |
|---|---:|---|
| Small sequential puts (`put/val/v8`, `put/order/seq`, `put/api/plain`) | 0.83–0.87× | E |
| … with the sequential-writes option | 0.66–0.77× | E |
| Reserved puts, milli's document path (`put/api/reserved`) | 0.96× | E |
| Random-order puts (`put/order/rand`) | 0.93× | E |
| `APPEND` puts (`put/order/append`) | 1.21× | [report](../benches/results/archive/2026-10-31-write-cursor-split-append-graviton.md) |
| Overwrites that grow a value (`put/over/grow`) | 1.22× | E |
| Puts drawing from a large free-list entry (`put/gc/drain_big`) | 1.14× | E |
| 4 KiB values (`put/val/v4k`) | 0.99× | E |
| Mixed reads and writes over 8 databases (`mixed/rw/8dbs`) | 0.92× | E |

### Deletes

| What | Ratio | Source |
|---|---:|---|
| Per-key deletes (`del/bulk/half`, `del/bulk/all`) | 1.35–1.36× | Q |
| Delete and re-insert churn (`del/churn/reinsert`) | 1.28× | Q |
| 1,000 random deletes per txn (`del/batch/k1000`) | 1.26× | Q |
| Draining through the write cursor, forward and backward | 1.66×, 1.50× | Q |
| `delete_range` (`del/range/half`) | 0.52× | Q |
| `clear` (`del/clear/all`) | 1.43× | Q |

### Commits and transactions

| What | Ratio | Source |
|---|---:|---|
| One-put no-sync commit (`commit/batch/n1`) | 1.17× | [report](../benches/results/archive/2026-10-31-writer-carry-graviton.md) |
| 100 and 10k puts per commit | 1.08×, 0.90× | E |
| Empty write txn (`env/txn/rw_empty_commit`) | 2.46×; Graviton4 3.42× with the lock file, 2.80× without | E, G |
| Read txn begin and abort (`env/txn/ro_begin_abort`) | 0.56×; Graviton4 0.73× with the lock file, 0.66× without | E, G |
| Writer with 0–4 concurrent readers | 1.15–1.18× | E |
| Durable commits (`commit/sync/*`) | 0.97–1.03× | E |
| YCSB B durable, Graviton4 NVMe | 258k vs 238k ops/s (0.92×) | M |
| YCSB A / B no-sync, Graviton4 NVMe | 1.21× / 1.38× | M |
| YCSB A / B no-sync, both engines with `WRITE_MAP` | 1.34× / 1.42× | M |

### Large transactions and memory

| What | Result | Source |
|---|---|---|
| 10M-key unsorted load in one txn, 2 GB cap | Completes, 539 MiB peak anonymous memory vs LMDB's 520 MiB | S |
| YCSB A / B / C after that load, no sync | 1.32× / 1.45× / 1.16× | S |
| File size after that load | +8 % (page fill under random inserts) | S |
| Meilisearch hackernews (1M docs), peak RSS | 4.02–4.27 vs 3.91–4.10 GB | E |

### Copies, stats and env open

| What | Ratio | Source |
|---|---:|---|
| Raw and compacting env copy | 0.94×, 0.99× | E |
| `non_free_pages_size` | 0.41×; Graviton4 0.48× with the lock file | L, G |
| Reopen an env | 0.96× with the lock file; 0.49× without | G2, G |
| Create an env | 10.50× with the lock file; 9.04× without, by design (see [Deliberate differences](#deliberate-differences)) | G |

### Consumers end to end

| Workload | Ratio | Source |
|---|---:|---|
| Meilisearch indexing, movies | 0.99× (E), 1.01× (M), 1.02× (C) | E, M, C |
| Meilisearch indexing, hackernews 1M docs | 0.99× | E |
| Meilisearch hackernews incremental additions | 0.99× (M); 1.03× on an 8 GiB Graviton4 host (C), most likely host memory; 0.89× on a 16 GiB laptop | M, C |
| Meilisearch search, movies | 0.95× (E), 0.94× (M), 0.99× (C) | E, M, C |
| Meilisearch search, hackernews | 1.01× | E |
| hannoy build, 512 / 768 / 1536 dims | 0.84× / 0.84× / 0.89× (E); 0.94× / 0.95× / 0.97× (C) | E, C |
| hannoy search, 512 / 768 / 1536 dims | 0.95× (E); 0.94× / 0.94× / 0.83× (C) | E, C |
| Public suite, Graviton4: YCSB B fsync / 4 KB values / YCSB A no-sync | 1.00× / 0.90× / 2.01× | P |

## What closed each gap

Each technique copies LMDB's own approach for that path unless marked "beyond
LMDB". Ratios are before → after.

### Read path

| Technique | Change | Before → after |
|---|---|---|
| <a id="a1"></a>Named-database records resolved once | Each read took a lock and descended the catalog. A read txn now resolves each database once into a table indexed by handle, as LMDB's `txn->mt_dbs[dbi]` does. | `scan/meta/len` 2.46× → 1.01× |
| <a id="a2"></a>Page validation once per page | ZeroDB checks every cell of a page before trusting it, so a corrupt file is a typed error. The check now runs once per page per txn, recorded in a lock-free memo, and pages the engine wrote itself are trusted. | milli indexing 2.68× → 1.12× (laptop) |
| Env-wide validated-pages cache (ADR-0018) | Read txns share one cache keyed by page number, kind and the txn id stamped in the page, so short txns stop re-validating hot pages. Since 2026-10-31 it has one slot per page of the file. | read txn plus get 1,984 → 1,008 ns; random gets 1.16× → 0.94× (V); 10M-key random get p50 1.32 → 1.23 µs (SC) |
| Trusted-file mode (ADR-0014, opt-in) | Skips the per-cell walk and the overflow header check, as LMDB does. | trusted `get/val/v4k` 1.29× → 0.91× |
| Field reads at fixed offsets | Page fields are read with `read_unaligned` at explicit offsets behind one bounds proof per view. | — |
| No allocation per get | Cursors keep their path in a fixed 32-frame inline stack, as LMDB's `CURSOR_STACK`. | — |
| <a id="b13"></a>Hot paths above LLVM's inlining threshold | Raising LLVM's threshold sped hot paths up 8–38 %. The nine helpers it then inlined are now `#[inline(always)]`. | CGU1: `get/access/hot` 1.37× → 1.11×; 37 rungs faster, none slower |
| Cursor-free point get | A `get` walks root to leaf and takes exactness from the leaf search, as `mdb_node_search`. | `get/access/hot` 1.11× → 0.94× |
| Integer compare for 4- and 8-byte keys (beyond LMDB) | Equal-length 8- or 4-byte keys compare as one big-endian integer, in the same order. Meilisearch's and hannoy's hot trees use such ids. | `get/access/hot` 1.00× → 0.80× |
| Scan steps | A step within a leaf reads the entry from the leaf view in hand, as `mdb_cursor_next`. | `scan/full/fwd` 2.25× → 1.45×, trusted 2.05× → 1.02× (X) |
| `NO_READ_AHEAD` honored | Advises the map `MADV_RANDOM`, as LMDB does. | bigger-than-RAM YCSB: 7–11× less read from disk |

### Write path and commits

| Technique | Change | Before → after |
|---|---|---|
| <a id="b1"></a>Write cursor keeps its path | The write cursor parks and resumes its path instead of re-seeking by key; a delete, a same-key put and a split keep the path, as LMDB fixes up its cursors. | Graviton4: `put/cursor/rewrite` 2.50× → 1.37×, `del/cursor/drain` 2.62× → 2.15× |
| <a id="b4"></a>Batched writes | Contiguous dirty pages go out in `pwritev` batches, as `mdb_page_flush`. | — |
| <a id="b6"></a>Reserved puts write in place | `put_reserved` hands the caller the page slot directly, as `MDB_RESERVE`. | `put/api/reserved` 0.96× |
| <a id="b11"></a>Writer slot without a syscall | Releasing a write txn wakes a waiter only if one waits. | `rw_empty_commit` 5.93× → 2.19× |
| Reused descent path; in-place `APPEND` check | One path buffer per write txn; `APPEND` compares the last key in place. | `put/order/append` 1.32× → 1.19× |
| <a id="b18"></a>Dirty frame pool | Page buffers are reused within and across write txns, as LMDB's `me_dpages`. | `put/gc/drain_big` 1.58× → 1.17× |
| <a id="b7"></a>O(1) free-list draw; meta free-list annex | Drawing a page from a free-list entry is O(1). A commit's freed-page list rides in its meta page when it fits, instead of a free-list tree put per commit (beyond LMDB, ADR-0022). | `put/gc/drain_big` 2.21× → 1.68×; `commit/batch/n1` 2.04× → 1.60× |
| <a id="b12"></a>Writer state reused across txns | Write txns reuse the previous one's containers, as LMDB's `me_txn0`: 19 → 5 heap allocations per single-put commit. | Graviton4 `commit/batch/n1` 1.26× → 1.17× |
| <a id="c2"></a>Spilling (ADR-0017) | Past the dirty limit, a write txn writes its highest dirty pages to the file and drops them, as `mdb_page_spill`. | YCSB C loads instead of being killed |
| <a id="b5"></a>In-place `WRITE_MAP` (ADR-0021, opt-in) | Dirty pages live in the writable map at their final offset, as `MDB_WRITEMAP`. | single-put no-sync commits 29.8–43.5 → 8.6–11.1 µs (laptop) |
| End splits on ascending inserts | Full branch and leaf pages split at the end on an end insert, as LMDB does, so ascending loads build LMDB's page counts. | 20k ascending keys: 949 → 480 branch pages (LMDB 558) |
| Sequential-writes option (ADR-0015, opt-in, beyond LMDB) | A per-tree rightmost-leaf finger lets ascending puts skip the descent. Off by default, because random puts get 3–5 % slower. | `put/val/v8` 0.83× → 0.66× |

### Deletes

| Technique | Change | Before → after |
|---|---|---|
| <a id="b22"></a>Cursor delete keeps its position | `del_current` deletes at the path it holds and settles on the successor, as LMDB's `C_DEL`, and keeps the path across rebalances. | `del/cursor/drain` 3.38× → 2.46× (laptop), then 2.41× → 1.66× (Graviton4) |
| <a id="b23"></a>`delete_range` as one leaf walk (beyond LMDB) | Splices covered cells leaf by leaf and rebalances each leaf once. | `del/range/half` 2.63× → 0.92×; 0.52× today |
| <a id="b8"></a>Rebalance moves cells verbatim (ADR-0028) | Borrows and merges move cells byte for byte and rewrite separators in place, as LMDB's node move. | `del/bulk/half` 2.46× → 1.30× (laptop) |
| <a id="b21"></a>`clear` frees leaves unread | Leaf page numbers are freed from their parents without loading the leaves, as `mdb_drop0`. | `del/clear/all` 12.73× → 1.78×; 1.43× today |

### Copies and stats

| Technique | Change | Before → after |
|---|---|---|
| <a id="b20"></a>`non_free_pages_size` from the catalog | Sums per-database page counts instead of decoding the free list, as heed does over LMDB. Meilisearch calls it before every write txn. | 317 µs → 148 ns |
| <a id="c1"></a>Streaming compaction (ADR-0009) | Compaction, the compacting copy and `zerodb-tools load` stream: peak memory is O(tree depth × page size), and the destination appears by atomic rename. | memory about 2× env size → bounded |
| <a id="b10"></a>Env copies write each byte once | A raw copy writes straight from the map; a compacting copy goes through 1 MiB buffers handed to a writer thread, as `mdb_env_copyfd1`/`2`. | raw 2.53× → 0.93×, compact 3.31× → 0.98× |

## Tried and not adopted

| Idea | Why not |
|---|---|
| Lazy validation: check only the cells a lookup touches (ADR-0016) | 1M-key tree −23 %, but sequential and missing-key gets +11–23 %; the env-wide cache removed most of the cost it targeted |
| Durable meta write through an `O_DSYNC` descriptor (ADR-0019) | 2 → 1 `fdatasync` per commit, but no measured gain on the hosts available |
| Copy-on-write that skips a page's free gap | Flat after the frame pool, and stale bytes would reach the file |
| `#[cold]` arms plus `#[inline]` hints | Flat at CGU16, +8.8 % on `put/val/v8` at CGU1 |
| Dirty-store arena or sorted map | Never cleared the noise bar |
| Caching the parent branch page during scans | 2–4 %, under the noise bar |
| LMDB's cell-removal loop shape | Slower: `copy_within` beats per-element indexing |
| Inline path stack on the write path | Zero-filling a 520-byte struct per op cost 8 % |
| Exact one-page slice on every page load | `get/access/hot` +20 % at CGU1 (code layout) |
| Branch-free leaf cell validation | `scan/full/fwd` +5 %: the early-exit loop is perfectly predicted on valid pages |
| Writers publishing into the validation cache | `put/val/v8` +12 % |
| Branchless page search (`select_unpredictable`) | Gets 8–25 % slower on Apple M-series; retry only with a Graviton A/B |
| Integer compare for any keys of at most 8 bytes | `scan/prefix/bucket` +18 % |
| Rightmost-leaf finger on by default | Random and mixed puts +3–5 %; shipped as an opt-in instead |

## What remains

Issues are tracked on [GitHub](https://github.com/qdequele/ZeroDB/issues).

- **Per-commit CPU.** `commit/batch/n1` is 1.17×, an empty write txn about
  2.5×, and no-sync YCSB on Graviton4 1.21–1.38×. The remaining cost is CPU per
  commit, not I/O: the writer slot's in-process mutex (about 8 atomic
  read-modify-writes per empty write txn against LMDB's 2; removing it needs an
  ADR), the snapshot published per commit, and the free-list record that small
  txns rewrite after a large delete (LMDB keeps its reclaimed list across txns).
- **Deletes.** 1.35× per key and 1.50–1.66× through the write cursor. The byte
  moves now match LMDB's exactly; what remains is bookkeeping around them, such
  as looking up pages by number where LMDB's cursor holds page pointers.
  Workloads with one delete per txn are bound by commit: re-encoding a full
  meta free-list annex dominates.
- **Scans.** 1.3–1.8× by default and 1.2× for range scans in trusted mode. A
  cursor keeps only its leaf; branch levels are re-resolved per step (a cheap
  cache hit).
- **Overflow values and wide keys.** 1.3× by default, because the overflow
  header is read and checked; 0.89–0.91× when both engines trust the file.
- **`WRITE_MAP`.** ZeroDB's in-place `WRITE_MAP` still trails LMDB's by
  1.34–1.42× on no-sync YCSB. Durable `WRITE_MAP` commits on macOS issue a
  second `fdatasync` after the meta write that LMDB does not.
- **Short read txns.** One read txn plus one get is 1.55× (V); per-thread reuse
  has since cut it by 26 % on macOS, and a Linux re-measure is pending.
- **Page fill under random inserts.** An unsorted 10M load makes an 8 % larger
  file than LMDB's.
- **Not measured yet:** durable commits on EBS gp3; a 24-byte page header
  instead of 32 (ADR-0020, a format change).

## Deliberate differences

<a id="d"></a><a id="b9"></a>

- **Meta-page CRC32C (ADR-0002).** One checksum per commit and per meta read,
  which crash recovery uses to detect torn writes. LMDB has none. On aarch64 it
  uses the ARMv8 `crc32c` instructions (ADR-0031): about 0.75 µs for a full
  16 KiB page on Graviton4.
- **Page validation by default.** A corrupt page is a typed error, never
  undefined behavior. Trusted-file mode turns it off.
- **Env creation syncs.** Creating an env syncs the file and its parent
  directory, so a crash cannot lose the name of a durable file. It is paid once
  per env, and is why `env/open/create` is about 9×.
- **Lock file on by default (ADR-0023).** Multi-process access costs about
  +30 ns per write commit, +7 ns per read txn begin and +7 ns per `stat` on
  Graviton4. `NO_LOCK` is flat. Reopen is at parity with LMDB (36.4 vs 37.8 µs),
  since the lock file is rebuilt in place as LMDB does.

These well-known techniques were evaluated and rejected; each line says why,
so they are not re-proposed:

- **`O_DIRECT` writes.** Reads go through a `MAP_SHARED` map of the page cache,
  and Linux does not guarantee that mapped pages are invalidated after an
  `O_DIRECT` write, so readers could see stale data.
- **Bε-trees or buffered writes.** Every read would have to merge pending
  buffered writes, losing zero-copy reads. milli is read-bound.
- **Per-page compression.** The dominant values (Roaring bitmaps, float32
  vectors) compress poorly, and decompression needs a buffer pool.
- **A hash-table database type.** It would lose ordered iteration, which milli
  and hannoy rely on, and make page faults and locality worse.
- **Bloom filters.** milli already checks word existence with its own FSTs
  before touching the store.
- **Pointer swizzling.** Mapped pages already have stable addresses.
- **`MDB_FIXEDMAP`.** Experimental upstream, fights address-space
  randomization, and no consumer needs it.

## Benchmark ladder

The ladder in `crates/zerodb-oracle/benches/engine_comparison/` answers
**where** the two engines diverge and **which mechanism** is responsible.
Adjacent rungs differ by exactly one mechanism, so a ratio that jumps between
two rungs names that mechanism's cost. (`just consumer-bench` and
`just hannoy-bench` answer the other question: is ZeroDB fast enough for this
consumer.)

`just bench [suite]` runs it, and `just bench-report` prints each rung's ratio
and its change against its family's **base** rung. The bases are set in
`LADDER_BASE` in `scripts/bench-report.py`.

### Reading it

1. **Compare within a family, not across.** `get/val/v4k` against `get/val/v8`
   is a measurement; against `put/order/rand` it is a coincidence.
2. **Some quantities are per engine.** For example, read `seek/ge/rand ÷
   seek/ge/seq` separately for each engine; a gap between the two engines' own
   ratios is a cursor-state finding.
3. **Respect the noise mark.** A rung marked `~` (the engines within a standard
   deviation) is not a finding, however far its ratio is from 1.00.

macOS runs are indicative only: they add macOS-only effects such as
`F_FULLFSYNC`. Claims about deployment, especially `commit/sync/*` and
`maint/*`, need a run on Linux aarch64.

### Families

| Family | Base rung | What the rungs isolate |
|---|---|---|
| `env` | `env/open/reopen`, `env/txn/ro_begin_abort` | Opening and creating an env; the reader-slot protocol; an empty write commit (the floor under every `commit/*` rung); `non_free_pages_size` over a fragmented free list |
| `get` | `get/db/root`, `get/access/hot`, `get/size/n1k`, `get/key/k8`, `get/val/v8` | Named-database resolution; per-call overhead vs locality (`hot`, `seq`, `rand`, `miss`); tree depth (`n1k` → `n1m`); key width (`k8` → `k128`); value width into overflow pages (`v8` → `v2page`, with `*_touch` rungs that actually read the value) |
| `scan` | `scan/full/fwd`, `scan/range/1pct` | Leaf walking forward and backward, a walk past every cache (`full/big`), `skip`/`take`, range and prefix bounds, first and last, `len` |
| `seek` | `seek/ge/seq` | Ascending, random and between-key positioning |
| `put` | `put/order/append`, `put/val/v8`, `put/api/plain`, `put/over/same_size` | Declared, undeclared and random order; value width; `MDB_RESERVE`; overwrites that fit or grow; write-cursor rewrites; draws from one large free-list entry |
| `del` | `del/bulk/half` | Per-key deletes, `delete_range`, write-cursor drains forward and backward, `clear`, delete and re-insert churn, batched random deletes, one delete per txn |
| `commit` | `commit/batch/n10k`, `commit/sync/n100` | Per-commit overhead (`n10k` → `n1`), with and without fsync |
| `mixed` | — | `mixed/rw/8dbs`: reads through a write txn over 8 databases, milli's extractor pattern |
| `concurrent` (long tier) | `concurrent/writer/r0` | The writer's time with 0, 1 and 4 reader threads scanning |
| `maint` | `maint/copy/raw` | Raw and compacting env copies |

Useful ratios: `rw_empty_commit ÷ ro_begin_abort` is the cost of a write txn
with nothing to write; `get/db/named ÷ get/db/root` is named-database
resolution; `put/order/rand ÷ put/order/seq` is the split and copy-on-write
cost; `del/cursor/drain ÷ del/bulk/half` is the write cursor's own overhead;
`commit/sync/n1 ÷ commit/batch/n1` is an engine's barrier cost.

### Fairness

These properties are structural, not conventions:

- **One body per operation.** `backend.rs` holds one macro body expanded over
  the two API-identical crates (`heed`, `heed_zerodb`).
- **One body per rung.** Each rung shape is one generic function over the
  `Backend` trait, so a copy-paste cannot compare one engine's `scan` with the
  other's `rev_scan`.
- **Teardown outside the clock.** Fixtures are returned from the timed routine
  and dropped after the clock stops.
- **Identical data.** Seeded splitmix64, so every machine compares the same
  bytes in the same order.
- **Identical page size.** LMDB is locked to the OS page size, so ZeroDB is
  pinned to the same value.
- **Named databases by default**, as milli and hannoy use the store.

### Judging one change

`just bench-ab <regex>` answers whether a change moved ZeroDB or whether the
machine moved, with three columns: LMDB, ZeroDB before and ZeroDB after.

- **Before** is the engine at `BASE` (default `HEAD`), built in a reusable
  worktree with the working tree's bench harness; **after** is the working
  tree.
- The two binaries run in interleaved rounds (default 3), alternating order.
- LMDB's code is the same in both binaries, so **its before ÷ after is pure
  drift**. A rung whose LMDB moved more than 3 % is reported as unreliable.
- A ZeroDB change counts only past the larger of a minimum effect and the
  noise (criterion's 95 % CI, or twice the round-to-round spread).
  `verdict.json` says `improved`, `regressed`, `flat` or `invalid`; a
  regression on any rung counts.

`just bench-gate` runs the comparison over the whole ladder against the merge
base with `main`, `just bench-profile <rung>` shows where one rung's time goes,
and `just perf-ledger show` lists every earlier attempt.

### Adding a rung

1. Decide which mechanism it isolates, and which existing rung it differs from
   by exactly that one mechanism. If there is none, add that base rung too.
2. Add the operation to `backend.rs` (inside the macro body) and to the
   `Backend` trait in `harness.rs`.
3. Add the rung to its suite with `pair!`.
4. Update the family table above and, for a new family, `LADDER_BASE` in
   `scripts/bench-report.py`.

## Item codes

Older ADRs, specs, reports and the perf ledger cite performance items by code.

| Code | Item | Code | Item |
|---|---|---|---|
| A1 | [Named-database records resolved once](#a1) | B10, B24 | [Env copies write each byte once](#b10) |
| A2, A8 | [Page validation once per page](#a2), validated-pages cache, lazy validation (not adopted) | B11 | [Writer slot without a syscall](#b11) |
| A3, A4, A6 | Read-path cleanups: field reads, page-size check at open only, no allocation per get | B12 | [Per-commit CPU](#b12) |
| A5 | Scans: branch levels re-resolved per step | B13 | [Hot paths above LLVM's inlining threshold](#b13) |
| A7 | Custom comparators (no consumer registers one) | B14–B17, B19 | Write-txn table, reused descent path, flag-only page views, cursor-free get, integer key compare |
| B1 | [Write cursor keeps its path](#b1) | B18 | [Dirty frame pool](#b18) |
| B2 | Splits without materializing the page | B20 | [`non_free_pages_size`](#b20) |
| B3 | Dirty-store arena or sorted map (not adopted) | B21 | [`clear` frees leaves unread](#b21) |
| B4 | [Batched writes](#b4) | B22, B8a | [Cursor delete keeps its position](#b22) |
| B5 | [In-place `WRITE_MAP`](#b5) | B23 | [`delete_range` as one leaf walk](#b23) |
| B6 | [Reserved puts write in place](#b6) | C1, C3 | [Streaming compaction](#c1) |
| B7 | [Free-list draw and meta annex](#b7) | C2 | [Spilling](#c2) |
| B8 | [Delete rebalance](#b8) | B9, D | [Deliberate differences](#deliberate-differences) |
