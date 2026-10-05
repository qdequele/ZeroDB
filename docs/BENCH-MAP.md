# Bench map: which rung names which mechanism

Companion to `crates/zerodb-oracle/benches/engine_comparison/`. Run it with
`just bench [suite]`, read it with `just bench-report`.

`just consumer-bench` and `just hannoy-bench` answer *is zerodb fast enough for
this consumer*. This harness answers a different question: **where** the two
engines diverge, and **which mechanism** is responsible. It does that by
arranging the rungs as a ladder — adjacent rungs differ by exactly one
mechanism, so a ratio that jumps between two rungs names that mechanism's cost.

Ratio convention throughout: **`ratio = zerodb ÷ lmdb`**, so above 1.00 means
zerodb is slower. `just bench-report` prints, per rung, both the ratio and its
delta against the family's base rung (the column marked `Δfam`).

The "implicates" columns below name topics from the performance inventory,
[`PERF-GAP-VS-LMDB.md`](PERF-GAP-VS-LMDB.md).

## How to read a jump

A single rung's ratio is weak evidence — it bundles every cost that rung pays.
A *delta between adjacent rungs* is strong evidence, because everything except
the one added mechanism cancels. Three rules:

1. **Compare within a family, not across.** `get/val/v4k` against
   `get/val/v8` is a measurement; against `put/order/rand` it is a coincidence.
2. **Read per-engine ratios where the doc says so.** For some rungs the
   interesting quantity is `zerodb's own seq÷rand` versus `LMDB's own seq÷rand`,
   not the cross-engine ratio — the doc says which.
3. **Respect the noise mark.** `bench-report` marks a rung `~` when the two
   engines are within a standard deviation of each other. A `~` rung is not a
   finding however far its ratio sits from 1.00.

And the standing caveat: macOS is indicative. It isolates CPU, allocator and
memcpy, and a laptop-SSD fsync, but adds macOS-only effects (`F_FULLFSYNC`,
LMDB's POSIX-semaphore writer lock). The reference ladder runs on a quiet
x86-64 Linux bench server (`benches/results/*-bench-server-linux-x86.md`).
Claims about the deployed system — especially anything in `commit/sync/*` and
`maint/*` — still need a run on linux-aarch64 (Graviton) + EBS gp3, where a
page fault or a barrier is a network round-trip.

## The ladder

The **base** column is the rung `bench-report` measures the family against; it
is the cheapest or most-isolated rung, so every delta reads as "what this rung's
extra mechanism added". Changing a base here means changing `LADDER_BASE` in
`scripts/bench-report.py` too.

### `env` — the floors paid before any key is touched

| rung | isolates | implicates |
|---|---|---|
| `env/open/reopen` **(base)** | mapping and validating an existing image | geometry and txnid validation at open (divergence D-017) |
| `env/open/create` | writing a fresh image: file creation, initial metas, the catalog write | env creation's two fsyncs, by design |
| `env/txn/ro_begin_abort` **(base)** | the reader-table slot pin/unpin protocol, nothing else | lock-free reader table (ADR-0006) |
| `env/txn/rw_empty_commit` | the same, plus the writer-lock handoff and the meta update, with **zero** dirty pages | the commit floor every `commit/*` rung sits on |
| `env/stat/non_free` | `non_free_pages_size()` over a fragmented free list (~1M entries deleted across ~1k commits under a pinned reader → on the order of 10^5 free pages at 4 KiB, in ~1k GC entries) | milli's per-write-txn used-bytes probe: both engines sum per-DB branch + leaf + overflow pages (LMDB via `mdb_stat`, zerodb from its catalog records), so the cost tracks the DB count; the large free list checks that it does not depend on the free list |

`rw_empty_commit ÷ ro_begin_abort` is what a write transaction costs when it has
nothing to write. Subtract it from `commit/batch/n1` to get the part of a
one-put commit that is actually about the put.

### `get` — the point-lookup ladder

| rung | isolates | implicates |
|---|---|---|
| `get/db/root` **(base)** | a descent with **no** catalog record to resolve | — |
| `get/db/named` | the same descent plus named-DB resolution | per-txn named-DB memo |
| `get/db/named_x8` | resolution against eight different catalog records | that memo's hit rate |
| `get/access/hot` **(base)** | per-call overhead only: one key, everything resident, every memo warm | per-get allocation, cursor-free point get, adapter dispatch |
| `get/access/seq` | + ascending locality | cursor/leaf memo |
| `get/access/rand` | + scattered access: cold pages, cold memo | page-cache and TLB behaviour |
| `get/access/miss` | a full descent with **no** value returned | descent cost net of the value copy |
| `get/size/n1k` **(base)** → `n50k` → `n1m` *(long)* | tree depth, and nothing else | branch levels re-resolved per descent |
| `get/key/k8` **(base)** → `k32` → `k128` | key width: comparison cost and cells per page | integer fast path for 8/4-byte key compares (so `k8` takes a path the wider rungs do not), leaf density; the custom-comparator call only matters on custom-comparator DBs, which no rung uses |
| `get/val/v8` **(base)** → `v256` → `v4k` → `v2page` | value width; at 2×page it crosses into overflow pages | value memcpy, overflow handling |
| `get/val/v8_touch` **(base)** → `v4k_touch` → `v2page_touch` | the same value-width sweep, but every returned value is actually read | overflow-page chase + value memcpy |

`named ÷ root` is the price of named-DB resolution. `n1m ÷ n50k` is roughly the
price of one more tree level — read it against `n50k ÷ n1k` to see whether
per-level cost is constant or growing.

The plain `get/val/*` rungs (`point_get`) never read the bytes behind the
returned value, so on `v4k`/`v2page` the overflow-page chase and the value
memcpy can be optimized away or never paid for. The `*_touch` rungs use
`point_get_touch`, shared by both engines in `backend.rs`, which reads the first
and last byte of every returned value through `std::hint::black_box`.
`v8_touch` is the family's own control: `v4k_touch ÷ v8_touch` and
`v2page_touch ÷ v8_touch` isolate what actually reading the value costs.

### `scan` — cursor iteration

| rung | isolates | implicates |
|---|---|---|
| `scan/full/fwd` **(base)** | one descent, then pure leaf walking | the cursor leaf memo |
| `scan/full/rev` | the same walk against the sibling-link direction | reverse-iteration path |
| `scan/range/1pct` **(base)** → `10pct` | a positioned descent plus a per-step bound test | `RoRange` bound handling |
| `scan/prefix/bucket` | the same through the prefix API | `prefix_iter` / `MDB_SET_RANGE` |
| `scan/edge/first_last` | leftmost/rightmost descent, no walking | descent-only cost |
| `scan/meta/len` | the DB record, no tree walk at all | record read + txn setup |

`full/fwd` ÷ `edge/first_last`, per engine, separates "getting there" from
"walking". `meta/len` should be nearly free on both; if it is not, the cost is
in resolving the handle, not in the tree.

### `seek` — `MDB_SET_RANGE` positioning

| rung | isolates | implicates |
|---|---|---|
| `seek/ge/seq` **(base)** | ascending probes: whatever cursor state survives between seeks can show here | per-level cursor caching |
| `seek/ge/rand` | scattered probes, no locality to exploit | descent cost |
| `seek/ge/gap` | probes that land *between* keys, so every seek settles forward | boundary/leaf-crossing path |

Read `rand ÷ seq` **per engine**. A gap between the two engines' own ratios is a
cursor-state finding; the cross-engine ratio alone cannot separate that from
plain descent cost.

### `put` — inserts, fsync off

| rung | isolates | implicates |
|---|---|---|
| `put/order/append` **(base)** | insertion the engine has been *told* is ascending | `MDB_APPEND` fast path |
| `put/order/seq` | the same keys, ascending but not declared | what the engine failed to infer |
| `put/order/rand` | the same keys, scattered: splits everywhere, far more dirty pages | page splits, the dirty-page store, page frame reuse |
| `put/val/v8` **(base)** → `v256` → `v4k` → `v2page` | value width, then overflow pages | value memcpy, overflow-page zeroing |
| `put/api/plain` **(base)** | `put` | — |
| `put/api/reserved` | `MDB_RESERVE` — milli's document-serialization path | reserved-space writes in the heed adapter (divergence D-015) |
| `put/over/same_size` **(base)** | overwriting a cell that still fits | in-place replacement |
| `put/over/grow` | overwriting a cell that no longer fits | page rearrangement and splits |
| `put/gc/drain_big` | overwrites whose COW pages are all drawn from ONE large free-list entry (half of a 300k-key tree deleted first, then aged one commit so both engines' reuse gates admit it) | free-list draw cost per reused page vs entry length (SPEC 05). Every other rung starts with an empty or tiny free list |

`rand ÷ seq` is the split-and-COW cost. `seq ÷ append` is what an undeclared
ascending order leaves on the table.

### `del` — removal and the freelist behind it

| rung | isolates | implicates |
|---|---|---|
| `del/bulk/half` **(base)** | per-key deletion, tree stays populated: rebalance without collapse | delete and rebalance |
| `del/bulk/all` | per-key deletion down to empty: every leaf eventually merges | delete and rebalance, merge path |
| `del/range/half` | the same span through **one** `delete_range` call | range delete (a leaf-granular splice, not a per-key delete) |
| `del/cursor/drain` | the same span drained through the **write cursor** (`range_mut` + `del_current`) rather than by key | cursor delete; the only rung where post-delete cursor position costs anything |
| `del/clear/all` | the whole tree dropped in one operation | page-list work, not per-key work (leaves freed from their parents unread) |
| `del/churn/reinsert` | delete and re-insert alternating: freed pages must be reclaimed and handed straight back out | free-list churn, SPEC 05 |

A gap concentrated in `churn/reinsert` is a reclamation finding. A gap spread
evenly across `bulk/*` is a tree finding. `range/half ÷ bulk/half` says how much
the range API saves over per-key deletes. `cursor/drain ÷ bulk/half` (same span,
by key) is the write cursor's own overhead.

### `commit` — the transaction boundary

| rung | isolates | implicates |
|---|---|---|
| `commit/batch/n10k` **(base)** | per-commit overhead amortized away; what is left is dirty-page write-out | write coalescing |
| `commit/batch/n100` | a realistic batch | — |
| `commit/batch/n1` | almost pure per-commit overhead | commit path, meta update, free-list save (ADR-0022) |
| `commit/sync/n100` **(base)** | the same batch **with fsync on** | the durability barrier |
| `commit/sync/n1` | one fsync per put | worst-case barrier cost |

`sync/n1 ÷ batch/n1`, per engine, is that engine's barrier cost. On a laptop
that is an SSD; the number that decides anything is the EBS gp3 one, which is
also where coalesced writes are supposed to pay for themselves.

### `mixed` — the milli-shaped rung

`mixed/rw/8dbs` is the only rung that reads through a **write** txn, so the
lookup must consult the transaction's own uncommitted pages before the map.
milli's extractor → `write_db` phase does exactly this, thousands of times per
batch. Read it against `get/db/named_x8` (same fan-out, read-only txn) and
`put/order/rand` (same writes, no reads): worse than both means the cost is the
dirty-store lookup rather than either half alone. Implicates the dirty-page
store and the write txn's named-DB table.

### `concurrent` *(long tier)* — MVCC under contention

`concurrent/writer/r0` **(base)** → `r1` → `r4`: the timed value is the
**writer's** work only (first `write_txn` to last `commit`, measured inside
`writer_under_readers` and reported through criterion's `iter_custom`), while
`r` reader threads full-scan in a loop. Every other suite is single-threaded,
which hides exactly what MVCC exists to manage — the reader table under
contention, and a writer whose page reclamation is pinned by the oldest live
reader. `r0` runs the identical txn count and overwrites-per-txn with zero
reader threads, so it is the family's same-shape base rung. Reader threads poll
the stop flag every 1024 entries, so their join stays short.

Read `concurrent/writer/rN ÷ concurrent/writer/r0` **per engine**: that is what
N readers cost that engine. A gap between the two engines' ratios is a
reader-table (ADR-0006) or reclamation (SPEC 05) finding, not a tree finding.
Long tier because on a laptop the reader threads compete with the writer for
the same few cores, and the rung gets noisy.

### `maint` — whole-environment maintenance

`maint/copy/raw` **(base)** → `compact`: `mdb_env_copy2` in both modes. Raw is
close to a page-for-page file copy, so it is bounded by I/O and by how large the
environment is; compact rebuilds the tree densely, so it is bounded by the walk
plus the rebuild. Meilisearch calls this on every snapshot, so it is user-visible
latency.

`compact ÷ raw`, per engine, is the price of compaction. Between engines, `raw`
also reflects **on-disk size** — a denser store has less to copy — so a `raw`
win may be density rather than speed. Check `zerodb-tools stat` before claiming
either. Implicates env copy (both modes stream straight into the caller's file;
the compacting copy overlaps packing and writing with a writer thread) and
streaming compaction with bounded RAM.

## Fairness properties

These are structural, not conventions to remember:

* **One body per operation.** `backend.rs` holds a single macro body expanded
  over the two API-identical crate paths (`heed`, `heed_zerodb`). Neither engine
  can be given a hand-written operation.
* **One body per rung.** Every rung shape in `harness.rs` is a generic function
  over the `Backend` trait, instantiated per engine, and the `pair!` /
  `pair_shape!` / `pair_op!` macros name the shape and the operation exactly
  once. A paste cannot compare one engine's `scan` against the other's
  `rev_scan`.
* **Teardown outside the clock.** Rungs that build a fresh fixture per
  iteration (`put/*`, `del/*`, `commit/*`, `mixed/*`, `maint/*`) return it
  (env unmap, temp-dir removal, copied file) from the timed routine; criterion
  drops it after stopping the clock. `concurrent/*` times only the writer loop.
* **Identical data.** Seeded splitmix64, no `rand`, so a Graviton run compares
  the same bytes in the same order as a laptop run.
* **Identical page size.** LMDB is locked to the OS page size and exposes no
  selector, so zerodb is pinned to that value. Without this a 4 KiB-vs-16 KiB
  geometry gap swamps everything.
* **Named databases by default.** How milli and hannoy actually use the store.
  `get/db/root` is the deliberate exception: it is the baseline the named rungs
  are measured against.

## Before/after: judging one change

The ladder above compares ZeroDB with LMDB. A change needs a second question
answered: did *this change* move ZeroDB, or did the machine move? `just
bench-ab <regex>` answers it with three columns: LMDB, ZeroDB before and ZeroDB
after.

* **Before** is the engine at `BASE` (default `HEAD`). It is built in a reusable
  worktree under `target/bench-ab/base` with the working tree's bench harness
  copied over it, so both sides are measured with the same harness.
  **After** is the working tree.
* The two binaries run in `ROUNDS` interleaved rounds (default 3), and even
  rounds flip the order.
* LMDB's code is identical in both binaries, so **its before÷after is pure
  drift**. A rung whose LMDB moved more than `MAX_DRIFT` (3 %) is reported as
  unreliable, not read.
* A ZeroDB change counts only past `max(MIN_EFFECT, noise)`. Noise is the larger
  of criterion's 95 % CI and twice the round-to-round spread.
* `verdict.json` gives one of `improved`, `regressed`, `flat` or `invalid`. A
  regression on any rung counts, not just on the targeted rungs.

`just bench-gate` runs the same comparison over the whole ladder against the
merge base with `main`. `just bench-profile <rung> [lmdb]` shows where one
rung's time goes. `just perf-ledger show` lists every earlier attempt, including
the reverted ones. A performance iteration chains these: pick a rung, profile
it, change one lever, A/B it against the base, keep or revert, and log the
result in the ledger.

## Adding a rung

1. Decide which mechanism it isolates and which existing rung it differs from by
   exactly that one mechanism. If there is no such rung, add the base rung too —
   a rung with no neighbour measures nothing attributable.
2. Add the operation to `backend.rs` (inside the macro body) and to the
   `Backend` trait in `harness.rs`.
3. Add the rung to its suite with `pair!`, so the operation is named once.
4. Add a row here, and — if it starts a new family — a `LADDER_BASE` entry in
   `scripts/bench-report.py`.
