# Adoption guide for Meilisearch and hannoy

ZeroDB is a pure-Rust embedded key-value engine, a drop-in replacement for
LMDB at the API of `heed` (the Rust LMDB wrapper Meilisearch uses). Its
consumers are `milli` (Meilisearch's indexing and search core) and `hannoy`
(Meilisearch's HNSW vector index, stored as a database inside each `milli`
index environment).

Several engine features help only if the calling code opts in. This page is
the recipe list: for each one, where it applies, what changes, what it
trades away, and whether the engine side has actually landed. (The drop-in
test and the consumer benchmarks themselves are covered by
[`CONSUMER-GATE.md`](CONSUMER-GATE.md), not this page.) Tracked in issue
[#69](https://github.com/qdequele/ZeroDB/issues/69).

Several of these options trade safety for speed (`NO_SYNC`, `WRITE_MAP`,
trusted-file mode). Each recipe below states the failure mode next to the
gain. **None of them should become a default** — they are opt-in on purpose.

## Actionable today

### 1. Durability checkpoints for the scheduler

Per issue #69, Meilisearch's index-scheduler opens its environments with
LMDB's default full durability — it sets neither `NO_SYNC` nor
`NO_META_SYNC` — so every scheduler write transaction pays a disk flush. How
many commits one scheduler task makes is **not measured** here; that needs
confirming on the Meilisearch side before sizing the change.

The opt-in: open the scheduler environment with `NO_META_SYNC` or `NO_SYNC`,
and call an explicit checkpoint at fixed points, such as once per scheduler
tick, instead of relying on every commit's own fsync.

- `Env::force_sync()` is the heed-compatible call: exactly `mdb_env_sync(env,
  1)`, the only form heed exposes (`crates/heed-zerodb/src/env.rs`).
- `Env::sync(force: bool)` is a ZeroDB extension with full
  `mdb_env_sync(env, force)` parity (`crates/heed-zerodb/src/env.rs`).
  `force = false` is a no-op on a `NO_SYNC` env — prior commits stay
  non-durable — and leaves `MAP_ASYNC` as an asynchronous `msync`;
  `force = true` always issues a real barrier.

**What a crash can lose**: under `NO_META_SYNC`, a crash can lose the most
recent commit's meta update even though its data pages reached disk; under
`NO_SYNC`, a crash can lose any commits made since the last explicit sync or
checkpoint, same as LMDB.

### 2. `NO_READ_AHEAD` for vector-heavy indexes

`NO_READ_AHEAD` is now honored as in LMDB: the map is advised
`madvise(MADV_RANDOM)`, so the kernel does not read ahead around page faults
(`docs/COMPATIBILITY.md`, `EnvFlags` table).

In a bigger-than-memory random-read test, the environment **without** the
flag read about 10 GB from disk in 60 s for a 1.5 GB database, because every
page fault pulled in a whole readahead window nobody asked for
(`benches/results/2026-09-29-ycsb-bigger-than-ram.md`, "First run: ZeroDB
ignored `NO_READ_AHEAD`").

hannoy's graph walk reads pages in a near-random order, so per issue #69 it
is the kind of workload this flag targets. Two things to check before
setting it:

- Because hannoy lives inside the shared `milli` index environment, the
  flag applies to the **whole index environment**, not just hannoy's
  database — confirm it does not slow `milli`'s own sequential scans before
  enabling it index-wide.
- It does not replace hannoy's own `prefetch_graph` (per issue #69,
  `MADV_WILLNEED` on graph pages in hannoy's `src/reader.rs` — a file in the
  hannoy repository, not this one). The two are complementary: `prefetch_graph`
  asks ahead for pages hannoy already knows it wants, `NO_READ_AHEAD` stops
  the kernel from guessing on everything else.

### 3. `WRITE_MAP`

Meilisearch already exposes `WRITE_MAP` behind an option, per issue #69, in
`crates/index-scheduler/src/index_mapper/index_map.rs` (Meilisearch's own
repository). ZeroDB's `WRITE_MAP` now writes dirty pages directly in the map
instead of staging them on the heap and copying them in at commit
(ADR-0021).

On YCSB with no-sync writes (Graviton4, local NVMe, 10 M × 128-byte
records), enabling `WRITE_MAP` on both engines makes ZeroDB 1.6–1.7× faster
than **default** LMDB (without `WRITE_MAP`), while still trailing LMDB's own
`WRITE_MAP` (`README.md`, "Performance"; same YCSB A/B figures as
`benches/results/2026-10-05-meta-annex-real-case.md`).

**Trade-off**: a writable map means a wild write through a dangling pointer
or a bug anywhere in the process can corrupt the database file directly,
with no page-by-page write boundary the way a heap-staged commit has. This
is the same trade LMDB's own `MDB_WRITEMAP` makes.

### 4. Trusted-file mode

`EnvOpenOptions::file_trust(FileTrust::trust_contents())` skips ZeroDB's
read-path page validation and reads pages the way LMDB does — by trusting
the mapped bytes (`crates/zerodb/src/lib.rs`; `docs/COMPATIBILITY.md`,
`file_trust(FileTrust)` row; ADR-0014).

The constructor is `unsafe`: once set, a corrupt or hostile data file
produces undefined behaviour instead of a typed error. The default keeps
validation, which is what makes a corrupt file fail safely in the first
place. Only consider this where the file's integrity is already guaranteed
some other way.

### 5. Sequential-writes fast path

`EnvOpenOptions::sequential_writes(bool)` sets an env-wide default; `Env::
set_sequential_writes(&db, Option<bool>)` overrides it per database
(`crates/heed-zerodb/src/env.rs`; `crates/zerodb-core/src/env.rs`;
`docs/COMPATIBILITY.md`, `sequential_writes(bool)` row; ADR-0015). A put
whose key sorts after the last key in the tree goes straight to the
rightmost leaf instead of descending from the root.

Measured on the bench server: ascending-key puts get faster, while
random-key puts get roughly 3–5 % slower (ADR-0015 measured `put/order/rand`
+3–5 % and `mixed/rw/8dbs` +3–4 % when always on, which is why it shipped as
an opt-in with a per-database override). Results are identical with the
option on or off.

Candidates per issue #69: tables whose keys only grow — documents by
internal id, hannoy item ids. Tables with random keys, such as word →
document ids, should stay off.

### 6. Build settings

Meilisearch's release profile is `codegen-units = 1` with no LTO, and its
global allocator is mimalloc, not jemalloc (`docs/CONSUMER-GATE.md`,
"Allocator: Meilisearch builds with its production allocator (mimalloc)";
`benches/results/2026-09-29-ycsb-bigger-than-ram.md`, "With Meilisearch's
allocator (mimalloc v3)").

Do **not** recommend `codegen-units = 1` as a ZeroDB speedup: measured, it
is ZeroDB's worst case relative to LMDB, slower than the default 16 on most
read benchmarks (issue #66; `docs/PERF-GAP-VS-LMDB.md`, "How it is
measured"). ZeroDB force-inlines its hot read-path helpers
(`#[inline(always)]`, commit `eb450ac`) so that a `codegen-units = 1` build
is covered without setting any flag (`docs/PERF-GAP-VS-LMDB.md`, "Hot paths
above LLVM's inlining threshold"). Target-CPU and PGO/BOLT guidance is
tracked separately in issue #66 and [`BUILD-GUIDE.md`](BUILD-GUIDE.md) —
**not measured yet**.

## Blocked on engine work — write the recipe, mark it pending

### 7. Fused read-modify-write

`milli`'s incremental indexing reads a value — often a document-id
bitmap — merges a change, and writes it back, which costs two tree descents
per key (issue #69). **Pending**: migrate these call sites once a
single-descent `update(key, closure)` lands
([issue #52](https://github.com/qdequele/ZeroDB/issues/52)). Not available
today.

### 8. Batched lookups

hannoy's per-hop neighbour reads and `milli`'s per-chunk key lookups each
issue independent `get`s (issue #69). **Pending**: adopt a batched get once
it lands ([issue #53](https://github.com/qdequele/ZeroDB/issues/53)). Not
available today.

### 9. Bounded relaxed durability

A mode where a crash loses at most a configured window of commits, never
consistency ([issue #35](https://github.com/qdequele/ZeroDB/issues/35)).
**Pending**: once it lands, it would replace the `NO_SYNC` plus checkpoint
recipe in item 1 above. Not available today.

## Why it matters

Items 1–6 can be used today, but only once someone writes down where they
apply and what they cost — that is this page. Items 7–9 mark the consumer
change to make as soon as the matching engine feature ships.

## See also

- [`CONSUMER-GATE.md`](CONSUMER-GATE.md) — the drop-in test and the
  LMDB-vs-ZeroDB Meilisearch/hannoy benchmarks.
- [`COMPATIBILITY.md`](COMPATIBILITY.md) — the full `EnvFlags` and
  `EnvOpenOptions` compatibility tables.
- [`PERF-GAP-VS-LMDB.md`](PERF-GAP-VS-LMDB.md) — the performance inventory
  these recipes draw their numbers from.
- [`BUILD-GUIDE.md`](BUILD-GUIDE.md) — build-time (not runtime-option)
  guidance: target-cpu, PGO/BOLT.
- ADR-0014 (trusted-file mode), ADR-0015 (sequential-writes option),
  ADR-0021 (in-place `WRITE_MAP`).
