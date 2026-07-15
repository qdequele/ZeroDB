# Project: ZeroDB — Pure-Rust Storage Engine — LMDB Parity Behind the heed API

## Mission

Build a pure-Rust transactional embedded KV engine that is a drop-in replacement
for LMDB **at the heed API level** for Meilisearch and hannoy. Phase 1 targets
strict LMDB feature parity behind heed's existing traits. Phase 2 fills the gaps
in heed's coverage of LMDB. Phase 3 diverges: workload-specific improvements
(Meilisearch indexing, hannoy HNSW) exposed through new opt-in APIs.

Project name: `zerodb` (ZeroDB).

## Non-negotiable ground rules (for the agent)

1. **The oracle is C LMDB.** Every behavior question is answered by writing a
   differential test against LMDB via heed, never by guessing. If LMDB's
   behavior is surprising, replicate it anyway in Phase 1; log it in
   `docs/DIVERGENCES.md` as a Phase 3 candidate.
2. **heed's trait surface is the frozen contract.** No signature changes to
   existing heed APIs in Phases 0–2. New capabilities are new methods/types.
3. **Every milestone ends green.** Each milestone below has acceptance criteria.
   Do not start the next milestone with failing tests. Never delete or weaken a
   test to make it pass.
4. **Clean-room discipline.** Read LMDB/libmdbx source to understand
   *algorithms and formats*, then implement from your own notes in
   `docs/SPEC/`. Never transliterate C code. All code Rust 2021+, MIT/Apache
   dual license.
5. **unsafe policy.** `unsafe` is allowed only in: mmap access, page casting,
   the reader table, and FFI in the test oracle. Every unsafe block gets a
   `// SAFETY:` comment. `cargo miri` must pass on all non-mmap logic.
6. **Target platforms:** linux-aarch64 (Graviton 3/4, primary), linux-x86_64,
   macOS aarch64 (dev). Test with 4K and 64K kernel page sizes. ARM's weak
   memory model is the default assumption — all atomics get explicit orderings
   with a comment justifying them.
7. **Single-process by default.** Cross-process readers are out of scope until
   explicitly added (Phase 3, optional). This deletes LMDB's robust-mutex and
   lockfile machinery. Document the divergence.

## Repository layout

```
zerodb/
  crates/
    zerodb-core/        # page formats, B+tree, txn, GC — no I/O policy
    zerodb-io/          # mmap reader, pwrite/io_uring writers, fsync strategies
    zerodb/             # public engine API (heed-shaped)
    heed-zerodb/        # heed backend adapter (or: fork heed, add backend feature)
    zerodb-tools/       # dump/load/stat/check/migrate binaries
    zerodb-oracle/      # differential test harness against C LMDB (via lmdb-master-sys)
  fuzz/                 # cargo-fuzz targets
  docs/
    SPEC/               # your own written spec of formats & algorithms (source of truth)
    DIVERGENCES.md      # intentional behavior differences vs LMDB
    DECISIONS.md        # ADR index (one line per ADR)
    adr/                # ADRs, one file per significant choice
  benches/              # criterion micro + macro benches
```

---

## Phase 0 — Contract extraction and oracle harness

**Goal: know exactly what to build, and have the machinery to prove it works.**

### 0.1 API surface inventory
- Scan Meilisearch (milli), hannoy, and arroy for every heed item used:
  `EnvOpenOptions` flags, `Env`, `RoTxn`/`RwTxn`, `Database`, typed codecs
  (`BytesEncode`/`BytesDecode`), cursors/iterators (fwd/rev/range/prefix),
  `put_with_flags` (APPEND, APPEND_DUP, CURRENT, NO_OVERWRITE, ...), DUPSORT /
  DUPFIXED usage, `del`, `clear`, nested txn usage, `copy_to_file` /
  compacting copy, `Env::resize`, stat/info calls.
- Include the easily-missed items: `Env::real_disk_size` and
  `Env::non_free_pages_size` (milli reads LMDB's freelist DB for disk-usage
  reporting — this is LMDB-format-specific and needs a native zerodb
  equivalent; it is a MUST, not a Phase 2 nicety), `Env` clone semantics, the
  same-process env registry, and `EnvClosingEvent`.
- Pin the exact heed / milli / hannoy / arroy versions (git SHAs) that define
  the frozen contract; record them in the spec doc.
- Output: `docs/SPEC/00-api-surface.md` — a checklist table: heed item →
  used by (milli / hannoy) → LMDB primitive → Phase 1 milestone that covers it.

### 0.2 LMDB flag & semantics matrix
- Enumerate all LMDB env/db/write flags. Classify each: MUST (used by
  consumers), SHOULD (heed exposes it), WON'T (document why —
  e.g. `MDB_NOTLS` becomes the default and only mode, cross-process modes
  deferred).
- Output: `docs/SPEC/01-flags.md`.

### 0.3 Oracle harness
- Crate `zerodb-oracle`: drives an identical operation sequence against
  (a) C LMDB and (b) zerodb, comparing every result: return values,
  error codes, iteration order, and post-txn reads.
- **Engine-agnostic ops trait.** The oracle defines its own thin `Engine`
  trait; LMDB implements it via heed (or lmdb-master-sys directly), zerodb via
  its **native API**. Do NOT route zerodb through `heed-zerodb` here — the
  adapter is built in 1.13, but every milestone from 1.2 onward needs the
  oracle. Adapter-level parity is re-verified at the 1.14 gate by re-running
  the oracle through the heed adapter.
- Operation model: an enum of ops (open_db, put, del, get, cursor ops,
  begin/commit/abort, nested begin, clear, resize...) + `arbitrary` impl for
  fuzzing.
- Output: `oracle::run(ops) -> Divergence?` usable from unit tests, proptest,
  and cargo-fuzz.

### 0.4 Written spec of the target format
- Write `docs/SPEC/02-pages.md`, `03-btree.md`, `04-txn-mvcc.md`, `05-gc.md`,
  `06-recovery.md` from reading LMDB 1.0 source and docs. This is the document
  the implementation is written FROM. Include: meta page double-buffer
  protocol **with a mandatory meta-page CRC** (torn-meta detection is Phase 1,
  not Phase 3 — a half-written meta from a real power cut must be rejected at
  open; data-page checksums stay Phase 3.9), page header (incl. txnID stamp —
  adopt the LMDB 1.0 idea), branch/leaf/overflow layouts, node formats,
  DUPSORT sub-tree/sub-page encoding, freelist encoding, commit ordering &
  fsync barriers.
- Spec the **write-txn value-borrow contract** in `04-txn-mvcc.md`: a `get`
  during a write txn may return bytes from a dirty page in heap memory (not
  the map); dirty-page storage must never move or reallocate while
  `&'txn [u8]` borrows are live. This is a Rust soundness requirement, the
  most likely soundness bug outside the reader table — it gets its own spec
  section and miri coverage.
- Note: we define **our own on-disk format** (no file-format compat with LMDB
  0.9 or 1.0). Migration is via logical dump/load. This is a hard decision,
  already made.

### 0.5 heed integration strategy (ADR required, human-approved)
- Decide how zerodb gets behind milli and hannoy: fork heed with a backend
  feature, upstream a backend abstraction into heed, or a standalone
  `heed-zerodb` crate re-exporting a heed-compatible surface. heed is NOT
  generic over a backend today and consumers depend on concrete heed types —
  including the env registry and `EnvClosingEvent` semantics Meilisearch
  relies on. This decision constrains lifetimes, error types, and `Env`
  semantics for all of Phase 1; milestone 1.13 implements it.
- Output: ADR in `docs/adr/`, indexed in `docs/DECISIONS.md`, human-approved.

**Acceptance:** spec docs reviewed; oracle harness runs a trivial op sequence
against LMDB alone (self-test mode); API checklist complete with pinned
consumer SHAs; integration-strategy ADR approved; 64K-page Graviton CI runner
provisioned (see Testing strategy).

---

## Phase 1 — Core engine: LMDB feature parity behind heed

Each milestone = one Claude Code working session (or a few). Keep PRs per
milestone.

### 1.1 Page model and file geometry
- Page types: meta ×2, branch, leaf, overflow, freelist. Const-generic-free,
  runtime page size (4K–64K, power of two), chosen at env creation, stored in
  meta.
- Page header includes: page number, type/flags, txnID of writer, entry count,
  free space bounds. Reserve space for optional checksum (Phase 3).
- Zero-copy page views: `&[u8]` → typed accessor structs with alignment-safe
  reads (no `#[repr(C)]` casting of unaligned data; use explicit
  offsets + `read_unaligned` where needed).
- **Accept:** proptest round-trips for every page/node encode/decode; fuzz
  target `fuzz_page_decode` runs clean 10 min.

### 1.2 Environment open/close and meta protocol
- Env open: create or open file, validate magic/version/page size, map file
  (read-only mmap), pick live meta (higher txnID with valid CRC — meta pages
  carry a mandatory CRC32C from day one so a torn meta is detected and the
  older meta wins).
- Map size handling: replicate LMDB semantics for Phase 1 (`map_size`,
  `MDB_MAP_FULL`/`MDB_MAP_RESIZED` equivalents through heed's error types).
  Auto-geometry is Phase 3.
- **Accept:** oracle parity on env lifecycle tests incl. reopen, wrong page
  size, corrupted meta (falls back to older meta).

### 1.3 Read path: B+tree search and cursors
- Search, `get`, full cursor state machine: first/last/next/prev,
  set/set_range (>=), get_both/get_both_range (dupsort later), prefix iteration
  as used by heed.
- RoTxn = pinned root + snapshot txnID. Reads are zero-copy `&[u8]` borrowed
  from the map with heed's lifetime model.
- **Accept:** oracle parity on read-only workloads over DBs *generated by
  zerodb's own loader* (write path not ready: use a test-only page builder that
  constructs trees from sorted data — this builder is the prototype of 3.4's
  `bulk_load`, so write it to be promoted, not discarded). Cursor semantics
  fuzz vs LMDB (LMDB DB populated identically via its own API).

### 1.4 Write path: single-writer txn, COW, commit
- Single writer (in-process mutex). Dirty page set, COW on first touch, page
  allocation from end-of-file (GC comes in 1.5), node insert/delete with
  split/merge/rebalance, overflow page chains for large values.
- Commit: write dirty pages, fsync(data), write meta, fsync(meta) — the
  ordering is the crash-safety invariant; encode it in one function with a
  test hook to inject crashes between steps.
- Dirty-page memory stability: reads in a write txn may borrow from dirty
  pages; the dirty set must guarantee those allocations never move or
  reallocate while `&'txn [u8]` borrows are live (contract specced in
  `04-txn-mvcc.md`, milestone 0.4). miri must exercise get-then-put sequences.
- Abort: drop dirty set.
- **Accept:** oracle parity on randomized single-threaded write workloads
  (put/del/commit/abort, values 0 B–16 MB); tree invariant checker
  (`zerodb-tools check`) passes after every fuzz run.

### 1.5 Free-page management (GC)
- LMDB-style for parity: freelist DB keyed by txnID, pages reusable once no
  reader can see them (reader table, 1.8, gates this — until then, oldest
  reader = current txn). Loose-page fast path.
- Known weakness (huge txns) is documented, not fixed here — fix is Phase 3.1.
- **Accept:** long-running fuzz shows bounded file growth on
  insert/delete-heavy churn; parity with LMDB on file size within a tolerance
  band; check tool verifies no page is both free and reachable.

### 1.6 Named databases and the catalog
- Main DB as catalog; open/create named DBs, per-DB flags (dupsort, integer
  keys...), per-DB stats. `clear` and `drop`.
- **Accept:** oracle parity on multi-DB workloads incl. create-in-txn-then-
  abort semantics.

### 1.7 DUPSORT / DUPFIXED
- Duplicate values per key: sub-page then sub-tree encoding, dup cursors
  (first_dup/next_dup/get_both...), DUPFIXED packed layout, APPEND_DUP.
- This is the highest-defect-density area of LMDB — budget extra fuzz time.
- **Accept:** dedicated dupsort differential fuzz target runs clean ≥ 2 h;
  milli's facet/word-docids access patterns replayed with parity.

### 1.8 MVCC reader table and concurrency
- In-process reader registry (per-thread slots, lock-free claim/release,
  explicit `Ordering`s). Oldest-reader computation gates GC. Readers never
  block; writer never blocks readers.
- `loom` tests for the reader table; `RoTxn: Send` semantics matching heed's
  current model (NOTLS-like is simply the default).
- **Accept:** loom suite passes; stress test (N reader threads + 1 writer,
  minutes) with invariant checks; GC never reclaims a page a live reader can
  reach (assert via shadow tracking in debug builds).

### 1.9 Nested (child) transactions
- **Contingent on the 0.1 inventory.** If neither milli nor hannoy uses nested
  txns (expected), descope: return a clean "unsupported" error, log it in
  DIVERGENCES.md, and scope the 1.14 heed-suite gate accordingly. Full
  implementation only if a consumer needs it.
- If implemented: child write txns with page shadowing, commit-into-parent,
  abort. Match LMDB restrictions (no cursors spanning, etc.) as heed exposes
  them.
- **Accept:** oracle parity on randomized nested-txn sequences (or the
  documented divergence + error-path tests if descoped).

### 1.10 Write flags and modes
- APPEND / APPEND_DUP (with correct misuse errors), NO_OVERWRITE, CURRENT,
  RESERVE (heed's `put_reserved`), MULTIPLE if heed uses it.
- Env durability flags parity: NOSYNC / NOMETASYNC / MAPASYNC semantics mapped
  onto our writer (even though the I/O layer differs), RDONLY envs.
- **Accept:** flag matrix table in docs each with a differential test;
  Meilisearch's exact indexing flag combo replayed at parity.

### 1.11 Recovery, torn writes, and crash consistency
- Crash-injection harness with **two mechanisms** — SIGKILL alone cannot tear
  a write (the OS page cache survives process death; only power loss tears or
  reorders sectors):
  1. Process kill: run workload in child process, SIGKILL at random points
     (and between the fsync barriers via the 1.4 hook), reopen, verify.
  2. Fault-injection write backend in `zerodb-io`: simulates power loss by
     tearing / reordering / dropping writes not yet covered by an fsync
     (CrashMonkey/ALICE-style), producing a disk image to reopen-and-verify.
- Verify after every cycle: last committed txn fully visible, no partial txn,
  torn meta rejected via CRC, check tool clean.
- Add optional O_DIRECT-friendly write sizing early so EBS behavior is sane.
- **Accept:** ≥ 10k crash-recovery cycles clean in CI (parallelized), across
  both mechanisms.

### 1.12 Tools and migration
- `zerodb-tools`: `stat`, `dump` (LMDB-compatible logical format), `load`,
  `check`, and `migrate-from-lmdb` (opens a real LMDB env read-only via
  vendored C or lmdb-master-sys, streams into zerodb — this is the one place
  linking C is fine).
- Compacting copy (`copy_to_file` equivalent) since Meilisearch snapshots use
  it.
- Tools take an **exclusive lock** on the env (or refuse with a clear error):
  with no cross-process reader protocol (D-001), pointing `stat`/`check` at a
  live env would be silently unsafe. Offline operation is the documented mode.
- **Accept:** round-trip LMDB → dump → zerodb → dump → byte-identical logical
  dumps; a real Meilisearch index migrated and byte-identical query results;
  a tool invoked on a locked live env fails cleanly.

### 1.13 heed backend plumbing
- Implement the 0.5 ADR: build `heed-zerodb` (or the heed fork / backend
  feature) covering the full SPEC 00 surface; map the error taxonomy 1:1;
  replicate heed's env registry and `EnvClosingEvent` semantics.
- Patch milli and hannoy to build against it behind a cargo feature, at the
  SHAs pinned in 0.1.
- **Accept:** milli and hannoy compile on the zerodb backend; heed's test
  suite (scoped per the 0.5 ADR and the 1.9 decision) passes; the oracle
  re-run **through the heed adapter** (not just the native API) shows zero
  divergences.

### 1.14 Integration gate (exit criteria for Phase 1)
- Meilisearch test suite (milli + integration) green on zerodb behind the
  cargo feature from 1.13.
- hannoy test suite + recall benchmarks green on zerodb.
- 24 h continuous differential fuzz with zero divergences.
- Bench report vs LMDB on Graviton (4K and 64K kernels): read point/range,
  write txn throughput, indexing macro-bench. Parity target: within ±15% on
  reads, no worse than LMDB on writes.

---

## Phase 2 — heed API completion (LMDB features heed doesn't expose)

Add to heed (as the zerodb backend's extension or upstreamed):
- 2.1 Full env/db stat & info structs (depth, pages by type, entries).
- 2.2 Reader introspection API (list readers, txnID ages) — replaces
  `mdb_reader_list/check` in a single-process world.
- 2.3 Compacting copy with progress callback.
- 2.4 `cmp`/custom key comparators as safe Rust closures/traits (LMDB has
  `mdb_set_compare`; heed hides it) — needed before Phase 3 features anyway.
- 2.5 Explicit `sync(force)` (mdb_env_sync parity).
- 2.6 Page-size selection at env creation (LMDB 1.0 feature, our engine already
  supports it internally).
- 2.7 Anything found in 0.1 marked SHOULD but unexposed.

**Accept:** each new API has differential semantics tests where LMDB has the
feature, and doc + unit tests where it's zerodb-defined.

---

## Phase 3 — Improvements (each behind a feature/option, each with a bench proving it)

Ordering chosen by expected impact for Meilisearch/hannoy on Graviton + EBS/NVMe.

### 3.1 GC redesign for huge write transactions
- Replace flat freelist with a structure that stays O(log n) under
  million-page txns (mdbx-inspired reclaiming; design in an ADR first).
- Bench: Meilisearch full reindex of a large dataset; target: eliminate the
  freelist-dominated tail.

### 3.2 Auto-geometry
- `set_geometry(min, max, growth_step, shrink_threshold)` (mdbx-shaped API).
  Kills `MDB_MAP_FULL` handling in milli. Keep manual mode for compat.

### 3.3 Bounded relaxed durability
- `SafeNoSync { max_bytes, max_period }`: crash loses ≤ window, never
  corruption (meta only advances to fully-synced txns). Designed for
  NVMe + replication deployments where replicated-ack is the real durability.

### 3.4 Bulk-load builder
- `Database::bulk_load(sorted_stream)` — bottom-up packed page construction,
  sequential writes, no rebalancing. Direct consumer: milli's grenad-sorted
  output replacing APPEND loops.
- Bench: indexing throughput on EBS gp3 (sequential-write win is largest
  there).

### 3.5 Pluggable write path: pwrite vs io_uring
- `zerodb-io` backends selectable at env open. io_uring path with batched
  submission for commit; benchmark both on Graviton EBS and instance NVMe.
  Reads stay mmap.

### 3.6 Aligned values and fixed-record tables (hannoy)
- Guaranteed value alignment (configurable, default 64 B) for SIMD-ready
  zero-copy reads.
- New table type: fixed-size record arena with O(1) id→offset lookup,
  COW-consistent with the txn model. heed extension API:
  `Env::create_fixed_database::<N>()`.
- Bench: hannoy distance-kernel throughput and search latency vs B-tree
  storage.

### 3.7 Prefetch and access hints
- `db.prefetch(keys)` / `txn.advise(range, Willneed|Random|Sequential)`
  mapping to madvise; replaces hannoy's env-var hack.

### 3.8 Snapshot reads during a write txn
- `RwTxn::snapshot() -> RoSnapshot` pinning the last committed root — legal
  concurrent reads from rayon workers while the build writes. Direct consumer:
  hannoy graph construction.

### 3.9 Page checksums and encryption-at-rest
- CRC32C (ARMv8 hw instructions) in the reserved header field — data pages
  only; meta pages already carry a mandatory CRC since Phase 1. Optional AEAD
  per page (page_no + txnID as IV, LMDB 1.0 design). Enterprise checkbox.

### 3.10 Incremental page shipping (backup/replication substrate)
- txnID-stamped pages ⇒ `env.pages_since(txn_id) -> stream`. Consumers:
  incremental S3 snapshots, PIT restoration, physical replica catch-up.
  Engine provides the stream; orchestration lives in Cloud.

### 3.11 Optional: key prefix compression; per-DB page size
- Only if benches on real Meilisearch key distributions justify complexity.

---

## Testing strategy (continuous, not a phase)

- **Differential fuzzing** (cargo-fuzz + oracle): the primary correctness tool.
  Targets: general ops, dupsort ops, cursor semantics, nested txns, page
  decode. Run in CI (short) + nightly long runs on an ARM box.
- **Crash consistency**: harness from 1.11 in nightly CI — fsync-barrier
  injection, SIGKILL, and the fault-injection write backend (torn/reordered/
  dropped writes).
- **Hardware**: 64K-page kernels cannot be simulated on standard CI runners or
  qemu-user. Provision a self-hosted Graviton runner with a 64K-page kernel
  (e.g. RHEL ARM) during Phase 0 so it exists long before the 1.14 gate.
- **loom** for the reader table and any lock-free structure; **miri** for all
  non-mmap unsafe.
- **Invariant checker** (`check` tool) run after every fuzz/crash iteration:
  tree ordering, page reachability xor freeness, dup structure validity,
  meta consistency.
- **Macro benches** on Graviton (c8g + i8g), 4K and 64K kernels, EBS gp3 and
  local NVMe: Meilisearch indexing + search suite, hannoy build/recall/latency.
  Store results in-repo (`benches/results/`) for trend tracking.

## Working with Claude Code on this repo

See **CLAUDE.md** — it is the single source of the working rules (agent
roster, ADR gates, unsafe policy, check commands). PLAN.md defines *what* to
build; CLAUDE.md defines *how* to work. Do not duplicate rules here.

## Suggested sequencing summary

| Stage | Content | Rough effort |
|---|---|---|
| Phase 0 | Contract, spec, oracle, integration ADR, CI hardware | 1–2 weeks |
| Phase 1 | Parity engine + heed plumbing | 2–4 months (1.7, 1.11, 1.13 are the long poles) |
| Phase 2 | heed completion | 2–3 weeks |
| Phase 3.1–3.4 | GC, geometry, durability, bulk load | 1–2 months |
| Phase 3.5–3.8 | io_uring, hannoy features | 1–2 months |
| Phase 3.9–3.11 | checksums/encryption, page shipping | as needed |

Effort assumes heavy agent assistance with human review on: unsafe code, fsync
ordering, GC correctness, and every ADR.
