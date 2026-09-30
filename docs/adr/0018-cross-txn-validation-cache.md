# ADR-0018: Env-wide validated-pages cache across transactions

- Status: Accepted (approved by Quentin 2026-09-29: "go adr 18"); implemented and measured 2026-09-30
- Milestone: Phase 3 (performance), follow-up of ADR-0016
- Date: 2026-09-29

## Context

By default a map page's cells are fully validated the first time **each txn**
views it; the validated-pages memo is txn-scoped (PERF-GAP A2). A workload of
short read txns therefore validates the same hot pages over and over. On the
bench server a one-get read txn over a 1M-key tree costs **2,176 ns against
LMDB's 714 ns**, and the profile puts **59 %** of it in the full walk
(`BranchRef::new` 39.5 %, `LeafRef::new` 19.8 %): every get re-validates the
root, the branches and the leaf it passes through. rust-storage-bench's YCSB
runs are this shape (one txn per operation), and so is Meilisearch search at a
coarser grain: every search request opens a fresh read txn and re-validates
every page it touches.

Trusted mode (ADR-0014) removes the walk for users who opt in; lazy
validation (ADR-0016) was measured and parked.

## Key observation

A committed page's bytes do not change while any snapshot can reach it, and
ZeroDB stamps every page it writes with the writing txn's id (the common
header's `txnid`, SPEC 02 §2). A page that is freed and later reused is
rewritten with a **newer** stamp. So the pair **(pgno, stamp)** identifies one
immutable version of a page: a validation result recorded for that pair stays
true for as long as the page carries that stamp, in every txn, without any
invalidation message from the writer.

## Options

### Option A — Env-wide direct-mapped cache keyed by (pgno, stamp) (proposed)
A fixed-size, lock-free table in the env (e.g. 64 Ki slots), consulted on a
txn-memo miss for a **map** page: read the page's stamp (a header field, same
cache line as the flags), probe the slot, and on an exact (pgno, stamp, kind)
hit take the zero-check view; on a miss validate fully and publish the entry.
A collision or a race only causes a miss, which revalidates. The txn-scoped
memo stays in front of it.
- **Coherent entries (required):** the key word (pgno and kind) and the stamp
  are two separate atomics, and Release/Acquire on each does not make two
  loads a coherent pair: a replacement could expose the old pgno with the new
  stamp, and since many pages share one txn's stamp, that mixed pair could
  match a page nobody validated. Each slot is therefore a **seqlock**: a
  version word that a publisher makes odd (CAS from even) before writing the
  two words and even again after; a lookup reads the version, the two words,
  then the version again, and counts a hit only for an unchanged, even
  version and an exact key and stamp. Any mixed read sees a changed or odd
  version and is a miss. The protocol gets a loom model.
- **Soundness:** a hit requires the exact stamp read from the current bytes;
  ZeroDB never writes two different contents with the same (pgno, stamp) that
  a snapshot can see (a txn writes each page once at commit, and a reused page
  gets the reusing txn's id — `RwTxn::touch` restamps every COW copy with the
  txn id, and fresh pages are initialized with it). A hostile file is still
  validated on its first view, since the cache starts empty at open.
- **Invariant to pin with a test:** every page written at commit (tree pages,
  overflow runs, GC pages) carries the committing txn's id, so no two
  visible versions of a pgno share a stamp.
- **Limits:** a file modified by another process while open breaks the
  immutability the cache relies on — already outside the single-process model
  (D-001), but it must be stated.
- **Cost:** one extra header read and one probe on a txn-memo miss; a fixed
  memory footprint per env.

### Option B — Invalidate by pgno from the writer at commit
Key by pgno only, and have the writer clear entries for reclaimed pgnos before
publishing. Needs writer/reader ordering (another publication protocol next to
the reader table) — more moving parts than the stamp, for the same result.

### Option C — Keep per-txn memos (today)
No change; short txns keep paying the walk.

## Decision

Option A, measured on the one-op-per-txn census, the `get/*` ladder (a fresh
txn per iteration), the YCSB runs and Meilisearch search; kept only if the
long-txn rungs stay flat.

Answers to the review questions (2026-09-29):

1. **Soundness argument:** accepted — (pgno, stamp) identity with the stated
   single-process assumption. Pinned by `zerodb/tests/page_version_identity.rs`,
   which fails if COW copies stop being restamped (mutation-checked).
2. **Size:** fixed, 64 Ki slots of 32 bytes (2 MiB per env at most),
   allocated in 32 KiB chunks on the first publish that lands in each, so an
   env pays only for the chunks its hot pages fall in (slot = pgno modulo
   64 Ki). Scaling with the map
   size is deferred until a measurement shows misses from collisions.
3. **Write txns:** no (revised after measurement, 2026-09-29). The soundness
   argument would hold for the pages a writer reads from the map, but the
   first version consulted the cache from write txns too and the ladder
   showed `put/val/v8` +12 % and `put/val/v256` +4 %: a writer copies on write
   most of the map pages it reads, so the probe and publish cost more than
   they save. Nested read txns share their parent writer's memo, so they do
   not use it either. ADR-0017 (spilling) would have had to revisit writer
   use anyway: a spilled page is written to the map before commit.

## Implementation

- `zerodb-core::stamps::StampCache`: direct-mapped slots `{seq, key, stamp}`,
  one per pgno hash. Publish takes the slot by CAS on an even sequence
  (`Acquire`), fences `Release`, stores the two words, then stores the next
  even sequence (`Release`). Lookup loads the sequence (`Acquire`), the two
  words, fences `Acquire`, and re-loads the sequence; a hit needs a stable,
  unchanged even sequence and an exact key and stamp. A busy slot is skipped
  on publish; a race or collision is a miss. Loom model L7.
- `btree`: the validating memo-miss arm of `leaf_view_over` /
  `branch_view_over` calls out-of-line `validate_*_miss`, which probes the
  cache with the page's header stamp before the cell walk and publishes after
  it. The memo-hit path and the trusted path are unchanged.
- The cache travels **inside** the txn's memo (`ValidatedPages<'e>`,
  field `shared: Option<&'e StampCache>`), so the tree code still passes one
  pointer, and a plain read txn borrows it from the env: opening a txn
  touches no shared refcount and its memo has no drop glue. Write txns, the
  env-owning `static_read_txn` (Meilisearch opens those only off its search
  path: render and chat routes, dynamic search rules, one scheduler call)
  and the trusting policy carry `None`.
- Slots are indexed by the **pgno itself** (masked), not a hash: pages the
  file holds side by side, such as a bulk-loaded tree's leaves, sit in
  adjacent slots, so a scan probes the table almost sequentially.
- Shapes measured and dropped on the way (bench server, 2026-09-29/30):

  | shape | cost |
  |---|---|
  | `Arc<StampCache>` cloned into every txn's memo | `env/txn/ro_begin_abort` +17 % (~14 ns/txn), `rw_empty_commit` +12 % |
  | writers consulting the cache | `put/val/v8` +12 %, `put/val/v256` +4 % |
  | table zero-filled in one piece on first publish | `env/open/reopen` +6 % |
  | two-word `(memo, cache)` handle through the tree code | 4–7 % on memo-hit and full-scan rungs, codegen-units=16 |
  | owning variant (`Arc`) kept for `static_read_txn` | `ro_begin_abort` +7 %, `env/stat/non_free` +6 % (drop glue) |
  | hashed slot index | `scan/full/*` +5 %: one random table line and page per leaf (perf: −15 % instructions, +17 % L1 misses per scan) |
  | cache for branch pages only | every gain gone: branches are memo hits after a txn's first lookups; the win is on leaves |

- Tests: `zerodb/tests/page_version_identity.rs` (the identity; short read
  txns against a model), `btree::tests::stamp_cache_serves_the_exact_page_version_across_txns`,
  `stamps::tests`, loom `loom_stamp_cache_never_mixes_publishes` (exhaustive,
  ~10 min; mutation-checked: without the reader's sequence re-check it finds
  a mixed pair in under a second).
