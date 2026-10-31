# ADR-0018: Env-wide validated-pages cache across transactions

- Status: Accepted (approved by the maintainer, 2026-09-29); implemented and measured 2026-09-30
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
  (D-001), but it must be stated. *(Amended 2026-10-13: ADR-0023 retired
  D-001. The cache stays on with several processes, because another
  process's rewrite of a page carries a newer stamp; ADR-0023 §5 and SPEC 04
  TXN-38 state the argument.)*
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

## Amendment: publish at commit; writers probe but never publish (2026-10-01)

Approved by the maintainer, 2026-10-01, as part of the
one-operation-txn cost work.

### Problem

The cache only learned a page version when a *reader* validated it. A
workload of one-operation txns (YCSB A; rust-storage-bench) rewrites the
root-to-leaf path on every commit, so every page a reader or the next writer
touches is freshly stamped and **misses**: the census showed one write txn at
15.2 µs vs LMDB's 5.9 µs with `BranchRef::new` + `LeafRef::new` ≈ 9.7 % of
the process (~1.6 µs/txn) re-validating pages the previous commit had just
written, and the one-get read txn paying the same walk on its first view of
each fresh page.

### Change

1. **The committer seeds the cache.** Every frame commit step C2 writes is
   engine-authored, carries the committing txn's stamp (`RwTxn::touch`
   restamps COW copies; fresh and GC pages are initialized with it), and is
   final once the commit has succeeded. After C5 (both barriers done, the
   commit can no longer fail) and before C6 publishes the snapshot, the
   committer publishes `(pgno, kind, txnid)` for each written **tree** frame
   (leaf or branch; overflow frames are not cacheable). Trusted-mode envs
   skip it (nothing probes the cache there).
2. **Write txns probe the cache but never publish from the miss arm.** The
   memo gains a `for_writer` constructor: `shared` is set, a new
   `publish_shared` flag is false (readers carry true). Nested read txns
   share the parent writer's memo and inherit the same behavior.

### Why it is safe

The soundness invariant is unchanged: an entry `(pgno, stamp)` is published
only for a byte image that is **committed and final**, so no two images the
env can expose ever share a pair.

- **Failed or aborted commits publish nothing.** The publish sits after C5.
  A txn that aborts, or whose commit fails at C1–C5, reuses its txnid (module
  doc, TXN-2), and the next writer may write *different* bytes at the same
  pgnos under the same stamp — which is exactly why no publish may happen
  before the commit is irrevocable.
- **Spilled pages are not published.** ADR-0017 writes spilled pages to the
  file mid-txn under the txn's stamp and may rewrite them in place
  (spill → unspill/touch → C2 rewrites the frame, same pgno, same stamp), so
  a mid-txn image is not final. Spilled pages absent from the dirty store at
  commit are simply not published; their first reader revalidates as before.
- **Writer probes cannot false-hit.** The cache only ever holds stamps of
  *successfully committed* txns, and the current writer's txnid is strictly
  greater than every committed txnid, so a probe on the writer's own spilled
  pages (stamp = its txnid) always misses; probes on committed map pages hit
  only the exact committed image.
- **Writer publishes nothing from the miss arm.** If the miss arm published,
  a writer that validated its own spilled page would record a non-final
  image (see above), and an aborted writer's publishes would survive the
  abort into a reused txnid. `publish_shared = false` closes both.
- **The committed image is valid without a walk.** A published frame skipped
  the reader's cell walk forever after. The frame is the output of this
  engine's page encoders (the same trust the dirty-frame
  `new_prevalidated` arm already extends, and less than LMDB extends to
  every page); a hostile *file* still validates on first view because the
  cache starts empty at open and only commits performed by this process
  publish.

### What was considered and not done

- Publishing spilled pages at commit (re-reading their kinds from the map):
  correct for the spilled-and-not-retouched subset, but it only matters for
  huge txns whose first re-read is a vanishing cost; not worth the extra
  commit-path code.
- A writer-private memo carried across txns under the write lock: strictly
  weaker than reading the shared cache (same hits, plus invalidation
  bookkeeping the stamp key already does for free).

### Tests

- `zerodb/tests/page_version_identity.rs` gains an abort-after-spill +
  txnid-reuse scenario over a tiny dirty limit (the model and ledger checks
  catch a publish of non-final bytes).
- `btree::tests::writer_memo_probes_the_cache_but_never_publishes` pins the
  probe/no-publish split.
- The existing identity ledger, loom model L7 (protocol untouched) and crash
  harness still apply; `just crash-test-quick` and `just stress` run because
  the commit pipeline gained a step.

### Measurements

Recorded in the perf ledger and the commit body (bench server, x86-64,
4 KiB): see `short_txn_census` three-column table and the `get/put/commit`
ladder A/B.

## Amendment: a table that covers the file — one-word slots, mapped by pgno (2026-10-31)

- Status: **Proposed** — awaiting maintainer approval; no implementation
  before it.

### Problem

Answer 2 above fixed the table at 64 Ki slots and deferred scaling "until a
measurement shows misses from collisions". The 2026-10-29 Graviton4 profile
(`c8g.xlarge`, `ycsb_a_census` `mix`: the public suite's YCSB A shape, 10M
16-byte keys, 100-byte values, one txn per op) is that measurement:

| | LMDB | ZeroDB | ZeroDB, trusted file |
|---|---:|---:|---:|
| read p50, 10M keys (1.2 GB, ~300k pages of 4 KiB) | 0.95–1.15 µs | 1.39–1.54 µs | ~1.09 µs (public suite) |

`validate_leaf_miss` + `validate_branch_miss` are 5.8 % of the mix's cycles.
The cause is capacity, not conflict: ~300k pages compete for 64 Ki slots
(slot = pgno mod 64 Ki), so about 4.6 pages share each slot. Under uniform
random reads a probe hits only if its page was the last one published to the
slot, about 64 Ki / 300k ≈ 22 % of the time, and every miss walks the leaf's
cells. Branch pages share slots with leaves, so even the ~2.5k branches are
evicted by leaf publishes and miss too. The trusted-file column bounds what
removing the walk can give: about 0.3–0.4 µs of p50.

### Options

**A — Raise the fixed size, keep the 32-byte seqlock slot.** Simplest diff:
`SLOTS` from 2^16 to 2^22 or 2^23; the loom model and protocol are unchanged.
Costs 32 B per page in use (0.78 % of a 4 KiB-page file: 9.4 MiB for this
1.2 GB file), up to 256 MiB per env at 2^23; the per-env chunk directory grows
to 8 Ki entries (~190 KiB at open) unless it gains a second level. Each probe
stays four loads and an `Acquire` fence, and each publish a CAS (an outlined
`__aarch64_cas8_acq` call on the default aarch64 target, per the same
profile).

**B — One-word slots mapped by pgno, lazily allocated (recommended).** See
below. 8 B per page in use (0.2 % of a 4 KiB-page file), one load per probe,
one store per publish, no seqlock.

**C — Set-associative at the same 2 MiB.** Associativity cures *conflict*
misses; these are *capacity* misses (more pages than entries), which no
replacement policy at 64 Ki entries removes. Rejected.

**D — Size the table from `map_size` at open.** `map_size` says nothing about
the pages in use: the census and the public-suite harness map 10^12 bytes,
Meilisearch maps several TiB. Sizing from the file at open and re-sizing as
`last_pg` grows needs a table that is swapped while readers probe it (a lock,
or `unsafe` reclamation in `zerodb-core`). B gets growth with the file for
free: slots are allocated where pgnos land, and pgnos only grow with the
file. Rejected as a separate mechanism.

### Option B in detail

**Index.** Slot = `pgno mod 2^23` — the pgno itself for every page of a file
up to 2^23 pages (32 GiB at 4 KiB pages, 128 GiB at 16 KiB). Below that size
two pages never share a slot, so the hit rate is limited only by pages that
were never validated (or were rewritten) since open. Above it, pgnos wrap and
collide exactly as today, 2^23 apart. Keeping the identity map keeps the
property measured in the original decision: a scan over a bulk-loaded tree
probes adjacent slots, now eight per cache line instead of two.

**Lazy allocation, three levels.** A fixed top array of 32 entries (allocated
at open, ~0.8 KiB) → 64-entry directory blocks (1.5 KiB, allocated on first
publish under them, each covering 256 Ki pages) → 4096-slot chunks (32 KiB,
allocated on first publish in them). All three use `OnceLock`, as the chunks
do today: no `unsafe`. A probe that finds an unallocated level is a miss. The
extra directory level is one more dependent load, of a line that stays in L1
on any hot path.

**Slot word.** One `AtomicU64`:

```
bit 63 ............ 14 | 13 ..... 2 |   1  | 0
     stamp (50 bits)   | pgno >> 23 | kind | 1
```

`pgno >> 23` needs 12 bits because every pgno is below
`MAX_MAP_SIZE / MIN_PAGE_SIZE` = 2^47 / 2^12 = 2^35. Bit 0 is set in every
published word, so an all-zero (never written) slot matches nothing. Within
one slot (fixed `pgno mod 2^23`), the word determines `(pgno, kind, stamp)`
uniquely, so word equality is exact key-and-stamp equality: no hash, no
truncation. A triple outside the domain — pgno ≥ 2^35 or stamp ≥ 2^50 — is
never published and its lookup answers `false` without a probe, which is
sound (a miss revalidates). 2^50 commits is 35 years at a million commits a
second; past it the cache would stop learning, never mis-answer.

**Protocol.** Publish: one `Release` store of the word. Lookup: one `Acquire`
load, compared with the word the probe expects. The seqlock existed only to
keep two words coherent (the "Coherent entries" requirement under Option A
above): a lookup must never pair one publish's key with another's stamp. With
one word, every value a load can return is a value one publish stored whole,
so the requirement holds by construction. Two racing publishers leave the
last store; both are true facts. The `Release`/`Acquire` pair keeps the
existing statement that the validation (or commit) preceding a publish
happens-before a hit; a hit's page bytes are additionally synchronized by
the reader's own snapshot pin (TXN-18/19). On aarch64 a probe is one `ldar`
(was `ldar`, two `ldr`, `dmb ishld`, `ldr`) and a publish one `stlr` (was a
CAS, an outlined call, and three stores).

**Everything else is unchanged:** the page-version identity, who publishes
(readers from the miss arm, the committer after C5, never a writer's miss
arm), who probes, trusted mode skipping it, and the stated limit for bytes
rewritten underneath the env by something other than a ZeroDB commit.

### Memory (Option B)

8 bytes per page in use, rounded up to 32 KiB chunks, plus 1.5 KiB per
256 Ki pages of directory:

| env | today | Option B | Option A at 2^23 |
|---|---:|---:|---:|
| at open, nothing published | 1.5 KiB | 0.8 KiB | ~190 KiB (or two-level) |
| a few pages | 33.5 KiB | 34 KiB | ~220 KiB |
| 1.2 GB file, 4 KiB pages (~300k pages) | 2 MiB | 2.3 MiB | 9.4 MiB |
| 10 GiB file, 4 KiB pages | 2 MiB | 20 MiB | 80 MiB |
| ≥ 32 GiB file, 4 KiB pages (cap) | 2 MiB | 64 MiB | 256 MiB |

At 16 KiB pages every row below the second is a quarter of that. Chunks stay
allocated until the env closes; their number follows the file's high-water
pgno.

### Tests and model

- Loom L7 is rewritten for the one-word protocol with the same shape
  (two publishers of distinct versions, a reader asserting that no
  never-published triple hits, exactly one entry at quiescence); it stays
  in `just loom`.
- Unit tests: word encoding exact at the domain edges (pgno 2^35 − 1, stamp
  2^50 − 1, both kinds) and refusal past them; identity below 2^23 (no two
  pages of a file under the cap evict each other); pgnos 2^23 apart sharing a
  slot and distinguished by the word; lazy allocation of directory blocks and
  chunks only where publishes land; the concurrent publish/lookup test.
- `zerodb/tests/page_version_identity.rs` and the btree cache tests apply
  unchanged (the identity they pin is the soundness argument).
- SPEC 04 TXN-38's shape sentence and the `stamps.rs` module docs are
  updated in the same change.

### Measurement plan

Spike on a Mac, 2026-10-31: inconclusive. Another workload shared the machine
and every variant (64 Ki seqlock, 4 Mi seqlock, one-word 8 Mi) swung between
1.9 and 3.7 µs read p50 from round to round, with I/O-sized p99s. No local
number is quoted. The gate runs on a quiet Graviton4 `c8g.xlarge`, three
columns (LMDB / ZeroDB before / ZeroDB after):

- `ycsb_a_census` `read` and `mix`, 10M items, two reps, engine order
  rotated: read p50/p99 and, for `mix`, write p50 (the committer's seeding
  cost).
- `scripts/bench-ab.sh '^(get/|scan/|seek/|env/)'`, 3 rounds: the probe on
  every short read txn, the scan's sequential probe, and `env/open/reopen`
  for the at-open cost.
- Memory: allocated cache bytes at the end of the 10M census.

Kept if the census read p50 moves toward the trusted-file figure and no
ladder rung regresses beyond 3 %.

### Open questions

1. Option B over A (a new, simpler protocol and a 4× smaller table, against
   A's untouched loom-checked protocol)?
2. The cap: 2^23 slots (64 MiB at most, files up to 32 GiB at 4 KiB pages
   collision-free), or 2^22 (32 MiB, 16 GiB)?
