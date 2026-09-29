# ADR-0018: Env-wide validated-pages cache across transactions

- Status: Draft — needs maintainer approval (reader-side concurrency)
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

## Decision (proposed)

Option A, measured on the one-op-per-txn census, the `get/*` ladder (a fresh
txn per iteration), the YCSB runs and Meilisearch search; kept only if the
long-txn rungs stay flat.

## Open questions for human review

1. Is (pgno, stamp) identity acceptable as the soundness argument, with the
   stated single-process assumption?
2. Cache size: fixed (e.g. 64 Ki entries, 1 MiB per env) or scaled with the
   map size?
3. Should write txns consult it too for pages they only read?
