# ADR-0037: Bounded free-page records

- Status: Accepted (Option C, maintainer decision 2026-11-02); implemented
- Related issue: #30 (also #146, cost 3; #32)
- Date: 2026-11-02

## Context

### What ZeroDB does today

A write transaction that frees pages records them under its own transaction
id `F`. Up to `(page size − 176) / 8` ids (490 at 4 KiB pages, 2,026 at
16 KiB) ride in its meta page (SPEC 05 §2a, ADR-0022). A longer list goes to
the free-page database (the GC tree) as **one** record, key `BE(F)`, value a
count and the sorted ids (SPEC 05 GC-1..5). A record longer than
`max_node_size` is stored on an overflow run (GC-5): about one page of run
per 508 ids at 4 KiB pages, per 2,044 at 16 KiB.

A later write transaction draws pages from the oldest record the reader gate
admits (GC-18). `gc_reclaim` (`crates/zerodb-core/src/rwtxn/alloc.rs`)
decodes and validates the **whole** record into a `Drain` the first time the
transaction touches it, and at commit `freelist_save`
(`crates/zerodb-core/src/rwtxn/freelist.rs`) rewrites the **whole**
remainder under the same key (GC-20): a new overflow run of the remainder's
length, with the old run freed into the committing transaction's own list.
Nothing of the decoded record survives the transaction. So every commit that
draws even one page from a record of `c` ids reads `8c` bytes, validates `c`
ids, writes `8c` bytes into freshly allocated overflow pages and frees as
many, until the record is used up. SPEC 05 §8 (GC-25..27) documents this as
the known weakness of one huge freeing transaction.

Two Meilisearch cases produce such records: a mass delete or a settings
change that makes milli clear and rebuild whole databases in one
transaction, and any large write while a snapshot or dump keeps a reader
open (the record piles up behind the gate, #32).

### What LMDB does

Read for the algorithm only, from the Meilisearch fork (`mdb.c`,
`mdb_page_alloc`, `mdb_freelist_save`, `mdb_txn_end`).

1. The freeing transaction writes its whole list as one record under its id,
   as ZeroDB does. A huge list is one multi-page overflow value, which needs a
   contiguous run of that many pages.
2. A later transaction that needs pages merges the **whole** oldest record
   into its in-memory free list (`me_pghead`) and marks it consumed
   (`me_pglast`).
3. At commit it deletes every record it merged and writes what is left of the
   in-memory list back **split into records of about one page each**
   (`maxfree_1pg` ids), under keys counting down from `me_pglast`. Every key
   at or below `me_pglast` is free (all those records were merged and
   deleted) and safe (all of them already passed the gate). When the keys run
   out (`head_id` reaches 1) the remaining ids are spread over the keys there
   are, as multi-page records.
4. The in-memory list is dropped at the end of every write transaction.

So LMDB pays for a huge record **once**, in the first transaction that draws
from it; after that each small commit reads and rewrites one page-sized
record, as long as there are enough free keys below the record's own.
ZeroDB pays for it **in every commit** until the record is used up.
The weakness SPEC 05 §8 calls LMDB parity is LMDB's only for the freeing
transaction and the first draw; for the commits after that, ZeroDB is behind
LMDB.

libmdbx, an LMDB fork, goes further with its BigFoot mode
(`MDBX_ENABLE_BIGFOOT`, on by default): the freeing transaction itself
splits a long list of retired pages into a chain of records under
consecutive transaction ids, so no record needs a long overflow run.

### What it costs

`crates/zerodb-oracle/examples/freelist_big_record.rs` (added with this ADR)
clears a database of `pages` overflow pages in one transaction, then times
a series of single-put write transactions on another database, for LMDB (heed 0.22.1
over the Meilisearch fork) and ZeroDB (heed-zerodb), both at the OS page
size. `pages = 0` is the control.

Measured on macOS aarch64, a laptop with 16 KiB pages, main at `3c4f37f`,
2026-11-02, while other builds were running (load average about 40): the
absolute times are noisy and indicative only; the shape is clear. At 16 KiB
pages a record of `c` ids takes about `c / 2,044` overflow pages, so the four
deletes leave a record of 3, 12, 47 and 94 overflow pages.

**Median small commit, `NO_SYNC`, 2,000 empty write transactions first**
(`freelist_big_record 500 0 2000 0 6000 24000 96000 192000`; the median over
three rounds of each round's median of commits 3 to 500, in µs):

| Pages freed by the delete | LMDB | ZeroDB | ZeroDB / LMDB |
|--:|--:|--:|--:|
| 0 (control) | 9.5 | 9.6 | 1.0 |
| 6,000 | 18.9 | 35.5 | 1.9 |
| 24,000 | 20.0 | 84.5 | 4.2 |
| 96,000 | 22.9 | 303 | 13 |
| 192,000 | 32.7 | 636 | 19 |

ZeroDB's small commit grows linearly with the size of the earlier delete;
LMDB's stays nearly flat. LMDB pays once instead: its second commit after
the delete (the first its gate lets draw from the record) took 30, 67, 433
and 1,173 µs for the four sizes, then fell back to the flat line.

**Without the warm-up** (a fresh environment, so the delete is transaction 3
and LMDB has only keys 1 to 3 to re-save the remainder under; same command
with `warmup = 0`), LMDB's median small commit is 81 µs at 96,000 pages and
199 µs at 192,000: the remainder is spread over three long records and LMDB
too pays per commit, about a quarter of ZeroDB's cost. That is the free-key limit
of Option B below, observed in LMDB itself.

**Durable commits** (default flush, 100 small commits, one round,
indicative only): median 5.2 ms LMDB / 8.0 ms ZeroDB with no delete, 4.5 /
22.5 ms after 24,000 pages, 11.2 / 55.3 ms after 96,000. Each ZeroDB commit
writes the record's new overflow run, so the extra pages go through the
flush too.

The large delete itself is not the problem for ZeroDB: its clearing commit
took 3 ms at 96,000 pages and 14 ms at 192,000, where LMDB's took 1.2 s
and 6.7 s (not investigated; LMDB has to store its own huge list as one
overflow value, which needs a contiguous run that long).

### Interaction with recent changes

- **Meta annex (ADR-0022).** Only lists longer than the annex reach the tree,
  so the records this ADR bounds are exactly the long ones. The tree arm of
  `freelist_save` writes the transaction's own list merged with the carried
  annex remainder (GC-30), all-or-nothing (GC-32).
- **Run search (ADR-0029).** A run of `n > 1` pages is first searched inside
  one record, then across the gated records merged oldest first, at most
  `n * 60` records (GC-21). Splitting a record cuts it into several records;
  a run that crosses a cut is still found by the cross-record step.
- **#146, cost 3.** "Small commits that follow a large delete decode and
  rewrite a free-list record, about 10 allocations each." The decode and the
  rewrite are this issue's O(`c`) work; the allocation count is #146's.
- **GC-13 (v), the own-entry size guard** (ADR-0029 outcome). It exists
  because a record on an overflow run can sit at a page-count boundary during
  `freelist_save`. Records that never leave the leaf remove that case.

## Options

Each option is judged on the same four questions: the on-disk format and its
compatibility plan, the GC invariants of SPEC 05 (above all, never hand out a
page a live reader or the crash-fallback meta can still reach), the
interaction with the annex, the run search and #146, and how the crash
harness and the differential tests cover it.

### Option A — Keep the format; document the gap

Change nothing; amend SPEC 05 §8 to say ZeroDB pays per commit where LMDB
pays once.

- Format: none.
- Pros: no risk.
- Cons: the measured cost above stays, for every commit until the record is
  used up, in exactly the cases Meilisearch hits (mass delete, settings
  change, dump in progress).

### Option B — LMDB's technique: re-split a drained record under older keys

Keep the format. When `freelist_save` rewrites a drained record's remainder
(GC-20) and the remainder is longer than one inline chunk (`K` ids, below),
delete the record and write the remainder as chunks of at most `K` ids under
keys below the smallest key in the GC tree, counting down, lowest ids under
the lowest key. As in LMDB, when the free keys run out the last chunk takes
the rest.

- **Format.** Unchanged bytes: an 8-byte big-endian key and a page-id list.
  What changes is the key's meaning: it is a transaction id at or before the
  one that freed the pages, no longer exactly that one (GC-1, SPEC 02 §7
  wording). No `FORMAT_VERSION` bump. A file written by the new engine opens
  in the old one and the other way round: the old engine reads re-keyed
  chunks as ordinary records, gated more strictly than needed.
- **GC invariants.** A key below the tree's smallest key is below `F`, which
  passed the gate (`F ≤ oldest_reader()`), and the gate only moves forward,
  so every chunk stays reclaimable and no reader older than its key exists.
  The pages are absent from the base snapshot's trees and from every later
  one, and the crash-fallback meta still lists them under the old record
  (TXN-62 argument unchanged). INV-23 (`key ≤ live txnid`) holds. No key
  collides: a future transaction writes keys at or above its own id, and the
  keys used are below every existing one. GC-19's pick (smallest id of the
  oldest record) is unchanged when the drawn record was the oldest; it can
  differ when an `n > 1` draw took pages from a newer one.
- **Free keys run out** in a young environment: an environment at
  transaction 40 has 39 keys below a record keyed 40. The measurement above
  shows it in LMDB itself (three free keys: a per-commit cost about a
  quarter of ZeroDB's today, instead of flat). Meilisearch index
  environments see one write transaction per indexing batch, so a settings
  change early in an index's life can leave few keys. Then the remainder
  degrades to a few long records, as in LMDB.
- **Annex, run search, #146.** The annex is untouched. The cross-record run
  search finds runs across chunks. #146 cost 3 is cut to one chunk per
  commit after the first draw. The first draw still reads, validates and
  rewrites the whole record, and the freeing transaction still writes one
  overflow value that needs a contiguous run of `c / 508` pages (4 KiB).
- **Tests.** The crash harness needs a workload with a large delete followed
  by small commits; the change is inside C1, so the existing atomicity
  argument (GC-14) covers it. `gc_churn_parity` and `freelist_big_record`
  compare file size and per-commit time with LMDB, which now uses the same
  technique.
- Pros: the LMDB technique for the path (no novel lever), no format change,
  compatible both ways, smallest diff in `freelist_save`.
- Cons: bounded only after one O(`c`) commit; the freeing transaction's own
  record is not bounded; free keys can run out; GC keys stop naming the
  freeing transaction, which weakens what `check` and `dump` can report.

### Option C — Composite key: transaction id plus a sequence number

Bound every record when it is written. A GC key becomes 16 bytes:
`BE(F) ‖ BE(seq)`, where `F` is still the transaction that freed the pages
and `seq` orders the chunks of its list. A chunk holds at most `K` ids, the
most that fits inline in a leaf (`8 + 16 + 8 + 8K ≤ max_node_size`):
`K = 249` at 4 KiB pages, 1,017 at 16 KiB, 4,089 at 64 KiB. The freeing
transaction cuts its sorted list into consecutive slices, so the chunks of
one `F` partition the page-id line into ranges. A GC record never spills to
an overflow run.

`seq` is not a counter but the low end of the chunk's range: 0 for the first
chunk, and for each later one the lowest id it held when it was cut. A
chunk keyed `s` holds only ids in `[s, s′)`, `s′` being the next chunk's
key. A counter would need renumbering whenever a chunk has to be split
during the save that writes it (below); a range key always leaves room for
the new chunk's key, the id it is cut at.

- **Format.** `FORMAT_VERSION` 2 → 3. SPEC 02 §7 and SPEC 05 GC-1/GC-2: the
  key is 16 bytes, big-endian `F` then big-endian `seq`; memcmp order is
  `(F, seq)` order, so the reclaim scan stays one forward cursor, oldest `F`
  first. GC-5 becomes "a GC value never exceeds `K` ids and is always
  inline". The value encoding (count, then sorted ids) is unchanged.
- **Compatibility plan.** As ADR-0022 did for format 2: a version-2 file is
  refused at open with `BadVersion`, and ZeroDB has no users whose files
  must carry over. A file that must survive goes through `zerodb-tools dump`
  with the old build and `load` with the new one; `migrate-from-lmdb` writes
  through the engine and needs nothing. If a lazy in-place upgrade is ever
  wanted, it is possible: an 8-byte key `BE(F)` is a prefix of every
  `BE(F) ‖ BE(seq)`, so it sorts just before the chunks of the same `F`, and
  a version-3 engine could read old records as single unbounded records and
  re-split them on their first rewrite. This ADR does not propose it.
- **GC invariants.** The gate reads `F` from the first 8 bytes, so GC-18 and
  the TXN-20/TXN-62 proofs apply word for word: every chunk of `F` holds only
  pages freed by `F`. GC-19 is unchanged: the smallest id of the oldest `F`
  is the first id of its first live chunk. GC-20 applies per chunk: a
  partly drained chunk is rewritten under its own key, an emptied one
  deleted. GC-4 and INV-24 are unchanged. New check invariant (INV-29): a
  chunk keyed `(F, s)` holds at most `K` ids, all in `[s, s′)` where `s′`
  is the key of the next chunk of `F`. After its commit a chunk only ever
  loses ids, so the invariant holds for the life of the record; draws
  empty the low chunks first and delete them, so the first remaining chunk
  of an `F` is keyed 0 only until then.
- **`freelist_save`.** Only the tree arm of step (c) changes: it writes
  `ceil(len / K)` chunks under `(W, 0..)` instead of one record. Inside one
  save, a later iteration may change the set (a GC page freed, an annex or
  drain id drawn); the ranges are fixed at the first tree-arm put, so an
  iteration rewrites only the chunks whose ids changed, splits one that
  grows past `K` at a new key inside its range, and deletes one that
  empties. If the first chunk empties, the next one is re-keyed 0 (a delete
  and a put of the same ids), so an id that joins the set later still has
  a range. With no
  overflow runs, no allocation inside `freelist_save` asks for more than one
  page: the GC-12 loose-run rule and the GC-13 (v) size guard lose their
  reason to exist, and GC-13's termination argument must be redone for chunk
  splits and merges (a leaf split allocates one page and may move one id out
  of the set; the hard iteration guard stays).
- **Annex (ADR-0022).** Unchanged: the annex arm is taken when the merged set
  fits the annex, else the tree arm writes chunks. GC-29's "no tree entry for
  `N`" becomes "no chunk of `N`". GC-32 (all-or-nothing) is kept.
- **Run search (ADR-0029).** Chunks of one `F` hold consecutive slices of
  its list, so a run that crosses a cut lies in two adjacent records, read
  one after the other by the cross-record step. The step reads at most
  `n * 60` records; with chunks, an old huge transaction fills that budget
  with chunks of `K` ids where today it is one record of all its ids, so the
  search sees fewer ids before it gives up and extends the file (open
  question 4). The draft proposes cutting at a gap between ids where one
  exists in the last eighth of a chunk, so most runs stay in one chunk (open
  question 3).
- **#146, cost 3.** A small commit after a large delete reads, validates and
  rewrites at most `K` ids (2 KiB at 4 KiB pages) and copies one leaf plus
  its branch path, instead of the whole record and its overflow run. The
  allocation count of cost 3 (the `Drain` vectors) is unchanged and stays
  #146's.
- **Secure.** A record's decode is bounded by `K` ids, so a hostile file can
  no longer make the writer allocate and validate an arbitrarily long list
  for one record; a chunk longer than `K`, a key of the wrong length or a
  `seq` order violation fails with `MdbError::Invalid` like any corrupt
  free list.
- **Tests.** The crash harness re-pins its seeds (allocation order and the GC
  tree's page count change) and gains a workload that clears a database of
  several thousand pages and then commits small writes, in every durability
  mode, checking INV-22..29 after every recovery. A property test drives
  `freelist_save` across chunk-count boundaries (the case that GC-13 (v)
  was added for, moved to chunk splits). The decoder fuzz target learns the
  16-byte key. The differential tests compare only observable data and file
  size bands with LMDB, so they need no change; `freelist_big_record` and
  `gc_churn_parity` give the three-column comparison (LMDB, ZeroDB before,
  ZeroDB after).
- Pros: bounded from the freeing transaction on, including the first draw and
  the record write itself (no long contiguous run needed to store a free
  list); keys still name the freeing transaction; no dependence on free key
  space; removes the overflow case from `freelist_save`.
- Cons: format bump; beyond LMDB (a novel lever, justified by the cost
  table above and by LMDB's own split of its leftovers); a large GC tree of
  small records instead of one record (more leaf pages for the freeing
  transaction: one leaf per one or two chunks).

### Option D — libmdbx BigFoot: a chain of records under consecutive transaction ids

The freeing transaction `W` stores its list as chunks keyed `W, W+1, …,
W+k−1` and advances the transaction id counter past them, since a key must
not be reused by a later transaction.

- **Format.** Keys stay 8 bytes; values unchanged. The meta gains nothing,
  but the transaction id sequence gains gaps.
- **GC invariants.** Safe: a chunk keyed `W+j ≥ W` is gated more strictly
  than the pages need.
- Cons that rule it out for ZeroDB:
  - Transaction ids are observable: heed's `RoTxn::id()` and
    `Env::info().last_txn_id`, which `env_info_differential.rs` pins exactly
    against LMDB. Default behavior must match LMDB.
  - The meta slot is `txnid & 1` (TXN-63): a jump of even length would make
    the new meta overwrite the base meta, the crash-fallback; every jump
    must keep the parity alternating.
  - Pages are reclaimable only once the gate passes the last key of the
    chain, so a huge freeing transaction delays its own pages' reuse by
    `k − 1` transactions.

### Option E — Encode free pages as extents

Store `(start, length)` pairs instead of single ids. A mass delete of a
freshly loaded database frees long runs, so records shrink a lot.

- Orthogonal to the bound: a fragmented list of single pages is as long as
  today. It would also change every PIL consumer (run search, drains, the
  annex). Worth its own measurement later; not an answer to #30.

## Scorecard

Against today's engine; "LMDB" says where LMDB stands on the same line.

| | A (keep) | B (re-split, older keys) | C (composite key) | D (BigFoot) |
|---|---|---|---|---|
| Faster | small commits up to 19× LMDB (measured) | after one O(`c`) commit, as LMDB | from the freeing commit on; beyond LMDB | as C, but reuse delayed |
| Reliable | no change | new key-space edge (keys run out) | new chunk logic in C1; GC-13 (v) removed | txnid gaps, slot-parity hazard |
| Durable | no change | inside C1, GC-14 unchanged | inside C1, GC-14 unchanged; seeds re-pinned | meta-slot hazard |
| Secure | no change | no change | per-record decode bounded by `K` | no change |
| Format | none | none (meaning of keys) | version 3, v2 refused | none (txnid gaps observable) |
| LMDB parity | below LMDB | LMDB's technique | beyond LMDB, observably identical | diverges observably |

## Decision

**Option C**, the composite key: maintainer decision 2026-11-02, which
approves the format change (`FORMAT_VERSION` 3). The draft's reasoning,
kept for the record:

It bounds the records where they are created, so neither the freeing
transaction, nor the first draw, nor any later commit pays for the whole
list, and it does so without the free-key scarcity of Option B, which bites
early in an environment's life (the Meilisearch settings-change case). It
keeps the key's meaning, so the gate proofs carry over unchanged, and it
removes overflow values from the free-page database, which deletes a
subtle part of `freelist_save` (the GC-13 (v) guard) rather than adding one.
The format bump is cheap now (no users) and expensive later.

Option B was the fallback had a format bump not been wanted: LMDB's own
technique, no format change, and it already removes the per-commit cost
after the first draw.

## Consequences

- SPEC 02 §7 (GC key), SPEC 05 GC-1, GC-2, GC-5, GC-11..13 (step (c) tree
  arm, the termination argument, GC-13 (v) removed), GC-20 (per chunk),
  GC-21 (run search over chunks), GC-25..27 (§8 rewritten: what remains of
  the weakness), GC-29 wording, INV-23 (16-byte key), new INV-29. SPEC 06:
  no change in kind; the crash harness covers the new C1 work.
- `FORMAT_VERSION` 3; `zerodb-tools` `check`, `dump` and `stat` learn the
  key.
- Gates: the full check list in `CONTRIBUTING.md`, the crash harness in every
  durability mode, miri on `zerodb-core`, fuzz, and the three-column bench
  (`freelist_big_record`, `put/gc/drain_big`, `commit/batch/*`,
  `free_run_churn`, `gc_churn_parity`).
- Allocation order changes where a run crosses a chunk cut; any test that
  pins page numbers against ZeroDB (never against LMDB) may need re-pinning.

## Open questions for human review

1. ~~Option C (format 3, bounded at write) or Option B (format 2, LMDB's
   re-split after the first draw)?~~ Closed with Option C (2026-11-02).
2. Refuse version-2 files at open (as format 2 did), or carry the lazy
   in-place upgrade described under Option C?
3. Chunk size: the most that fits inline (`K` above), or smaller so a leaf
   holds several chunks; and whether to cut at a gap between ids so most
   runs stay inside one chunk.
4. Should the run search's `n * 60` record bound count chunks or distinct
   freeing transactions, now that one transaction can own many records?
