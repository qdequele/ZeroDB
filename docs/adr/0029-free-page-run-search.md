# ADR-0029: Free-page run search across freed-page records

- Status: Accepted (Option C, maintainer decision 2026-10-19), implemented
- Related issue: #29 (also tracks the cross-record part of closed #31);
  divergence D-027 ((b) resolved by Option C; (a) proposed)
- Date: 2026-10-19

## Context

### What ZeroDB does today

A write transaction that frees pages records them under its own transaction
id: in its meta page when the list is small enough (format v2, SPEC 05 §2a,
ADR-0022), otherwise as one record of the free-page database (the GC tree,
SPEC 05 §1). A later write transaction draws pages back once no live reader
can see them (SPEC 05 GC-18). Its in-memory bookkeeping lives on `RwTxn`
(`crates/zerodb-core/src/rwtxn.rs`):

- `drains: BTreeMap<u64, Drain>` maps each record it drew from (keyed by the
  freeing transaction id) to the ids still unused, a consumed-prefix view so a
  front draw is O(1) (GC-20);
- `reclaimed: HashSet<u64>` holds every page it handed out (the loose-page
  classification in `free_page` and the TXN-62 debug assert);
- `annex: Drain` is the base meta page's freed list (GC-30/31).

`allocate(n)` (GC-16) tries the loose list, then `gc_reclaim`, which walks the
GC tree from its oldest record and returns the first record whose remaining
ids hold a run of `n` contiguous pages (`find_run`), then the annex pool, then
extends the file. A run is searched **inside one record at a time**. At
commit, `freelist_save` rewrites each drained record's remainder under its
own key (GC-20) and stores the transaction's own freed list.

### What LMDB does

The reference is the Meilisearch fork (`mdb.c`, `mdb_page_alloc`,
`mdb_freelist_save`, `mdb_find_oldest`), read for the algorithm only.

1. **One sorted in-memory free list.** The writer keeps a single array of
   reusable page ids (`me_pghead`), sorted in descending order, plus the id of
   the newest record it has merged into it (`me_pglast`). The array starts
   empty in every write transaction.
2. **Lazy, oldest-first refill.** `mdb_page_alloc(n)` first searches the
   array. If it holds no fitting run, the writer reads the next free-page
   database record after `me_pglast` (ascending transaction id), merges its
   ids into the array with a sorted merge, and searches again. It stops at the
   first record that is too recent, at the end of the database, or, for
   `n > 1`, after `n * 60` failed searches, and then extends the file.
3. **Run search over the whole array.** For `n > 1` it scans from the
   smallest id up for `n` consecutive ids, so a run whose pages came from two
   records is found. For `n = 1` it takes the smallest id without reading
   another record when the array is not empty.
4. **Reuse gate.** A record is usable when its transaction id is below
   `oldest = min(live reader snapshots, writer_txnid − 1)`, that is, freed by
   a transaction at least two before the writer when no reader is open.
5. **Save.** At commit, `mdb_freelist_save` deletes every record it merged,
   writes the transaction's own freed pages as one record under its id, and
   writes what is left of the array back into records keyed by the merged
   records' ids (any key up to `me_pglast` is safe, since all of them already
   passed the gate), split into pieces of about one page each, so writing
   them back never needs a multi-page run of its own. The leftover is
   therefore stored **already merged**: free pages from many transactions coalesce over time.

### What the differential test observed

`crates/zerodb-oracle/tests/lmdb_free_run_observation.rs` (heed 0.22.1 over
the fork against heed-zerodb; macOS aarch64 with 16 KiB pages and Linux
aarch64 with 4 KiB pages, identical outcomes, 2026-10-19; page counts below
are the 16 KiB run). Two overflow values `a` and `b` are written side by side, freed, and a value `c`
exactly as long as both is written after one small spacer commit:

| Case | LMDB | ZeroDB |
|------|------|--------|
| `a`, `b` freed by one txn | reused, file +0 | reused, file +0 |
| `a`, `b` (3 pages each) freed by two txns | reused, file +0 | reused, file +0 |
| `a`, `b` (2,090 pages each) freed by two txns | reused, file +1 | **end of file, +4,180** |
| `a` (2,090 pages), `b` (3 pages) freed by two txns | reused, file +1 | **end of file, +2,093** |
| `a`, `b` freed by one txn, no spacer | **end of file, +7** | reused, file +0 |

- The cross-record miss is real but narrower than the issue says: a small
  freed list rides in the meta page and is carried into the next commit's own
  list (GC-30), so two small records end up in one. The miss needs at least
  one record in the GC tree, which takes more freed pages in one commit than
  the meta page holds: `(page size − 176) / 8`, 490 ids at 4 KiB pages and
  2,026 at 16 KiB.
- LMDB's gate is one transaction stricter than ZeroDB's (last row). ZeroDB's
  earlier reuse is safe by its reader protocol (SPEC 04 TXN-20) and only ever
  makes its file smaller. It is filed as D-027 (a) with the run search,
  D-027 (b), since the test pins both.

### What it costs

Measured on macOS aarch64 (a laptop, 16 KiB pages, noisy:
indicative only), main at `f98cd08`, 2026-10-19.

**File growth.** `crates/zerodb-oracle/examples/free_run_churn.rs` drives the
same seeded workload through LMDB and ZeroDB: 20,000 keys, then every commit
rewrites a number of random keys with a new value, inline half the time and
otherwise an overflow value of 2 to 16 pages (Meilisearch documents and
compressed id sets that change size between indexing batches). File pages
are `last_page_number + 1`; used pages are the tree's branch, leaf and
overflow pages.

| Workload | LMDB file (used) | ZeroDB file (used) | ZeroDB / LMDB file |
|----------|------------------|--------------------|--------------------|
| 2,000 rewrites per commit, 200 commits | 121,032 (96,877) | 197,960 (94,098) | **1.64** |
| 2,000 rewrites per commit, 400 commits | 121,713 (96,868) | 295,585 (93,982) | **2.43** |
| 100 rewrites per commit, 400 commits | 101,111 (93,747) | 99,315 (93,550) | 0.98 |
| control: inline values only, 2,000 per commit, 200 commits | 9,405 (6,012) | 7,555 (5,861) | 0.80 |

The same example on Linux aarch64 (OrbStack VM, 4 KiB pages, value lengths
scale with the page size): 2,000 rewrites per commit, 200 commits, LMDB
121,666 file pages (97,238 used), ZeroDB 200,662 (94,412), **1.65×**; 100
rewrites per commit, 200 commits, 0.97×.

With large commits (each frees more than a meta page's list holds, so its
record goes to the GC tree), LMDB's file is flat between 200 and 400 commits
while ZeroDB's keeps growing: 2.1 then 3.1 file pages per used page. With
small commits, whose freed lists merge forward through the meta page, and
with no overflow values at all, ZeroDB is at or below LMDB's size. That
points at the run search. The run search is not proved the only cause until
a fix is measured on the same workload (Option C below is that test). The
existing `gc_churn_parity` test allows ZeroDB up to 1.5× LMDB's size partly
for this gap (its module comment says so).

**Per-commit CPU of the bookkeeping.** `scripts/bench-profile.sh` (macOS
`sample`, 20 s per rung) on `commit/batch/n1` (single-put commits, no
fsync): of 11,431 samples in the timed commit loop, the draw from the meta
page's list (`annex_draw`) takes 150, the GC-tree draw (`gc_reclaim`) 5, the
`Drain` structure 11, and all hash-set work (`reclaimed` among others) 89,
about 2 % in total; `write_frames` and the meta page encode dominate. On
`put/gc/drain_big` (20,000 overwrites drawing from one large record),
`gc_reclaim`, `Drain` and hash-set work add up to 68 of 14,725 samples
(setup included). Ratios to LMDB on the engine_comparison bench, median of
3 rounds, same machine: `commit/batch/n1` 1.10×, `n100` 1.06×, `n10k`
0.79×, `put/gc/drain_big` 0.91×. A "last hit" shortcut for the GC draw was
already measured flat (`benches/results/perf-ledger.jsonl`, 2026-09-28). The
bookkeeping is not where commit time goes: a flat array would save at most a
few percent of commit CPU, so CPU alone does not justify a change here. File
growth does.

## Options

### Option A — Keep the per-record search; approve the divergence

Change nothing in the engine; flip D-027 (b) to approved with the numbers
above.

- Pros: no risk in a correctness-critical area; no work.
- Cons: the file grows where LMDB's does not on workloads that free more than
  a meta page's worth of pages per commit and later need long runs, such as a
  Meilisearch reindex followed by document updates. Free pages from different
  records never coalesce, so the effect compounds over an environment's life.

### Option B — LMDB's technique: one sorted free list per write transaction

Replace `drains`, `reclaimed`'s role in the run search, and the separate annex
pool with one sorted array of reusable ids (ascending, consumed from the
front, which is equivalent to LMDB's descending array cut at the tail),
filled lazily oldest-first from the GC tree and then from the base meta's
list, with a sorted merge per record. `allocate(n)` searches the whole array;
on a miss it merges the next record and searches again, with LMDB's bound on
failed searches for `n > 1`, then extends.

`freelist_save` deletes every merged record and writes the leftover array
under the keys of the merged records (LMDB's step 5), each piece small enough
to stay inline in a GC leaf. The meta page's list keeps its GC-29/32 rules
for the transaction's own freed pages.

- Pros: matches LMDB's allocation, including run reuse across records and
  coalescing of leftovers on disk. One structure instead of three; the GC-20
  per-record rewrite and its provenance tracking go away. Deterministic: the
  merge order and the pick (smallest id, first run from the low end) are
  fixed.
- Cons:
  - Leftover pages are re-keyed under another record's id. Safe (every merged
    id already passed the gate, and the gate only moves forward), but GC keys
    stop meaning "the transaction that freed these pages"; INV-23 (key ≤ live
    txnid) still holds, and GC-18, GC-20 and the check tool's wording change.
  - `freelist_save` changes: its fixed-point loop (GC-11..13) must be
    re-proved with the merged list as the in-save pool. This is the riskiest
    part, and the reason LMDB's own save is intricate.
  - The allocation order changes wherever a run now spans records, so the
    crash-harness seeds and any test that pins page numbers may need
    re-pinning (never an LMDB pin).
  - `reclaimed` is still needed for the loose-page classification in
    `free_page`; it can become a check against the merged ids, but that is
    a separate step.
- Crash safety: unchanged in kind. The ids handed out all passed the gate; the
  persisted free set is still exactly the old set minus what was used plus
  what was freed, written in the same commit as the meta page (GC-14).
- ARM: no new shared state; the reader-table scan and its orderings are
  untouched.

### Option C — Keep the records, add a cross-record run search on miss

Keep `drains` and the GC-20 save unchanged. When `n > 1` finds no run in any
single record, merge the live ids of every loaded drain and the annex pool
into a temporary sorted view, loading further gated records oldest-first
with LMDB's bound, and search that view. Take the run out of each drain it
came from (each id is located by binary search in its own drain).

- Pros: the smallest change that closes D-027 (b) on reuse; the save, the
  on-disk records and their keys are untouched, so GC-11..14 and GC-20 keep
  their proofs. Can land behind the differential test above.
- Cons: two structures stay (per-record drains plus a transient merged view);
  O(loaded ids) per miss, bounded like LMDB's; leftovers never coalesce on
  disk, so a run that needs pages from many records is rebuilt from records
  on every miss.

### Option D — The huge-transaction free-page redesign

The planned replacement of the flat txnid-keyed free list by a structure that
stays fast when one transaction frees millions of pages (SPEC 05 §8, GC-27;
its own ADR, not written yet). Any of A–C must not block it; B is closest in
shape, since it already reasons about one merged set rather than per-record
lists.

## Decision

**Option C**, maintainer decision 2026-10-19 ("for #29 yes we have to do
something"). The draft's reasoning, kept for the record: Option C is the smallest change that tests the
riskiest assumption (that the cross-record miss is what grows the file),
touches no on-disk records and no save logic, and lands behind the
differential test above. Measure `free_run_churn` and `gc_churn_parity`
three ways (LMDB, ZeroDB before, ZeroDB after). Move to Option B only if C
leaves the file growing, which would mean leftovers have to coalesce on disk
as LMDB's do. Replacing `drains` and `reclaimed` for CPU alone (the issue's
title) is not recommended: the profile puts the bookkeeping at about 2 % of
a small commit.

## Outcome (implementation, 2026-10-19)

Implemented as allocate step 2c (`RwTxn::cross_record_run`,
`crates/zerodb-core/src/rwtxn.rs`; SPEC 05 GC-16, GC-21). For `n > 1`, only
after the per-record search and the annex pool miss: the live ids of the
gated tree records (oldest first, at most `n * 60` records, LMDB's bound) and
of the annex pool (when GC-31 admits it) are merged into one sorted list, and
the first run of `n` consecutive ids from the low end is taken. Each source
gives up its share (`Drain::take_span`), so `freelist_save` and the on-disk
records are unchanged. A page listed by two sources fails the allocation with
`MdbError::Invalid` (the records are untrusted input). A request at least as
long as one that already missed after reading every gated record skips the
search for the rest of the transaction, since a write transaction's reusable
pages only shrink during its operations. The merged list is built on the
first miss and kept for the transaction, as LMDB keeps its free list: a
first version rebuilt it on every miss and made `put/gc/ovf_across` (200
six-page values into two interleaved records) about 2.5× slower than
before. Ids the other draw arms take later stay in the list until a
candidate run meets them; every draw records its pages in `reclaimed`, so
a candidate is checked against it and a taken id is dropped then.

**The riskiest assumption held.** `free_run_churn` (macOS aarch64, 16 KiB
pages; file pages, deterministic; ZeroDB / LMDB in brackets):

| Workload | LMDB | ZeroDB before | ZeroDB after |
|----------|------|---------------|--------------|
| 2,000 rewrites per commit, 200 commits | 121,032 | 197,960 (1.64) | 128,710 (1.06) |
| 2,000 rewrites per commit, 400 commits | 121,713 | 295,585 (2.43) | 134,357 (1.10) |
| 100 rewrites per commit, 400 commits | 101,111 | 99,315 (0.98) | 98,762 (0.98) |
| inline values only, 2,000 per commit, 200 commits | 9,405 | 7,555 (0.80) | 7,555 (0.80) |

The cross-record miss was the cause of the growth; the residual (ZeroDB's
file still grows 4 % from 200 to 400 commits, LMDB's 0.6 %) is what
Option B would add (leftovers coalesced on disk) and is left open.
`gc_churn_parity` is unchanged (its values never need a run across records).

**Differential test.** In `lmdb_free_run_observation.rs` the two new
`_cushioned` cases (a run across two GC-tree records, or a GC-tree record and
the annex, still whole when the value is written) now match LMDB (reused,
file +0); ZeroDB before placed the value at the end of the file. The two
original cross-record cases still differ: there ZeroDB's own bookkeeping
pages (the leaf, the GC-tree leaf, the records' overflow values) are drawn
from the lowest free ids, the front of `a`, starting in the transaction right
after the one that freed `a` — which LMDB's gate does not allow (D-027 (a)).
Traced at 16 KiB pages, 10 of `a`'s pages are gone before `c`, leaving a free
run 7 pages short. Closing that needs D-027 (a) decided, not more search;
those two cases stay held to ZeroDB's outcome, now attributed to (a).

**A commit-time loop the property test found.** On Linux CI the property
test shrank to a commit whose `freelist_save` never reached its fixed point
(GC-13): the transaction's own freed-page record, which in the tree arm also
lists the carried annex pool, sat at an overflow page-count boundary. The
put shrank it by a page, listing the freed tail page grew it back, and the
grow drew a fresh run from the annex, so each cycle moved two annex ids into
the freed set and the loop needed as many iterations as the pool held ids.
The cross-record search only made the state reachable (no drain pool was
loaded in that commit). LMDB keeps a larger dirty overflow run in place when
a value shrinks; SPEC 02 §5 ties the run length to the value size, so
ZeroDB instead takes a grow that follows a shrink from outside the record's
set: the drain pool or the end of the file (SPEC 05 GC-13 (v)). The case
converges in 4 iterations, and `free_run_churn` reads the same file sizes
as in the table above.

## Consequences

- Option B or C: SPEC 05 GC-16, GC-18, GC-21 (and for B GC-11..13, GC-20,
  GC-27) are amended in the same change; D-027 (b) is retired when
  `lmdb_free_run_observation.rs` holds ZeroDB to LMDB's pins in the
  cross-record cases; `gc_churn_parity`'s band note about the run search is
  updated.
- Gates: the differential oracle and fuzz against LMDB unchanged, the crash
  harness in every durability mode, miri on `zerodb-core`, and a three-column
  bench (LMDB, ZeroDB before, ZeroDB after) on `put/gc/drain_big`,
  `commit/batch/*` and the churn example.
- D-027 (a) is independent: matching LMDB's stricter gate would make ZeroDB's
  files larger and gains nothing in safety under ZeroDB's reader protocol;
  the draft proposes sanctioning it.

## Open questions for human review

1. ~~Close the cross-record gap (Option B or C), or approve it as a
   divergence (Option A)?~~ Closed with Option C (2026-10-19).
2. Option B (leftovers coalesced on disk, re-keyed) stays open: worth it only
   if the residual growth above matters.
3. Sanction D-027 (a), ZeroDB's one-transaction-earlier reuse? It is now the
   only reason the two uncushioned observation cases differ from LMDB.
