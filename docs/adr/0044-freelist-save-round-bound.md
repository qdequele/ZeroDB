# ADR-0044: The round limit of the free-list save has no derivation

- Status: Accepted (Option C with Option B's budget, maintainer, 2026-11-12), implemented
- Related issue: none
- Date: 2026-11-12

## Context

Commit step C1, `RwTxn::freelist_save` (`crates/zerodb-core/src/rwtxn/freelist.rs`),
writes the free-page database in a loop that runs until nothing changes:
rewriting a record can allocate a page, the allocation can draw a page from a
pool the records list, and the records must then be rewritten
([SPEC 05](../SPEC/05-gc.md) GC-11 to GC-13).

GC-13 proves the loop ends. Every round that does not exit strictly reduces
one of four finite quantities: the drain pool, the annex pool, the committed
free-tree pages not yet copied, and the loose pages not yet folded in.

The code holds two guards in release builds:

| Guard | Limit | Where the limit comes from |
|---|---|---|
| Inner loop (`rewrite_pending_drains`) | pending records + ids in the drain pool + 16 | The quantities of the proof |
| Outer loop | `FREELIST_SAVE_MAX_ITERS = 64` | Nothing. The proof bounds the rounds by the four quantities, which can be thousands |

The outer limit is therefore not implied by GC-13. A valid state could in
principle need more than 64 rounds, and the commit would panic.

**What a panic costs.** Nothing has reached the disk before step C2, so no
data is lost and the previous snapshot stays intact (GC-14). But the save is
a function of the base snapshot and the transaction's operations: an
application that retries the same batch is likely to hit the same panic
again. For Meilisearch that is an index that cannot be written until someone
intervenes.

**Prior art.** LMDB's `mdb_freelist_save` loops with no limit at all. An
earlier ZeroDB design did cycle for many rounds in one case (an own record on
an overflow run at a page-count boundary, issue #29); ADR-0037 removed that
case by making records inline and bounded. So the guard has caught a real
defect once, and a hang there is not hypothetical.

**Measured.** With a temporary counter in `freelist_save` (not committed), at
commit `9115969` on macOS arm64:

| Run | Largest number of rounds | Saves that took 3 rounds |
|---|---:|---:|
| `cargo test --workspace` | 3 | 43 |
| Crash harness, 200 cycles | 2 or fewer | 0 |
| Crash harness, multi-process, 50 cycles | 2 or fewer | 0 |
| `reader_stress` and `multi_process_stress`, 40 s each | 3 | 96 |

Among the saves that took 3 rounds, the inner loop used at most 19 % of its
budget. The fuzz targets were not
run with the counter.

So no run has come near 64. That is evidence, not a bound: the tests cover
the states someone thought of.

## Options

### Option A — Keep 64, record the evidence

Add the table above to SPEC 05 GC-13 and say the limit is empirical.

- For: no code change.
- Against: the one release-build panic in the commit path that the proof does
  not cover stays as it is.

### Option B — Derive the outer limit from the proof

Replace the constant with a budget computed at entry, as the inner loop
does: pending records + ids in the drain pool + ids in the annex pool +
pages of the committed free tree (`branch_pages + leaf_pages` of its record)
+ loose pages + a constant.

- For: a few lines. The panic then fires only if GC-13 is wrong, which is
  what a guard is for. A valid state can no longer trip it.
- Against: if the loop has a defect that makes it slow to converge rather
  than endless, the commit spins for thousands of rounds before the panic.
  The retry problem above is unchanged when the guard does fire.
- Crash safety: unchanged; nothing is written before C2.

### Option C — After a few rounds, stop drawing from the pools (recommended)

Keep a small threshold (8 is proposed; the largest value seen is 3). When the
save passes it, allocations inside the save stop drawing from the drain pool
and the annex pool, and take loose pages or extend the file. Pool draws are
the only way a record written in one round is made stale by the next, so
without them the feedback stops: what remains is the copying of committed
free-tree pages, which is finite and feeds nothing back. The save then ends
a few rounds later, and the commit succeeds with a file a few pages larger
than it could have been. Option B's derived budget stays as the last guard.

- For: a state that converges slowly costs a few pages of file growth, not a
  failed commit. The pages left in the pools are not lost: they stay listed
  and are reusable by later transactions.
- Against: a second allocation mode inside the save, reached almost never,
  so it needs its own tests (a test-only threshold of 1 makes every save
  take that path) and its own termination argument in SPEC 05. About 30
  lines of engine code.
- Crash safety: unchanged. Extending the file inside the save is already an
  allowed outcome (GC-12).
- The argument that the save ends once draws stop is sketched here, not
  proven. It must be written into GC-13 and reviewed before the code.

### Option D — Return an error where the code panics

`freelist_save` returns an error, the commit fails, and the transaction is
aborted.

- For: the application sees a normal error. No unwinding through the commit.
- Against: it does not fix the retry problem; the batch fails every time, with
  an error in place of a panic. The engine's LMDB-shaped error set has no
  variant that fits (LMDB's `MDB_PROBLEM` is not among them), so a new one
  is needed.
- It can be combined with B or C for the last guard.

### Option E — Remove the limit, as LMDB

- Against: a defect becomes a commit that never returns while holding the
  writer lock. Rejected.

## Decision

**Option C, with Option B's derived budget as the last guard.**

The argument that the save ends once draws stop is SPEC 05 GC-13a; the
budget is derived in GC-13.

## Consequences

- `freelist.rs`: `FREELIST_SAVE_DRAW_ROUNDS = 8` replaces the constant 64;
  the `assert!` uses the budget `3·(P + D + A) + G + 64`, never
  below the old limit.
- `alloc.rs`: a third allocation mode, `GcSaveExtend`, in which the save
  takes loose pages or extends. When the map is full it draws from the
  pools as before, so that a save the pools can serve does not fail with
  `MapFull`; that case ends by GC-13 and the budget alone.
- Tests: `just save-no-draw`, also a CI job, builds with the threshold at 0
  and at 1 (`--cfg zerodb_save_draw_rounds`) and runs the tests of
  `zerodb-core` and `zerodb` and 200 crash cycles. The tests check contents
  and page accounting (every page reachable or listed free, never both)
  through the existing `check`. Two tests are skipped in that mode because
  they assert a file size and page numbers that hold only when the save
  reuses pool pages: `over_cap_freed_set_spills_whole_to_gc_tree` and
  `delete_rebalance_logical_images_match_golden`. At the default threshold
  no test reaches the degraded mode. `save_at_map_limit.rs` covers the full
  map: it fails in that run if the save does not fall back to the pools.
- SPEC 05: GC-12, GC-13 (the budget) and the new GC-13a.
- No format change. Files written by either version are read by the other.
- The differential oracle is unaffected: page reclamation is not observable
  through the heed API.

## Open questions for human review

1. **Threshold.** 8 leaves a margin over the 3 observed. The nightly fuzz
   and the full crash run were not measured with the counter.
2. **Last guard: panic or error?** It is still a panic. If an error, which
   variant (Option D).
3. **Keep a counter?** A permanent maximum-rounds statistic, readable from
   `stat`, would show in production whether the degraded mode is ever
   reached.
