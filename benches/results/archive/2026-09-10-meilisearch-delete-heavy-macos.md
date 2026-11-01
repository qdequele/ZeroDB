# Meilisearch v1.53.1 on LMDB vs ZeroDB — **delete-heavy** workloads, macOS, 2026-09-10

Companion to [`2026-09-09-meilisearch-v1.53.1-movies-macos.md`](2026-09-09-meilisearch-v1.53.1-movies-macos.md),
which measured an **insert-only** workload and found parity (1.00× indexing,
0.99× `write_db::all`). This run answers the question that one could not: does
the microbench ladder's delete gap (PERF-GAP `B8`/`B8a`, `del/*` at 1.6–2.7×)
reach a real consumer?

**It does.** On removing filterable attributes, the delete phase is **2.42×**
and costs **+472 ms**, while every other phase is at parity or faster.

## Setup

- Meilisearch v1.53.1 (`577f7af28`) — the same pin as the 2026-09-09 run, so
  the two are directly comparable. Zero source changes; `lmdb-master-sys`
  absent from the ZeroDB binary's dependency tree.
- Apple M1 Pro, macOS 27.0. Both binaries `cargo build --release -p meilisearch`,
  Meilisearch's production allocator.
- `scripts/consumer.sh bench`, **`ROUNDS=2`** — each round runs LMDB then
  ZeroDB and round 2 flips the order, so thermal drift cannot be attributed to
  one engine. `run_count` 5 per workload per round → 10 runs per engine.
- Dataset `150k-people.json` (150 000 documents) for both workloads.

**Why these two workloads.** They were chosen after reading milli, not guessed:

| milli code | primitive | rung it maps to |
|---|---|---|
| `delete_old_fid_from_facet_databases` → `IndexingStep::DeletingFromAllFilters` (`update/new/indexer/mod.rs:840`) | `prefix_iter_mut` + `del_current` loop | `B8a`, measured 4.2× |
| `clear_facet_levels` (`update/facet/mod.rs:315`) | `db.delete_range` | `B8`, `del/range/half` 2.74× |
| `delete_old_fid_word_count_docids` (`indexer/mod.rs:1136`) | `prefix_iter_mut` + `del_current` | `B8a` |
| `post_processing::prefix::delete_prefixes` | `del_current` loop | `B8a` |

`del_current` appears at **9 call sites in milli, 4 of them in the current
indexer** — this is production code, not an incidental API.

## Result 1 — `settings-add-remove-filters` (removes two filterable attributes)

```text
   total (self time)                            lmdb    5.260 s   zerodb    5.618 s   ratio  1.07x
   span (median inclusive over 10 runs)                lmdb         zerodb  ratio
   ::meta::total                                   3.151 s      3.633 s    1.15x
   indexing::scheduler::process_batch              2.504 s      2.955 s    1.18x
   indexing::scheduler::apply_index_operation      2.459 s      2.884 s    1.17x
   indexing::write_db::all                         1.532 s      1.469 s    0.96x   <- ZeroDB faster
   indexing::documents::extract                    1.530 s      1.468 s    0.96x   <- ZeroDB faster
   indexing::post_processing::post_process         603.5 ms     579.8 ms   0.96x
   indexing::documents::extract::faceted           571.9 ms     539.3 ms   0.94x
```

The totals understate it. Broken out by **self** time — time inside a span not
attributed to any child — the entire regression is one span:

| span (self time, median of 10) | LMDB | ZeroDB | ratio | Δ |
|---|---:|---:|---:|---:|
| **`indexing::scheduler::apply_index_operation`** | **332.8 ms** | **805.1 ms** | **2.42×** | **+472 ms** |
| `indexing::scheduler::commit` | 38.2 ms | 47.2 ms | 1.23× | +9 ms |
| `indexing::facet_fst::merge_and_write` | 181.1 ms | 175.6 ms | 0.97× | −6 ms |
| `indexing::merge::merge_and_send_facet_docids` | 49.4 ms | 43.7 ms | 0.88× | −6 ms |
| `indexing::post_processing::facet_field_ids::string` | 92.1 ms | 85.0 ms | 0.92× | −7 ms |

**Why that span is the delete phase.** `reindex()` calls
`delete_old_fid_from_facet_databases` at `indexer/mod.rs:265`, and neither that
function nor the `DeletingFromAllFilters` loop inside it carries a
`#[tracing::instrument]`. Their time therefore lands in the caller's self time,
which is `apply_index_operation`'s — the one span that moved. Everything that
*is* instrumented is at parity or faster.

**2.42× against the ladder's `del/*` cluster at 1.97–2.74×**: the microbench
predicted the consumer number.

Per round, with the order flipped:

| round | LMDB | ZeroDB | ratio |
|---|---:|---:|---:|
| 1 (LMDB first) | 494.1 ms | 826.0 ms | 1.67× |
| 2 (order flipped) | 319.8 ms | 784.8 ms | 2.45× |

ZeroDB is stable across both (826 / 785 ms); the spread is LMDB's round-1 run
being cold. Warm-vs-warm is ~2.4×.

## Result 2 — `settings-remove-add-swap-searchable`

End-to-end **0.99×** — parity. The workload is dominated by extraction
(`write_db::all` alone is 5.2 s of a 12.6 s total), so the delete work is
diluted. But the delete span itself is not:

| span (self time, median of 10) | LMDB | ZeroDB | ratio |
|---|---:|---:|---:|
| **`post_processing::prefix::delete_prefixes`** | **3.9 ms** | **14.1 ms** | **3.63×** |
| `post_processing::facet_field_ids::string` | 153.9 ms | 196.1 ms | 1.27× |
| `documents::extract::word_pair_proximity_docids_extraction` | 1397.8 ms | 1580.5 ms | 1.13× |
| `write_db::all` | 5218.3 ms | 5237.7 ms | 1.00× |

`delete_prefixes` is another `del_current` loop and reproduces at **3.65× /
3.60×** across the two rounds — the cleanest consumer-level confirmation of
`B8a`, whose microbench figure was 4.2×. Absolute cost is only +10 ms here, but
it scales with the number of prefixes removed.

**Unexplained, flagged not claimed:** `word_pair_proximity_docids_extraction`
is 1.13× / 1.11× — stable across both rounds, so it is real rather than noise,
but it is an *extraction* span and should not depend on the storage engine.
Worth a look before it is assumed benign.

## Verdict

- **`B8`/`B8a` reach Meilisearch.** The insert-only bench could not have found
  them: both fire only on settings changes that *remove* an attribute.
- **The blast radius is bounded.** Worst end-to-end figure is 1.07×, on a
  workload whose whole point is removing filterable attributes. Steady-state
  indexing and search remain at parity (2026-09-09 run).
- **It scales with index size.** +472 ms on 150 k documents is a settings
  operation, not a per-query cost — but it is linear in the entries deleted, so
  a 10 M-document index removing a filterable attribute pays ~30 s where LMDB
  pays ~13 s.
- **ZeroDB is faster on everything else in these runs**: `write_db::all` 0.96×,
  `extract` 0.96×, `faceted` 0.94×, `merge_and_send_facet_docids` 0.88×.

Both findings are therefore worth their ADR/spec work, `B8a` first: it is the
larger ratio, the more contained fix, and it sits on public heed API that milli
uses in nine places.

## Reproducing

```bash
WORKLOADS="workloads/settings-add-remove-filters.json workloads/settings-remove-add-swap-searchable.json" \
ROUNDS=2 MEILISEARCH_SRC=~/path/to/meilisearch scripts/consumer.sh bench
```
