# Delete path: write-cursor step in place, left-pairing repair, `clear` free list (issue #137) — 2026-10-31

- Change: four levers on the delete path, each from LMDB's own technique for
  that path (below). Tree shape unchanged: no split or merge decision changed,
  only bookkeeping around them.
- Base: `0adf868` (main). Candidate: this branch.
- Hosts: Apple M-series laptop (macOS, 16 KiB pages, busy: load average
  11–240 during the session) for profiles and retired-instruction counts;
  AWS Graviton4 `c8g.xlarge` (Ubuntu 24.04, 4 KiB pages, gp3 root) for the
  criterion three-column A/B.

## Where the time went before

Profiles (`scripts/bench-profile.sh`, macOS `sample`; then a standalone probe
that runs one engine and one rung N times, read with `sample`, Xcode's CPU
Profiler and `/usr/bin/time -l` retired instructions):

- **`del/cursor/drain`** cost *more* instructions than deleting the same keys
  by key (191 M vs 169 M per drain), while LMDB's cursor drain costs *less*
  than its keyed delete (106 M vs 131 M). The write cursor's own step was the
  largest term: after every delete, `next` rebuilt a cursor around the parked
  path, copied the 520-byte path stack about six times, re-read the leaf
  three times and allocated a copy of the deleted key.
- In every delete, `remove_cell` fixed the cell pointers with a bounds-checked
  pass and then shifted them in a second pass (15 % of the drain's
  self time); LMDB's `mdb_node_del` does it in one.
- The front drain never takes the re-descent the issue names: it starts at
  the tree's first key, so every rebalance pairs a parent's child 0 with its
  right sibling, which was already repaired. A drain that starts mid-tree
  rebalances on only ~3 % of its deletes (each drained leaf's remainder
  merges once into the survivor page on its left). A **backward** drain is
  where the left pairing bites: every rebalance pairs with the left sibling,
  and every `prev` after a delete discarded the path and re-descended.
- **`clear`** (850 pages): 29–43 % in sorting a clone of the collected page
  list (the hostile-image duplicate check), 23 % in `free_page`, whose
  `DirtyStore::discard` probe ran for committed pages that cannot have a
  frame, and 8 % in growing the page list one push at a time. About 30 µs of
  `del/clear/all`'s gap is the commit's fixed cost, shared with every write
  transaction (issue #8).
- On macOS's 16 KiB pages the ladder's 50k-key fixture is one level deeper in
  ZeroDB than in LMDB (863 leaves both; ZeroDB's 10-byte branch cells,
  ADR-0002, need 3 branch pages where LMDB needs 1). On 4 KiB pages both are
  depth 3. Mac delete ratios carry that extra level; Graviton's do not.

## The levers

| # | lever | LMDB technique | code |
|---|---|---|---|
| 1 | Write-cursor step in place: `next`/`prev` that stay in the parked leaf read the slot straight from the parked path; `del_current` deletes at the parked frames in place and copies the key into the buffer the previous one used | `mdb_cursor_next`/`prev` move `mc_ki` in the page the cursor holds; after `C_DEL`, `next` returns the node at `mc_ki` | `RwCursor::drive`, `del_current`; `Tree::entry_in_leaf` |
| 2 | One pass over the pointer array in `remove_cell` (shift, then a chunked fix-up with no per-slot bounds check) | `mdb_node_del`'s single fix-up loop | `page::tree::remove_cell` |
| 3 | Left-pairing path repair: a borrow from the left moves the slot up one, a merge into the left sibling moves the frame there with the slot past the sibling's entries; `prev` after a delete steps back from the kept path | `mdb_node_move` and `mdb_page_merge` cursor fix-ups; `mdb_cursor_prev` after `C_DEL` | `rebalance`, `merge_pages`, `drive`; SPEC 03 §5.4a |
| 4 | `clear`: a committed page is freed with a list append (no dirty-store probe; TXN-62 guarantees it has no frame); the page list is reserved per bottom branch; the duplicate check is a bitmap over the pgno span instead of a sort | `mdb_drop0` appends child pgnos after `mdb_midl_need`; LMDB has no duplicate check (ZeroDB keeps it, linear) | `free_page`, `collect_tree_skip_leaves`, `has_duplicate` |

## Retired instructions per operation (macOS, probe, 10 runs, fixture subtracted)

| rung (probe) | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after |
|---|---:|---:|---:|---:|
| front drain (`del/cursor/drain`) | 106 M | 191 M | 152 M | 1.81× → 1.44× |
| backward drain (`del/cursor/rev`) | 70 M | 184 M | 109 M | 2.62× → 1.55× |
| mid-tree drain (keys N/4..3N/4) | 73 M | 138 M | 100 M | 1.89× → 1.37× |
| keyed delete (`del/bulk/half`) | 131 M | 169 M | 160 M | 1.29× → 1.22× |

Per lever (same probe): lever 1 took the front drain 196 → 162 M and the
mid-tree drain 137 → 109 M with the keyed delete flat (169.9 → 169.7 M);
lever 2 took both the drain and the keyed delete down another 10 M
(162 → 152 M, 170 → 160 M); lever 3 is the backward drain's 184 → 109 M
together with lever 1 (on the mid-tree drain it is within noise: it saves
~725 re-descents in 25k deletes). `clear` alone (no commit, min of 25):
35.2 µs → 19.6 µs, LMDB 7.7–8.7 µs.

## Criterion ladder, Graviton4, 3 interleaved rounds (`scripts/bench-ab.sh`)

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `del/batch/k1000` 🎯 | 31.930 ms | 40.489 ms | 40.259 ms | 1.27× → **1.26×** | 0.996 (±0.030) | 1.000 | flat |
| `del/bulk/all` 🎯 | 13.868 ms | 19.718 ms | 18.803 ms | 1.42× → **1.36×** | 0.952 (±0.030) | 0.994 | ✅ faster |
| `del/bulk/half` 🎯 | 7.048 ms | 9.966 ms | 9.496 ms | 1.41× → **1.35×** | 0.949 (±0.030) | 0.997 | ✅ faster |
| `del/churn/reinsert` 🎯 | 9.404 ms | 12.339 ms | 12.043 ms | 1.31× → **1.28×** | 0.979 (±0.030) | 1.003 | flat |
| `del/clear/all` 🎯 | 116.205 µs | 201.380 µs | 166.618 µs | 1.73× → **1.43×** | 0.830 (±0.030) | 0.987 | ✅ faster |
| `del/cursor/drain` 🎯 | 5.224 ms | 12.594 ms | 8.680 ms | 2.41× → **1.66×** | 0.688 (±0.030) | 1.007 | ✅ faster |
| `del/cursor/rev` 🎯 | 5.441 ms | 16.651 ms | 8.145 ms | 3.06× → **1.50×** | 0.488 (±0.030) | 0.994 | ✅ faster |
| `del/random/n1` | 898.093 ms | 1.483 s | 1.429 s | 1.65× → **1.59×** | 0.968 (±0.030) | 1.002 | ✅ faster |
| `del/range/half` | 5.227 ms | 3.081 ms | 2.742 ms | 0.59× → **0.52×** | 0.890 (±0.035) | 1.002 | ✅ faster |

**Verdict: improved**. Improved: 7, regressed: 0, flat: 2, unreliable (LMDB drift > 3%): 0. 3 interleaved round(s); before = `0adf868c3`, after = `0adf868+dirty`, host `Linux aarch64`.

## Regression check: `^(put|commit)/`, Graviton4, 3 rounds

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `commit/batch/n1` 🎯 | 7.347 ms | 9.275 ms | 9.177 ms | 1.26× → **1.25×** | 0.989 (±0.030) | 1.006 | flat |
| `commit/batch/n100` 🎯 | 6.258 ms | 7.035 ms | 6.994 ms | 1.12× → **1.12×** | 0.996 (±0.061) | 1.002 | flat |
| `commit/batch/n10k` 🎯 | 13.707 ms | 13.810 ms | 13.630 ms | 1.01× → **0.99×** | 0.987 (±0.135) | 1.000 | flat |
| `commit/sync/n1` 🎯 | 394.030 ms | 386.731 ms | 386.778 ms | 0.98× → **0.98×** | 1.000 (±0.030) | 0.999 | flat |
| `commit/sync/n100` 🎯 | 766.722 ms | 768.331 ms | 768.992 ms | 1.00× → **1.00×** | 1.002 (±0.030) | 1.001 | flat |
| `commit/sync/n1_map16g` 🎯 | 393.962 ms | 386.901 ms | 386.836 ms | 0.98× → **0.98×** | 1.000 (±0.030) | 1.001 | flat |
| `put/api/plain` 🎯 | 14.747 ms | 14.315 ms | 14.164 ms | 0.97× → **0.96×** | 0.990 (±0.030) | 1.005 | flat |
| `put/api/reserved` 🎯 | 14.851 ms | 16.111 ms | 16.036 ms | 1.08× → **1.08×** | 0.996 (±0.030) | 1.004 | flat |
| `put/cursor/rewrite` 🎯 | 1.514 ms | 3.207 ms | 2.645 ms | 2.12× → **1.75×** | 0.822 (±0.030) | 1.000 | ✅ faster |
| `put/gc/drain_big` 🎯 | 22.499 ms | 25.727 ms | 25.746 ms | 1.14× → **1.14×** | 1.000 (±0.030) | 1.012 | flat |
| `put/gc/ovf_across` 🎯 | 484.501 µs | 773.745 µs | 782.129 µs | 1.60× → **1.61×** | 1.011 (±0.030) | 0.992 | flat |
| `put/gc/ovf_miss` 🎯 | 1.186 ms | 1.263 ms | 1.267 ms | 1.06× → **1.07×** | 0.994 (±0.080) | 1.003 | flat |
| `put/order/append` 🎯 | 9.199 ms | 11.722 ms | 11.669 ms | 1.27× → **1.27×** | 0.996 (±0.030) | 1.003 | flat |
| `put/order/rand` 🎯 | 35.669 ms | 33.901 ms | 34.005 ms | 0.95× → **0.95×** | 1.002 (±0.030) | 0.998 | flat |
| `put/order/seq` 🎯 | 14.352 ms | 14.318 ms | 14.179 ms | 1.00× → **0.99×** | 0.990 (±0.030) | 1.005 | flat |
| `put/over/grow` 🎯 | 8.683 ms | 9.573 ms | 9.565 ms | 1.10× → **1.10×** | 0.996 (±0.030) | 1.017 | flat |
| `put/over/same_size` 🎯 | 2.964 ms | 3.221 ms | 3.190 ms | 1.09× → **1.08×** | 0.992 (±0.030) | 1.002 | flat |
| `put/val/v256` 🎯 | 2.591 ms | 2.637 ms | 2.615 ms | 1.02× → **1.01×** | 0.994 (±0.030) | 1.005 | flat |
| `put/val/v2page` 🎯 | 6.503 ms | 6.467 ms | 6.486 ms | 0.99× → **1.00×** | 1.005 (±0.033) | 1.005 | flat |
| `put/val/v2page_abort` 🎯 | 1.404 ms | 1.620 ms | 1.622 ms | 1.15× → **1.15×** | 1.005 (±0.030) | 0.998 | flat |
| `put/val/v4k` 🎯 | 23.532 ms | 23.170 ms | 23.150 ms | 0.98× → **0.98×** | 0.999 (±0.030) | 1.003 | flat |
| `put/val/v8` 🎯 | 1.440 ms | 1.464 ms | 1.454 ms | 1.02× → **1.01×** | 0.994 (±0.030) | 1.009 | flat |

**Verdict: improved**. Improved: 1, regressed: 0, flat: 21, unreliable (LMDB drift > 3%): 0. 3 interleaved round(s); before = `0adf868c3`, after = `0adf868+dirty`, host `Linux aarch64`.

`put/cursor/rewrite` (`range_mut` + `put_current` over every entry) gains from
lever 1: a put that stays in its leaf keeps the parked path (SPEC 03 §5.4b),
so its next `next` is now the in-leaf step too.
