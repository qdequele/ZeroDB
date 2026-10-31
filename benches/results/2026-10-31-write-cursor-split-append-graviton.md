# Write-cursor puts across splits, cursor steps in place, APPEND (issue #138), Graviton4 — 2026-10-31

Host: AWS Graviton4 `c8g.xlarge` (4 vCPU, 8 GiB), Ubuntu 24.04, 60 GiB gp3 root,
4 KiB OS pages, CGU1, load ~1. Before = `0adf868` (main), after = `0adf868` +
the #138 change. `scripts/bench-ab.sh`, 3 interleaved rounds, LMDB measured in
both halves as the drift control.

## Put ladder (`^put/`)

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `put/api/plain` | 14.614 ms | 14.271 ms | 14.327 ms | 0.98× → **0.98×** | 1.004 (±0.030) | 1.006 | flat |
| `put/api/reserved` | 14.705 ms | 16.060 ms | 16.147 ms | 1.09× → **1.10×** | 1.005 (±0.030) | 1.005 | flat |
| `put/cursor/rewrite` 🎯 | 1.509 ms | 3.776 ms | 2.069 ms | 2.50× → **1.37×** | 0.547 (±0.030) | 0.981 | ✅ faster |
| `put/gc/drain_big` | 22.608 ms | 25.890 ms | 26.093 ms | 1.15× → **1.15×** | 1.008 (±0.030) | 0.998 | flat |
| `put/gc/ovf_across` | 485.344 µs | 763.301 µs | 795.009 µs | 1.57× → **1.64×** | 1.042 (±0.041) | 0.998 | ❌ slower |
| `put/gc/ovf_miss` | 1.173 ms | 1.242 ms | 1.282 ms | 1.06× → **1.09×** | 1.028 (±0.030) | 0.992 | flat |
| `put/order/append` 🎯 | 9.134 ms | 11.505 ms | 11.050 ms | 1.26× → **1.21×** | 0.962 (±0.030) | 1.004 | ✅ faster |
| `put/order/rand` | 35.429 ms | 34.247 ms | 34.102 ms | 0.97× → **0.96×** | 0.996 (±0.030) | 0.998 | flat |
| `put/order/seq` 🎯 | 14.208 ms | 14.329 ms | 14.300 ms | 1.01× → **1.01×** | 0.994 (±0.030) | 1.008 | flat |
| `put/over/grow` | 8.604 ms | 9.678 ms | 9.736 ms | 1.12× → **1.13×** | 1.002 (±0.030) | 0.997 | flat |
| `put/over/same_size` | 2.920 ms | 3.179 ms | 3.177 ms | 1.09× → **1.09×** | 1.000 (±0.030) | 1.002 | flat |
| `put/val/v256` 🎯 | 2.560 ms | 2.618 ms | 2.607 ms | 1.02× → **1.02×** | 0.997 (±0.030) | 1.003 | flat |
| `put/val/v2page` 🎯 | 6.467 ms | 6.421 ms | 6.451 ms | 0.99× → **1.00×** | 1.009 (±0.030) | 1.001 | flat |
| `put/val/v2page_abort` 🎯 | 1.385 ms | 1.606 ms | 1.609 ms | 1.16× → **1.16×** | 1.011 (±0.030) | 1.007 | flat |
| `put/val/v4k` 🎯 | 23.506 ms | 23.132 ms | 23.202 ms | 0.98× → **0.99×** | 1.004 (±0.030) | 1.006 | flat |
| `put/val/v8` 🎯 | 1.417 ms | 1.448 ms | 1.458 ms | 1.02× → **1.03×** | 1.000 (±0.030) | 0.995 | flat |

`put/cursor/rewrite` never splits (same-size values): its gain comes from the
cursor stepping on its parked path in place and from the one-access rewrite on a
dirty leaf, not from keeping the path across splits. `put/gc/ovf_across` and
`put/gc/ovf_miss` moved the wrong way in all three rounds (+4.2 %, +2.8 %); see
"Open" below.

## Delete and commit ladders (`^(del|commit)/`)

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| `commit/batch/n1` | 7.380 ms | 9.091 ms | 9.210 ms | 1.23× → **1.25×** | 1.013 (±0.030) | 1.001 | flat |
| `commit/batch/n100` | 6.210 ms | 6.922 ms | 6.932 ms | 1.11× → **1.12×** | 1.001 (±0.030) | 0.997 | flat |
| `commit/batch/n10k` | 13.490 ms | 13.507 ms | 13.638 ms | 1.00× → **1.01×** | 1.008 (±0.030) | 1.002 | flat |
| `commit/sync/n1` | 394.662 ms | 386.701 ms | 387.107 ms | 0.98× → **0.98×** | 1.000 (±0.030) | 1.000 | flat |
| `commit/sync/n100` | 769.101 ms | 769.473 ms | 769.795 ms | 1.00× → **1.00×** | 1.000 (±0.030) | 0.999 | flat |
| `commit/sync/n1_map16g` | 394.843 ms | 387.119 ms | 387.606 ms | 0.98× → **0.98×** | 1.001 (±0.030) | 1.001 | flat |
| `del/batch/k1000` | 30.775 ms | 39.026 ms | 38.811 ms | 1.27× → **1.26×** | 0.994 (±0.030) | 0.997 | flat |
| `del/bulk/all` | 13.900 ms | 19.294 ms | 19.925 ms | 1.39× → **1.43×** | 1.031 (±0.030) | 0.995 | ❌ slower |
| `del/bulk/half` | 7.082 ms | 9.767 ms | 10.047 ms | 1.38× → **1.42×** | 1.026 (±0.040) | 0.995 | flat |
| `del/churn/reinsert` | 9.168 ms | 11.987 ms | 12.188 ms | 1.31× → **1.33×** | 1.017 (±0.030) | 1.004 | flat |
| `del/clear/all` | 114.462 µs | 203.004 µs | 206.658 µs | 1.77× → **1.81×** | 1.012 (±0.058) | 1.022 | flat |
| `del/cursor/drain` | 5.219 ms | 13.693 ms | 11.218 ms | 2.62× → **2.15×** | 0.819 (±0.030) | 0.990 | ✅ faster |
| `del/random/n1` | 873.587 ms | 1.371 s | 1.314 s | 1.57× → **1.50×** | 0.959 (±0.040) | 0.997 | ✅ faster |
| `del/range/half` | 5.186 ms | 3.037 ms | 3.025 ms | 0.59× → **0.58×** | 1.009 (±0.030) | 1.000 | flat |

`del/cursor/drain` gains from the in-place cursor steps. `del/bulk/all` (+3.1 %)
and `del/random/n1` (−4.1 %) run the same point-delete path, which this change
does not touch.

## Loops without commit (`cursor_put_census`, CPU ns/op, mean of 3)

| loop | LMDB | ZeroDB before | ZeroDB after |
|---|---:|---:|---:|
| cursor rewrite (10k same-size `put_current`) | 57.7 | 234.5 (4.06×) | 119.7 (2.07×) |
| `APPEND` load (50k, 256 B values) | 109.1 | 167.4 (1.53×) | 147.8 (1.35×) |

## Linux perf, `APPEND` load

ZeroDB after:

| self | function |
|---:|---|
| 22.35 % | `<zerodb_core::dirty::DirtyStore>::bytes` |
| 22.25 % | `<zerodb_core::rwtxn::RwTxn>::append_tree` |
| 4.34 % | `<zerodb_core::rwtxn::RwTxn>::append_apply` |
| 4.18 % | `<zerodb_core::page::tree::LeafRef>::key` |
| 3.83 % | `<zerodb_core::dirty::DirtyStore>::bytes_mut` |
| 3.60 % | `<zerodb_core::page::tree::LeafRef>::new_prevalidated` |
| 3.38 % | `<zerodb_core::page::tree::LeafMut>::insert_cell` |
| 3.29 % | `<zerodb_core::rotxn::Database>::put_with_flags` |

LMDB:

| self | function |
|---:|---|
| 22.97 % | `mdb_mid2l_search` |
| 18.44 % | `mdb_page_search` |
| 13.68 % | `_mdb_cursor_put` |
| 8.09 % | `mdb_cursor_last` |
| 6.10 % | `mdb_node_add` |
| 5.93 % | `mdb_page_get.constprop.0.isra.0` |
| 3.63 % | `mdb_page_touch` |
| 3.07 % | `mdb_cmp_memn` |

The descent now costs the same kind of work in both engines: one dirty-page
lookup per level (`DirtyStore::bytes`, a hash probe, against `mdb_mid2l_search`,
a binary search of the dirty list) plus the per-level walk (`append_tree` against
`mdb_page_search`/`mdb_cursor_last`). Before the change ZeroDB also re-viewed the
leaf in `append_apply` and touched every level again (`RwTxn::touch` 4.9 %).

## Linux perf, cursor rewrite

ZeroDB after:

| self | function |
|---:|---|
| 13.24 % | `<zerodb_core::rwtxn::RwCursor>::drive` |
| 7.75 % | `<zerodb_core::dirty::DirtyStore>::bytes` |
| 7.44 % | `<zerodb_core::rwtxn::RwCursor>::put_at_parked` |
| 5.78 % | `<zerodb_core::page::tree::LeafRef>::new_prevalidated` |
| 5.71 % | `<zerodb_core::dirty::DirtyStore>::bytes_mut` |
| 5.07 % | `<heed_zerodb::iterator::RwGuts>::step` |
| 4.54 % | `<zerodb_core::btree::Cursor<&mut zerodb_core::btree::PathStack>>::next` |
| 4.53 % | `<zerodb_core::page::tree::LeafMut>::key` |

LMDB:

| self | function |
|---:|---|
| 16.74 % | `_mdb_cursor_put` |
| 12.99 % | `mdb_cursor_next` |
| 11.96 % | `mdb_page_touch` |
| 9.91 % | `cursor_put_census::main` |
| 5.46 % | `0x00000000000a1c38` |
| 4.44 % | `mdb_cursor_touch` |
| 3.07 % | `mdb_mid2l_search` |
| 3.07 % | `mdb_page_spill` |

## Open

- `put/gc/ovf_across` +4.2 % and `put/gc/ovf_miss` +2.8 %, consistent across rounds,
  on keyed puts of 6-page values. The only change in that hot loop was
  `node_view` becoming a wrapper over the new `node_view_classified`; `node_view`
  was restored to its own body after this run, without a new Graviton
  measurement. A Mac screen of the restored tree (16 KiB pages, loaded host,
  3 rounds, `HEAD` vs working tree) showed `del/bulk/all` 0.994 (flat),
  `put/gc/ovf_miss` 1.041 ±0.378 (flat, noisy), `put/gc/ovf_across` 0.983 and
  `put/cursor/rewrite` 0.652 (LMDB drift 3–66 %, unreliable): no sign of the
  regression, but not proof. Re-run `^(put/gc/ovf|del/bulk)` on the next
  Graviton host.
