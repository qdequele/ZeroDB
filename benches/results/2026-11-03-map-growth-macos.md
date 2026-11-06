# Map growth on demand (ADR-0036), macOS laptop

- **Change:** `EnvOpenOptions::initial_map_size` (ADR-0036 Option A-min):
  the map, and the data file under `WRITE_MAP`, start at the initial size
  and grow inside write txns up to `map_size`. Off by default.
- **Machine:** Apple M1 Pro, 16 GiB, macOS 27, 16 KiB OS pages, rustc 1.99.0.
  Other sessions' builds ran throughout (load average up to 45 at the end), so
  every number here is indicative; a verdict at the 3 % level needs a quiet
  Linux host.
- **Method:** `scripts/bench-ab.sh`, three interleaved rounds, LMDB (heed
  0.22.1 over the Meilisearch fork) measured in both halves as the drift
  control. Ratios are ZeroDB time ÷ LMDB time.

## 1. Option off: the default path against main

`BASE=ec84097 scripts/bench-ab.sh '^(put|commit/batch|env/txn)/'`, after =
the implementation in the working tree. Verdict **flat**: 22 flat, 0
regressed, 2 unreliable (LMDB drift above 3 %).

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
| `commit/batch/n1` 🎯 | 22.711 ms | 23.233 ms | 22.838 ms | 1.02× → **1.01×** | 0.983 (±0.030) | 0.991 | flat |
| `commit/batch/n100` 🎯 | 9.086 ms | 8.888 ms | 8.802 ms | 0.98× → **0.97×** | 0.992 (±0.030) | 0.995 | flat |
| `commit/batch/n10k` 🎯 | 14.731 ms | 12.242 ms | 12.278 ms | 0.83× → **0.83×** | 1.003 (±0.030) | 1.003 | flat |
| `env/txn/ro_begin_abort` 🎯 | 8.062 ms | 351.237 µs | 349.386 µs | 0.04× → **0.04×** | 0.994 (±0.030) | 1.002 | flat |
| `env/txn/ro_get_one` 🎯 | 12.235 ms | 4.934 ms | 5.022 ms | 0.40× → **0.41×** | 1.018 (±0.045) | 0.998 | flat |
| `env/txn/rw_empty_commit` 🎯 | 772.955 µs | 1.235 ms | 1.226 ms | 1.60× → **1.59×** | 1.000 (±0.030) | 1.010 | flat |
| `put/api/plain` 🎯 | 15.357 ms | 13.045 ms | 12.930 ms | 0.85× → **0.84×** | 0.991 (±0.036) | 0.996 | flat |
| `put/api/reserved` 🎯 | 15.656 ms | 14.856 ms | 14.736 ms | 0.95× → **0.94×** | 0.991 (±0.030) | 1.000 | flat |
| `put/cursor/rewrite` 🎯 | 1.209 ms | 1.772 ms | 1.760 ms | 1.47× → **1.46×** | 0.993 (±0.030) | 1.002 | flat |
| `put/cursor/rewrite_reserved` 🎯 | 1.482 ms | 2.144 ms | 2.130 ms | 1.45× → **1.44×** | 0.995 (±0.089) | 0.941 | flat (layout offset, threshold widened) |
| `put/cursor/rewrite_reserved_ovf` 🎯 | 73.008 ms | 89.807 ms | 90.196 ms | 1.23× → **1.24×** | 1.009 (±0.034) | 1.008 | flat |
| `put/gc/drain_big` 🎯 | 28.459 ms | 26.693 ms | 26.410 ms | 0.94× → **0.93×** | 0.984 (±0.033) | 0.993 | flat |
| `put/gc/ovf_across` 🎯 | 3.219 ms | 4.713 ms | 4.668 ms | 1.46× → **1.45×** | 0.993 (±0.030) | 0.985 | flat |
| `put/gc/ovf_miss` 🎯 | 3.567 ms | 3.818 ms | 4.628 ms | 1.07× → **1.30×** | 1.229 (±0.300) | 1.025 | flat |
| `put/order/append` 🎯 | 6.222 ms | 8.586 ms | 8.498 ms | 1.38× → **1.37×** | 0.989 (±0.030) | 0.980 | flat |
| `put/order/rand` 🎯 | 28.662 ms | 25.798 ms | 25.833 ms | 0.90× → **0.90×** | 1.006 (±0.036) | 1.019 | flat |
| `put/order/seq` 🎯 | 15.364 ms | 13.021 ms | 12.858 ms | 0.85× → **0.84×** | 0.987 (±0.030) | 0.985 | flat |
| `put/over/grow` 🎯 | 7.781 ms | 8.439 ms | 8.404 ms | 1.08× → **1.08×** | 0.995 (±0.030) | 0.999 | flat |
| `put/over/same_size` 🎯 | 3.312 ms | 3.841 ms | 3.833 ms | 1.16× → **1.16×** | 0.999 (±0.030) | 1.004 | flat |
| `put/val/v256` 🎯 | 2.514 ms | 2.655 ms | 2.799 ms | 1.06× → **1.11×** | 1.045 (±0.101) | 1.001 | flat |
| `put/val/v2page` 🎯 | 31.066 ms | 23.888 ms | 25.443 ms | 0.77× → **0.82×** | 0.963 (±0.230) | 1.161 | ⚠ drift |
| `put/val/v2page_abort` 🎯 | 3.161 ms | 3.963 ms | 3.968 ms | 1.25× → **1.26×** | 1.001 (±0.030) | 0.995 | flat |
| `put/val/v4k` 🎯 | 24.686 ms | 24.547 ms | 24.564 ms | 0.99× → **1.00×** | 1.001 (±0.134) | 0.777 | ⚠ drift |
| `put/val/v8` 🎯 | 1.954 ms | 1.956 ms | 1.951 ms | 1.00× → **1.00×** | 1.012 (±0.062) | 0.997 | flat |

## 2. Option on, heap staging

One binary (`b0f3cbb`); after = `ZERODB_BENCH_INITIAL_MAP=65536`, so every
ZeroDB env of every iteration starts from a 64 KiB map and grows, under the
rung's map size as the ceiling. Verdict **flat**: 16 flat, 0 regressed, 5
unreliable.

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
| `commit/batch/n1` 🎯 | 21.040 ms | 19.143 ms | 22.421 ms | 0.91× → **1.07×** | 1.030 (±0.190) | 1.004 | flat |
| `commit/batch/n100` 🎯 | 8.233 ms | 7.840 ms | 8.931 ms | 0.95× → **1.08×** | 1.081 (±0.149) | 1.000 | flat |
| `commit/batch/n10k` 🎯 | 13.201 ms | 12.301 ms | 11.158 ms | 0.93× → **0.85×** | 0.996 (±0.175) | 1.013 | flat |
| `put/api/plain` 🎯 | 13.843 ms | 11.650 ms | 13.087 ms | 0.84× → **0.95×** | 0.998 (±0.207) | 0.999 | flat |
| `put/api/reserved` 🎯 | 14.172 ms | 12.617 ms | 14.781 ms | 0.89× → **1.04×** | 1.027 (±0.219) | 0.993 | flat |
| `put/cursor/rewrite` 🎯 | 1.161 ms | 1.580 ms | 1.643 ms | 1.36× → **1.41×** | 1.021 (±0.040) | 0.997 | flat |
| `put/cursor/rewrite_reserved` 🎯 | 1.315 ms | 1.849 ms | 2.008 ms | 1.41× → **1.53×** | 1.005 (±0.096) | 1.026 | flat |
| `put/cursor/rewrite_reserved_ovf` 🎯 | 72.013 ms | 92.941 ms | 90.532 ms | 1.29× → **1.26×** | 0.979 (±0.050) | 0.991 | flat |
| `put/gc/drain_big` 🎯 | 28.592 ms | 25.869 ms | 25.623 ms | 0.90× → **0.90×** | 1.004 (±0.052) | 0.996 | flat |
| `put/gc/ovf_across` 🎯 | 3.450 ms | 4.666 ms | 4.603 ms | 1.35× → **1.33×** | 0.997 (±0.136) | 0.983 | flat |
| `put/gc/ovf_miss` 🎯 | 3.501 ms | 4.874 ms | 3.880 ms | 1.39× → **1.11×** | 0.796 (±0.191) | 1.046 | ⚠ drift |
| `put/order/append` 🎯 | 6.164 ms | 8.742 ms | 8.650 ms | 1.42× → **1.40×** | 0.952 (±0.123) | 1.022 | flat |
| `put/order/rand` 🎯 | 28.174 ms | 25.827 ms | 25.961 ms | 0.92× → **0.92×** | 0.926 (±0.189) | 0.946 | ⚠ drift |
| `put/order/seq` 🎯 | 14.376 ms | 13.014 ms | 13.053 ms | 0.91× → **0.91×** | 0.891 (±0.161) | 1.064 | ⚠ drift |
| `put/over/grow` 🎯 | 7.154 ms | 8.472 ms | 8.486 ms | 1.18× → **1.19×** | 1.002 (±0.474) | 1.008 | flat |
| `put/over/same_size` 🎯 | 3.027 ms | 3.266 ms | 3.862 ms | 1.08× → **1.28×** | 1.004 (±0.241) | 0.982 | flat |
| `put/val/v256` 🎯 | 2.513 ms | 2.642 ms | 2.597 ms | 1.05× → **1.03×** | 0.916 (±0.094) | 0.997 | flat |
| `put/val/v2page` 🎯 | 25.860 ms | 24.950 ms | 22.812 ms | 0.96× → **0.88×** | 0.908 (±0.081) | 0.851 | ⚠ drift |
| `put/val/v2page_abort` 🎯 | 3.057 ms | 4.027 ms | 3.970 ms | 1.32× → **1.30×** | 0.979 (±0.096) | 0.997 | flat |
| `put/val/v4k` 🎯 | 27.224 ms | 25.267 ms | 24.680 ms | 0.93× → **0.91×** | 0.979 (±0.103) | 0.917 | ⚠ drift |
| `put/val/v8` 🎯 | 1.787 ms | 1.926 ms | 2.025 ms | 1.08× → **1.13×** | 1.024 (±0.682) | 1.001 | flat |

## 3. Option on, `WRITE_MAP` (in place)

As 2 with `ZERODB_BENCH_WRITE_MAP=1` for both engines. Verdict
**regressed**: 3 slower, 13 flat, 5 unreliable.

| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after | after ÷ before | LMDB drift | verdict |
| `commit/batch/n1` 🎯 | 2.468 ms | 5.084 ms | 5.290 ms | 2.06× → **2.14×** | 1.041 (±0.540) | 1.092 | ⚠ drift |
| `commit/batch/n100` 🎯 | 4.366 ms | 4.577 ms | 4.765 ms | 1.05× → **1.09×** | 1.108 (±1.432) | 1.011 | flat |
| `commit/batch/n10k` 🎯 | 10.800 ms | 9.479 ms | 10.605 ms | 0.88× → **0.98×** | 1.108 (±1.227) | 1.014 | flat |
| `put/api/plain` 🎯 | 10.758 ms | 9.621 ms | 9.935 ms | 0.89× → **0.92×** | 1.039 (±0.030) | 0.994 | ❌ slower |
| `put/api/reserved` 🎯 | 11.012 ms | 10.776 ms | 10.881 ms | 0.98× → **0.99×** | 1.010 (±0.089) | 1.003 | flat |
| `put/cursor/rewrite` 🎯 | 826.877 µs | 1.326 ms | 1.354 ms | 1.60× → **1.64×** | 1.021 (±0.910) | 1.011 | flat |
| `put/cursor/rewrite_reserved` 🎯 | 993.911 µs | 1.734 ms | 1.631 ms | 1.74× → **1.64×** | 1.142 (±0.619) | 1.000 | flat |
| `put/cursor/rewrite_reserved_ovf` 🎯 | 55.417 ms | 68.122 ms | 70.356 ms | 1.23× → **1.27×** | 1.102 (±0.584) | 1.010 | flat |
| `put/gc/drain_big` 🎯 | 17.590 ms | 14.829 ms | 15.368 ms | 0.84× → **0.87×** | 1.063 (±0.368) | 1.050 | ⚠ drift |
| `put/gc/ovf_across` 🎯 | 350.767 µs | 1.063 ms | 1.777 ms | 3.03× → **5.07×** | 1.670 (±0.352) | 1.013 | ❌ slower |
| `put/gc/ovf_miss` 🎯 | 3.024 ms | 3.335 ms | 3.122 ms | 1.10× → **1.03×** | 0.872 (±2.229) | 0.926 | ⚠ drift |
| `put/order/append` 🎯 | 4.051 ms | 5.821 ms | 6.321 ms | 1.44× → **1.56×** | 1.103 (±0.258) | 0.996 | flat |
| `put/order/rand` 🎯 | 20.851 ms | 21.453 ms | 23.210 ms | 1.03× → **1.11×** | 1.097 (±0.102) | 1.006 | flat |
| `put/order/seq` 🎯 | 10.618 ms | 8.429 ms | 9.575 ms | 0.79× → **0.90×** | 1.077 (±0.205) | 0.994 | flat |
| `put/over/grow` 🎯 | 5.986 ms | 6.677 ms | 7.172 ms | 1.12× → **1.20×** | 1.074 (±0.933) | 1.060 | ⚠ drift |
| `put/over/same_size` 🎯 | 2.572 ms | 2.862 ms | 2.878 ms | 1.11× → **1.12×** | 1.042 (±1.226) | 0.993 | flat |
| `put/val/v256` 🎯 | 1.927 ms | 2.252 ms | 2.149 ms | 1.17× → **1.11×** | 1.119 (±0.312) | 1.040 | flat (layout offset, threshold widened) |
| `put/val/v2page` 🎯 | 10.878 ms | 16.219 ms | 18.648 ms | 1.49× → **1.71×** | 1.137 (±0.354) | 1.059 | ⚠ drift |
| `put/val/v2page_abort` 🎯 | 10.966 ms | 15.303 ms | 15.917 ms | 1.40× → **1.45×** | 1.022 (±0.074) | 0.988 | flat |
| `put/val/v4k` 🎯 | 14.977 ms | 18.589 ms | 22.521 ms | 1.24× → **1.50×** | 1.212 (±0.070) | 0.986 | ❌ slower |
| `put/val/v8` 🎯 | 1.457 ms | 1.587 ms | 1.599 ms | 1.09× → **1.10×** | 1.152 (±0.256) | 1.007 | flat |

## Reading

- With the option off, nothing moved: the added code runs only where
  `MapFull` used to be returned and on a begin that would have returned
  `MapResized`.
- With the option on, these rungs are the worst case: each iteration builds a
  fresh environment, so it pays every growth from 64 KiB to the rung's data
  size, about nine of them, inside the timed region. In heap mode a growth is
  one lazy `mmap` and the cost disappears in the noise. Under `WRITE_MAP`
  a growth is a file extension (`ftruncate`), a new writable mapping, and
  page faults again on pages the txn already touched through the old mapping.
  That costs `put/gc/ovf_across` (overflow runs written in place) about
  0.7 ms per iteration, and `put/val/v4k` about 4 ms. Neither path changes
  once the map has reached its working size.
- Not measured here: Linux (`ftruncate` and fault costs differ by
  filesystem), and a long load that grows to several GiB against LMDB opened
  at the final size. Both belong to the quiet-host gate.
