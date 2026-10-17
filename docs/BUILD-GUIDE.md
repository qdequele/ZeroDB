# Build guidance for consumers

ZeroDB is a pure-Rust embedded key-value engine, a drop-in replacement for
LMDB at the API of `heed` (the Rust LMDB wrapper Meilisearch uses). Its first
consumers are Meilisearch's indexing and search core (`milli`) and its HNSW
vector index (`hannoy`).

Cargo applies a `[profile]` section only from the final binary's own
workspace, never from a dependency, so build-setting guidance belongs with
the consumer, not inside ZeroDB. This page records what is measured and what
is still open (tracked in issue
[#66](https://github.com/qdequele/ZeroDB/issues/66)).

In every ratio below, **ZeroDB time ÷ LMDB time**; below 1.00 means ZeroDB is
faster. See [`PERF-GAP-VS-LMDB.md`](PERF-GAP-VS-LMDB.md) for the full
methodology and [`benches/results/perf-ledger.jsonl`](../benches/results/perf-ledger.jsonl)
for every build-setting experiment, kept or reverted.

## Already settled

- **The benchmarks are optimized builds.** The microbench ladder in
  `crates/zerodb-oracle/benches/engine_comparison/` runs under Cargo's bench
  profile, never a debug build.
- **`codegen-units = 1`, the setting Meilisearch's release profile uses, is
  ZeroDB's worst case**, not its best. It measured slower than the default
  `codegen-units = 16` on most read benchmarks (issue #66). Rungs are read at
  both settings; `codegen-units = 16` is also hannoy's build
  (`PERF-GAP-VS-LMDB.md`, "How it is measured").
- **Fat LTO was tried and dropped from the build comparison**, because it
  also speeds up heed's own Rust wrapper around LMDB (LMDB's scan times
  dropped to 0.75–0.86 of their previous value in that experiment) and so
  stops separating the two engines (issue #66).
- **The LLVM inlining audit landed at the source level**, not as a build
  flag. Raising LLVM's inlining threshold showed that 8–38 % of ZeroDB's
  hot-path time came from functions LLVM declined to inline by default; an
  `nm` diff of builds with and without the raised threshold identified
  exactly which functions, and those are now marked `#[inline(always)]`
  (commit `eb450ac`). This improved 37 benchmarks at `codegen-units = 1` and
  31 at `codegen-units = 16`, with no regressions in either build — for
  example, at `codegen-units = 1`: hot point get 1.37× → 1.11×, first/last
  scan 2.22× → 1.56×, sequential put 1.23× → 1.10×
  (`PERF-GAP-VS-LMDB.md`, "Hot paths above LLVM's inlining threshold").
  Consumers get this without setting any flag.
- **Both consumer builds are benchmarked.** Before/after performance runs use
  `codegen-units = 1` (set with `CARGO_PROFILE_BENCH_CODEGEN_UNITS=1`) and
  the default 16; the result files in `benches/results/` record which
  setting was used.
- **No `[profile]` section is needed in ZeroDB's own root `Cargo.toml`.**
  Cargo ignores profiles declared by a dependency, so one there would not
  reach a Meilisearch or hannoy build. Pinning one setting for in-repo
  benchmarks would also hide the other consumer's build (issue #66).

## Graviton `target-cpu` (not measured yet)

AWS Graviton3 is `-C target-cpu=neoverse-v1`; Graviton4 is
`-C target-cpu=neoverse-v2`. Neither has been benchmarked against ZeroDB's
default build yet — **not measured**. When this is run, the recommended
procedure (per issue #66) is:

1. Build the engine microbench ladder (`just bench`) and a representative
   Meilisearch workload (`scripts/consumer.sh bench`, see
   [`CONSUMER-GATE.md`](CONSUMER-GATE.md)) three ways on the same commit: the
   unmodified LMDB control, ZeroDB's default build, and ZeroDB built with
   `RUSTFLAGS="-C target-cpu=neoverse-v1"` (or `neoverse-v2` on Graviton4).
2. Compare the target-cpu build against **both** the LMDB control and
   ZeroDB's own default build. LMDB's C core is compiled by its own C build
   and never sees Cargo's `RUSTFLAGS`, so a `target-cpu` win over LMDB could
   also just be a win any Rust binary gets on that CPU — the comparison
   against ZeroDB's own default build is what isolates ZeroDB's gain.
3. Document the flag here, with numbers and a link to the bench report in
   `benches/results/`, only if it measurably helps. If it does not, record
   that here too so the option is not re-tried from scratch.

## PGO/BOLT experiment (optional, not measured yet)

Issue #66 also tracks an optional experiment: train profile-guided
optimization (PGO), and optionally BOLT (binary layout optimization), on a
representative Meilisearch indexing and search workload, and report the gain
at `codegen-units = 1` (Meilisearch's release setting). This is consumer-side
build tooling — it would be documented here, not built into ZeroDB itself.
**Not measured yet**; no PGO or BOLT run exists in `benches/results/` or
`benches/results/perf-ledger.jsonl` as of this writing.

## See also

- [`PERF-GAP-VS-LMDB.md`](PERF-GAP-VS-LMDB.md) — the full LMDB-vs-ZeroDB
  performance inventory, including the LLVM inlining-threshold section.
- [`benches/results/perf-ledger.jsonl`](../benches/results/perf-ledger.jsonl)
  — every performance lever tried, kept or reverted.
- [`CONSUMER-GATE.md`](CONSUMER-GATE.md) — how the Meilisearch and hannoy
  consumer benchmarks are run.
- [ADOPTION-GUIDE.md](ADOPTION-GUIDE.md) — item 6 covers the allocator and
  `codegen-units` setting Meilisearch already uses in production.
