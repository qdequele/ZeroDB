# Task runner — install: cargo install just

# fmt, clippy and the test suite: the first three checks of CONTRIBUTING.md.
check:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace

# The core's non-mmap logic under miri (nightly).
miri:
    cargo miri test -p zerodb-core

# Only the loom_* tests run; the rest of the lib is compiled but filtered under
# --cfg loom (loom primitives panic outside a model). --release per ADR-0006,
# with debug assertions forced on: the pin path's slot ≤ snap debug_assert is
# one of the suite's publish-inversion detectors.
#
# Loom model check of the reader table (ADR-0006).
loom:
    RUSTFLAGS="--cfg loom" CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true cargo test -p zerodb-core --lib --release loom_

# Unoptimized on purpose: debug assertions keep the writer-side GC shadow gate
# armed. A ~5 s variant runs in the normal `cargo test` suite; this one runs
# nightly in CI and before a consumer integration gate (ADR-0006).
#
# 3-minute reader/writer stress, in one process and across processes.
stress:
    ZERODB_STRESS_SECS=180 cargo test -p zerodb --test reader_stress -- --nocapture stress_
    ZERODB_STRESS_SECS=180 cargo test -p zerodb --test multi_process_stress -- --nocapture stress_

# cargo-fuzz needs nightly. 10 min LMDB vs ZeroDB, then 2 min each of page
# decoding, hostile-image open, and LMDB vs ZeroDB through the heed adapter.
#
# Fuzz gate for every change (CI runs it on every PR).
fuzz-quick:
    cargo +nightly fuzz run diff_ops -- -max_total_time=600
    cargo +nightly fuzz run fuzz_page_decode -- -max_total_time=120
    cargo +nightly fuzz run fuzz_image_open -- -max_total_time=120
    ZERODB_FUZZ_PAIR=heed cargo +nightly fuzz run diff_ops -- -max_total_time=120

# Nightly CI runs `just fuzz-long 12600` to fit GitHub's 6 h job cap; the 24 h
# soak a consumer integration gate asks for needs a self-hosted box.
#
# Long differential fuzz: SECS native, then SECS/8 through the heed adapter.
fuzz-long SECS='21600':
    cargo +nightly fuzz run diff_ops -- -max_total_time={{SECS}}
    ZERODB_FUZZ_PAIR=heed cargo +nightly fuzz run diff_ops -- -max_total_time=$(({{SECS}} / 8))

# Example: just fuzz-decode fuzz/artifacts/diff_ops/crash-…
#
# Print the engine mode and op sequence a diff_ops fuzz artifact decodes to.
fuzz-decode ARTIFACT:
    cargo run -q -p zerodb-oracle --example decode_artifact -- '{{ARTIFACT}}'

# Crash-consistency smoke, run by CI on every PR.
crash-test-quick:
    cargo run -p zerodb-oracle --bin crash-harness -- --cycles 200
    cargo run -p zerodb-oracle --bin crash-harness -- --mechanism mp --cycles 50

# Multi-process crash cycles: writer killed mid-commit, reader live (ADR-0023).
crash-test-mp CYCLES='500':
    cargo run -p zerodb-oracle --bin crash-harness -- --mechanism mp --cycles {{CYCLES}}

# The full crash run, nightly in CI.
crash-test-full:
    cargo run -p zerodb-oracle --bin crash-harness -- --cycles 10000

# --- Engine comparison: zerodb vs the LMDB fork (heed), both in one binary ---
#
# The bench is a LADDER: adjacent rungs differ by exactly one mechanism, so a
# ratio that jumps between two rungs names the cost. docs/PERFORMANCE.md maps
# every rung to the mechanism it isolates and the docs/PERFORMANCE.md item it
# implicates. macOS numbers are indicative; Graviton + EBS gp3 is the referee.
#
#   just bench            the whole ladder
#   just bench get        one suite (`just bench-list` for the names)
#   just bench-report     the LMDB-vs-ZeroDB ratio table from the last run

# Run the comparison ladder; pass a suite name to run only that suite.
bench SUITE='':
    cargo bench -p zerodb-oracle --bench engine_comparison -- '^{{SUITE}}'

# List the suite names `just bench <suite>` accepts.
bench-list:
    @echo "env get scan seek put del commit mixed concurrent maint"

# The ladder plus the long tier: the 1M-entry depth rung and the concurrent suite.
bench-long SUITE='':
    ZERODB_BENCH_TIER=long cargo bench -p zerodb-oracle --bench engine_comparison -- '^{{SUITE}}'

# Fast indicative pass (criterion --quick): checks a rung runs, proves no number.
bench-quick SUITE='':
    cargo bench -p zerodb-oracle --bench engine_comparison -- --quick '^{{SUITE}}'

# Ratio table from the last run: zerodb/lmdb per rung, plus ladder-family deltas.
bench-report *ARGS:
    scripts/bench-report.py {{ARGS}}

# --- Before/after: the perf-iteration loop ---
#
#   just bench-ab '^del/'        ZeroDB at BASE (default HEAD) vs the working tree,
#                                interleaved ROUNDS (default 3), LMDB as drift control;
#                                prints the three-column table, writes verdict.json
#   just bench-gate              the whole ladder vs the merge base with main
#   just bench-profile del/range/half [lmdb]    where the time goes, one rung
#   just perf-ledger show        prior attempts; read before picking a lever

# Before/after A/B on a criterion regex (env: BASE ROUNDS TARGET MIN_EFFECT MAX_DRIFT).
bench-ab FILTER='':
    scripts/bench-ab.sh '{{FILTER}}'

# Branch-level gate: every rung, before = merge base with main, one round.
bench-gate:
    BASE="$(git merge-base HEAD main)" ROUNDS="${ROUNDS:-1}" scripts/bench-ab.sh ''

# Profile one rung on one engine (samply, else macOS `sample`, else perf).
bench-profile RUNG ENGINE='zerodb':
    scripts/bench-profile.sh '{{RUNG}}' '{{ENGINE}}'

# The perf-attempt ledger (benches/results/perf-ledger.jsonl).
perf-ledger *ARGS:
    scripts/perf-ledger.py {{ARGS}}

# MEILISEARCH_REF, MEILISEARCH_SRC, WORKLOADS and ROUNDS are documented in the
# header of scripts/consumer.sh.
#
# Consumer gate: milli and index-scheduler build on ZeroDB, with no C LMDB left.
consumer-check:
    scripts/consumer.sh check

# consumer-check, plus the milli and index-scheduler test suites on ZeroDB.
consumer-suites:
    scripts/consumer.sh suites

# Same workloads, two binaries (stock LMDB vs ZeroDB), spans compared per run.
consumer-bench:
    scripts/consumer.sh bench

# hannoy (HNSW vector index) test suite on ZeroDB.
hannoy-suites:
    scripts/hannoy.sh suites

# hannoy divan build/search benches on both engines.
hannoy-bench:
    scripts/hannoy.sh bench
