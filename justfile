# Task runner — install: cargo install just

check:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace

miri:
    cargo miri test -p zerodb-core

# Loom model-checking suite for the reader table (ADR-0006 L1–L5).
# Only the loom_* tests run; the rest of the lib is compiled-but-filtered
# under --cfg loom (loom primitives panic if used outside a model).
# --release per the ratified ADR text; debug assertions stay force-enabled
# because the pin path's slot≤snap debug_assert is one of the suite's
# publish-inversion detectors.
loom:
    RUSTFLAGS="--cfg loom" CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true cargo test -p zerodb-core --lib --release loom_

# Reader/writer stress, minutes-long variant (nightly CI + mandatory before
# a consumer integration gate — ADR-0006). Unoptimized on purpose: debug_assertions
# keep the writer-side GC shadow gate armed. The ~5s default variant runs in
# the normal `cargo test` suite.
stress:
    ZERODB_STRESS_SECS=180 cargo test -p zerodb --test reader_stress -- --nocapture stress_

# Differential fuzz gate for every change (10 min diff_ops + 2 min hostile-image
# open; cargo-fuzz needs nightly)
fuzz-quick:
    cargo +nightly fuzz run diff_ops -- -max_total_time=600
    cargo +nightly fuzz run fuzz_image_open -- -max_total_time=120

# Long fuzz for nightly CI (6 h; the consumer integration gate asks for a 24 h
# soak — run it on a self-hosted box or chain runs. A dup fuzz target only
# arrives if the parked DUPSORT work resumes.)
fuzz-long:
    cargo +nightly fuzz run diff_ops -- -max_total_time=21600

# Crash-consistency smoke
crash-test-quick:
    cargo run -p zerodb-oracle --bin crash-harness -- --cycles 200

crash-test-full:
    cargo run -p zerodb-oracle --bin crash-harness -- --cycles 10000

# --- Engine comparison: zerodb vs the LMDB fork (heed), both in one binary ---
#
# The bench is a LADDER: adjacent rungs differ by exactly one mechanism, so a
# ratio that jumps between two rungs names the cost. docs/BENCH-MAP.md maps
# every rung to the mechanism it isolates and the PERF-GAP-VS-LMDB item it
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

# Consumer gate — Meilisearch on ZeroDB (scripts/consumer.sh; MEILISEARCH_REF,
# MEILISEARCH_SRC, WORKLOADS, ROUNDS documented in the script header).
consumer-check:
    scripts/consumer.sh check

consumer-suites:
    scripts/consumer.sh suites

# Same workloads, two binaries (stock LMDB vs ZeroDB), spans compared per run.
consumer-bench:
    scripts/consumer.sh bench

# hannoy (HNSW) on ZeroDB — suite, and divan build/search benches on both engines.
hannoy-suites:
    scripts/hannoy.sh suites

hannoy-bench:
    scripts/hannoy.sh bench
