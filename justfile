# Task runner — install: cargo install just

check:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace

miri:
    cargo miri test -p zerodb-core

# Loom model-checking suite for the reader table (M1.8; ADR-0006 L1–L5).
# Only the loom_* tests run; the rest of the lib is compiled-but-filtered
# under --cfg loom (loom primitives panic if used outside a model).
# --release per the ratified ADR text; debug assertions stay force-enabled
# because the pin path's slot≤snap debug_assert is one of the suite's
# publish-inversion detectors.
loom:
    RUSTFLAGS="--cfg loom" CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true cargo test -p zerodb-core --lib --release loom_

# M1.8 reader/writer stress, minutes-long variant (nightly CI + mandatory
# before the 1.14 gate — ADR-0006). Unoptimized on purpose: debug_assertions
# keep the writer-side GC shadow gate armed. The ~5s default variant runs in
# the normal `cargo test` suite.
stress:
    ZERODB_STRESS_SECS=180 cargo test -p zerodb --test reader_stress -- --nocapture stress_

# 10-minute differential fuzz (gate for every milestone; cargo-fuzz needs nightly)
fuzz-quick:
    cargo +nightly fuzz run diff_ops -- -max_total_time=600

# Long fuzz for nightly CI (6 h; PLAN 1.14 asks for a 24 h soak — run it on a
# self-hosted box or chain runs. A dup fuzz target only arrives if 2.8 resumes.)
fuzz-long:
    cargo +nightly fuzz run diff_ops -- -max_total_time=21600

# Crash-consistency smoke (exists from milestone 1.11)
crash-test-quick:
    cargo run -p zerodb-oracle --bin crash-harness -- --cycles 200

crash-test-full:
    cargo run -p zerodb-oracle --bin crash-harness -- --cycles 10000

# Dual-backend comparison: zerodb vs the LMDB fork (heed), both in one binary.
# `bench` = full criterion run; `bench-quick` = fast, fewer samples (indicative).
# macOS is indicative; run on Graviton + EBS gp3 for the representative numbers.
bench:
    cargo bench -p zerodb-oracle --bench engine_comparison

bench-quick:
    cargo bench -p zerodb-oracle --bench engine_comparison -- --quick

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
