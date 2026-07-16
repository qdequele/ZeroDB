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

# Long fuzz for nightly CI (diff_dupsort target joins in Phase 2.8, see D-004)
fuzz-long:
    cargo +nightly fuzz run diff_ops -- -max_total_time=21600

# Crash-consistency smoke (exists from milestone 1.11)
crash-test-quick:
    cargo run -p zerodb-oracle --bin crash-harness -- --cycles 200

crash-test-full:
    cargo run -p zerodb-oracle --bin crash-harness -- --cycles 10000

bench-quick:
    cargo bench --workspace -- --quick
