# Task runner — install: cargo install just

check:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace

miri:
    cargo miri test -p zerodb-core

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
