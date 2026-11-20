#!/usr/bin/env bash
# hannoy (HNSW vector index) on ZeroDB — the second consumer.
#
#   scripts/hannoy.sh suites   # hannoy's own test suite on ZeroDB (+ no-LMDB check)
#   scripts/hannoy.sh bench    # `cargo bench --bench benchmark` (divan: build_hnsw /
#                              #   search_hnsw at 512/768/1536 dims) on LMDB then ZeroDB,
#                              #   medians compared
#
# Environment:
#   HANNOY_REF    git ref (default: v0.2.0, the hannoy release Meilisearch v1.55
#                 depends on and the latest on heed 0.22.1; v0.3.0 uses heed 0.23.
#                 The July 2026 hannoy numbers were taken on v0.1.7-nested-rtxns,
#                 whose own snapshot tests fail on any engine: they record
#                 version 0.1.6)
#   RUSTUP_TOOLCHAIN  toolchain for the build (default: stable; hannoy v0.2.0
#                 pins Rust 1.90, below ZeroDB's rust-version)
#   HANNOY_SRC    local checkout to `git clone --shared` from (never modified)
#   HANNOY_CLONE  reuse this clone
#   WORKDIR       scratch root (default: <zerodb>/target/consumer)
#   ROUNDS        bench rounds, order flipped on even rounds (default: 1)
#   BENCH_ARGS    extra args after `--` for divan (e.g. "build_hnsw" to filter)
#
# Same mechanism as scripts/consumer.sh: `[patch.crates-io] heed = { path =
# …/crates/heed-zerodb }` between marker lines, removed again for the stock build.
set -euo pipefail

ZERODB="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-suites}"
HANNOY_REF="${HANNOY_REF:-v0.2.0}"
export RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-stable}"
WORKDIR="${WORKDIR:-$ZERODB/target/consumer}"
CLONE="${HANNOY_CLONE:-$WORKDIR/hannoy}"
ROUNDS="${ROUNDS:-1}"
BENCH_ARGS="${BENCH_ARGS:-}"
MARK_BEGIN="# >>> zerodb consumer gate (scripts/hannoy.sh) >>>"
MARK_END="# <<< zerodb consumer gate <<<"

LOG_TAG=hannoy
# shellcheck source=scripts/lib/consumer-common.sh
source "$ZERODB/scripts/lib/consumer-common.sh"

# hannoy builds against stock heed 0.22.1: no feature to turn on.
heed_feature_manifest() { :; }

do_suites() {
    set_patch on
    log "cargo test on ZeroDB"
    run_tests
    assert_no_lmdb ""
}

run_bench() { # $1 = lmdb | zerodb, $2 = round
    local out="$WORKDIR/reports/hannoy/$1-round-$2.txt"
    mkdir -p "$(dirname "$out")"
    if [ "$1" = zerodb ]; then set_patch on; else set_patch stock; fi
    log "cargo bench --bench benchmark ($1, round $2) → $out"
    # shellcheck disable=SC2086
    (cd "$CLONE" && cargo bench --bench benchmark -- $BENCH_ARGS) | tee "$out"
    if [ "$1" = zerodb ]; then assert_no_lmdb ""; fi
}

do_bench() {
    for r in $(seq 1 "$ROUNDS"); do
        if [ $((r % 2)) -eq 1 ]; then
            run_bench lmdb "$r"; run_bench zerodb "$r"
        else
            run_bench zerodb "$r"; run_bench lmdb "$r"
        fi
    done
    log "comparison"
    python3 "$ZERODB/scripts/divan-compare.py" "$WORKDIR/reports/hannoy"
}

# Whatever happens, leave the clone with its stock manifest and keep the real
# exit status.
cleanup() {
    local rc=$?
    set_patch off
    exit "$rc"
}

ensure_clone "$HANNOY_REF" "${HANNOY_SRC:-}" https://github.com/nnethercott/hannoy
report_versions hannoy "$HANNOY_REF"
trap cleanup EXIT
case "$MODE" in
    suites) do_suites ;;
    bench)  do_bench ;;
    *) echo "usage: $0 {suites|bench}" >&2; exit 2 ;;
esac
log "done ($MODE); clone left at $CLONE with its stock Cargo.toml"
