#!/usr/bin/env bash
# Profile one engine on one ladder rung, so a perf lever is picked from where the
# time actually goes rather than from a guess.
#
#   scripts/bench-profile.sh <rung> [engine]    # e.g. del/range/half  (engine: zerodb|lmdb)
#
# Runs the working tree's bench binary with criterion's --profile-time (the rung
# body in a loop, no statistics), under the first profiler found:
#   samply   (cargo install samply) — writes a Firefox-profiler JSON
#   sample   (macOS built-in)       — writes a text call tree
#   perf     (Linux)                — writes perf.data plus a `perf report` text dump
# Profiling LMDB on the same rung is the fastest way to see what it does not do.
# Rungs that rebuild a fixture per iteration (the `heavy` ones: del/*, put/val/*,
# maint/*) profile that setup too. Discount the fixture's bulk_put frames and
# read only the operation under test.
#
# Environment: SECONDS_ (profile duration, default 15), OUT (default
# target/bench-profile/<rung>-<engine>).
set -euo pipefail

ZERODB="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNG="${1:?usage: bench-profile.sh <rung> [zerodb|lmdb]}"
ENGINE="${2:-zerodb}"
DUR="${SECONDS_:-15}"
OUT="${OUT:-$ZERODB/target/bench-profile/${RUNG//\//_}-$ENGINE}"
mkdir -p "$OUT"

# Debug symbols without changing optimisation: frames must be attributable. A
# separate target dir, so this build never evicts the ones bench-ab reuses.
export CARGO_PROFILE_BENCH_DEBUG=line-tables-only
export CARGO_TARGET_DIR="$ZERODB/target/profile-build"
EXE="$(cd "$ZERODB" && cargo bench -q -p zerodb-oracle --bench engine_comparison --no-run \
    --message-format=json |
    jq -r 'select(.reason == "compiler-artifact" and .target.name == "engine_comparison"
                  and .executable != null) | .executable' | tail -1)"
FILTER="^${RUNG}/${ENGINE}\$"
ARGS=(--bench --profile-time "$DUR" "$FILTER")

if command -v samply >/dev/null; then
    samply record --save-only -o "$OUT/profile.json.gz" -- "$EXE" "${ARGS[@]}"
    echo "profile: $OUT/profile.json.gz  (open with: samply load $OUT/profile.json.gz)"
elif command -v sample >/dev/null; then
    "$EXE" "${ARGS[@]}" >"$OUT/bench.log" 2>&1 &
    PID=$!
    # Attach at once and cover the whole run. Criterion's "Profiling" progress
    # line has no newline, so it stays buffered until the run ends; waiting for
    # it attached after exit and produced an empty call graph.
    sample "$PID" "$((DUR + 5))" -mayDie -file "$OUT/sample.txt" >/dev/null 2>&1 || true
    wait "$PID" || true
    # `sample` leaves Rust's v0 symbols mangled (`_RNv...11remove_cell`).
    if command -v rustfilt >/dev/null; then
        rustfilt -i "$OUT/sample.txt" -o "$OUT/sample.demangled.txt" && mv "$OUT/sample.demangled.txt" "$OUT/sample.txt"
    else
        echo "note: symbols are v0-mangled; \`cargo install rustfilt\` makes them readable" >&2
    fi
    echo "profile: $OUT/sample.txt  (start at 'Sort by top of stack' for self time, then the 'Call graph')"
elif command -v perf >/dev/null; then
    perf record -g -o "$OUT/perf.data" -- "$EXE" "${ARGS[@]}"
    perf report -i "$OUT/perf.data" --stdio --no-children --percent-limit 1 >"$OUT/report.txt"
    echo "profile: $OUT/report.txt"
else
    echo "no profiler found (install samply: cargo install samply)" >&2
    exit 1
fi
