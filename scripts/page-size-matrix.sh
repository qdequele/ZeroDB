#!/usr/bin/env bash
# Page-size matrix: the test suite, the crash harness at database pages of 4,
# 16 and 64 KiB, the stress tests and a fuzz pass, on whatever kernel runs it.
# Its purpose is a host whose OS page size is not 4 KiB: a 64 KiB-page aarch64
# kernel above all, or a 16 KiB one (Apple silicon, some aarch64 Linux).
# docs/maintainers/HARDENING-RUNS.md says which machines to use.
#
#   scripts/page-size-matrix.sh
#   SKIP_FUZZ=1 scripts/page-size-matrix.sh     # without nightly / cargo-fuzz
#
# Steps, in order; a failing step is recorded and the next one still runs:
#   1. cargo test --workspace: every test, including the OS-page-size battery
#      (crates/zerodb/tests/os_page_size.rs: DB pages of 4, 16 and 64 KiB in
#      every backing) and the heed adapter, whose default page size is the OS
#      page size
#   2. the crash harness, CRASH_CYCLES single-process and CRASH_MP_CYCLES
#      multi-process cycles at each DB page size (ZERODB_CRASH_PAGE_SIZE)
#   3. the reader/writer stress tests, STRESS_SECS each
#   4. the fuzz gate: diff_ops native and through the heed adapter, the two
#      decoder targets (FUZZ_SECS each)
#
# Environment:
#   OUT              output directory (default: target/page-size-matrix/<UTC timestamp>)
#   CRASH_CYCLES     single-process crash cycles per page size (default: 2000)
#   CRASH_MP_CYCLES  multi-process crash cycles per page size (default: 200)
#   STRESS_SECS      seconds per stress test (default: 180, as `just stress`)
#   FUZZ_SECS        seconds per fuzz target (default: 600)
#   SKIP_FUZZ        set to skip step 4 (recorded in the summary)
#
# Output, under OUT: summary.md (the text a docs/PROVENANCE.md entry is written
# from), one log per step, fuzz/<job>/artifacts, crash/target/crash-repro.
# Exit status: 0 when every step that ran passed.
set -euo pipefail

ZERODB="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_TAG=page-size-matrix
OUT="${OUT:-$ZERODB/target/page-size-matrix/$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
# shellcheck source=lib/hardening-common.sh
source "$ZERODB/scripts/lib/hardening-common.sh"

CRASH_CYCLES="${CRASH_CYCLES:-2000}"
CRASH_MP_CYCLES="${CRASH_MP_CYCLES:-200}"
STRESS_SECS="${STRESS_SECS:-180}"
FUZZ_SECS="${FUZZ_SECS:-600}"
SKIP_FUZZ="${SKIP_FUZZ:-}"
OS_PAGE="$(os_page_size)"
START="$(now)"

write_host_info "$OUT/host.md"
log "OS page size $OS_PAGE bytes; output in $OUT"
[ "$OS_PAGE" -ne 4096 ] || log "note: this host has 4 KiB OS pages; the run is valid, but it is not the 16 or 64 KiB data point"

ROWS=()
FAILED=0
# step NAME LOG CMD...: run CMD from the repository root, log to LOG, record
# a summary row.
step() {
    local name="$1" logf="$2" t0 rc=0
    shift 2
    t0="$(date +%s)"
    log "$name"
    (cd "$ZERODB" && "$@") >"$logf" 2>&1 || rc=$?
    [ "$rc" -eq 0 ] || FAILED=$((FAILED + 1))
    ROWS+=("| $name | $([ "$rc" -eq 0 ] && echo pass || echo "FAIL (exit $rc)") | $(($(date +%s) - t0)) s | \`$(basename "$logf")\` |")
}

# 1. The whole test suite.
step "cargo test --workspace" "$OUT/test.log" cargo test --workspace

# The OS-page-size battery's own lines, so the summary shows it ran here.
grep -a -E '^OS page size|os_page_size|^test (growth_resize|small_commits|a_store_shorter|a_reader_spans)' \
    "$OUT/test.log" >"$OUT/os-page-size-battery.txt" 2>/dev/null || true

# 2. The crash harness at each DB page size. Repro artifacts land in
# $OUT/crash/target/crash-repro.
cargo build -p zerodb-oracle --bin crash-harness --manifest-path "$ZERODB/Cargo.toml" >>"$OUT/run.log" 2>&1
mkdir -p "$OUT/crash"
crash_at() {
    local ps="$1"
    cd "$OUT/crash"
    ZERODB_CRASH_PAGE_SIZE="$ps" "$ZERODB/target/debug/crash-harness" --cycles "$CRASH_CYCLES"
    ZERODB_CRASH_PAGE_SIZE="$ps" "$ZERODB/target/debug/crash-harness" --mechanism mp --cycles "$CRASH_MP_CYCLES"
}
for ps in 4096 16384 65536; do
    step "crash harness, $((ps / 1024)) KiB DB pages" "$OUT/crash-$ps.log" crash_at "$ps"
done

# 3. The stress tests, unoptimized, as `just stress`.
step "reader/writer stress, ${STRESS_SECS} s each" "$OUT/stress.log" \
    env ZERODB_STRESS_SECS="$STRESS_SECS" bash -c \
    'cargo test -p zerodb --test reader_stress -- --nocapture stress_ &&
     cargo test -p zerodb --test multi_process_stress -- --nocapture stress_'

# 4. The fuzz gate.
FUZZ_ROWS=()
if [ -n "$SKIP_FUZZ" ]; then
    ROWS+=("| fuzz | skipped (SKIP_FUZZ) | | |")
else
    FUZZ_BIN="$(fuzz_build 2>>"$OUT/run.log")"
    fuzz_one() {
        local name="$1" target="$2"
        shift 2
        local dir="$OUT/fuzz/$name"
        mkdir -p "$dir/corpus" "$dir/artifacts"
        cd "$dir"
        env "$@" "$FUZZ_BIN/$target" -max_total_time="$FUZZ_SECS" -print_final_stats=1 \
            -artifact_prefix="$dir/artifacts/" "$dir/corpus"
    }
    mkdir -p "$OUT/fuzz"
    for job in "diff_ops-native diff_ops" "diff_ops-heed diff_ops ZERODB_FUZZ_PAIR=heed" \
        "fuzz_page_decode fuzz_page_decode" "fuzz_image_open fuzz_image_open"; do
        # shellcheck disable=SC2086 # the job string is split on purpose
        set -- $job
        step "fuzz $1, ${FUZZ_SECS} s" "$OUT/fuzz/$1.log" fuzz_one "$@"
        FUZZ_ROWS+=("| $1 | $(fuzz_execs "$OUT/fuzz/$1.log") | $(fuzz_findings "$OUT/fuzz/$1/artifacts" | wc -l | tr -d ' ') |")
    done
fi

END="$(now)"
{
    echo "# ZeroDB page-size matrix, $START to $END"
    echo
    echo "OS page size: **$OS_PAGE bytes**. Command: \`scripts/page-size-matrix.sh\`" \
        "(CRASH_CYCLES=$CRASH_CYCLES, CRASH_MP_CYCLES=$CRASH_MP_CYCLES," \
        "STRESS_SECS=$STRESS_SECS, FUZZ_SECS=$FUZZ_SECS${SKIP_FUZZ:+, SKIP_FUZZ=1})."
    echo
    cat "$OUT/host.md"
    echo
    echo "| Step | Result | Time | Log |"
    echo "|---|---|---|---|"
    printf '%s\n' "${ROWS[@]}"
    echo
    echo "Test totals: $(grep -a -E '^test result:' "$OUT/test.log" |
        awk '{p += $4; f += $6; i += $8} END {printf "%d passed, %d failed, %d ignored", p, f, i}')"
    echo
    echo "Crash cycles verified:"
    for ps in 4096 16384 65536; do
        echo "- $((ps / 1024)) KiB DB pages: $(crash_verified "$OUT/crash-$ps.log")"
    done
    if [ "${#FUZZ_ROWS[@]}" -gt 0 ]; then
        echo
        echo "| Fuzz job | Inputs executed | Findings |"
        echo "|---|---|---|"
        printf '%s\n' "${FUZZ_ROWS[@]}"
    fi
    echo
    echo "## Verdict: $([ "$FAILED" -eq 0 ] && echo pass || echo "fail ($FAILED steps)")"
} >"$OUT/summary.md"
log "summary: $OUT/summary.md"
cat "$OUT/summary.md"
[ "$FAILED" -eq 0 ]
