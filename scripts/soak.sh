#!/usr/bin/env bash
# Hardening soak: the differential fuzz targets, the crash harness and the
# reader/writer stress tests, all running side by side for HOURS on one
# dedicated machine, with sequence counts and every finding collected.
#
#   scripts/soak.sh              # 24 hours
#   HOURS=0.1 CRASH_ROUND_CYCLES=200 scripts/soak.sh   # a 6-minute rehearsal
#
# What runs, in parallel, until the deadline:
#   - diff_ops, ZeroDB native vs LMDB, libFuzzer fork mode on FUZZ_JOBS workers
#   - diff_ops through the heed adapter (ZERODB_FUZZ_PAIR=heed), HEED_JOBS workers
#   - fuzz_page_decode and fuzz_image_open, one worker each
#   - the crash harness in rounds of CRASH_ROUND_CYCLES single-process and a
#     twentieth as many multi-process cycles, CRASH_JOBS threads, each round
#     on the next page
#     size of: the seed's own (4 or 8 KiB), 16 KiB, 64 KiB
#   - the in-process and cross-process reader/writer stress tests in rounds of
#     at most STRESS_ROUND_SECS, unoptimized (debug assertions on)
# Fuzzing keeps going after a finding (-fork with -ignore_crashes), so one bug
# does not end the soak; every input that failed is kept.
#
# Environment:
#   HOURS              run length (default: 24; fractions are fine)
#   OUT                output directory (default: target/soak/<UTC timestamp>)
#   FUZZ_JOBS          diff_ops native workers (default: half the CPUs)
#   HEED_JOBS          diff_ops heed workers (default: an eighth of the CPUs, min 1)
#   CRASH_JOBS         crash-harness threads (default: a quarter of the CPUs, min 1)
#   CRASH_ROUND_CYCLES single-process crash cycles per round (default: 10000,
#                      the nightly full run; a rehearsal wants a few hundred)
#   STRESS_ROUND_SECS  longest stress round (default: 1800)
#   SEED               crash-harness base seed (default: from the clock; printed)
#
# Needs: nightly with cargo-fuzz 0.13.2, a C compiler (the oracle builds LMDB),
# and the disk the run directory sits on to have a few GB free.
#
# Output, under OUT:
#   summary.md         host, commit, start/end, sequence counts, verdict: the
#                      text a docs/PROVENANCE.md entry is written from
#   run.log            this script's own log
#   fuzz/<job>.log     libFuzzer output; fuzz/<job>/corpus, fuzz/<job>/artifacts
#   crash/crash.log    every crash-harness round; crash/target/crash-repro holds
#                      the repro artifacts of any violation
#   stress/stress.log  every stress round
# Exit status: 0 when every component finished without a finding.
set -euo pipefail

ZERODB="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_TAG=soak
HOURS="${HOURS:-24}"
OUT="${OUT:-$ZERODB/target/soak/$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
# shellcheck source=lib/hardening-common.sh
source "$ZERODB/scripts/lib/hardening-common.sh"

CPUS="$(cpu_count)"
at_least_1() { if [ "$1" -lt 1 ]; then echo 1; else echo "$1"; fi; }
FUZZ_JOBS="${FUZZ_JOBS:-$(at_least_1 $((CPUS / 2)))}"
HEED_JOBS="${HEED_JOBS:-$(at_least_1 $((CPUS / 8)))}"
CRASH_JOBS="${CRASH_JOBS:-$(at_least_1 $((CPUS / 4)))}"
CRASH_ROUND_CYCLES="${CRASH_ROUND_CYCLES:-10000}"
CRASH_MP_CYCLES="$(at_least_1 $((CRASH_ROUND_CYCLES / 20)))"
STRESS_ROUND_SECS="${STRESS_ROUND_SECS:-1800}"
SEED="${SEED:-$(date +%s)}"
SECS="$(awk -v h="$HOURS" 'BEGIN {printf "%d", h * 3600}')"
[ "$SECS" -ge 60 ] || { echo "HOURS=$HOURS is under a minute" >&2; exit 2; }

write_host_info "$OUT/host.md"
log "soak for ${HOURS} h (${SECS} s) into $OUT; $(os_page_size)-byte OS pages, $CPUS CPUs"
log "workers: diff_ops native $FUZZ_JOBS, heed $HEED_JOBS, crash harness $CRASH_JOBS; crash seed $SEED"

# --- Build everything first, so the clock covers running only. ---
log "building the fuzz targets, the crash harness and the stress tests"
FUZZ_BIN="$(fuzz_build)"
(cd "$ZERODB" && cargo build -p zerodb-oracle --bin crash-harness)
CRASH_BIN="$ZERODB/target/debug/crash-harness"
(cd "$ZERODB" && cargo test -p zerodb --test reader_stress --test multi_process_stress --no-run)

# The clock starts once everything is built.
START="$(now)"
DEADLINE=$(($(date +%s) + SECS))
remaining() { echo $((DEADLINE - $(date +%s))); }

# --- Fuzzing ---
mkdir -p "$OUT/fuzz"
# run_fuzz NAME TARGET WORKERS [ENV=VALUE...]: one libFuzzer run until the
# deadline, with its own corpus (seeded from fuzz/corpus/TARGET when present)
# and artifact directory.
run_fuzz() {
    local name="$1" target="$2" workers="$3"
    shift 3
    local dir="$OUT/fuzz/$name"
    mkdir -p "$dir/corpus" "$dir/artifacts"
    if [ -d "$ZERODB/fuzz/corpus/$target" ]; then
        cp -R "$ZERODB/fuzz/corpus/$target/." "$dir/corpus/" 2>/dev/null || true
    fi
    # Fork mode even for one worker: each child process lives at most 300 s,
    # so the sanitizer's own memory, which grows with the number of inputs
    # run in one process, never reaches the RSS limit over a long run. An
    # input that does exhaust memory is still written as an oom-* artifact.
    local mode=(-fork="$workers" -ignore_crashes=1 -ignore_timeouts=1 -ignore_ooms=1)
    (
        cd "$dir"
        env "$@" "$FUZZ_BIN/$target" "${mode[@]}" \
            -max_total_time="$(remaining)" -print_final_stats=1 \
            -artifact_prefix="$dir/artifacts/" "$dir/corpus"
    ) >"$OUT/fuzz/$name.log" 2>&1
}

PIDS=()
NAMES=()
spawn() {
    local name="$1"
    shift
    "$@" &
    PIDS+=("$!")
    NAMES+=("$name")
}

spawn diff_ops-native run_fuzz diff_ops-native diff_ops "$FUZZ_JOBS"
spawn diff_ops-heed run_fuzz diff_ops-heed diff_ops "$HEED_JOBS" ZERODB_FUZZ_PAIR=heed
spawn fuzz_page_decode run_fuzz fuzz_page_decode fuzz_page_decode 1
spawn fuzz_image_open run_fuzz fuzz_image_open fuzz_image_open 1

# --- Crash harness rounds ---
crash_loop() {
    # An empty size leaves ZERODB_CRASH_PAGE_SIZE unset: the seed picks.
    local dir="$OUT/crash" round=0 failed=0 sizes=("" 16384 65536)
    mkdir -p "$dir"
    cd "$dir"
    while [ "$(remaining)" -gt 60 ]; do
        # The harness derives cycle seeds as splitmix64(base ^ cut * 0x9E3779B9),
        # which stays below 2^48 for any round size used here; giving each
        # round its own high bits keeps the rounds' cycle seeds disjoint.
        local ps="${sizes[$((round % 3))]}" seed=$((SEED ^ (round << 48)))
        {
            echo "=== round $round, seed $seed, page size ${ps:-from seed}, $(now)"
            env ${ps:+ZERODB_CRASH_PAGE_SIZE=$ps} "$CRASH_BIN" --cycles "$CRASH_ROUND_CYCLES" --seed "$seed" \
                --jobs "$CRASH_JOBS" --keep-going || failed=$((failed + 1))
            echo "=== round $round multi-process, seed $seed, $(now)"
            env ${ps:+ZERODB_CRASH_PAGE_SIZE=$ps} "$CRASH_BIN" --mechanism mp --cycles "$CRASH_MP_CYCLES" \
                --seed "$seed" --jobs "$CRASH_JOBS" --keep-going || failed=$((failed + 1))
        } >>"$dir/crash.log" 2>&1
        round=$((round + 1))
    done
    echo "$round $failed" >"$dir/rounds"
    [ "$failed" -eq 0 ]
}
spawn crash crash_loop

# --- Stress rounds ---
stress_loop() {
    local dir="$OUT/stress" round=0 failed=0
    mkdir -p "$dir"
    cd "$ZERODB"
    while [ "$(remaining)" -gt 60 ]; do
        local secs
        secs="$(remaining)"
        secs=$(((secs - 30) / 2))
        [ "$secs" -gt "$STRESS_ROUND_SECS" ] && secs="$STRESS_ROUND_SECS"
        [ "$secs" -lt 10 ] && secs=10
        {
            echo "=== round $round, ${secs} s per test, $(now)"
            ZERODB_STRESS_SECS="$secs" cargo test -p zerodb --test reader_stress \
                -- --nocapture stress_ || failed=$((failed + 1))
            ZERODB_STRESS_SECS="$secs" cargo test -p zerodb --test multi_process_stress \
                -- --nocapture stress_ || failed=$((failed + 1))
        } >>"$dir/stress.log" 2>&1
        round=$((round + 1))
    done
    echo "$round $failed" >"$dir/rounds"
    [ "$failed" -eq 0 ]
}
spawn stress stress_loop

log "running until $(date -u -r "$DEADLINE" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d "@$DEADLINE" +%Y-%m-%dT%H:%M:%SZ)"
STATUS=()
for i in "${!PIDS[@]}"; do
    if wait "${PIDS[$i]}"; then STATUS+=(0); else STATUS+=($?); fi
    log "${NAMES[$i]} finished (exit ${STATUS[$i]})"
done
END="$(now)"

# --- Summary ---
VERDICT=pass
{
    echo "# ZeroDB soak, $START to $END"
    echo
    echo "Requested length: ${HOURS} h (the clock starts after the build; the last crash and"
    echo "stress rounds may run past it). Command: \`HOURS=$HOURS CRASH_ROUND_CYCLES=$CRASH_ROUND_CYCLES scripts/soak.sh\`."
    echo
    cat "$OUT/host.md"
    echo
    echo "## Fuzzing"
    echo
    echo "| Job | Workers | Inputs executed | Exit | Findings |"
    echo "|---|---|---|---|---|"
    for i in 0 1 2 3; do
        name="${NAMES[$i]}"
        workers=1
        [ "$name" = diff_ops-native ] && workers="$FUZZ_JOBS"
        [ "$name" = diff_ops-heed ] && workers="$HEED_JOBS"
        findings="$(fuzz_findings "$OUT/fuzz/$name/artifacts" | wc -l | tr -d ' ')"
        [ "$findings" -eq 0 ] && [ "${STATUS[$i]}" -eq 0 ] || VERDICT=fail
        echo "| $name | $workers | $(fuzz_execs "$OUT/fuzz/$name.log") | ${STATUS[$i]} | $findings |"
    done
    echo
    read -r crash_rounds crash_failed <"$OUT/crash/rounds" 2>/dev/null || crash_rounds=0 crash_failed=?
    echo "## Crash harness"
    echo
    echo "- Rounds: $crash_rounds (each $CRASH_ROUND_CYCLES single-process + $CRASH_MP_CYCLES multi-process cycles, $CRASH_JOBS threads), base seed $SEED"
    echo "- Cycles verified: $(crash_verified "$OUT/crash/crash.log")"
    echo "- Failed runs: $crash_failed"
    [ "${STATUS[4]}" -eq 0 ] || VERDICT=fail
    read -r stress_rounds stress_failed <"$OUT/stress/rounds" 2>/dev/null || stress_rounds=0 stress_failed=?
    echo
    echo "## Stress"
    echo
    echo "- Rounds: $stress_rounds (in-process and cross-process reader/writer stress)"
    echo "- Failed runs: $stress_failed"
    [ "${STATUS[5]}" -eq 0 ] || VERDICT=fail
    echo
    echo "## Verdict: $VERDICT"
    if [ "$VERDICT" = fail ]; then
        echo
        echo "Findings and failing runs:"
        echo
        for name in "${NAMES[@]:0:4}"; do
            fuzz_findings "$OUT/fuzz/$name/artifacts" | sed 's/^/- /'
        done
        grep -a -E 'violation|VIOLATION|repro:' "$OUT/crash/crash.log" 2>/dev/null | sed 's/^/- /' | head -50
        grep -a -E '^test .* FAILED|panicked' "$OUT/stress/stress.log" 2>/dev/null | sed 's/^/- /' | head -50
    fi
} >"$OUT/summary.md"
log "summary: $OUT/summary.md"
cat "$OUT/summary.md"
[ "$VERDICT" = pass ]
