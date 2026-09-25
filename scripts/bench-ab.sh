#!/usr/bin/env bash
# Before/after A/B of the engine_comparison ladder: ZeroDB at BASE vs ZeroDB in
# the working tree, with LMDB measured in both halves as the drift control.
#
#   scripts/bench-ab.sh [FILTER]        # FILTER is a criterion regex, e.g. '^del/'
#                                       #   or '^del/(range|bulk/half)'; empty = all
#
# What it does:
#   1. Materialises BASE in a reusable git worktree (target/bench-ab/base) and
#      overlays the WORKING TREE's bench harness (crates/zerodb-oracle/benches
#      and its Cargo.toml) onto it. The ruler is held fixed; only the engine
#      source differs between the two binaries. This also lets BASE predate the
#      ladder itself.
#   2. Builds both bench binaries, each into its OWN target dir. They cannot
#      share one: cargo's artifact hash ignores the workspace path, so the second
#      build would silently overwrite the first and both halves would run the
#      same binary (the first self-test did exactly that). target/bench-ab/target
#      is kept between runs, so only the first run pays a full dependency build.
#   3. Runs ROUNDS rounds, alternating which binary goes first, so thermal and
#      background drift cannot be attributed to one side.
#   4. Hands the rounds to scripts/bench-ab.py, which prints the three-column
#      table (LMDB | ZeroDB before | ZeroDB after) and writes verdict.json.
#
# Environment:
#   BASE        git rev for "before" (default: HEAD — the last kept state, so the
#               candidate is whatever is uncommitted in the working tree). Use
#               BASE=$(git merge-base HEAD main) for the branch-level gate.
#   ROUNDS      interleaved rounds (default 3; 1 is a smoke run, not a verdict)
#   TARGET      regex of the rungs the change claims to improve (default: FILTER).
#               A verdict of "improved" needs at least one of these to improve.
#   MIN_EFFECT  smallest speed change reported as real (default 0.03 = 3 %)
#   MAX_DRIFT   LMDB movement between halves beyond which a rung is unreliable
#               (default 0.03)
#   OUT         output dir (default target/bench-ab/runs/<timestamp>)
#   ZERODB_BENCH_TIER is passed through (long = include the 1M rung + concurrent).
set -euo pipefail

ZERODB="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FILTER="${1:-}"
BASE="${BASE:-HEAD}"
ROUNDS="${ROUNDS:-3}"
TARGET="${TARGET:-$FILTER}"
MIN_EFFECT="${MIN_EFFECT:-0.03}"
MAX_DRIFT="${MAX_DRIFT:-0.03}"
AB="$ZERODB/target/bench-ab"
BASE_TREE="$AB/base"
OUT="${OUT:-$AB/runs/$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUT" && OUT="$(cd "$OUT" && pwd)" # absolute: builds run from other dirs

log() { printf '\n\033[1m[bench-ab] %s\033[0m\n' "$*" >&2; }

BASE_SHA="$(git -C "$ZERODB" rev-parse --verify "$BASE^{commit}")"
CAND_DESC="$(git -C "$ZERODB" rev-parse --short HEAD)$(git -C "$ZERODB" diff --quiet HEAD -- crates || echo '+dirty')"

# -- 1. the base worktree ----------------------------------------------------
git -C "$ZERODB" worktree prune
if [ -e "$BASE_TREE/.git" ]; then
    git -C "$BASE_TREE" checkout -q --force --detach "$BASE_SHA"
    git -C "$BASE_TREE" clean -fdq
else
    mkdir -p "$AB"
    git -C "$ZERODB" worktree add -q --force --detach "$BASE_TREE" "$BASE_SHA"
fi
rm -rf "$BASE_TREE/crates/zerodb-oracle/benches"
cp -R "$ZERODB/crates/zerodb-oracle/benches" "$BASE_TREE/crates/zerodb-oracle/benches"
cp "$ZERODB/crates/zerodb-oracle/Cargo.toml" "$BASE_TREE/crates/zerodb-oracle/Cargo.toml"
log "before = $(git -C "$ZERODB" rev-parse --short "$BASE_SHA") (engine) + working-tree harness; after = $CAND_DESC"

# -- 2. build both -----------------------------------------------------------
bench_exe() { # workspace side target-dir -> prints the bench executable path
    local json="$OUT/build-$2.json"
    if ! (cd "$1" && CARGO_TARGET_DIR="$3" cargo bench -q -p zerodb-oracle --bench engine_comparison --no-run \
        --message-format=json >"$json"); then
        jq -r 'select(.reason == "compiler-message") | .message.rendered' "$json" >&2
        echo "bench build failed for $2 ($1)" >&2
        return 1
    fi
    jq -r 'select(.reason == "compiler-artifact" and .target.name == "engine_comparison"
                  and .executable != null) | .executable' "$json" | tail -1
}
log "building before"
EXE_BASE="$(bench_exe "$BASE_TREE" before "$AB/target")"
log "building after"
EXE_CAND="$(bench_exe "$ZERODB" after "$ZERODB/target")"
if [ "$EXE_BASE" = "$EXE_CAND" ]; then
    echo "both builds produced the same file ($EXE_BASE) — refusing to compare a binary with itself" >&2
    exit 1
fi
if cmp -s "$EXE_BASE" "$EXE_CAND"; then
    # Identical binaries are fine only if the engine really did not change. If it
    # did, a build was stale (a shared fingerprint once made cargo skip the
    # candidate rebuild), and any verdict would compare a binary with itself.
    if git -C "$ZERODB" diff --quiet "$BASE_SHA" -- crates \
        ':!crates/zerodb-oracle/benches' ':!crates/zerodb-oracle/Cargo.toml'; then
        log "warning: the two binaries are byte-identical — the engine did not change"
    else
        echo "engine source differs from $BASE but the binaries are identical: stale build." >&2
        echo "fix: cargo clean --release -p zerodb-core -p zerodb-io -p zerodb -p heed-zerodb -p zerodb-oracle" >&2
        exit 1
    fi
fi

# -- 3. interleaved rounds ---------------------------------------------------
# A loaded machine makes every rung noise; say so up front and record it. The
# 1-minute load average still counts the builds that just finished, so give it
# up to LOAD_WAIT seconds (default 120) to settle before judging the machine.
load_now() {
    if [ -r /proc/loadavg ]; then cut -d' ' -f1 /proc/loadavg
    else sysctl -n vm.loadavg | awk '{print $2}'; fi
}
if [ -r /proc/loadavg ]; then NCPU="$(nproc)"; else NCPU="$(sysctl -n hw.ncpu)"; fi
busy() { awk -v l="$1" -v n="$NCPU" 'BEGIN { exit !(l > n / 2) }'; }
LOAD="$(load_now)"
for _ in $(seq 1 $(( ${LOAD_WAIT:-120} / 5 ))); do
    busy "$LOAD" || break
    sleep 5
    LOAD="$(load_now)"
done
if busy "$LOAD"; then
    log "warning: load average $LOAD on $NCPU CPUs — expect an invalid verdict; stop other work first"
fi
cat > "$OUT/meta.json" <<EOF
{"base": "$BASE_SHA", "candidate": "$CAND_DESC", "filter": "$FILTER", "target": "$TARGET",
 "rounds": $ROUNDS, "load_avg": $LOAD, "ncpu": $NCPU, "tier": "${ZERODB_BENCH_TIER:-default}", "host": "$(uname -sm)",
 "date": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"}
EOF
run_side() { # side exe round
    log "round $3/$ROUNDS: $1"
    CRITERION_HOME="$OUT/round-$3/$1" "$2" --bench --noplot ${FILTER:+"$FILTER"} >"$OUT/round-$3/$1.log" 2>&1 ||
        { tail -20 "$OUT/round-$3/$1.log" >&2; exit 1; }
}
for r in $(seq 1 "$ROUNDS"); do
    mkdir -p "$OUT/round-$r"
    if [ $((r % 2)) -eq 1 ]; then
        run_side before "$EXE_BASE" "$r"; run_side after "$EXE_CAND" "$r"
    else
        run_side after "$EXE_CAND" "$r"; run_side before "$EXE_BASE" "$r"
    fi
done

# -- 4. verdict --------------------------------------------------------------
"$ZERODB/scripts/bench-ab.py" "$OUT" --target "$TARGET" \
    --min-effect "$MIN_EFFECT" --max-drift "$MAX_DRIFT"
log "verdict: $OUT/verdict.json"
