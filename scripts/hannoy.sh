#!/usr/bin/env bash
# hannoy (HNSW vector index) on ZeroDB — the second consumer.
#
#   scripts/hannoy.sh suites   # hannoy's own test suite on the ZeroDB shim (+ no-LMDB check)
#   scripts/hannoy.sh bench    # `cargo bench --bench benchmark` (divan: build_hnsw /
#                              #   search_hnsw at 512/768/1536 dims) on LMDB then ZeroDB,
#                              #   medians compared
#
# Environment:
#   HANNOY_REF    git ref (default: v0.1.7-nested-rtxns — the latest tag, and the one
#                 the July 2026 numbers in PROGRESS.md were taken on; milli pins 0.1.3)
#   HANNOY_SRC    local checkout to `git clone --shared` from (never modified)
#   HANNOY_CLONE  reuse this clone
#   WORKDIR       scratch root (default: <zerodb>/target/consumer)
#   ROUNDS        bench rounds, order flipped on even rounds (default: 1)
#   BENCH_ARGS    extra args after `--` for divan (e.g. "build_hnsw" to filter)
#
# Same mechanism as scripts/consumer.sh: `[patch.crates-io] heed = { path = …/heed-shim }`
# between marker lines, removed again for the stock build.
set -euo pipefail

ZERODB="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-suites}"
HANNOY_REF="${HANNOY_REF:-v0.1.7-nested-rtxns}"
WORKDIR="${WORKDIR:-$ZERODB/target/consumer}"
CLONE="${HANNOY_CLONE:-$WORKDIR/hannoy}"
ROUNDS="${ROUNDS:-1}"
BENCH_ARGS="${BENCH_ARGS:-}"
MARK_BEGIN="# >>> zerodb consumer gate (scripts/hannoy.sh) >>>"
MARK_END="# <<< zerodb consumer gate <<<"

log() { printf '\n\033[1m[hannoy] %s\033[0m\n' "$*"; }

ensure_clone() {
    if [ -d "$CLONE/.git" ]; then
        # A previous run may have died with the patch block still in place.
        set_patch off
        if git -C "$CLONE" rev-parse --verify -q "$HANNOY_REF^{commit}" >/dev/null; then
            git -C "$CLONE" checkout -q --detach "$HANNOY_REF"
        else
            git -C "$CLONE" fetch -q origin "$HANNOY_REF"
            git -C "$CLONE" checkout -q --detach FETCH_HEAD
        fi
        log "reusing clone at $CLONE, now at $HANNOY_REF ($(git -C "$CLONE" rev-parse --short HEAD))"
        return
    fi
    mkdir -p "$(dirname "$CLONE")"
    if [ -n "${HANNOY_SRC:-}" ]; then
        log "shared clone from $HANNOY_SRC at $HANNOY_REF"
        git clone -q --local --shared "$HANNOY_SRC" "$CLONE"
        git -C "$CLONE" checkout -q "$HANNOY_REF"
    else
        log "cloning hannoy at $HANNOY_REF"
        git clone -q --depth 1 --branch "$HANNOY_REF" https://github.com/nnethercott/hannoy "$CLONE"
    fi
}

set_patch() {
    local manifest="$CLONE/Cargo.toml"
    python3 - "$manifest" "$MARK_BEGIN" "$MARK_END" <<'EOF'
import sys
path, b, e = sys.argv[1:4]
s = open(path).read()
i, j = s.find(b), s.find(e)
if i != -1 and j != -1:
    s = s[:i].rstrip('\n') + '\n' + s[j + len(e):].lstrip('\n')
open(path, 'w').write(s)
EOF
    if [ "$1" = on ]; then
        cat >> "$manifest" <<EOF

$MARK_BEGIN
[patch.crates-io]
heed = { path = "$ZERODB/crates/heed-shim" }
$MARK_END
EOF
    fi
}

assert_no_lmdb() {
    if (cd "$CLONE" && cargo tree -i lmdb-master-sys >/dev/null 2>&1); then
        echo "FAIL: lmdb-master-sys is still in hannoy's dependency tree" >&2
        (cd "$CLONE" && cargo tree -i lmdb-master-sys | head -20) >&2
        exit 1
    fi
    log "lmdb-master-sys absent from the dependency tree (pure ZeroDB)"
}

do_suites() {
    set_patch on
    log "cargo test on the ZeroDB shim"
    (cd "$CLONE" && cargo test)
    assert_no_lmdb
}

run_bench() { # $1 = lmdb | zerodb, $2 = round
    local out="$WORKDIR/reports/hannoy/$1-round-$2.txt"
    mkdir -p "$(dirname "$out")"
    if [ "$1" = zerodb ]; then set_patch on; else set_patch off; fi
    log "cargo bench --bench benchmark ($1, round $2) → $out"
    # shellcheck disable=SC2086
    (cd "$CLONE" && cargo bench --bench benchmark -- $BENCH_ARGS) | tee "$out"
    if [ "$1" = zerodb ]; then assert_no_lmdb; fi
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

ensure_clone
trap cleanup EXIT
case "$MODE" in
    suites) do_suites ;;
    bench)  do_bench ;;
    *) echo "usage: $0 {suites|bench}" >&2; exit 2 ;;
esac
log "done ($MODE); clone left at $CLONE with its stock Cargo.toml"
