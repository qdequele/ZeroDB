#!/usr/bin/env bash
# Consumer gate: does ZeroDB work as a drop-in for LMDB inside Meilisearch, and
# how does Meilisearch perform on each engine?
#
#   scripts/consumer.sh check    # milli + index-scheduler compile on the shim,
#                                #   and no C LMDB is left in the dependency tree
#   scripts/consumer.sh suites   # check + the milli and index-scheduler test suites
#                                #   (FULL=1 adds meilisearch --lib and meilisearch-auth)
#   scripts/consumer.sh bench    # build meilisearch twice (stock LMDB, ZeroDB) and
#                                #   run the same `cargo xtask bench` workloads on both
#
# Environment:
#   MEILISEARCH_REF    git ref to test (default: v1.53.1 — the pin last verified)
#   MEILISEARCH_SRC    a local Meilisearch checkout to `git clone --shared` from
#                      (saves the network clone; the checkout itself is never touched)
#   MEILISEARCH_CLONE  reuse this clone instead of creating one under WORKDIR
#   WORKDIR            scratch root (default: <zerodb>/target/consumer)
#   WORKLOADS          space-separated workload files, relative to the Meilisearch
#                      repo (default: workloads/movies.json workloads/search/movies.json)
#   ROUNDS             bench rounds; each round runs LMDB then ZeroDB, and even rounds
#                      flip the order so thermal drift is not attributed to one engine
#                      (default: 1)
#   MEILI_PORT         port the benched server listens on (default: 7700). The xtask
#                      runner hardcodes 7700; another value patches that constant in
#                      the clone's xtask for the run and sets MEILI_HTTP_ADDR for the
#                      spawned server. Useful when a local container holds 7700.
#
# The shim is patched in through `[patch.crates-io] heed = { path = .../heed-shim }`
# (the only form that works — see crates/heed-shim/Cargo.toml). The patch is
# appended to the clone's Cargo.toml between marker lines and removed again for
# the stock build, so the clone can be reused across runs.
set -euo pipefail

ZERODB="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-check}"
MEILISEARCH_REF="${MEILISEARCH_REF:-v1.53.1}"
WORKDIR="${WORKDIR:-$ZERODB/target/consumer}"
CLONE="${MEILISEARCH_CLONE:-$WORKDIR/meilisearch}"
WORKLOADS="${WORKLOADS:-workloads/movies.json workloads/search/movies.json}"
ROUNDS="${ROUNDS:-1}"
MEILI_PORT="${MEILI_PORT:-7700}"
MARK_BEGIN="# >>> zerodb consumer gate (scripts/consumer.sh) >>>"
MARK_END="# <<< zerodb consumer gate <<<"

log() { printf '\n\033[1m[consumer] %s\033[0m\n' "$*"; }

ensure_clone() {
    if [ -d "$CLONE/.git" ]; then
        log "reusing clone at $CLONE ($(git -C "$CLONE" rev-parse --short HEAD))"
        return
    fi
    mkdir -p "$(dirname "$CLONE")"
    if [ -n "${MEILISEARCH_SRC:-}" ]; then
        log "shared clone from $MEILISEARCH_SRC at $MEILISEARCH_REF"
        git clone -q --local --shared "$MEILISEARCH_SRC" "$CLONE"
        git -C "$CLONE" checkout -q "$MEILISEARCH_REF"
    else
        log "cloning meilisearch at $MEILISEARCH_REF"
        git clone -q --depth 1 --branch "$MEILISEARCH_REF" https://github.com/meilisearch/meilisearch "$CLONE"
    fi
}

# Remove any previous patch block, then (if $1 = on) append a fresh one.
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
    # The whole point of the drop-in: with the shim patched in, the C LMDB fork
    # must vanish from the dependency graph of the crate under test.
    if (cd "$CLONE" && cargo tree -p "$1" -i lmdb-master-sys >/dev/null 2>&1); then
        echo "FAIL: lmdb-master-sys is still in $1's dependency tree" >&2
        (cd "$CLONE" && cargo tree -p "$1" -i lmdb-master-sys | head -20) >&2
        exit 1
    fi
    log "$1: lmdb-master-sys absent from the dependency tree (pure ZeroDB)"
}

do_check() {
    set_patch on
    log "cargo check -p milli -p index-scheduler on the ZeroDB shim"
    (cd "$CLONE" && cargo check -p milli -p index-scheduler)
    assert_no_lmdb milli
    assert_no_lmdb index-scheduler
}

do_suites() {
    do_check
    log "milli + index-scheduler test suites on ZeroDB"
    (cd "$CLONE" && cargo test -p milli -p index-scheduler)
    if [ "${FULL:-0}" = 1 ]; then
        log "meilisearch --lib + meilisearch-auth on ZeroDB"
        (cd "$CLONE" && cargo test -p meilisearch-auth && cargo test -p meilisearch --lib)
    fi
}

build_binary() { # $1 = lmdb | zerodb
    local out="$WORKDIR/bin/meilisearch-$1"
    if [ "$1" = zerodb ]; then set_patch on; else set_patch off; fi
    log "building meilisearch ($1) → $out"
    (cd "$CLONE" && cargo build --release -p meilisearch)
    if [ "$1" = zerodb ]; then assert_no_lmdb meilisearch; fi
    mkdir -p "$WORKDIR/bin"
    cp "$CLONE/target/release/meilisearch" "$out"
}

port_setup() {
    if lsof -nP -iTCP:"$MEILI_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "FAIL: port $MEILI_PORT is in use; the bench spawns Meilisearch there." >&2
        echo "      Free it or pick another with MEILI_PORT=<port>." >&2
        lsof -nP -iTCP:"$MEILI_PORT" -sTCP:LISTEN >&2 || true
        exit 1
    fi
    if [ "$MEILI_PORT" != 7700 ]; then
        # xtask hardcodes the client URLs; retarget them for this run only.
        sed -i.consumer-bak "s#127.0.0.1:7700#127.0.0.1:$MEILI_PORT#g" "$CLONE/crates/xtask/src/bench/mod.rs"
        export MEILI_HTTP_ADDR="127.0.0.1:$MEILI_PORT"
    fi
}

port_teardown() {
    if [ -f "$CLONE/crates/xtask/src/bench/mod.rs.consumer-bak" ]; then
        mv "$CLONE/crates/xtask/src/bench/mod.rs.consumer-bak" "$CLONE/crates/xtask/src/bench/mod.rs"
    fi
}

run_bench() { # $1 = lmdb | zerodb, $2 = round
    local bin="$WORKDIR/bin/meilisearch-$1"
    local reports="$WORKDIR/reports/$1"
    log "xtask bench ($1, round $2, port $MEILI_PORT): $WORKLOADS"
    # shellcheck disable=SC2086
    (cd "$CLONE" && cargo xtask bench --no-dashboard \
        --binary-path "$bin" \
        --asset-folder "$WORKDIR/assets" \
        --report-folder "$reports/round-$2" \
        -- $WORKLOADS)
}

do_bench() {
    build_binary lmdb
    build_binary zerodb
    set_patch off
    port_setup
    trap port_teardown EXIT
    for r in $(seq 1 "$ROUNDS"); do
        if [ $((r % 2)) -eq 1 ]; then
            run_bench lmdb "$r"; run_bench zerodb "$r"
        else
            run_bench zerodb "$r"; run_bench lmdb "$r"
        fi
    done
    port_teardown
    log "comparison"
    python3 "$ZERODB/scripts/bench-compare.py" "$WORKDIR/reports/lmdb" "$WORKDIR/reports/zerodb"
}

ensure_clone
case "$MODE" in
    check)  do_check ;;
    suites) do_suites ;;
    bench)  do_bench ;;
    *) echo "usage: $0 {check|suites|bench}" >&2; exit 2 ;;
esac
set_patch off
log "done ($MODE); clone left at $CLONE with its stock Cargo.toml"
