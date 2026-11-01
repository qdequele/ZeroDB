#!/usr/bin/env bash
# Consumer gate: does ZeroDB work as a drop-in for LMDB inside Meilisearch, and
# how does Meilisearch perform on each engine?
#
#   scripts/consumer.sh check    # milli + index-scheduler compile on ZeroDB,
#                                #   and no C LMDB is left in the dependency tree
#   scripts/consumer.sh suites   # check + the milli and index-scheduler test suites
#                                #   (FULL=1 adds meilisearch --lib and meilisearch-auth)
#   scripts/consumer.sh bench    # build meilisearch twice (stock LMDB, ZeroDB) and
#                                #   run the same `cargo xtask bench` workloads on both
#
# Environment:
#   MEILISEARCH_REF    git ref to test (default: v1.55.0, which builds against
#                      Meilisearch's heed fork; v1.53.1 is the last release on
#                      stock heed 0.22.1)
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
# ZeroDB's heed adapter (crates/heed-zerodb, a crate named `heed`) is patched in
# through `[patch.crates-io] heed = { path = ... }`, plus one `[patch."<url>"]`
# per git source of heed. When the clone resolves heed from Meilisearch's
# mutexify-open-dbi fork, as v1.55 and later do, milli's manifest also gets a
# dependency that turns on the adapter's `unique-txns` feature (the fork's API,
# ADR-0027), since a `[patch]` cannot enable features. Every edit sits between
# marker lines and is removed again for the stock build, so the clone can be
# reused across runs.
set -euo pipefail

ZERODB="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-check}"
MEILISEARCH_REF="${MEILISEARCH_REF:-v1.55.0}"
WORKDIR="${WORKDIR:-$ZERODB/target/consumer}"
CLONE="${MEILISEARCH_CLONE:-$WORKDIR/meilisearch}"
WORKLOADS="${WORKLOADS:-workloads/movies.json workloads/search/movies.json}"
ROUNDS="${ROUNDS:-1}"
MEILI_PORT="${MEILI_PORT:-7700}"
MARK_BEGIN="# >>> zerodb consumer gate (scripts/consumer.sh) >>>"
MARK_END="# <<< zerodb consumer gate <<<"

LOG_TAG=consumer
# shellcheck source=scripts/lib/consumer-common.sh
source "$ZERODB/scripts/lib/consumer-common.sh"

# milli's manifest when the clone builds against Meilisearch's heed fork
# (ADR-0027), so the adapter's fork API gets turned on; nothing otherwise.
# Reads the committed lock file: a build with the adapter patched in rewrites
# the one on disk without the fork's entry. `grep -c`, not `grep -q`: an early
# exit would kill `git show` with SIGPIPE and fail the pipe under `pipefail`.
heed_feature_manifest() {
    if git -C "$CLONE" show HEAD:Cargo.lock 2>/dev/null | grep -c 'heed?branch=mutexify-open-dbi' >/dev/null; then
        echo "$CLONE/crates/milli/Cargo.toml"
    fi
}

do_check() {
    set_patch on
    log "heed patched to $ZERODB/crates/heed-zerodb"
    log "cargo check -p milli -p index-scheduler on ZeroDB"
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
    if [ "$1" = zerodb ]; then set_patch on; else set_patch stock; fi
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

# The xtask runner stops a server with `kill -TERM` and starts the next run
# without waiting for that server to exit (its `kill_meili` returns once the
# `kill` command does). A slow shutdown then still holds the port and the next
# server fails with "Address already in use". The runner therefore starts each
# server through this wrapper, which waits until nothing listens on the port.
# The wait happens before the server starts, so no measured span includes it.
write_port_wait_wrapper() { # $1 = lmdb | zerodb
    local wrapper="$WORKDIR/bin/meilisearch-$1-wait-port"
    cat > "$wrapper" <<EOF
#!/usr/bin/env bash
for _ in \$(seq 1 600); do
    (exec 3<>/dev/tcp/127.0.0.1/$MEILI_PORT) 2>/dev/null || break
    sleep 0.1
done
exec "$WORKDIR/bin/meilisearch-$1" "\$@"
EOF
    chmod +x "$wrapper"
    echo "$wrapper"
}

run_bench() { # $1 = lmdb | zerodb, $2 = round
    local bin
    bin="$(write_port_wait_wrapper "$1")"
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

# Whatever happens — a failing suite, a killed bench — leave the clone with its
# stock manifest and the runner unpatched, and keep the real exit status.
cleanup() {
    local rc=$?
    port_teardown
    set_patch off
    exit "$rc"
}

ensure_clone "$MEILISEARCH_REF" "${MEILISEARCH_SRC:-}" https://github.com/meilisearch/meilisearch
trap cleanup EXIT
case "$MODE" in
    check)  do_check ;;
    suites) do_suites ;;
    bench)  do_bench ;;
    *) echo "usage: $0 {check|suites|bench}" >&2; exit 2 ;;
esac
log "done ($MODE); clone left at $CLONE with its stock Cargo.toml"
