# shellcheck shell=bash
# Helpers shared by scripts/consumer.sh and scripts/hannoy.sh, sourced by both.
#
# The sourcing script sets ZERODB (this repository), CLONE (the consumer
# checkout), MARK_BEGIN / MARK_END (the patch block markers) and LOG_TAG, and
# defines `heed_shim`, which prints the shim crate to patch in.

log() { printf '\n\033[1m[%s] %s\033[0m\n' "$LOG_TAG" "$*"; }

# ensure_clone REF SRC URL: check out REF in $CLONE, reusing an existing clone,
# else cloning from the local checkout SRC (when set) or from URL.
ensure_clone() {
    local ref="$1" src="$2" url="$3"
    if [ -d "$CLONE/.git" ]; then
        # A previous run may have died with the patch block still in place.
        set_patch off
        if git -C "$CLONE" rev-parse --verify -q "$ref^{commit}" >/dev/null; then
            git -C "$CLONE" checkout -q --detach "$ref"
        else
            git -C "$CLONE" fetch -q origin "$ref"
            git -C "$CLONE" checkout -q --detach FETCH_HEAD
        fi
        log "reusing clone at $CLONE, now at $ref ($(git -C "$CLONE" rev-parse --short HEAD))"
        return
    fi
    mkdir -p "$(dirname "$CLONE")"
    if [ -n "$src" ]; then
        log "shared clone from $src at $ref"
        git clone -q --local --shared "$src" "$CLONE"
        git -C "$CLONE" checkout -q "$ref"
    else
        log "cloning $url at $ref"
        git clone -q --depth 1 --branch "$ref" "$url" "$CLONE"
    fi
}

# Patch heed to the shim ($1 = on), prepare a stock build ($1 = stock) or restore
# the manifest ($1 = off); see scripts/patch-heed.py.
set_patch() {
    python3 "$ZERODB/scripts/patch-heed.py" "$CLONE/Cargo.toml" "$1" \
        "$MARK_BEGIN" "$MARK_END" "$(heed_shim)"
}

# assert_no_lmdb PACKAGE: the whole point of the drop-in. With the shim
# patched in, the C LMDB fork must vanish from the dependency graph of PACKAGE
# (or of the whole clone when PACKAGE is empty).
assert_no_lmdb() {
    local what="${1:-$LOG_TAG}"
    local args=()
    if [ -n "${1:-}" ]; then args=(-p "$1"); fi
    if (cd "$CLONE" && cargo tree ${args[@]+"${args[@]}"} -i lmdb-master-sys >/dev/null 2>&1); then
        echo "FAIL: lmdb-master-sys is still in $what's dependency tree" >&2
        (cd "$CLONE" && cargo tree ${args[@]+"${args[@]}"} -i lmdb-master-sys | head -20) >&2
        exit 1
    fi
    log "$what: lmdb-master-sys absent from the dependency tree (pure ZeroDB)"
}
