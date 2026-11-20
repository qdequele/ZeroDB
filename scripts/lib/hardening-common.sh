# shellcheck shell=bash
# Helpers shared by scripts/soak.sh and scripts/page-size-matrix.sh, sourced
# by both. The sourcing script sets ZERODB (this repository), OUT (the run's
# output directory) and LOG_TAG.

log() { printf '\n\033[1m[%s] %s\033[0m\n' "$LOG_TAG" "$*" | tee -a "$OUT/run.log" >&2; }

now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

cpu_count() { getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu; }

os_page_size() { getconf PAGESIZE; }

# write_host_info FILE: the machine, kernel, page size, toolchains and commit,
# as the Markdown list a PROVENANCE.md entry quotes.
write_host_info() {
    local f="$1" dirty=""
    git -C "$ZERODB" diff --quiet HEAD -- || dirty=" (working tree modified)"
    {
        echo "- Host: \`$(uname -a)\`"
        if [ -r /etc/os-release ]; then
            echo "- Distribution: $(. /etc/os-release && echo "${PRETTY_NAME:-unknown}")"
        fi
        echo "- OS page size: $(os_page_size) bytes"
        echo "- CPUs: $(cpu_count)"
        if [ -r /proc/cpuinfo ]; then
            echo "- CPU: $(grep -m1 -E '^(model name|CPU part)' /proc/cpuinfo | cut -d: -f2- | xargs)"
        elif command -v sysctl >/dev/null; then
            echo "- CPU: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)"
        fi
        echo "- Filesystem of the run directory: $(df -PT "$OUT" 2>/dev/null | awk 'NR==2 {print $2}' || echo unknown)"
        echo "- Stable toolchain: \`$(rustc -V)\`"
        echo "- Nightly toolchain: \`$(rustc +nightly -V 2>/dev/null || echo 'not installed')\`"
        echo "- ZeroDB commit: \`$(git -C "$ZERODB" rev-parse HEAD)\`$dirty"
    } >"$f"
}

# fuzz_build: build every fuzz target once (cargo-fuzz, nightly, ASan), so
# parallel runs do not race on the build, and print the binaries' directory.
fuzz_build() {
    local triple
    triple="$(rustc +nightly -vV | sed -n 's/^host: //p')"
    (cd "$ZERODB" && cargo +nightly fuzz build >&2)
    echo "$ZERODB/fuzz/target/$triple/release"
}

# fuzz_execs LOG: the number of inputs a libFuzzer run executed, from the last
# status line of its log (`#N ...` in plain mode, `#N: ...` in fork mode).
fuzz_execs() {
    grep -aoE '^#[0-9]+' "$1" 2>/dev/null | tail -1 | tr -d '#' || true
}

# fuzz_findings DIR: the crash, timeout, leak and OOM inputs libFuzzer left in
# an artifact directory.
fuzz_findings() {
    find "$1" -type f \( -name 'crash-*' -o -name 'timeout-*' -o -name 'leak-*' \
        -o -name 'oom-*' \) 2>/dev/null | sort
}

# crash_verified LOG: the sum of the "cycles verified" counts of every
# crash-harness run in LOG.
crash_verified() {
    grep -aoE '^cycles verified : [0-9]+' "$1" 2>/dev/null |
        awk '{s += $4} END {print s + 0}'
}
