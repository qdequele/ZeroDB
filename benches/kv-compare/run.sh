#!/usr/bin/env bash
# Run the full kv-compare suite on a dedicated Linux host. See README.md.
#
#   nohup ./run.sh > run.log 2>&1 &
#
# Environment (defaults in brackets):
#   DATA         directory on the disk under test                [/data/kvc]
#   DEVICE       block device of DATA, for disk-write counting    [auto]
#   ENGINES      engines to run                                   [all seven]
#   MEM_CAP      memory limit of each run (systemd MemoryMax)     [8G]
#   CACHE_MIB    cache of engines that keep their own             [2048]
#   SMALL        records of the data set that fits in memory      [10000000]
#   LARGE        records of the data set larger than memory       [100000000]
#   VALUE        value size in bytes, both data sets              [100]
#   SECS         seconds per measurement                          [60]
#   WARMUP       unmeasured seconds of the same work before each  [20]
#   THREADS      reader thread counts for the scaling tests       ["1 2 4 8 16"]
#   GROW_SECS    length of the continuous-insert test             [1800]
#   SETS         data sets to run                                 ["small large"]
#
# Every measurement runs in its own process, in its own memory-limited
# cgroup, after the page cache is dropped, so no run inherits another's
# cached pages. It then reads the data files once (as far as the memory
# limit allows) and runs WARMUP seconds of the same work unmeasured, so it
# measures warm caches. Needs passwordless sudo (drop_caches, systemd-run).
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
BIN=${BIN:-$HERE/target/release/kv-compare}
DATA=${DATA:-/data/kvc}
ENGINES=(${ENGINES:-lmdb zerodb fjall rocksdb redb sqlite zerodb-trusted})
MEM_CAP=${MEM_CAP:-8G}
CACHE_MIB=${CACHE_MIB:-2048}
SMALL=${SMALL:-10000000}
LARGE=${LARGE:-100000000}
VALUE=${VALUE:-100}
SECS=${SECS:-60}
WARMUP=${WARMUP:-20}
THREADS=(${THREADS:-1 2 4 8 16})
GROW_SECS=${GROW_SECS:-1800}
SETS=(${SETS:-small large})
OUT=${OUT:-$HERE/results/$(date -u +%Y-%m-%dT%H%MZ)}

mkdir -p "$OUT" "$DATA"
if [ -z "${DEVICE:-}" ]; then
    src=$(findmnt -no SOURCE --target "$DATA")
    DEVICE=$(lsblk -no PKNAME "$src" 2>/dev/null | head -1)
    [ -n "$DEVICE" ] || DEVICE=$(basename "$src")
fi
export KVC_DEVICE=$DEVICE

{
    echo "date: $(date -u +%FT%TZ)"
    echo "engines: ${ENGINES[*]}"
    echo "mem_cap=$MEM_CAP cache_mib=$CACHE_MIB small=$SMALL large=$LARGE value=$VALUE"
    echo "secs=$SECS warmup=$WARMUP threads=${THREADS[*]} grow_secs=$GROW_SECS sets=${SETS[*]}"
    # A copied tree without .git can name its source in a SOURCE file.
    echo "zerodb: $(git -C "$HERE/../.." log --oneline -1 2>/dev/null || cat "$HERE/SOURCE" 2>/dev/null)"
    echo "rustc: $(rustc --version 2>/dev/null)"
    echo "kernel: $(uname -srm)"
    lscpu | grep -E 'Model name|^CPU\(s\)'
    free -g | head -2
    echo "device: $DEVICE"
    findmnt --target "$DATA"
} > "$OUT/config.txt"

# One measurement: `phase engine records [args...]`.
measure() {
    local phase=$1 eng=$2 records=$3; shift 3
    sync
    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    echo "$(date -u +%T) $phase $eng $records $*" >> "$OUT/STATUS"
    # RocksDB keeps every table file open: lift the default 1,024-file limit.
    sudo -E systemd-run --scope -q -p MemoryMax="$MEM_CAP" -p MemorySwapMax=0 \
        prlimit --nofile=1048576 "$BIN" "$phase" --engine "$eng" --dir "$DATA/$eng" --records "$records" \
        --value-size "$VALUE" --cache-mib "$CACHE_MIB" --out "$OUT/results.jsonl" "$@" \
        >> "$OUT/run.log" 2>&1 || echo "$(date -u +%T) FAILED $phase $eng $*" >> "$OUT/STATUS"
}

fresh() {
    sudo rm -rf "$DATA/$1"
}

for set in "${SETS[@]}"; do
    records=$SMALL; [ "$set" = large ] && records=$LARGE
    for eng in "${ENGINES[@]}"; do
        fresh "$eng"
        measure load "$eng" "$records" --label "$set"
        for t in "${THREADS[@]}"; do
            measure get "$eng" "$records" --threads "$t" --seconds "$SECS" --warmup "$WARMUP" --prefetch --label "$set"
        done
        for t in 1 "${THREADS[-1]}"; do
            measure range "$eng" "$records" --threads "$t" --seconds "$SECS" --warmup "$WARMUP" --prefetch --label "$set"
        done
        for t in "${THREADS[@]}"; do
            measure mixed "$eng" "$records" --threads "$t" --seconds "$SECS" --warmup "$WARMUP" --prefetch --label "$set"
        done
        # Writes change the data set, so they come last.
        measure write "$eng" "$records" --batch 1000 --seconds "$SECS" --warmup "$WARMUP" --prefetch --label "$set"
        measure write "$eng" "$records" --batch 1 --durable --seconds "$SECS" --warmup "$WARMUP" --prefetch --label "$set"
        fresh "$eng"
    done
done

# Stability: continuous inserts, 100 records per commit, starting from the
# small data set and growing past the memory limit.
if [ "$GROW_SECS" -gt 0 ]; then
    for eng in "${ENGINES[@]}"; do
        fresh "$eng"
        measure load "$eng" "$SMALL" --label grow
        measure grow "$eng" "$SMALL" --batch 100 --seconds "$GROW_SECS" --prefetch --label grow
        fresh "$eng"
    done
fi

echo done >> "$OUT/STATUS"
echo "$OUT"
