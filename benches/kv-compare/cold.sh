#!/usr/bin/env bash
# Cold-cache reads on a data set larger than memory: LMDB and ZeroDB, with
# ZeroDB at several page sizes, with and without kernel read-ahead. See
# README.md, "Cold-cache reads".
#
#   nohup ./cold.sh > cold.log 2>&1 &
#
# Environment (defaults in brackets):
#   DATA         directory on the disk under test                 [/data/kvc]
#   DEVICE       block device of DATA, for disk-read counting      [auto]
#   VARIANTS     engine:page-KiB pairs; 0 is the engine's default  ["lmdb:0 zerodb:0 zerodb:16"]
#   SHAPES       label:value-bytes:records triples                 ["small:100:30000000 large:3000:1500000"]
#   MEM_CAP      memory limit of each measurement                  [1G]
#   SECS         seconds per measurement                           [60]
#   THREADS      reader thread counts for random gets              ["1 8"]
#
# Each data set is loaded once per variant, without a memory limit. Each
# measurement then runs in its own process and memory-limited cgroup, right
# after the page cache is dropped, with no warm-up: it measures the first
# reads of a process that has nothing cached. Needs passwordless sudo
# (drop_caches, systemd-run, sysctl).
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
BIN=${BIN:-$HERE/target/release/kv-compare}
DATA=${DATA:-/data/kvc}
VARIANTS=(${VARIANTS:-lmdb:0 zerodb:0 zerodb:16})
SHAPES=(${SHAPES:-small:100:30000000 large:3000:1500000})
MEM_CAP=${MEM_CAP:-1G}
SECS=${SECS:-60}
THREADS=(${THREADS:-1 8})
OUT=${OUT:-$HERE/results/cold-$(date -u +%Y-%m-%dT%H%MZ)}

mkdir -p "$OUT" "$DATA"
if [ -z "${DEVICE:-}" ]; then
    src=$(findmnt -no SOURCE --target "$DATA")
    DEVICE=$(lsblk -no PKNAME "$src" 2>/dev/null | head -1)
    [ -n "$DEVICE" ] || DEVICE=$(basename "$src")
fi
export KVC_DEVICE=$DEVICE

# The load inserts in random key order. Under the kernel's default dirty-page
# limits it is throttled to the disk's random-write rate; let it buffer in
# memory instead, and restore the limits on exit. The measurements are not
# affected: each one starts after a sync and a page-cache drop.
read -r dirty_ratio dirty_bg dirty_expire <<< "$(sysctl -n vm.dirty_ratio vm.dirty_background_ratio vm.dirty_expire_centisecs | tr '\n' ' ')"
sudo sysctl -qw vm.dirty_ratio=80 vm.dirty_background_ratio=60 vm.dirty_expire_centisecs=720000
trap 'sudo sysctl -qw vm.dirty_ratio=$dirty_ratio vm.dirty_background_ratio=$dirty_bg vm.dirty_expire_centisecs=$dirty_expire' EXIT

{
    echo "date: $(date -u +%FT%TZ)"
    echo "variants: ${VARIANTS[*]}"
    echo "shapes: ${SHAPES[*]}"
    echo "mem_cap=$MEM_CAP secs=$SECS threads=${THREADS[*]}"
    echo "zerodb: $(git -C "$HERE/../.." log --oneline -1 2>/dev/null || cat "$HERE/SOURCE" 2>/dev/null)"
    echo "rustc: $(rustc --version 2>/dev/null)"
    echo "kernel: $(uname -srm)"
    lscpu | grep -E 'Model name|^CPU\(s\)'
    free -g | head -2
    echo "os page: $(getconf PAGESIZE)"
    echo "device: $DEVICE"
    findmnt --target "$DATA"
} > "$OUT/config.txt"

# One cold measurement: `phase engine page-KiB dir records value [args...]`.
measure() {
    local phase=$1 eng=$2 page=$3 dir=$4 records=$5 value=$6; shift 6
    sync
    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    echo "$(date -u +%T) $phase $eng page=$page $*" >> "$OUT/STATUS"
    sudo -E systemd-run --scope -q -p MemoryMax="$MEM_CAP" -p MemorySwapMax=0 \
        "$BIN" "$phase" --engine "$eng" --dir "$dir" --records "$records" \
        --value-size "$value" --page-kib "$page" --out "$OUT/results.jsonl" "$@" \
        >> "$OUT/run.log" 2>&1 || echo "$(date -u +%T) FAILED $phase $eng page=$page $*" >> "$OUT/STATUS"
}

for shape in "${SHAPES[@]}"; do
    IFS=: read -r label value records <<< "$shape"
    for variant in "${VARIANTS[@]}"; do
        IFS=: read -r eng page <<< "$variant"
        dir="$DATA/$eng-$page"
        sudo rm -rf "$dir"
        echo "$(date -u +%T) load $eng page=$page $label" >> "$OUT/STATUS"
        sudo -E "$BIN" load --engine "$eng" --dir "$dir" --records "$records" \
            --value-size "$value" --page-kib "$page" --label "$label" --out "$OUT/results.jsonl" \
            >> "$OUT/run.log" 2>&1 || echo "$(date -u +%T) FAILED load $eng page=$page" >> "$OUT/STATUS"
        for ra in "" "--read-ahead"; do
            for t in "${THREADS[@]}"; do
                measure get "$eng" "$page" "$dir" "$records" "$value" --threads "$t" --seconds "$SECS" --label "$label" $ra
            done
            measure range "$eng" "$page" "$dir" "$records" "$value" --threads 1 --seconds "$SECS" --label "$label" $ra
        done
        sudo rm -rf "$dir"
    done
done

echo done >> "$OUT/STATUS"
echo "$OUT"
