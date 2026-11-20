#!/usr/bin/env bash
# Power-loss test below the file system, with dm-log-writes (Linux, root).
#
# For each (file system, mode, spill, seed): create the file system on a
# dm-log-writes device, run the seeded ZeroDB workload on it with a log mark
# after every acknowledged commit, then rebuild the device at every flush in
# the log (a power cut that drops every write the device had not been told
# to make durable), mount each state on a throwaway snapshot and verify it.
# See docs/maintainers/POWER-LOSS.md.
#
#   sudo scripts/power-loss/run.sh                 # defaults below
#   sudo FS="ext4" MODES="default" SEEDS="7" MAX_POINTS=50 scripts/power-loss/run.sh
#
# Environment:
#   FS          file systems to test (default: "ext4 xfs")
#   MODES       durability modes (default: "default writemap nometasync")
#   SPILL       "on", "off" or "both" (default: both): large commits that spill
#   SEEDS       workload seeds (default: "7 24")
#   ROUNDS      workload rounds (default: 8; compaction runs after round 3)
#   MAX_POINTS  check at most this many cut points per run, evenly spaced,
#               always including the last (default: 0 = every flush)
#   EVERY       also check every N-th log entry, not only flushes (default: 0)
#   BIN_DIR     directory holding power-loss and zerodb-tools (default: build
#               them with cargo into target/release as the invoking user)
#   WORK        scratch directory for the sparse device images (default:
#               target/power-loss; needs about 2 GiB free per run, sparse)
#   KEEP_GOING  1 to check every point after a violation (default: 0)
#
# Exit status: 0 if every run verified, 1 on a violation, 2 on a setup error.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HERE="$ROOT/scripts/power-loss"
FS="${FS:-ext4 xfs}"
MODES="${MODES:-default writemap nometasync}"
SPILL="${SPILL:-both}"
SEEDS="${SEEDS:-7 24}"
ROUNDS="${ROUNDS:-8}"
MAX_POINTS="${MAX_POINTS:-0}"
EVERY="${EVERY:-0}"
WORK="${WORK:-$ROOT/target/power-loss}"
KEEP_GOING="${KEEP_GOING:-0}"

die() { echo "power-loss: $*" >&2; exit 2; }

[ "$(uname -s)" = Linux ] || die "needs Linux (device-mapper); see docs/maintainers/POWER-LOSS.md"
[ "$(id -u)" = 0 ] || die "needs root (loop devices, device-mapper, mount)"
"$HERE/probe.sh" || die "prerequisites missing (see above)"

if [ -z "${BIN_DIR:-}" ]; then
  # Build as the invoking user, not root, so target/ stays theirs.
  build=(cargo build --release -p zerodb-oracle --bin power-loss -p zerodb-tools --bin zerodb-tools)
  if [ -n "${SUDO_USER:-}" ]; then
    sudo -u "$SUDO_USER" -H bash -lc "cd '$ROOT' && ${build[*]}"
  else
    (cd "$ROOT" && "${build[@]}")
  fi
  BIN_DIR="$ROOT/target/release"
fi
PL="$BIN_DIR/power-loss"
TOOLS="$BIN_DIR/zerodb-tools"
[ -x "$PL" ] && [ -x "$TOOLS" ] || die "missing $PL or $TOOLS"

LW=zdb-pl-logwrites
SNAP=zdb-pl-snap
mkdir -p "$WORK"
MNT="$WORK/mnt"
mkdir -p "$MNT"
LOOPS=()

cleanup() {
  set +e
  mountpoint -q "$MNT" && umount "$MNT"
  dmsetup info "$SNAP" >/dev/null 2>&1 && dmsetup remove "$SNAP"
  dmsetup info "$LW" >/dev/null 2>&1 && dmsetup remove "$LW"
  for l in "${LOOPS[@]}"; do losetup -d "$l"; done
  LOOPS=()
  rm -f "$WORK"/*.img
  set -e
}
trap cleanup EXIT

mkloop() { # mkloop <var> <image> <size>: attach a sparse image, set <var> to its loop device
  truncate -s "$3" "$2"
  local dev
  dev="$(losetup --find --show "$2")"
  LOOPS+=("$dev")
  printf -v "$1" '%s' "$dev"
}

mkfs_for() {
  case "$1" in
    # Initialize inode tables and the journal now, so no background zeroing
    # runs during the workload.
    ext4) mkfs.ext4 -q -F -E nodiscard,lazy_itable_init=0,lazy_journal_init=0 "$2" ;;
    xfs) mkfs.xfs -q -f -K "$2" ;;
    *) die "unknown file system $1" ;;
  esac
}

failed=0
summary=()
for fs in $FS; do
  for mode in $MODES; do
    case "$SPILL" in
      both) spills="off on" ;;
      on | off) spills="$SPILL" ;;
      *) die "SPILL must be on, off or both" ;;
    esac
    for spill in $spills; do
      for seed in $SEEDS; do
        run="fs=$fs mode=$mode spill=$spill seed=$seed"
        echo "=== $run"
        spill_arg=()
        [ "$spill" = on ] && spill_arg=(--spill)
        run_args=(--seed "$seed" --mode "$mode" --rounds "$ROUNDS" "${spill_arg[@]}")

        mkloop data "$WORK/data.img" 1G
        mkloop log "$WORK/log.img" 8G
        mkloop replay "$WORK/replay.img" 1G
        mkloop cow "$WORK/cow.img" 1G
        sectors="$(blockdev --getsz "$data")"

        dmsetup create "$LW" --table "0 $sectors log-writes $data $log"
        mkfs_for "$fs" "/dev/mapper/$LW"
        dmsetup message "$LW" 0 mark mkfs
        mount "/dev/mapper/$LW" "$MNT"
        status=0
        "$PL" workload --dir "$MNT" --mark-cmd "dmsetup message $LW 0 mark" "${run_args[@]}" ||
          status=$?
        umount "$MNT"
        # Removing the target flushes every pending log entry to the log device.
        dmsetup remove "$LW"
        if [ "$status" != 0 ]; then
          echo "power-loss: workload failed ($status) for $run" >&2
          summary+=("SETUP-FAIL $run")
          failed=1
          cleanup
          continue
        fi
        "$PL" inspect --log "$log"

        verify_args=(--log "$log" --replay "$replay" --mnt "$MNT" --hook "$HERE/hook.sh"
          --tools "$TOOLS" --max-points "$MAX_POINTS" --every "$EVERY")
        [ "$KEEP_GOING" = 1 ] && verify_args+=(--keep-going)
        status=0
        ZDB_PL_FS="$fs" ZDB_PL_REPLAY="$replay" ZDB_PL_COW="$cow" ZDB_PL_MNT="$MNT" \
          ZDB_PL_SNAP="$SNAP" "$PL" verify "${verify_args[@]}" "${run_args[@]}" || status=$?
        if [ "$status" = 0 ]; then
          summary+=("ok         $run")
        else
          summary+=("VIOLATION  $run (exit $status)")
          failed=1
        fi
        cleanup
      done
    done
  done
done

echo
echo "=== power-loss summary ($(uname -r))"
printf '%s\n' "${summary[@]}"
exit "$failed"
