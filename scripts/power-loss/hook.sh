#!/usr/bin/env bash
# Mount hook of the power-loss verifier (`power-loss verify --hook`).
#
#   hook.sh mount    put a non-persistent dm-snapshot over the replay device and
#                    mount it, so the file system's journal recovery writes to
#                    the snapshot and the replay device keeps the logged state
#   hook.sh umount   unmount and drop the snapshot
#
# Set by scripts/power-loss/run.sh: ZDB_PL_FS, ZDB_PL_REPLAY, ZDB_PL_COW,
# ZDB_PL_MNT, ZDB_PL_SNAP.
set -euo pipefail
: "${ZDB_PL_FS:?}" "${ZDB_PL_REPLAY:?}" "${ZDB_PL_COW:?}" "${ZDB_PL_MNT:?}" "${ZDB_PL_SNAP:?}"

case "${1:-}" in
  mount)
    sectors="$(blockdev --getsz "$ZDB_PL_REPLAY")"
    # N: non-persistent (the copy-on-write store is discarded with the target);
    # 8: 4 KiB chunks.
    dmsetup create "$ZDB_PL_SNAP" --table "0 $sectors snapshot $ZDB_PL_REPLAY $ZDB_PL_COW N 8"
    opts=()
    # The replayed file system carries the logged one's UUID.
    [ "$ZDB_PL_FS" = xfs ] && opts=(-o nouuid)
    if ! mount "${opts[@]}" "/dev/mapper/$ZDB_PL_SNAP" "$ZDB_PL_MNT"; then
      dmsetup remove "$ZDB_PL_SNAP"
      echo "power-loss hook: the replayed $ZDB_PL_FS file system does not mount" >&2
      exit 1
    fi
    ;;
  umount)
    umount "$ZDB_PL_MNT"
    dmsetup remove "$ZDB_PL_SNAP"
    ;;
  *)
    echo "usage: hook.sh mount|umount" >&2
    exit 2
    ;;
esac
