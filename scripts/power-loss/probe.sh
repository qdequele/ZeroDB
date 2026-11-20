#!/usr/bin/env bash
# Check that this machine can run the power-loss harness: Linux, root, the
# device-mapper targets log-writes and snapshot, loop devices and the mkfs
# tools. Loads the kernel modules when they are present but not loaded.
# Prints every missing piece; exit status 1 if any is missing.
set -uo pipefail

missing=0
need() { echo "power-loss probe: missing $*" >&2; missing=1; }

[ "$(uname -s)" = Linux ] || { need "Linux"; exit 1; }
[ "$(id -u)" = 0 ] || need "root"
for tool in dmsetup losetup blockdev mount umount mkfs.ext4 mkfs.xfs truncate; do
  command -v "$tool" >/dev/null 2>&1 || need "the $tool command"
done
if command -v dmsetup >/dev/null 2>&1 && [ "$(id -u)" = 0 ]; then
  for target in log-writes snapshot; do
    if ! dmsetup targets | awk '{print $1}' | grep -qx "$target"; then
      modprobe "dm-$target" 2>/dev/null
      dmsetup targets | awk '{print $1}' | grep -qx "$target" ||
        need "the dm-$target device-mapper target (kernel $(uname -r); on Ubuntu it ships in linux-modules-extra-$(uname -r))"
    fi
  done
  losetup --find >/dev/null 2>&1 || { modprobe loop 2>/dev/null; losetup --find >/dev/null 2>&1 || need "a free loop device"; }
fi
[ "$missing" = 0 ] && echo "power-loss probe: ok (kernel $(uname -r))"
exit "$missing"
