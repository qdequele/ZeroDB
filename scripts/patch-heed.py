#!/usr/bin/env python3
"""Point a consumer's `heed` dependency at the ZeroDB shim, or undo it.

Usage: patch-heed.py <Cargo.toml> on|off <begin marker> <end marker> [<heed-shim path>]

`on` adds `heed = { path = "<heed-shim>" }` under `[patch.crates-io]`. When the
manifest already has that section (Meilisearch does, for its own heed fork),
the line goes inside it and the consumer's own `heed` entry is commented out,
since cargo refuses a second `[patch.crates-io]` table and two `heed` keys.
Otherwise a new section is appended. Every edit is fenced by the markers or
tagged, so `off` restores the manifest byte for byte, also after a run died
with the patch in place.
"""
import re
import sys

TAG = "# zerodb-shim-disabled: "


def off(s, begin, end):
    i, j = s.find(begin), s.find(end)
    if i != -1 and j != -1:
        head, tail = s[:i], s[j + len(end):]
        if tail.startswith("\n"):
            tail = tail[1:]
        # An appended block also took the blank line before it.
        if head.endswith("\n\n") and not tail.strip():
            head = head[:-1]
        s = head + tail
    return "\n".join(
        line[len(TAG):] if line.startswith(TAG) else line for line in s.split("\n")
    )


def on(s, begin, end, shim):
    entry = f'heed = {{ path = "{shim}" }}'
    lines = s.split("\n")
    header = next(
        (k for k, l in enumerate(lines) if re.fullmatch(r"\[patch\.crates-io\]\s*", l)),
        None,
    )
    if header is None:
        if not s.endswith("\n"):
            s += "\n"
        return s + f"\n{begin}\n[patch.crates-io]\n{entry}\n{end}\n"
    k = header + 1
    while k < len(lines) and not lines[k].lstrip().startswith("["):
        if re.match(r"\s*heed\s*=", lines[k]):
            lines[k] = TAG + lines[k]
        k += 1
    lines[header + 1:header + 1] = [begin, entry, end]
    return "\n".join(lines)


def main():
    path, mode, begin, end = sys.argv[1:5]
    s = open(path).read()
    s = off(s, begin, end)
    if mode == "on":
        s = on(s, begin, end, sys.argv[5])
    open(path, "w").write(s)


if __name__ == "__main__":
    main()
