#!/usr/bin/env python3
"""Point a consumer's `heed` dependency at the ZeroDB shim, or undo it.

Usage: patch-heed.py <Cargo.toml> on|stock|off <begin marker> <end marker> [<heed-shim path>]

`on` adds `heed = { path = "<heed-shim>" }` under `[patch.crates-io]`. When the
manifest already has that section (Meilisearch does, for its own heed fork),
the line goes inside it and the consumer's own `heed` entry is commented out,
since cargo refuses a second `[patch.crates-io]` table and two `heed` keys.
Otherwise a new section is appended. Crates that take heed from a git
repository instead (Meilisearch v1.55's cellulite takes it from Meilisearch's
heed fork) are not reached by `[patch.crates-io]`: every git source of heed in
the consumer's `Cargo.lock` gets its own `[patch."<url>"]` table. 
`stock` leaves heed alone for the LMDB build. Both `on` and `stock` give a
manifest with no `[workspace]` table an empty one, so a consumer cloned under
this repository's `target/` is not taken for a member of its workspace.

Every edit is fenced by the markers or tagged, so `off` restores the manifest
byte for byte, also after a run died with the patch in place.
"""
import os
import re
import subprocess
import sys

TAG = "# zerodb-shim-disabled: "


def off(s, begin, end):
    while True:
        i = s.find(begin)
        j = s.find(end, i)
        if i == -1 or j == -1:
            break
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


def heed_git_sources(manifest):
    """The git repository URLs `Cargo.lock` resolves a `heed` package from.

    Reads the lock file as committed as well as the one on disk: a build with
    the shim patched in rewrites the lock file without the git entries.
    """
    root = os.path.dirname(os.path.abspath(manifest))
    texts = []
    committed = subprocess.run(
        ["git", "-C", root, "show", "HEAD:Cargo.lock"], capture_output=True, text=True
    )
    if committed.returncode == 0:
        texts.append(committed.stdout)
    lock = os.path.join(root, "Cargo.lock")
    if os.path.exists(lock):
        texts.append(open(lock).read())
    urls = []
    for block in "".join(texts).split("[[package]]"):
        if re.search(r'^name = "heed"$', block, re.M):
            m = re.search(r'^source = "git\+([^?#"]+)', block, re.M)
            if m and m.group(1) not in urls:
                urls.append(m.group(1))
    return urls


def on(s, begin, end, shim, git_urls):
    """`shim` None is the stock build: only the `[workspace]` table, if missing."""
    entry = f'heed = {{ path = "{shim}" }}'
    lines = s.split("\n")
    header = next(
        (k for k, l in enumerate(lines) if re.fullmatch(r"\[patch\.crates-io\]\s*", l)),
        None,
    )
    tail = []
    if shim is None:
        pass
    elif header is None:
        tail += ["[patch.crates-io]", entry]
    else:
        k = header + 1
        while k < len(lines) and not lines[k].lstrip().startswith("["):
            if re.match(r"\s*heed\s*=", lines[k]):
                lines[k] = TAG + lines[k]
            k += 1
        lines[header + 1:header + 1] = [begin, entry, end]
    for url in git_urls if shim is not None else []:
        tail += [f'[patch."{url}"]', entry]
    if not any(re.fullmatch(r"\[workspace\]\s*", l) for l in lines):
        tail += ["[workspace]"]
    s = "\n".join(lines)
    if tail:
        if not s.endswith("\n"):
            s += "\n"
        s += "\n" + "\n".join([begin] + tail + [end]) + "\n"
    return s


def main():
    path, mode, begin, end = sys.argv[1:5]
    s = open(path).read()
    s = off(s, begin, end)
    if mode == "on":
        s = on(s, begin, end, sys.argv[5], heed_git_sources(path))
    elif mode == "stock":
        s = on(s, begin, end, None, [])
    elif mode != "off":
        sys.exit(f"unknown mode {mode!r}: expected on, stock or off")
    open(path, "w").write(s)


if __name__ == "__main__":
    main()
