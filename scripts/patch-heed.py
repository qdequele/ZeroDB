#!/usr/bin/env python3
"""Point a consumer's `heed` dependency at ZeroDB's heed adapter, or undo it.

Usage: patch-heed.py <Cargo.toml> on|stock|off <begin marker> <end marker>
                     [<adapter path> [<feature manifest>]]

The adapter is `crates/heed-zerodb`, a crate named `heed` at version 0.22.1.
`on` adds `heed = { path = "<adapter>" }` under `[patch.crates-io]`. When the
manifest already has that section (Meilisearch does, for its own heed fork),
the line goes inside it and the consumer's own `heed` entry is commented out,
since cargo refuses a second `[patch.crates-io]` table and two `heed` keys.
Otherwise a new section is appended. Crates that take heed from a git
repository instead (Meilisearch v1.55's cellulite takes it from Meilisearch's
heed fork) are not reached by `[patch.crates-io]`: every git source of heed in
the consumer's `Cargo.lock` gets its own `[patch."<url>"]` table. 

A `[patch]` cannot turn on a feature, so a consumer built against Meilisearch's
heed fork needs the adapter's `unique-txns` feature enabled by one of its own
dependencies: given a feature manifest (a member crate in every build, such as
milli), `on` adds `unique-txns` to that manifest's `heed` dependency.

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

TAG = "# zerodb-patch-disabled: "


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
    """The `(url, query)` git sources `Cargo.lock` resolves a `heed` package
    from, `query` being the lock's `branch=…`/`rev=…`/`tag=…` part or empty.

    Reads the lock file as committed as well as the one on disk: a build with
    the adapter patched in rewrites the lock file without the git entries.
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
            m = re.search(r'^source = "git\+([^?#"]+)(?:\?([^#"]*))?', block, re.M)
            if m and (m.group(1), m.group(2) or "") not in urls:
                urls.append((m.group(1), m.group(2) or ""))
    return urls


def on(s, begin, end, adapter, git_urls):
    """`adapter` None is the stock build: only the `[workspace]` table, if missing."""
    entry = f'heed = {{ path = "{adapter}" }}'
    lines = s.split("\n")
    header = next(
        (k for k, l in enumerate(lines) if re.fullmatch(r"\[patch\.crates-io\]\s*", l)),
        None,
    )
    tail = []
    if adapter is None:
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
    for url, _ in git_urls if adapter is not None else []:
        tail += [f'[patch."{url}"]', entry]
    if not any(re.fullmatch(r"\[workspace\]\s*", l) for l in lines):
        tail += ["[workspace]"]
    s = "\n".join(lines)
    if tail:
        if not s.endswith("\n"):
            s += "\n"
        s += "\n" + "\n".join([begin] + tail + [end]) + "\n"
    return s


def enable_feature(s, begin, end):
    """Add `unique-txns` to the features of the manifest's `heed` dependency.

    The entry is an inline table, possibly over several lines, as milli's is.
    Its lines are commented out with the tag and the edited copy goes between
    the markers, so `off` restores them. Cargo refuses a second, renamed
    dependency on the same crate, hence the edit in place.
    """
    lines = s.split("\n")
    k = next((i for i, l in enumerate(lines) if re.match(r"heed\s*=\s*\{", l)), None)
    if k is None:
        sys.exit("no `heed = { ... }` dependency to turn the unique-txns feature on in")
    depth, j = 0, k
    while True:
        depth += lines[j].count("{") - lines[j].count("}")
        if depth == 0:
            break
        j += 1
    entry = "\n".join(lines[k:j + 1])
    if re.search(r"features\s*=\s*\[", entry):
        entry = re.sub(r"(features\s*=\s*\[)", r'\1"unique-txns", ', entry, count=1)
    else:
        entry = re.sub(r"heed\s*=\s*\{", 'heed = { features = ["unique-txns"],', entry, count=1)
    original = [TAG + l for l in lines[k:j + 1]]
    lines[k:j + 1] = original + [begin] + entry.split("\n") + [end]
    return "\n".join(lines)


def main():
    path, mode, begin, end = sys.argv[1:5]
    adapter = sys.argv[5] if len(sys.argv) > 5 else None
    feature_manifest = sys.argv[6] if len(sys.argv) > 6 and sys.argv[6] else None
    sources = heed_git_sources(path) if mode == "on" else []
    s = off(open(path).read(), begin, end)
    if mode == "on":
        s = on(s, begin, end, adapter, sources)
    elif mode == "stock":
        s = on(s, begin, end, None, [])
    elif mode != "off":
        sys.exit(f"unknown mode {mode!r}: expected on, stock or off")
    open(path, "w").write(s)
    if feature_manifest and os.path.exists(feature_manifest):
        f = off(open(feature_manifest).read(), begin, end)
        if mode == "on" and sources:
            f = enable_feature(f, begin, end)
        open(feature_manifest, "w").write(f)


if __name__ == "__main__":
    main()
