#!/usr/bin/env python3
"""Compare divan bench tables captured by scripts/hannoy.sh: LMDB vs ZeroDB.

Reads every `lmdb-round-N.txt` / `zerodb-round-N.txt` under the given directory,
parses divan's table (`name │ fastest │ slowest │ median │ mean │ samples │
iters`, drawn as a tree with ├─ ╰─ │ connectors), and prints the median per
benchmark for both engines with the ZeroDB/LMDB ratio. Several rounds are
combined by taking the median of the per-round medians.

Usage: divan-compare.py <reports-dir>
"""
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

UNIT = {"ns": 1e-9, "µs": 1e-6, "us": 1e-6, "ms": 1e-3, "s": 1.0}
TIME = re.compile(r"(\d+(?:\.\d+)?)\s*(ns|µs|us|ms|s)\b")
TREE = re.compile(r"[├╰└│─]+")


def parse(path):
    """{bench name: median seconds} from one divan run."""
    out = {}
    groups = []  # nesting stack of header names, e.g. ["hnsw", "build_hnsw"]
    for raw in path.read_text(errors="replace").splitlines():
        if "│" not in raw or raw.lstrip().startswith("benchmark"):
            continue
        # divan indents 3 columns per nesting level; the name starts after the
        # tree glyphs, so its column gives the depth (root group at column 3).
        padded = TREE.sub(lambda m: " " * len(m.group()), raw)
        name_col = len(padded) - len(padded.lstrip(" "))
        depth = max(name_col // 3 - 1, 0)
        tokens = padded.split()
        if not tokens:
            continue
        name = tokens[0]
        times = [float(v) * UNIT[u] for v, u in TIME.findall(padded)]
        if not times:
            # a group header row at this depth
            groups = groups[:depth] + [name]
            continue
        if len(times) < 4:
            continue
        parent = groups[depth - 1] if depth >= 1 and len(groups) >= depth else None
        full = f"{parent}::{name}" if parent else name
        out[full] = times[2]  # fastest, slowest, MEDIAN, mean
    return out


def fmt(s):
    if s >= 1:
        return f"{s:8.3f} s "
    if s >= 1e-3:
        return f"{s * 1e3:8.2f} ms"
    return f"{s * 1e6:8.1f} µs"


def main(argv):
    if len(argv) != 2:
        print(__doc__)
        return 2
    root = Path(argv[1])
    runs = {"lmdb": defaultdict(list), "zerodb": defaultdict(list)}
    for f in sorted(root.glob("*-round-*.txt")):
        engine = f.name.split("-round-")[0]
        if engine not in runs:
            continue
        for name, med in parse(f).items():
            runs[engine][name].append(med)
    names = sorted(set(runs["lmdb"]) | set(runs["zerodb"]))
    if not names:
        print(f"no divan rows parsed under {root}")
        return 1
    rounds = max((len(v) for e in runs.values() for v in e.values()), default=0)
    print(f"\nhannoy `cargo bench --bench benchmark`, median per bench ({rounds} round(s) per engine)")
    print(f"   {'bench':40s} {'lmdb':>11s}  {'zerodb':>13s}  ratio")
    for n in names:
        a, b = runs["lmdb"].get(n), runs["zerodb"].get(n)
        if not a or not b:
            print(f"   {n[:40]:40s} {'—' if not a else fmt(statistics.median(a))}  "
                  f"{'—' if not b else fmt(statistics.median(b))}  (one side missing)")
            continue
        ma, mb = statistics.median(a), statistics.median(b)
        print(f"   {n[:40]:40s} {fmt(ma)}  {fmt(mb)}  {mb / ma:5.2f}x")
    print("\nratio < 1.00x means ZeroDB is faster.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
