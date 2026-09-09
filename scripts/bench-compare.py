#!/usr/bin/env python3
"""Compare two `cargo xtask bench --no-dashboard` report trees: LMDB vs ZeroDB.

Each report file (`<workload>-<run>-report.json`) is JSON Lines, one object per
span: `{"<span name>": {"call_count": N, "time": ns, "self_time": ns}}`. A
workload with `run_count: 10` yields ten such files per invocation. Reports may
be nested one level (`round-N/`), as scripts/consumer.sh writes them.

For every workload this prints the per-run total (sum of every span's
`self_time`, i.e. the wall time spent inside instrumented code) as median and
min over runs for both engines, plus the spans that dominate, so a regression
can be attributed to indexing, search, commit, and so on. The per-span column
is deliberately the *inclusive* `time` (what Meilisearch's benchmark dashboard
shows): `indexing::write_db::all` is only meaningful with its children
counted. Inclusive spans therefore overlap and do not sum to the total.

Usage: bench-compare.py <lmdb-report-dir> <zerodb-report-dir> [--top N]
"""
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def load(root: Path):
    """{workload: [ {span: (calls, time, self_time)}, ... one per run ]}"""
    runs = defaultdict(list)
    for f in sorted(root.rglob("*-report.json")):
        name, _, _ = f.name.rpartition("-report.json")[0].rpartition("-")
        spans = {}
        with f.open() as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                (k, v), = json.loads(line).items()
                spans[k] = (v["call_count"], v["time"], v["self_time"])
        runs[name].append(spans)
    return runs


def total_ns(spans):
    return sum(s[2] for s in spans.values())


def fmt(ns):
    if ns >= 1e9:
        return f"{ns / 1e9:8.3f} s "
    return f"{ns / 1e6:8.1f} ms"


def main(argv):
    top = 12
    if "--top" in argv:
        i = argv.index("--top")
        top = int(argv[i + 1])
        del argv[i:i + 2]
    if len(argv) != 3:
        print(__doc__)
        return 2
    a, b = load(Path(argv[1])), load(Path(argv[2]))
    workloads = sorted(set(a) | set(b))
    if not workloads:
        print("no *-report.json files found under the given directories")
        return 1
    for w in workloads:
        ra, rb = a.get(w, []), b.get(w, [])
        print(f"\n== {w}  (runs: lmdb {len(ra)}, zerodb {len(rb)})")
        if not ra or not rb:
            print("   one side has no runs; skipping")
            continue
        ta = [total_ns(r) for r in ra]
        tb = [total_ns(r) for r in rb]
        ma, mb = statistics.median(ta), statistics.median(tb)
        print(f"   {'total (self time)':44s} lmdb {fmt(ma)}  zerodb {fmt(mb)}  ratio {mb / ma:5.2f}x"
              f"   (min {fmt(min(ta)).strip()} / {fmt(min(tb)).strip()})")
        # dominant spans by LMDB median *inclusive* time (index 1), compared on
        # the ZeroDB side; the total above is self time (index 2) — see the docstring
        names = set()
        for r in ra + rb:
            names.update(r)
        rows = []
        for n in names:
            sa = [r[n][1] for r in ra if n in r]
            sb = [r[n][1] for r in rb if n in r]
            if len(sa) < len(ra) // 2 or len(sb) < len(rb) // 2:
                continue  # not present in most runs on both sides
            rows.append((statistics.median(sa), statistics.median(sb), n))
        rows.sort(reverse=True)
        print(f"   {'span (median inclusive time over runs)':44s} {'lmdb':>11s}  {'zerodb':>13s}  ratio")
        for sa, sb, n in rows[:top]:
            ratio = f"{sb / sa:5.2f}x" if sa else "   n/a"
            print(f"   {n[:44]:44s} {fmt(sa)}  {fmt(sb)}  {ratio}")
    print("\nratio < 1.00x means ZeroDB is faster; spans are inclusive and summed across calls per run.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
