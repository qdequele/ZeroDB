#!/usr/bin/env python3
"""Turn a completed `engine_comparison` criterion run into the LMDB-vs-ZeroDB
ratio table.

Criterion writes one directory per benchmark id under `target/criterion/`, each
holding `new/benchmark.json` (the id: group + function) and `new/estimates.json`
(the statistics). This pairs the `lmdb` and `zerodb` functions inside each
group and prints `ratio = zerodb / lmdb` — above 1.0 means zerodb is slower.

The point of the ladder (see `docs/BENCH-MAP.md`) is that adjacent rungs in a
family differ by exactly one mechanism, so this also prints, per family, how
much each rung's ratio moved against the family's FIRST rung. A rung whose
ratio jumps is naming the mechanism that rung added.

Usage:
    scripts/bench-report.py [--dir target/criterion] [--md] [--min-jump 0.15]
"""
import argparse
import json
import sys
from collections import OrderedDict
from pathlib import Path

# Criterion's function name for each engine, and the direction of the ratio.
LMDB = "lmdb"
ZERODB = "zerodb"

# The reference rung of each ladder family: the cheapest or most-isolated one,
# so every other rung's delta reads as "what this rung's extra mechanism added".
# `docs/BENCH-MAP.md` lists the same base per family and explains each choice —
# that document is the source of truth; this table only encodes the ordering.
# A family missing here (or whose base was filtered out of the run) falls back
# to its first rung, and the report says so.
LADDER_BASE = {
    "env/open": "env/open/reopen",
    "env/txn": "env/txn/ro_begin_abort",
    "get/db": "get/db/root",
    "get/access": "get/access/hot",
    "get/size": "get/size/n1k",
    "get/key": "get/key/k8",
    "get/val": "get/val/v8",
    "scan/full": "scan/full/fwd",
    "scan/range": "scan/range/1pct",
    "seek/ge": "seek/ge/seq",
    "put/order": "put/order/append",
    "put/val": "put/val/v8",
    "put/api": "put/api/plain",
    "put/over": "put/over/same_size",
    "del/bulk": "del/bulk/half",
    "commit/batch": "commit/batch/n10k",
    "commit/sync": "commit/sync/n100",
    "maint/copy": "maint/copy/raw",
    "concurrent/writer": "concurrent/writer/r1",
}


def load(root: Path):
    """{group_id: {function_id: (point_estimate_ns, std_dev_ns)}} in run order."""
    groups = OrderedDict()
    for bench in sorted(root.rglob("new/benchmark.json")):
        est = bench.parent / "estimates.json"
        if not est.exists():
            continue
        with bench.open() as fh:
            bid = json.load(fh)
        group = bid.get("group_id")
        func = bid.get("function_id")
        if not group or not func:
            continue
        with est.open() as fh:
            e = json.load(fh)
        # `slope` is criterion's preferred estimator when it has one (linear
        # sampling); `mean` is the fallback for flat sampling. `typical()` in
        # criterion's own reporter makes exactly this choice.
        typical = e.get("slope") or e.get("mean")
        groups.setdefault(group, {})[func] = (
            typical["point_estimate"],
            e["std_dev"]["point_estimate"],
        )
    return groups


def human(ns: float) -> str:
    for unit, scale in (("s", 1e9), ("ms", 1e6), ("µs", 1e3)):
        if ns >= scale:
            return f"{ns / scale:.3f} {unit}"
    return f"{ns:.1f} ns"


def family_of(group: str) -> str:
    """`get/db/named` -> `get/db`. The rungs a ladder step compares within."""
    parts = group.split("/")
    return "/".join(parts[:2]) if len(parts) > 2 else group


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dir", default="target/criterion", type=Path)
    ap.add_argument("--md", action="store_true", help="emit a Markdown table")
    ap.add_argument(
        "--min-jump",
        type=float,
        default=0.15,
        help="flag a rung whose ratio differs from its family's first rung by "
        "more than this (default 0.15 = 15 percentage points of ratio)",
    )
    args = ap.parse_args()

    if not args.dir.exists():
        print(f"no criterion output at {args.dir} — run `just bench` first", file=sys.stderr)
        return 1

    groups = load(args.dir)
    skipped = []
    measured = OrderedDict()

    for group, funcs in groups.items():
        if LMDB not in funcs or ZERODB not in funcs:
            skipped.append(group)
            continue
        l_ns, l_sd = funcs[LMDB]
        z_ns, z_sd = funcs[ZERODB]
        measured[group] = (
            l_ns,
            z_ns,
            z_ns / l_ns if l_ns else float("nan"),
            # Noise guard: a ratio whose two sides are each within a std dev of
            # the other is not a finding, however far from 1.0 it sits.
            abs(z_ns - l_ns) < (l_sd + z_sd),
        )

    # Group into families, base rung first, so the table reads as a ladder.
    families = OrderedDict()
    for group in measured:
        families.setdefault(family_of(group), []).append(group)

    rows, improvised = [], []
    for fam, members in families.items():
        want = LADDER_BASE.get(fam)
        if want in members:
            base_name = want
        else:
            base_name = members[0]
            if want:
                improvised.append((fam, want, base_name))
        ordered = [base_name] + [m for m in members if m != base_name]
        base_ratio = measured[base_name][2]
        for group in ordered:
            l_ns, z_ns, ratio, noisy = measured[group]
            rows.append((group, l_ns, z_ns, ratio, ratio - base_ratio, noisy))

    if not rows:
        print("no group had both engines — was the run filtered?", file=sys.stderr)
        return 1

    if args.md:
        print("| rung | LMDB | ZeroDB | ratio | vs family base |")
        print("|---|---:|---:|---:|---:|")
        for g, l, z, r, j, noisy in rows:
            flag = " ⚠" if abs(j) > args.min_jump else ""
            note = " *(noise)*" if noisy else ""
            print(
                f"| `{g}` | {human(l)} | {human(z)} | **{r:.2f}×**{note} "
                f"| {j:+.2f}{flag} |"
            )
    else:
        w = max(len(r[0]) for r in rows)
        print(f"{'rung':<{w}}  {'LMDB':>11}  {'ZeroDB':>11}  {'ratio':>7}  {'Δfam':>7}")
        print("-" * (w + 44))
        for g, l, z, r, j, noisy in rows:
            flag = " <<" if abs(j) > args.min_jump else ""
            note = " ~" if noisy else ""
            print(
                f"{g:<{w}}  {human(l):>11}  {human(z):>11}  {r:>6.2f}x{note}  {j:>+6.2f}{flag}"
            )

    slowest = sorted((r for r in rows if not r[5]), key=lambda r: -r[3])[:5]
    if slowest:
        print("\nWorst rungs (noise-filtered), slowest first:", file=sys.stderr)
        for g, _l, _z, r, _j, _n in slowest:
            print(f"  {r:5.2f}x  {g}", file=sys.stderr)
    for fam, want, used in improvised:
        print(
            f"\nnote: family {fam} has no `{want}` in this run — deltas are "
            f"against `{used}` instead.",
            file=sys.stderr,
        )
    if skipped:
        print(
            f"\nSkipped (only one engine present): {', '.join(skipped)}",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
