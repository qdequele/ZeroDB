#!/usr/bin/env python3
"""Verdict for a `scripts/bench-ab.sh` run: ZeroDB before vs after, LMDB as the
drift control.

Input is the run directory bench-ab.sh writes:

    <run>/meta.json
    <run>/round-N/{before,after}/     criterion output of each binary, per round

Every rung then has, per round, four estimates: LMDB and ZeroDB from the
"before" binary and the same two from the "after" binary. LMDB's source is
identical in both binaries, so its before/after movement is pure measurement
drift, not code. Per rung:

* ``speedup = median over rounds of (ZeroDB after ÷ ZeroDB before)``;
  below 1 means the change made ZeroDB faster.
* ``drift = median over rounds of (LMDB after ÷ LMDB before)``. More than
  ``--max-drift`` away from 1 marks the rung *unreliable*: the machine moved
  under the measurement, so nothing about ZeroDB can be read from it.
* ``noise = max(criterion's 95 % CI on the two ZeroDB estimates, combined,
  2 × round-to-round stdev of the speedup)``. A change is only reported when
  it exceeds ``max(--min-effect, noise)``.

Per rung, the verdict is improved, regressed, flat or unreliable. The run verdict
(``verdict.json`` → ``verdict``) is one of:

* ``invalid``: more than half the rungs are unreliable, or every target rung
  is. Re-run it on a quieter machine; do not decide anything from it.
* ``regressed``: a reliable rung got slower. This counts even outside the target
  set, because a win that costs elsewhere is not a win.
* ``improved``: at least one target rung got faster and nothing regressed.
* ``flat``: none of the above.

Stdout is the three-column Markdown table (LMDB | ZeroDB before | ZeroDB after)
that the performance-claim rule asks for; paste it as-is.

Usage: bench-ab.py <run-dir> [--target REGEX] [--min-effect 0.03] [--max-drift 0.03]
"""
import argparse
import importlib.util
import json
import math
import re
import statistics
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "bench_report", Path(__file__).with_name("bench-report.py")
)
report = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(report)


def rel_ci(e) -> float:
    """Half-width of the 95 % CI as a fraction of the point estimate."""
    return (e["hi"] - e["lo"]) / 2 / e["point"] if e["point"] else math.inf


def rounds_of(run: Path):
    """[(round_no, {group: {func: est}} before, ... after)] in round order."""
    out = []
    for d in sorted(run.glob("round-*"), key=lambda p: int(p.name.split("-")[1])):
        b, a = d / "before", d / "after"
        if b.exists() and a.exists():
            out.append((int(d.name.split("-")[1]),
                        report.load_estimates(b), report.load_estimates(a)))
    return out


def analyse(rounds, target: str, min_effect: float, max_drift: float):
    tre = re.compile(target) if target else None
    rungs = []
    groups = []
    for _, before, after in rounds:
        for g in list(before) + list(after):
            if g not in groups:
                groups.append(g)
    for g in groups:
        per = []
        for n, before, after in rounds:
            fb, fa = before.get(g, {}), after.get(g, {})
            if all(k in f for f in (fb, fa) for k in (report.LMDB, report.ZERODB)):
                per.append((n, fb[report.LMDB], fb[report.ZERODB],
                            fa[report.LMDB], fa[report.ZERODB]))
        if not per:
            continue
        speed = [zc["point"] / zb["point"] for _, _, zb, _, zc in per]
        drift = [lc["point"] / lb["point"] for _, lb, _, lc, _ in per]
        ci = statistics.median(
            math.hypot(rel_ci(zb), rel_ci(zc)) for _, _, zb, _, zc in per
        )
        spread = 2 * statistics.stdev(speed) if len(speed) >= 2 else 0.0
        noise = max(ci, spread)
        threshold = max(min_effect, noise)
        s, d = statistics.median(speed), statistics.median(drift)
        if abs(d - 1) > max_drift:
            state = "unreliable"
        elif s < 1 - threshold:
            state = "improved"
        elif s > 1 + threshold:
            state = "regressed"
        else:
            state = "flat"
        lmdb = statistics.median(
            [lb["point"] for _, lb, _, _, _ in per] + [lc["point"] for _, _, _, lc, _ in per]
        )
        before_ns = statistics.median(zb["point"] for _, _, zb, _, _ in per)
        after_ns = statistics.median(zc["point"] for _, _, _, _, zc in per)
        rungs.append({
            "rung": g,
            "target": bool(tre.search(g)) if tre else True,
            "state": state,
            "lmdb_ns": lmdb,
            "before_ns": before_ns,
            "after_ns": after_ns,
            "ratio_vs_lmdb_before": before_ns / lmdb,
            "ratio_vs_lmdb_after": after_ns / lmdb,
            "speedup": s,
            "lmdb_drift": d,
            "noise": noise,
            "threshold": threshold,
            "rounds": [
                {"round": n, "lmdb_before": lb["point"], "zerodb_before": zb["point"],
                 "lmdb_after": lc["point"], "zerodb_after": zc["point"]}
                for n, lb, zb, lc, zc in per
            ],
        })
    return rungs


def overall(rungs):
    by = {k: [r["rung"] for r in rungs if r["state"] == k]
          for k in ("improved", "regressed", "flat", "unreliable")}
    targets = [r for r in rungs if r["target"]]
    if not rungs or len(by["unreliable"]) * 2 > len(rungs) or (
        targets and all(r["state"] == "unreliable" for r in targets)
    ):
        v = "invalid"
    elif by["regressed"]:
        v = "regressed"
    elif any(r["state"] == "improved" for r in targets):
        v = "improved"
    else:
        v = "flat"
    return v, by


def table(rungs) -> str:
    h = report.human
    lines = [
        "| rung | LMDB | ZeroDB before | ZeroDB after | vs LMDB before → after "
        "| after ÷ before | LMDB drift | verdict |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for r in rungs:
        mark = {"improved": "✅ faster", "regressed": "❌ slower",
                "flat": "flat", "unreliable": "⚠ drift"}[r["state"]]
        tgt = " 🎯" if r["target"] else ""
        lines.append(
            f"| `{r['rung']}`{tgt} | {h(r['lmdb_ns'])} | {h(r['before_ns'])} "
            f"| {h(r['after_ns'])} | {r['ratio_vs_lmdb_before']:.2f}× → "
            f"**{r['ratio_vs_lmdb_after']:.2f}×** | {r['speedup']:.3f} "
            f"(±{r['threshold']:.3f}) | {r['lmdb_drift']:.3f} | {mark} |"
        )
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run", type=Path)
    ap.add_argument("--target", default="",
                    help="regex of the rungs the change claims to improve (default: all)")
    ap.add_argument("--min-effect", type=float, default=0.03)
    ap.add_argument("--max-drift", type=float, default=0.03)
    args = ap.parse_args()

    rounds = rounds_of(args.run)
    if not rounds:
        print(f"no complete round under {args.run}", file=sys.stderr)
        return 1
    rungs = analyse(rounds, args.target, args.min_effect, args.max_drift)
    verdict, by = overall(rungs)

    meta_path = args.run / "meta.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    out = {
        **meta,
        "verdict": verdict,
        "rounds_complete": len(rounds),
        "min_effect": args.min_effect,
        "max_drift": args.max_drift,
        "summary": by,
        "rungs": rungs,
    }
    (args.run / "verdict.json").write_text(json.dumps(out, indent=1) + "\n")

    print(table(rungs))
    print(f"\n**Verdict: {verdict}**. Improved: {len(by['improved'])}, regressed: "
          f"{len(by['regressed'])}, flat: {len(by['flat'])}, unreliable (LMDB drift "
          f"> {args.max_drift:.0%}): {len(by['unreliable'])}. {len(rounds)} interleaved "
          f"round(s); before = `{meta.get('base', '?')[:9]}`, after = "
          f"`{meta.get('candidate', '?')}`, host `{meta.get('host', '?')}`.")
    if meta.get("load_avg", 0) > meta.get("ncpu", 1e9) / 2:
        print(f"\n_Load average was {meta['load_avg']} on {meta['ncpu']} CPUs when the "
              "run started: the machine was busy, so read every row with suspicion._")
    if len(rounds) < 3:
        print("\n_Fewer than 3 rounds: the round-to-round spread is not estimated; "
              "treat this as a smoke run._")
    return 0


if __name__ == "__main__":
    sys.exit(main())
