#!/usr/bin/env python3
"""Append-only ledger of performance attempts: kept, reverted and abandoned.

The ledger exists so that a disproved idea is never re-tried by the next
session. BENCH-MAP records two blind picks the July 2026 campaign disproved; this
file is where the next ones go. One JSON object per line in
`benches/results/perf-ledger.jsonl`, committed with the change or with the revert.

    scripts/perf-ledger.py show [--rung REGEX] [--last N]
        Prior attempts, newest last. Read this before choosing a lever.

    scripts/perf-ledger.py add --rung R --hypothesis TEXT --outcome O
                               [--verdict PATH] [--commit SHA] [--notes TEXT] [--date ISO]
        O is one of: kept, reverted, abandoned, blocked-adr.
        --verdict copies the headline numbers out of a bench-ab verdict.json
        (the run directory itself lives under target/ and is not kept).
"""
import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

LEDGER = Path(__file__).resolve().parent.parent / "benches/results/perf-ledger.jsonl"
OUTCOMES = ("kept", "reverted", "abandoned", "blocked-adr")


def entries():
    if not LEDGER.exists():
        return []
    return [json.loads(l) for l in LEDGER.read_text().splitlines() if l.strip()]


def cmd_show(args) -> int:
    rows = entries()
    if args.rung:
        rx = re.compile(args.rung)
        rows = [e for e in rows if rx.search(e.get("rung", ""))
                or any(rx.search(r["rung"]) for r in e.get("rungs", []))]
    rows = rows[-args.last:] if args.last else rows
    if not rows:
        print("(no matching attempts)")
    for e in rows:
        print(f"{e['date'][:10]}  {e['outcome']:<11} {e['rung']}  "
              f"[{e.get('verdict', 'no bench')}]  {e.get('commit', '')}")
        print(f"    hypothesis: {e['hypothesis']}")
        for r in e.get("rungs", []):
            print(f"    {r['rung']}: {r['ratio_vs_lmdb_before']:.2f}× → "
                  f"{r['ratio_vs_lmdb_after']:.2f}× vs LMDB ({r['state']})")
        if e.get("notes"):
            print(f"    notes: {e['notes']}")
    return 0


def cmd_add(args) -> int:
    e = {
        "date": args.date or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rung": args.rung,
        "hypothesis": args.hypothesis,
        "outcome": args.outcome,
    }
    if args.commit:
        e["commit"] = args.commit
    if args.verdict:
        v = json.loads(Path(args.verdict).read_text())
        e["verdict"] = v["verdict"]
        e["base"] = v.get("base")
        e["host"] = v.get("host")
        e["rounds"] = v.get("rounds_complete")
        # Keep the target rungs plus anything that moved; flat off-target rungs
        # are noise in a ledger.
        e["rungs"] = [
            {k: r[k] for k in ("rung", "state", "ratio_vs_lmdb_before",
                               "ratio_vs_lmdb_after", "speedup")}
            for r in v["rungs"] if r["target"] or r["state"] in ("improved", "regressed")
        ]
    if args.notes:
        e["notes"] = args.notes
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    with LEDGER.open("a") as fh:
        fh.write(json.dumps(e, ensure_ascii=False) + "\n")
    print(f"recorded: {args.outcome} {args.rung}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("show")
    s.add_argument("--rung", default="")
    s.add_argument("--last", type=int, default=0)
    a = sub.add_parser("add")
    a.add_argument("--rung", required=True, help="the primary target rung")
    a.add_argument("--hypothesis", required=True)
    a.add_argument("--outcome", required=True, choices=OUTCOMES)
    a.add_argument("--verdict", help="path to a bench-ab verdict.json")
    a.add_argument("--commit")
    a.add_argument("--notes")
    a.add_argument("--date", help="ISO date, for back-filling a historical attempt")
    args = ap.parse_args()
    return cmd_show(args) if args.cmd == "show" else cmd_add(args)


if __name__ == "__main__":
    sys.exit(main())
