#!/usr/bin/env python3
"""Table of a cold.sh run: `cold-report.py results/<run>/results.jsonl`.

One row per measurement: reads per second, latency, and what each read cost
the disk (read requests and bytes, from the block device's counters).
"""
import json
import sys

rows = [json.loads(line) for line in open(sys.argv[1]) if line.strip()]
print("| data set | engine | page | read-ahead | phase | threads | reads/s | p50 | p99 | disk reads per read | KiB read per read | file GiB |")
print("|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|")
for r in rows:
    if r["phase"] == "load":
        continue
    reads = r["reads"] or 1
    ios, nbytes = r.get("device_read_ios"), r.get("device_read_bytes")
    per = lambda v, div=1: "n/a" if v is None else f"{v / reads / div:.2f}"
    ms = lambda us: f"{us / 1000:.2f} ms" if us >= 1000 else f"{us:.0f} µs"
    page = f"{r['page_kib']} KiB" if r.get("page_kib") else "default"
    print(
        f"| {r['label']} | {r['engine']} | {page} | {'on' if r.get('read_ahead') else 'off'} "
        f"| {r['phase']} | {r['threads']} | {r['reads_per_sec']:.0f} | {ms(r['read_p50_us'])} "
        f"| {ms(r['read_p99_us'])} | {per(ios)} | {per(nbytes, 1024)} | {r['disk_bytes'] / 2**30:.2f} |"
    )
