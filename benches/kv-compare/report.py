#!/usr/bin/env python3
"""Turn a kv-compare `results.jsonl` into Markdown tables and SVG charts.

    python3 report.py results/<run>/results.jsonl [--out results/<run>]

Writes `<out>/report.md` (every number, as tables),
`<out>/charts/<name>.{light,dark}.svg`, and `<out>/readme-section.md`: the
section to paste into the top-level README, charts referenced under
`--chart-path` (the charts' path from the repository root). Charts come in a
light and a dark file, picked with
`<picture><source media="(prefers-color-scheme: dark)" ...>`. Standard
library only.
"""

import argparse
import json
import os
from collections import defaultdict

# Fixed engine order and colours: a colour always means the same engine.
# Validated as a set (adjacent CVD and normal-vision separation, both modes).
ENGINES = ["zerodb", "lmdb", "fjall", "rocksdb", "redb", "sqlite", "zerodb-trusted"]
NAMES = {
    "zerodb": "ZeroDB",
    "lmdb": "LMDB",
    "fjall": "fjall",
    "rocksdb": "RocksDB",
    "redb": "redb",
    "sqlite": "SQLite",
    "zerodb-trusted": "ZeroDB, trusted file",
}
THEMES = {
    "light": {
        "series": ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6"],
        "text": "#0b0b0b",
        "muted": "#52514e",
        "grid": "#e4e3df",
        "axis": "#c3c2b7",
        "surface": "#ffffff",
    },
    "dark": {
        "series": ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9"],
        "text": "#f0efec",
        "muted": "#c3c2b7",
        "grid": "#2f2f2c",
        "axis": "#4a4a46",
        "surface": "#0d1117",
    },
}
FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"
SETS = {"small": "Fits in memory", "large": "Larger than memory"}

# What each store is, from each project's documentation and source
# (fjall's 64 KiB key limit: its `Keyspace::insert` docs; ZeroDB processes:
# docs/COMPATIBILITY.md, ADR-0023).
FEATURES = [
    ("Design", ["B+ tree, memory-mapped", "B+ tree, memory-mapped", "LSM tree", "LSM tree",
                "Copy-on-write B-tree", "B-tree"]),
    ("Written in", ["Rust", "C", "Rust", "C++", "Rust", "C"]),
    ("Writers at the same time", ["1", "1", "Many", "Many", "1", "1"]),
    ("Read-write transactions", ["Yes", "Yes", "Optional mode", "Optional mode", "Yes", "Yes"]),
    ("Several processes on one database", ["Yes", "Yes", "No", "One writer, read-only others",
                                           "One writer, read-only others", "Yes"]),
    ("Largest key", ["511 bytes", "511 bytes", "64 KiB", "No practical limit", "No practical limit",
                     "No practical limit"]),
]
FEATURE_ENGINES = ["zerodb", "lmdb", "fjall", "rocksdb", "redb", "sqlite"]


def human(v):
    """1234567 -> '1.2M', 2345 -> '2.3k', 12.3 -> '12'."""
    if v == 0:
        return "0"
    for div, suffix in ((1e9, "G"), (1e6, "M"), (1e3, "k")):
        if abs(v) >= div:
            x = v / div
            return f"{x:.1f}{suffix}" if x < 100 else f"{x:.0f}{suffix}"
    return f"{v:.1f}" if abs(v) < 10 else f"{v:.0f}"


def gb(v):
    return f"{v / 1e9:.1f} GB" if v >= 1e9 else f"{v / 1e6:.0f} MB"


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def nice_max(v):
    """A round axis maximum at or above v."""
    if v <= 0:
        return 1
    mag = 10 ** (len(str(int(v))) - 1)
    for m in (1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10):
        if m * mag >= v:
            return m * mag
    return 10 * mag


class Svg:
    def __init__(self, w, h, theme):
        self.w, self.h, self.t = w, h, THEMES[theme]
        self.parts = []

    def text(self, x, y, s, size=12, color="text", anchor="start", weight=400):
        c = self.t[color] if color in self.t else color
        self.parts.append(
            f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" fill="{c}" '
            f'text-anchor="{anchor}" font-weight="{weight}">{esc(s)}</text>'
        )

    def line(self, x1, y1, x2, y2, color="grid", width=1):
        c = self.t[color] if color in self.t else color
        self.parts.append(
            f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{c}" stroke-width="{width}"/>'
        )

    def bar(self, x, y, w, h, color, title):
        # Rounded data end, square at the baseline.
        r = min(4, w / 2, h / 2)
        if w <= 0:
            return
        d = (
            f"M{x:.1f},{y:.1f} h{w - r:.1f} a{r},{r} 0 0 1 {r},{r} v{h - 2 * r:.1f} "
            f"a{r},{r} 0 0 1 {-r},{r} h{-(w - r):.1f} z"
        )
        self.parts.append(f'<path d="{d}" fill="{color}"><title>{esc(title)}</title></path>')

    def polyline(self, pts, color, title):
        p = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
        self.parts.append(
            f'<polyline points="{p}" fill="none" stroke="{color}" stroke-width="2" '
            f'stroke-linejoin="round" stroke-linecap="round"><title>{esc(title)}</title></polyline>'
        )

    def dot(self, x, y, color, title):
        self.parts.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{color}" stroke="{self.t["surface"]}" '
            f'stroke-width="2"><title>{esc(title)}</title></circle>'
        )

    def render(self):
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.w}" height="{self.h}" '
            f'viewBox="0 0 {self.w} {self.h}" font-family="{FONT}">' + "".join(self.parts) + "</svg>"
        )


def color(theme, eng):
    return THEMES[theme]["series"][ENGINES.index(eng)]


# ---------------------------------------------------------------------------
# Chart forms
# ---------------------------------------------------------------------------


def bars(theme, title, subtitle, panels, fmt=human):
    """Horizontal bars, one panel per data set. panels: [(label, {eng: value})]."""
    engines = [e for e in ENGINES if any(e in p for _, p in panels)]
    row, label_w, panel_w, gap = 22, 150, 300, 40
    w = label_w + len(panels) * panel_w + (len(panels) - 1) * gap + 20
    top = 70
    h = top + len(engines) * row + 20
    s = Svg(w, h, theme)
    s.text(0, 20, title, 15, weight=600)
    s.text(0, 40, subtitle, 12, "muted")
    for i, eng in enumerate(engines):
        s.text(label_w - 10, top + i * row + 15, NAMES[eng], 12, "text", "end")
    for pi, (plabel, values) in enumerate(panels):
        x0 = label_w + pi * (panel_w + gap)
        s.text(x0, top - 12, plabel, 12, "muted", weight=600)
        vmax = max(values.values(), default=0) or 1
        s.line(x0, top - 4, x0, top + len(engines) * row, "axis")
        for i, eng in enumerate(engines):
            if eng not in values:
                continue
            v = values[eng]
            bw = (panel_w - 60) * v / vmax
            y = top + i * row + 4
            s.bar(x0 + 1, y, bw, 14, color(theme, eng), f"{NAMES[eng]}: {fmt(v)}")
            s.text(x0 + bw + 6, y + 11, fmt(v), 11, "muted")
    return s.render()


def legend_rows(engines, width):
    """Lay legend entries (a short line in the series colour, then the name)
    left to right, wrapping at `width`: rows of [(x, engine)]."""
    rows, x = [[]], 0
    for eng in engines:
        # About 6.2 px per character at 11 px, plus the swatch and a gap.
        size = 19 + 6.2 * len(NAMES[eng]) + 18
        if x and x + size > width:
            rows.append([])
            x = 0
        rows[-1].append((x, eng))
        x += size
    return rows if engines else []


def lines(theme, title, subtitle, series, xlabel, ylabel, xticks=None, fmt=human, end_labels=True):
    """One line per engine. series: {eng: [(x, y)]}. A legend names every
    line; up to four lines are also labelled at their ends, beyond which
    the labels crowd away from their lines."""
    engines = [e for e in ENGINES if series.get(e)]
    end_labels = end_labels and len(engines) <= 4
    w = 640
    legend = legend_rows(engines, w)
    left, right, top, bottom = 60, 150 if end_labels else 20, 76 + 18 * len(legend), 46
    h = top + 218 + bottom
    s = Svg(w, h, theme)
    s.text(0, 20, title, 15, weight=600)
    s.text(0, 40, subtitle, 12, "muted")
    for row, entries in enumerate(legend):
        for x, eng in entries:
            y = 60 + 18 * row
            s.line(x, y - 4, x + 14, y - 4, color(theme, eng), 2)
            s.text(x + 19, y, NAMES[eng], 11, "text")
    xs = sorted({x for pts in series.values() for x, _ in pts})
    if not xs:
        return s.render()
    ymax = nice_max(max(y for pts in series.values() for _, y in pts))
    xmin, xmax = min(xs), max(xs)
    pw, ph = w - left - right, h - top - bottom

    def X(x):
        if xticks:  # categorical, evenly spaced
            return left + pw * xticks.index(x) / max(1, len(xticks) - 1)
        return left + pw * (x - xmin) / max(1e-9, xmax - xmin)

    def Y(y):
        return top + ph * (1 - y / ymax)

    for k in range(5):
        yv = ymax * k / 4
        s.line(left, Y(yv), left + pw, Y(yv), "grid")
        s.text(left - 6, Y(yv) + 4, fmt(yv), 11, "muted", "end")
    for x in xticks or [xmin + (xmax - xmin) * k / 5 for k in range(6)]:
        s.text(X(x), top + ph + 16, f"{x:g}", 11, "muted", "middle")
    s.text(left + pw / 2, h - 6, xlabel, 11, "muted", "middle")
    s.text(left - 6, top - 16, ylabel, 11, "muted", "end")
    ends = []
    for eng in ENGINES:
        pts = series.get(eng)
        if not pts:
            continue
        c = color(theme, eng)
        s.polyline([(X(x), Y(y)) for x, y in pts], c, NAMES[eng])
        if xticks:
            for x, y in pts:
                s.dot(X(x), Y(y), c, f"{NAMES[eng]}, {x:g}: {fmt(y)}")
        ends.append([Y(pts[-1][1]), eng])
    if end_labels:
        # Spread end labels so they never overlap.
        ends.sort()
        for i in range(1, len(ends)):
            ends[i][0] = max(ends[i][0], ends[i - 1][0] + 14)
        for y, eng in ends:
            s.text(left + pw + 10, y + 4, NAMES[eng], 11, "text")
    return s.render()


def small_multiples(theme, title, subtitle, series, xlabel, fmt=human):
    """One small line panel per engine on a shared y scale; `xlabel` formats
    the x-axis end value."""
    engines = [e for e in ENGINES if series.get(e)]
    cols, pw, ph, gapx, gapy = 2, 290, 110, 50, 50
    rows = (len(engines) + cols - 1) // cols
    left, top = 50, 64
    w = left + cols * pw + (cols - 1) * gapx + 10
    h = top + rows * (ph + gapy) + 10
    s = Svg(w, h, theme)
    s.text(0, 20, title, 15, weight=600)
    s.text(0, 40, subtitle, 12, "muted")
    ymax = nice_max(max((y for e in engines for _, y in series[e]), default=1))
    xmax = max((x for e in engines for x, _ in series[e]), default=1) or 1
    for i, eng in enumerate(engines):
        x0 = left + (i % cols) * (pw + gapx)
        y0 = top + (i // cols) * (ph + gapy) + 16
        s.text(x0, y0 - 8, NAMES[eng], 12, weight=600)
        for k in range(3):
            yv = ymax * k / 2
            yy = y0 + ph * (1 - yv / ymax)
            s.line(x0, yy, x0 + pw, yy, "grid")
            s.text(x0 - 6, yy + 4, fmt(yv), 10, "muted", "end")
        pts = [(x0 + pw * x / xmax, y0 + ph * (1 - y / ymax)) for x, y in series[eng]]
        s.polyline(pts, color(theme, eng), NAMES[eng])
        s.text(x0 + pw, y0 + ph + 14, xlabel(xmax), 10, "muted", "end")
        s.text(x0, y0 + ph + 14, "0", 10, "muted")
    return s.render()


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------


def load(path):
    runs = [json.loads(line) for line in open(path) if line.strip()]
    for r in runs:
        r["record_bytes"] = 16 + r["value_size"]
    return runs


def pick(runs, **kw):
    """Median-free selection: the last run matching every field (reruns win)."""
    out = {}
    for r in runs:
        if all(r.get(k) == v for k, v in kw.items()):
            out[r["engine"]] = r
    return out


def written(r):
    return r.get("device_written_bytes") or r.get("written_bytes")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results")
    ap.add_argument("--out")
    ap.add_argument("--chart-path", default="charts", help="charts' path from the repository root")
    a = ap.parse_args()
    out = a.out or os.path.dirname(os.path.abspath(a.results))
    os.makedirs(os.path.join(out, "charts"), exist_ok=True)
    runs = load(a.results)
    sets = [s for s in SETS if any(r["label"] == s for r in runs)]
    charts = {}
    md = ["# kv-compare results", ""]

    def per_set(metric, **kw):
        return [(SETS[s], {e: metric(r) for e, r in pick(runs, label=s, **kw).items()}) for s in sets]

    # Basic operations.
    ops = [
        ("read", "Read one key", "Random reads, one thread, reads per second (higher is better)",
         dict(phase="get", threads=1), lambda r: r["reads_per_sec"]),
        ("range", "Read 100 keys in order", "From a random key, one thread, range reads per second (higher is better)",
         dict(phase="range", threads=1), lambda r: r["reads_per_sec"]),
        ("write-durable", "Write one key, saved to disk",
         "Each write waits until it is on disk, writes per second (higher is better)",
         dict(phase="write", batch=1, durable=True), lambda r: r["records_written_per_sec"]),
        ("write-batch", "Write 1,000 keys per commit",
         "Random overwrites, not waiting for the disk, keys per second (higher is better)",
         dict(phase="write", batch=1000, durable=False), lambda r: r["records_written_per_sec"]),
        ("load", "Load the data set", "Insert every record into an empty store, records per second (higher is better)",
         dict(phase="load"), lambda r: r["records_written_per_sec"]),
    ]
    md += ["## Basic operations", ""]
    for name, title, sub, sel, metric in ops:
        panels = per_set(metric, **sel)
        charts[f"op-{name}"] = lambda t, title=title, sub=sub, panels=panels: bars(t, title, sub, panels)
        md += table(title, panels, human)

    # Thread scaling and reads while writing.
    for phase, title, sub in (
        ("get", "Reads on more threads", "Random reads per second, all threads together (higher is better)"),
        ("mixed", "Reads while one thread writes",
         "Random reads per second, all reader threads together, with a writer committing one key at a time"),
    ):
        for s in sets:
            # The last run at each thread count (reruns win), as in `pick`.
            ser = defaultdict(dict)
            for r in runs:
                if r["phase"] == phase and r["label"] == s:
                    ser[r["engine"]][r["threads"]] = r["reads_per_sec"]
            ser = {e: sorted(p.items()) for e, p in ser.items()}
            ticks = sorted({x for p in ser.values() for x, _ in p})
            name = f"{phase}-threads-{s}"
            charts[name] = lambda t, ser=ser, ticks=ticks, title=title, sub=sub, s=s: lines(
                t, f"{title}: {SETS[s].lower()}", sub, ser, "threads", "reads / s", ticks
            )
            md += [f"### {title}: {SETS[s].lower()}", "", "| engine | " + " | ".join(f"{x} thr" for x in ticks) + " |",
                   "|---|" + "---:|" * len(ticks)]
            for e in ENGINES:
                if e in ser:
                    vals = dict(ser[e])
                    md.append(f"| {NAMES[e]} | " + " | ".join(human(vals[x]) if x in vals else "" for x in ticks) + " |")
            if phase == "mixed":
                md += ["", "Read p99 µs / writes per second at each thread count:", "",
                       "| engine | " + " | ".join(f"{x} thr" for x in ticks) + " |", "|---|" + "---:|" * len(ticks)]
                for e in ENGINES:
                    rs = {r["threads"]: r for r in runs if r["phase"] == "mixed" and r["label"] == s and r["engine"] == e}
                    if rs:
                        md.append(f"| {NAMES[e]} | " + " | ".join(
                            f"{rs[x]['read_p99_us']:.1f} / {human(rs[x]['records_written_per_sec'])}" if x in rs else ""
                            for x in ticks) + " |")
            md.append("")

    # Cost.
    disk = per_set(lambda r: r["disk_bytes"], phase="load")
    charts["cost-disk"] = lambda t: bars(t, "Disk space", "Size on disk after loading the data set (lower is better)", disk, gb)
    md += ["## Cost", ""] + table("Disk space after load", disk, gb)
    amp = per_set(lambda r: (written(r) or 0) / max(1, r["records_written"] * r["record_bytes"]),
                  phase="write", batch=1000, durable=False)
    if any(v for _, p in amp for v in p.values()):
        def times(v):
            return f"{v:.1f}×"

        charts["cost-write-amp"] = lambda t: bars(
            t, "Bytes written to disk per byte of data",
            "During random overwrites, 1,000 keys per commit (lower is better)", amp, times)
        md += table("Write amplification (random overwrites)", amp, times)
    rss = per_set(lambda r: r["peak_rss_bytes"], phase="get", threads=1)
    md += table("Peak memory during single-thread reads (memory-mapped engines count cached file pages)", rss, gb)

    # Stability.
    grow = pick(runs, phase="grow")
    if grow:
        ser = {}
        for e, r in grow.items():
            tl = r["write_timeline"]
            step = 10
            ser[e] = [((i + step) / 60, sum(tl[i:i + step]) / step) for i in range(0, len(tl) - step + 1, step)]
        start = next(iter(grow.values()))["records"]
        charts["stability"] = lambda t: small_multiples(
            t, "Inserts over time",
            f"Continuous inserts, 100 per commit, from {human(start)} records; inserts per second, 10-second average",
            ser, lambda m: f"{m:.0f} min" if m >= 2 else f"{m * 60:.0f} s")
        md += ["## Stability", "", "| engine | records/s overall | slowest 10 s | fastest 10 s |", "|---|---:|---:|---:|"]
        for e in ENGINES:
            if e in ser and ser[e]:
                ys = [y for _, y in ser[e]]
                md.append(f"| {NAMES[e]} | {human(grow[e]['records_written_per_sec'])} | {human(min(ys))} | {human(max(ys))} |")
        md.append("")

    for name, make in charts.items():
        for theme in THEMES:
            with open(os.path.join(out, "charts", f"{name}.{theme}.svg"), "w") as f:
                f.write(make(theme))
    with open(os.path.join(out, "report.md"), "w") as f:
        f.write("\n".join(md) + "\n")
    with open(os.path.join(out, "readme-section.md"), "w") as f:
        f.write(readme_section(charts, a.chart_path))
    print(f"{len(charts)} charts and report.md in {out}")


def picture(name, path, alt):
    return (
        f'<picture><source media="(prefers-color-scheme: dark)" srcset="{path}/{name}.dark.svg">'
        f'<img alt="{esc(alt)}" src="{path}/{name}.light.svg"></picture>'
    )


def readme_section(charts, path):
    """The comparison section for the top-level README."""
    head = "| | " + " | ".join(NAMES[e] for e in FEATURE_ENGINES) + " |"
    rows = [head, "|---|" + "---|" * len(FEATURE_ENGINES)]
    rows += [f"| {label} | " + " | ".join(cells) + " |" for label, cells in FEATURES]
    parts = [
        "### What each store is", "", *rows, "",
        "### Speed of each basic operation", "",
        "Each test does one thing, on a data set that fits in memory and one that does not.", "",
    ]
    groups = [
        ["op-read", "op-range", "op-write-durable", "op-write-batch", "op-load"],
        ["### Many threads", ""],
        ["get-threads-small", "get-threads-large", "mixed-threads-small", "mixed-threads-large"],
        ["### Cost", ""],
        ["cost-disk", "cost-write-amp"],
        ["### Steady over time?", ""],
        ["stability"],
    ]
    for g in groups:
        if g and g[0].startswith("#"):
            parts += g
            continue
        for name in g:
            if name in charts:
                parts += [picture(name, path, name.replace("-", " ")), ""]
    return "\n".join(parts) + "\n"


def table(title, panels, fmt):
    labels = [p for p, _ in panels]
    rows = [f"### {title}", "", "| engine | " + " | ".join(labels) + " |", "|---|" + "---:|" * len(labels)]
    for e in ENGINES:
        if any(e in v for _, v in panels):
            rows.append(f"| {NAMES[e]} | " + " | ".join(fmt(v[e]) if e in v else "" for _, v in panels) + " |")
    return rows + [""]


if __name__ == "__main__":
    main()
