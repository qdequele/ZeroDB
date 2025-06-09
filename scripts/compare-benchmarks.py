#!/usr/bin/env python3
"""
Compare benchmark results between the base branch and current PR.
Generates a markdown comment for the PR with performance comparisons.
"""

import json
import os
import sys
import subprocess
from typing import Dict, List, Tuple

def parse_criterion_output(json_file: str) -> Dict[str, float]:
    """Parse criterion JSON output and extract benchmark results."""
    results = {}
    try:
        with open(json_file, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if data.get('reason') == 'benchmark-complete':
                        bench_id = data['id']
                        # Extract median time in nanoseconds
                        median = data['median']['point_estimate']
                        results[bench_id] = median
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        print(f"Warning: {json_file} not found")
    return results

def format_time(nanos: float) -> str:
    """Format nanoseconds into human-readable time."""
    if nanos < 1000:
        return f"{nanos:.1f}ns"
    elif nanos < 1_000_000:
        return f"{nanos/1000:.1f}µs"
    elif nanos < 1_000_000_000:
        return f"{nanos/1_000_000:.1f}ms"
    else:
        return f"{nanos/1_000_000_000:.2f}s"

def calculate_change(old: float, new: float) -> Tuple[float, str]:
    """Calculate percentage change and format it."""
    if old == 0:
        return 0, "N/A"
    
    change = ((new - old) / old) * 100
    if change > 0:
        emoji = "🔴" if change > 5 else "🟡" if change > 2 else ""
        return change, f"+{change:.1f}% {emoji}"
    else:
        emoji = "🟢" if change < -5 else ""
        return change, f"{change:.1f}% {emoji}"

def generate_markdown_report(base_results: Dict[str, float], 
                           pr_results: Dict[str, float]) -> str:
    """Generate a markdown report comparing benchmark results."""
    report = ["## Benchmark Results\n"]
    report.append("| Benchmark | Base | PR | Change |")
    report.append("|-----------|------|----|---------:|")
    
    all_benchmarks = sorted(set(base_results.keys()) | set(pr_results.keys()))
    
    regressions = []
    improvements = []
    
    for bench in all_benchmarks:
        base_time = base_results.get(bench, 0)
        pr_time = pr_results.get(bench, 0)
        
        if base_time and pr_time:
            change_pct, change_str = calculate_change(base_time, pr_time)
            base_str = format_time(base_time)
            pr_str = format_time(pr_time)
            
            report.append(f"| `{bench}` | {base_str} | {pr_str} | {change_str} |")
            
            if change_pct > 5:
                regressions.append((bench, change_pct))
            elif change_pct < -5:
                improvements.append((bench, change_pct))
        elif pr_time:
            report.append(f"| `{bench}` | - | {format_time(pr_time)} | New |")
        else:
            report.append(f"| `{bench}` | {format_time(base_time)} | - | Removed |")
    
    # Add summary
    report.append("\n### Summary\n")
    
    if regressions:
        report.append("#### ⚠️ Performance Regressions")
        for bench, change in sorted(regressions, key=lambda x: x[1], reverse=True):
            report.append(f"- `{bench}`: {change:.1f}% slower")
    
    if improvements:
        report.append("\n#### ✅ Performance Improvements")
        for bench, change in sorted(improvements, key=lambda x: x[1]):
            report.append(f"- `{bench}`: {-change:.1f}% faster")
    
    if not regressions and not improvements:
        report.append("No significant performance changes detected (threshold: ±5%)")
    
    return "\n".join(report)

def main():
    # Check if we're in a PR context
    pr_number = os.environ.get('GITHUB_EVENT_NUMBER')
    if not pr_number:
        print("Not in a PR context, skipping comparison")
        return
    
    # Get base branch results (would be fetched from gh-pages or artifact storage)
    base_results_file = "cache/criterion-results.json"
    pr_results_file = "criterion-results/output.json"
    
    base_results = parse_criterion_output(base_results_file)
    pr_results = parse_criterion_output(pr_results_file)
    
    if not base_results:
        print("No base results found, this might be the first run")
        # Still create a report showing current results
        report = "## Benchmark Results (First Run)\n\n"
        report += "| Benchmark | Time |\n"
        report += "|-----------|------|\n"
        for bench, time in sorted(pr_results.items()):
            report += f"| `{bench}` | {format_time(time)} |\n"
    else:
        report = generate_markdown_report(base_results, pr_results)
    
    # Write report to file for GitHub Action to pick up
    with open("benchmark-report.md", "w") as f:
        f.write(report)
    
    print("Benchmark comparison complete")
    
    # Exit with error if significant regressions found
    if base_results:
        for bench in pr_results:
            if bench in base_results:
                change = ((pr_results[bench] - base_results[bench]) / base_results[bench]) * 100
                if change > 10:  # 10% regression threshold for CI failure
                    print(f"ERROR: Significant regression in {bench}: {change:.1f}%")
                    sys.exit(1)

if __name__ == "__main__":
    main()