#!/usr/bin/env python3
"""
Generate comprehensive benchmark reports from criterion output.

Usage:
    python3 scripts/benchmark_report.py [--compare-dbs] [--zerodb-only]
"""

import json
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import subprocess

class BenchmarkReport:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.criterion_dir = base_dir / "target" / "criterion"
        
    def parse_criterion_data(self, bench_name: str) -> Dict:
        """Parse criterion benchmark results."""
        results = {}
        bench_dir = self.criterion_dir / bench_name
        
        if not bench_dir.exists():
            return results
            
        for test_dir in bench_dir.iterdir():
            if test_dir.is_dir() and not test_dir.name.startswith('.'):
                estimates_file = test_dir / "base" / "estimates.json"
                if estimates_file.exists():
                    with open(estimates_file) as f:
                        data = json.load(f)
                        results[test_dir.name] = {
                            'mean': data['mean']['point_estimate'] / 1e9,  # Convert to seconds
                            'std_dev': data['std_dev']['point_estimate'] / 1e9,
                        }
        
        return results

    def generate_database_comparison_report(self):
        """Generate report comparing different databases."""
        print("# Database Comparison Report")
        print(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # Parse all benchmark results
        results = self.parse_criterion_data("database_comparison")
        
        if not results:
            print("No benchmark results found. Run benchmarks first:")
            print("  cargo bench --bench database_comparison")
            return
        
        # Organize results by operation type
        operations = {}
        for test_name, data in results.items():
            parts = test_name.split('/')
            if len(parts) >= 2:
                db_name = parts[0]
                op_type = parts[1]
                
                if op_type not in operations:
                    operations[op_type] = {}
                operations[op_type][db_name] = data
        
        # Generate comparison tables
        print("## Sequential Write Performance\n")
        self._print_comparison_table(operations.get('sequential_writes', {}))
        
        print("\n## Random Write Performance\n")
        self._print_comparison_table(operations.get('random_writes', {}))
        
        print("\n## Read Performance\n")
        self._print_comparison_table(operations.get('random_reads', {}))
        
        print("\n## Concurrent Read Performance\n")
        self._print_comparison_table(operations.get('concurrent_reads', {}))
        
        print("\n## Full Scan Performance\n")
        self._print_comparison_table(operations.get('full_scan', {}))
        
        print("\n## Mixed Workload Performance\n")
        self._print_comparison_table(operations.get('mixed_workload', {}))
        
        # Generate summary
        self._generate_summary(operations)
        
    def generate_zerodb_performance_report(self):
        """Generate detailed ZeroDB performance report."""
        print("# ZeroDB Performance Report")
        print(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # Get version info
        try:
            git_hash = subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode().strip()[:8]
            print(f"Git commit: {git_hash}")
        except:
            pass
            
        # Parse benchmark results
        results = self.parse_criterion_data("zerodb_performance")
        
        if not results:
            print("No benchmark results found. Run benchmarks first:")
            print("  cargo bench --bench zerodb_performance")
            return
        
        # Group by categories
        categories = {
            'btree_operations': [],
            'page_allocation': [],
            'overflow_handling': [],
            'cursor_operations': [],
            'transaction_overhead': [],
            'durability_modes': [],
            'concurrent_operations': [],
            'memory_efficiency': [],
            'special_patterns': []
        }
        
        for test_name, data in results.items():
            for category in categories:
                if test_name.startswith(category):
                    categories[category].append((test_name, data))
        
        # Generate detailed reports for each category
        for category, tests in categories.items():
            if tests:
                print(f"\n## {category.replace('_', ' ').title()}\n")
                for test_name, data in sorted(tests):
                    ops_per_sec = 1.0 / data['mean'] if data['mean'] > 0 else 0
                    print(f"- **{test_name}**: {self._format_throughput(ops_per_sec)} "
                          f"(±{data['std_dev']*1000:.2f}ms)")
        
        # Performance trends
        self._generate_performance_trends(results)
        
        # Regression detection
        self._check_regressions(results)
        
    def _print_comparison_table(self, data: Dict):
        """Print a comparison table for databases."""
        if not data:
            print("No data available")
            return
            
        databases = sorted(set(db for test_data in data.values() for db in test_data))
        
        print("| Test | " + " | ".join(databases) + " |")
        print("|------|" + "|".join(["------" for _ in databases]) + "|")
        
        for test in sorted(data.keys()):
            row = [test]
            for db in databases:
                if db in data[test]:
                    ops_per_sec = 1.0 / data[test][db]['mean'] if data[test][db]['mean'] > 0 else 0
                    row.append(self._format_throughput(ops_per_sec))
                else:
                    row.append("N/A")
            print("| " + " | ".join(row) + " |")
    
    def _format_throughput(self, ops_per_sec: float) -> str:
        """Format throughput numbers."""
        if ops_per_sec > 1_000_000:
            return f"{ops_per_sec/1_000_000:.2f}M ops/s"
        elif ops_per_sec > 1_000:
            return f"{ops_per_sec/1_000:.2f}K ops/s"
        else:
            return f"{ops_per_sec:.2f} ops/s"
    
    def _generate_summary(self, operations: Dict):
        """Generate overall summary and recommendations."""
        print("\n## Summary\n")
        
        # Calculate relative performance
        print("### Relative Performance (vs LMDB)\n")
        
        for op_type, data in operations.items():
            if 'lmdb' in data and 'zerodb' in data:
                lmdb_perf = 1.0 / data['lmdb']['mean']
                zerodb_perf = 1.0 / data['zerodb']['mean']
                ratio = zerodb_perf / lmdb_perf
                
                print(f"- **{op_type}**: ZeroDB is {ratio:.2f}x "
                      f"{'faster' if ratio > 1 else 'slower'} than LMDB")
        
        print("\n### Recommendations\n")
        print("- **Use ZeroDB when**: Sequential write performance is critical")
        print("- **Use LMDB when**: Random write performance is needed")
        print("- **Use RocksDB when**: Compression is required")
        print("- **Use redb when**: Pure Rust is mandatory")
        
    def _generate_performance_trends(self, results: Dict):
        """Analyze performance trends."""
        print("\n## Performance Characteristics\n")
        
        # Analyze value size impact
        overflow_tests = [(k, v) for k, v in results.items() if 'overflow' in k]
        if overflow_tests:
            print("### Value Size Impact")
            for test, data in sorted(overflow_tests):
                print(f"- {test}: {data['mean']*1000:.2f}ms per operation")
        
        # Analyze concurrency scalability
        concurrent_tests = [(k, v) for k, v in results.items() if 'concurrent' in k]
        if concurrent_tests:
            print("\n### Concurrency Scalability")
            for test, data in sorted(concurrent_tests):
                print(f"- {test}: {data['mean']*1000:.2f}ms")
    
    def _check_regressions(self, results: Dict):
        """Check for performance regressions."""
        print("\n## Regression Check\n")
        
        # Define thresholds
        thresholds = {
            'sequential_insert': 0.001,  # 1ms per op
            'random_reads': 0.002,       # 2ms per op
            'forward_iteration': 0.0001, # 0.1ms per item
        }
        
        regressions = []
        for test_name, data in results.items():
            for threshold_test, threshold in thresholds.items():
                if threshold_test in test_name and data['mean'] > threshold:
                    regressions.append(f"- **{test_name}**: {data['mean']*1000:.2f}ms "
                                     f"(threshold: {threshold*1000:.2f}ms)")
        
        if regressions:
            print("⚠️ **Performance Regressions Detected:**")
            for r in regressions:
                print(r)
        else:
            print("✅ No performance regressions detected")

def main():
    parser = argparse.ArgumentParser(description='Generate benchmark reports')
    parser.add_argument('--compare-dbs', action='store_true', 
                        help='Generate database comparison report')
    parser.add_argument('--zerodb-only', action='store_true',
                        help='Generate ZeroDB performance report')
    
    args = parser.parse_args()
    
    # Find project root
    base_dir = Path(__file__).parent.parent
    report = BenchmarkReport(base_dir)
    
    if args.compare_dbs:
        report.generate_database_comparison_report()
    elif args.zerodb_only:
        report.generate_zerodb_performance_report()
    else:
        # Generate both reports
        report.generate_database_comparison_report()
        print("\n" + "="*80 + "\n")
        report.generate_zerodb_performance_report()

if __name__ == "__main__":
    main()