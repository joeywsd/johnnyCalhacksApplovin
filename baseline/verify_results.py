#!/usr/bin/env python3
"""
Verify that optimized results match baseline results.

Usage:
  python verify_results.py --baseline ./out_baseline --optimized ./out
"""
import argparse
import csv
from pathlib import Path
import sys


def normalize_value(val, tolerance=1e-6):
    """Normalize a value for comparison. Round floats to avoid precision issues."""
    try:
        # Try to parse as float
        fval = float(val)
        # Round to 6 decimal places to avoid floating point precision issues
        return str(round(fval, 6))
    except (ValueError, TypeError):
        # Not a float, return as-is
        return val


def read_csv_to_set(csv_path):
    """Read CSV file and return set of tuples for comparison."""
    rows = []
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            # Normalize each value to handle floating point precision
            normalized_row = tuple(normalize_value(val) for val in row)
            rows.append(normalized_row)
    return header, set(rows)


def compare_results(baseline_dir, optimized_dir):
    """Compare all query results between baseline and optimized."""
    baseline_dir = Path(baseline_dir)
    optimized_dir = Path(optimized_dir)

    # Find all query result files
    baseline_files = sorted(baseline_dir.glob("q*.csv"))

    if not baseline_files:
        print(f"[ERROR] No baseline files found in {baseline_dir}")
        return False

    print(f"[VERIFY] Comparing {len(baseline_files)} query results...")

    all_match = True
    for baseline_file in baseline_files:
        query_num = baseline_file.stem  # e.g., "q1"
        optimized_file = optimized_dir / baseline_file.name

        if not optimized_file.exists():
            print(f"[FAIL] {query_num}: Optimized file not found")
            all_match = False
            continue

        # Read both files
        baseline_header, baseline_rows = read_csv_to_set(baseline_file)
        optimized_header, optimized_rows = read_csv_to_set(optimized_file)

        # Compare headers
        if baseline_header != optimized_header:
            print(f"[FAIL] {query_num}: Headers don't match")
            print(f"   Baseline:  {baseline_header}")
            print(f"   Optimized: {optimized_header}")
            all_match = False
            continue

        # Compare row counts
        if len(baseline_rows) != len(optimized_rows):
            print(f"[FAIL] {query_num}: Row count mismatch")
            print(f"   Baseline:  {len(baseline_rows)} rows")
            print(f"   Optimized: {len(optimized_rows)} rows")
            all_match = False
            continue

        # Compare actual data (order-independent)
        if baseline_rows != optimized_rows:
            print(f"[FAIL] {query_num}: Data mismatch")

            # Find differences
            only_in_baseline = baseline_rows - optimized_rows
            only_in_optimized = optimized_rows - baseline_rows

            if only_in_baseline:
                print(f"   Rows only in baseline ({len(only_in_baseline)}): {list(only_in_baseline)[:3]}...")
            if only_in_optimized:
                print(f"   Rows only in optimized ({len(only_in_optimized)}): {list(only_in_optimized)[:3]}...")

            all_match = False
            continue

        print(f"[PASS] {query_num}: {len(baseline_rows)} rows match")

    return all_match


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Verify optimized results match baseline"
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=Path("./out_baseline"),
        help="Baseline output directory (default: ./out_baseline)"
    )
    parser.add_argument(
        "--optimized",
        type=Path,
        default=Path("./out"),
        help="Optimized output directory (default: ./out)"
    )

    args = parser.parse_args()

    if not args.baseline.exists():
        print(f"[ERROR] Baseline directory not found: {args.baseline}")
        print(f"\nPlease run the baseline first:")
        print(f"  python main.py --data-dir ../data-lite/data-lite/data-lite --out-dir {args.baseline}")
        sys.exit(1)

    if not args.optimized.exists():
        print(f"[ERROR] Optimized directory not found: {args.optimized}")
        print(f"\nPlease run the optimized solution first:")
        print(f"  python run.py --out-dir {args.optimized}")
        sys.exit(1)

    success = compare_results(args.baseline, args.optimized)

    if success:
        print(f"\n[SUCCESS] All results match! Optimized solution is correct.")
        sys.exit(0)
    else:
        print(f"\n[FAILURE] Some results don't match. Please investigate.")
        sys.exit(1)
