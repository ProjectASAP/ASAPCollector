#!/usr/bin/env python3
"""
Paper §6: Accuracy evaluation — compare sketch query results to ground truth.

Usage:
    python3 evaluation/accuracy_test.py \
        --ground-truth raw_timeseries.csv \
        --sketch-results sketch_output.csv \
        --output evaluation/results/accuracy.csv

Input: raw time series CSV (timestamp, value) and sketch query results.
Output: per-query accuracy metrics (relative error, rank error, etc.)
"""

import argparse
import csv
import json
import math
import sys
from collections import defaultdict

def load_csv(path):
    """Load a CSV as list of dicts."""
    with open(path) as f:
        return list(csv.DictReader(f))

def compute_quantile_exact(values, phi):
    """Exact quantile from sorted values."""
    if not values:
        return 0.0
    sorted_v = sorted(values)
    idx = max(0, min(len(sorted_v) - 1, int(math.ceil(phi * len(sorted_v))) - 1))
    return sorted_v[idx]

def compute_count_exact(values):
    """Exact count."""
    return len(values)

def compute_distinct_exact(values):
    """Exact distinct count."""
    return len(set(values))

def relative_error(exact, approx):
    """Relative error, handling zero."""
    if exact == 0:
        return 0.0 if approx == 0 else float('inf')
    return abs(exact - approx) / abs(exact)

def evaluate_accuracy(ground_truth_path, sketch_results_path, output_path):
    """Compare sketch results against ground truth."""
    gt_rows = load_csv(ground_truth_path)
    sk_rows = load_csv(sketch_results_path)

    # Group ground truth by window
    windows = defaultdict(list)
    for row in gt_rows:
        ts = float(row.get('timestamp', row.get('ts', 0)))
        val = float(row.get('value', row.get('val', 0)))
        # 60-second windows
        window_id = int(ts // 60)
        windows[window_id].append(val)

    results = []
    for sk_row in sk_rows:
        window_id = int(float(sk_row.get('window_id', sk_row.get('timestamp', 0))) // 60)
        query_type = sk_row.get('query_type', 'quantile')
        approx_val = float(sk_row.get('value', sk_row.get('result', 0)))

        gt_values = windows.get(window_id, [])
        if not gt_values:
            continue

        if query_type == 'quantile':
            phi = float(sk_row.get('phi', 0.99))
            exact_val = compute_quantile_exact(gt_values, phi)
        elif query_type == 'count':
            exact_val = compute_count_exact(gt_values)
        elif query_type == 'distinct':
            exact_val = compute_distinct_exact(gt_values)
        else:
            exact_val = sum(gt_values) / len(gt_values)  # avg

        err = relative_error(exact_val, approx_val)
        results.append({
            'window_id': window_id,
            'query_type': query_type,
            'exact': exact_val,
            'approx': approx_val,
            'relative_error': err,
        })

    # Write results
    if output_path:
        with open(output_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['window_id', 'query_type', 'exact', 'approx', 'relative_error'])
            writer.writeheader()
            writer.writerows(results)

    # Summary
    if results:
        errors = [r['relative_error'] for r in results if r['relative_error'] != float('inf')]
        if errors:
            avg_err = sum(errors) / len(errors)
            max_err = max(errors)
            p99_err = sorted(errors)[int(0.99 * len(errors))] if len(errors) > 1 else errors[0]
            print(json.dumps({
                'num_windows': len(results),
                'avg_relative_error': round(avg_err, 6),
                'max_relative_error': round(max_err, 6),
                'p99_relative_error': round(p99_err, 6),
                'within_5_percent': sum(1 for e in errors if e <= 0.05) / len(errors),
            }, indent=2))
        else:
            print('{"error": "no valid comparisons"}')
    else:
        print('{"error": "no results"}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Accuracy evaluation')
    parser.add_argument('--ground-truth', required=True, help='Path to ground truth CSV')
    parser.add_argument('--sketch-results', required=True, help='Path to sketch results CSV')
    parser.add_argument('--output', default=None, help='Output CSV path')
    args = parser.parse_args()
    evaluate_accuracy(args.ground_truth, args.sketch_results, args.output)
