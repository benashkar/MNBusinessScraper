#!/usr/bin/env python3
"""
Merge all worker output files into a single CSV.
"""

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8', errors='replace')


def merge_worker_outputs(output_dir='output', output_file='businesses_merged.csv'):
    """Merge all worker CSV files into one."""

    output_path = Path(output_dir)
    worker_files = sorted(output_path.glob('businesses_worker_*.csv'))

    if not worker_files:
        print("No worker output files found.")
        return

    print(f"Found {len(worker_files)} worker files to merge:")
    for f in worker_files:
        print(f"  - {f.name}")

    # Read and combine all files
    dfs = []
    for f in worker_files:
        try:
            df = pd.read_csv(f)
            print(f"  {f.name}: {len(df):,} records")
            dfs.append(df)
        except Exception as e:
            print(f"  {f.name}: ERROR - {e}")

    if not dfs:
        print("No data to merge.")
        return

    # Combine all dataframes
    combined = pd.concat(dfs, ignore_index=True)

    # Remove duplicates based on file_number
    original_count = len(combined)
    combined = combined.drop_duplicates(subset=['file_number'], keep='first')
    deduped_count = len(combined)

    if original_count != deduped_count:
        print(f"\nRemoved {original_count - deduped_count:,} duplicate records")

    # Sort by file number
    combined = combined.sort_values('file_number')

    # Save merged file
    merged_path = output_path / output_file
    combined.to_csv(merged_path, index=False)

    print(f"\n{'=' * 60}")
    print(f"MERGED OUTPUT")
    print(f"{'=' * 60}")
    print(f"Total records: {len(combined):,}")
    print(f"Output file: {merged_path}")
    print(f"File number range: {combined['file_number'].min():,} - {combined['file_number'].max():,}")

    # Also include the original sequential scraper output if it exists
    original_file = output_path / 'businesses.csv'
    if original_file.exists():
        print(f"\nNote: Original scraper output exists at {original_file}")
        print("To merge with original, run: python merge_results.py --include-original")


def merge_all(output_dir='output'):
    """Merge worker outputs AND original scraper output."""

    output_path = Path(output_dir)

    # Get all CSV files
    all_files = list(output_path.glob('businesses*.csv'))
    # Exclude the merged file itself
    all_files = [f for f in all_files if 'merged' not in f.name]

    print(f"Found {len(all_files)} files to merge:")
    for f in all_files:
        print(f"  - {f.name}")

    dfs = []
    for f in all_files:
        try:
            df = pd.read_csv(f)
            print(f"  {f.name}: {len(df):,} records")
            dfs.append(df)
        except Exception as e:
            print(f"  {f.name}: ERROR - {e}")

    if not dfs:
        print("No data to merge.")
        return

    combined = pd.concat(dfs, ignore_index=True)
    original_count = len(combined)
    combined = combined.drop_duplicates(subset=['file_number'], keep='first')
    combined = combined.sort_values('file_number')

    merged_path = output_path / 'businesses_all.csv'
    combined.to_csv(merged_path, index=False)

    print(f"\n{'=' * 60}")
    print(f"COMPLETE MERGE")
    print(f"{'=' * 60}")
    print(f"Total unique records: {len(combined):,}")
    print(f"Duplicates removed: {original_count - len(combined):,}")
    print(f"Output file: {merged_path}")


if __name__ == '__main__':
    if '--include-original' in sys.argv or '--all' in sys.argv:
        merge_all()
    else:
        merge_worker_outputs()
