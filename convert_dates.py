#!/usr/bin/env python3
"""
Convert existing CSV files to use YYYY-MM-DD date format.
"""

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8', errors='replace')


def convert_date(date_str):
    """Convert MM/DD/YYYY to YYYY-MM-DD format."""
    if pd.isna(date_str) or not str(date_str).strip():
        return ''

    date_str = str(date_str).strip()

    # Try MM/DD/YYYY format
    try:
        dt = datetime.strptime(date_str, '%m/%d/%Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        pass

    # Try M/D/YYYY format
    try:
        dt = datetime.strptime(date_str, '%m/%d/%Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        pass

    # If already in ISO format (with or without time), extract date part
    if 'T' in date_str:
        return date_str.split('T')[0]

    # Check if already YYYY-MM-DD
    try:
        dt = datetime.strptime(date_str[:10], '%Y-%m-%d')
        return date_str[:10]
    except ValueError:
        pass

    # Return original if can't parse
    return date_str


def convert_csv_dates(input_path, output_path=None, show_sample=True):
    """Convert dates in a CSV file."""

    print(f"Reading: {input_path}")
    df = pd.read_csv(input_path)
    original_count = len(df)

    # Date columns to convert
    date_columns = ['filing_date', 'renewal_due_date', 'scraped_at']

    if show_sample:
        print("\n" + "=" * 70)
        print("BEFORE CONVERSION (first 5 rows):")
        print("=" * 70)
        cols_present = [c for c in date_columns if c in df.columns]
        print(df[['file_number'] + cols_present].head().to_string())

    # Convert each date column
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].apply(convert_date)
            print(f"Converted column: {col}")

    if show_sample:
        print("\n" + "=" * 70)
        print("AFTER CONVERSION (first 5 rows):")
        print("=" * 70)
        print(df[['file_number'] + cols_present].head().to_string())

    # Save
    if output_path is None:
        output_path = input_path  # Overwrite

    df.to_csv(output_path, index=False)
    print(f"\nSaved {original_count} records to: {output_path}")

    return df


def convert_all_outputs(output_dir='output'):
    """Convert all CSV files in the output directory."""

    output_path = Path(output_dir)
    csv_files = list(output_path.glob('businesses*.csv'))

    print(f"Found {len(csv_files)} CSV files to convert")
    print("=" * 70)

    for csv_file in csv_files:
        print(f"\nProcessing: {csv_file.name}")
        convert_csv_dates(csv_file, show_sample=False)

    print("\n" + "=" * 70)
    print("ALL FILES CONVERTED")
    print("=" * 70)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--all':
        convert_all_outputs()
    elif len(sys.argv) > 1:
        convert_csv_dates(sys.argv[1])
    else:
        # Show sample conversion on main file
        convert_csv_dates('output/businesses.csv', show_sample=True)
