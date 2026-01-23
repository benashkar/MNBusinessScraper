#!/usr/bin/env python3
"""
Filter scraped businesses to show only those filed since 2019.
Also provides statistics about the data.
"""

import sys
import pandas as pd
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8', errors='replace')


def filter_recent_businesses(csv_path='output/businesses.csv', year_threshold=2019):
    """Filter businesses by filing year and generate reports."""

    print(f"Loading data from {csv_path}...")
    df = pd.read_csv(csv_path)
    print(f"Total records: {len(df)}")

    # Parse filing year
    def get_year(date_str):
        if pd.isna(date_str):
            return None
        try:
            parts = str(date_str).split('/')
            if len(parts) == 3:
                year = int(parts[2])
                # Handle 2-digit years
                if year < 100:
                    year = 1900 + year if year > 50 else 2000 + year
                return year
        except:
            pass
        return None

    df['filing_year'] = df['filing_date'].apply(get_year)

    # Statistics
    print("\n" + "=" * 60)
    print("FILING YEAR DISTRIBUTION")
    print("=" * 60)

    year_counts = df['filing_year'].value_counts().sort_index()
    for year, count in year_counts.items():
        if year:
            marker = " <-- RECENT" if year >= year_threshold else ""
            print(f"  {int(year)}: {count:,} businesses{marker}")

    # Filter recent
    recent = df[df['filing_year'] >= year_threshold].copy()
    print(f"\n{'=' * 60}")
    print(f"BUSINESSES FILED SINCE {year_threshold}")
    print("=" * 60)
    print(f"Found: {len(recent):,} businesses")

    if len(recent) > 0:
        # Save to separate CSV
        output_path = f'output/businesses_since_{year_threshold}.csv'
        recent.to_csv(output_path, index=False)
        print(f"Saved to: {output_path}")

        # Show sample
        print("\nSample recent businesses:")
        cols = ['file_number', 'business_name', 'filing_date', 'business_type', 'status']
        print(recent[cols].head(20).to_string())

        # Business type breakdown for recent
        print(f"\nBusiness types (since {year_threshold}):")
        type_counts = recent['business_type'].value_counts()
        for btype, count in type_counts.items():
            print(f"  {btype}: {count}")

        # Status breakdown
        print(f"\nStatus (since {year_threshold}):")
        status_counts = recent['status'].value_counts()
        for status, count in status_counts.items():
            print(f"  {status}: {count}")

    else:
        print("\nNo businesses found from this time period in the scraped data yet.")
        print("The sequential file numbers (1-300,000) contain mostly older filings.")
        print("\nNewer businesses (2019+) appear to use non-sequential 12-13 digit file numbers")
        print("that cannot be efficiently iterated through.")

    # Summary
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print("=" * 60)

    oldest = df['filing_year'].min()
    newest = df['filing_year'].max()
    print(f"Date range in data: {int(oldest) if oldest else 'N/A'} - {int(newest) if newest else 'N/A'}")
    print(f"Total scraped: {len(df):,}")
    print(f"Since {year_threshold}: {len(recent):,}")
    print(f"Percentage recent: {100*len(recent)/len(df):.1f}%")

    return recent


if __name__ == '__main__':
    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2019
    filter_recent_businesses(year_threshold=year)
