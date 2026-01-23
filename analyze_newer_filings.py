#!/usr/bin/env python3
"""Analyze file number patterns to find 2019+ business filings."""

import asyncio
import sys
import re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.path.insert(0, '.')

from mn_scraper import MNBusinessScraper


async def probe_file_numbers():
    """Probe various file number patterns to find 2019+ businesses."""
    scraper = MNBusinessScraper(headless=True)

    # Based on known patterns:
    # 768883700027 = 2014 Business Corp (12 digits)
    # 1349132200021 = 2022 LLC (13 digits)
    #
    # Hypothesis: The format might be PREFIX + YYMMDD + SEQUENCE
    # For 1349132200021:
    #   - 134913 might be entity type prefix
    #   - 22 = 2022
    #   - 00021 = sequence
    #
    # Let's probe different patterns for years 2019-2026

    # Test patterns - trying to find the year encoding
    test_numbers = [
        # Try variations around the known 2022 LLC pattern
        # 1349132200021 = 2022 LLC
        # Try 2019 (19), 2020 (20), 2021 (21), 2023 (23), 2024 (24), 2025 (25)

        # LLC pattern variations (prefix 134913)
        1349131900001,  # 2019?
        1349132000001,  # 2020?
        1349132100001,  # 2021?
        1349132200001,  # 2022? (close to known)
        1349132300001,  # 2023?
        1349132400001,  # 2024?
        1349132500001,  # 2025?

        # Business Corp pattern (prefix 768883)
        768883190001,   # 2019?
        768883200001,   # 2020?
        768883210001,   # 2021?
        768883220001,   # 2022?
        768883230001,   # 2023?
        768883240001,   # 2024?
        768883250001,   # 2025?

        # Try shorter prefixes
        # Maybe format is TTTTYYNNNNNN (type + year + sequence)
        100019000001,   # Type 1000 + 19 + sequence
        100020000001,   # Type 1000 + 20 + sequence
        100021000001,
        100022000001,
        100023000001,
        100024000001,
        100025000001,

        # Also try the known working 2022 number variations
        1349132200021,  # Known 2022 LLC
        1349132200001,  # First in 2022?
        1349132200100,  # 100th in 2022?

        768883700027,   # Known 2014 Corp
    ]

    try:
        await scraper.initialize()

        print("Probing file number patterns to find 2019+ businesses...")
        print("=" * 80)

        results = []

        for file_num in test_numbers:
            print(f"\nTesting: {file_num}")
            data = await scraper.scrape_business(file_num)

            if data:
                filing_date = data.get('filing_date', 'Unknown')
                business_type = data.get('business_type', 'Unknown')
                business_name = data.get('business_name', 'Unknown')

                print(f"  [FOUND] {business_name}")
                print(f"          Type: {business_type}")
                print(f"          Filed: {filing_date}")

                results.append({
                    'file_number': file_num,
                    'name': business_name,
                    'type': business_type,
                    'filing_date': filing_date
                })
            else:
                print(f"  [NOT FOUND]")

            await asyncio.sleep(1.5)

        print("\n" + "=" * 80)
        print("SUMMARY OF FOUND BUSINESSES:")
        print("=" * 80)

        for r in results:
            year = "Unknown"
            if r['filing_date'] and '/' in r['filing_date']:
                parts = r['filing_date'].split('/')
                if len(parts) == 3:
                    year = parts[2]

            print(f"File #{r['file_number']}: {r['name']}")
            print(f"    Type: {r['type']}, Year: {year}")
            print()

        return results

    finally:
        await scraper.close()


if __name__ == '__main__':
    asyncio.run(probe_file_numbers())
