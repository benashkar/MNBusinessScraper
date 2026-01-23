#!/usr/bin/env python3
"""
Discover all business types by sampling file numbers across different ranges.
Samples numbers of each digit length to find variety of business types.
"""

import asyncio
import random
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.path.insert(0, '.')

from collections import defaultdict
from mn_scraper import MNBusinessScraper


async def discover_business_types(samples_per_range: int = 5):
    """
    Sample file numbers across different digit ranges to discover business types.

    Args:
        samples_per_range: Number of random samples to try per digit range
    """
    scraper = MNBusinessScraper(headless=True)

    # Track discovered business types
    business_types = defaultdict(list)  # type -> list of (file_number, name)
    all_fields_seen = set()
    failed_numbers = []

    # Define ranges to sample (by number of digits)
    # Max is 13 digits based on 1349132200021
    ranges = [
        (1, 9),                          # 1 digit
        (10, 99),                        # 2 digits
        (100, 999),                      # 3 digits
        (1000, 9999),                    # 4 digits
        (10000, 99999),                  # 5 digits
        (100000, 999999),                # 6 digits
        (1000000, 9999999),              # 7 digits
        (10000000, 99999999),            # 8 digits
        (100000000, 999999999),          # 9 digits
        (1000000000, 9999999999),        # 10 digits
        (10000000000, 99999999999),      # 11 digits
        (100000000000, 999999999999),    # 12 digits
        (1000000000000, 9349132200021),  # 13 digits (up to known max)
    ]

    try:
        await scraper.initialize()

        total_found = 0
        total_tried = 0

        for min_val, max_val in ranges:
            digits = len(str(min_val))
            print(f"\n{'='*60}")
            print(f"Sampling {digits}-digit numbers ({min_val:,} - {max_val:,})")
            print('='*60)

            # Generate random samples for this range
            samples = set()
            attempts = 0
            while len(samples) < samples_per_range and attempts < samples_per_range * 10:
                samples.add(random.randint(min_val, max_val))
                attempts += 1

            for file_num in sorted(samples):
                total_tried += 1
                print(f"\nTrying {file_num}...", end=" ")

                data = await scraper.scrape_business(file_num)

                if data and data.get('business_name'):
                    total_found += 1
                    biz_type = data.get('business_type', 'Unknown')
                    biz_name = data.get('business_name', 'Unknown')

                    print(f"FOUND: {biz_type}")
                    print(f"  Name: {biz_name}")

                    # Track this business type
                    business_types[biz_type].append((file_num, biz_name))

                    # Track all non-empty fields
                    for key, val in data.items():
                        if val and key not in ['scraped_at', 'file_number']:
                            all_fields_seen.add(key)
                else:
                    print("not found")
                    failed_numbers.append(file_num)

                await asyncio.sleep(1.5)  # Rate limiting

        # Print summary
        print(f"\n\n{'='*60}")
        print("DISCOVERY SUMMARY")
        print('='*60)
        print(f"\nTotal tried: {total_tried}")
        print(f"Total found: {total_found}")
        print(f"Success rate: {total_found/total_tried*100:.1f}%")

        print(f"\n--- BUSINESS TYPES FOUND ({len(business_types)}) ---")
        for biz_type in sorted(business_types.keys()):
            examples = business_types[biz_type]
            print(f"\n  {biz_type}:")
            for file_num, name in examples[:3]:  # Show up to 3 examples
                print(f"    - {file_num}: {name[:50]}")
            if len(examples) > 3:
                print(f"    ... and {len(examples) - 3} more")

        print(f"\n--- ALL FIELDS SEEN ({len(all_fields_seen)}) ---")
        for field in sorted(all_fields_seen):
            print(f"  - {field}")

        return business_types

    finally:
        await scraper.close()


if __name__ == '__main__':
    # Sample 5 numbers per digit range by default
    # Increase for more thorough discovery
    samples = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    print(f"Sampling {samples} numbers per digit range...")
    print("This may take a while...\n")

    asyncio.run(discover_business_types(samples_per_range=samples))
