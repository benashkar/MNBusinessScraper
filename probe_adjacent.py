#!/usr/bin/env python3
"""Probe numbers adjacent to known 2022 LLC to find pattern."""

import asyncio
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.path.insert(0, '.')

from mn_scraper import MNBusinessScraper


async def probe_adjacent():
    """Probe numbers around known 2022 LLC."""
    scraper = MNBusinessScraper(headless=True)

    # Known working: 1349132200021 (2022 LLC)
    # Let's probe nearby numbers

    base = 1349132200021
    test_numbers = []

    # Try +/- 1000 around the base
    for offset in range(-50, 51, 5):
        test_numbers.append(base + offset)

    # Also try incrementing/decrementing different digit positions
    # Maybe the last digits are the sequence
    test_numbers.extend([
        1349132200001,
        1349132200010,
        1349132200020,
        1349132200021,  # known
        1349132200022,
        1349132200030,
        1349132200050,
        1349132200100,
        1349132200500,
        1349132201000,
    ])

    # Try changing the "22" portion to find other years
    # If 22 is 2022, then 19, 20, 21, 23, 24, 25 should give other years
    # But the format might not be that simple

    # Try format: 13491322 + 5-digit sequence
    for seq in [1, 10, 100, 1000, 10000]:
        test_numbers.append(13491322 * 100000 + seq)

    # Try varying the digit before "22"
    for prefix in range(0, 10):
        test_numbers.append(int(f"134913{prefix}200021"))

    # Remove duplicates and sort
    test_numbers = sorted(set(test_numbers))

    try:
        await scraper.initialize()

        print("Probing adjacent file numbers to understand pattern...")
        print("=" * 80)

        results = []

        for file_num in test_numbers:
            data = await scraper.scrape_business(file_num)

            if data:
                filing_date = data.get('filing_date', 'Unknown')
                business_type = data.get('business_type', 'Unknown')
                business_name = data.get('business_name', 'Unknown')

                print(f"[FOUND] #{file_num}: {business_name}")
                print(f"        Type: {business_type}, Filed: {filing_date}")

                results.append({
                    'file_number': file_num,
                    'name': business_name,
                    'type': business_type,
                    'filing_date': filing_date
                })
            else:
                print(f"[MISS]  #{file_num}")

            await asyncio.sleep(1)

        print("\n" + "=" * 80)
        print("ANALYSIS:")
        print("=" * 80)

        if results:
            # Analyze the file numbers that worked
            for r in results:
                fn = str(r['file_number'])
                year = "?"
                if r['filing_date'] and '/' in r['filing_date']:
                    parts = r['filing_date'].split('/')
                    if len(parts) == 3:
                        year = parts[2]
                print(f"File #{fn} (len={len(fn)}): Year={year}, Type={r['type']}")

        return results

    finally:
        await scraper.close()


if __name__ == '__main__':
    asyncio.run(probe_adjacent())
