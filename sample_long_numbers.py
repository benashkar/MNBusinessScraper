#!/usr/bin/env python3
"""Sample random long file numbers to find 2019+ businesses."""

import asyncio
import random
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.path.insert(0, '.')

from mn_scraper import MNBusinessScraper


async def sample_long_numbers(num_samples=100):
    """Randomly sample long file numbers to find businesses and their filing dates."""
    scraper = MNBusinessScraper(headless=True)

    # Known valid long numbers:
    # 768883700027 (12 digits) - 2014 Corp
    # 1349132200021 (13 digits) - 2022 LLC
    #
    # Let's sample various ranges of 12-13 digit numbers

    ranges = [
        # 12-digit ranges
        (100000000000, 999999999999, "12-digit"),
        # 13-digit ranges
        (1000000000000, 9999999999999, "13-digit"),
        # Specific ranges around known numbers
        (760000000000, 780000000000, "12-digit around 768B"),
        (1340000000000, 1360000000000, "13-digit around 1349T"),
    ]

    all_samples = []
    for rmin, rmax, desc in ranges:
        samples = [random.randint(rmin, rmax) for _ in range(num_samples // len(ranges))]
        all_samples.extend(samples)
        print(f"Generated {len(samples)} samples for {desc}")

    random.shuffle(all_samples)

    try:
        await scraper.initialize()

        print(f"\nSampling {len(all_samples)} random long file numbers...")
        print("=" * 80)

        results = []
        found_count = 0
        miss_count = 0

        for i, file_num in enumerate(all_samples):
            data = await scraper.scrape_business(file_num)

            if data:
                filing_date = data.get('filing_date', 'Unknown')
                business_type = data.get('business_type', 'Unknown')
                business_name = data.get('business_name', 'Unknown')[:50]

                year = "?"
                if filing_date and '/' in filing_date:
                    parts = filing_date.split('/')
                    if len(parts) == 3:
                        year = parts[2]

                print(f"[{i+1}/{len(all_samples)}] FOUND #{file_num}: {business_name} ({year})")

                results.append({
                    'file_number': file_num,
                    'name': business_name,
                    'type': business_type,
                    'filing_date': filing_date,
                    'year': year
                })
                found_count += 1
            else:
                miss_count += 1
                if miss_count % 10 == 0:
                    print(f"[{i+1}/{len(all_samples)}] {miss_count} misses so far...")

            await asyncio.sleep(0.5)  # Faster since most will miss

        print("\n" + "=" * 80)
        print(f"RESULTS: Found {found_count} out of {len(all_samples)} samples ({100*found_count/len(all_samples):.1f}%)")
        print("=" * 80)

        if results:
            # Group by year
            by_year = {}
            for r in results:
                y = r['year']
                if y not in by_year:
                    by_year[y] = []
                by_year[y].append(r)

            print("\nBusinesses found by year:")
            for year in sorted(by_year.keys()):
                print(f"  {year}: {len(by_year[year])} businesses")
                for r in by_year[year][:3]:  # Show first 3
                    print(f"    - #{r['file_number']}: {r['name']}")

            # Show file number patterns
            print("\nFile number patterns observed:")
            for r in results:
                fn = str(r['file_number'])
                print(f"  {fn} (len={len(fn)}) -> {r['year']} {r['type'][:30]}")

        return results

    finally:
        await scraper.close()


if __name__ == '__main__':
    samples = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    asyncio.run(sample_long_numbers(samples))
