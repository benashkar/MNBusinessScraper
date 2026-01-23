#!/usr/bin/env python3
"""
Focused discovery - sample more heavily in ranges where businesses exist.
"""

import asyncio
import random
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.path.insert(0, '.')

from collections import defaultdict
from mn_scraper import MNBusinessScraper


async def focused_discovery(total_samples: int = 50):
    """Sample more in productive ranges to discover more business types."""
    scraper = MNBusinessScraper(headless=True)

    business_types = defaultdict(list)

    # Focus on ranges that had results (1-6 digits)
    # Plus try some specific patterns for longer numbers
    samples = set()

    # Heavy sampling in low ranges
    for _ in range(total_samples // 2):
        samples.add(random.randint(1, 999999))  # 1-6 digits

    # Medium sampling in mid ranges
    for _ in range(total_samples // 4):
        samples.add(random.randint(1, 99999))  # 1-5 digits

    # Try some specific known patterns for corps/LLCs
    # These seem to follow YYMMDD + sequence pattern
    known_patterns = [
        # Try various year prefixes for 12-digit numbers
        random.randint(100000000000, 200000000000),
        random.randint(700000000000, 800000000000),
        random.randint(1300000000000, 1400000000000),
        # Sequential in known working ranges
        768883700000 + random.randint(1, 100),
        1349132200000 + random.randint(1, 100),
    ]
    samples.update(known_patterns)

    try:
        await scraper.initialize()

        for i, file_num in enumerate(sorted(samples)):
            print(f"[{i+1}/{len(samples)}] Trying {file_num}...", end=" ")

            data = await scraper.scrape_business(file_num)

            if data and data.get('business_name'):
                biz_type = data.get('business_type', 'Unknown')
                biz_name = data.get('business_name', 'Unknown')[:40]
                print(f"FOUND: {biz_type} - {biz_name}")
                business_types[biz_type].append((file_num, data.get('business_name')))
            else:
                print("not found")

            await asyncio.sleep(1.2)

        # Summary
        print(f"\n\n{'='*60}")
        print(f"BUSINESS TYPES FOUND: {len(business_types)}")
        print('='*60)
        for biz_type in sorted(business_types.keys()):
            examples = business_types[biz_type]
            print(f"\n{biz_type} ({len(examples)} found):")
            for file_num, name in examples[:2]:
                print(f"  - {file_num}: {name[:50]}")

        return business_types

    finally:
        await scraper.close()


if __name__ == '__main__':
    samples = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    asyncio.run(focused_discovery(total_samples=samples))
