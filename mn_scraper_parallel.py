#!/usr/bin/env python3
"""
Parallel Minnesota Business Scraper

Runs multiple scraper instances simultaneously, each handling a different
range of file numbers to speed up data collection.
"""

import argparse
import asyncio
import json
import sys
import os
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from mn_scraper import MNBusinessScraper
import config


async def scrape_range(worker_id: int, start: int, end: int, headless: bool = True):
    """
    Scrape a specific range of file numbers.

    Args:
        worker_id: Unique identifier for this worker
        start: Starting file number (inclusive)
        end: Ending file number (inclusive)
        headless: Run browser in headless mode
    """
    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / f'businesses_worker_{worker_id}.csv'
    progress_file = Path(f'progress_worker_{worker_id}.json')

    # Check for existing progress
    current_start = start
    if progress_file.exists():
        try:
            with open(progress_file) as f:
                progress = json.load(f)
                saved_pos = progress.get('last_file_number', start)
                if saved_pos >= start and saved_pos < end:
                    current_start = saved_pos + 1
                    print(f"[Worker {worker_id}] Resuming from file #{current_start}")
        except:
            pass

    print(f"[Worker {worker_id}] Scraping range {current_start:,} - {end:,}")

    scraper = MNBusinessScraper(headless=headless)

    try:
        await scraper.initialize()

        consecutive_misses = 0
        found_count = 0

        for file_number in range(current_start, end + 1):
            try:
                data = await scraper.scrape_business(file_number)

                if data:
                    consecutive_misses = 0
                    found_count += 1

                    # Save to worker-specific CSV
                    import pandas as pd
                    df = pd.DataFrame([data])

                    if not output_file.exists():
                        df.to_csv(output_file, index=False)
                    else:
                        df.to_csv(output_file, mode='a', header=False, index=False)

                    if found_count % 10 == 0:
                        print(f"[Worker {worker_id}] {found_count} found, at #{file_number:,}")
                else:
                    consecutive_misses += 1

                # Save progress every 10 file numbers
                if file_number % 10 == 0:
                    with open(progress_file, 'w') as f:
                        json.dump({
                            'worker_id': worker_id,
                            'start': start,
                            'end': end,
                            'last_file_number': file_number,
                            'found_count': found_count,
                            'updated_at': datetime.now().isoformat()
                        }, f, indent=2)

                # Rate limiting
                delay = config.REQUEST_DELAY + (hash(file_number) % 100) / 100 * config.DELAY_JITTER
                await asyncio.sleep(delay)

            except Exception as e:
                print(f"[Worker {worker_id}] Error at #{file_number}: {e}")
                await asyncio.sleep(5)
                continue

        print(f"[Worker {worker_id}] COMPLETED - Found {found_count} businesses in range {start:,}-{end:,}")

    finally:
        await scraper.close()

    return found_count


async def run_parallel(num_workers: int, total_start: int, total_end: int, headless: bool = True):
    """
    Run multiple scrapers in parallel.

    Args:
        num_workers: Number of parallel workers
        total_start: Overall starting file number
        total_end: Overall ending file number
        headless: Run browsers in headless mode
    """
    # Calculate ranges for each worker
    total_range = total_end - total_start + 1
    chunk_size = total_range // num_workers

    ranges = []
    for i in range(num_workers):
        start = total_start + (i * chunk_size)
        if i == num_workers - 1:
            # Last worker takes any remainder
            end = total_end
        else:
            end = start + chunk_size - 1
        ranges.append((i, start, end))

    print("=" * 60)
    print(f"PARALLEL SCRAPER - {num_workers} WORKERS")
    print("=" * 60)
    for worker_id, start, end in ranges:
        print(f"  Worker {worker_id}: {start:,} - {end:,} ({end-start+1:,} file numbers)")
    print("=" * 60)
    print()

    # Launch all workers
    tasks = [
        scrape_range(worker_id, start, end, headless)
        for worker_id, start, end in ranges
    ]

    # Run all workers concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    total_found = 0
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"  Worker {i}: ERROR - {result}")
        else:
            print(f"  Worker {i}: Found {result:,} businesses")
            total_found += result
    print(f"\nTotal businesses found: {total_found:,}")
    print("\nOutput files created:")
    for i in range(num_workers):
        print(f"  output/businesses_worker_{i}.csv")
    print("\nRun 'python merge_results.py' to combine all worker outputs.")


def main():
    parser = argparse.ArgumentParser(description='Parallel MN Business Scraper')
    parser.add_argument('--workers', '-w', type=int, default=4,
                        help='Number of parallel workers (default: 4)')
    parser.add_argument('--start', '-s', type=int, default=1,
                        help='Starting file number (default: 1)')
    parser.add_argument('--end', '-e', type=int, default=800000,
                        help='Ending file number (default: 800000)')
    parser.add_argument('--visible', action='store_true',
                        help='Run browsers visibly (not headless)')

    args = parser.parse_args()

    asyncio.run(run_parallel(
        num_workers=args.workers,
        total_start=args.start,
        total_end=args.end,
        headless=not args.visible
    ))


if __name__ == '__main__':
    main()
