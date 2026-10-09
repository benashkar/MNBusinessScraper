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
import shutil
from pathlib import Path
from datetime import datetime, timedelta

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Global for periodic backup tracking
_last_backup_time = None
BACKUP_INTERVAL_HOURS = 4

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from mn_scraper import MNBusinessScraper
import config


def do_periodic_backup(output_dir: Path, force: bool = False):
    """Create timestamped backup of all worker CSV files every BACKUP_INTERVAL_HOURS hours."""
    global _last_backup_time

    now = datetime.now()
    if not force and _last_backup_time is not None:
        if now - _last_backup_time < timedelta(hours=BACKUP_INTERVAL_HOURS):
            return False

    backup_dir = output_dir / 'backups'
    backup_dir.mkdir(exist_ok=True)

    timestamp = now.strftime('%Y%m%d_%H%M%S')
    backup_count = 0

    # Backup all worker CSV files
    for csv_file in output_dir.glob('businesses_worker_*.csv'):
        backup_name = f"{csv_file.stem}_backup_{timestamp}.csv"
        backup_path = backup_dir / backup_name
        try:
            shutil.copy2(csv_file, backup_path)
            backup_count += 1
        except Exception as e:
            print(f"[BACKUP] Error backing up {csv_file.name}: {e}")

    if backup_count > 0:
        print(f"\n[BACKUP] Created {backup_count} backup files at {timestamp}")
        _last_backup_time = now
        return True
    return False


async def scrape_range(worker_id: int, start: int, end: int, headless: bool = True,
                       chunk_size: int = None, max_file_number: int = 800000):
    """
    Scrape a specific range of file numbers, then continue to next range when done.

    Args:
        worker_id: Unique identifier for this worker
        start: Starting file number (inclusive)
        end: Ending file number (inclusive)
        headless: Run browser in headless mode
        chunk_size: Size of each range chunk (for auto-continuation)
        max_file_number: Maximum file number to scrape before stopping
    """
    import pandas as pd

    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / f'businesses_worker_{worker_id}.csv'
    progress_file = Path(f'progress_worker_{worker_id}.json')

    # Calculate chunk size if not provided
    if chunk_size is None:
        chunk_size = end - start + 1

    # Check for existing progress
    current_start = start
    current_end = end
    if progress_file.exists():
        try:
            with open(progress_file) as f:
                progress = json.load(f)
                saved_pos = progress.get('last_file_number', start)
                saved_end = progress.get('end', end)
                if saved_pos >= start:
                    current_start = saved_pos + 1
                    current_end = saved_end
                    print(f"[Worker {worker_id}] Resuming from file #{current_start}")
        except:
            pass

    scraper = MNBusinessScraper(headless=headless)
    total_found = 0

    try:
        await scraper.initialize()

        # Continue scraping ranges until we hit max_file_number
        while current_start <= max_file_number:
            print(f"[Worker {worker_id}] Scraping range {current_start:,} - {current_end:,}")

            consecutive_misses = 0
            range_found = 0

            for file_number in range(current_start, min(current_end, max_file_number) + 1):
                try:
                    data = await scraper.scrape_business(file_number)

                    if data:
                        consecutive_misses = 0
                        range_found += 1
                        total_found += 1

                        # Save to worker-specific CSV
                        df = pd.DataFrame([data])

                        if not output_file.exists():
                            df.to_csv(output_file, index=False)
                        else:
                            df.to_csv(output_file, mode='a', header=False, index=False)

                        if total_found % 10 == 0:
                            print(f"[Worker {worker_id}] {total_found} found, at #{file_number:,}")
                    else:
                        consecutive_misses += 1

                    # Save progress every 10 file numbers
                    if file_number % 10 == 0:
                        with open(progress_file, 'w') as f:
                            json.dump({
                                'worker_id': worker_id,
                                'start': start,
                                'end': current_end,
                                'last_file_number': file_number,
                                'found_count': total_found,
                                'updated_at': datetime.now().isoformat()
                            }, f, indent=2)

                    # Periodic backup check (only worker 0 does this to avoid conflicts)
                    if worker_id == 0 and file_number % 100 == 0:
                        do_periodic_backup(output_dir)

                    # Rate limiting
                    delay = config.REQUEST_DELAY + (hash(file_number) % 100) / 100 * config.DELAY_JITTER
                    await asyncio.sleep(delay)

                except Exception as e:
                    print(f"[Worker {worker_id}] Error at #{file_number}: {e}")
                    await asyncio.sleep(5)
                    continue

            print(f"[Worker {worker_id}] Completed range {current_start:,}-{current_end:,}, found {range_found}")

            # Move to next range
            current_start = current_end + 1
            current_end = current_start + chunk_size - 1

            if current_start > max_file_number:
                print(f"[Worker {worker_id}] Reached max file number {max_file_number:,}")
                break

        print(f"[Worker {worker_id}] FINISHED - Total found: {total_found:,}")

    finally:
        await scraper.close()

    return total_found


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
    print(f"Backup interval: {BACKUP_INTERVAL_HOURS} hours")
    print("=" * 60)
    for worker_id, start, end in ranges:
        print(f"  Worker {worker_id}: {start:,} - {end:,} ({end-start+1:,} file numbers)")
    print("=" * 60)
    print()

    # Initialize backup timer
    global _last_backup_time
    _last_backup_time = datetime.now()

    # Launch all workers with auto-continuation enabled
    tasks = [
        scrape_range(worker_id, start, end, headless, chunk_size=chunk_size, max_file_number=800000)
        for worker_id, start, end in ranges
    ]

    # Run all workers concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Final backup
    output_dir = Path('output')
    do_periodic_backup(output_dir, force=True)

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
    print(f"\nBackups saved in: output/backups/")
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
