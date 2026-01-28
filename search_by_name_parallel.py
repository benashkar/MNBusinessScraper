#!/usr/bin/env python3
"""
Parallel Alphabetical MN Business Scraper

Searches for LLCs and Corporations by name patterns (aa, ab, ac, ... zz)
using multiple parallel workers. Focuses on recent filings (2024-2026).
"""

import argparse
import asyncio
import json
import string
import sys
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

import pandas as pd
from playwright.async_api import async_playwright

# Import the scraper for data extraction
sys.path.insert(0, str(Path(__file__).parent))
from mn_scraper import MNBusinessScraper

# Target years for recent filings
TARGET_YEARS = ['2024', '2025', '2026']

# Business type filters
BUSINESS_TYPE_KEYWORDS = ['LLC', 'L.L.C.', 'CORPORATION', 'CORP', 'INC', 'INCORPORATED']


def generate_patterns():
    """Generate all two-letter search patterns (aa-zz = 676 patterns)."""
    patterns = []
    for c1 in string.ascii_lowercase:
        for c2 in string.ascii_lowercase:
            patterns.append(c1 + c2)
    return patterns


async def search_by_name(search_term: str, page, max_results: int = 500):
    """Search for businesses by name and return results with GUIDs."""
    results = []

    try:
        await page.goto("https://mblsportal.sos.mn.gov/Business/Search", timeout=30000)
        await page.wait_for_load_state("networkidle")

        # Select "Contains" for broader search
        await page.evaluate('document.getElementById("containsz").checked = true')

        # Enter search term
        await page.fill('#BusinessName', search_term)

        # Submit search
        search_btn = page.locator('button:has-text("Search")').first
        await search_btn.click()
        await page.wait_for_load_state("networkidle")
        await asyncio.sleep(1.5)

        # Check for results table
        table = await page.query_selector('table.table')
        if not table:
            return results

        # Get all result rows
        rows = await table.query_selector_all('tbody tr')

        for row in rows[:max_results]:
            try:
                cells = await row.query_selector_all('td')
                if len(cells) >= 1:
                    name_cell = cells[0]
                    name_elem = await name_cell.query_selector('strong')
                    if name_elem:
                        name = (await name_elem.inner_text()).strip()
                    else:
                        name = (await name_cell.inner_text()).strip()

                    # Get GUID from Details link
                    details_link = await row.query_selector('a[href*="filingGuid"]')
                    guid = None
                    if details_link:
                        href = await details_link.get_attribute('href')
                        if 'filingGuid=' in href:
                            guid = href.split('filingGuid=')[-1]

                    # Filter for LLCs and Corporations
                    name_upper = name.upper()
                    if guid and any(kw in name_upper for kw in BUSINESS_TYPE_KEYWORDS):
                        results.append({
                            'business_name': name,
                            'guid': guid
                        })
            except Exception:
                continue

        return results

    except Exception as e:
        print(f"Error searching '{search_term}': {e}")
        return results


async def worker_scrape(worker_id: int, patterns: list, headless: bool = True):
    """
    Worker function to scrape a subset of patterns.

    Args:
        worker_id: Unique worker identifier
        patterns: List of search patterns for this worker
        headless: Run browser in headless mode
    """
    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / f'businesses_alpha_worker_{worker_id}.csv'
    progress_file = Path(f'progress_alpha_worker_{worker_id}.json')

    # Load progress
    completed_patterns = set()
    processed_guids = set()
    if progress_file.exists():
        try:
            with open(progress_file) as f:
                progress = json.load(f)
                completed_patterns = set(progress.get('completed_patterns', []))
                processed_guids = set(progress.get('processed_guids', []))
        except Exception:
            pass

    # Load existing GUIDs from output file to avoid duplicates
    if output_file.exists():
        try:
            existing_df = pd.read_csv(output_file)
            if 'file_number' in existing_df.columns:
                processed_guids.update(existing_df['file_number'].astype(str).tolist())
        except Exception:
            pass

    remaining_patterns = [p for p in patterns if p not in completed_patterns]
    print(f"[Worker {worker_id}] {len(remaining_patterns)} patterns remaining (of {len(patterns)})")

    if not remaining_patterns:
        print(f"[Worker {worker_id}] All patterns completed!")
        return 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-dev-shm-usage',
            ]
        )
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
        )
        page = await context.new_page()

        # Remove webdriver flag
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        # Initialize scraper for detailed extraction
        scraper = MNBusinessScraper(headless=headless)
        await scraper.initialize()

        found_count = 0
        recent_count = 0

        try:
            for pattern in remaining_patterns:
                print(f"[Worker {worker_id}] Searching: '{pattern}'...")

                try:
                    results = await search_by_name(pattern, page, max_results=500)
                except Exception as e:
                    print(f"[Worker {worker_id}] Search error for '{pattern}': {e}")
                    await asyncio.sleep(5)
                    continue

                new_results = [r for r in results if r['guid'] not in processed_guids]
                print(f"[Worker {worker_id}] '{pattern}': {len(results)} results, {len(new_results)} new")

                # Get details for each new result
                for r in new_results:
                    guid = r['guid']

                    try:
                        data = await scraper.scrape_business_by_guid(guid)
                        if data:
                            if not data.get('business_name'):
                                data['business_name'] = r['business_name']

                            # Check if it's from target years
                            filing_date = data.get('filing_date', '')
                            filing_year = filing_date[:4] if filing_date else ''

                            if filing_year in TARGET_YEARS:
                                recent_count += 1
                                print(f"[Worker {worker_id}] [RECENT {filing_year}] {data['business_name']}")

                                # Save to CSV
                                df = pd.DataFrame([data])
                                if not output_file.exists():
                                    df.to_csv(output_file, index=False)
                                else:
                                    df.to_csv(output_file, mode='a', header=False, index=False)

                                found_count += 1

                            processed_guids.add(guid)

                        await asyncio.sleep(0.3)

                    except Exception as e:
                        print(f"[Worker {worker_id}] Error scraping {guid}: {e}")
                        continue

                # Mark pattern complete and save progress
                completed_patterns.add(pattern)
                with open(progress_file, 'w') as f:
                    json.dump({
                        'worker_id': worker_id,
                        'completed_patterns': list(completed_patterns),
                        'processed_guids': list(processed_guids)[-10000:],  # Keep last 10k to limit file size
                        'found_count': found_count,
                        'recent_count': recent_count,
                        'updated_at': datetime.now().isoformat()
                    }, f, indent=2)

                # Rate limiting between patterns
                await asyncio.sleep(1)

            print(f"[Worker {worker_id}] COMPLETED - Found {found_count} recent businesses ({recent_count} from {TARGET_YEARS})")

        except Exception as e:
            print(f"[Worker {worker_id}] Fatal error: {e}")

        finally:
            await scraper.close()
            await context.close()
            await browser.close()

    return found_count


async def run_parallel(num_workers: int, headless: bool = True):
    """
    Run multiple workers in parallel.

    Args:
        num_workers: Number of parallel workers
        headless: Run browsers in headless mode
    """
    patterns = generate_patterns()
    total_patterns = len(patterns)

    # Distribute patterns among workers
    chunk_size = len(patterns) // num_workers
    worker_patterns = []

    for i in range(num_workers):
        start_idx = i * chunk_size
        if i == num_workers - 1:
            # Last worker gets remaining patterns
            end_idx = len(patterns)
        else:
            end_idx = start_idx + chunk_size
        worker_patterns.append(patterns[start_idx:end_idx])

    print("=" * 70)
    print(f"PARALLEL ALPHABETICAL SCRAPER - {num_workers} WORKERS")
    print(f"Target Years: {', '.join(TARGET_YEARS)}")
    print(f"Business Types: LLC, Corporation, Inc")
    print("=" * 70)
    for i, wp in enumerate(worker_patterns):
        print(f"  Worker {i}: patterns {wp[0]} - {wp[-1]} ({len(wp)} patterns)")
    print("=" * 70)
    print()

    # Launch all workers
    tasks = [
        worker_scrape(i, worker_patterns[i], headless)
        for i in range(num_workers)
    ]

    # Run all workers concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    total_found = 0
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"  Worker {i}: ERROR - {result}")
        else:
            print(f"  Worker {i}: Found {result} recent businesses")
            total_found += result

    print(f"\nTotal recent businesses found: {total_found}")
    print("\nOutput files:")
    for i in range(num_workers):
        output_file = Path('output') / f'businesses_alpha_worker_{i}.csv'
        if output_file.exists():
            try:
                df = pd.read_csv(output_file)
                print(f"  {output_file.name}: {len(df)} records")
            except:
                print(f"  {output_file.name}: exists")
        else:
            print(f"  {output_file.name}: not created yet")

    print("\nRun 'python merge_results.py --alpha' to combine all worker outputs.")


def main():
    parser = argparse.ArgumentParser(description='Parallel Alphabetical MN Business Scraper')
    parser.add_argument('--workers', '-w', type=int, default=8,
                        help='Number of parallel workers (default: 8)')
    parser.add_argument('--visible', action='store_true',
                        help='Run browsers visibly (not headless)')
    parser.add_argument('--years', '-y', nargs='+', default=['2024', '2025', '2026'],
                        help='Target filing years (default: 2024 2025 2026)')

    args = parser.parse_args()

    # Update target years if specified
    global TARGET_YEARS
    TARGET_YEARS = args.years
    print(f"Targeting years: {TARGET_YEARS}")

    asyncio.run(run_parallel(
        num_workers=args.workers,
        headless=not args.visible
    ))


if __name__ == '__main__':
    main()
