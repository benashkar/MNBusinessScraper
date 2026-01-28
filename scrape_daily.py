#!/usr/bin/env python3
"""
Daily MN Business Scraper for GitHub Actions

Searches for recent LLC and Corporation filings and saves to CSV.
Designed to run daily via GitHub Actions cron job.
"""

import asyncio
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

import pandas as pd
from playwright.async_api import async_playwright

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))
from mn_scraper import MNBusinessScraper, convert_date_to_iso


async def search_recent_filings(page, search_term: str, max_results: int = 500):
    """
    Search for businesses by name and return results with GUIDs.
    """
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
        await asyncio.sleep(2)

        # Check for results table
        table = await page.query_selector('table.table')
        if not table:
            return results

        # Get all result rows (skip header)
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

                    if guid and any(term in name.upper() for term in ['LLC', 'L.L.C.', 'CORPORATION', 'CORP', 'INC']):
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


async def scrape_daily_filings():
    """
    Scrape recent LLC and Corporation filings.
    Searches for current year to find new filings.
    """
    output_dir = Path('data')
    output_dir.mkdir(exist_ok=True)

    today = datetime.now()
    current_year = today.strftime('%Y')
    output_file = output_dir / 'daily_filings.csv'
    progress_file = output_dir / 'daily_progress.json'

    # Load existing GUIDs to avoid duplicates
    existing_guids = set()
    if output_file.exists():
        try:
            existing_df = pd.read_csv(output_file)
            existing_guids = set(existing_df['file_number'].astype(str).tolist())
            print(f"Loaded {len(existing_guids)} existing records")
        except Exception:
            pass

    # Search terms to find recent filings
    search_terms = [
        current_year,  # e.g., "2026"
        'LLC',
        'Inc',
        'Corp',
    ]

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
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
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        scraper = MNBusinessScraper(headless=True)
        await scraper.initialize()

        new_records = []
        guids_processed = set()

        try:
            for search_term in search_terms:
                print(f"\nSearching: '{search_term}'...")
                results = await search_recent_filings(page, search_term, max_results=200)
                print(f"  Found {len(results)} LLC/Corp results")

                for r in results:
                    guid = r['guid']

                    # Skip if already processed or existing
                    if guid in guids_processed or guid in existing_guids:
                        continue

                    guids_processed.add(guid)

                    try:
                        data = await scraper.scrape_business_by_guid(guid)
                        if data:
                            if not data.get('business_name'):
                                data['business_name'] = r['business_name']

                            # Check if it's recent (last 30 days preferred, but accept current year)
                            filing_date = data.get('filing_date', '')
                            filing_year = filing_date[:4] if filing_date else ''

                            if filing_year == current_year:
                                print(f"    [NEW] {data['business_name']} - {filing_date}")
                                new_records.append(data)

                        await asyncio.sleep(0.5)
                    except Exception as e:
                        print(f"    Error: {e}")
                        continue

                await asyncio.sleep(2)  # Rate limit between searches

            # Save new records
            if new_records:
                new_df = pd.DataFrame(new_records)

                if output_file.exists():
                    # Append to existing
                    existing_df = pd.read_csv(output_file)
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    combined_df.drop_duplicates(subset=['file_number'], keep='last', inplace=True)
                    combined_df.to_csv(output_file, index=False)
                    print(f"\nAdded {len(new_records)} new records. Total: {len(combined_df)}")
                else:
                    new_df.to_csv(output_file, index=False)
                    print(f"\nSaved {len(new_records)} new records")

            # Update progress
            with open(progress_file, 'w') as f:
                json.dump({
                    'last_run': today.isoformat(),
                    'records_found': len(new_records),
                    'total_processed': len(guids_processed),
                }, f, indent=2)

            print(f"\nDaily scrape complete. Found {len(new_records)} new filings.")

        finally:
            await scraper.close()
            await context.close()
            await browser.close()

    return new_records


if __name__ == '__main__':
    asyncio.run(scrape_daily_filings())
