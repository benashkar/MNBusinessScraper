#!/usr/bin/env python3
"""
Search MN SOS by business name to find recent LLCs and Corporations.
Iterates through name patterns (a, aa, aaa, b, bb, etc.)
"""

import asyncio
import sys
import json
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

import pandas as pd
from playwright.async_api import async_playwright

# Import the scraper for data extraction
sys.path.insert(0, str(Path(__file__).parent))
from mn_scraper import MNBusinessScraper, convert_date_to_iso


async def search_by_name(search_term: str, page, max_results: int = 100):
    """
    Search for businesses by name and return results.
    """
    results = []

    try:
        # Navigate to search page
        await page.goto("https://mblsportal.sos.mn.gov/Business/Search", timeout=30000)
        await page.wait_for_load_state("networkidle")

        # Select "Contains" for broader search
        await page.evaluate('document.getElementById("containsz").checked = true')

        # Enter search term
        await page.fill('#BusinessName', search_term)

        # Submit search - use text selector for Search button
        search_btn = page.locator('button:has-text("Search")').first
        await search_btn.click()
        await page.wait_for_load_state("networkidle")
        await asyncio.sleep(2)

        # Check for results table
        table = await page.query_selector('table.table')
        if not table:
            return results

        # Get all result rows
        rows = await table.query_selector_all('tbody tr')

        for row in rows[:max_results]:
            try:
                cells = await row.query_selector_all('td')
                if len(cells) >= 2:
                    # Get business name and file number
                    name_cell = cells[0]
                    name_elem = await name_cell.query_selector('strong')
                    if name_elem:
                        name = (await name_elem.inner_text()).strip()
                    else:
                        name = (await name_cell.inner_text()).strip()

                    # Get file number from Details link (can be SearchDetails or BusinessDetails)
                    details_link = await row.query_selector('a[href*="filingGuid"]')
                    file_number = None
                    if details_link:
                        href = await details_link.get_attribute('href')
                        if 'filingGuid=' in href:
                            file_number = href.split('filingGuid=')[-1]

                    results.append({
                        'business_name': name,
                        'file_number': file_number
                    })
            except Exception as e:
                continue

        return results

    except Exception as e:
        print(f"Error searching '{search_term}': {e}")
        return results


async def scrape_business_details(file_number: str, scraper: MNBusinessScraper):
    """Get full details for a business by file number."""
    try:
        data = await scraper.scrape_business(file_number)
        return data
    except:
        return None


async def explore_name_search():
    """Test the name-based search approach."""

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

        # Remove webdriver flag
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        try:
            # Test with "LLC" to find LLCs
            print("Testing search for 'LLC'...")
            results = await search_by_name("LLC", page, max_results=20)
            print(f"Found {len(results)} results for 'LLC'")
            for r in results[:5]:
                print(f"  - {r['business_name']} (#{r['file_number']})")

            print("\nTesting search for 'Corporation'...")
            results = await search_by_name("Corporation", page, max_results=20)
            print(f"Found {len(results)} results for 'Corporation'")
            for r in results[:5]:
                print(f"  - {r['business_name']} (#{r['file_number']})")

            print("\nTesting search for '2024'...")
            results = await search_by_name("2024", page, max_results=20)
            print(f"Found {len(results)} results for '2024'")
            for r in results[:5]:
                print(f"  - {r['business_name']} (#{r['file_number']})")

            print("\nTesting search for '2025'...")
            results = await search_by_name("2025", page, max_results=20)
            print(f"Found {len(results)} results for '2025'")
            for r in results[:5]:
                print(f"  - {r['business_name']} (#{r['file_number']})")

        finally:
            await context.close()
            await browser.close()


async def scrape_llc_by_name_patterns():
    """
    Scrape LLCs by iterating through name patterns.
    Uses patterns like: a, aa, aaa, b, bb, etc.
    """
    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / 'businesses_llc_search.csv'
    progress_file = Path('progress_llc_search.json')

    # Load progress
    completed_patterns = set()
    if progress_file.exists():
        with open(progress_file) as f:
            progress = json.load(f)
            completed_patterns = set(progress.get('completed_patterns', []))

    # Generate patterns: a, b, c, ..., aa, ab, ..., aaa, etc.
    import string
    patterns = []

    # Single letters
    for c in string.ascii_lowercase:
        patterns.append(c)

    # Two letter combinations
    for c1 in string.ascii_lowercase:
        for c2 in string.ascii_lowercase:
            patterns.append(c1 + c2)

    print(f"Generated {len(patterns)} search patterns")
    print(f"Already completed: {len(completed_patterns)}")

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

        # Remove webdriver flag
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        # Also initialize scraper for detailed extraction
        scraper = MNBusinessScraper(headless=True)
        await scraper.initialize()

        all_results = []

        try:
            for pattern in patterns:
                if pattern in completed_patterns:
                    continue

                print(f"\nSearching: '{pattern}'...")
                results = await search_by_name(pattern, page, max_results=500)

                # Filter for LLCs and Corporations only
                llc_corp_results = [r for r in results if r['file_number'] and
                                   any(term in r['business_name'].upper() for term in ['LLC', 'L.L.C.', 'CORPORATION', 'CORP', 'INC'])]

                print(f"  Found {len(results)} total, {len(llc_corp_results)} LLCs/Corps")

                # Get details for LLC/Corp results using GUID method
                for r in llc_corp_results[:20]:  # Limit to avoid overload
                    try:
                        data = await scraper.scrape_business_by_guid(r['file_number'])
                        if data:
                            # Use business name from search if not extracted
                            if not data.get('business_name'):
                                data['business_name'] = r['business_name']
                            # Check if it's recent (2024 or 2025)
                            filing_year = data.get('filing_date', '')[:4]
                            if filing_year in ['2024', '2025', '2026']:
                                print(f"    [RECENT] {data['business_name']} - {data['filing_date']}")
                            all_results.append(data)
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        continue

                # Save progress
                completed_patterns.add(pattern)
                with open(progress_file, 'w') as f:
                    json.dump({
                        'completed_patterns': list(completed_patterns),
                        'total_found': len(all_results),
                        'updated_at': datetime.now().isoformat()
                    }, f, indent=2)

                # Save results periodically
                if all_results and len(all_results) % 50 == 0:
                    df = pd.DataFrame(all_results)
                    df.to_csv(output_file, index=False)
                    print(f"  Saved {len(all_results)} records")

                await asyncio.sleep(1)

            # Final save
            if all_results:
                df = pd.DataFrame(all_results)
                df.to_csv(output_file, index=False)
                print(f"\nFinal save: {len(all_results)} records to {output_file}")

        finally:
            await scraper.close()
            await context.close()
            await browser.close()


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--full':
        asyncio.run(scrape_llc_by_name_patterns())
    else:
        asyncio.run(explore_name_search())
