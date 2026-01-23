#!/usr/bin/env python3
"""Test name-based search to find businesses and check filing dates in results."""

import asyncio
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from playwright.async_api import async_playwright


async def test_name_search():
    """Test searching by name and see what data is available in search results."""

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        try:
            print("Navigating to MN SOS Business Search...")
            await page.goto("https://mblsportal.sos.mn.gov/Business/Search", timeout=30000)
            await page.wait_for_load_state("networkidle")

            # Try a simple name search
            search_term = "2024"  # Try searching for businesses with 2024 in name

            print(f"\nSearching for businesses containing '{search_term}'...")

            # Select "Contains" radio button using JavaScript
            await page.evaluate('document.getElementById("containsz").checked = true')

            # Enter search term
            await page.fill('#BusinessName', search_term)

            # Click search button
            await page.click('button.btn-success, input[type="submit"]')
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(3000)

            # Check results
            print("\n" + "=" * 80)
            print("SEARCH RESULTS")
            print("=" * 80)

            # Check for results table
            results_table = await page.query_selector('table')
            if results_table:
                # Get headers
                headers = await results_table.query_selector_all('th')
                header_texts = [await h.inner_text() for h in headers]
                print(f"Table columns: {header_texts}")

                # Get first 15 rows
                rows = await results_table.query_selector_all('tbody tr')
                print(f"\nFound {len(rows)} results visible. First 15:")

                for i, row in enumerate(rows[:15]):
                    cells = await row.query_selector_all('td')
                    cell_texts = [(await c.inner_text()).strip()[:40] for c in cells]
                    print(f"  {i+1}. {cell_texts}")

                # Check if filing date is visible in results
                if any("Date" in h or "Filing" in h for h in header_texts):
                    print("\n[+] Date column found in search results!")
                else:
                    print(f"\n[-] No date column. Columns: {header_texts}")

            # Check pagination info
            pagination = await page.query_selector('.dataTables_info, .pagination-info')
            if pagination:
                info = await pagination.inner_text()
                print(f"\nPagination info: {info}")

            # Take screenshot for review
            await page.screenshot(path="search_results.png")
            print("\nScreenshot saved to search_results.png")

            await page.wait_for_timeout(2000)

        finally:
            await browser.close()


if __name__ == '__main__':
    asyncio.run(test_name_search())
