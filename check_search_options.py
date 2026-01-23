#!/usr/bin/env python3
"""Check what search options are available on the MN SOS website."""

import asyncio
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from playwright.async_api import async_playwright


async def explore_search_page():
    """Explore the search page to find all available search options."""

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Visible to see what's happening
        page = await browser.new_page()

        try:
            print("Navigating to MN SOS Business Search...")
            await page.goto("https://mblsportal.sos.mn.gov/Business/Search", timeout=30000)
            await page.wait_for_load_state("networkidle")

            print("\n" + "=" * 80)
            print("EXPLORING SEARCH OPTIONS")
            print("=" * 80)

            # Get all tabs/search types
            tabs = await page.query_selector_all('.nav-tabs a[data-toggle="tab"]')
            print(f"\nSearch tabs found: {len(tabs)}")
            for tab in tabs:
                text = await tab.inner_text()
                href = await tab.get_attribute("href")
                print(f"  - {text} ({href})")

            # Check for advanced search options
            print("\n--- Looking for date/filter inputs ---")

            # Get all form inputs on the page
            inputs = await page.query_selector_all('input, select')
            for inp in inputs:
                input_type = await inp.get_attribute("type")
                input_id = await inp.get_attribute("id")
                input_name = await inp.get_attribute("name")
                placeholder = await inp.get_attribute("placeholder")
                tag = await inp.evaluate("el => el.tagName")

                if input_id or input_name:
                    print(f"  {tag}: id={input_id}, name={input_name}, type={input_type}, placeholder={placeholder}")

            # Check for checkboxes or advanced options
            print("\n--- Looking for checkboxes/advanced options ---")
            checkboxes = await page.query_selector_all('input[type="checkbox"]')
            for cb in checkboxes:
                cb_id = await cb.get_attribute("id")
                label = await page.query_selector(f'label[for="{cb_id}"]')
                label_text = await label.inner_text() if label else "No label"
                print(f"  Checkbox: {cb_id} - {label_text}")

            # Check for select dropdowns
            print("\n--- Dropdown menus ---")
            selects = await page.query_selector_all('select')
            for sel in selects:
                sel_id = await sel.get_attribute("id")
                sel_name = await sel.get_attribute("name")
                options = await sel.query_selector_all('option')
                opt_texts = [await opt.inner_text() for opt in options[:5]]
                print(f"  Select: id={sel_id}, name={sel_name}")
                print(f"    Options (first 5): {opt_texts}")

            # Check for Advanced Search link
            print("\n--- Looking for Advanced Search ---")
            links = await page.query_selector_all('a')
            for link in links:
                text = await link.inner_text()
                href = await link.get_attribute("href")
                if "advanced" in text.lower() or "filter" in text.lower() or "date" in text.lower():
                    print(f"  Found: {text} -> {href}")

            # Click through each tab and explore
            for tab in tabs:
                tab_text = await tab.inner_text()
                print(f"\n--- Exploring '{tab_text}' tab ---")
                await tab.click()
                await page.wait_for_timeout(500)

                # Get form elements in this tab
                visible_inputs = await page.query_selector_all('input:visible, select:visible')
                for inp in visible_inputs:
                    input_id = await inp.get_attribute("id")
                    input_name = await inp.get_attribute("name")
                    placeholder = await inp.get_attribute("placeholder")
                    if input_id or input_name:
                        print(f"    Input: id={input_id}, name={input_name}, placeholder={placeholder}")

            await page.wait_for_timeout(2000)  # Let user see the page

        finally:
            await browser.close()


if __name__ == '__main__':
    asyncio.run(explore_search_page())
