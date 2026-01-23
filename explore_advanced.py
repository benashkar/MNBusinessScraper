#!/usr/bin/env python3
"""Explore advanced search options on MN SOS website."""

import asyncio
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from playwright.async_api import async_playwright


async def explore_advanced():
    """Click Advanced Options and explore what's available."""

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        try:
            print("Navigating to MN SOS Business Search...")
            await page.goto("https://mblsportal.sos.mn.gov/Business/Search", timeout=30000)
            await page.wait_for_load_state("networkidle")

            # Click Advanced Options
            print("\nClicking 'Advanced Options'...")
            advanced_link = await page.query_selector('a:has-text("Advanced Options")')
            if advanced_link:
                await advanced_link.click()
                await page.wait_for_timeout(1000)

                print("\n" + "=" * 80)
                print("ADVANCED OPTIONS REVEALED")
                print("=" * 80)

                # Get all visible form elements now
                all_elements = await page.query_selector_all('input, select, label')
                for el in all_elements:
                    tag = await el.evaluate("el => el.tagName")
                    el_id = await el.get_attribute("id")
                    el_name = await el.get_attribute("name")
                    el_type = await el.get_attribute("type")
                    visible = await el.is_visible()

                    if visible:
                        if tag == "LABEL":
                            text = await el.inner_text()
                            for_attr = await el.get_attribute("for")
                            print(f"LABEL: '{text}' (for={for_attr})")
                        elif tag == "SELECT":
                            options = await el.query_selector_all('option')
                            opt_texts = [await opt.inner_text() for opt in options]
                            print(f"SELECT: id={el_id}, name={el_name}")
                            print(f"  Options: {opt_texts}")
                        else:
                            print(f"INPUT: id={el_id}, name={el_name}, type={el_type}")

                # Look specifically for date fields
                print("\n--- Looking for date-related fields ---")
                date_inputs = await page.query_selector_all('input[type="date"], input[name*="date" i], input[id*="date" i]')
                for inp in date_inputs:
                    inp_id = await inp.get_attribute("id")
                    inp_name = await inp.get_attribute("name")
                    print(f"  Date field: id={inp_id}, name={inp_name}")

                # Check for entity type dropdown
                print("\n--- Entity Type Options ---")
                entity_select = await page.query_selector('select[name="EntityType"], select[id*="entity" i], select[id*="type" i]')
                if entity_select:
                    options = await entity_select.query_selector_all('option')
                    for opt in options:
                        text = await opt.inner_text()
                        value = await opt.get_attribute("value")
                        print(f"  {value}: {text}")

                # Print full page HTML of the form for analysis
                print("\n--- Form HTML snippet ---")
                form = await page.query_selector('form')
                if form:
                    html = await form.inner_html()
                    # Print first 3000 chars
                    print(html[:3000])

            await page.wait_for_timeout(3000)  # Keep visible for inspection

        finally:
            await browser.close()


if __name__ == '__main__':
    asyncio.run(explore_advanced())
