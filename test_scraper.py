#!/usr/bin/env python3
"""Test script to verify scraper with specific file numbers."""

import asyncio
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.path.insert(0, '.')

from mn_scraper import MNBusinessScraper


async def test_file_numbers(file_numbers: list[int]):
    """Test scraping specific file numbers."""
    scraper = MNBusinessScraper(headless=False)

    try:
        await scraper.initialize()

        for file_num in file_numbers:
            print(f"\n{'='*70}")
            print(f"Testing file number: {file_num}")
            print('='*70)

            data = await scraper.scrape_business(file_num)

            if data:
                print(f"[FOUND]:")

                # Group fields for better readability
                print("\n--- Basic Info ---")
                for key in ['file_number', 'business_name', 'mn_statute', 'business_type',
                           'home_jurisdiction', 'filing_date', 'status', 'renewal_due_date',
                           'mark_type', 'number_of_shares', 'chief_executive_officer', 'manager']:
                    if data.get(key):
                        print(f"  {key}: {data[key]}")

                print("\n--- Principal Place of Business Address (Assumed Names) ---")
                has_principal = any(data.get(k) for k in ['principal_street_number', 'principal_street_name',
                           'principal_city', 'principal_state', 'principal_zip'])
                if has_principal:
                    for key in ['principal_street_number', 'principal_street_name',
                               'principal_street_type', 'principal_street_direction',
                               'principal_unit', 'principal_city', 'principal_state', 'principal_zip']:
                        val = data.get(key, '')
                        if val:
                            print(f"  {key}: {val}")
                    if data.get('principal_address_raw'):
                        raw = data['principal_address_raw'].replace('\n', ' | ')
                        print(f"  (raw): {raw[:80]}")
                else:
                    print("  (none)")

                print("\n--- Registered Office Address (Corporations) ---")
                has_reg_office = any(data.get(k) for k in ['reg_office_street_number', 'reg_office_street_name',
                           'reg_office_city', 'reg_office_state', 'reg_office_zip'])
                if has_reg_office:
                    for key in ['reg_office_street_number', 'reg_office_street_name',
                               'reg_office_street_type', 'reg_office_street_direction',
                               'reg_office_unit', 'reg_office_city', 'reg_office_state', 'reg_office_zip']:
                        val = data.get(key, '')
                        if val:
                            print(f"  {key}: {val}")
                    if data.get('reg_office_address_raw'):
                        raw = data['reg_office_address_raw'].replace('\n', ' | ')
                        print(f"  (raw): {raw[:80]}")
                else:
                    print("  (none)")

                print("\n--- Principal Executive Office Address (Corporations) ---")
                has_exec_office = any(data.get(k) for k in ['exec_office_street_number', 'exec_office_street_name',
                           'exec_office_city', 'exec_office_state', 'exec_office_zip'])
                if has_exec_office:
                    for key in ['exec_office_street_number', 'exec_office_street_name',
                               'exec_office_street_type', 'exec_office_street_direction',
                               'exec_office_unit', 'exec_office_city', 'exec_office_state', 'exec_office_zip']:
                        val = data.get(key, '')
                        if val:
                            print(f"  {key}: {val}")
                    if data.get('exec_office_address_raw'):
                        raw = data['exec_office_address_raw'].replace('\n', ' | ')
                        print(f"  (raw): {raw[:80]}")
                else:
                    print("  (none)")

                print("\n--- Applicant/Markholder Info ---")
                if data.get('applicant_name'):
                    print(f"  applicant_name: {data['applicant_name']}")
                    for key in ['applicant_street_number', 'applicant_street_name',
                               'applicant_street_type', 'applicant_street_direction',
                               'applicant_unit', 'applicant_city', 'applicant_state', 'applicant_zip']:
                        val = data.get(key, '')
                        if val:
                            print(f"  {key}: {val}")
                    if data.get('applicant_address_raw'):
                        print(f"  (raw): {data['applicant_address_raw']}")
                else:
                    print("  (none)")

                print("\n--- Other ---")
                if data.get('registered_agent_name'):
                    print(f"  registered_agent_name: {data['registered_agent_name']}")
                if data.get('filing_history'):
                    hist = data['filing_history'][:100] + '...' if len(data['filing_history']) > 100 else data['filing_history']
                    print(f"  filing_history: {hist}")
            else:
                print(f"[NOT FOUND] No result for file number {file_num}")

            await asyncio.sleep(2)

    finally:
        await scraper.close()


if __name__ == '__main__':
    # Test various business types - try some sequential low numbers
    # to find Limited Partnerships, Nonprofits, Cooperatives, etc.
    test_numbers = [
        3000,   # Try around here for older entities
        5000,
        15000,  # Mix of types expected
        20000,
        25000,
        30000,
    ]
    asyncio.run(test_file_numbers(test_numbers))
