#!/usr/bin/env python3
"""
Minnesota Business Scraper

Scrapes business filings from the Minnesota Secretary of State portal
using sequential file number iteration.
"""

import argparse
import asyncio
import json
import logging
import re
import random
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

import config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('scraper.log')
    ]
)
logger = logging.getLogger(__name__)


# Common street types and their abbreviations
STREET_TYPES = {
    'street', 'st', 'str', 'avenue', 'ave', 'av', 'road', 'rd', 'drive', 'dr',
    'lane', 'ln', 'court', 'ct', 'circle', 'cir', 'boulevard', 'blvd', 'way',
    'place', 'pl', 'trail', 'trl', 'tr', 'parkway', 'pkwy', 'highway', 'hwy',
    'terrace', 'ter', 'path', 'loop', 'square', 'sq', 'crossing', 'xing'
}

# Directional suffixes/prefixes
DIRECTIONS = {'n', 's', 'e', 'w', 'ne', 'nw', 'se', 'sw', 'north', 'south', 'east', 'west'}


def convert_date_to_iso(date_str: str) -> str:
    """
    Convert date from MM/DD/YYYY to YYYY-MM-DD format.
    Returns empty string if date is invalid or empty.
    """
    if not date_str or not date_str.strip():
        return ''

    date_str = date_str.strip()

    # Try MM/DD/YYYY format
    try:
        dt = datetime.strptime(date_str, '%m/%d/%Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        pass

    # Try M/D/YYYY format (single digit month/day)
    try:
        dt = datetime.strptime(date_str, '%m/%d/%Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        pass

    # If already in YYYY-MM-DD format, return as-is
    if re.match(r'^\d{4}-\d{2}-\d{2}', date_str):
        return date_str[:10]

    # Return original if can't parse
    return date_str


def parse_address(address_str: str) -> dict:
    """
    Parse an address string into components.

    Returns dict with keys:
        street_number, street_name, street_type, street_direction,
        unit, city, state, zip
    """
    result = {
        'street_number': '',
        'street_name': '',
        'street_type': '',
        'street_direction': '',
        'unit': '',
        'city': '',
        'state': '',
        'zip': ''
    }

    if not address_str:
        return result

    # Normalize the address
    address_str = address_str.strip()

    # Handle multi-line addresses
    # e.g., "1646 DIFFLEY RD\nSTE 200\nEAGAN, MN 55122\nUSA"
    lines = [line.strip() for line in address_str.split('\n') if line.strip()]

    # Filter out country line
    lines = [l for l in lines if l.upper() not in ['USA', 'US', 'UNITED STATES']]

    # Also handle comma-separated single-line format
    # e.g., "15556 Jackson Str NE, Ham Lake, MN 55304"
    if len(lines) == 1 and ',' in lines[0]:
        parts = lines[0].split(',')
        if len(parts) >= 2:
            lines = [parts[0].strip()]
            remainder = ','.join(parts[1:]).strip()
            lines.append(remainder)

    # Identify which lines are what
    street_line = ''
    unit_line = ''
    city_state_zip_line = ''

    # Unit/suite patterns
    unit_patterns = re.compile(r'^(STE|SUITE|APT|APARTMENT|UNIT|#|FL|FLOOR|RM|ROOM|BLDG|BUILDING)\s*\.?\s*\d*', re.IGNORECASE)

    for line in lines:
        # Check if this is a city, state, zip line
        if re.match(r'^.+,\s*[A-Z]{2}\s+\d{5}', line, re.IGNORECASE):
            city_state_zip_line = line
        elif re.match(r'^.+,\s*[A-Z]{2}\s*$', line, re.IGNORECASE):
            city_state_zip_line = line
        # Check if this is a unit/suite line
        elif unit_patterns.match(line):
            unit_line = line
        # Otherwise it's likely the street line (take the first one)
        elif not street_line:
            street_line = line

    # Parse street address
    if street_line:
        # Extract street number (leading digits)
        match = re.match(r'^(\d+[-\d]*)\s+(.+)$', street_line)
        if match:
            result['street_number'] = match.group(1)
            remainder = match.group(2)
        else:
            remainder = street_line

        # Parse the remainder for street name, type, and direction
        words = remainder.split()

        # Check last word for direction
        if words and words[-1].lower() in DIRECTIONS:
            result['street_direction'] = words[-1].upper()
            words = words[:-1]

        # Check for street type
        if words:
            if words[-1].lower() in STREET_TYPES:
                result['street_type'] = words[-1]
                words = words[:-1]

        # Remaining words are the street name
        if words:
            result['street_name'] = ' '.join(words)

    # Store unit/suite
    if unit_line:
        result['unit'] = unit_line

    # Parse city, state, zip
    if city_state_zip_line:
        # Pattern: "City, ST 12345" or "City, ST 12345-6789" (handle en-dash too)
        match = re.match(r'^(.+?),\s*([A-Z]{2})\s+([\d\-–]+)$', city_state_zip_line, re.IGNORECASE)
        if match:
            result['city'] = match.group(1).strip()
            result['state'] = match.group(2).upper()
            # Normalize dash in zip
            result['zip'] = match.group(3).replace('–', '-')
        else:
            # Try without zip
            match = re.match(r'^(.+?),\s*([A-Z]{2})$', city_state_zip_line, re.IGNORECASE)
            if match:
                result['city'] = match.group(1).strip()
                result['state'] = match.group(2).upper()
            else:
                result['city'] = city_state_zip_line

    return result


class MNBusinessScraper:
    """Scraper for Minnesota Secretary of State business filings."""

    def __init__(self, start_number: int = None, headless: bool = None):
        self.start_number = start_number or config.START_FILE_NUMBER
        self.headless = headless if headless is not None else config.HEADLESS
        self.consecutive_misses = 0
        self.current_file_number = self.start_number
        self.browser = None
        self.context = None
        self.page = None

        # Setup output directory
        self.output_dir = Path(config.OUTPUT_DIR)
        self.output_dir.mkdir(exist_ok=True)
        self.output_file = self.output_dir / config.OUTPUT_FILE
        self.progress_file = Path(config.PROGRESS_FILE)

        # CSV columns
        self.columns = [
            'file_number', 'business_name', 'mn_statute', 'business_type',
            'home_jurisdiction', 'filing_date', 'status', 'renewal_due_date',
            'mark_type',  # For trademarks
            'number_of_shares',  # For corporations
            'chief_executive_officer',  # For corporations
            'manager',  # For LLCs
            # Principal Place of Business Address components (Assumed Names)
            'principal_street_number', 'principal_street_name', 'principal_street_type',
            'principal_street_direction', 'principal_unit', 'principal_city', 'principal_state', 'principal_zip',
            'principal_address_raw',
            # Registered Office Address (for corporations)
            'reg_office_street_number', 'reg_office_street_name', 'reg_office_street_type',
            'reg_office_street_direction', 'reg_office_unit', 'reg_office_city', 'reg_office_state', 'reg_office_zip',
            'reg_office_address_raw',
            # Principal Executive Office Address (for corporations)
            'exec_office_street_number', 'exec_office_street_name', 'exec_office_street_type',
            'exec_office_street_direction', 'exec_office_unit', 'exec_office_city', 'exec_office_state', 'exec_office_zip',
            'exec_office_address_raw',
            # Applicant/Markholder info (unified field names)
            'applicant_name',  # Also used for Markholder
            # Applicant/Markholder Address components
            'applicant_street_number', 'applicant_street_name', 'applicant_street_type',
            'applicant_street_direction', 'applicant_unit', 'applicant_city', 'applicant_state', 'applicant_zip',
            'applicant_address_raw',
            # Additional fields
            'registered_agent_name', 'filing_history', 'scraped_at'
        ]

    async def initialize(self):
        """Initialize Playwright browser."""
        logger.info("Initializing browser...")
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(headless=self.headless)
        self.context = await self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        self.page = await self.context.new_page()
        self.page.set_default_timeout(config.TIMEOUT)
        logger.info("Browser initialized")

    async def close(self):
        """Close browser and cleanup."""
        if self.browser:
            await self.browser.close()
            logger.info("Browser closed")

    def load_progress(self) -> int:
        """Load last scraped file number from progress file."""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                    return data.get('last_file_number', self.start_number)
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Could not load progress file: {e}")
        return self.start_number

    def save_progress(self, file_number: int):
        """Save current progress to file."""
        with open(self.progress_file, 'w') as f:
            json.dump({
                'last_file_number': file_number,
                'updated_at': datetime.now().isoformat()
            }, f, indent=2)

    def init_csv(self):
        """Initialize CSV file with headers if it doesn't exist."""
        if not self.output_file.exists():
            df = pd.DataFrame(columns=self.columns)
            df.to_csv(self.output_file, index=False)
            logger.info(f"Created output file: {self.output_file}")

    def append_to_csv(self, data: dict):
        """Append a single record to CSV."""
        df = pd.DataFrame([data])
        df.to_csv(self.output_file, mode='a', header=False, index=False)

    async def search_by_file_number(self, file_number: int) -> tuple[bool, str]:
        """
        Search for a business by file number.
        Returns (found: bool, business_name: str).
        """
        try:
            # Navigate to search page
            await self.page.goto(config.BASE_URL, wait_until='networkidle')

            # Click the "File Number" tab to reveal the file number input
            file_number_tab = await self.page.wait_for_selector('a[href="#fileNumberTab"]', timeout=10000)
            await file_number_tab.click()
            await asyncio.sleep(0.3)  # Brief wait for tab switch

            # Wait for the FileNumber input to be visible
            await self.page.wait_for_selector('#FileNumber:visible', timeout=5000)

            # Clear and enter file number
            await self.page.fill('#FileNumber', str(file_number))

            # Click the search button within the file number tab
            await self.page.click('#fileNumberTab button[type="submit"]')

            # Wait for results
            await self.page.wait_for_load_state('networkidle')
            await asyncio.sleep(0.5)  # Brief wait for dynamic content

            # Check for "no results" messages
            page_text = await self.page.inner_text('body')
            if 'no results' in page_text.lower() or 'no businesses found' in page_text.lower():
                return False, ''

            # Get business name from the strong tag in results
            name_element = await self.page.query_selector('table tbody tr td strong')
            business_name = ''
            if name_element:
                business_name = (await name_element.inner_text()).strip()

            # Click the "Details" link to go to the business details page
            details_link = await self.page.query_selector('a[href*="SearchDetails"]')
            if details_link:
                await details_link.click()
                await self.page.wait_for_load_state('networkidle')
                return True, business_name

            # Check if we're already on a details page (direct navigation)
            current_url = self.page.url
            if 'SearchDetails' in current_url or 'Details' in current_url:
                return True, business_name

            return False, ''

        except PlaywrightTimeout:
            logger.warning(f"Timeout searching for file number {file_number}")
            return False, ''
        except Exception as e:
            logger.error(f"Error searching file number {file_number}: {e}")
            return False, ''

    async def extract_text(self, selector: str, default: str = '') -> str:
        """Extract text from element, return default if not found."""
        try:
            element = await self.page.query_selector(selector)
            if element:
                text = await element.inner_text()
                return text.strip()
        except Exception:
            pass
        return default

    async def extract_business_data(self, file_number: int, business_name: str = '') -> dict:
        """Extract all business data from the current details page."""
        data = {
            'file_number': file_number,
            'business_name': business_name,
            'mn_statute': '',
            'business_type': '',
            'home_jurisdiction': '',
            'filing_date': '',
            'status': '',
            'renewal_due_date': '',
            'mark_type': '',  # For trademarks
            'number_of_shares': '',  # For corporations
            'chief_executive_officer': '',  # For corporations
            'manager': '',  # For LLCs
            # Principal Place of Business address components (Assumed Names)
            'principal_street_number': '',
            'principal_street_name': '',
            'principal_street_type': '',
            'principal_street_direction': '',
            'principal_unit': '',
            'principal_city': '',
            'principal_state': '',
            'principal_zip': '',
            'principal_address_raw': '',
            # Registered Office Address (for corporations)
            'reg_office_street_number': '',
            'reg_office_street_name': '',
            'reg_office_street_type': '',
            'reg_office_street_direction': '',
            'reg_office_unit': '',
            'reg_office_city': '',
            'reg_office_state': '',
            'reg_office_zip': '',
            'reg_office_address_raw': '',
            # Principal Executive Office Address (for corporations)
            'exec_office_street_number': '',
            'exec_office_street_name': '',
            'exec_office_street_type': '',
            'exec_office_street_direction': '',
            'exec_office_unit': '',
            'exec_office_city': '',
            'exec_office_state': '',
            'exec_office_zip': '',
            'exec_office_address_raw': '',
            # Applicant/Markholder info
            'applicant_name': '',
            # Applicant/Markholder address components
            'applicant_street_number': '',
            'applicant_street_name': '',
            'applicant_street_type': '',
            'applicant_street_direction': '',
            'applicant_unit': '',
            'applicant_city': '',
            'applicant_state': '',
            'applicant_zip': '',
            'applicant_address_raw': '',
            # Additional fields
            'registered_agent_name': '',
            'filing_history': '',
            'scraped_at': datetime.now().strftime('%Y-%m-%d')
        }

        try:
            # Map dt labels to our data fields
            label_mapping = {
                'business type': 'business_type',
                'mn statute': 'mn_statute',
                'home jurisdiction': 'home_jurisdiction',
                'filing date': 'filing_date',
                'date of incorporation': 'filing_date',
                'status': 'status',
                'renewal due date': 'renewal_due_date',
                'mark type': 'mark_type',  # For trademarks
                'number of shares': 'number_of_shares',  # For corporations
                'chief executive officer': 'chief_executive_officer',  # For corporations
                'manager': 'manager',  # For LLCs
                'registered agent': 'registered_agent_name',
                'registered agent(s)': 'registered_agent_name',
            }

            # Extract data using DT/DD pairs (the page's actual structure)
            principal_address_raw = ''
            reg_office_address_raw = ''
            exec_office_address_raw = ''
            dts = await self.page.query_selector_all('dt')
            for dt in dts:
                try:
                    label = (await dt.inner_text()).strip().lower()
                    dd_text = await dt.evaluate('el => el.nextElementSibling?.innerText || ""')
                    dd_text = dd_text.strip()

                    # Check for principal place of business address (Assumed Names)
                    if 'principal place of business' in label and 'address' in label:
                        principal_address_raw = dd_text
                        data['principal_address_raw'] = dd_text
                        continue

                    # Check for principal executive office address (Corporations)
                    if 'principal executive office' in label and 'address' in label:
                        exec_office_address_raw = dd_text
                        data['exec_office_address_raw'] = dd_text
                        continue

                    # Check for registered office address (Corporations)
                    if 'registered office' in label and 'address' in label:
                        reg_office_address_raw = dd_text
                        data['reg_office_address_raw'] = dd_text
                        continue

                    # Map other fields
                    for key, field in label_mapping.items():
                        if key in label and dd_text:
                            if not data[field]:
                                data[field] = dd_text
                            break
                except Exception:
                    continue

            # Parse principal place of business address into components
            if principal_address_raw:
                parsed = parse_address(principal_address_raw)
                data['principal_street_number'] = parsed['street_number']
                data['principal_street_name'] = parsed['street_name']
                data['principal_street_type'] = parsed['street_type']
                data['principal_street_direction'] = parsed['street_direction']
                data['principal_unit'] = parsed['unit']
                data['principal_city'] = parsed['city']
                data['principal_state'] = parsed['state']
                data['principal_zip'] = parsed['zip']

            # Parse registered office address into components
            if reg_office_address_raw:
                parsed = parse_address(reg_office_address_raw)
                data['reg_office_street_number'] = parsed['street_number']
                data['reg_office_street_name'] = parsed['street_name']
                data['reg_office_street_type'] = parsed['street_type']
                data['reg_office_street_direction'] = parsed['street_direction']
                data['reg_office_unit'] = parsed['unit']
                data['reg_office_city'] = parsed['city']
                data['reg_office_state'] = parsed['state']
                data['reg_office_zip'] = parsed['zip']

            # Parse principal executive office address into components
            if exec_office_address_raw:
                parsed = parse_address(exec_office_address_raw)
                data['exec_office_street_number'] = parsed['street_number']
                data['exec_office_street_name'] = parsed['street_name']
                data['exec_office_street_type'] = parsed['street_type']
                data['exec_office_street_direction'] = parsed['street_direction']
                data['exec_office_unit'] = parsed['unit']
                data['exec_office_city'] = parsed['city']
                data['exec_office_state'] = parsed['state']
                data['exec_office_zip'] = parsed['zip']

            # Extract applicant/markholder info from table
            # Handles both "Applicant | Applicant Address" and "Markholder | Markholder Address"
            tables = await self.page.query_selector_all('table')
            for table in tables:
                headers = await table.query_selector_all('th')
                header_texts = [(await h.inner_text()).strip().lower() for h in headers]

                # Check if this is the applicant or markholder table
                is_applicant_table = any('applicant' in h for h in header_texts)
                is_markholder_table = any('markholder' in h for h in header_texts)

                if is_applicant_table or is_markholder_table:
                    rows = await table.query_selector_all('tbody tr')
                    if rows:
                        cells = await rows[0].query_selector_all('td')
                        if len(cells) >= 2:
                            # First cell: Applicant/Markholder name
                            data['applicant_name'] = (await cells[0].inner_text()).strip()
                            # Second cell: Applicant/Markholder address
                            applicant_addr_raw = (await cells[1].inner_text()).strip()
                            data['applicant_address_raw'] = applicant_addr_raw

                            # Parse applicant/markholder address
                            parsed = parse_address(applicant_addr_raw)
                            data['applicant_street_number'] = parsed['street_number']
                            data['applicant_street_name'] = parsed['street_name']
                            data['applicant_street_type'] = parsed['street_type']
                            data['applicant_street_direction'] = parsed['street_direction']
                            data['applicant_unit'] = parsed['unit']
                            data['applicant_city'] = parsed['city']
                            data['applicant_state'] = parsed['state']
                            data['applicant_zip'] = parsed['zip']
                    break

            # Extract filing history from the filing history table
            filing_history = []
            for table in tables:
                headers = await table.query_selector_all('th')
                header_texts = [(await h.inner_text()).strip().lower() for h in headers]

                # Check if this is the filing history table (has "Filing Date" or "Filing" header)
                if any('filing' in h for h in header_texts) and not any('applicant' in h for h in header_texts):
                    rows = await table.query_selector_all('tbody tr')
                    for row in rows:
                        try:
                            cells = await row.query_selector_all('td')
                            if cells:
                                row_data = []
                                for cell in cells:
                                    text = (await cell.inner_text()).strip()
                                    if text:
                                        row_data.append(text)
                                if row_data:
                                    filing_history.append(' | '.join(row_data))
                        except Exception:
                            continue
                    break

            if filing_history:
                data['filing_history'] = ' ;; '.join(filing_history[:20])

            # Convert dates to YYYY-MM-DD format
            if data.get('filing_date'):
                data['filing_date'] = convert_date_to_iso(data['filing_date'])
            if data.get('renewal_due_date'):
                data['renewal_due_date'] = convert_date_to_iso(data['renewal_due_date'])

        except Exception as e:
            logger.error(f"Error extracting data for file {file_number}: {e}")

        return data

    async def scrape_business(self, file_number: int) -> dict | None:
        """
        Scrape a single business by file number.
        Returns business data dict or None if not found.
        """
        for attempt in range(config.MAX_RETRIES):
            try:
                found, business_name = await self.search_by_file_number(file_number)

                if not found:
                    return None

                data = await self.extract_business_data(file_number, business_name)
                return data

            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for file {file_number}: {e}")
                if attempt < config.MAX_RETRIES - 1:
                    await asyncio.sleep(config.RETRY_DELAY)
                else:
                    logger.error(f"All retries failed for file {file_number}")
                    return None

        return None

    async def scrape_business_by_guid(self, guid: str) -> dict | None:
        """
        Scrape a single business by GUID (for search results).
        Returns business data dict or None if not found.
        """
        for attempt in range(config.MAX_RETRIES):
            try:
                # Navigate directly to the business details page
                url = f'https://mblsportal.sos.mn.gov/Business/SearchDetails?filingGuid={guid}'
                await self.page.goto(url, wait_until='networkidle')
                await asyncio.sleep(0.5)

                # Check for valid page
                title = await self.page.title()
                if 'Details' not in title:
                    return None

                # Get business name from h2
                business_name = ''
                h2 = await self.page.query_selector('h2')
                if h2:
                    business_name = (await h2.inner_text()).strip()

                # Extract data using the existing method (use GUID as file_number)
                data = await self.extract_business_data(guid, business_name)
                return data

            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for GUID {guid}: {e}")
                if attempt < config.MAX_RETRIES - 1:
                    await asyncio.sleep(config.RETRY_DELAY)
                else:
                    logger.error(f"All retries failed for GUID {guid}")
                    return None

        return None

    async def add_delay(self):
        """Add rate-limiting delay with jitter."""
        delay = config.REQUEST_DELAY + random.uniform(0, config.DELAY_JITTER)
        await asyncio.sleep(delay)

    async def run(self, resume: bool = True):
        """
        Main scraping loop.

        Args:
            resume: If True, resume from last progress; otherwise start fresh
        """
        try:
            await self.initialize()
            self.init_csv()

            # Determine starting point
            if resume:
                saved_progress = self.load_progress()
                if saved_progress > self.start_number:
                    self.current_file_number = saved_progress + 1
                    logger.info(f"Resuming from file number {self.current_file_number}")
                else:
                    self.current_file_number = self.start_number
            else:
                self.current_file_number = self.start_number

            logger.info(f"Starting scrape from file number {self.current_file_number}")
            logger.info(f"Will stop after {config.MAX_CONSECUTIVE_MISSES} consecutive misses")

            scraped_count = 0

            while self.consecutive_misses < config.MAX_CONSECUTIVE_MISSES:
                file_number = self.current_file_number

                logger.info(f"Scraping file number {file_number}...")

                data = await self.scrape_business(file_number)

                if data:
                    self.append_to_csv(data)
                    scraped_count += 1
                    self.consecutive_misses = 0
                    logger.info(f"[FOUND] {data.get('business_name', 'Unknown')} (#{file_number})")
                else:
                    self.consecutive_misses += 1
                    logger.debug(f"[MISS] No result for file number {file_number} ({self.consecutive_misses} consecutive misses)")

                # Save progress periodically
                if file_number % 10 == 0:
                    self.save_progress(file_number)
                    logger.info(f"Progress: {scraped_count} businesses scraped, at file #{file_number}")

                self.current_file_number += 1
                await self.add_delay()

            logger.info(f"Stopping: {config.MAX_CONSECUTIVE_MISSES} consecutive misses reached")
            logger.info(f"Total businesses scraped: {scraped_count}")
            self.save_progress(self.current_file_number - 1)

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            self.save_progress(self.current_file_number - 1)
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            self.save_progress(self.current_file_number - 1)
            raise
        finally:
            await self.close()


def main():
    parser = argparse.ArgumentParser(description='Minnesota Business Scraper')
    parser.add_argument('--start', type=int, help='Starting file number')
    parser.add_argument('--no-resume', action='store_true', help='Start fresh, ignore saved progress')
    parser.add_argument('--visible', action='store_true', help='Run browser in visible mode (not headless)')
    args = parser.parse_args()

    scraper = MNBusinessScraper(
        start_number=args.start,
        headless=not args.visible
    )

    asyncio.run(scraper.run(resume=not args.no_resume))


if __name__ == '__main__':
    main()
