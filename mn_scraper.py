#!/usr/bin/env python3
"""
===============================================================================
MINNESOTA BUSINESS SCRAPER - CORE MODULE
===============================================================================

This module provides the core scraping functionality for the Minnesota Secretary
of State business portal. It handles browser automation, data extraction, and
CSV file management.

PURPOSE:
--------
This scraper retrieves business registration data from the MN SOS portal at:
https://mblsportal.sos.mn.gov/Business/Search

It can search businesses by:
1. File Number - Sequential numeric identifiers assigned to each filing
2. GUID - Unique identifiers for direct detail page access

DATA COLLECTED:
---------------
For each business, the scraper extracts:
- Basic info: file number, name, type, status, filing date
- Address info: registered office, principal executive office, principal place
- Officer info: CEO, manager, registered agent
- Filing history: list of all filings for the business

DEPENDENCIES:
-------------
- playwright: Browser automation library (async)
- pandas: Data manipulation and CSV handling
- asyncio: Async/await support for concurrent operations

USAGE:
------
    # As a standalone script:
    python mn_scraper.py --start 1000000 --visible

    # As an imported module:
    from mn_scraper import MNBusinessScraper
    scraper = MNBusinessScraper(start_number=1000000)
    await scraper.run()

FILE OUTPUTS:
-------------
- output/businesses.csv: Main output file with all scraped records
- progress.json: Tracks last scraped file number for resume capability
- scraper.log: Log file with scraping activity

FOR JUNIOR DEVELOPERS:
----------------------
Key concepts used in this code:
1. ASYNC/AWAIT: This code uses asynchronous programming. Functions marked with
   'async def' can pause execution (await) while waiting for slow operations
   like network requests, allowing other code to run in the meantime.

2. CLASS-BASED DESIGN: The MNBusinessScraper class encapsulates all scraping
   logic. This makes the code organized and reusable.

3. PLAYWRIGHT: A browser automation library that controls a real Chrome browser.
   This is needed because the website uses JavaScript to render content.

4. PANDAS: A data analysis library. Here we use it for CSV file operations.

5. REGULAR EXPRESSIONS (regex): Used for parsing addresses and dates.
   The 're' module provides pattern matching capabilities.

===============================================================================
Author: MN Business Data Team
Last Updated: 2026-02-02
===============================================================================
"""

# =============================================================================
# IMPORTS - External Libraries and Modules
# =============================================================================

import argparse          # For parsing command-line arguments
import asyncio           # For async/await functionality
import json              # For reading/writing JSON files (progress tracking)
import logging           # For logging messages to console and file
import re                # For regular expressions (pattern matching)
import random            # For adding random delays (to be polite to server)
from datetime import datetime  # For date/time operations
from pathlib import Path       # For cross-platform file path handling

# Third-party libraries (must be installed via pip)
import pandas as pd      # Data manipulation library
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# Local module imports
import config  # Configuration settings (BASE_URL, timeouts, etc.)


# =============================================================================
# LOGGING SETUP
# =============================================================================
# Logging helps you track what the program is doing and debug issues.
# We log to both the console (so you can see what's happening in real-time)
# and to a file (so you can review later).

logging.basicConfig(
    level=logging.INFO,  # Show INFO level and above (INFO, WARNING, ERROR)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Timestamp - Level - Message
    handlers=[
        logging.StreamHandler(),              # Prints to console
        logging.FileHandler('scraper.log')    # Writes to file
    ]
)
logger = logging.getLogger(__name__)  # Get a logger for this module


# =============================================================================
# CONSTANTS - Address Parsing Reference Data
# =============================================================================
# These sets contain common street types and directions used for parsing
# addresses. When we parse "123 Main Street NE", we need to identify each part.

# Common street types and their abbreviations
# Used to identify the type of street in an address (e.g., "Street", "Ave")
STREET_TYPES = {
    'street', 'st', 'str',           # Street variations
    'avenue', 'ave', 'av',            # Avenue variations
    'road', 'rd',                     # Road
    'drive', 'dr',                    # Drive
    'lane', 'ln',                     # Lane
    'court', 'ct',                    # Court
    'circle', 'cir',                  # Circle
    'boulevard', 'blvd',              # Boulevard
    'way',                            # Way
    'place', 'pl',                    # Place
    'trail', 'trl', 'tr',             # Trail
    'parkway', 'pkwy',                # Parkway
    'highway', 'hwy',                 # Highway
    'terrace', 'ter',                 # Terrace
    'path',                           # Path
    'loop',                           # Loop
    'square', 'sq',                   # Square
    'crossing', 'xing'                # Crossing
}

# Directional suffixes/prefixes (e.g., "123 Main St NE")
# These indicate which part of the city the address is in
DIRECTIONS = {
    'n', 's', 'e', 'w',               # Single letter: North, South, East, West
    'ne', 'nw', 'se', 'sw',           # Combined: Northeast, Northwest, etc.
    'north', 'south', 'east', 'west'  # Full words
}


# =============================================================================
# HELPER FUNCTIONS - Date and Address Parsing
# =============================================================================

def convert_date_to_iso(date_str: str) -> str:
    """
    Convert a date from MM/DD/YYYY format to YYYY-MM-DD format.

    WHY ISO FORMAT?
    ---------------
    ISO 8601 format (YYYY-MM-DD) is the international standard for dates.
    It's better for:
    1. Sorting: "2024-01-15" sorts correctly as text
    2. Database storage: Most databases prefer this format
    3. Programming: Python's datetime uses this natively

    EXAMPLES:
    ---------
    >>> convert_date_to_iso("01/15/2024")
    '2024-01-15'

    >>> convert_date_to_iso("12/31/2023")
    '2023-12-31'

    >>> convert_date_to_iso("")
    ''

    >>> convert_date_to_iso("2024-01-15")  # Already ISO format
    '2024-01-15'

    PARAMETERS:
    -----------
    date_str : str
        The date string to convert. Expected format: "MM/DD/YYYY"
        Examples: "01/15/2024", "12/31/2023", "1/5/2024"

    RETURNS:
    --------
    str
        The date in ISO format (YYYY-MM-DD) or empty string if invalid.
        If already in ISO format, returns as-is (first 10 characters).
    """
    # Handle empty or whitespace-only input
    if not date_str or not date_str.strip():
        return ''

    # Remove leading/trailing whitespace
    date_str = date_str.strip()

    # Try parsing as MM/DD/YYYY format
    # strptime = "string parse time" - converts string to datetime object
    try:
        dt = datetime.strptime(date_str, '%m/%d/%Y')
        # strftime = "string format time" - converts datetime to string
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        # ValueError means the string didn't match the expected format
        pass

    # Try M/D/YYYY format (single digit month/day like "1/5/2024")
    # Note: This is actually the same as above - Python's %m/%d handles both
    try:
        dt = datetime.strptime(date_str, '%m/%d/%Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        pass

    # Check if already in YYYY-MM-DD format (ISO format)
    # Regular expression explanation:
    # ^       = start of string
    # \d{4}   = exactly 4 digits (year)
    # -       = literal hyphen
    # \d{2}   = exactly 2 digits (month)
    # -       = literal hyphen
    # \d{2}   = exactly 2 digits (day)
    if re.match(r'^\d{4}-\d{2}-\d{2}', date_str):
        return date_str[:10]  # Return first 10 chars (in case there's extra)

    # If we couldn't parse it, return the original string
    # This prevents data loss even if the format is unexpected
    return date_str


def parse_address(address_str: str) -> dict:
    """
    Parse a full address string into its component parts.

    WHY PARSE ADDRESSES?
    --------------------
    Raw addresses like "123 Main St NE\nSte 200\nMinneapolis, MN 55401" are
    hard to search, filter, and analyze. Breaking them into components allows:
    1. Search by city, state, or zip
    2. Filter by street name
    3. Standardize address formats
    4. Detect duplicates with slightly different formatting

    EXAMPLES:
    ---------
    >>> parse_address("123 Main Street NE\\nMinneapolis, MN 55401")
    {
        'street_number': '123',
        'street_name': 'Main',
        'street_type': 'Street',
        'street_direction': 'NE',
        'unit': '',
        'city': 'Minneapolis',
        'state': 'MN',
        'zip': '55401'
    }

    >>> parse_address("456 Oak Ave\\nSte 100\\nSt Paul, MN 55102")
    {
        'street_number': '456',
        'street_name': 'Oak',
        'street_type': 'Ave',
        'street_direction': '',
        'unit': 'Ste 100',
        'city': 'St Paul',
        'state': 'MN',
        'zip': '55102'
    }

    PARAMETERS:
    -----------
    address_str : str
        The full address string to parse. Can be multi-line (\\n separated)
        or comma-separated.

    RETURNS:
    --------
    dict
        Dictionary with keys:
        - street_number: The house/building number (e.g., "123")
        - street_name: The street name without type/direction (e.g., "Main")
        - street_type: The street type (e.g., "Street", "Ave")
        - street_direction: Directional suffix (e.g., "NE", "SW")
        - unit: Suite/apartment/floor (e.g., "Ste 200")
        - city: City name (e.g., "Minneapolis")
        - state: Two-letter state code (e.g., "MN")
        - zip: ZIP code (e.g., "55401" or "55401-1234")
    """
    # Initialize result dictionary with empty values
    # This ensures all keys exist even if we can't parse some parts
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

    # Handle empty input
    if not address_str:
        return result

    # Remove leading/trailing whitespace
    address_str = address_str.strip()

    # =======================================================================
    # STEP 1: Split multi-line addresses into separate lines
    # =======================================================================
    # Addresses often come as:
    # "123 Main St
    #  Ste 200
    #  Minneapolis, MN 55401
    #  USA"

    lines = [line.strip() for line in address_str.split('\n') if line.strip()]

    # Remove country line (we only care about US addresses)
    lines = [l for l in lines if l.upper() not in ['USA', 'US', 'UNITED STATES']]

    # =======================================================================
    # STEP 2: Handle comma-separated single-line format
    # =======================================================================
    # Some addresses are like: "123 Main St, Minneapolis, MN 55401"
    # We need to split these into street line + city/state/zip line

    if len(lines) == 1 and ',' in lines[0]:
        parts = lines[0].split(',')
        if len(parts) >= 2:
            # First part is the street address
            lines = [parts[0].strip()]
            # Rest is city, state, zip (rejoin in case city has comma)
            remainder = ','.join(parts[1:]).strip()
            lines.append(remainder)

    # =======================================================================
    # STEP 3: Identify which line is which (street, unit, city/state/zip)
    # =======================================================================

    street_line = ''       # Main street address (e.g., "123 Main St NE")
    unit_line = ''         # Suite/apartment (e.g., "Ste 200")
    city_state_zip_line = ''  # City, state, zip (e.g., "Minneapolis, MN 55401")

    # Pattern to match unit/suite lines
    # Matches: STE, SUITE, APT, UNIT, #, FL, FLOOR, RM, ROOM, BLDG, BUILDING
    # followed by optional number
    unit_patterns = re.compile(
        r'^(STE|SUITE|APT|APARTMENT|UNIT|#|FL|FLOOR|RM|ROOM|BLDG|BUILDING)\s*\.?\s*\d*',
        re.IGNORECASE  # Case-insensitive matching
    )

    for line in lines:
        # Check if this looks like "City, ST 12345" (city, state, zip)
        if re.match(r'^.+,\s*[A-Z]{2}\s+\d{5}', line, re.IGNORECASE):
            city_state_zip_line = line
        # Check if this is just "City, ST" without zip
        elif re.match(r'^.+,\s*[A-Z]{2}\s*$', line, re.IGNORECASE):
            city_state_zip_line = line
        # Check if this is a unit/suite line
        elif unit_patterns.match(line):
            unit_line = line
        # Otherwise it's probably the street line (take first one found)
        elif not street_line:
            street_line = line

    # =======================================================================
    # STEP 4: Parse the street address
    # =======================================================================

    if street_line:
        # Extract street number (leading digits, possibly with hyphen like "123-125")
        # Regular expression: ^ = start, (\d+[-\d]*) = digits with optional hyphen+digits
        match = re.match(r'^(\d+[-\d]*)\s+(.+)$', street_line)
        if match:
            result['street_number'] = match.group(1)  # The number
            remainder = match.group(2)                 # Rest of street address
        else:
            # No number found, entire line is the "remainder"
            remainder = street_line

        # Split remainder into words for further parsing
        words = remainder.split()

        # Check if last word is a direction (N, S, E, W, NE, etc.)
        if words and words[-1].lower() in DIRECTIONS:
            result['street_direction'] = words[-1].upper()
            words = words[:-1]  # Remove direction from words list

        # Check if last word is a street type (St, Ave, Rd, etc.)
        if words:
            if words[-1].lower() in STREET_TYPES:
                result['street_type'] = words[-1]
                words = words[:-1]  # Remove street type from words list

        # Whatever's left is the street name
        if words:
            result['street_name'] = ' '.join(words)

    # Store the unit/suite line as-is
    if unit_line:
        result['unit'] = unit_line

    # =======================================================================
    # STEP 5: Parse city, state, and ZIP
    # =======================================================================

    if city_state_zip_line:
        # Try to match: "City Name, ST 12345" or "City Name, ST 12345-6789"
        # The – is an en-dash that sometimes appears in ZIP codes
        match = re.match(
            r'^(.+?),\s*([A-Z]{2})\s+([\d\-–]+)$',
            city_state_zip_line,
            re.IGNORECASE
        )
        if match:
            result['city'] = match.group(1).strip()
            result['state'] = match.group(2).upper()
            # Normalize en-dash to regular hyphen in ZIP
            result['zip'] = match.group(3).replace('–', '-')
        else:
            # Try without ZIP code: "City Name, ST"
            match = re.match(
                r'^(.+?),\s*([A-Z]{2})$',
                city_state_zip_line,
                re.IGNORECASE
            )
            if match:
                result['city'] = match.group(1).strip()
                result['state'] = match.group(2).upper()
            else:
                # Couldn't parse - just store as city
                result['city'] = city_state_zip_line

    return result


# =============================================================================
# MAIN SCRAPER CLASS
# =============================================================================

class MNBusinessScraper:
    """
    Scraper for Minnesota Secretary of State business filings.

    This class handles all aspects of scraping business data:
    1. Browser management (start/stop Chrome via Playwright)
    2. Navigation and search
    3. Data extraction from business detail pages
    4. Progress tracking for resume capability
    5. CSV file management

    USAGE:
    ------
    # Basic usage with async/await:
    scraper = MNBusinessScraper(start_number=1000000)
    await scraper.initialize()  # Start browser

    # Scrape a single business
    data = await scraper.scrape_business(1000000)

    # Run the main loop (scrapes sequentially until max misses)
    await scraper.run()

    # Always close when done
    await scraper.close()

    ATTRIBUTES:
    -----------
    start_number : int
        The file number to start scraping from
    headless : bool
        Whether to run browser without visible window
    consecutive_misses : int
        Counter for consecutive failed searches (no results)
    current_file_number : int
        The file number currently being processed
    browser, context, page : Playwright objects
        Browser automation handles
    output_dir : Path
        Directory for output files
    output_file : Path
        Path to the main CSV output file
    progress_file : Path
        Path to the JSON progress tracking file
    columns : list
        List of column names for the CSV file
    """

    def __init__(self, start_number: int = None, headless: bool = None):
        """
        Initialize the scraper with configuration.

        PARAMETERS:
        -----------
        start_number : int, optional
            The file number to start from. If None, uses config.START_FILE_NUMBER.
            File numbers are sequential identifiers assigned by MN SOS.

        headless : bool, optional
            Whether to run the browser without a visible window.
            - True: Browser runs in background (faster, uses less resources)
            - False: Browser window is visible (useful for debugging)
            If None, uses config.HEADLESS setting.

        EXAMPLE:
        --------
        # Start from file 1000000 with visible browser
        scraper = MNBusinessScraper(start_number=1000000, headless=False)

        # Use config defaults
        scraper = MNBusinessScraper()
        """
        # Use provided values or fall back to config defaults
        self.start_number = start_number or config.START_FILE_NUMBER
        self.headless = headless if headless is not None else config.HEADLESS

        # Tracking variables
        self.consecutive_misses = 0       # How many searches in a row found nothing
        self.current_file_number = self.start_number  # Current position

        # Playwright browser objects (initialized in initialize())
        self.browser = None   # The browser instance
        self.context = None   # Browser context (like an incognito session)
        self.page = None      # The active page/tab

        # =================================================================
        # FILE PATH SETUP
        # =================================================================
        # Path objects handle cross-platform path differences automatically
        # (e.g., forward slash vs backslash)

        self.output_dir = Path(config.OUTPUT_DIR)
        self.output_dir.mkdir(exist_ok=True)  # Create if doesn't exist
        self.output_file = self.output_dir / config.OUTPUT_FILE
        self.progress_file = Path(config.PROGRESS_FILE)

        # =================================================================
        # CSV COLUMN DEFINITIONS
        # =================================================================
        # These define all the fields we extract for each business.
        # The order here determines the column order in the CSV.

        self.columns = [
            # Basic business information
            'file_number',           # MN SOS assigned number
            'business_name',         # Official registered name
            'mn_statute',            # Minnesota statute governing this entity
            'business_type',         # LLC, Corporation, etc.
            'home_jurisdiction',     # Where the business is incorporated
            'filing_date',           # When first filed with MN SOS
            'status',                # Active, Inactive, Dissolved, etc.
            'renewal_due_date',      # When next renewal is due

            # Type-specific fields
            'mark_type',             # For trademarks only
            'number_of_shares',      # For corporations
            'chief_executive_officer',  # For corporations
            'manager',               # For LLCs

            # Principal Place of Business Address (for Assumed Names)
            # Broken into components for easy searching/filtering
            'principal_street_number',
            'principal_street_name',
            'principal_street_type',
            'principal_street_direction',
            'principal_unit',
            'principal_city',
            'principal_state',
            'principal_zip',
            'principal_address_raw',  # Original unparsed address

            # Registered Office Address (for corporations)
            'reg_office_street_number',
            'reg_office_street_name',
            'reg_office_street_type',
            'reg_office_street_direction',
            'reg_office_unit',
            'reg_office_city',
            'reg_office_state',
            'reg_office_zip',
            'reg_office_address_raw',

            # Principal Executive Office Address (for corporations)
            'exec_office_street_number',
            'exec_office_street_name',
            'exec_office_street_type',
            'exec_office_street_direction',
            'exec_office_unit',
            'exec_office_city',
            'exec_office_state',
            'exec_office_zip',
            'exec_office_address_raw',

            # Applicant/Markholder information
            'applicant_name',        # Person/entity that filed
            'applicant_street_number',
            'applicant_street_name',
            'applicant_street_type',
            'applicant_street_direction',
            'applicant_unit',
            'applicant_city',
            'applicant_state',
            'applicant_zip',
            'applicant_address_raw',

            # Additional fields
            'registered_agent_name',  # Person designated to receive legal docs
            'filing_history',         # List of all filings (separated by ;;)
            'scraped_at'             # When we scraped this record
        ]

    # =========================================================================
    # BROWSER LIFECYCLE METHODS
    # =========================================================================

    async def initialize(self):
        """
        Initialize the Playwright browser.

        This starts a Chromium browser that we'll use to navigate the website.
        We use Playwright because the MN SOS website uses JavaScript to render
        content, so simple HTTP requests won't work.

        WHAT THIS DOES:
        ---------------
        1. Starts the Playwright engine
        2. Launches a Chromium browser (headless or visible based on config)
        3. Creates a new browser context with a custom user agent
        4. Opens a new page/tab
        5. Sets the default timeout for all operations

        WHY CUSTOM USER AGENT?
        ----------------------
        The user agent tells the website what browser we're using. Setting a
        realistic user agent helps avoid being blocked as a bot.

        MUST BE CALLED:
        ---------------
        Call this before any scraping operations! The browser won't exist
        until initialize() is called.
        """
        logger.info("Initializing browser...")

        # Start Playwright engine
        playwright = await async_playwright().start()

        # Launch Chromium browser
        # headless=True means no visible window
        self.browser = await playwright.chromium.launch(headless=self.headless)

        # Create a browser context (like an incognito session)
        # user_agent makes us look like a regular browser
        self.context = await self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )

        # Open a new page/tab in the context
        self.page = await self.context.new_page()

        # Set default timeout for all page operations (in milliseconds)
        self.page.set_default_timeout(config.TIMEOUT)

        logger.info("Browser initialized")

    async def close(self):
        """
        Close the browser and clean up resources.

        IMPORTANT: Always call this when you're done scraping!
        If you don't, browser processes will keep running in the background
        and use up memory.

        USAGE:
        ------
        try:
            await scraper.initialize()
            # ... scraping code ...
        finally:
            await scraper.close()
        """
        if self.browser:
            await self.browser.close()
            logger.info("Browser closed")

    # =========================================================================
    # PROGRESS TRACKING METHODS
    # =========================================================================

    def load_progress(self) -> int:
        """
        Load the last scraped file number from the progress file.

        WHY TRACK PROGRESS?
        -------------------
        Scraping can take days. If the script crashes or you need to stop it,
        you don't want to start from the beginning. The progress file remembers
        where you left off.

        RETURNS:
        --------
        int
            The last successfully scraped file number, or the start number
            if no progress file exists.

        EXAMPLE PROGRESS FILE (progress.json):
        --------------------------------------
        {
            "last_file_number": 1000500,
            "updated_at": "2024-01-15T10:30:00"
        }
        """
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                    return data.get('last_file_number', self.start_number)
            except (json.JSONDecodeError, IOError) as e:
                # If file is corrupted or can't be read, log and continue
                logger.warning(f"Could not load progress file: {e}")
        return self.start_number

    def save_progress(self, file_number: int):
        """
        Save current progress to the progress file.

        PARAMETERS:
        -----------
        file_number : int
            The file number to save as the "last completed" number.

        NOTE:
        -----
        Progress is saved periodically (every 10 file numbers) during scraping,
        and also when the script is interrupted or completes.
        """
        with open(self.progress_file, 'w') as f:
            json.dump({
                'last_file_number': file_number,
                'updated_at': datetime.now().isoformat()
            }, f, indent=2)  # indent=2 makes the file human-readable

    # =========================================================================
    # CSV FILE MANAGEMENT METHODS
    # =========================================================================

    def init_csv(self):
        """
        Initialize the CSV file with headers if it doesn't exist.

        This creates an empty CSV with just the header row. New records will
        be appended to this file as they're scraped.
        """
        if not self.output_file.exists():
            # Create an empty DataFrame with our columns
            df = pd.DataFrame(columns=self.columns)
            # Write to CSV (this creates a file with just headers)
            df.to_csv(self.output_file, index=False)
            logger.info(f"Created output file: {self.output_file}")

    def append_to_csv(self, data: dict):
        """
        Append a single business record to the CSV file.

        PARAMETERS:
        -----------
        data : dict
            Dictionary with keys matching self.columns and values for each field.

        NOTE:
        -----
        This appends one row at a time. It's less efficient than batch writing,
        but ensures data is saved immediately (no data loss if script crashes).
        """
        # Create a DataFrame with a single row
        df = pd.DataFrame([data])
        # Append to CSV (mode='a'), don't write headers again (header=False)
        df.to_csv(self.output_file, mode='a', header=False, index=False)

    # =========================================================================
    # SEARCH METHODS
    # =========================================================================

    async def search_by_file_number(self, file_number: int) -> tuple[bool, str]:
        """
        Search for a business by its file number.

        This navigates to the MN SOS website, enters the file number in the
        search form, and checks if any results were found.

        PARAMETERS:
        -----------
        file_number : int
            The MN SOS file number to search for.

        RETURNS:
        --------
        tuple[bool, str]
            - found: True if a business was found, False otherwise
            - business_name: The name of the business if found, empty string if not

        HOW IT WORKS:
        -------------
        1. Navigate to the search page
        2. Click the "File Number" tab
        3. Enter the file number
        4. Click search
        5. Check for results
        6. If found, click through to the details page
        """
        try:
            # Navigate to search page
            # wait_until='networkidle' waits for network to be quiet
            await self.page.goto(config.BASE_URL, wait_until='networkidle')

            # Click the "File Number" tab to show the file number search field
            file_number_tab = await self.page.wait_for_selector(
                'a[href="#fileNumberTab"]',
                timeout=10000
            )
            await file_number_tab.click()
            await asyncio.sleep(0.3)  # Brief wait for tab animation

            # Wait for the file number input field to be visible
            await self.page.wait_for_selector('#FileNumber:visible', timeout=5000)

            # Clear any existing value and enter our file number
            await self.page.fill('#FileNumber', str(file_number))

            # Click the search button (within the file number tab)
            await self.page.click('#fileNumberTab button[type="submit"]')

            # Wait for results to load
            await self.page.wait_for_load_state('networkidle')
            await asyncio.sleep(0.5)  # Extra wait for dynamic content

            # Check for "no results" message
            page_text = await self.page.inner_text('body')
            if 'no results' in page_text.lower() or 'no businesses found' in page_text.lower():
                return False, ''

            # Try to get the business name from results
            name_element = await self.page.query_selector('table tbody tr td strong')
            business_name = ''
            if name_element:
                business_name = (await name_element.inner_text()).strip()

            # Click the "Details" link to go to full business page
            details_link = await self.page.query_selector('a[href*="SearchDetails"]')
            if details_link:
                await details_link.click()
                await self.page.wait_for_load_state('networkidle')
                return True, business_name

            # Check if we're already on a details page
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

    # =========================================================================
    # DATA EXTRACTION METHODS
    # =========================================================================

    async def extract_text(self, selector: str, default: str = '') -> str:
        """
        Extract text from an element on the page.

        A helper method that safely extracts text from a CSS selector,
        returning a default value if the element isn't found.

        PARAMETERS:
        -----------
        selector : str
            CSS selector for the element (e.g., 'h2', '.class-name', '#id')
        default : str
            Value to return if element isn't found (default: empty string)

        RETURNS:
        --------
        str
            The text content of the element, or the default value
        """
        try:
            element = await self.page.query_selector(selector)
            if element:
                text = await element.inner_text()
                return text.strip()
        except Exception:
            pass
        return default

    async def extract_business_data(self, file_number: int, business_name: str = '') -> dict:
        """
        Extract all available data from a business details page.

        This is the main data extraction method. It reads the current page
        and extracts all relevant business information into a dictionary.

        PARAMETERS:
        -----------
        file_number : int
            The file number (or GUID) of the business
        business_name : str
            The business name (may be pre-populated from search results)

        RETURNS:
        --------
        dict
            Dictionary with all business fields populated (or empty strings)

        HOW THE PAGE IS STRUCTURED:
        ---------------------------
        The MN SOS detail page uses <dt>/<dd> pairs for most data:
        <dt>Business Type</dt>
        <dd>Limited Liability Company</dd>

        Tables are used for:
        - Applicant/Markholder information
        - Filing history
        """
        # Initialize data dictionary with all fields set to empty
        data = {
            'file_number': file_number,
            'business_name': business_name,
            'mn_statute': '',
            'business_type': '',
            'home_jurisdiction': '',
            'filing_date': '',
            'status': '',
            'renewal_due_date': '',
            'mark_type': '',
            'number_of_shares': '',
            'chief_executive_officer': '',
            'manager': '',
            # Principal Place of Business address
            'principal_street_number': '',
            'principal_street_name': '',
            'principal_street_type': '',
            'principal_street_direction': '',
            'principal_unit': '',
            'principal_city': '',
            'principal_state': '',
            'principal_zip': '',
            'principal_address_raw': '',
            # Registered Office Address
            'reg_office_street_number': '',
            'reg_office_street_name': '',
            'reg_office_street_type': '',
            'reg_office_street_direction': '',
            'reg_office_unit': '',
            'reg_office_city': '',
            'reg_office_state': '',
            'reg_office_zip': '',
            'reg_office_address_raw': '',
            # Principal Executive Office Address
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
            # =================================================================
            # MAPPING: Website labels -> our field names
            # =================================================================
            # This maps the text in <dt> elements to our data fields
            label_mapping = {
                'business type': 'business_type',
                'mn statute': 'mn_statute',
                'home jurisdiction': 'home_jurisdiction',
                'filing date': 'filing_date',
                'date of incorporation': 'filing_date',  # Alternative label
                'status': 'status',
                'renewal due date': 'renewal_due_date',
                'mark type': 'mark_type',
                'number of shares': 'number_of_shares',
                'chief executive officer': 'chief_executive_officer',
                'manager': 'manager',
                'registered agent': 'registered_agent_name',
                'registered agent(s)': 'registered_agent_name',
            }

            # Variables to hold raw addresses for parsing later
            principal_address_raw = ''
            reg_office_address_raw = ''
            exec_office_address_raw = ''

            # =================================================================
            # EXTRACT DT/DD PAIRS
            # =================================================================
            # Get all <dt> elements on the page
            dts = await self.page.query_selector_all('dt')

            for dt in dts:
                try:
                    # Get the label text (lowercase for matching)
                    label = (await dt.inner_text()).strip().lower()

                    # Get the corresponding <dd> value
                    # This JavaScript gets the next sibling element's text
                    dd_text = await dt.evaluate(
                        'el => el.nextElementSibling?.innerText || ""'
                    )
                    dd_text = dd_text.strip()

                    # Check for Principal Place of Business Address
                    if 'principal place of business' in label and 'address' in label:
                        principal_address_raw = dd_text
                        data['principal_address_raw'] = dd_text
                        continue

                    # Check for Principal Executive Office Address
                    if 'principal executive office' in label and 'address' in label:
                        exec_office_address_raw = dd_text
                        data['exec_office_address_raw'] = dd_text
                        continue

                    # Check for Registered Office Address
                    if 'registered office' in label and 'address' in label:
                        reg_office_address_raw = dd_text
                        data['reg_office_address_raw'] = dd_text
                        continue

                    # Map other standard fields
                    for key, field in label_mapping.items():
                        if key in label and dd_text:
                            # Only set if not already set (first match wins)
                            if not data[field]:
                                data[field] = dd_text
                            break

                except Exception:
                    # Skip problematic elements and continue
                    continue

            # =================================================================
            # PARSE ADDRESSES INTO COMPONENTS
            # =================================================================

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

            # =================================================================
            # EXTRACT APPLICANT/MARKHOLDER FROM TABLE
            # =================================================================

            tables = await self.page.query_selector_all('table')

            for table in tables:
                # Get table headers
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
                            # First cell: Name
                            data['applicant_name'] = (await cells[0].inner_text()).strip()
                            # Second cell: Address
                            applicant_addr_raw = (await cells[1].inner_text()).strip()
                            data['applicant_address_raw'] = applicant_addr_raw

                            # Parse the address
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

            # =================================================================
            # EXTRACT FILING HISTORY
            # =================================================================

            filing_history = []

            for table in tables:
                headers = await table.query_selector_all('th')
                header_texts = [(await h.inner_text()).strip().lower() for h in headers]

                # Find the filing history table
                if any('filing' in h for h in header_texts) and \
                   not any('applicant' in h for h in header_texts):
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
                                    # Join cell values with pipe separator
                                    filing_history.append(' | '.join(row_data))
                        except Exception:
                            continue
                    break

            # Join all filing history entries (max 20 to avoid huge strings)
            if filing_history:
                data['filing_history'] = ' ;; '.join(filing_history[:20])

            # =================================================================
            # CONVERT DATES TO ISO FORMAT
            # =================================================================

            if data.get('filing_date'):
                data['filing_date'] = convert_date_to_iso(data['filing_date'])
            if data.get('renewal_due_date'):
                data['renewal_due_date'] = convert_date_to_iso(data['renewal_due_date'])

        except Exception as e:
            logger.error(f"Error extracting data for file {file_number}: {e}")

        return data

    # =========================================================================
    # MAIN SCRAPING METHODS
    # =========================================================================

    async def scrape_business(self, file_number: int) -> dict | None:
        """
        Scrape a single business by file number.

        This combines searching and extracting into one method.
        It includes retry logic for handling transient errors.

        PARAMETERS:
        -----------
        file_number : int
            The MN SOS file number to scrape

        RETURNS:
        --------
        dict or None
            Business data dictionary if found, None if not found or error
        """
        for attempt in range(config.MAX_RETRIES):
            try:
                # Search for the business
                found, business_name = await self.search_by_file_number(file_number)

                if not found:
                    return None

                # Extract data from the details page
                data = await self.extract_business_data(file_number, business_name)
                return data

            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for file {file_number}: {e}")
                if attempt < config.MAX_RETRIES - 1:
                    # Wait before retrying
                    await asyncio.sleep(config.RETRY_DELAY)
                else:
                    logger.error(f"All retries failed for file {file_number}")
                    return None

        return None

    async def scrape_business_by_guid(self, guid: str) -> dict | None:
        """
        Scrape a single business by GUID (for search results).

        When searching by name, the results include GUIDs that can be used
        to directly access business detail pages.

        PARAMETERS:
        -----------
        guid : str
            The GUID of the business (from search results URL)

        RETURNS:
        --------
        dict or None
            Business data dictionary if found, None if not found or error
        """
        for attempt in range(config.MAX_RETRIES):
            try:
                # Navigate directly to the details page using GUID
                url = f'https://mblsportal.sos.mn.gov/Business/SearchDetails?filingGuid={guid}'
                await self.page.goto(url, wait_until='networkidle')
                await asyncio.sleep(0.5)

                # Check if we got a valid page
                title = await self.page.title()
                if 'Details' not in title:
                    return None

                # Get business name from h2 heading
                business_name = ''
                h2 = await self.page.query_selector('h2')
                if h2:
                    business_name = (await h2.inner_text()).strip()

                # Extract data (use GUID as file_number for tracking)
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
        """
        Add a polite delay between requests.

        WHY DELAY?
        ----------
        1. Be polite to the server - don't overload it with requests
        2. Avoid getting blocked/banned for scraping too fast
        3. Give the server time to respond properly

        The delay includes some randomness (jitter) to make the requests
        look more like human browsing behavior.
        """
        # Base delay plus random jitter
        delay = config.REQUEST_DELAY + random.uniform(0, config.DELAY_JITTER)
        await asyncio.sleep(delay)

    async def run(self, resume: bool = True):
        """
        Main scraping loop.

        This is the entry point for running the scraper. It:
        1. Initializes the browser
        2. Loads progress (if resuming)
        3. Scrapes businesses sequentially by file number
        4. Saves progress periodically
        5. Stops after too many consecutive misses (reached end of numbers)

        PARAMETERS:
        -----------
        resume : bool
            If True, resume from last saved progress
            If False, start fresh from self.start_number

        STOPPING CONDITIONS:
        --------------------
        The scraper stops when it encounters MAX_CONSECUTIVE_MISSES in a row.
        This indicates we've reached the end of the file number range.
        """
        try:
            # Initialize browser and CSV file
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

            # Main loop - continue until too many consecutive misses
            while self.consecutive_misses < config.MAX_CONSECUTIVE_MISSES:
                file_number = self.current_file_number

                logger.info(f"Scraping file number {file_number}...")

                # Attempt to scrape this file number
                data = await self.scrape_business(file_number)

                if data:
                    # Found a business - save it!
                    self.append_to_csv(data)
                    scraped_count += 1
                    self.consecutive_misses = 0  # Reset miss counter
                    logger.info(f"[FOUND] {data.get('business_name', 'Unknown')} (#{file_number})")
                else:
                    # No business found at this file number
                    self.consecutive_misses += 1
                    logger.debug(f"[MISS] No result for file number {file_number} "
                               f"({self.consecutive_misses} consecutive misses)")

                # Save progress every 10 file numbers
                if file_number % 10 == 0:
                    self.save_progress(file_number)
                    logger.info(f"Progress: {scraped_count} businesses scraped, at file #{file_number}")

                # Move to next file number
                self.current_file_number += 1

                # Be polite - wait before next request
                await self.add_delay()

            # Reached stopping condition
            logger.info(f"Stopping: {config.MAX_CONSECUTIVE_MISSES} consecutive misses reached")
            logger.info(f"Total businesses scraped: {scraped_count}")
            self.save_progress(self.current_file_number - 1)

        except KeyboardInterrupt:
            # User pressed Ctrl+C
            logger.info("Interrupted by user")
            self.save_progress(self.current_file_number - 1)
        except Exception as e:
            # Unexpected error
            logger.error(f"Fatal error: {e}")
            self.save_progress(self.current_file_number - 1)
            raise
        finally:
            # Always close the browser
            await self.close()


# =============================================================================
# COMMAND-LINE INTERFACE
# =============================================================================

def main():
    """
    Command-line entry point for the scraper.

    USAGE:
    ------
    # Start from beginning with default settings
    python mn_scraper.py

    # Start from specific file number
    python mn_scraper.py --start 1000000

    # Start fresh (ignore saved progress)
    python mn_scraper.py --no-resume

    # Run with visible browser (for debugging)
    python mn_scraper.py --visible

    # Combine options
    python mn_scraper.py --start 1000000 --visible --no-resume
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Minnesota Business Scraper')
    parser.add_argument(
        '--start',
        type=int,
        help='Starting file number (default: from config)'
    )
    parser.add_argument(
        '--no-resume',
        action='store_true',
        help='Start fresh, ignore saved progress'
    )
    parser.add_argument(
        '--visible',
        action='store_true',
        help='Run browser in visible mode (not headless)'
    )

    args = parser.parse_args()

    # Create scraper with command-line options
    scraper = MNBusinessScraper(
        start_number=args.start,
        headless=not args.visible  # Invert: --visible means headless=False
    )

    # Run the async scraper
    # asyncio.run() handles creating and running the event loop
    asyncio.run(scraper.run(resume=not args.no_resume))


# This runs when the script is executed directly (not imported)
if __name__ == '__main__':
    main()
