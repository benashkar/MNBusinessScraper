#!/usr/bin/env python3
"""
=============================================================================
MINNESOTA BUSINESS SCRAPER - PARALLEL ALPHABETICAL SEARCH
=============================================================================

PURPOSE:
    This script scrapes business filings from the Minnesota Secretary of State
    website (https://mblsportal.sos.mn.gov/Business/Search) by searching for
    businesses alphabetically using two-letter patterns (aa, ab, ac, ... zz).

HOW IT WORKS:
    1. Generates 676 two-letter search patterns (aa through zz)
    2. Divides patterns among multiple "worker" processes
    3. Each worker searches the website for businesses matching its patterns
    4. Filters results by target years (e.g., 2023, 2024) and business types
    5. Saves matching businesses to CSV files
    6. Auto-saves progress to GitHub every 4 hours

USAGE:
    # Basic usage with 8 workers for years 2024-2026
    python search_by_name_parallel.py --workers 8 --years 2024 2025 2026

    # Run with 2 workers for year 2023 only
    python search_by_name_parallel.py --workers 2 --years 2023

    # Run with visible browser windows (for debugging)
    python search_by_name_parallel.py --workers 2 --visible

OUTPUT FILES:
    - output/businesses_alpha_worker_0.csv  (Worker 0's results)
    - output/businesses_alpha_worker_1.csv  (Worker 1's results)
    - ... (one file per worker)
    - progress_alpha_worker_0.json  (Worker 0's progress/resume info)
    - progress_alpha_worker_1.json  (Worker 1's progress/resume info)
    - ... (one file per worker)

RESUME CAPABILITY:
    The script automatically saves progress. If interrupted, simply run
    the same command again and it will resume from where it left off.

AUTHOR: Auto-generated for MN Business Scraper project
DATE: January 2026
=============================================================================
"""

# =============================================================================
# IMPORTS - External libraries we need
# =============================================================================

import argparse          # For parsing command-line arguments (--workers, --years, etc.)
import asyncio           # For running multiple tasks concurrently (async/await)
import json              # For reading/writing JSON files (progress tracking)
import logging           # For logging messages to files and console
import os                # For file system operations
import string            # For generating letter patterns (a-z)
import subprocess        # For running git commands
import sys               # For system-level operations
from datetime import datetime  # For timestamps
from pathlib import Path       # For cross-platform file path handling

# Configure stdout to handle special characters (like business names with accents)
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

import pandas as pd      # For handling CSV files and data manipulation
from playwright.async_api import async_playwright  # Browser automation library

# Import our custom scraper class (from mn_scraper.py in same folder)
sys.path.insert(0, str(Path(__file__).parent))
from mn_scraper import MNBusinessScraper

# =============================================================================
# LOGGING SETUP - Records what the scraper does to files and console
# =============================================================================

# Create logs directory if it doesn't exist
Path('logs').mkdir(exist_ok=True)

# Configure logging to write to both console and file
logging.basicConfig(
    level=logging.INFO,  # Log INFO level and above (INFO, WARNING, ERROR)
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s',
    handlers=[
        # Handler 1: Print to console
        logging.StreamHandler(),
        # Handler 2: Write to daily log file
        logging.FileHandler(
            f'logs/scraper_{datetime.now().strftime("%Y%m%d")}.log',
            encoding='utf-8'
        )
    ]
)
logger = logging.getLogger('parallel_scraper')

# =============================================================================
# CONFIGURATION - Settings that control how the scraper behaves
# =============================================================================

# TARGET_YEARS: Which years of business filings to collect
# This is set dynamically via command-line argument (--years)
# Example: ['2023'] means only save businesses filed in 2023
TARGET_YEARS = ['2023']

# BUSINESS_TYPE_KEYWORDS: Words to look for in business names during search
# This is a BROAD filter - we search for any name containing these words
# The actual filtering by business type happens later (see TARGET_BUSINESS_TYPES)
BUSINESS_TYPE_KEYWORDS = [
    'LLC',          # Limited Liability Company
    'L.L.C.',       # Alternative LLC format
    'CORPORATION',  # Corporation
    'CORP',         # Corporation abbreviation
    'INC',          # Incorporated
    'INCORPORATED', # Full form
    'NONPROFIT',    # Nonprofit organization
    'NON-PROFIT'    # Alternative nonprofit format
]

# TARGET_BUSINESS_TYPES: Exact business types to SAVE (after getting details)
# Only businesses matching these exact types will be saved to CSV
# This is the STRICT filter applied after we fetch business details
TARGET_BUSINESS_TYPES = [
    'Limited Liability Company (Domestic)',   # MN-based LLC
    'Limited Liability Company (Foreign)',    # Out-of-state LLC registered in MN
    'Business Corporation (Domestic)',        # MN-based corporation
    'Business Corporation (Foreign)',         # Out-of-state corp registered in MN
    'Nonprofit Corporation (Domestic)',       # MN-based nonprofit
    'Nonprofit Corporation (Foreign)',        # Out-of-state nonprofit registered in MN
]

# AUTO_SAVE_INTERVAL: How often to save progress to GitHub (in seconds)
# 4 hours = 4 * 60 minutes * 60 seconds = 14,400 seconds
AUTO_SAVE_INTERVAL = 4 * 60 * 60

# Global variable to track when we last saved
last_save_time = None


# =============================================================================
# HELPER FUNCTIONS - Small utility functions used throughout the script
# =============================================================================

def generate_patterns():
    """
    Generate all two-letter search patterns from 'aa' to 'zz'.

    This creates 676 patterns (26 letters x 26 letters = 676).
    We use these patterns to search the MN business database, which
    helps us find ALL businesses (not just the most recent ones).

    Returns:
        list: ['aa', 'ab', 'ac', ... 'zx', 'zy', 'zz']

    Example:
        >>> patterns = generate_patterns()
        >>> len(patterns)
        676
        >>> patterns[:5]
        ['aa', 'ab', 'ac', 'ad', 'ae']
    """
    patterns = []
    # string.ascii_lowercase = 'abcdefghijklmnopqrstuvwxyz'
    for first_letter in string.ascii_lowercase:      # a, b, c, ... z
        for second_letter in string.ascii_lowercase:  # a, b, c, ... z
            patterns.append(first_letter + second_letter)
    return patterns


def run_auto_save():
    """
    Merge all worker output files and push to GitHub.

    This function:
    1. Reads CSV files from each worker (businesses_alpha_worker_0.csv, etc.)
    2. Combines them into one dataset
    3. Removes duplicate businesses (based on file_number)
    4. Merges with existing data in data/businesses.csv
    5. Creates a summary.json with statistics
    6. Commits and pushes changes to GitHub

    This runs automatically every 4 hours and when scraping completes.
    """
    global last_save_time

    logger.info("=" * 60)
    logger.info(f"AUTO-SAVE STARTED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # Define directory paths
    repo_dir = Path(__file__).parent      # The folder containing this script
    output_dir = repo_dir / 'output'      # Where worker CSV files are saved
    data_dir = repo_dir / 'data'          # Where final merged data goes
    data_dir.mkdir(exist_ok=True)         # Create data folder if it doesn't exist

    # -------------------------------------------------------------------------
    # STEP 1: Read all worker CSV files
    # -------------------------------------------------------------------------
    dfs = []  # List to hold DataFrames from each worker

    # Check workers 0-19 (we support up to 20 workers)
    for worker_id in range(20):
        worker_file = output_dir / f'businesses_alpha_worker_{worker_id}.csv'

        if worker_file.exists():
            try:
                # Read the CSV file into a pandas DataFrame
                df = pd.read_csv(worker_file, low_memory=False)
                dfs.append(df)
                logger.info(f"  Worker {worker_id}: {len(df)} records")
            except Exception as e:
                logger.error(f"  Worker {worker_id}: Error reading file - {e}")

    # If no worker files found, nothing to save
    if not dfs:
        logger.warning("No worker data found to save!")
        return

    # -------------------------------------------------------------------------
    # STEP 2: Combine all worker data and remove duplicates
    # -------------------------------------------------------------------------
    # pd.concat joins all DataFrames into one big DataFrame
    merged = pd.concat(dfs, ignore_index=True)

    # Remove duplicate businesses (keep the most recent entry for each)
    if 'file_number' in merged.columns:
        merged = merged.drop_duplicates(subset=['file_number'], keep='last')

    logger.info(f"Total unique records from workers: {len(merged)}")

    # -------------------------------------------------------------------------
    # STEP 3: Merge with existing data (if any)
    # -------------------------------------------------------------------------
    existing_file = data_dir / 'businesses.csv'

    if existing_file.exists():
        try:
            # Load existing data
            existing = pd.read_csv(existing_file, low_memory=False)
            logger.info(f"Existing records in data/: {len(existing)}")

            # Combine new data with existing data
            merged = pd.concat([existing, merged], ignore_index=True)

            # Remove duplicates again (in case new data overlaps with existing)
            merged = merged.drop_duplicates(subset=['file_number'], keep='last')
            logger.info(f"Combined unique records: {len(merged)}")

        except Exception as e:
            logger.error(f"Error loading existing data: {e}")

    # -------------------------------------------------------------------------
    # STEP 4: Save the merged data
    # -------------------------------------------------------------------------
    merged.to_csv(data_dir / 'businesses.csv', index=False)
    logger.info(f"Saved {len(merged)} records to data/businesses.csv")

    # -------------------------------------------------------------------------
    # STEP 5: Create summary statistics
    # -------------------------------------------------------------------------
    summary = {
        "last_updated": datetime.now().isoformat(),
        "total_businesses": len(merged),
        "target_types": TARGET_BUSINESS_TYPES,
    }

    # Count businesses by filing year
    if 'filing_date' in merged.columns:
        years = merged['filing_date'].astype(str).str[:4]  # Extract year (first 4 chars)
        year_counts = years.value_counts().head(10)  # Top 10 years
        summary['by_year'] = {k: int(v) for k, v in year_counts.items() if k != 'nan'}

    # Count businesses by type
    if 'business_type' in merged.columns:
        type_counts = merged['business_type'].value_counts()
        summary['by_type'] = {k: int(v) for k, v in type_counts.items()}

    # Save summary to JSON file
    with open(data_dir / 'summary.json', 'w') as f:
        json.dump(summary, f, indent=2)

    # -------------------------------------------------------------------------
    # STEP 6: Commit and push to GitHub
    # -------------------------------------------------------------------------
    try:
        original_dir = os.getcwd()  # Remember current directory
        os.chdir(repo_dir)          # Change to repo directory

        # Stage the data folder for commit
        subprocess.run(['git', 'add', 'data/'], capture_output=True)

        # Check if there are changes to commit
        result = subprocess.run(
            ['git', 'status', '--porcelain', 'data/'],
            capture_output=True,
            text=True
        )

        if result.stdout.strip():  # If there are changes
            # Create commit message with timestamp
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
            commit_msg = f"Auto-save: {len(merged)} records ({timestamp})"

            # Commit the changes
            subprocess.run(['git', 'commit', '-m', commit_msg], capture_output=True)

            # Push to GitHub
            push_result = subprocess.run(['git', 'push'], capture_output=True, text=True)

            if push_result.returncode == 0:
                logger.info("Successfully pushed to GitHub")
            else:
                logger.error(f"Git push failed: {push_result.stderr}")
        else:
            logger.info("No changes to commit")

        os.chdir(original_dir)  # Return to original directory

    except Exception as e:
        logger.error(f"Git operation failed: {e}")

    # Update last save time
    last_save_time = datetime.now()
    logger.info(f"Auto-save complete. Next save in 4 hours.")
    logger.info("=" * 60)


async def auto_save_task():
    """
    Background task that runs auto_save every 4 hours.

    This is an "async" function that runs in the background while
    the scraper workers are doing their job. It wakes up every
    4 hours, saves progress, then goes back to sleep.

    The 'await asyncio.sleep()' allows other tasks to run while
    this one is sleeping (that's the magic of async programming!).
    """
    global last_save_time
    last_save_time = datetime.now()

    while True:  # Run forever (until cancelled)
        # Sleep for 4 hours
        await asyncio.sleep(AUTO_SAVE_INTERVAL)

        try:
            # Wake up and save progress
            run_auto_save()
        except Exception as e:
            logger.error(f"Auto-save error: {e}")


# =============================================================================
# SEARCH FUNCTIONS - Functions that interact with the MN SOS website
# =============================================================================

async def search_by_name(search_term: str, page, max_results: int = 500):
    """
    Search for businesses by name on the MN Secretary of State website.

    This function:
    1. Navigates to the search page
    2. Enters the search term (e.g., "aa", "ab", etc.)
    3. Clicks the search button
    4. Extracts business names and their unique IDs (GUIDs) from results

    Args:
        search_term (str): The search pattern to use (e.g., "aa", "corp")
        page: A Playwright page object (browser tab)
        max_results (int): Maximum number of results to collect (default: 500)

    Returns:
        list: List of dictionaries with 'business_name' and 'guid' keys
              Example: [{'business_name': 'ABC LLC', 'guid': 'abc-123-def'}, ...]

    Note:
        This function uses Playwright for browser automation. It's "async"
        because web requests take time, and we don't want to block other tasks.
    """
    results = []  # Will hold our search results

    try:
        # ---------------------------------------------------------------------
        # Navigate to the search page
        # ---------------------------------------------------------------------
        await page.goto(
            "https://mblsportal.sos.mn.gov/Business/Search",
            timeout=30000  # Wait up to 30 seconds for page to load
        )
        # Wait for page to fully load (no more network requests)
        await page.wait_for_load_state("networkidle")

        # ---------------------------------------------------------------------
        # Configure search options
        # ---------------------------------------------------------------------
        # Select "Contains" option for broader search
        # This finds businesses where the name CONTAINS our search term
        # (vs "Starts with" which would only find names STARTING with the term)
        await page.evaluate('document.getElementById("containsz").checked = true')

        # Enter the search term into the business name field
        await page.fill('#BusinessName', search_term)

        # ---------------------------------------------------------------------
        # Submit the search
        # ---------------------------------------------------------------------
        # Find and click the Search button
        search_btn = page.locator('button:has-text("Search")').first
        await search_btn.click()

        # Wait for search results to load
        await page.wait_for_load_state("networkidle")
        await asyncio.sleep(1.5)  # Extra wait for JavaScript to finish

        # ---------------------------------------------------------------------
        # Extract results from the page
        # ---------------------------------------------------------------------
        # Look for the results table
        table = await page.query_selector('table.table')
        if not table:
            # No table means no results found
            return results

        # Get all rows from the table body (skip header row)
        rows = await table.query_selector_all('tbody tr')

        # Process each row (up to max_results)
        for row in rows[:max_results]:
            try:
                # Get all cells in this row
                cells = await row.query_selector_all('td')

                if len(cells) >= 1:
                    # Get the business name from the first cell
                    name_cell = cells[0]
                    name_elem = await name_cell.query_selector('strong')

                    if name_elem:
                        name = (await name_elem.inner_text()).strip()
                    else:
                        name = (await name_cell.inner_text()).strip()

                    # Get the unique business ID (GUID) from the Details link
                    details_link = await row.query_selector('a[href*="filingGuid"]')
                    guid = None

                    if details_link:
                        href = await details_link.get_attribute('href')
                        if 'filingGuid=' in href:
                            # Extract GUID from URL like "...?filingGuid=abc-123-def"
                            guid = href.split('filingGuid=')[-1]

                    # Only keep results that:
                    # 1. Have a valid GUID
                    # 2. Have a name containing our target keywords
                    name_upper = name.upper()
                    if guid and any(kw in name_upper for kw in BUSINESS_TYPE_KEYWORDS):
                        results.append({
                            'business_name': name,
                            'guid': guid
                        })

            except Exception:
                # Skip any rows that cause errors
                continue

        return results

    except Exception as e:
        logger.error(f"Error searching for '{search_term}': {e}")
        return results


# =============================================================================
# WORKER FUNCTION - The main scraping logic for each worker
# =============================================================================

async def worker_scrape(worker_id: int, patterns: list, headless: bool = True):
    """
    Worker function that scrapes a subset of search patterns.

    Each worker is assigned a range of patterns (e.g., Worker 0 gets aa-mm,
    Worker 1 gets mn-zz). This allows multiple workers to scrape in parallel,
    making the overall process much faster.

    Args:
        worker_id (int): Unique identifier for this worker (0, 1, 2, etc.)
        patterns (list): List of search patterns for this worker to process
        headless (bool): If True, run browser invisibly. If False, show browser.

    Returns:
        int: Number of businesses found and saved by this worker

    Resume Capability:
        This function saves progress after each pattern. If interrupted,
        it will skip already-completed patterns when restarted.
    """
    # -------------------------------------------------------------------------
    # Setup file paths for this worker
    # -------------------------------------------------------------------------
    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)  # Create output folder if needed

    # Each worker has its own output CSV and progress JSON file
    output_file = output_dir / f'businesses_alpha_worker_{worker_id}.csv'
    progress_file = Path(f'progress_alpha_worker_{worker_id}.json')

    # -------------------------------------------------------------------------
    # Load previous progress (for resume capability)
    # -------------------------------------------------------------------------
    completed_patterns = set()  # Patterns we've already finished
    processed_guids = set()     # Business GUIDs we've already seen

    if progress_file.exists():
        try:
            with open(progress_file) as f:
                progress = json.load(f)
                completed_patterns = set(progress.get('completed_patterns', []))
                processed_guids = set(progress.get('processed_guids', []))
        except Exception:
            pass  # If can't load progress, start fresh

    # Also load GUIDs from output file (in case progress file is outdated)
    if output_file.exists():
        try:
            existing_df = pd.read_csv(output_file)
            if 'file_number' in existing_df.columns:
                processed_guids.update(existing_df['file_number'].astype(str).tolist())
        except Exception:
            pass

    # -------------------------------------------------------------------------
    # Calculate remaining work
    # -------------------------------------------------------------------------
    remaining_patterns = [p for p in patterns if p not in completed_patterns]
    logger.info(f"[Worker {worker_id}] {len(remaining_patterns)} patterns remaining (of {len(patterns)} total)")

    # If all patterns done, exit early
    if not remaining_patterns:
        logger.info(f"[Worker {worker_id}] All patterns already completed!")
        return 0

    # -------------------------------------------------------------------------
    # Initialize the browser
    # -------------------------------------------------------------------------
    async with async_playwright() as p:
        # Launch Chrome browser
        browser = await p.chromium.launch(
            headless=headless,  # True = invisible, False = visible window
            args=[
                # Make browser less detectable as automated
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-dev-shm-usage',
            ]
        )

        # Create a browser context (like an incognito window)
        context = await browser.new_context(
            # Pretend to be a regular Chrome browser
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
        )

        # Create a new page (tab) in the browser
        page = await context.new_page()

        # Hide the fact that we're using automated browser
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        # Initialize our custom scraper for getting business details
        scraper = MNBusinessScraper(headless=headless)
        await scraper.initialize()

        # -------------------------------------------------------------------------
        # Main scraping loop
        # -------------------------------------------------------------------------
        found_count = 0   # Number of businesses saved
        recent_count = 0  # Number of businesses from target years

        try:
            # Process each remaining pattern
            for pattern in remaining_patterns:
                logger.info(f"[Worker {worker_id}] Searching: '{pattern}'...")

                # Search for businesses matching this pattern
                try:
                    results = await search_by_name(pattern, page, max_results=500)
                except Exception as e:
                    logger.error(f"[Worker {worker_id}] Search error for '{pattern}': {e}")
                    await asyncio.sleep(5)  # Wait before retrying
                    continue

                # Filter out businesses we've already processed
                new_results = [r for r in results if r['guid'] not in processed_guids]
                logger.info(f"[Worker {worker_id}] '{pattern}': {len(results)} results, {len(new_results)} new")

                # Get detailed info for each new business
                for r in new_results:
                    guid = r['guid']

                    try:
                        # Fetch full business details from the website
                        data = await scraper.scrape_business_by_guid(guid)

                        if data:
                            # Use name from search if not in details
                            if not data.get('business_name'):
                                data['business_name'] = r['business_name']

                            # Extract filing year from date (e.g., "2023-01-15" -> "2023")
                            filing_date = data.get('filing_date', '')
                            filing_year = filing_date[:4] if filing_date else ''

                            # Get the business type
                            business_type = data.get('business_type', '')

                            # Check if this business matches our criteria:
                            # 1. Filed in one of our target years
                            # 2. Is one of our target business types
                            if filing_year in TARGET_YEARS and business_type in TARGET_BUSINESS_TYPES:
                                recent_count += 1
                                logger.info(f"[Worker {worker_id}] [SAVED {filing_year}] {data['business_name']} ({business_type})")

                                # Save to CSV file
                                df = pd.DataFrame([data])
                                if not output_file.exists():
                                    # First record - include header
                                    df.to_csv(output_file, index=False)
                                else:
                                    # Append without header
                                    df.to_csv(output_file, mode='a', header=False, index=False)

                                found_count += 1

                            elif filing_year in TARGET_YEARS:
                                # Business is from target year but wrong type - log for debugging
                                logger.debug(f"[Worker {worker_id}] [SKIP] {data['business_name']} (type: {business_type})")

                            # Remember we processed this business
                            processed_guids.add(guid)

                        # Small delay to be nice to the server
                        await asyncio.sleep(0.3)

                    except Exception as e:
                        logger.error(f"[Worker {worker_id}] Error scraping {guid}: {e}")
                        continue

                # -----------------------------------------------------------------
                # Save progress after each pattern (for resume capability)
                # -----------------------------------------------------------------
                completed_patterns.add(pattern)

                with open(progress_file, 'w') as f:
                    json.dump({
                        'worker_id': worker_id,
                        'completed_patterns': list(completed_patterns),
                        # Only keep last 10,000 GUIDs to limit file size
                        'processed_guids': list(processed_guids)[-10000:],
                        'found_count': found_count,
                        'recent_count': recent_count,
                        'updated_at': datetime.now().isoformat()
                    }, f, indent=2)

                # Small delay between patterns (rate limiting)
                await asyncio.sleep(1)

            logger.info(f"[Worker {worker_id}] COMPLETED - Found {found_count} businesses ({recent_count} from {TARGET_YEARS})")

        except Exception as e:
            logger.error(f"[Worker {worker_id}] Fatal error: {e}")

        finally:
            # Always clean up browser resources
            await scraper.close()
            await context.close()
            await browser.close()

    return found_count


# =============================================================================
# PARALLEL EXECUTION - Coordinates multiple workers
# =============================================================================

async def run_parallel(num_workers: int, headless: bool = True):
    """
    Run multiple workers in parallel to speed up scraping.

    This function:
    1. Generates all 676 search patterns (aa-zz)
    2. Divides patterns evenly among workers
    3. Launches all workers simultaneously
    4. Starts background auto-save task
    5. Waits for all workers to finish
    6. Runs final save to GitHub

    Args:
        num_workers (int): Number of parallel workers to run
        headless (bool): If True, run browsers invisibly

    Example:
        With 8 workers and 676 patterns:
        - Worker 0: patterns aa-cn (84 patterns)
        - Worker 1: patterns co-fb (84 patterns)
        - ... and so on
    """
    # Generate all search patterns
    patterns = generate_patterns()  # ['aa', 'ab', ... 'zz']
    total_patterns = len(patterns)  # 676

    # -------------------------------------------------------------------------
    # Divide patterns among workers
    # -------------------------------------------------------------------------
    chunk_size = len(patterns) // num_workers  # Integer division
    worker_patterns = []

    for i in range(num_workers):
        start_idx = i * chunk_size

        if i == num_workers - 1:
            # Last worker gets any remaining patterns
            end_idx = len(patterns)
        else:
            end_idx = start_idx + chunk_size

        worker_patterns.append(patterns[start_idx:end_idx])

    # -------------------------------------------------------------------------
    # Print startup banner
    # -------------------------------------------------------------------------
    print("=" * 70)
    print(f"PARALLEL ALPHABETICAL SCRAPER - {num_workers} WORKERS")
    print(f"Target Years: {', '.join(TARGET_YEARS)}")
    print(f"Target Business Types:")
    for bt in TARGET_BUSINESS_TYPES:
        print(f"  - {bt}")
    print(f"Auto-save: Every 4 hours to local + GitHub")
    print("=" * 70)

    # Show pattern assignments
    for i, wp in enumerate(worker_patterns):
        print(f"  Worker {i}: patterns {wp[0]} - {wp[-1]} ({len(wp)} patterns)")
    print("=" * 70)
    print()

    # -------------------------------------------------------------------------
    # Launch all workers
    # -------------------------------------------------------------------------
    # Create a task for each worker
    tasks = [
        worker_scrape(i, worker_patterns[i], headless)
        for i in range(num_workers)
    ]

    # Start the auto-save background task
    save_task = asyncio.create_task(auto_save_task())

    # Run all workers concurrently and wait for them to finish
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        # Cancel auto-save task when workers finish
        save_task.cancel()

        # Run one final save
        print("\nScraping complete. Running final save...")
        run_auto_save()

    # -------------------------------------------------------------------------
    # Print summary
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    total_found = 0
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"  Worker {i}: ERROR - {result}")
        else:
            print(f"  Worker {i}: Found {result} businesses")
            total_found += result

    print(f"\nTotal businesses found: {total_found}")

    # Show output file statistics
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


# =============================================================================
# MAIN ENTRY POINT - Where the script starts
# =============================================================================

def main():
    """
    Main entry point for the scraper.

    Parses command-line arguments and starts the scraping process.

    Command-line Arguments:
        --workers, -w  : Number of parallel workers (default: 8)
        --visible      : Show browser windows (for debugging)
        --years, -y    : Target filing years (default: 2024 2025 2026)

    Examples:
        python search_by_name_parallel.py --workers 8 --years 2024 2025 2026
        python search_by_name_parallel.py -w 2 -y 2023
        python search_by_name_parallel.py --workers 4 --visible
    """
    # Create argument parser
    parser = argparse.ArgumentParser(
        description='Parallel Alphabetical MN Business Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python search_by_name_parallel.py --workers 8 --years 2024 2025 2026
  python search_by_name_parallel.py -w 2 -y 2023
  python search_by_name_parallel.py --workers 4 --visible
        """
    )

    # Define command-line arguments
    parser.add_argument(
        '--workers', '-w',
        type=int,
        default=8,
        help='Number of parallel workers (default: 8). More workers = faster but uses more resources.'
    )

    parser.add_argument(
        '--visible',
        action='store_true',
        help='Run browsers visibly (not headless). Useful for debugging.'
    )

    parser.add_argument(
        '--years', '-y',
        nargs='+',  # Accept multiple values
        default=['2024', '2025', '2026'],
        help='Target filing years to collect (default: 2024 2025 2026)'
    )

    # Parse the arguments
    args = parser.parse_args()

    # Update the global TARGET_YEARS based on command-line argument
    global TARGET_YEARS
    TARGET_YEARS = args.years

    logger.info(f"Starting scraper for years: {TARGET_YEARS}")
    logger.info(f"Using {args.workers} workers")

    # Run the scraper
    asyncio.run(run_parallel(
        num_workers=args.workers,
        headless=not args.visible  # headless is opposite of visible
    ))


# This block only runs when the script is executed directly
# (not when imported as a module)
if __name__ == '__main__':
    main()
