"""Configuration settings for MN Business Scraper."""

# File number iteration settings
START_FILE_NUMBER = 1
MAX_CONSECUTIVE_MISSES = 100  # Stop after this many not-found in a row

# Rate limiting
REQUEST_DELAY = 1.5  # Base delay between requests (seconds)
DELAY_JITTER = 0.5   # Random jitter added to delay (0 to this value)

# Browser settings
HEADLESS = True  # Run browser invisibly
TIMEOUT = 30000  # Page timeout in milliseconds

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 5  # Seconds to wait before retry

# Output settings
OUTPUT_DIR = "output"
OUTPUT_FILE = "businesses.csv"
PROGRESS_FILE = "progress.json"

# URL
BASE_URL = "https://mblsportal.sos.mn.gov/Business/Search"
