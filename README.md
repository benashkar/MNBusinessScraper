# Minnesota Business Scraper

Scrapes ALL business filings from the Minnesota Secretary of State portal (https://mblsportal.sos.mn.gov/Business/Search).

## Quick Start

```bash
# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Install Playwright browser
playwright install chromium

# 3. Run the parallel scraper (8 workers recommended)
python mn_scraper_parallel.py --workers 8 --start 1 --end 800000

# 4. After completion, merge all worker outputs
python merge_results.py --all
```

### Single-Threaded (Slower)
```bash
# Run single-threaded scraper (auto-resumes from last position)
python mn_scraper.py
```

## Files

| File | Description |
|------|-------------|
| `mn_scraper_parallel.py` | **Parallel scraper** - runs multiple workers for faster scraping |
| `mn_scraper.py` | Single-threaded scraper - iterates through file numbers sequentially |
| `merge_results.py` | Merge worker outputs into single CSV |
| `filter_recent.py` | Filter results by filing year (e.g., 2019+) |
| `config.py` | Configuration settings (delays, timeouts, file paths) |
| `requirements.txt` | Python dependencies (playwright, pandas) |
| `test_scraper.py` | Test specific file numbers |
| `progress.json` | Tracks last scraped file number (auto-created) |
| `output/businesses.csv` | Output data (auto-created) |

## Usage

### Parallel Scraper (Recommended)
```bash
# Run with 8 parallel workers (8x faster)
python mn_scraper_parallel.py --workers 8 --start 1 --end 800000

# Run with 4 workers (lighter on resources)
python mn_scraper_parallel.py --workers 4 --start 1 --end 800000

# Resume from where you left off (auto-detects progress per worker)
python mn_scraper_parallel.py --workers 8 --start 1 --end 800000

# Run with visible browsers (for debugging)
python mn_scraper_parallel.py --workers 2 --visible

# After scraping, merge all worker outputs
python merge_results.py --all
```

Each worker saves to `output/businesses_worker_N.csv` and tracks progress in `progress_worker_N.json`.

### Single-Threaded Scraper
```bash
# Start from beginning (or resume from progress.json)
python mn_scraper.py

# Start from specific file number
python mn_scraper.py --start 100000

# Start fresh (ignore saved progress)
python mn_scraper.py --no-resume

# Run with visible browser (for debugging)
python mn_scraper.py --visible
```

### Test Specific File Numbers
Edit `test_scraper.py` to set file numbers, then:
```bash
python test_scraper.py
```

### Discover Business Types
```bash
# Sample 5 numbers per digit range
python discover_business_types.py 5

# Focused discovery (more samples in productive ranges)
python discover_focused.py 50
```

## Business Types Supported

| Type | Example File # | Unique Fields |
|------|----------------|---------------|
| Assumed Name | 1, 124000 | Principal Place of Business, Applicant |
| Trademark | 7 | Mark Type, Markholder |
| Trademark - Service Mark | 8347 | Mark Type, Markholder |
| Business Corporation (Domestic) | 768883700027 | Shares, CEO, Reg Office, Exec Office |
| Business Corporation (Foreign) | 22400 | Reg Office, Registered Agent |
| Limited Liability Company (Domestic) | 1349132200021 | Manager, Reg Office, Exec Office |
| Limited Liability Company (Foreign) | - | Same as Domestic LLC |
| Limited Partnership | - | Similar structure |
| Nonprofit Corporation | - | Similar structure |
| Cooperative | - | Similar structure |

## Output CSV Columns (51 total)

### Basic Info
- `file_number` - Unique identifier
- `business_name` - Legal name
- `mn_statute` - Minnesota statute reference (e.g., 302A, 322C, 333)
- `business_type` - Entity type
- `home_jurisdiction` - State of formation
- `filing_date` - Original filing date
- `status` - Active/Inactive
- `renewal_due_date` - Next renewal date
- `mark_type` - For trademarks
- `number_of_shares` - For corporations
- `chief_executive_officer` - For corporations
- `manager` - For LLCs

### Principal Place of Business Address (Assumed Names)
- `principal_street_number`, `principal_street_name`, `principal_street_type`
- `principal_street_direction`, `principal_unit`
- `principal_city`, `principal_state`, `principal_zip`
- `principal_address_raw` - Original unparsed address

### Registered Office Address (Corporations/LLCs)
- `reg_office_street_number`, `reg_office_street_name`, `reg_office_street_type`
- `reg_office_street_direction`, `reg_office_unit`
- `reg_office_city`, `reg_office_state`, `reg_office_zip`
- `reg_office_address_raw`

### Principal Executive Office Address (Corporations/LLCs)
- `exec_office_street_number`, `exec_office_street_name`, `exec_office_street_type`
- `exec_office_street_direction`, `exec_office_unit`
- `exec_office_city`, `exec_office_state`, `exec_office_zip`
- `exec_office_address_raw`

### Applicant/Markholder Info
- `applicant_name` - Person/entity name
- `applicant_street_number`, `applicant_street_name`, `applicant_street_type`
- `applicant_street_direction`, `applicant_unit`
- `applicant_city`, `applicant_state`, `applicant_zip`
- `applicant_address_raw`

### Other
- `registered_agent_name` - Registered agent
- `filing_history` - Filing events (semicolon-separated)
- `scraped_at` - Timestamp

## Configuration (config.py)

```python
START_FILE_NUMBER = 1           # Starting file number
MAX_CONSECUTIVE_MISSES = 100    # Stop after this many not-found
REQUEST_DELAY = 1.5             # Seconds between requests
DELAY_JITTER = 0.5              # Random jitter (0 to this value)
HEADLESS = True                 # Run browser invisibly
TIMEOUT = 30000                 # Page timeout (ms)
MAX_RETRIES = 3                 # Retry attempts per request
```

## File Number Patterns

- **1 - ~300,000**: Sequential numbers (older filings from 1970s-2000s)
- **12-13 digit numbers**: Newer format (recent corporations, LLCs)
  - Example: `768883700027` (2014 Domestic Corp)
  - Example: `1349132200021` (2022 Domestic LLC)

## Merging & Filtering Results

### Merge Worker Outputs
```bash
# Merge only parallel worker outputs
python merge_results.py

# Merge ALL outputs (workers + single-threaded)
python merge_results.py --all
```

### Filter by Filing Year
```bash
# Get businesses filed since 2019
python filter_recent.py 2019

# Get businesses filed since 2020
python filter_recent.py 2020
```

## Resuming After Interruption

### Parallel Scraper
Each worker saves progress to `progress_worker_N.json`. Simply re-run with the same parameters:
```bash
python mn_scraper_parallel.py --workers 8 --start 1 --end 800000
```

### Single-Threaded Scraper
Progress is saved to `progress.json` every 10 file numbers:
```bash
# Simply run again - it will auto-resume
python mn_scraper.py

# Or force start from specific number
python mn_scraper.py --start 50000
```

## Rate Limiting

- Default: 1.5 second delay + 0-0.5s random jitter between requests
- Adjust in `config.py` if needed
- The site may temporarily block if requests are too fast

## Splitting Large Output

If the CSV gets too large, you can split by file number ranges:

```bash
# Run for specific ranges
python mn_scraper.py --start 1 --no-resume      # Creates businesses.csv
# Rename and continue
mv output/businesses.csv output/businesses_1_to_100000.csv
python mn_scraper.py --start 100001 --no-resume
```

## Troubleshooting

### Connection Reset Errors
The site may rate-limit. Wait a few minutes and retry.

### Browser Not Found
Run `playwright install chromium` to install the browser.

### Timeout Errors
Increase `TIMEOUT` in `config.py` or check your internet connection.

## License

For educational and research purposes. Respect the Minnesota SOS website's terms of service.
