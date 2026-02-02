"""
===============================================================================
UNIT TESTS FOR mn_scraper.py
===============================================================================

This file contains unit tests for the core scraper module. It tests the
helper functions and class methods that don't require a live browser.

WHAT WE TEST:
-------------
1. Date conversion (convert_date_to_iso)
2. Address parsing (parse_address)
3. Progress file handling (save/load)
4. CSV initialization

WHAT WE DON'T TEST HERE:
------------------------
- Browser automation (requires Playwright and live website)
- Actual scraping (would need mock data or integration tests)

RUNNING THESE TESTS:
--------------------
    pytest tests/test_mn_scraper.py -v

===============================================================================
"""

import json
import os
import sys
import tempfile
from pathlib import Path

import pytest
import pandas as pd

# Add the parent directory to Python's path so we can import our modules
# This allows running tests from any directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from mn_scraper import convert_date_to_iso, parse_address, MNBusinessScraper


# =============================================================================
# DATE CONVERSION TESTS
# =============================================================================

class TestConvertDateToISO:
    """
    Tests for the convert_date_to_iso() function.

    This function converts dates from MM/DD/YYYY format to YYYY-MM-DD format.
    These tests verify it handles various input formats correctly.
    """

    def test_standard_format(self):
        """Test conversion of standard MM/DD/YYYY format."""
        assert convert_date_to_iso("01/15/2024") == "2024-01-15"
        assert convert_date_to_iso("12/31/2023") == "2023-12-31"
        assert convert_date_to_iso("06/01/2020") == "2020-06-01"

    def test_single_digit_month_day(self):
        """Test conversion when month/day are single digits."""
        # Python's strptime handles both "01/05" and "1/5" with %m/%d
        assert convert_date_to_iso("1/5/2024") == "2024-01-05"
        assert convert_date_to_iso("3/15/2023") == "2023-03-15"

    def test_empty_input(self):
        """Test that empty input returns empty string."""
        assert convert_date_to_iso("") == ""
        assert convert_date_to_iso("   ") == ""
        assert convert_date_to_iso(None) == ""

    def test_already_iso_format(self):
        """Test that ISO format dates are returned as-is."""
        assert convert_date_to_iso("2024-01-15") == "2024-01-15"
        assert convert_date_to_iso("2023-12-31") == "2023-12-31"

    def test_iso_with_extra_content(self):
        """Test ISO dates with extra content (like timestamps)."""
        # Should return just the date portion
        assert convert_date_to_iso("2024-01-15T10:30:00") == "2024-01-15"

    def test_whitespace_handling(self):
        """Test that whitespace is properly stripped."""
        assert convert_date_to_iso("  01/15/2024  ") == "2024-01-15"
        assert convert_date_to_iso("\t12/31/2023\n") == "2023-12-31"

    def test_invalid_format_returns_original(self):
        """Test that unparseable formats are returned unchanged."""
        assert convert_date_to_iso("January 15, 2024") == "January 15, 2024"
        assert convert_date_to_iso("15-01-2024") == "15-01-2024"


# =============================================================================
# ADDRESS PARSING TESTS
# =============================================================================

class TestParseAddress:
    """
    Tests for the parse_address() function.

    This function breaks down address strings into their component parts.
    These tests verify correct parsing of various address formats.
    """

    def test_simple_address(self):
        """Test parsing a simple street address."""
        result = parse_address("123 Main Street\nMinneapolis, MN 55401")
        assert result['street_number'] == "123"
        assert result['street_name'] == "Main"
        assert result['street_type'] == "Street"
        assert result['city'] == "Minneapolis"
        assert result['state'] == "MN"
        assert result['zip'] == "55401"

    def test_address_with_direction(self):
        """Test parsing address with directional suffix."""
        result = parse_address("456 Oak Ave NE\nSt Paul, MN 55102")
        assert result['street_number'] == "456"
        assert result['street_name'] == "Oak"
        assert result['street_type'] == "Ave"
        assert result['street_direction'] == "NE"
        assert result['city'] == "St Paul"
        assert result['state'] == "MN"
        assert result['zip'] == "55102"

    def test_address_with_unit(self):
        """Test parsing address with suite/unit number."""
        result = parse_address("789 Broadway Blvd\nSte 200\nRochester, MN 55901")
        assert result['street_number'] == "789"
        assert result['street_name'] == "Broadway"
        assert result['street_type'] == "Blvd"
        assert result['unit'] == "Ste 200"
        assert result['city'] == "Rochester"

    def test_comma_separated_address(self):
        """Test parsing single-line comma-separated address."""
        result = parse_address("123 First Ave, Duluth, MN 55802")
        assert result['street_number'] == "123"
        assert result['street_name'] == "First"
        assert result['street_type'] == "Ave"
        assert result['city'] == "Duluth"
        assert result['state'] == "MN"
        assert result['zip'] == "55802"

    def test_address_with_extended_zip(self):
        """Test parsing address with ZIP+4."""
        result = parse_address("100 State St\nSt Cloud, MN 56301-1234")
        assert result['zip'] == "56301-1234"

    def test_address_filters_usa(self):
        """Test that USA/US country line is filtered out."""
        result = parse_address("123 Main St\nCity, MN 55401\nUSA")
        assert result['city'] == "City"
        assert result['state'] == "MN"

    def test_empty_input(self):
        """Test that empty input returns empty dict values."""
        result = parse_address("")
        assert result['street_number'] == ""
        assert result['street_name'] == ""
        assert result['city'] == ""
        assert result['state'] == ""
        assert result['zip'] == ""

    def test_none_input(self):
        """Test that None input returns empty dict values."""
        result = parse_address(None)
        assert result['street_number'] == ""
        assert result['city'] == ""

    def test_address_without_state_zip(self):
        """Test parsing incomplete address."""
        result = parse_address("123 Unknown Lane")
        assert result['street_number'] == "123"
        assert result['street_name'] == "Unknown"
        assert result['street_type'] == "Lane"
        # City/state/zip should be empty
        assert result['city'] == ""
        assert result['state'] == ""

    def test_hyphenated_street_number(self):
        """Test parsing address with hyphenated street number."""
        result = parse_address("123-125 Twin Ave\nCity, MN 55401")
        assert result['street_number'] == "123-125"
        assert result['street_name'] == "Twin"


# =============================================================================
# SCRAPER CLASS TESTS
# =============================================================================

class TestMNBusinessScraper:
    """
    Tests for the MNBusinessScraper class.

    These tests focus on non-browser functionality like file handling.
    """

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    @pytest.fixture
    def mock_config(self, temp_dir, monkeypatch):
        """
        Mock the config module values for testing.

        monkeypatch is a pytest fixture that lets us temporarily change values.
        """
        import config
        monkeypatch.setattr(config, 'OUTPUT_DIR', temp_dir)
        monkeypatch.setattr(config, 'OUTPUT_FILE', 'test_businesses.csv')
        monkeypatch.setattr(config, 'PROGRESS_FILE', os.path.join(temp_dir, 'test_progress.json'))
        monkeypatch.setattr(config, 'START_FILE_NUMBER', 1000)
        monkeypatch.setattr(config, 'HEADLESS', True)
        return temp_dir

    def test_init_creates_output_directory(self, mock_config):
        """Test that __init__ creates output directory if it doesn't exist."""
        scraper = MNBusinessScraper(start_number=1000)
        assert scraper.output_dir.exists()

    def test_init_sets_start_number(self, mock_config):
        """Test that start_number is properly set."""
        scraper = MNBusinessScraper(start_number=5000)
        assert scraper.start_number == 5000

    def test_init_default_start_number(self, mock_config):
        """Test that default start_number comes from config."""
        scraper = MNBusinessScraper()
        assert scraper.start_number == 1000  # From mock_config

    def test_init_csv_creates_file(self, mock_config):
        """Test that init_csv creates a CSV with headers."""
        scraper = MNBusinessScraper()
        scraper.init_csv()

        assert scraper.output_file.exists()

        # Read the file and check it has headers
        df = pd.read_csv(scraper.output_file)
        assert 'file_number' in df.columns
        assert 'business_name' in df.columns
        assert 'business_type' in df.columns

    def test_save_and_load_progress(self, mock_config):
        """Test saving and loading progress."""
        scraper = MNBusinessScraper()

        # Save progress
        scraper.save_progress(2500)

        # Load progress
        loaded = scraper.load_progress()
        assert loaded == 2500

    def test_load_progress_no_file(self, mock_config):
        """Test loading progress when no file exists."""
        scraper = MNBusinessScraper(start_number=1000)

        # No progress file exists, should return start_number
        loaded = scraper.load_progress()
        assert loaded == 1000

    def test_append_to_csv(self, mock_config):
        """Test appending data to CSV."""
        scraper = MNBusinessScraper()
        scraper.init_csv()

        # Append a record
        test_data = {
            'file_number': 12345,
            'business_name': 'Test Business LLC',
            'business_type': 'LLC Domestic',
            'status': 'Active',
            'filing_date': '2024-01-15',
        }
        scraper.append_to_csv(test_data)

        # Read back and verify
        df = pd.read_csv(scraper.output_file)
        assert len(df) == 1
        assert df.iloc[0]['file_number'] == 12345
        assert df.iloc[0]['business_name'] == 'Test Business LLC'

    def test_columns_defined(self, mock_config):
        """Test that all expected columns are defined."""
        scraper = MNBusinessScraper()

        # Check for key columns
        assert 'file_number' in scraper.columns
        assert 'business_name' in scraper.columns
        assert 'business_type' in scraper.columns
        assert 'filing_date' in scraper.columns
        assert 'status' in scraper.columns
        assert 'reg_office_city' in scraper.columns
        assert 'scraped_at' in scraper.columns


# =============================================================================
# EDGE CASE TESTS
# =============================================================================

class TestEdgeCases:
    """
    Tests for edge cases and error handling.

    These tests verify the code handles unusual inputs gracefully.
    """

    def test_parse_address_multiple_commas(self):
        """Test address with multiple commas in city name."""
        # Some cities have commas (e.g., "St. Paul, East")
        result = parse_address("123 Main St, St Paul, MN 55101")
        assert result['state'] == "MN"

    def test_parse_address_all_caps(self):
        """Test parsing all-caps address (common in official records)."""
        result = parse_address("123 MAIN STREET NE\nMINNEAPOLIS, MN 55401")
        assert result['street_number'] == "123"
        assert result['street_name'] == "MAIN"
        assert result['street_direction'] == "NE"

    def test_parse_address_lowercase(self):
        """Test parsing lowercase address."""
        result = parse_address("123 main street\nminneapolis, mn 55401")
        assert result['street_type'] == "street"
        assert result['state'] == "MN"  # Should be uppercased

    def test_convert_date_very_old(self):
        """Test date conversion for very old dates."""
        assert convert_date_to_iso("01/01/1881") == "1881-01-01"

    def test_convert_date_future(self):
        """Test date conversion for future dates."""
        assert convert_date_to_iso("12/31/2030") == "2030-12-31"


# =============================================================================
# MAIN - Run tests if executed directly
# =============================================================================

if __name__ == '__main__':
    # This allows running tests directly: python tests/test_mn_scraper.py
    pytest.main([__file__, '-v'])
