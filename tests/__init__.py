"""
===============================================================================
MINNESOTA BUSINESS SCRAPER - TEST SUITE
===============================================================================

This package contains unit tests for the MN Business Scraper project.

RUNNING TESTS:
--------------
From the project root directory:

    # Run all tests
    pytest tests/

    # Run with verbose output
    pytest tests/ -v

    # Run a specific test file
    pytest tests/test_mn_scraper.py

    # Run a specific test function
    pytest tests/test_mn_scraper.py::test_convert_date_to_iso

    # Run tests with coverage report
    pytest tests/ --cov=. --cov-report=html

TEST STRUCTURE:
---------------
- test_mn_scraper.py: Tests for the core mn_scraper.py module
- test_search_by_name_parallel.py: Tests for the parallel scraper

FOR JUNIOR DEVELOPERS:
----------------------
Unit tests verify that individual pieces of code work correctly.
Each test function should:
1. Set up the test conditions (Arrange)
2. Run the code being tested (Act)
3. Check that the result is correct (Assert)

Example:
    def test_addition():
        # Arrange
        a = 2
        b = 3

        # Act
        result = a + b

        # Assert
        assert result == 5, "2 + 3 should equal 5"
===============================================================================
"""
