#!/usr/bin/env python3
"""
===============================================================================
MINNESOTA BUSINESS SCRAPER - WEB DASHBOARD
===============================================================================

A simple Flask web dashboard for viewing scraping progress and data statistics.

PURPOSE:
--------
This dashboard provides a web interface to:
1. View current scraping progress
2. See statistics about scraped data (counts by year, type, etc.)
3. Monitor scraper health (last activity, errors)
4. Download data exports

RUNNING THE DASHBOARD:
----------------------
    # Basic usage (runs on http://localhost:5000)
    python dashboard.py

    # Specify a different port
    python dashboard.py --port 8080

    # Run in debug mode (auto-reloads on code changes)
    python dashboard.py --debug

    # Make accessible from other computers on network
    python dashboard.py --host 0.0.0.0

REQUIREMENTS:
-------------
    pip install flask pandas

FOR JUNIOR DEVELOPERS:
----------------------
Flask is a lightweight web framework for Python. Key concepts:

1. ROUTES: The @app.route() decorator maps URLs to Python functions.
   Example: @app.route('/') maps the homepage to the index() function.

2. TEMPLATES: HTML files in templates/ folder. Flask uses Jinja2 templating
   which allows embedding Python expressions in HTML.

3. STATIC FILES: CSS, JavaScript, and images go in the static/ folder.

4. REQUEST/RESPONSE: When a user visits a URL, Flask calls the mapped function
   and returns whatever that function returns (usually HTML or JSON).

===============================================================================
"""

import argparse
import json
import os
from datetime import datetime
from pathlib import Path

# Flask is a web framework - install with: pip install flask
from flask import Flask, render_template_string, jsonify

# Pandas for data analysis - install with: pip install pandas
import pandas as pd


# =============================================================================
# FLASK APP SETUP
# =============================================================================

# Create the Flask application
# __name__ tells Flask where to find templates and static files
app = Flask(__name__)

# Configuration - adjust these paths for your setup
DATA_DIR = Path(__file__).parent / 'data'
OUTPUT_DIR = Path(__file__).parent / 'output'
PROGRESS_DIR = Path(__file__).parent


# =============================================================================
# HTML TEMPLATES
# =============================================================================
# For simplicity, we're embedding the HTML directly in this file.
# In a larger project, you'd put these in a templates/ folder.

# Main dashboard template
DASHBOARD_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MN Business Scraper Dashboard</title>
    <style>
        /* Basic CSS styling */
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background-color: #f5f5f5;
            color: #333;
            line-height: 1.6;
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }

        header {
            background-color: #003366;
            color: white;
            padding: 20px;
            margin-bottom: 20px;
        }

        header h1 {
            font-size: 24px;
        }

        header p {
            opacity: 0.8;
            font-size: 14px;
        }

        .card {
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }

        .card h2 {
            color: #003366;
            margin-bottom: 15px;
            font-size: 18px;
            border-bottom: 2px solid #eee;
            padding-bottom: 10px;
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
        }

        .stat-box {
            background: linear-gradient(135deg, #003366, #004080);
            color: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }

        .stat-box .number {
            font-size: 36px;
            font-weight: bold;
        }

        .stat-box .label {
            font-size: 14px;
            opacity: 0.9;
        }

        table {
            width: 100%;
            border-collapse: collapse;
        }

        table th, table td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }

        table th {
            background-color: #f8f9fa;
            font-weight: 600;
        }

        table tr:hover {
            background-color: #f8f9fa;
        }

        .status-active {
            color: #28a745;
            font-weight: bold;
        }

        .status-idle {
            color: #ffc107;
        }

        .status-error {
            color: #dc3545;
        }

        .progress-bar {
            width: 100%;
            height: 20px;
            background-color: #e9ecef;
            border-radius: 10px;
            overflow: hidden;
        }

        .progress-bar-fill {
            height: 100%;
            background: linear-gradient(90deg, #003366, #0066cc);
            border-radius: 10px;
            transition: width 0.3s ease;
        }

        .last-updated {
            color: #666;
            font-size: 12px;
            margin-top: 10px;
        }

        .refresh-btn {
            background-color: #003366;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 5px;
            cursor: pointer;
            font-size: 14px;
        }

        .refresh-btn:hover {
            background-color: #004080;
        }

        footer {
            text-align: center;
            padding: 20px;
            color: #666;
            font-size: 12px;
        }
    </style>
</head>
<body>
    <header>
        <div class="container">
            <h1>Minnesota Business Scraper Dashboard</h1>
            <p>Real-time monitoring of business data collection</p>
        </div>
    </header>

    <div class="container">
        <!-- Overview Statistics -->
        <div class="stats-grid">
            <div class="stat-box">
                <div class="number">{{ total_records | default(0) | int }}</div>
                <div class="label">Total Records</div>
            </div>
            <div class="stat-box">
                <div class="number">{{ patterns_completed | default(0) }}</div>
                <div class="label">Patterns Completed</div>
            </div>
            <div class="stat-box">
                <div class="number">{{ active_workers | default(0) }}</div>
                <div class="label">Active Workers</div>
            </div>
            <div class="stat-box">
                <div class="number">{{ years_scraped | default(0) }}</div>
                <div class="label">Years Covered</div>
            </div>
        </div>

        <!-- Progress Card -->
        <div class="card">
            <h2>Scraping Progress</h2>
            <div class="progress-bar">
                <div class="progress-bar-fill" style="width: {{ progress_pct | default(0) }}%;"></div>
            </div>
            <p style="margin-top: 10px;">{{ progress_pct | default(0) }}% complete ({{ patterns_completed | default(0) }}/676 patterns)</p>
            <p class="last-updated">Last updated: {{ last_updated | default('Never') }}</p>
        </div>

        <!-- Records by Year -->
        <div class="card">
            <h2>Records by Year</h2>
            <table>
                <thead>
                    <tr>
                        <th>Year</th>
                        <th>Record Count</th>
                        <th>Percentage</th>
                    </tr>
                </thead>
                <tbody>
                    {% for year in years_data %}
                    <tr>
                        <td>{{ year.year }}</td>
                        <td>{{ year.count | int }}</td>
                        <td>{{ year.pct }}%</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <!-- Records by Type -->
        <div class="card">
            <h2>Records by Business Type</h2>
            <table>
                <thead>
                    <tr>
                        <th>Business Type</th>
                        <th>Record Count</th>
                        <th>Percentage</th>
                    </tr>
                </thead>
                <tbody>
                    {% for type in types_data %}
                    <tr>
                        <td>{{ type.type }}</td>
                        <td>{{ type.count | int }}</td>
                        <td>{{ type.pct }}%</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <!-- Worker Status -->
        <div class="card">
            <h2>Worker Status</h2>
            <table>
                <thead>
                    <tr>
                        <th>Worker ID</th>
                        <th>Current Pattern</th>
                        <th>Records Found</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    {% for worker in workers_data %}
                    <tr>
                        <td>Worker {{ worker.id }}</td>
                        <td>{{ worker.pattern }}</td>
                        <td>{{ worker.records | int }}</td>
                        <td class="status-{{ worker.status }}">{{ worker.status | title }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <!-- Actions -->
        <div class="card">
            <h2>Actions</h2>
            <button class="refresh-btn" onclick="location.reload();">Refresh Data</button>
            <a href="/api/stats" target="_blank">
                <button class="refresh-btn" style="margin-left: 10px;">View Raw JSON</button>
            </a>
        </div>
    </div>

    <footer>
        <p>MN Business Scraper Dashboard | Data updates every 4 hours</p>
    </footer>

    <!-- Auto-refresh every 60 seconds -->
    <script>
        setTimeout(function() {
            location.reload();
        }, 60000);
    </script>
</body>
</html>
"""


# =============================================================================
# DATA LOADING FUNCTIONS
# =============================================================================

def load_csv_data():
    """
    Load the main business data from CSV files.

    Tries to find the businesses.csv file in either data/ or output/ directory.

    RETURNS:
    --------
    pandas.DataFrame or None
        The business data, or None if no file found.
    """
    # Try different possible locations for the CSV
    possible_paths = [
        DATA_DIR / 'businesses.csv',
        OUTPUT_DIR / 'businesses.csv',
        Path(__file__).parent / 'businesses.csv',
    ]

    for path in possible_paths:
        if path.exists():
            try:
                df = pd.read_csv(path)
                return df
            except Exception as e:
                print(f"Error loading {path}: {e}")

    return None


def load_progress_files():
    """
    Load progress information from worker progress files.

    Looks for files matching pattern: progress_alpha_worker_*.json

    RETURNS:
    --------
    list
        List of dictionaries with worker progress info.
    """
    workers = []
    progress_pattern = PROGRESS_DIR / 'progress_alpha_worker_*.json'

    # Find all progress files
    for i in range(20):  # Check workers 0-19
        progress_file = PROGRESS_DIR / f'progress_alpha_worker_{i}.json'
        if progress_file.exists():
            try:
                with open(progress_file, 'r') as f:
                    data = json.load(f)
                    workers.append({
                        'id': i,
                        'pattern': data.get('last_pattern', 'N/A'),
                        'completed': data.get('completed_patterns', []),
                        'records': 0,  # Would need to count from worker CSV
                        'status': 'idle' if data.get('completed', False) else 'active',
                        'updated_at': data.get('updated_at', '')
                    })
            except Exception as e:
                print(f"Error loading progress file {i}: {e}")

    return workers


def calculate_stats(df):
    """
    Calculate statistics from the business data.

    PARAMETERS:
    -----------
    df : pandas.DataFrame
        The business data.

    RETURNS:
    --------
    dict
        Dictionary with calculated statistics.
    """
    if df is None or len(df) == 0:
        return {
            'total_records': 0,
            'years_data': [],
            'types_data': [],
        }

    stats = {
        'total_records': len(df),
    }

    # Count by year (extract year from filing_date)
    if 'filing_date' in df.columns:
        df['year'] = pd.to_datetime(df['filing_date'], errors='coerce').dt.year
        year_counts = df['year'].value_counts().sort_index(ascending=False)

        years_data = []
        for year, count in year_counts.head(10).items():
            if pd.notna(year):
                pct = round(count / len(df) * 100, 1)
                years_data.append({
                    'year': int(year),
                    'count': count,
                    'pct': pct
                })
        stats['years_data'] = years_data

    # Count by business type
    if 'business_type' in df.columns:
        type_counts = df['business_type'].value_counts()

        types_data = []
        for btype, count in type_counts.head(10).items():
            pct = round(count / len(df) * 100, 1)
            types_data.append({
                'type': str(btype),
                'count': count,
                'pct': pct
            })
        stats['types_data'] = types_data

    return stats


# =============================================================================
# FLASK ROUTES
# =============================================================================

@app.route('/')
def index():
    """
    Main dashboard page.

    This is the homepage that shows all the statistics and progress.
    """
    # Load data
    df = load_csv_data()
    workers = load_progress_files()

    # Calculate stats
    stats = calculate_stats(df)

    # Calculate progress
    total_patterns = 676  # aa to zz
    completed_patterns = sum(len(w.get('completed', [])) for w in workers)
    progress_pct = round(completed_patterns / total_patterns * 100, 1) if total_patterns > 0 else 0

    # Get unique years for years_scraped count
    years_scraped = len(stats.get('years_data', []))

    # Format worker data for display
    workers_data = []
    for w in workers:
        workers_data.append({
            'id': w['id'],
            'pattern': w.get('pattern', 'N/A'),
            'records': w.get('records', 0),
            'status': w.get('status', 'idle')
        })

    # Render the template
    return render_template_string(
        DASHBOARD_TEMPLATE,
        total_records=stats.get('total_records', 0),
        patterns_completed=completed_patterns,
        progress_pct=progress_pct,
        active_workers=sum(1 for w in workers if w.get('status') == 'active'),
        years_scraped=years_scraped,
        years_data=stats.get('years_data', []),
        types_data=stats.get('types_data', []),
        workers_data=workers_data,
        last_updated=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    )


@app.route('/api/stats')
def api_stats():
    """
    API endpoint that returns statistics as JSON.

    Useful for programmatic access or building other dashboards.

    RETURNS:
    --------
    JSON response with all statistics.
    """
    df = load_csv_data()
    workers = load_progress_files()
    stats = calculate_stats(df)

    # Add progress info
    total_patterns = 676
    completed_patterns = sum(len(w.get('completed', [])) for w in workers)
    stats['patterns_completed'] = completed_patterns
    stats['patterns_total'] = total_patterns
    stats['progress_pct'] = round(completed_patterns / total_patterns * 100, 1) if total_patterns > 0 else 0
    stats['workers'] = workers
    stats['timestamp'] = datetime.now().isoformat()

    return jsonify(stats)


@app.route('/api/health')
def api_health():
    """
    Health check endpoint.

    Returns 200 OK if the dashboard is running.
    Useful for monitoring systems.
    """
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    })


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """
    Start the Flask dashboard server.

    COMMAND LINE ARGUMENTS:
    -----------------------
    --host : str
        Host to bind to (default: 127.0.0.1)
        Use 0.0.0.0 to allow external connections

    --port : int
        Port to run on (default: 5000)

    --debug : flag
        Enable debug mode (auto-reload on changes)
    """
    parser = argparse.ArgumentParser(description='MN Business Scraper Dashboard')
    parser.add_argument(
        '--host',
        default='127.0.0.1',
        help='Host to bind to (default: 127.0.0.1, use 0.0.0.0 for external)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=5000,
        help='Port to run on (default: 5000)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode'
    )

    args = parser.parse_args()

    print(f"""
    ╔══════════════════════════════════════════════════════════════╗
    ║       MN Business Scraper Dashboard                          ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  Dashboard URL: http://{args.host}:{args.port}                        ║
    ║  API Stats:     http://{args.host}:{args.port}/api/stats              ║
    ║  Health Check:  http://{args.host}:{args.port}/api/health             ║
    ║                                                              ║
    ║  Press Ctrl+C to stop the server                             ║
    ╚══════════════════════════════════════════════════════════════╝
    """)

    # Run the Flask app
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == '__main__':
    main()
