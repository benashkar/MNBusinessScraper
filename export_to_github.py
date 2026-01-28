#!/usr/bin/env python3
"""
Export scraped business data to CSV, JSON, and SQL, then push to GitHub.

Runs automatically every 4 hours when used with --auto flag.
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

import pandas as pd

# Auto-save interval (4 hours in seconds)
AUTO_SAVE_INTERVAL = 4 * 60 * 60


def merge_csv_files(output_dir: Path, patterns: list):
    """Merge all worker CSV files into a single DataFrame."""
    all_files = []
    for pattern in patterns:
        all_files.extend(list(output_dir.glob(pattern)))

    if not all_files:
        print(f"No CSV files found in {output_dir}")
        return None

    print(f"Found {len(all_files)} CSV files to merge")

    dfs = []
    for f in all_files:
        try:
            df = pd.read_csv(f, low_memory=False)
            dfs.append(df)
            print(f"  {f.name}: {len(df)} records")
        except Exception as e:
            print(f"  Error reading {f.name}: {e}")

    if not dfs:
        return None

    merged = pd.concat(dfs, ignore_index=True)

    # Remove duplicates based on file_number
    if 'file_number' in merged.columns:
        before = len(merged)
        merged = merged.drop_duplicates(subset=['file_number'], keep='last')
        after = len(merged)
        if before != after:
            print(f"Removed {before - after} duplicates")

    return merged


def export_csv(df: pd.DataFrame, output_path: Path):
    """Export DataFrame to CSV file."""
    df.to_csv(output_path, index=False)
    print(f"Exported {len(df)} records to {output_path}")


def export_json(df: pd.DataFrame, output_path: Path):
    """Export DataFrame to JSON file."""
    records = df.to_dict(orient='records')

    # Clean up NaN values
    for record in records:
        for key, value in list(record.items()):
            if pd.isna(value):
                record[key] = None

    output = {
        "metadata": {
            "total_records": len(records),
            "exported_at": datetime.now().isoformat(),
            "source": "Minnesota Secretary of State Business Search",
            "url": "https://mblsportal.sos.mn.gov/Business/Search"
        },
        "businesses": records
    }

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False)

    print(f"Exported {len(records)} records to {output_path}")


def export_sql(df: pd.DataFrame, output_path: Path):
    """Export DataFrame to MySQL-compatible SQL file."""

    # SQL column type mapping
    column_types = {
        'file_number': 'VARCHAR(100)',
        'business_name': 'VARCHAR(500)',
        'mn_statute': 'VARCHAR(20)',
        'business_type': 'VARCHAR(100)',
        'home_jurisdiction': 'VARCHAR(100)',
        'filing_date': 'DATE',
        'status': 'VARCHAR(50)',
        'renewal_due_date': 'DATE',
        'mark_type': 'VARCHAR(50)',
        'number_of_shares': 'VARCHAR(50)',
        'chief_executive_officer': 'VARCHAR(200)',
        'manager': 'VARCHAR(200)',
        'registered_agent_name': 'VARCHAR(200)',
        'filing_history': 'TEXT',
        'scraped_at': 'DATE',
    }

    # Address fields pattern
    address_fields = ['street_number', 'street_name', 'street_type', 'street_direction',
                      'unit', 'city', 'state', 'zip', 'address_raw']
    address_prefixes = ['principal_', 'reg_office_', 'exec_office_', 'applicant_']

    for prefix in address_prefixes:
        for field in address_fields:
            col = prefix + field
            if 'raw' in field:
                column_types[col] = 'TEXT'
            elif field in ['city', 'street_name']:
                column_types[col] = 'VARCHAR(200)'
            else:
                column_types[col] = 'VARCHAR(100)'

    column_types['applicant_name'] = 'VARCHAR(200)'

    lines = []
    lines.append("-- Minnesota Business Data Export")
    lines.append(f"-- Generated: {datetime.now().isoformat()}")
    lines.append(f"-- Total Records: {len(df)}")
    lines.append("")
    lines.append("SET NAMES utf8mb4;")
    lines.append("SET CHARACTER SET utf8mb4;")
    lines.append("")

    # DROP and CREATE TABLE
    lines.append("DROP TABLE IF EXISTS mn_businesses;")
    lines.append("")
    lines.append("CREATE TABLE mn_businesses (")
    lines.append("    id INT AUTO_INCREMENT PRIMARY KEY,")

    col_defs = []
    for col in df.columns:
        col_type = column_types.get(col, 'TEXT')
        col_defs.append(f"    `{col}` {col_type}")

    lines.append(",\n".join(col_defs))
    lines.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;")
    lines.append("")

    # CREATE INDEX
    lines.append("CREATE INDEX idx_file_number ON mn_businesses(file_number(50));")
    lines.append("CREATE INDEX idx_business_name ON mn_businesses(business_name(100));")
    lines.append("CREATE INDEX idx_filing_date ON mn_businesses(filing_date);")
    lines.append("CREATE INDEX idx_status ON mn_businesses(status);")
    lines.append("CREATE INDEX idx_business_type ON mn_businesses(business_type);")
    lines.append("")

    # INSERT statements (batch of 100)
    lines.append("-- Data")
    batch_size = 100

    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]

        cols = ", ".join([f"`{c}`" for c in df.columns])
        lines.append(f"INSERT INTO mn_businesses ({cols}) VALUES")

        values = []
        for _, row in batch.iterrows():
            row_values = []
            for col in df.columns:
                val = row[col]
                if pd.isna(val):
                    row_values.append("NULL")
                elif isinstance(val, (int, float)):
                    row_values.append(str(val))
                else:
                    # Escape quotes and backslashes
                    escaped = str(val).replace("\\", "\\\\").replace("'", "\\'")
                    row_values.append(f"'{escaped}'")
            values.append(f"({', '.join(row_values)})")

        lines.append(",\n".join(values) + ";")
        lines.append("")

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))

    print(f"Exported {len(df)} records to {output_path}")


def git_commit_and_push(repo_dir: Path, message: str):
    """Commit data files and push to GitHub."""
    import os
    original_dir = os.getcwd()

    try:
        os.chdir(repo_dir)

        # Add data directory
        subprocess.run(['git', 'add', 'data/'], capture_output=True)

        # Check for changes
        result = subprocess.run(['git', 'status', '--porcelain', 'data/'], capture_output=True, text=True)
        if not result.stdout.strip():
            print("No changes to commit")
            return False

        # Commit
        subprocess.run(['git', 'commit', '-m', message], capture_output=True)
        print(f"Committed: {message}")

        # Push
        result = subprocess.run(['git', 'push'], capture_output=True, text=True)
        if result.returncode == 0:
            print("Pushed to GitHub successfully")
        else:
            print(f"Push output: {result.stderr}")

        return True

    except Exception as e:
        print(f"Git error: {e}")
        return False
    finally:
        os.chdir(original_dir)


def run_export(repo_dir: Path, push: bool = True):
    """Run the full export process."""
    output_dir = repo_dir / 'output'
    data_dir = repo_dir / 'data'
    data_dir.mkdir(exist_ok=True)

    print(f"\n{'='*60}")
    print(f"EXPORT STARTED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print('='*60)

    # Merge all CSV files (both alphabetical and historical workers)
    patterns = [
        'businesses_alpha_worker_*.csv',
        'businesses_worker_*.csv',
    ]

    df = merge_csv_files(output_dir, patterns)

    # Also include main files if they exist
    for main_file in ['businesses.csv', 'businesses_all.csv']:
        main_path = output_dir / main_file
        if main_path.exists():
            try:
                main_df = pd.read_csv(main_path, low_memory=False)
                print(f"Including {main_file}: {len(main_df)} records")
                if df is not None:
                    df = pd.concat([df, main_df], ignore_index=True)
                else:
                    df = main_df
            except Exception as e:
                print(f"Error reading {main_file}: {e}")

    if df is None or len(df) == 0:
        print("No data to export!")
        return

    # Deduplicate
    if 'file_number' in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=['file_number'], keep='last')
        print(f"Final dataset: {len(df)} records (removed {before - len(df)} duplicates)")

    # Export all formats
    print("\n--- Exporting CSV ---")
    export_csv(df, data_dir / 'businesses.csv')

    print("\n--- Exporting JSON ---")
    export_json(df, data_dir / 'businesses.json')

    print("\n--- Exporting SQL ---")
    export_sql(df, data_dir / 'businesses.sql')

    # Create summary
    summary = {
        "last_updated": datetime.now().isoformat(),
        "total_businesses": len(df),
        "files": {
            "csv": "businesses.csv",
            "json": "businesses.json",
            "sql": "businesses.sql"
        }
    }

    if 'filing_date' in df.columns:
        df['year'] = df['filing_date'].astype(str).str[:4]
        summary['by_year'] = {k: int(v) for k, v in df['year'].value_counts().head(10).items() if k != 'nan'}

    if 'business_type' in df.columns:
        summary['by_type'] = {k: int(v) for k, v in df['business_type'].value_counts().head(10).items()}

    with open(data_dir / 'summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"\nCreated summary.json")

    # Push to GitHub
    if push:
        print("\n--- Pushing to GitHub ---")
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
        message = f"Auto-save: {len(df)} records ({timestamp})"
        git_commit_and_push(repo_dir, message)

    print(f"\nExport complete: {len(df)} total records")
    print('='*60)


def auto_save_loop(repo_dir: Path):
    """Run export every 4 hours."""
    print(f"Starting auto-save mode (every 4 hours)")
    print(f"Press Ctrl+C to stop\n")

    while True:
        try:
            run_export(repo_dir, push=True)

            next_run = datetime.now().timestamp() + AUTO_SAVE_INTERVAL
            next_run_str = datetime.fromtimestamp(next_run).strftime('%Y-%m-%d %H:%M:%S')
            print(f"\nNext auto-save at: {next_run_str}")
            print("(Ctrl+C to stop)\n")

            time.sleep(AUTO_SAVE_INTERVAL)

        except KeyboardInterrupt:
            print("\nAuto-save stopped by user")
            break
        except Exception as e:
            print(f"Error during auto-save: {e}")
            print("Retrying in 5 minutes...")
            time.sleep(300)


def main():
    parser = argparse.ArgumentParser(description='Export business data to CSV, JSON, SQL and push to GitHub')
    parser.add_argument('--auto', action='store_true', help='Run auto-save every 4 hours')
    parser.add_argument('--no-push', action='store_true', help='Export files but do not push to GitHub')
    parser.add_argument('--once', action='store_true', help='Run export once and exit (default)')
    args = parser.parse_args()

    repo_dir = Path(__file__).parent

    if args.auto:
        auto_save_loop(repo_dir)
    else:
        run_export(repo_dir, push=not args.no_push)


if __name__ == '__main__':
    main()
