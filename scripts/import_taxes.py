#!/usr/bin/env python3
"""
Command-line script for importing taxes to HubSpot
"""

import os
import sys
import argparse

# Add parent directory to path so we can import our module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hubspot_tools.taxes.tax_import import import_taxes

def main():
    parser = argparse.ArgumentParser(description="Import tax data from CSV to HubSpot")
    parser.add_argument("csv_file", help="Path to the CSV file containing tax data")
    parser.add_argument("--dry-run", action="store_true", help="Validate the CSV without importing")
    args = parser.parse_args()
    
    # Ensure the CSV file exists
    if not os.path.isfile(args.csv_file):
        print(f"Error: File {args.csv_file} does not exist")
        sys.exit(1)
    
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Import the taxes
    try:
        results = import_taxes(args.csv_file)
        print(f"Import completed: {results['successful']} successful, {results['failed']} failed")
    except Exception as e:
        print(f"Import failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
