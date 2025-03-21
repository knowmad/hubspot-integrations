#!/usr/bin/env python3
"""
Command-line script for importing taxes to HubSpot
"""

import os
import sys
import argparse

# Add parent directory to path so we can import our module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hubspot_tools.taxes.tax_import import import_taxes, get_hubspot_api_token

def main():
    parser = argparse.ArgumentParser(description="Import tax data from CSV to HubSpot")
    parser.add_argument("csv_file", help="Path to the CSV file containing tax data")
    parser.add_argument("--portal", help="HubSpot portal name to use (default: uses defaultPortal from config)")
    parser.add_argument("--config", default="../hubspot.config.yml", 
                      help="Path to HubSpot config file (default: ../hubspot.config.yml)")
    parser.add_argument("--dry-run", action="store_true", help="Validate the CSV without importing")
    args = parser.parse_args()
    
    # Ensure the CSV file exists
    if not os.path.isfile(args.csv_file):
        print(f"Error: File {args.csv_file} does not exist")
        sys.exit(1)
    
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Get API token based on specified portal and config
    try:
        api_token = get_hubspot_api_token(args.config, args.portal)
        portal_name = args.portal or "default portal from config"
        print(f"Using API token for portal: {portal_name}")
    except Exception as e:
        print(f"Error loading API token: {e}")
        sys.exit(1)
    
    # Import the taxes
    try:
        # Only validate if dry-run is specified
        if args.dry_run:
            print(f"Dry run mode: Validating {args.csv_file} without importing")
            # For now, just read and validate the CSV
            from hubspot_tools.taxes.tax_import import read_csv_data, validate_csv_data
            data = read_csv_data(args.csv_file)
            if validate_csv_data(data):
                print(f"CSV validation completed successfully. Found {len(data)} records.")
            else:
                print("CSV validation failed.")
            sys.exit(0)
        
        # Pass the API token to the import function
        results = import_taxes(args.csv_file, api_token)
        print(f"Import completed: {results['successful']} successful, {results['failed']} failed")
    except Exception as e:
        print(f"Import failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()