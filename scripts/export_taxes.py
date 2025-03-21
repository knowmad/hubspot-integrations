#!/usr/bin/env python3
"""
Command-line script for exporting taxes from HubSpot
"""

import os
import sys
import json
import argparse
import csv

# Add parent directory to path so we can import our module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hubspot_tools.taxes.tax_import import get_hubspot_api_token, get_hubspot_taxes

def main():
    parser = argparse.ArgumentParser(description="Export taxes from HubSpot")
    parser.add_argument("--portal", help="HubSpot portal name to use (default: uses defaultPortal from config)")
    parser.add_argument("--config", default="../hubspot.config.yml", 
                      help="Path to HubSpot config file (default: ../hubspot.config.yml)")
    parser.add_argument("--limit", type=int, default=100, 
                      help="Limit the number of taxes to retrieve (default: 100)")
    parser.add_argument("--output", help="Path for output CSV file (optional)")
    parser.add_argument("--format", choices=["json", "csv", "table"], default="table",
                      help="Output format (default: table)")
    args = parser.parse_args()
    
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
    
    # Get taxes from HubSpot
    try:
        taxes = get_hubspot_taxes(api_token, args.limit)
        print(f"Retrieved {len(taxes)} tax objects from HubSpot")
        
        if not taxes:
            print("No tax objects found.")
            sys.exit(0)
            
        # Output in the requested format
        if args.format == "json":
            # Print as formatted JSON
            print(json.dumps(taxes, indent=2))
            
        elif args.format == "csv":
            # Define field names from the first tax object
            first_tax = taxes[0]
            fields = ["id"]
            if "properties" in first_tax:
                fields.extend(first_tax["properties"].keys())
                
            # Output to file if specified, otherwise to stdout
            output_file = args.output if args.output else sys.stdout
            
            with open(output_file, 'w', newline='') if isinstance(output_file, str) else output_file as f:
                writer = csv.writer(f)
                # Write header
                writer.writerow(fields)
                
                # Write data rows
                for tax in taxes:
                    row = [tax.get("id", "")]
                    props = tax.get("properties", {})
                    for field in fields[1:]:  # Skip 'id' which we already handled
                        row.append(props.get(field, ""))
                    writer.writerow(row)
                    
            if args.output:
                print(f"CSV exported to {args.output}")
                
        else:  # table format
            # Print as a formatted table
            print("\nTax Objects:\n")
            print(f"{'ID':<10} {'Name':<30} {'Rate':<10} {'External ID':<20} {'Other Properties'}")
            print("-" * 80)
            
            for tax in taxes:
                id = tax.get("id", "unknown")
                props = tax.get("properties", {})
                name = props.get("name", "unnamed")
                rate = props.get("rate", "unknown")
                externalid = props.get("externalid", "")
                
                # Collect other properties
                other_props = []
                for key, value in props.items():
                    if key not in ["name", "rate", "externalid"]:
                        other_props.append(f"{key}={value}")
                        
                other = ", ".join(other_props)
                
                print(f"{id:<10} {name[:28]:<30} {rate:<10} {externalid[:18]:<20} {other[:30]}")
            
            # Print the raw structure of the first object for reference
            print("\nSample tax object structure:")
            print(json.dumps(taxes[0], indent=2))
            
    except Exception as e:
        print(f"Error exporting taxes: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()