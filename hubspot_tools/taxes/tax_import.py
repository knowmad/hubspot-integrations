#!/usr/bin/env python3
"""
HubSpot Tax Batch Import Script
-------------------------------
This script imports tax data from a CSV file into HubSpot using the batch create API.
"""

import os
import csv
import json
import time
import logging
from typing import List, Dict, Any
import requests
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/tax_import.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
API_KEY = os.getenv("HUBSPOT_API_KEY")
if not API_KEY:
    logger.error("HUBSPOT_API_KEY not found in environment variables")
    exit(1)

# Constants
BATCH_SIZE = 100  # HubSpot's maximum batch size
API_ENDPOINT = "https://api.hubapi.com/crm/v3/objects/taxes/batch/create"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

def read_csv_data(file_path: str) -> List[Dict[str, Any]]:
    """Read and parse the CSV file containing tax data."""
    try:
        with open(file_path, 'r', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            return list(reader)
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise

def validate_csv_data(data: List[Dict[str, Any]]) -> bool:
    """Validate that the CSV data contains the required fields."""
    if not data:
        logger.error("CSV file is empty")
        return False
    
    required_fields = ['name', 'rate', 'country', 'state']  # Adjust based on your tax CSV structure
    
    # Check if required fields exist in the first row
    first_row = data[0]
    missing_fields = [field for field in required_fields if field not in first_row]
    
    if missing_fields:
        logger.error(f"CSV is missing required fields: {', '.join(missing_fields)}")
        return False
    
    return True

def transform_record_for_hubspot(record: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a CSV record into the format expected by HubSpot API."""
    # Adjust these mappings based on your CSV structure and HubSpot properties
    properties = {
        "name": record.get("name", ""),
        "rate": record.get("rate", ""),
        "country": record.get("country", ""),
        "state": record.get("state", ""),
        "city": record.get("city", ""),
        "postal_code": record.get("postal_code", ""),
        "type": record.get("type", ""),
        "category": record.get("category", ""),
        "description": record.get("description", "")
    }
    
    # Remove empty values to avoid creating blank properties
    return {k: v for k, v in properties.items() if v}

def chunk_data(data: List[Dict[str, Any]], chunk_size: int) -> List[List[Dict[str, Any]]]:
    """Split data into chunks of specified size."""
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

def create_batch_payload(records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Create the payload for a batch request."""
    inputs = []
    for record in records:
        inputs.append({
            "properties": record
        })
    
    return {
        "inputs": inputs
    }

def send_batch_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Send a batch request to the HubSpot API."""
    try:
        response = requests.post(API_ENDPOINT, headers=HEADERS, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Response: {e.response.text}")
        raise

def import_taxes(csv_file_path: str) -> Dict[str, int]:
    """Main function to import taxes from CSV to HubSpot."""
    stats = {
        "total": 0,
        "successful": 0,
        "failed": 0
    }
    
    # Read and validate CSV data
    data = read_csv_data(csv_file_path)
    if not validate_csv_data(data):
        logger.error("CSV validation failed. Import aborted.")
        return stats
    
    stats["total"] = len(data)
    logger.info(f"Starting import of {stats['total']} tax records")
    
    # Transform CSV records to HubSpot format
    transformed_records = [transform_record_for_hubspot(record) for record in data]
    
    # Split data into batches
    batches = chunk_data(transformed_records, BATCH_SIZE)
    logger.info(f"Data split into {len(batches)} batches")
    
    # Process each batch
    for i, batch in enumerate(batches, 1):
        logger.info(f"Processing batch {i} of {len(batches)} ({len(batch)} records)")
        
        try:
            # Create and send batch payload
            payload = create_batch_payload(batch)
            result = send_batch_request(payload)
            
            # Process results
            successful_results = len(result.get("results", []))
            stats["successful"] += successful_results
            logger.info(f"Batch {i} completed: {successful_results} records imported successfully")
            
            # Handle any errors in the batch results
            if "errors" in result and result["errors"]:
                for error in result["errors"]:
                    logger.error(f"Error in batch {i}: {json.dumps(error)}")
                    stats["failed"] += 1
            
            # Add a small delay to prevent rate limiting
            time.sleep(0.5)
            
        except Exception as e:
            logger.error(f"Failed to process batch {i}: {e}")
            stats["failed"] += len(batch)
    
    logger.info(f"Import completed: {stats['successful']} successful, {stats['failed']} failed")
    return stats

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Import tax data from CSV to HubSpot")
    parser.add_argument("csv_file", help="Path to the CSV file containing tax data")
    args = parser.parse_args()
    
    try:
        # Ensure logs directory exists
        os.makedirs("logs", exist_ok=True)
        
        results = import_taxes(args.csv_file)
        logger.info(f"Import summary: {json.dumps(results)}")
    except Exception as e:
        logger.error(f"Import failed: {e}")
