#!/usr/bin/env python3
"""
HubSpot Tax Batch Import Module
-------------------------------
This module imports tax data from a CSV file into HubSpot using the batch create API.
"""

import os
import csv
import json
import time
import logging
import yaml
from typing import List, Dict, Any, Optional
import requests

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

def get_hubspot_api_token(config_path="../hubspot.config.yml", portal_name=None):
    """
    Read HubSpot API token from the hubspot.config.yml file.
    
    Args:
        config_path: Path to the hubspot.config.yml file
        portal_name: Name of the portal to use (if None, uses defaultPortal)
        
    Returns:
        The access token for the specified portal
    """
    # Ensure the config file exists
    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"HubSpot config file not found at {config_path}")
    
    # Read the config file
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Determine which portal to use
    target_portal = portal_name or config.get('defaultPortal')
    if not target_portal:
        raise ValueError("No portal specified and no defaultPortal found in config")
    
    # Find the portal in the config
    portal_config = None
    for portal in config.get('portals', []):
        if portal.get('name') == target_portal:
            portal_config = portal
            break
    
    if not portal_config:
        raise ValueError(f"Portal '{target_portal}' not found in config")
    
    # Extract the access token
    auth = portal_config.get('auth', {})
    token_info = auth.get('tokenInfo', {})
    access_token = token_info.get('accessToken')
    
    if not access_token:
        raise ValueError(f"No access token found for portal '{target_portal}'")
    
    # Clean up the token (remove any YAML block indicators like '>')
    access_token = access_token.strip()
    
    return access_token

# Constants
BATCH_SIZE = 100  # HubSpot's maximum batch size
API_ENDPOINT = "https://api.hubapi.com/crm/v3/objects/taxes/batch/create"

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
    
    required_fields = ['jurisdiction_id', 'jurisdiction_desc', 'tax_percentage']
    
    # Check if required fields exist in the first row
    first_row = data[0]
    missing_fields = [field for field in required_fields if field not in first_row]
    
    if missing_fields:
        logger.error(f"CSV is missing required fields: {', '.join(missing_fields)}")
        return False
    
    return True

def transform_record_for_hubspot(record: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a CSV record into the format expected by HubSpot API."""
    # Extract values from the record
    jurisdiction_id = record.get("jurisdiction_id", "")
    jurisdiction_desc = record.get("jurisdiction_desc", "")
    tax_percentage = record.get("tax_percentage", "")
    
    # Create HubSpot tax object properties
    properties = {
        "name": jurisdiction_desc,         # Use description as name
        "rate": tax_percentage,            # Tax rate/percentage
        "externalId": jurisdiction_id      # Store original ID as reference
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

def send_batch_request(payload: Dict[str, Any], api_token: str) -> Dict[str, Any]:
    """Send a batch request to the HubSpot API."""
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(API_ENDPOINT, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Response: {e.response.text}")
        raise

def import_taxes(csv_file_path: str, api_token: Optional[str] = None) -> Dict[str, int]:
    """
    Main function to import taxes from CSV to HubSpot.
    
    Args:
        csv_file_path: Path to the CSV file containing tax data
        api_token: HubSpot API token (if None, will try to load from config)
        
    Returns:
        Dictionary with import statistics
    """
    stats = {
        "total": 0,
        "successful": 0,
        "failed": 0
    }
    
    # If no API token provided, try to load it
    if not api_token:
        try:
            api_token = get_hubspot_api_token()
            logger.info("Successfully loaded API token from default config")
        except Exception as e:
            logger.error(f"Failed to load HubSpot API token: {e}")
            raise
    
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
            result = send_batch_request(payload, api_token)
            
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