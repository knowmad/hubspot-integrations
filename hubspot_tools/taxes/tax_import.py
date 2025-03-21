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

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

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

def validate_api_token(api_token: str) -> bool:
    """Validate the API token by making a simple HubSpot API call."""
    logger.info("Validating API token...")
    
    # Try to get account information using the token
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    try:
        # Use a simple endpoint to validate the token
        response = requests.get("https://api.hubapi.com/integrations/v1/me", headers=headers)
        if response.status_code == 200:
            logger.info("API token is valid")
            return True
        else:
            logger.error(f"API token validation failed: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return False
    except Exception as e:
        logger.error(f"API token validation error: {e}")
        return False

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
    
    # Try to convert tax_percentage to float if it's not empty
    try:
        rate_value = float(tax_percentage) if tax_percentage else None
    except (ValueError, TypeError):
        rate_value = tax_percentage  # Keep as string if conversion fails
    
    # Create HubSpot tax object properties
    properties = {
        "name": jurisdiction_desc,
        "rate": rate_value,
        "name": jurisdiction_id
    }
    
    # Remove empty or None values
    return {k: v for k, v in properties.items() if v is not None and v != ""}

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
    
    logger.info(f"Sending request to: {API_ENDPOINT}")
    logger.info(f"Request payload sample: {json.dumps(payload)[:200]}...")  # Log first 200 chars of payload
    
    try:
        response = requests.post(API_ENDPOINT, headers=headers, json=payload)
        
        # Log detailed information about the response
        logger.info(f"Response status code: {response.status_code}")
        
        # If there's an error, log the complete response text
        if response.status_code >= 400:
            logger.error(f"Error response: {response.text}")
        
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
    
    # Validate the API token
    if not validate_api_token(api_token):
        logger.error("Invalid API token. Please check your configuration.")
        stats["failed"] = 1  # To indicate failure
        return stats
    
    # Read and validate CSV data
    data = read_csv_data(csv_file_path)
    if not validate_csv_data(data):
        logger.error("CSV validation failed. Import aborted.")
        return stats
    
    # Print a sample record for diagnostic purposes
    if data:
        logger.info(f"Sample record from CSV: {json.dumps(data[0])}")
        sample_transformed = transform_record_for_hubspot(data[0])
        logger.info(f"Sample transformed record: {json.dumps(sample_transformed)}")
    
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

def get_hubspot_taxes(api_token: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Retrieve all tax objects from HubSpot with their properties.
    Handles pagination to get all records beyond the limit.
    
    Args:
        api_token: HubSpot API token
        limit: Maximum number of records per page (default: 100)
        
    Returns:
        List of tax objects with all their properties
    """
    # First, get the properties available for tax objects
    properties_endpoint = "https://api.hubapi.com/crm/v3/properties/taxes"
    taxes_endpoint = "https://api.hubapi.com/crm/v3/objects/taxes"
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    all_taxes = []
    
    try:
        # Step 1: Get available properties for taxes
        logger.info("Retrieving available properties for tax objects...")
        props_response = requests.get(properties_endpoint, headers=headers)
        
        if props_response.status_code >= 400:
            logger.error(f"Error getting properties: {props_response.text}")
            props_response.raise_for_status()
            
        props_data = props_response.json()
        property_names = [prop["name"] for prop in props_data.get("results", [])]
        
        logger.info(f"Found {len(property_names)} available properties for taxes")
        
        # Step 2: Get all tax objects with pagination
        after = None
        page_count = 0
        
        while True:
            page_count += 1
            params = {
                "limit": limit,
                "properties": property_names
            }
            
            # Add pagination token if we have one
            if after:
                params["after"] = after
                
            logger.info(f"Retrieving tax page {page_count} (limit: {limit}, after: {after})...")
            taxes_response = requests.get(taxes_endpoint, headers=headers, params=params)
            
            if taxes_response.status_code >= 400:
                logger.error(f"Error getting taxes: {taxes_response.text}")
                taxes_response.raise_for_status()
                
            taxes_data = taxes_response.json()
            page_results = taxes_data.get("results", [])
            
            # Add this page's results to our collection
            all_taxes.extend(page_results)
            logger.info(f"Retrieved {len(page_results)} tax objects on page {page_count}")
            
            # Check if there are more pages
            paging = taxes_data.get("paging", {})
            next_page = paging.get("next", {})
            after = next_page.get("after")
            
            # If no "after" token, we've reached the last page
            if not after:
                break
        
        logger.info(f"Retrieved a total of {len(all_taxes)} tax objects")
        
        # Log a sample for debugging
        if all_taxes:
            logger.info(f"Sample tax object: {json.dumps(all_taxes[0])}")
            
        return all_taxes
        
    except Exception as e:
        logger.error(f"Failed to retrieve taxes: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Response: {e.response.text}")
        raise