#!/usr/bin/env python3
"""
Tests for the HubSpot Tax Import script
"""

import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock
import csv
import json

from hubspot_tools.taxes.tax_import import (
    read_csv_data,
    validate_csv_data,
    transform_record_for_hubspot,
    chunk_data,
    create_batch_payload,
    send_batch_request
)

class TestTaxImport(unittest.TestCase):
    def setUp(self):
        # Create a temporary CSV file for testing
        self.temp_csv = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        self.csv_path = self.temp_csv.name
        
        # Sample data
        self.sample_data = [
            {
                'name': 'Sales Tax NY',
                'rate': '8.875',
                'country': 'United States',
                'state': 'NY',
                'city': 'New York',
                'postal_code': '10001',
                'type': 'sales',
                'category': 'state',
                'description': 'New York State and City combined sales tax'
            },
            {
                'name': 'VAT UK',
                'rate': '20',
                'country': 'United Kingdom',
                'state': '',
                'city': '',
                'postal_code': '',
                'type': 'vat',
                'category': 'country',
                'description': 'UK Value Added Tax'
            }
        ]
        
        # Write sample data to CSV
        with open(self.csv_path, 'w', newline='') as csvfile:
            fieldnames = self.sample_data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in self.sample_data:
                writer.writerow(row)
    
    def tearDown(self):
        # Clean up the temporary file
        os.unlink(self.csv_path)
    
    def test_read_csv_data(self):
        """Test reading data from CSV file."""
        data = read_csv_data(self.csv_path)
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]['name'], 'Sales Tax NY')
        self.assertEqual(data[1]['name'], 'VAT UK')
    
    def test_validate_csv_data(self):
        """Test CSV data validation."""
        # Valid data
        self.assertTrue(validate_csv_data(self.sample_data))
        
        # Invalid data (missing required fields)
        invalid_data = [{'name': 'Test', 'country': 'US'}]  # Missing 'rate' and 'state'
        self.assertFalse(validate_csv_data(invalid_data))
    
    def test_transform_record_for_hubspot(self):
        """Test transformation of records for HubSpot API."""
        record = self.sample_data[0]
        transformed = transform_record_for_hubspot(record)
        
        # Check that all fields are present
        self.assertEqual(transformed['name'], record['name'])
        self.assertEqual(transformed['rate'], record['rate'])
        
        # Test with missing fields
        incomplete_record = {'name': 'Test Tax', 'rate': '5'}
        transformed = transform_record_for_hubspot(incomplete_record)
        self.assertEqual(len(transformed), 2)  # Only name and rate should be present
    
    def test_chunk_data(self):
        """Test chunking of data into batches."""
        # Create a list of 25 items
        data = list(range(25))
        
        # Chunk into batches of 10
        chunks = chunk_data(data, 10)
        
        self.assertEqual(len(chunks), 3)
        self.assertEqual(len(chunks[0]), 10)
        self.assertEqual(len(chunks[1]), 10)
        self.assertEqual(len(chunks[2]), 5)
    
    def test_create_batch_payload(self):
        """Test creation of batch payload for API."""
        records = [{'name': 'Test1'}, {'name': 'Test2'}]
        payload = create_batch_payload(records)
        
        self.assertIn('inputs', payload)
        self.assertEqual(len(payload['inputs']), 2)
        self.assertEqual(payload['inputs'][0]['properties']['name'], 'Test1')
    
    @patch('requests.post')
    def test_send_batch_request(self, mock_post):
        """Test sending batch request to API."""
        # Mock response
        mock_response = MagicMock()
        mock_response.json.return_value = {'results': [{'id': '1'}, {'id': '2'}]}
        mock_post.return_value = mock_response
        
        payload = {'inputs': [{'properties': {'name': 'Test'}}]}
        result = send_batch_request(payload)
        
        self.assertIn('results', result)
        self.assertEqual(len(result['results']), 2)
        
        # Verify the API call
        mock_post.assert_called_once()

if __name__ == '__main__':
    unittest.main()
