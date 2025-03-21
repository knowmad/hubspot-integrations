# HubSpot Tax Import Tool

This module provides functionality for importing tax data from CSV files into HubSpot using the batch create API.

## Requirements

- HubSpot API key with permissions to create tax objects
- CSV file containing tax jurisdiction data

## CSV File Format

The script expects a CSV file with the following columns:
- `jurisdiction_id`: Unique identifier for the tax jurisdiction
- `jurisdiction_desc`: Description of the tax jurisdiction (used as tax name)
- `tax_percentage`: The tax rate value

## Usage

### Basic Usage

```bash
python -m hubspot_tools.taxes.tax_import /path/to/your/tax_data.csv
```

### From Scripts Directory

```bash
python import_taxes.py /path/to/your/tax_data.csv
```

## Configuration

Set your HubSpot API key in a `.env` file:

```
HUBSPOT_API_KEY=your_api_key_here
```

## API Endpoint

This tool uses the HubSpot batch create endpoint:
`https://api.hubapi.com/crm/v3/objects/taxes/batch/create`

## Logging

Logs are written to:
- Console output
- `logs/tax_import.log` file

## Error Handling

- The script validates CSV content before import
- Import errors are logged with details
- Summary statistics are reported after completion