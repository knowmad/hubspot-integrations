# HubSpot Tax Import Tool

This module provides functionality for importing tax data from CSV files into HubSpot using the batch create API.

## Requirements

- HubSpot configuration file with API tokens
- CSV file containing tax jurisdiction data
- Python 3.6+
- Required packages (install via `pip install -r requirements.txt`)

## CSV File Format

The CSV file must contain the following columns:
- `jurisdiction_id`: Unique identifier for the tax jurisdiction
- `jurisdiction_desc`: Description of the tax jurisdiction (used as tax name)
- `tax_percentage`: The tax rate value

## Configuration

The tool uses the HubSpot CLI configuration file format (`hubspot.config.yml`), which should contain your portal information and API tokens. By default, it looks for this file in the parent directory.

Example configuration:
```yaml
defaultPortal: PortalName
portals:
  - name: PortalName
    portalId: 123456
    auth:
      tokenInfo:
        accessToken: your-access-token-here
```

## Usage

### Command Line Options

```bash
# Basic usage with default portal
python scripts/import_taxes.py data/your_tax_data.csv

# Specify a different portal
python scripts/import_taxes.py data/your_tax_data.csv --portal PortalName
python scripts/import_taxes.py data/sample_taxes.csv --portal AmPro

# Use a different config file location
python scripts/import_taxes.py data/your_tax_data.csv --config /path/to/hubspot.config.yml

# Validate the CSV without importing (dry run)
python scripts/import_taxes.py data/your_tax_data.csv --dry-run
```

### Usage from Python Code

```python
from hubspot_tools.taxes.tax_import import import_taxes, get_hubspot_api_token

# Get API token from config
api_token = get_hubspot_api_token(portal_name="PortalName")

# Import taxes with the token
results = import_taxes("data/your_tax_data.csv", api_token)
print(f"Import summary: {results}")
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

## Sample Files

The repository includes sample data files in the `data` directory:
- `sample_taxes.csv`: A template with the required format
- `ampro-tax_jurisdictions.csv`: Example tax jurisdiction data