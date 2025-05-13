# Gazetteer Data Management

This document describes how to download and import gazetteer data using the provided scripts.

## Overview

The gazetteer data management system consists of two main components:
1. Downloaders - Scripts to fetch and prepare data from various gazetteer sources
2. Importers - Scripts to load the prepared data into the database

Currently supported gazetteers:
- BTAA Placenames
- GeoNames
- OCLC FAST Geographic (FAST)
- Who's on First (WOF)

## Prerequisites

- Python 3.x
- PostgreSQL database
- Required Python packages (install via `pip`):
  - SQLAlchemy
  - psycopg2-binary
  - requests
  - pandas

## Directory Structure

```
data/
  gazetteers/
    wof/              # Who's on First data
      csv/            # Exported CSV files
    geonames/         # GeoNames data
    btaa/             # BTAA Geoportal data
    fast/             # OCLC FAST Geographic data
```

## Downloading Data

### Using the Download Script

The `download.py` script in the `app/gazetteer` directory provides a unified interface for downloading data from all supported gazetteers.

To download data from all gazetteers:
```bash
python app/gazetteer/download.py --all-gazetteers
```

To download data from a specific gazetteer:
```bash
python app/gazetteer/download.py --gazetteers [gazetteer_name]
```

Where `[gazetteer_name]` can be:
- `wof` - Who's on First
- `geonames` - GeoNames
- `btaa` - BTAA Geoportal
- `fast` - FAST (Faceted Application of Subject Terminology)

### Gazetteer-Specific Notes

#### BTAA Geoportal
1. Downloads BTAA Geoportal data
2. Processes it into the required format

#### GeoNames
1. Downloads US-specific GeoNames data
2. Extracts and processes the data into a tab-delimited format

#### OCLC FAST (Faceted Application of Subject Terminology)
1. Downloads the FAST Geographic MARCXML dataset
2. Extracts the MARCXML file from the ZIP archive
3. Stores the MARCXML file in the data directory for processing by the importer

#### Who's on First (WOF)
1. Downloads the SQLite database
2. Exports the following tables to CSV:
   - spr.csv (main properties)
   - ancestors.csv
   - concordances.csv
   - names.csv
   - geojson.csv

## Importing Data

### Using the Import Script

The `import_all.py` script in the `app/gazetteer` directory handles importing data from all supported gazetteers.

To import data from all gazetteers:
```bash
python app/gazetteer/import_all.py --all-gazetteers
```

To import data from a specific gazetteer:
```bash
python app/gazetteer/import_all.py --gazetteers [gazetteer_name]
```

Where `[gazetteer_name]` can be:
- `wof` - Who's on First
- `geonames` - GeoNames
- `btaa` - BTAA Geoportal
- `fast` - FAST (Faceted Application of Subject Terminology)

### Import Process

For each gazetteer:
1. The importer truncates existing tables
2. Reads the CSV files
3. Cleans and validates the data
4. Imports the data in chunks to manage memory usage
5. Reports progress and any errors encountered

### Gazetteer-Specific Notes

#### Who's on First (WOF)
- Uses a chunk size of 500 for the SPR table due to the large number of fields
- Handles large GeoJSON fields by automatically increasing the CSV field size limit
- Processes multi-value fields (e.g., supersedes, superseded_by) by taking the first value

#### GeoNames
- Processes tab-delimited text files
- Handles specific GeoNames field formats and data types

#### BTAA Geoportal
- Uses a chunk size of 2000 for optimal performance
- Handles BTAA-specific data formats and fields

#### FAST (Faceted Application of Subject Terminology)
- Parses the MARCXML file using a SAX parser for efficient memory usage
- Extracts geographic entries with their associated metadata
- Processes the following fields:
  - FAST ID (from 016_7_a field)
  - URI (from 024_7_a field)
  - Label (from 151 field)
  - GeoNames ID (from 751_4_0 field, if available)
- Uses a chunk size of 1000 for optimal performance
- Exports processed data to CSV for verification before database import

## Troubleshooting

### Common Issues

1. CSV Field Size Limit
   - Error: "field larger than field limit (131072)"
   - Solution: The importer automatically increases the field size limit

2. PostgreSQL Parameter Limits
   - Error: "too many parameters"
   - Solution: The importers use appropriate chunk sizes to avoid this issue

3. Missing Files
   - Error: "No CSV files found"
   - Solution: Ensure you've run the downloader first and check the data directory structure

4. Import Errors
   - The importers log detailed error messages
   - Check the console output for specific error information
   - Look for warnings about data validation or conversion issues

5. MARCXML Parsing Issues (FAST)
   - Error: "Error parsing MARCXML file"
   - Solution: Check if the MARCXML file was downloaded correctly and is not corrupted
   - Verify that the file contains the expected fields and structure

## Monitoring Progress

Both downloaders and importers provide detailed logging:
- Progress updates
- File processing status
- Record counts
- Error messages

Monitor the console output to track progress and identify any issues that need attention. 