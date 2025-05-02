# OCLC FAST Dataset Importer

This importer downloads and processes the OCLC FAST (Faceted Application of Subject Terminology) Dataset's Geographic entries.

## Overview

The FAST Dataset is a faceted thesaurus based on the Library of Congress Subject Headings (LCSH). The Geographic entries subset contains geographic place names and their relationships.

## Data Source

The data is downloaded from:
https://researchworks.oclc.org/researchdata/fast/FASTGeographic.marcxml.zip

This ZIP file contains a MARCXML file with geographic entries.

## Data Processing

The importer performs the following steps:

1. Downloads the ZIP file from the OCLC website
2. Extracts the MARCXML file
3. Parses the MARCXML file using SAX to extract the following fields:
   - `fast_id`: The numeric ID from the FAST record (e.g., "1204260" from "fst01204260")
   - `uri`: The URI of the FAST record (e.g., "https://id.worldcat.org/fast/1204260")
   - `type`: Always set to "place" for geographic entries
   - `label`: The geographic name, constructed from the 151 field (e.g., "Minnesota--Minneapolis")
   - `geoname_id`: The GeoNames ID, extracted from geonames.org URIs in the 751 field
   - `viaf_id`: The VIAF ID, extracted from viaf.org URIs in the 751 field
   - `wikipedia_id`: The Wikipedia page identifier, extracted from wikipedia.org URIs in the 751 field

4. Processes the records:
   - Validates required fields (fast_id, uri, label)
   - Cleans and normalizes the data
   - Handles missing optional fields (geoname_id, viaf_id, wikipedia_id)

5. Exports the processed data to a CSV file for verification
6. Imports the data into the `gazetteer_fast` database table using bulk inserts with a chunk size of 1000 records

## Database Schema

The `gazetteer_fast` table has the following schema:

| Column       | Type      | Description                                |
|--------------|-----------|--------------------------------------------|
| id           | Integer   | Primary key, auto-increment                |
| fast_id      | String    | The FAST ID (e.g., "1204260")              |
| uri          | String    | The FAST URI (e.g., "https://id.worldcat.org/fast/1204260") |
| type         | String    | Always "place" for geographic entries      |
| label        | String    | The geographic name                        |
| geoname_id   | String    | The GeoNames ID (optional)                 |
| viaf_id      | String    | The VIAF ID (optional)                     |
| wikipedia_id | String    | The Wikipedia page identifier (optional)   |
| created_at   | Timestamp | When the record was created                |
| updated_at   | Timestamp | When the record was last updated           |

## Setup

Before running the importer, you need to create the database table. Run the following command:

```bash
./scripts/run_migration.py add_fast_gazetteer
```

This will create the `gazetteer_fast` table with the appropriate schema and indexes.

## Usage

To run the importer, use the following command:

```bash
./scripts/import_fast.py
```

The importer will:
1. Download the FAST Geographic MARCXML dataset
2. Process the data
3. Import it into the database
4. Log the results

## Example Record

Here's an example of a processed record:

```json
{
  "fast_id": "1204260",
  "uri": "https://id.worldcat.org/fast/1204260",
  "type": "place",
  "label": "Minnesota--Minneapolis",
  "geoname_id": "5037649",
  "viaf_id": "158360170",
  "wikipedia_id": "Minneapolis"
}
```

## Mappings

The importer maps the following MARCXML fields to the database columns:

| MARCXML Field | Database Column | Description                                |
|---------------|-----------------|--------------------------------------------|
| 016_7_a       | fast_id         | The FAST ID (e.g., "fst01204260" → "1204260") |
| -             | uri             | Constructed from fast_id: "https://id.worldcat.org/fast/{fast_id}" |
| -             | type            | Always set to "place"                      |
| 151_*         | label           | The geographic name, constructed from all subfields |
| 751_*         | geoname_id      | The GeoNames ID, extracted from geonames.org URIs |
| 751_*         | viaf_id         | The VIAF ID, extracted from viaf.org URIs |
| 751_*         | wikipedia_id    | The Wikipedia page identifier, extracted from wikipedia.org URIs | 