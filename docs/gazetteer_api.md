# Gazetteer API Documentation

The BTAA Geoportal API provides access to multiple gazetteers (GeoNames, Who's on First, and BTAA) through a unified API. This document describes the available endpoints and how to use them.

## Overview

The API provides the following gazetteer endpoints:

- **List Gazetteers**: Get information about all available gazetteers
- **Search GeoNames**: Search the GeoNames gazetteer
- **Search Who's on First**: Search the Who's on First gazetteer
- **Get WOF Details**: Get detailed information about a specific Who's on First place
- **Search BTAA**: Search the BTAA gazetteer
- **Unified Search**: Search across all gazetteers with a single query

All endpoints include caching for improved performance. The cache duration can be configured using the `GAZETTEER_CACHE_TTL` environment variable (default: 3600 seconds/1 hour).

## Endpoints

### List All Gazetteers

**Endpoint:** `GET /api/v1/gazetteers`

Returns information about all available gazetteers, including record counts.

**Example Response:**

```json
{
  "data": [
    {
      "id": "geonames",
      "type": "gazetteer",
      "attributes": {
        "name": "GeoNames",
        "description": "GeoNames geographical database",
        "record_count": 25000,
        "website": "https://www.geonames.org/"
      }
    },
    {
      "id": "wof",
      "type": "gazetteer",
      "attributes": {
        "name": "Who's on First",
        "description": "Who's on First gazetteer from Mapzen",
        "record_count": 15000,
        "website": "https://whosonfirst.org/",
        "additional_tables": {
          "ancestors": 20000,
          "concordances": 18000,
          "geojson": 15000,
          "names": 30000
        }
      }
    },
    {
      "id": "btaa",
      "type": "gazetteer",
      "attributes": {
        "name": "BTAA",
        "description": "Big Ten Academic Alliance Geoportal gazetteer",
        "record_count": 5000,
        "website": "https://geo.btaa.org/"
      }
    }
  ],
  "meta": {
    "total_gazetteers": 3,
    "total_records": 45000
  }
}
```

### Search GeoNames

**Endpoint:** `GET /api/v1/gazetteers/geonames`

Search for places in the GeoNames gazetteer.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| q | string | General search query (searches name, asciiname, and alternatenames) |
| name | string | Exact name match |
| country_code | string | Two-letter country code |
| feature_class | string | Feature class (A, P, H, etc.) |
| feature_code | string | Feature code (e.g., PPL, ADM1) |
| admin1_code | string | First-level administrative division code |
| admin2_code | string | Second-level administrative division code |
| population_min | integer | Minimum population |
| population_max | integer | Maximum population |
| offset | integer | Result offset for pagination (default: 0) |
| limit | integer | Maximum number of results to return (default: 20) |

**Example Request:**

```
GET /api/v1/gazetteers/geonames?q=london&country_code=GB&limit=5
```

**Example Response:**

```json
{
  "data": [
    {
      "id": "2643743",
      "type": "geoname",
      "attributes": {
        "name": "London",
        "asciiname": "London",
        "latitude": 51.50853,
        "longitude": -0.12574,
        "feature_class": "P",
        "feature_code": "PPLC",
        "country_code": "GB",
        "admin1_code": "ENG",
        "admin2_code": "GLA",
        "admin3_code": null,
        "admin4_code": null,
        "population": 8961989,
        "timezone": "Europe/London",
        "modification_date": "2022-01-15",
        "elevation": 25,
        "dem": 25,
        "cc2": null,
        "alternatenames": "Londres,Londyn,Lundun,..."
      }
    },
    // Additional results...
  ],
  "meta": {
    "total_count": 25,
    "offset": 0,
    "limit": 5,
    "query": {
      "q": "london",
      "country_code": "GB",
      "feature_class": null,
      "feature_code": null,
      "admin1_code": null,
      "admin2_code": null,
      "population_min": null,
      "population_max": null
    }
  }
}
```

### Search Who's on First

**Endpoint:** `GET /api/v1/gazetteers/wof`

Search for places in the Who's on First gazetteer.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| q | string | General search query (searches name) |
| name | string | Exact name match |
| placetype | string | Place type (e.g., country, region, locality) |
| country | string | Two-letter country code |
| is_current | integer | Whether the place is current (1) or not (0) |
| parent_id | integer | ID of the parent place |
| offset | integer | Result offset for pagination (default: 0) |
| limit | integer | Maximum number of results to return (default: 20) |

**Example Request:**

```
GET /api/v1/gazetteers/wof?q=new%20york&placetype=locality&limit=5
```

**Example Response:**

```json
{
  "data": [
    {
      "id": "85977539",
      "type": "wof",
      "attributes": {
        "name": "New York",
        "placetype": "locality",
        "country": "US",
        "parent_id": 102081863,
        "latitude": 40.71427,
        "longitude": -74.00597,
        "min_latitude": 40.47739,
        "min_longitude": -74.25909,
        "max_latitude": 40.91757,
        "max_longitude": -73.70009,
        "is_current": 1,
        "is_deprecated": 0,
        "is_ceased": 0,
        "is_superseded": 0,
        "is_superseding": 0,
        "repo": "whosonfirst-data-admin-us",
        "lastmodified": 1614963272
      }
    },
    // Additional results...
  ],
  "meta": {
    "total_count": 15,
    "offset": 0,
    "limit": 5,
    "query": {
      "q": "new york",
      "name": null,
      "placetype": "locality",
      "country": null,
      "is_current": null,
      "parent_id": null
    }
  }
}
```

### Get WOF Details

**Endpoint:** `GET /api/v1/gazetteers/wof/{wok_id}`

Get detailed information about a specific Who's on First place, including ancestors, names, concordances, and GeoJSON data.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| wok_id | integer | Who's on First ID (required) |

**Example Request:**

```
GET /api/v1/gazetteers/wof/85977539
```

**Example Response:**

```json
{
  "id": "85977539",
  "type": "wof_detail",
  "attributes": {
    "spr": {
      "name": "New York",
      "placetype": "locality",
      "country": "US",
      "parent_id": 102081863,
      "latitude": 40.71427,
      "longitude": -74.00597,
      "min_latitude": 40.47739,
      "min_longitude": -74.25909,
      "max_latitude": 40.91757,
      "max_longitude": -73.70009,
      "is_current": 1,
      "is_deprecated": 0,
      "is_ceased": 0,
      "is_superseded": 0,
      "is_superseding": 0,
      "repo": "whosonfirst-data-admin-us",
      "lastmodified": 1614963272
    },
    "ancestors": [
      {
        "wok_id": 85977539,
        "ancestor_id": 102081863,
        "ancestor_placetype": "region",
        "lastmodified": 1614963272
      },
      // Additional ancestors...
    ],
    "names": [
      {
        "wok_id": 85977539,
        "placetype": "locality",
        "country": "US",
        "language": "eng",
        "name": "New York",
        "lastmodified": 1614963272
      },
      // Additional names...
    ],
    "concordances": [
      {
        "wok_id": 85977539,
        "other_id": "5128581",
        "other_source": "geonames",
        "lastmodified": 1614963272
      },
      // Additional concordances...
    ],
    "geojson": [
      {
        "wok_id": 85977539,
        "body": "{ \"type\": \"Feature\", ... }",
        "source": "whosonfirst",
        "is_alt": false,
        "lastmodified": 1614963272
      }
    ]
  }
}
```

### Search BTAA

**Endpoint:** `GET /api/v1/gazetteers/btaa`

Search for places in the BTAA gazetteer.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| q | string | General search query (searches fast_area, state_name, and namelsad) |
| fast_area | string | Exact FAST area match |
| state_abbv | string | Two-letter state abbreviation |
| county_fips | string | County FIPS code |
| offset | integer | Result offset for pagination (default: 0) |
| limit | integer | Maximum number of results to return (default: 20) |

**Example Request:**

```
GET /api/v1/gazetteers/btaa?q=minnesota&limit=5
```

**Example Response:**

```json
{
  "data": [
    {
      "id": "1",
      "type": "btaa",
      "attributes": {
        "fast_area": "Minnesota",
        "bounding_box": "-97.23,43.50,-89.53,49.38",
        "geometry": "{ \"type\": \"Polygon\", ... }",
        "geonames_id": "5037779",
        "state_abbv": "MN",
        "state_name": "Minnesota",
        "county_fips": null,
        "statefp": "27",
        "namelsad": "Minnesota"
      }
    },
    // Additional results...
  ],
  "meta": {
    "total_count": 12,
    "offset": 0,
    "limit": 5,
    "query": {
      "q": "minnesota",
      "fast_area": null,
      "state_abbv": null,
      "county_fips": null
    }
  }
}
```

### Unified Search

**Endpoint:** `GET /api/v1/gazetteers/search`

Search across all gazetteers with a single query.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| q | string | Search query (required) |
| gazetteer | string | Specific gazetteer to search (geonames, wof, btaa, or all) |
| country_code | string | Two-letter country code (for GeoNames and WOF) |
| state_abbv | string | Two-letter state abbreviation (for BTAA) |
| offset | integer | Result offset for pagination (default: 0) |
| limit | integer | Maximum number of results to return (default: 20) |

**Example Request:**

```
GET /api/v1/gazetteers/search?q=chicago&limit=5
```

**Example Response:**

```json
{
  "data": [
    {
      "id": "4887398",
      "type": "geoname",
      "source": "geonames",
      "attributes": {
        "name": "Chicago",
        "asciiname": "Chicago",
        "latitude": 41.85003,
        "longitude": -87.65005,
        "feature_class": "P",
        "feature_code": "PPLA2",
        "country_code": "US",
        "admin1_code": "IL",
        "admin2_code": "031",
        "population": 2695598,
        "timezone": "America/Chicago"
      }
    },
    {
      "id": "85940735",
      "type": "wof",
      "source": "wof",
      "attributes": {
        "name": "Chicago",
        "placetype": "locality",
        "country": "US",
        "parent_id": 102081501,
        "latitude": 41.83755,
        "longitude": -87.68029
      }
    },
    {
      "id": "123",
      "type": "btaa",
      "source": "btaa",
      "attributes": {
        "fast_area": "Chicago",
        "state_abbv": "IL",
        "state_name": "Illinois",
        "county_fips": "031",
        "namelsad": "Chicago city"
      }
    },
    // Additional results...
  ],
  "meta": {
    "total_count": 45,
    "offset": 0,
    "limit": 5,
    "query": {
      "q": "chicago",
      "gazetteer": null,
      "country_code": null,
      "state_abbv": null,
      "gazetteers_searched": ["geonames", "wof", "btaa"]
    }
  }
}
```

## Testing the API

A test script is provided at `scripts/test_gazetteer_api.py` to verify the functionality of the gazetteer API endpoints. To use the script:

```bash
# Make sure the script is executable
chmod +x scripts/test_gazetteer_api.py

# Run the script with default settings (localhost)
./scripts/test_gazetteer_api.py

# Or specify a different base URL
./scripts/test_gazetteer_api.py --base-url https://your-api-server.com/api/v1
```

## Environment Variables

The gazetteer API uses the following environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| ENDPOINT_CACHE | Enable/disable endpoint caching | false |
| GAZETTEER_CACHE_TTL | Cache duration in seconds | 3600 (1 hour) |
| REDIS_HOST | Redis host for caching | localhost |
| REDIS_PORT | Redis port | 6379 |
| REDIS_PASSWORD | Redis password | None |

## Importing Gazetteer Data

To import gazetteer data, use the `app/gazetteer/import_all.py` script:

```bash
# Run the script to import all gazetteers
python app/gazetteer/import_all.py

# Import specific gazetteers
python app/gazetteer/import_all.py --gazetteers geonames wof

# Specify a custom data directory
python app/gazetteer/import_all.py --data-dir /path/to/data

# Output detailed results to a file
python app/gazetteer/import_all.py --output import_results.json
```

### Data Structure

The importers expect data in the following directory structure:

```
data/
  gazetteers/
    geonames/
      *.csv
    wof/
      spr/
        *.csv
      ancestors/
        *.csv
      concordances/
        *.csv
      geojson/
        *.csv
      names/
        *.csv
    btaa/
      *.csv
```

Each CSV file should match the expected format for the corresponding gazetteer. 