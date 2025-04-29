# Gazetteer Service

The Gazetteer Service provides functionality to look up geographic places and entities in a local GeoNames database. It's designed to work with the LLM Service to identify and disambiguate geographic entities mentioned in text.

## Database Requirements

The Gazetteer Service expects a PostgreSQL database with the GeoNames schema. The database should contain at least the following tables:

- `geoname`: The main table containing geographic entities
- `countryinfo`: Information about countries

### Required Columns in the `geoname` Table

- `geonameid`: Unique identifier
- `name`: Place name
- `asciiname`: ASCII version of the name
- `alternatenames`: Alternative names
- `feature_class`: Feature class code (A, H, L, P, R, S, T, U, V)
- `feature_code`: Feature code
- `country_code`: ISO country code
- `admin1_code`, `admin2_code`, `admin3_code`, `admin4_code`: Administrative division codes
- `population`: Population count
- `elevation`: Elevation in meters
- `dem`: Digital elevation model
- `timezone`: Timezone
- `modification_date`: Last modification date
- `geom`: Geometry column (PostGIS)

### Required Columns in the `countryinfo` Table

- `iso_alpha2`: ISO 2-letter country code
- `name`: Country name

## Environment Variables

The following environment variables can be used to configure the database connection:

- `DB_HOST`: Database host (default: localhost)
- `DB_PORT`: Database port (default: 5432)
- `DB_NAME`: Database name (default: geonames)
- `DB_USER`: Database user (default: postgres)
- `DB_PASSWORD`: Database password

## Usage

### Basic Usage

```python
from app.services.gazetteer_service import GazetteerService

# Initialize the service
gazetteer_service = GazetteerService()

# Look up a place
place = await gazetteer_service.lookup_place("New York", entity_type="city")
if place:
    print(f"Found: {place['name']} ({place['id']})")
    print(f"Coordinates: {place['latitude']}, {place['longitude']}")
    print(f"Country: {place['country']}")
    print(f"Type: {place['type']}")
    print(f"Confidence: {place['confidence']:.2f}")

# Clean up
await gazetteer_service.disconnect()
```

### With LLM Service

```python
from app.services.gazetteer_service import GazetteerService
from app.services.llm_service import LLMService

# Initialize the services
gazetteer_service = GazetteerService()
llm_service = LLMService(gazetteer_service=gazetteer_service)

# Identify geographic entities in text
text = "The Mississippi River flows through New Orleans, Louisiana."
entities, prompt, parser = await llm_service.identify_geo_entities(text)

# Process the results
for entity in entities:
    print(f"Entity: {entity['name']} ({entity['type']})")
    if "gazetteer_id" in entity:
        print(f"  Gazetteer: {entity['gazetteer_name']} ({entity['gazetteer_id']})")
        print(f"  Country: {entity['gazetteer_country']}")
        print(f"  Coordinates: {entity['gazetteer_lat']}, {entity['gazetteer_lng']}")

# Clean up
await gazetteer_service.disconnect()
```

## Implementation Details

### Entity Type Mapping

The service maps common entity types to GeoNames feature classes:

| Entity Type | Feature Class |
|-------------|---------------|
| country     | A             |
| state       | A             |
| province    | A             |
| city        | P             |
| town        | P             |
| village     | P             |
| river       | H             |
| lake        | H             |
| ocean       | H             |
| sea         | H             |
| mountain    | T             |
| hill        | T             |
| valley      | T             |
| forest      | V             |
| park        | L             |
| island      | L             |
| peninsula   | L             |

### Confidence Scoring

The service calculates a confidence score for each match based on:

1. Name match (exact or partial)
2. Entity type match
3. Population factor (more populous places are more likely to be the intended match)

## Example Script

See `examples/geo_entity_example.py` for a complete example of using the Gazetteer Service with the LLM Service. 