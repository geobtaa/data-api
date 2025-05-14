import asyncio
import csv
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from elasticsearch import AsyncElasticsearch

# Load environment variables from .env file
load_dotenv()

from app.elasticsearch.mappings import INDEX_MAPPING

# Get Elasticsearch URL from environment or use default
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
TEST_INDEX_NAME = "data_api_test"

# Add the project root directory to the Python path
project_root = str(Path(__file__).parent.parent)
sys.path.insert(0, project_root)


def process_geometry(geom_str):
    """Convert ENVELOPE format to GeoJSON polygon."""
    if not geom_str or geom_str == "null":
        return None

    if geom_str.startswith("ENVELOPE"):
        # Extract coordinates from ENVELOPE(minX, maxX, maxY, minY)
        coords = geom_str.replace("ENVELOPE(", "").replace(")", "").split(",")
        minX, maxX, maxY, minY = map(float, coords)

        # Create a polygon from the envelope coordinates
        return {
            "type": "polygon",
            "coordinates": [[[minX, minY], [maxX, minY], [maxX, maxY], [minX, maxY], [minX, minY]]],
        }
    return None


def process_references(ref_str):
    """Convert JSON string to object for dct_references_s field."""
    if not ref_str or ref_str == "null":
        return None
    try:
        return json.loads(ref_str)
    except json.JSONDecodeError:
        return None


def process_value(key, value):
    """Process a value based on its field type."""
    if not value or value == "null":
        return None

    if key in ["locn_geometry", "dcat_bbox"]:
        return process_geometry(value)
    elif key == "dct_references_s":
        return process_references(value)
    elif key in ["gbl_indexyear_im"]:
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    elif key in ["gbl_georeferenced_b"]:
        return value.lower() == "true"
    elif key in [
        "dct_creator_sm",
        "dct_spatial_sm",
        "dct_language_sm",
        "gbl_resourceclass_sm",
        "gbl_resourcetype_sm",
        "dct_accessrights_sm",
    ]:
        if "|" in value:
            return [v.strip() for v in value.split("|")]
        return [value]
    return value


async def load_test_data():
    """Load test data from CSV into Elasticsearch."""
    # Create Elasticsearch client
    client = AsyncElasticsearch(
        hosts=[ELASTICSEARCH_URL],
        verify_certs=False,
        ssl_show_warn=False,
        request_timeout=60,
        retry_on_timeout=True,
        max_retries=3,
    )

    try:
        # Check connection
        info = await client.info()
        print(f"Connected to Elasticsearch cluster: {info['cluster_name']}")

        # Delete the test index if it exists
        if await client.indices.exists(index=TEST_INDEX_NAME):
            await client.indices.delete(index=TEST_INDEX_NAME)
            print(f"Deleted existing index {TEST_INDEX_NAME}")

        # Create the index with mappings
        await client.indices.create(
            index=TEST_INDEX_NAME,
            mappings=INDEX_MAPPING["mappings"],
            settings=INDEX_MAPPING["settings"],
        )
        print(f"Created index {TEST_INDEX_NAME}")

        # Load test data from CSV
        with open("tests/fixtures/gbl_fixtures_data.csv", "r") as f:
            reader = csv.DictReader(f)
            bulk_data = []

            for row in reader:
                # Convert string values to appropriate types
                doc = {}
                for key, value in row.items():
                    processed_value = process_value(key, value)
                    if processed_value is not None:
                        doc[key] = processed_value

                # Add document to bulk data
                bulk_data.append({"index": {"_index": TEST_INDEX_NAME, "_id": doc["id"]}})
                bulk_data.append(doc)

        # Bulk index the documents
        if bulk_data:
            response = await client.bulk(operations=bulk_data, refresh=True)
            if response.get("errors"):
                print(f"Errors occurred during bulk indexing: {response['items']}")
            else:
                print(f"Successfully indexed {len(bulk_data) // 2} documents")

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(load_test_data())
