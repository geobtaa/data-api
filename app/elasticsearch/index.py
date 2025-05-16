import json
import logging
import os
import re

from dotenv import load_dotenv

from db.database import database
from db.models import items

from .client import es

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


async def index_items():
    """Index all items from PostgreSQL into Elasticsearch."""
    index_name = os.getenv("ELASTICSEARCH_INDEX", "btaa_geometadata_api")

    if await es.indices.exists(index=index_name):
        await es.indices.delete(index=index_name)

    from .client import init_elasticsearch

    await init_elasticsearch()

    item_rows = await database.fetch_all(items.select())
    bulk_data = await prepare_bulk_data(item_rows, index_name)

    if bulk_data:
        return await perform_bulk_indexing(bulk_data, index_name)

    return {"message": "No items to index"}


async def prepare_bulk_data(items, index_name):
    """Prepare items for bulk indexing."""
    bulk_data = []
    for item in items:
        item_dict = await process_item(dict(item))
        bulk_data.append({"index": {"_index": index_name, "_id": item_dict["id"]}})
        bulk_data.append(item_dict)
    return bulk_data


async def process_item(item_dict):
    """Process a single item for indexing."""
    processed_dict = {}

    for key, value in item_dict.items():
        if isinstance(value, (list, tuple)):
            processed_dict[key] = list(value)
        elif key == "dct_references_s" and value:
            try:
                processed_dict[key] = json.loads(value)
            except json.JSONDecodeError:
                processed_dict[key] = value
        # Handle geometry fields
        elif key in ["locn_geometry", "dcat_bbox", "dcat_centroid"]:
            # Store original string value
            processed_dict[f"{key}_original"] = value
            # Convert to GeoJSON for Elasticsearch
            if value:
                try:
                    # Check if it's an ENVELOPE format (case insensitive)
                    envelope_match = re.match(
                        r"ENVELOPE\(([-\d.]+)\s*,\s*([-\d.]+)\s*,\s*([-\d.]+)\s*,\s*([-\d.]+)\)",
                        value,
                        re.IGNORECASE,
                    )
                    if envelope_match:
                        # Extract coordinates from ENVELOPE(minx,maxx,maxy,miny)
                        minx, maxx, maxy, miny = map(float, envelope_match.groups())
                        # Create a polygon from the envelope coordinates in counterclockwise order
                        processed_dict[key] = {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [minx, miny],  # bottom left
                                    [maxx, miny],  # bottom right
                                    [maxx, maxy],  # top right
                                    [minx, maxy],  # top left
                                    [minx, miny],  # close the polygon
                                ]
                            ],
                        }
                    else:
                        # Try to parse as JSON if it's not an ENVELOPE
                        try:
                            geom = json.loads(value)
                            if isinstance(geom, dict) and "type" in geom:
                                # Ensure type is capitalized
                                geom["type"] = geom["type"].capitalize()
                                processed_dict[key] = geom
                            else:
                                processed_dict[key] = None
                        except json.JSONDecodeError:
                            processed_dict[key] = None
                except Exception:
                    processed_dict[key] = None
            else:
                processed_dict[key] = None
        else:
            processed_dict[key] = value

    # Add summaries to the document
    processed_dict["ai_summaries"] = await get_item_summaries(processed_dict["id"])

    # Clean and prepare suggestion inputs
    suggestion_inputs = []

    # Add title if it exists
    if title := processed_dict.get("dct_title_s"):
        suggestion_inputs.append(title)

    # Add creators
    if creators := processed_dict.get("dct_creator_sm"):
        if isinstance(creators, list):
            suggestion_inputs.extend(creators)
        else:
            suggestion_inputs.append(creators)

    # Add publishers
    if publishers := processed_dict.get("dct_publisher_sm"):
        if isinstance(publishers, list):
            suggestion_inputs.extend(publishers)
        else:
            suggestion_inputs.append(publishers)

    # Add provider
    if provider := processed_dict.get("schema_provider_s"):
        suggestion_inputs.append(provider)

    # Add subjects
    if subjects := processed_dict.get("dct_subject_sm"):
        if isinstance(subjects, list):
            suggestion_inputs.extend(subjects)
        else:
            suggestion_inputs.append(subjects)

    # Add spatial
    if spatial := processed_dict.get("dct_spatial_sm"):
        if isinstance(spatial, list):
            suggestion_inputs.extend(spatial)
        else:
            suggestion_inputs.append(spatial)

    # Add keywords
    if keywords := processed_dict.get("dcat_keyword_sm"):
        if isinstance(keywords, list):
            suggestion_inputs.extend(keywords)
        else:
            suggestion_inputs.append(keywords)

    # Filter out None values and empty strings
    suggestion_inputs = [s for s in suggestion_inputs if s and str(s).strip()]

    # Get resource classes, ensuring it's a list and has at least one value
    resource_classes = processed_dict.get("gbl_resourceclass_sm", [])
    if isinstance(resource_classes, str):
        resource_classes = [resource_classes]
    if not resource_classes:
        resource_classes = ["none"]

    # Add suggestion field with cleaned data - removed contexts
    processed_dict["suggest"] = {"input": suggestion_inputs}

    return processed_dict


async def get_item_summaries(item_id):
    """Get summaries for an item."""
    try:
        query = """
            SELECT enrichment_id, ai_provider, model, response, created_at
            FROM item_ai_enrichments
            WHERE item_id = :item_id
            ORDER BY created_at DESC
        """
        summaries = await database.fetch_all(query, {"item_id": item_id})

        # Process summaries
        processed_summaries = []
        for summary in summaries:
            summary_dict = dict(summary)

            # Extract the summary text from the response JSON
            if summary_dict.get("response"):
                try:
                    response_data = (
                        json.loads(summary_dict["response"])
                        if isinstance(summary_dict["response"], str)
                        else summary_dict["response"]
                    )
                    summary_dict["summary"] = response_data.get("summary", "")
                except (json.JSONDecodeError, AttributeError):
                    summary_dict["summary"] = ""

            processed_summaries.append(summary_dict)

        return processed_summaries
    except Exception as e:
        print(f"Error getting summaries for item {item_id}: {str(e)}")
        return []


def process_geometry(geometry):
    """Process geometry for Elasticsearch."""
    if not geometry:
        return None

    try:
        # Try to parse as GeoJSON
        if isinstance(geometry, str):
            # Check if it's an ENVELOPE format (case insensitive)
            envelope_match = re.match(
                r"ENVELOPE\(([-\d.]+)\s*,\s*([-\d.]+)\s*,\s*([-\d.]+)\s*,\s*([-\d.]+)\)",
                geometry,
                re.IGNORECASE,
            )
            if envelope_match:
                # Extract coordinates from ENVELOPE(minx,maxx,maxy,miny)
                minx, maxx, maxy, miny = map(float, envelope_match.groups())
                # Create a polygon from the envelope coordinates in counterclockwise order
                return {
                    "type": "polygon",
                    "coordinates": [
                        [
                            [minx, miny],  # bottom left
                            [maxx, miny],  # bottom right
                            [maxx, maxy],  # top right
                            [minx, maxy],  # top left
                            [minx, miny],  # close the polygon
                        ]
                    ],
                }

            # Try to parse as JSON
            try:
                geometry = json.loads(geometry)
            except json.JSONDecodeError:
                return None

        # Handle different geometry types
        if isinstance(geometry, dict):
            geom_type = geometry.get("type", "").lower()
            if geom_type == "point":
                return {"type": "point", "coordinates": geometry.get("coordinates", [0, 0])}
            elif geom_type in ["polygon", "multipolygon"]:
                return {"type": geom_type, "coordinates": geometry["coordinates"]}
            else:
                return None
        else:
            return None

    except Exception:
        return None


async def perform_bulk_indexing(bulk_data, index_name, bulk_size=100):
    """Perform bulk indexing in smaller chunks."""
    # Split the bulk_data into smaller chunks
    for i in range(0, len(bulk_data), bulk_size):
        chunk = bulk_data[i : i + bulk_size]
        try:
            # Perform the bulk operation for the current chunk
            response = await es.bulk(operations=chunk, index=index_name, refresh=True)
            # Check for errors in the response
            if response.get("errors"):
                print(f"Errors occurred during bulk indexing: {response['items']}")
        except Exception as e:
            print(f"Exception during bulk indexing: {str(e)}")
            # Optionally, implement retry logic here


async def reindex_items():
    """Reindex all items from PostgreSQL into Elasticsearch with the new mapping."""
    index_name = os.getenv("ELASTICSEARCH_INDEX", "btaa_geometadata_api")

    try:
        # Delete the existing index if it exists
        if await es.indices.exists(index=index_name):
            logger.info(f"Deleting existing index {index_name}")
            await es.indices.delete(index=index_name)

        # Initialize Elasticsearch with the new mapping
        from .client import init_elasticsearch

        await init_elasticsearch()

        # Process items in chunks
        chunk_size = 1000  # Adjust this based on your needs
        offset = 0
        total_processed = 0

        while True:
            # Fetch a chunk of documents from the database
            query = items.select().offset(offset).limit(chunk_size)
            chunk = await database.fetch_all(query)

            if not chunk:
                break  # No more items to process

            # Prepare bulk data for this chunk
            bulk_data = await prepare_bulk_data(chunk, index_name)

            if bulk_data:
                # Index this chunk
                await perform_bulk_indexing(bulk_data, index_name)
                total_processed += len(chunk)
                logger.info(f"Indexed {total_processed} items so far")

            offset += chunk_size

        if total_processed > 0:
            return {"message": f"Successfully indexed {total_processed} items"}
        return {"message": "No items to index"}

    except Exception as e:
        logger.error(f"Error during reindexing: {str(e)}", exc_info=True)
        raise
