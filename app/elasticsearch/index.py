from .client import es
from db.database import database
from db.models import geoblacklight_development, ai_enrichments
import json
import re
import os
import logging

logger = logging.getLogger(__name__)


async def index_documents():
    """Index all documents from PostgreSQL into Elasticsearch."""
    index_name = os.getenv("ELASTICSEARCH_INDEX", "geoblacklight")

    if await es.indices.exists(index=index_name):
        await es.indices.delete(index=index_name)

    from .client import init_elasticsearch

    await init_elasticsearch()

    documents = await database.fetch_all(geoblacklight_development.select())
    bulk_data = await prepare_bulk_data(documents, index_name)

    if bulk_data:
        return await perform_bulk_indexing(bulk_data, index_name)

    return {"message": "No documents to index"}


async def prepare_bulk_data(documents, index_name):
    """Prepare documents for bulk indexing."""
    bulk_data = []
    for doc in documents:
        doc_dict = await process_document(dict(doc))
        bulk_data.append({"index": {"_index": index_name, "_id": doc_dict["id"]}})
        bulk_data.append(doc_dict)
    return bulk_data


async def process_document(doc_dict):
    """Process a single document for indexing."""
    for key, value in doc_dict.items():
        if isinstance(value, (list, tuple)):
            doc_dict[key] = list(value)
        elif key == "dct_references_s" and value:
            try:
                doc_dict[key] = json.loads(value)
            except json.JSONDecodeError:
                doc_dict[key] = value
        elif key == "locn_geometry":
            try:
                doc_dict[key] = process_geometry(value)
            except Exception:
                doc_dict[key] = None
        elif key == "dcat_bbox":
            try:
                doc_dict[key] = process_geometry(value)
            except Exception:
                doc_dict[key] = None
        elif key == "dcat_centroid":
            try:
                doc_dict[key] = process_geometry(value)
            except Exception:
                doc_dict[key] = None

    # Add summaries to the document
    doc_dict["ai_summaries"] = await get_document_summaries(doc_dict["id"])

    # Clean and prepare suggestion inputs
    suggestion_inputs = []

    # Add title if it exists
    if title := doc_dict.get("dct_title_s"):
        suggestion_inputs.append(title)

    # Add creators
    if creators := doc_dict.get("dct_creator_sm"):
        if isinstance(creators, list):
            suggestion_inputs.extend(creators)
        else:
            suggestion_inputs.append(creators)

    # Add publishers
    if publishers := doc_dict.get("dct_publisher_sm"):
        if isinstance(publishers, list):
            suggestion_inputs.extend(publishers)
        else:
            suggestion_inputs.append(publishers)

    # Add provider
    if provider := doc_dict.get("schema_provider_s"):
        suggestion_inputs.append(provider)

    # Add subjects
    if subjects := doc_dict.get("dct_subject_sm"):
        if isinstance(subjects, list):
            suggestion_inputs.extend(subjects)
        else:
            suggestion_inputs.append(subjects)

    # Add spatial
    if spatial := doc_dict.get("dct_spatial_sm"):
        if isinstance(spatial, list):
            suggestion_inputs.extend(spatial)
        else:
            suggestion_inputs.append(spatial)

    # Add keywords
    if keywords := doc_dict.get("dcat_keyword_sm"):
        if isinstance(keywords, list):
            suggestion_inputs.extend(keywords)
        else:
            suggestion_inputs.append(keywords)

    # Filter out None values and empty strings
    suggestion_inputs = [s for s in suggestion_inputs if s and str(s).strip()]

    # Get resource classes, ensuring it's a list and has at least one value
    resource_classes = doc_dict.get("gbl_resourceclass_sm", [])
    if isinstance(resource_classes, str):
        resource_classes = [resource_classes]
    if not resource_classes:
        resource_classes = ["none"]

    # Add suggestion field with cleaned data - removed contexts
    doc_dict["suggest"] = {"input": suggestion_inputs}

    return doc_dict


async def get_document_summaries(document_id):
    """Get summaries for a document."""
    try:
        query = """
            SELECT enrichment_id, ai_provider, model, response, created_at
            FROM ai_enrichments
            WHERE document_id = :document_id
            ORDER BY created_at DESC
        """
        summaries = await database.fetch_all(query, {"document_id": document_id})

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
        print(f"Error getting summaries for document {document_id}: {str(e)}")
        return []


def process_geometry(geometry):
    """Process geometry for Elasticsearch."""
    if not geometry:
        return None

    try:
        # Try to parse as GeoJSON
        if isinstance(geometry, str):
            geometry = json.loads(geometry)

        # Handle different geometry types
        if geometry.get("type") == "Point":
            return {"type": "point", "coordinates": geometry.get("coordinates", [0, 0])}
        elif geometry.get("type") in ["Polygon", "MultiPolygon"]:
            return geometry
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


async def reindex_documents():
    """Reindex all documents from PostgreSQL into Elasticsearch with the new mapping."""
    index_name = os.getenv("ELASTICSEARCH_INDEX", "geoblacklight")

    try:
        # Delete the existing index if it exists
        if await es.indices.exists(index=index_name):
            logger.info(f"Deleting existing index {index_name}")
            await es.indices.delete(index=index_name)

        # Initialize Elasticsearch with the new mapping
        from .client import init_elasticsearch

        await init_elasticsearch()

        # Fetch all documents from the database
        documents = await database.fetch_all(geoblacklight_development.select())

        # Prepare bulk data for indexing
        bulk_data = await prepare_bulk_data(documents, index_name)

        if bulk_data:
            return await perform_bulk_indexing(bulk_data, index_name)

        return {"message": "No documents to index"}

    except Exception as e:
        logger.error(f"Error during reindexing: {str(e)}", exc_info=True)
        raise
