import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import parse_qs

from dotenv import load_dotenv
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from sqlalchemy import select

from app.elasticsearch.index import reindex_items
from app.services.download_service import DownloadService
from db.database import database
from db.models import items

from ...elasticsearch import search_items
from ...elasticsearch.client import es
from ...services.cache_service import (
    ENDPOINT_CACHE,
    CacheService,
    cached_endpoint,
    invalidate_cache_with_prefix,
)
from ...services.citation_service import CitationService
from ...services.image_service import ImageService
from ...services.viewer_service import create_viewer_attributes
from ...tasks.entities import generate_geo_entities
from ...tasks.summarization import generate_item_summary
from .jsonp import JSONPResponse
from .shared import SORT_MAPPINGS, SortOption

# Load environment variables from .env file
load_dotenv()

router = APIRouter()

logger = logging.getLogger(__name__)

base_url = os.getenv("APPLICATION_URL", "http://localhost:8000/api/v1/")

# Cache TTL configuration in seconds
ITEM_CACHE_TTL = int(os.getenv("ITEM_CACHE_TTL", 86400))  # 24 hours
SEARCH_CACHE_TTL = int(os.getenv("SEARCH_CACHE_TTL", 3600))  # 1 hour
SUGGEST_CACHE_TTL = int(os.getenv("SUGGEST_CACHE_TTL", 7200))  # 2 hours
LIST_CACHE_TTL = int(os.getenv("LIST_CACHE_TTL", 43200))  # 12 hours


@router.get("")
async def api_root():
    """Return basic API information including version."""
    return JSONResponse(
        content={
            "api": "BTAA Geodata API",
            "version": "0.1.0",
            "description": (
                "API for accessing geospatial data from the Big Ten Academic Alliance Geoportal"
            ),
            "endpoints": [
                "/items",
                "/search",
                "/suggest",
                "/thumbnails",
                "/cache/clear",
                "/reindex",
            ],
        }
    )


def add_thumbnail_url(item: Dict) -> Dict:
    """Add the ui_thumbnail_url to the item attributes."""
    # Ensure 'attributes' key exists
    if "attributes" not in item:
        item["attributes"] = {}

    image_service = ImageService(item)
    thumbnail_url = image_service.get_thumbnail_url()
    item["attributes"]["ui_thumbnail_url"] = thumbnail_url
    return item


def add_citations(item: Dict) -> Dict:
    """Add citations to an item."""
    # Ensure 'attributes' key exists
    if "attributes" not in item:
        item["attributes"] = {}

    try:
        citation_service = CitationService(item)
        item["attributes"]["ui_citation"] = citation_service.get_citation()
    except Exception as e:
        logger.error(f"Failed to generate citation: {str(e)}")
        item["attributes"]["ui_citation"] = "Citation unavailable"
    return item


def sanitize_for_json(obj: Any) -> Any:
    """Recursively convert datetime objects to ISO format strings."""
    if isinstance(obj, dict):
        return {key: sanitize_for_json(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(item) for item in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def create_response(
    content: Dict | JSONResponse, callback: Optional[str] = None, status_code: int = 200
) -> JSONResponse:
    """Create either a JSON or JSONP response based on callback parameter."""
    # If content is already a JSONResponse, return it as is
    if isinstance(content, JSONResponse):
        return content

    # Sanitize content before serialization
    sanitized_content = sanitize_for_json(content)

    if callback:
        return JSONPResponse(content=sanitized_content, callback=callback, status_code=status_code)
    return JSONResponse(content=sanitized_content, status_code=status_code)


def add_ui_attributes(item: Dict) -> Dict:
    """Add UI attributes to an item."""
    # Parse references if needed
    if isinstance(item.get("dct_references_s"), str):
        try:
            item["dct_references_s"] = json.loads(item["dct_references_s"])
        except json.JSONDecodeError:
            item["dct_references_s"] = {}

    # Create services
    image_service = ImageService(item)
    citation_service = CitationService(item)
    download_service = DownloadService(item)

    # Add viewer attributes
    item.update(create_viewer_attributes(item))

    # Add thumbnail URL if available
    if thumbnail_url := image_service.get_thumbnail_url():
        item["ui_thumbnail_url"] = thumbnail_url

    # Add citation
    item["ui_citation"] = citation_service.get_citation()

    # Add download options
    item["ui_downloads"] = download_service.get_download_options()

    return item


async def get_item_relationships(item_id: str) -> Dict:
    """Get all relationships for an item."""
    try:
        logger.info(f"Fetching relationships for item: {item_id}")

        # Get outgoing relationships (where item is subject)
        relationships_query = """
            SELECT predicate, object_id, dct_title_s
            FROM item_relationships
            JOIN items 
            ON items.id = item_relationships.object_id
            WHERE subject_id = :item_id
            ORDER BY dct_title_s ASC
        """
        db_relationships = await database.fetch_all(relationships_query, {"item_id": item_id})
        logger.info(f"Found {len(db_relationships)} relationships")
        logger.info(f"Relationships: {db_relationships}")

        relationships = {}

        # Process outgoing relationships
        for rel in db_relationships:
            if rel["predicate"] not in relationships:
                relationships[rel["predicate"]] = []
            relationships[rel["predicate"]].append(
                {
                    "item_id": rel["object_id"],
                    "item_title": rel["dct_title_s"],
                    "link": f"{base_url}/items/{rel['object_id']}",
                }
            )
            logger.debug(f"Added relationship: {rel['predicate']} -> {rel['object_id']}")

        logger.info(f"Final relationships structure: {relationships}")
        return relationships

    except Exception as e:
        logger.error(f"Error getting relationships: {e}", exc_info=True)
        return {}


@router.get("/items/{id}")
@cached_endpoint(ttl=ITEM_CACHE_TTL)
async def get_item(
    id: str,
    callback: Optional[str] = None,
    include_relationships: bool = True,
    include_summaries: bool = True,
):
    """Get a single item by ID."""
    try:
        # Get item
        query = items.select().where(items.c.id == id)
        result = await database.fetch_one(query)

        if not result:
            raise HTTPException(status_code=404, detail="Item not found")

        # Convert to dict and process
        item = dict(result)

        # Add UI attributes
        processed_item = add_ui_attributes(item)

        # Get relationships
        if include_relationships:
            relationships = await get_item_relationships(id)
            logger.info(f"Got relationships for {id}: {relationships}")  # Debug line
        else:
            relationships = {}

        # Add relationships to UI attributes
        processed_item["ui_relationships"] = relationships

        # Get summaries if requested
        summaries = []
        if include_summaries:
            try:
                summaries_query = """
                    SELECT * FROM item_ai_enrichments 
                    WHERE item_id = :item_id 
                    ORDER BY created_at DESC
                """
                summaries_result = await database.fetch_all(summaries_query, {"item_id": id})
                summaries = [dict(summary) for summary in summaries_result]
                logger.info(f"Got {len(summaries)} summaries for {id}")
            except Exception as e:
                logger.error(f"Error fetching summaries: {str(e)}")
                summaries = []

        # Add summaries to UI attributes
        processed_item["ui_summaries"] = summaries

        # Create response
        response = {"data": {"type": "item", "id": id, "attributes": processed_item}}

        logger.info(f"Final response structure: {response}")  # Debug line
        return create_response(response, callback)

    except HTTPException:
        # Re-raise HTTPException to be handled by FastAPI's exception handler
        raise
    except Exception as e:
        logger.error(f"Item fetch failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/items/")
@cached_endpoint(ttl=LIST_CACHE_TTL)
async def list_items(
    skip: int = 0,
    limit: int = 10,
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    query = items.select().offset(skip).limit(limit)
    results = await database.fetch_all(query)

    processed_items = []
    for item in results:
        item_dict = dict(item)
        item_dict = add_thumbnail_url(item_dict)
        item_dict = add_citations(item_dict)
        viewer_attributes = create_viewer_attributes(item_dict)

        # Use DownloadService to get download options
        download_service = DownloadService(item_dict)
        ui_downloads = download_service.get_download_options()

        processed_items.append(
            {
                "type": "item",
                "id": str(item_dict["id"]),
                "attributes": {
                    **item_dict,
                    **viewer_attributes,
                    "ui_citation": item_dict.get("ui_citation"),
                    "ui_thumbnail_url": item_dict.get("ui_thumbnail_url"),
                    "ui_viewer_endpoint": viewer_attributes.get("ui_viewer_endpoint"),
                    "ui_viewer_geometry": viewer_attributes.get("ui_viewer_geometry"),
                    "ui_viewer_protocol": viewer_attributes.get("ui_viewer_protocol"),
                    "ui_downloads": ui_downloads,  # Include ui_downloads in the response
                },
            }
        )

    return create_response({"data": processed_items}, callback)


@router.get("/search")
@cached_endpoint(ttl=SEARCH_CACHE_TTL)
async def search(
    request: Request,
    q: Optional[str] = Query(None, description="Search query"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Results per page"),
    sort: Optional[SortOption] = None,
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    """Search endpoint with caching support."""
    try:
        # Set default sort option inside the function instead of in the parameter default
        if sort is None:
            sort = SortOption.RELEVANCE

        timings = {}
        cache_status = "miss"  # Default to cache miss for timing info

        if ENDPOINT_CACHE:
            timings["cache"] = "enabled"
        else:
            timings["cache"] = "disabled"

        start_time = time.time()

        # Calculate skip from page/limit
        skip = (page - 1) * limit

        # Get filter queries from request
        query_string = str(request.query_params)
        filter_query = extract_filter_queries(query_string)

        # Get sort mapping
        sort_mapping = SORT_MAPPINGS.get(sort, None)

        # Elasticsearch query
        es_start = time.time()
        results = await search_items(
            query=q,
            fq=filter_query,
            skip=skip,
            limit=limit,
            sort=sort_mapping,
        )
        es_time = (time.time() - es_start) * 1000
        timings["elasticsearch"] = f"{es_time:.0f}ms"

        # Process each item
        process_start = time.time()
        docs_processed = 0
        citation_time = 0
        thumbnail_time = 0
        viewer_time = 0

        for item in results["data"]:
            doc_start = time.time()

            # Add thumbnail URL
            thumb_start = time.time()
            image_service = ImageService(item["attributes"])
            item["attributes"]["ui_thumbnail_url"] = image_service.get_thumbnail_url()
            thumbnail_time += time.time() - thumb_start

            # Add citation
            cite_start = time.time()
            citation_service = CitationService(item["attributes"])
            item["attributes"]["ui_citation"] = citation_service.get_citation()
            citation_time += time.time() - cite_start

            # Add viewer attributes
            viewer_start = time.time()
            viewer_attrs = create_viewer_attributes(item["attributes"])
            item["attributes"].update(viewer_attrs)
            viewer_time += time.time() - viewer_start

            docs_processed += 1

        process_time = time.time() - process_start
        timings["item_processing"] = {
            "total": f"{(process_time * 1000):.0f}ms",
            "per_item": (
                f"{((process_time / docs_processed) * 1000):.0f}ms" if docs_processed > 0 else "0ms"
            ),
            "thumbnail_service": f"{(thumbnail_time * 1000):.0f}ms",
            "citation_service": f"{(citation_time * 1000):.0f}ms",
            "viewer_service": f"{(viewer_time * 1000):.0f}ms",
        }

        total_time = time.time() - start_time
        timings["total_response_time"] = f"{(total_time * 1000):.0f}ms"

        results["query_time"] = timings

        # Extract and add suggestions to meta if they exist
        if "meta" in results and "suggestions" in results["meta"]:
            results["meta"]["spelling_suggestions"] = results["meta"].pop("suggestions")

        # Sanitize the entire results object for JSON
        sanitized_results = sanitize_for_json(results)

        return create_response(sanitized_results, callback)

    except Exception as e:
        logger.error("Search endpoint error", exc_info=True)
        error_response = {
            "message": "Search operation failed",
            "error": str(e),
            "query": q,
            "filters": filter_query if "filter_query" in locals() else None,
            "sort": sort.value if sort else None,
        }
        return create_response(error_response, callback)


def extract_filter_queries(params: Dict) -> Dict:
    """Extract filter queries from request parameters."""
    filter_query = {}
    # Parse the raw query string to handle multiple values
    raw_params = parse_qs(str(params))

    agg_to_field = {
        "id_agg": "id",
        "spatial_agg": "dct_spatial_sm",
        "resource_type_agg": "gbl_resourcetype_sm",
        "resource_class_agg": "gbl_resourceclass_sm",
        "index_year_agg": "gbl_indexyear_im",
        "language_agg": "dct_language_sm",
        "creator_agg": "dct_creator_sm",
        "provider_agg": "schema_provider_s",
        "access_rights_agg": "dct_accessrights_sm",
        "georeferenced_agg": "gbl_georeferenced_b",
    }

    for key, values in raw_params.items():
        if key.startswith("fq[") and key.endswith("][]"):
            field = key[3:-3]  # Remove 'fq[' and '[]'
            if field in agg_to_field:
                es_field = agg_to_field[field]
                if values:  # values is already a list from parse_qs
                    filter_query[es_field] = values

    return filter_query


@router.get("/suggest")
@cached_endpoint(ttl=SUGGEST_CACHE_TTL)
async def suggest(
    q: str = Query(..., description="Query string for suggestions"),
    resource_class: Optional[str] = Query(None, description="Filter suggestions by resource class"),
    size: int = Query(5, ge=1, le=20, description="Number of suggestions to return"),
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    """Get autocomplete suggestions with caching support."""
    try:
        # Simplified query without contexts first
        suggest_query = {
            "_source": [
                "dct_title_s",
                "dct_creator_sm",
                "dct_publisher_sm",
                "schema_provider_s",
                "dct_subject_sm",
                "dct_spatial_sm",
            ],
            "suggest": {
                "my-suggestion": {  # Changed name to be more explicit
                    "prefix": q,
                    "completion": {
                        "field": "suggest",
                        "size": size,
                        "skip_duplicates": True,
                        "fuzzy": {"fuzziness": "AUTO"},
                    },
                }
            },
        }

        index_name = os.getenv("ELASTICSEARCH_INDEX", "btaa_geometadata_api")

        # Print the query for debugging
        # print("Suggest Query:", json.dumps(suggest_query, indent=2))

        response = await es.search(index=index_name, body=suggest_query)

        # Convert response to dict for serialization
        response_dict = response.body

        # Print the full response for debugging
        # print("ES Response:", json.dumps(response_dict, indent=2))

        suggestions = []
        seen_ids = set()  # Track seen suggestion IDs
        if response_dict.get("suggest", {}).get("my-suggestion"):
            for suggestion in response_dict["suggest"]["my-suggestion"]:
                # print(f"Processing suggestion: {suggestion}")  # Debug print
                if options := suggestion.get("options", []):
                    for option in options:
                        suggestion_id = option["_id"]
                        if suggestion_id not in seen_ids:  # Check for duplicates
                            seen_ids.add(suggestion_id)
                            suggestions.append(
                                {
                                    "type": "suggestion",
                                    "id": suggestion_id,
                                    "attributes": {
                                        "text": option.get("text", ""),
                                        "title": option.get("_source", {}).get("dct_title_s", ""),
                                        "score": option.get("_score", 0),
                                    },
                                }
                            )

        response = {
            "data": suggestions,
            "meta": {
                "query": q,
                "resource_class": resource_class,
                "es_query": suggest_query,
                "es_response": response_dict,
            },
        }

        return create_response(response, callback)

    except Exception as e:
        # print(f"Suggestion error: {e}")
        error_response = {"detail": f"Suggestion error: {str(e)}\nQuery: {suggest_query}"}
        return create_response(error_response, callback)


@router.get("/cache/clear")
async def clear_cache(
    cache_type: Optional[str] = Query(
        None, description="Type of cache to clear (search, item, suggest, all)"
    ),
):
    """Clear specified cache or all cache if not specified."""
    if not ENDPOINT_CACHE:
        return JSONResponse(
            content={"message": "Caching is disabled. Set ENDPOINT_CACHE=true to enable."}
        )

    try:
        cache_service = CacheService()

        if cache_type == "search" or cache_type is None:
            await invalidate_cache_with_prefix("app.api.v1.endpoints:search")

        if cache_type == "item" or cache_type is None:
            await invalidate_cache_with_prefix("app.api.v1.endpoints:get_item")

        if cache_type == "suggest" or cache_type is None:
            await invalidate_cache_with_prefix("app.api.v1.endpoints:suggest")

        if cache_type == "all" or cache_type is None:
            await cache_service.flush_all()

        return JSONResponse(
            content={"message": f"Cache cleared successfully: {cache_type or 'all'}"}
        )
    except Exception as e:
        logger.error(f"Error clearing cache: {str(e)}")
        return JSONResponse(content={"error": f"Failed to clear cache: {str(e)}"}, status_code=500)


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
                logger.error(f"Errors occurred during bulk indexing: {response['items']}")
        except Exception as e:
            logger.error(f"Exception during bulk indexing: {str(e)}")
            # Optionally, implement retry logic here


@router.get("/thumbnails/{image_hash}")
async def get_thumbnail(image_hash: str):
    """Serve a cached thumbnail image."""
    try:
        # Create service without item (we only need cache access)
        image_service = ImageService({})
        image_data = await image_service.get_cached_image(image_hash)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    if not image_data:
        raise HTTPException(status_code=404, detail="Image not found")

    return Response(
        content=image_data,
        media_type="image/jpeg",
        headers={"Cache-Control": "public, max-age=31536000"},  # Cache for 1 year
    )


@router.post("/items/{id}/summarize")
async def summarize_item(
    id: str,
    background_tasks: BackgroundTasks,
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    """
    Trigger the generation of a summary and OCR text for an item.
    This endpoint will:
    1. Fetch the item metadata
    2. Get the asset path and type
    3. Trigger asynchronous tasks to generate the summary and OCR text
    4. Return immediately with task IDs
    """
    try:
        # Fetch the item
        async with database.transaction():
            query = select(items).where(items.c.id == id)
            result = await database.fetch_one(query)

            if not result:
                raise HTTPException(status_code=404, detail="Item not found")

            # Convert to dict and handle datetime serialization
            item = dict(result)
            for key, value in item.items():
                if isinstance(value, datetime):
                    item[key] = value.isoformat()

            logger.info(f"Processing item {id}")
            logger.debug(f"Raw item data: {json.dumps(item, indent=2)}")

            # Get asset information
            asset_path = None
            asset_type = None

            # Parse dct_references_s to identify candidate assets
            references = item.get("dct_references_s", {})
            logger.info(f"Raw references for item {id}: {references}")

            if isinstance(references, str):
                try:
                    references = json.loads(references)
                    logger.info(
                        f"Parsed references for item {id}: {json.dumps(references, indent=2)}"
                    )
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse references JSON for item {id}: {references}")
                    references = {}

            # Define asset type mappings
            asset_type_mappings = {
                "http://schema.org/downloadUrl": "download",
                "http://iiif.io/api/image": "iiif_image",
                "http://iiif.io/api/presentation#manifest": "iiif_manifest",
                "https://github.com/cogeotiff/cog-spec": "cog",
                "https://github.com/protomaps/PMTiles": "pmtiles",
            }

            # Check for each reference type
            for ref_type, asset_type_name in asset_type_mappings.items():
                if ref_type in references:
                    ref_value = references[ref_type]
                    logger.info(
                        f"Found reference type {ref_type} with value {ref_value} for item {id}"
                    )

                    # Handle both string and array values
                    if isinstance(ref_value, list) and ref_value:
                        # For arrays, take the first item for now
                        asset_path = ref_value[0]
                        asset_type = asset_type_name
                        logger.info(
                            f"Using first item from array: asset_path={asset_path}, "
                            f"asset_type={asset_type}"
                        )
                        break
                    elif isinstance(ref_value, str) and ref_value:
                        asset_path = ref_value
                        asset_type = asset_type_name
                        logger.info(
                            f"Using string value: asset_path={asset_path}, asset_type={asset_type}"
                        )
                        break

            # If no specific asset type was found, use the item format as fallback
            if not asset_type:
                asset_type = item.get("dc_format_s")
                logger.info(f"No specific asset type found, using format fallback: {asset_type}")

            logger.info(
                f"Final asset determination for item {id}: path={asset_path}, type={asset_type}"
            )

            # Trigger the summarization task
            summary_task = generate_item_summary.delay(
                item_id=id, metadata=item, asset_path=asset_path, asset_type=asset_type
            )
            logger.info(f"Started summary task {summary_task.id} for item {id}")

            # If we have an asset, also trigger OCR
            ocr_task = None
            if asset_path and asset_type:
                from app.tasks.ocr import generate_item_ocr

                ocr_task = generate_item_ocr.delay(
                    item_id=id, metadata=item, asset_path=asset_path, asset_type=asset_type
                )
                logger.info(f"Started OCR task {ocr_task.id} for item {id}")
            else:
                logger.warning(f"No asset found for OCR processing on item {id}")
                logger.debug(f"Missing: asset_path={asset_path}, asset_type={asset_type}")

            # Invalidate the item cache since we'll be updating it
            invalidate_cache_with_prefix(f"item:{id}")

            # Create response data and ensure all datetime objects are serialized
            response_data = {
                "status": "success",
                "message": "Summary and OCR generation started",
                "tasks": {"summary": summary_task.id, "ocr": ocr_task.id if ocr_task else None},
            }

            # Sanitize the response data before returning
            sanitized_response = sanitize_for_json(response_data)
            return create_response(sanitized_response, callback)

    except Exception as e:
        logger.error(f"Error triggering summary and OCR generation for item {id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/items/{id}/summaries")
async def get_item_summaries(
    id: str, callback: Optional[str] = Query(None, description="JSONP callback name")
):
    """
    Get all summaries for an item.

    Args:
        id: The item ID
        callback: Optional JSONP callback name

    Returns:
        JSON response with the summaries
    """
    try:
        # Query the database for summaries
        async with database.transaction():
            query = """
                SELECT * FROM ai_enrichments 
                WHERE item_id = :item_id 
                ORDER BY created_at DESC
            """
            summaries = await database.fetch_all(query, {"item_id": id})

            # Convert to list of dicts
            summaries_list = [dict(summary) for summary in summaries]

            # Create response
            response_data = {
                "data": {"type": "summaries", "id": id, "attributes": {"summaries": summaries_list}}
            }

            return create_response(response_data, callback)

    except Exception as e:
        logger.error(f"Error retrieving summaries for item {id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/reindex", response_model=dict)
async def reindex(callback: Optional[str] = Query(None, description="JSONP callback name")):
    """Trigger reindexing of all items in Elasticsearch."""
    try:
        # When reindexing, invalidate all search and suggest caches
        if ENDPOINT_CACHE:
            logger.info("Invalidating search and suggest caches")
            await invalidate_cache_with_prefix("app.api.v1.endpoints:search")
            await invalidate_cache_with_prefix("app.api.v1.endpoints:suggest")

        result = await reindex_items()
        return create_response(
            {"status": "success", "message": "Reindexing completed", "details": result}, callback
        )
    except Exception as e:
        logger.error(f"Reindexing failed: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail={"message": "Reindexing failed", "error": str(e)}
        ) from e


@router.post("/items/{id}/identify-geo-entities")
async def identify_geo_entities(
    id: str,
    background_tasks: BackgroundTasks,
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    """
    Trigger the identification of geographic entities in an item.
    This endpoint will:
    1. Fetch the item metadata
    2. Trigger an asynchronous task to identify geographic entities
    3. Return immediately with task ID
    """
    try:
        # Fetch the item
        async with database.transaction():
            query = select(items).where(items.c.id == id)
            result = await database.fetch_one(query)

            if not result:
                raise HTTPException(status_code=404, detail="Item not found")

            # Convert to dict and handle datetime serialization
            item = dict(result)
            for key, value in item.items():
                if isinstance(value, datetime):
                    item[key] = value.isoformat()

            logger.info(f"Processing item {id} for geographic entity identification")
            logger.debug(f"Raw item data: {json.dumps(item, indent=2)}")

            # Trigger the geographic entity identification task
            geo_entities_task = generate_geo_entities.delay(item_id=id, metadata=item)
            logger.info(
                f"Started geographic entity identification task {geo_entities_task.id} "
                f"for item {id}"
            )

            # Invalidate the item cache since we'll be updating it
            invalidate_cache_with_prefix(f"item:{id}")

            # Create response data
            response_data = {
                "status": "success",
                "message": "Geographic entity identification started",
                "task_id": geo_entities_task.id,
            }

            return create_response(response_data, callback)

    except Exception as e:
        logger.error(f"Error triggering geographic entity identification for item {id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/items/{id}/ocr")
async def generate_ocr(
    id: str,
    background_tasks: BackgroundTasks,
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    """
    Trigger OCR generation for an item.
    This endpoint will:
    1. Fetch the item metadata
    2. Get the asset path and type
    3. Trigger an asynchronous task to generate OCR text
    4. Return immediately with task ID
    """
    try:
        # Fetch the item
        async with database.transaction():
            query = select(items).where(items.c.id == id)
            result = await database.fetch_one(query)

            if not result:
                raise HTTPException(status_code=404, detail="Item not found")

            # Convert to dict and handle datetime serialization
            item = dict(result)
            for key, value in item.items():
                if isinstance(value, datetime):
                    item[key] = value.isoformat()

            logger.info(f"Processing item {id} for OCR")
            logger.debug(f"Raw item data: {json.dumps(item, indent=2)}")

            # Get asset information
            asset_path = None
            asset_type = None

            # Parse dct_references_s to identify candidate assets
            references = item.get("dct_references_s", {})
            logger.info(f"Raw references for item {id}: {references}")

            if isinstance(references, str):
                try:
                    references = json.loads(references)
                    logger.info(
                        f"Parsed references for item {id}: {json.dumps(references, indent=2)}"
                    )
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse references JSON for item {id}: {references}")
                    references = {}

            # Define asset type mappings
            asset_type_mappings = {
                "http://schema.org/downloadUrl": "download",
                "http://iiif.io/api/image": "iiif_image",
                "http://iiif.io/api/presentation#manifest": "iiif_manifest",
                "https://github.com/cogeotiff/cog-spec": "cog",
                "https://github.com/protomaps/PMTiles": "pmtiles",
            }

            # Check for each reference type
            for ref_type, asset_type_name in asset_type_mappings.items():
                if ref_type in references:
                    ref_value = references[ref_type]
                    logger.info(
                        f"Found reference type {ref_type} with value {ref_value} for item {id}"
                    )

                    # Handle both string and array values
                    if isinstance(ref_value, list) and ref_value:
                        asset_path = ref_value[0]
                        asset_type = asset_type_name
                        break
                    elif isinstance(ref_value, str) and ref_value:
                        asset_path = ref_value
                        asset_type = asset_type_name
                        break

            # If no specific asset type was found, use the item format as fallback
            if not asset_type:
                asset_type = item.get("dc_format_s")
                logger.info(f"No specific asset type found, using format fallback: {asset_type}")

            logger.info(
                f"Final asset determination for item {id}: path={asset_path}, type={asset_type}"
            )

            # Trigger the OCR task
            from app.tasks.ocr import generate_item_ocr

            ocr_task = generate_item_ocr.delay(
                item_id=id, metadata=item, asset_path=asset_path, asset_type=asset_type
            )
            logger.info(f"Started OCR task {ocr_task.id} for item {id}")

            # Invalidate the item cache since we'll be updating it
            invalidate_cache_with_prefix(f"item:{id}")

            # Create response data
            response_data = {
                "status": "success",
                "message": "OCR generation started",
                "task_id": ocr_task.id,
            }

            return create_response(response_data, callback)

    except Exception as e:
        logger.error(f"Error triggering OCR generation for item {id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) from e
