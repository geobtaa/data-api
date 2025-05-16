import json
import logging
import os
from datetime import datetime
from typing import Dict, Optional

from dotenv import load_dotenv
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from sqlalchemy import select

from app.api.v1.utils import (
    JSONResponse,
    add_citations,
    add_thumbnail_url,
    add_ui_attributes,
    create_response,
    sanitize_for_json,
)
from app.elasticsearch.index import reindex_items
from app.services.cache_service import (
    ENDPOINT_CACHE,
    cached_endpoint,
    invalidate_cache_with_prefix,
)
from app.services.citation_service import CitationService
from app.services.download_service import DownloadService
from app.services.image_service import ImageService
from app.services.search_service import SearchService
from app.services.viewer_service import ViewerService
from app.tasks.entities import generate_geo_entities
from app.tasks.ocr import generate_item_ocr
from app.tasks.summarization import generate_item_summary
from db.database import database
from db.models import items

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
            "endpoints": ["/items", "/search", "/suggest"],
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


@router.get("/items/{id}")
@cached_endpoint(ttl=ITEM_CACHE_TTL)
async def get_item(
    id: str,
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    """Get a single item by ID."""
    try:
        search_service = SearchService()
        item = await search_service.get_item(id)
        if not item:
            return JSONResponse(content={"error": "Item not found"}, status_code=404)

        # Sanitize the item data for JSON serialization
        item = sanitize_for_json(item)
        return create_response(item, callback)
    except HTTPException as e:
        # Re-raise HTTP exceptions to maintain their status code
        raise
    except Exception as e:
        logger.error(f"Error getting item {id}: {str(e)}", exc_info=True)
        return JSONResponse(content={"error": str(e)}, status_code=500)


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
        # Convert to dict and sanitize datetime objects
        item_dict = sanitize_for_json(dict(item))
        item_dict = add_thumbnail_url(item_dict)
        item_dict = add_citations(item_dict)

        # Use ViewerService to get viewer attributes
        viewer_service = ViewerService(item_dict)
        viewer_attributes = viewer_service.get_viewer_attributes()

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
                    "ui_downloads": ui_downloads,
                },
            }
        )

    return create_response({"data": processed_items}, callback)


@router.get("/search")
@cached_endpoint(ttl=SEARCH_CACHE_TTL)
async def search(
    request: Request,
    q: Optional[str] = Query(None, description="Search query"),
    page: int = Query(1, description="Page number"),
    per_page: int = Query(10, description="Items per page"),
    sort: Optional[str] = Query(None, description="Sort option (relevance, year_desc, year_asc, title_asc, title_desc)"),
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    """Search items."""
    try:
        search_service = SearchService()
        results = await search_service.search(
            q=q,
            page=page,
            limit=per_page,
            sort=sort,
            request_query_params=str(request.query_params),
            callback=callback,
        )

        # Sanitize the results for JSON serialization
        results = sanitize_for_json(results)

        # Create the response
        response = create_response(results, callback)

        # Return the response
        return response
    except Exception as e:
        logger.error(f"Error performing search: {str(e)}", exc_info=True)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.get("/suggest")
@cached_endpoint(ttl=SUGGEST_CACHE_TTL)
async def suggest(
    q: str = Query(..., description="Search query for suggestions"),
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    """Get search suggestions."""
    try:
        search_service = SearchService()
        suggestions = await search_service.suggest(q)
        return create_response(suggestions, callback)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


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
