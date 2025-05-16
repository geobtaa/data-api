import json
import logging
import os
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from fastapi.security import HTTPBasic
from sqlalchemy import select

from app.api.v1.auth import verify_credentials
from app.api.v1.utils import create_response, sanitize_for_json
from app.elasticsearch.index import reindex_items
from app.services.cache_service import ENDPOINT_CACHE, CacheService, invalidate_cache_with_prefix
from app.tasks.entities import generate_geo_entities
from app.tasks.summarization import generate_item_summary
from db.database import database
from db.models import items

logger = logging.getLogger(__name__)

# Load environment variables
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "changeme")

security = HTTPBasic()
router = APIRouter(dependencies=[Depends(verify_credentials)])


@router.post("/cache/clear")
async def clear_cache(
    cache_type: Optional[str] = Query(
        None, description="Type of cache to clear (search, item, suggest, all)"
    ),
):
    """Clear specified cache or all cache if not specified."""
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

        return create_response({"message": f"Cache cleared successfully: {cache_type or 'all'}"})
    except Exception as e:
        return create_response({"error": f"Failed to clear cache: {str(e)}"}, status_code=500)


@router.post("/reindex")
async def reindex(
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
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


@router.post("/items/{id}/summarize")
async def summarize_item(
    id: str,
    background_tasks: BackgroundTasks,
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    """
    Trigger the generation of a summary for an item.
    This endpoint will:
    1. Fetch the item metadata
    2. Get the asset path and type
    3. Trigger an asynchronous task to generate the summary
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

            # Invalidate the item cache since we'll be updating it
            invalidate_cache_with_prefix(f"item:{id}")

            # Create response data and ensure all datetime objects are serialized
            response_data = {
                "status": "success",
                "message": "Summary generation started",
                "task_id": summary_task.id,
            }

            # Sanitize the response data before returning
            sanitized_response = sanitize_for_json(response_data)
            return create_response(sanitized_response, callback)

    except Exception as e:
        logger.error(f"Error triggering summary generation for item {id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) from e


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
