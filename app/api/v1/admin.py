import os
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy import select

from app.api.v1.auth import verify_credentials
from app.api.v1.utils import create_response, sanitize_for_json
from app.elasticsearch.index import reindex_items
from app.services.cache_service import CacheService, invalidate_cache_with_prefix
from app.services.image_service import ImageService
from app.tasks.entities import generate_geo_entities
from app.tasks.ocr import generate_item_ocr
from app.tasks.summarization import generate_item_summary
from db.database import database
from db.models import items

# Load environment variables
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "changeme")

security = HTTPBasic()
router = APIRouter()


@router.post("/cache/clear")
async def clear_cache(
    cache_type: Optional[str] = Query(
        None, description="Type of cache to clear (search, item, suggest, all)"
    ),
    credentials: HTTPBasicCredentials = None,
):
    """Clear specified cache or all cache if not specified."""
    if credentials is None:
        credentials = Depends(verify_credentials)

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
    credentials: HTTPBasicCredentials = None,
):
    """Trigger reindexing of all items in Elasticsearch."""
    if credentials is None:
        credentials = Depends(verify_credentials)

    try:
        result = await reindex_items()
        return create_response(
            {"status": "success", "message": "Reindexing completed", "details": result}, callback
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, detail={"message": "Reindexing failed", "error": str(e)}
        ) from e


@router.post("/items/{id}/summarize")
async def summarize_item(
    id: str,
    callback: Optional[str] = Query(None, description="JSONP callback name"),
    credentials: HTTPBasicCredentials = None,
):
    """Trigger the generation of a summary and OCR text for an item."""
    if credentials is None:
        credentials = Depends(verify_credentials)

    try:
        # Fetch the item
        async with database.transaction():
            query = select(items).where(items.c.id == id)
            result = await database.fetch_one(query)

            if not result:
                raise HTTPException(status_code=404, detail="Item not found")

            # Convert to dict and handle datetime serialization
            item = dict(result)
            item = sanitize_for_json(item)

            # Trigger the summarization task
            summary_task = generate_item_summary.delay(item_id=id, metadata=item)

            # Trigger the OCR task
            ocr_task = generate_item_ocr.delay(item_id=id, metadata=item)

            response_data = {
                "status": "success",
                "message": "Summary and OCR generation started",
                "tasks": {"summary": summary_task.id, "ocr": ocr_task.id},
            }

            return create_response(response_data, callback)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/items/{id}/identify-geo-entities")
async def identify_geo_entities(
    id: str,
    callback: Optional[str] = Query(None, description="JSONP callback name"),
    credentials: HTTPBasicCredentials = None,
):
    """Trigger the identification of geographic entities in an item."""
    if credentials is None:
        credentials = Depends(verify_credentials)

    try:
        # Fetch the item
        async with database.transaction():
            query = select(items).where(items.c.id == id)
            result = await database.fetch_one(query)

            if not result:
                raise HTTPException(status_code=404, detail="Item not found")

            # Convert to dict and handle datetime serialization
            item = dict(result)
            item = sanitize_for_json(item)

            # Trigger the geographic entity identification task
            geo_entities_task = generate_geo_entities.delay(item_id=id, metadata=item)

            response_data = {
                "status": "success",
                "message": "Geographic entity identification started",
                "task_id": geo_entities_task.id,
            }

            return create_response(response_data, callback)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/thumbnails/{image_hash}")
async def get_thumbnail(
    image_hash: str,
    credentials: HTTPBasicCredentials = None,
):
    """Serve a cached thumbnail image."""
    if credentials is None:
        credentials = Depends(verify_credentials)

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


@router.get("/items/{id}/summaries")
async def get_item_summaries(
    id: str,
    callback: Optional[str] = Query(None, description="JSONP callback name"),
    credentials: HTTPBasicCredentials = None,
):
    """Get all summaries for an item."""
    if credentials is None:
        credentials = Depends(verify_credentials)

    try:
        # Query the database for summaries
        async with database.transaction():
            query = """
                SELECT * FROM ai_enrichments 
                WHERE item_id = :item_id 
                ORDER BY created_at DESC
            """
            summaries = await database.fetch_all(query, {"item_id": id})

            # Convert to list of dicts and sanitize
            summaries_list = [sanitize_for_json(dict(summary)) for summary in summaries]

            # Create response
            response_data = {
                "data": {"type": "summaries", "id": id, "attributes": {"summaries": summaries_list}}
            }

            return create_response(response_data, callback)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
