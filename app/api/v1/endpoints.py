import logging
import os
from typing import Optional

from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from app.api.v1.utils import (
    add_thumbnail_url,
    create_response,
    sanitize_for_json,
)
from app.services.cache_service import (
    cached_endpoint,
)
from app.services.download_service import DownloadService
from app.services.image_service import ImageService
from app.services.search_service import SearchService
from app.services.viewer_service import ViewerService
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
    except HTTPException:
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
    sort: Optional[str] = Query(
        None, description="Sort option (relevance, year_desc, year_asc, title_asc, title_desc)"
    ),
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


@router.get("/items/{id}/summaries")
async def get_item_summaries(
    id: str,
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    """Get all summaries for an item."""
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
