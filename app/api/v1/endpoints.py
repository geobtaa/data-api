from fastapi import APIRouter, HTTPException, Query, Request, BackgroundTasks
from db.database import database
from db.models import geoblacklight_development
from sqlalchemy import select, func
import json
from ...elasticsearch import index_documents, search_documents
from ...services.viewer_service import create_viewer_attributes
from typing import Optional, Dict, List, Any, Union
from urllib.parse import parse_qs
from .shared import SortOption, SORT_MAPPINGS
import os
from ...elasticsearch.client import es
from ...services.image_service import ImageService
from ...services.citation_service import CitationService
from ...services.cache_service import (
    cached_endpoint,
    CacheService,
    ENDPOINT_CACHE,
    invalidate_cache_with_prefix,
)
from ...services.llm_service import LLMService
from ...tasks.summarization import generate_item_summary
import logging
import time
from fastapi.responses import JSONResponse, Response
from .jsonp import JSONPResponse
from datetime import datetime
from app.services.download_service import DownloadService
from app.elasticsearch.index import reindex_documents

router = APIRouter()

logger = logging.getLogger(__name__)

base_url = os.getenv("APPLICATION_URL", "http://localhost:8000/api/v1/")

# Cache TTL configuration in seconds
DOCUMENT_CACHE_TTL = int(os.getenv("DOCUMENT_CACHE_TTL", 86400))  # 24 hours
SEARCH_CACHE_TTL = int(os.getenv("SEARCH_CACHE_TTL", 3600))  # 1 hour
SUGGEST_CACHE_TTL = int(os.getenv("SUGGEST_CACHE_TTL", 7200))  # 2 hours
LIST_CACHE_TTL = int(os.getenv("LIST_CACHE_TTL", 43200))  # 12 hours


def add_thumbnail_url(document: Dict) -> Dict:
    """Add the ui_thumbnail_url to the document attributes."""
    # Ensure 'attributes' key exists
    if "attributes" not in document:
        document["attributes"] = {}

    image_service = ImageService(document)
    thumbnail_url = image_service.get_thumbnail_url()
    document["attributes"]["ui_thumbnail_url"] = thumbnail_url
    return document


def add_citations(document: Dict) -> Dict:
    """Add citations to a document."""
    # Ensure 'attributes' key exists
    if "attributes" not in document:
        document["attributes"] = {}

    try:
        citation_service = CitationService(document)
        document["attributes"]["ui_citation"] = citation_service.get_citation()
    except Exception as e:
        logger.error(f"Failed to generate citation: {str(e)}")
        document["attributes"]["ui_citation"] = "Citation unavailable"
    return document


def sanitize_for_json(obj: Any) -> Any:
    """Recursively convert datetime objects to ISO format strings."""
    if isinstance(obj, dict):
        return {key: sanitize_for_json(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(item) for item in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def create_response(content: Dict, callback: Optional[str] = None) -> JSONResponse:
    """Create either a JSON or JSONP response based on callback parameter."""
    # Sanitize content before serialization
    sanitized_content = sanitize_for_json(content)
    if callback:
        return JSONPResponse(content=sanitized_content, callback=callback)
    return JSONResponse(content=sanitized_content)


def add_ui_attributes(doc: Dict) -> Dict:
    """Add UI attributes to a document."""
    # Parse references if needed
    if isinstance(doc.get("dct_references_s"), str):
        try:
            doc["dct_references_s"] = json.loads(doc["dct_references_s"])
        except json.JSONDecodeError:
            doc["dct_references_s"] = {}

    # Create services
    image_service = ImageService(doc)
    citation_service = CitationService(doc)
    download_service = DownloadService(doc)

    # Add viewer attributes
    doc.update(create_viewer_attributes(doc))

    # Add thumbnail URL if available
    if thumbnail_url := image_service.get_thumbnail_url():
        doc["ui_thumbnail_url"] = thumbnail_url

    # Add citation
    doc["ui_citation"] = citation_service.get_citation()

    # Add download options
    doc["ui_downloads"] = download_service.get_download_options()

    return doc


async def get_document_relationships(doc_id: str) -> Dict:
    """Get all relationships for a document."""
    try:
        logger.info(f"Fetching relationships for document: {doc_id}")

        # Get outgoing relationships (where doc is subject)
        relationships_query = """
            SELECT predicate, object_id, dct_title_s
            FROM document_relationships
            JOIN geoblacklight_development ON geoblacklight_development.id = document_relationships.object_id
            WHERE subject_id = :doc_id
            ORDER BY dct_title_s ASC
        """
        db_relationships = await database.fetch_all(relationships_query, {"doc_id": doc_id})
        logger.info(f"Found {len(db_relationships)} relationships")
        logger.info(f"Relationships: {db_relationships}")

        relationships = {}

        # Process outgoing relationships
        for rel in db_relationships:
            if rel["predicate"] not in relationships:
                relationships[rel["predicate"]] = []
            relationships[rel["predicate"]].append(
                {
                    "doc_id": rel["object_id"],
                    "doc_title": rel["dct_title_s"],
                    "link": f"{base_url}/documents/{rel['object_id']}",
                }
            )
            logger.debug(f"Added relationship: {rel['predicate']} -> {rel['object_id']}")

        logger.info(f"Final relationships structure: {relationships}")
        return relationships

    except Exception as e:
        logger.error(f"Error getting relationships: {e}", exc_info=True)
        return {}


@router.get("/documents/{id}")
@cached_endpoint(ttl=DOCUMENT_CACHE_TTL)
async def get_document(
    id: str,
    callback: Optional[str] = None,
    include_relationships: bool = True,
    include_summaries: bool = True,
):
    """Get a single document by ID."""
    try:
        # Get document
        query = geoblacklight_development.select().where(geoblacklight_development.c.id == id)
        result = await database.fetch_one(query)

        if not result:
            raise HTTPException(status_code=404, detail="Document not found")

        # Convert to dict and process
        doc = dict(result)

        # Add UI attributes
        processed_doc = add_ui_attributes(doc)

        # Get relationships
        if include_relationships:
            relationships = await get_document_relationships(id)
            logger.info(f"Got relationships for {id}: {relationships}")  # Debug line
        else:
            relationships = {}

        # Add relationships to UI attributes
        processed_doc["ui_relationships"] = relationships

        # Get summaries if requested
        summaries = []
        if include_summaries:
            try:
                summaries_query = """
                    SELECT * FROM ai_enrichments 
                    WHERE document_id = :document_id 
                    ORDER BY created_at DESC
                """
                summaries_result = await database.fetch_all(summaries_query, {"document_id": id})
                summaries = [dict(summary) for summary in summaries_result]
                logger.info(f"Got {len(summaries)} summaries for {id}")
            except Exception as e:
                logger.error(f"Error fetching summaries: {str(e)}")
                summaries = []

        # Add summaries to UI attributes
        processed_doc["ui_summaries"] = summaries

        # Create response
        response = {"data": {"type": "document", "id": id, "attributes": processed_doc}}

        logger.info(f"Final response structure: {response}")  # Debug line
        return create_response(response, callback)

    except Exception as e:
        logger.error(f"Document fetch failed: {e}", exc_info=True)
        error_response = {"message": "Document fetch failed", "error": str(e)}
        return create_response(error_response, callback)


@router.get("/documents/")
@cached_endpoint(ttl=LIST_CACHE_TTL)
async def list_documents(
    skip: int = 0,
    limit: int = 10,
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    query = geoblacklight_development.select().offset(skip).limit(limit)
    documents = await database.fetch_all(query)

    processed_documents = []
    for document in documents:
        doc_dict = dict(document)
        doc_dict = add_thumbnail_url(doc_dict)
        doc_dict = add_citations(doc_dict)
        viewer_attributes = create_viewer_attributes(doc_dict)

        # Use DownloadService to get download options
        download_service = DownloadService(doc_dict)
        ui_downloads = download_service.get_download_options()

        processed_documents.append(
            {
                "type": "document",
                "id": str(doc_dict["id"]),
                "attributes": {
                    **doc_dict,
                    **viewer_attributes,
                    "ui_citation": doc_dict.get("ui_citation"),
                    "ui_thumbnail_url": doc_dict.get("ui_thumbnail_url"),
                    "ui_viewer_endpoint": viewer_attributes.get("ui_viewer_endpoint"),
                    "ui_viewer_geometry": viewer_attributes.get("ui_viewer_geometry"),
                    "ui_viewer_protocol": viewer_attributes.get("ui_viewer_protocol"),
                    "ui_downloads": ui_downloads,  # Include ui_downloads in the response
                },
            }
        )

    return create_response({"data": processed_documents}, callback)


@router.get("/search")
@cached_endpoint(ttl=SEARCH_CACHE_TTL)
async def search(
    request: Request,
    q: Optional[str] = Query(None, description="Search query"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Results per page"),
    sort: SortOption = Query(SortOption.RELEVANCE, description="Sort order"),
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    """Search endpoint with caching support."""
    try:
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
        results = await search_documents(
            query=q,
            fq=filter_query,
            skip=skip,
            limit=limit,
            sort=sort_mapping,
        )
        es_time = (time.time() - es_start) * 1000
        timings["elasticsearch"] = f"{es_time:.0f}ms"

        # Process each document
        process_start = time.time()
        docs_processed = 0
        citation_time = 0
        thumbnail_time = 0
        viewer_time = 0

        for document in results["data"]:
            doc_start = time.time()

            # Add thumbnail URL
            thumb_start = time.time()
            image_service = ImageService(document["attributes"])
            document["attributes"]["ui_thumbnail_url"] = image_service.get_thumbnail_url()
            thumbnail_time += time.time() - thumb_start

            # Add citation
            cite_start = time.time()
            citation_service = CitationService(document["attributes"])
            document["attributes"]["ui_citation"] = citation_service.get_citation()
            citation_time += time.time() - cite_start

            # Add viewer attributes
            viewer_start = time.time()
            viewer_attrs = create_viewer_attributes(document["attributes"])
            document["attributes"].update(viewer_attrs)
            viewer_time += time.time() - viewer_start

            docs_processed += 1

        process_time = time.time() - process_start
        timings["document_processing"] = {
            "total": f"{(process_time * 1000):.0f}ms",
            "per_document": (
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
        logger.error(f"Search endpoint error", exc_info=True)
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

        index_name = os.getenv("ELASTICSEARCH_INDEX", "geoblacklight")

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
        None, description="Type of cache to clear (search, document, suggest, all)"
    )
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

        if cache_type == "document" or cache_type is None:
            await invalidate_cache_with_prefix("app.api.v1.endpoints:get_document")

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
        # Create service without document (we only need cache access)
        image_service = ImageService({})
        image_data = await image_service.get_cached_image(image_hash)

        if image_data:
            return Response(
                content=image_data,
                media_type="image/jpeg",
                headers={"Cache-Control": "public, max-age=31536000"},  # Cache for 1 year
            )

        raise HTTPException(status_code=404, detail="Image not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/documents/{id}/summarize")
async def summarize_document(
    id: str,
    background_tasks: BackgroundTasks,
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    """
    Trigger the generation of a summary and OCR text for a document.
    This endpoint will:
    1. Fetch the document metadata
    2. Get the asset path and type
    3. Trigger asynchronous tasks to generate the summary and OCR text
    4. Return immediately with task IDs
    """
    try:
        # Fetch the document
        async with database.transaction():
            query = select(geoblacklight_development).where(geoblacklight_development.c.id == id)
            result = await database.fetch_one(query)

            if not result:
                raise HTTPException(status_code=404, detail="Document not found")

            # Convert to dict
            document = dict(result)
            logger.info(f"Processing document {id}")
            logger.debug(f"Raw document data: {json.dumps(document, indent=2)}")

            # Get asset information
            asset_path = None
            asset_type = None
            
            # Parse dct_references_s to identify candidate assets
            references = document.get("dct_references_s", {})
            logger.info(f"Raw references for document {id}: {references}")
            
            if isinstance(references, str):
                try:
                    references = json.loads(references)
                    logger.info(f"Parsed references for document {id}: {json.dumps(references, indent=2)}")
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse references JSON for document {id}: {references}")
                    references = {}
            
            # Define asset type mappings
            asset_type_mappings = {
                "http://schema.org/downloadUrl": "download",
                "http://iiif.io/api/image": "iiif_image",
                "http://iiif.io/api/presentation#manifest": "iiif_manifest",
                "https://github.com/cogeotiff/cog-spec": "cog",
                "https://github.com/protomaps/PMTiles": "pmtiles"
            }
            
            # Check for each reference type
            for ref_type, asset_type_name in asset_type_mappings.items():
                if ref_type in references:
                    ref_value = references[ref_type]
                    logger.info(f"Found reference type {ref_type} with value {ref_value} for document {id}")
                    
                    # Handle both string and array values
                    if isinstance(ref_value, list) and ref_value:
                        # For arrays, take the first item for now
                        asset_path = ref_value[0]
                        asset_type = asset_type_name
                        logger.info(f"Using first item from array: asset_path={asset_path}, asset_type={asset_type}")
                        break
                    elif isinstance(ref_value, str) and ref_value:
                        asset_path = ref_value
                        asset_type = asset_type_name
                        logger.info(f"Using string value: asset_path={asset_path}, asset_type={asset_type}")
                        break
            
            # If no specific asset type was found, use the document format as fallback
            if not asset_type:
                asset_type = document.get("dc_format_s")
                logger.info(f"No specific asset type found, using format fallback: {asset_type}")

            logger.info(f"Final asset determination for document {id}: path={asset_path}, type={asset_type}")

            # Trigger the summarization task
            summary_task = generate_item_summary.delay(
                item_id=id, metadata=document, asset_path=asset_path, asset_type=asset_type
            )
            logger.info(f"Started summary task {summary_task.id} for document {id}")

            # If we have an asset, also trigger OCR
            ocr_task = None
            if asset_path and asset_type:
                from app.tasks.ocr import generate_item_ocr
                ocr_task = generate_item_ocr.delay(
                    item_id=id, metadata=document, asset_path=asset_path, asset_type=asset_type
                )
                logger.info(f"Started OCR task {ocr_task.id} for document {id}")
            else:
                logger.warning(f"No asset found for OCR processing on document {id}")
                logger.debug(f"Missing: asset_path={asset_path}, asset_type={asset_type}")

            # Invalidate the document cache since we'll be updating it
            invalidate_cache_with_prefix(f"document:{id}")

            response_data = {
                "status": "success",
                "message": "Summary and OCR generation started",
                "tasks": {
                    "summary": summary_task.id,
                    "ocr": ocr_task.id if ocr_task else None
                }
            }

            return create_response(response_data, callback)

    except Exception as e:
        logger.error(f"Error triggering summary and OCR generation for document {id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/documents/{id}/summaries")
async def get_document_summaries(
    id: str, callback: Optional[str] = Query(None, description="JSONP callback name")
):
    """
    Get all summaries for a document.

    Args:
        id: The document ID
        callback: Optional JSONP callback name

    Returns:
        JSON response with the summaries
    """
    try:
        # Query the database for summaries
        async with database.transaction():
            query = """
                SELECT * FROM ai_enrichments 
                WHERE document_id = :document_id 
                ORDER BY created_at DESC
            """
            summaries = await database.fetch_all(query, {"document_id": id})

            # Convert to list of dicts
            summaries_list = [dict(summary) for summary in summaries]

            # Create response
            response_data = {
                "data": {"type": "summaries", "id": id, "attributes": {"summaries": summaries_list}}
            }

            return create_response(response_data, callback)

    except Exception as e:
        logger.error(f"Error retrieving summaries for document {id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reindex", response_model=dict)
async def reindex(callback: Optional[str] = Query(None, description="JSONP callback name")):
    """Trigger reindexing of all documents in Elasticsearch."""
    try:
        # When reindexing, invalidate all search and suggest caches
        if ENDPOINT_CACHE:
            logger.info("Invalidating search and suggest caches")
            await invalidate_cache_with_prefix("app.api.v1.endpoints:search")
            await invalidate_cache_with_prefix("app.api.v1.endpoints:suggest")

        result = await reindex_documents()
        return create_response(
            {"status": "success", "message": "Reindexing completed", "details": result}, callback
        )
    except Exception as e:
        logger.error(f"Reindexing failed: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail={"message": "Reindexing failed", "error": str(e)}
        )
