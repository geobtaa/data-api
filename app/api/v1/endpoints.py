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
import logging
import time
from fastapi.responses import JSONResponse, Response
from .jsonp import JSONPResponse
from datetime import datetime
from app.services.download_service import DownloadService

router = APIRouter()

logger = logging.getLogger(__name__)

base_url = os.getenv("APPLICATION_URL", "http://localhost:8000/api/v1/")


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
            relationships[rel["predicate"]].append({
                "doc_id": rel["object_id"],
                "doc_title": rel["dct_title_s"],
                "link": f"{base_url}/documents/{rel['object_id']}"
            })
            logger.debug(f"Added relationship: {rel['predicate']} -> {rel['object_id']}")

        logger.info(f"Final relationships structure: {relationships}")
        return relationships

    except Exception as e:
        logger.error(f"Error getting relationships: {e}", exc_info=True)
        return {}


@router.get("/documents/{id}")
async def get_document(id: str, callback: Optional[str] = None, include_relationships: bool = True):
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
        
        # Create response
        response = {
                "data": {
                    "type": "document",
                    "id": id,
                    "attributes": processed_doc
                }
            }
        
        logger.info(f"Final response structure: {response}")  # Debug line
        return create_response(response, callback)
        
    except Exception as e:
        logger.error(f"Document fetch failed: {e}", exc_info=True)
        error_response = {"message": "Document fetch failed", "error": str(e)}
        return create_response(error_response, callback)


@router.get("/documents/")
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


@router.post("/index")
async def index_to_elasticsearch(
    callback: Optional[str] = Query(None, description="JSONP callback name")
):
    """Index all documents from PostgreSQL to Elasticsearch."""
    result = await index_documents()
    return create_response(result, callback)


@router.get("/search")
async def search(
    request: Request,
    q: Optional[str] = Query(None, description="Search query"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Results per page"),
    sort: SortOption = Query(SortOption.RELEVANCE, description="Sort order"),
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    """Search endpoint."""
    try:
        timings = {}
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
            "per_document": f"{((process_time / docs_processed) * 1000):.0f}ms" if docs_processed > 0 else "0ms",
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
async def suggest(
    q: str = Query(..., description="Query string for suggestions"),
    resource_class: Optional[str] = Query(None, description="Filter suggestions by resource class"),
    size: int = Query(5, ge=1, le=20, description="Number of suggestions to return"),
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    """Get autocomplete suggestions."""
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