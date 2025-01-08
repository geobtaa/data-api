from fastapi import APIRouter, HTTPException, Query, Request
from db.database import database
from db.models import geoblacklight_development
from sqlalchemy import select, func
from ...viewers import ItemViewer
import json
from ...elasticsearch import index_documents, search_documents
from typing import Optional, Dict, List

router = APIRouter()

@router.get("/documents/{document_id}")
async def read_item(document_id: str):
    query = geoblacklight_development.select().where(
        geoblacklight_development.c.id == document_id
    )
    document = await database.fetch_one(query)

    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    # Parse references and create viewer
    try:
        references = json.loads(document["dct_references_s"]) if document["dct_references_s"] else {}
        # Add locn_geometry to references if it exists
        if document["locn_geometry"]:
            references["locn_geometry"] = document["locn_geometry"]
        
        viewer = ItemViewer(references)
        
        # Get viewer attributes
        ui_viewer_protocol = viewer.viewer_protocol()
        ui_viewer_endpoint = viewer.viewer_endpoint()
        ui_viewer_geometry = viewer.viewer_geometry()
    except json.JSONDecodeError:
        ui_viewer_protocol = None
        ui_viewer_endpoint = ""
        ui_viewer_geometry = None

    # Transform the document into JSON:API format
    json_api_response = {
        "data": {
            "type": "document",
            "id": str(document["id"]),
            "attributes": {
                **{
                    key: (json.loads(value) if key == "dct_references_s" else value)
                    for key, value in dict(document).items()
                    if key != "id"
                },
                "ui_viewer_protocol": ui_viewer_protocol,
                "ui_viewer_endpoint": ui_viewer_endpoint,
                "ui_viewer_geometry": ui_viewer_geometry
            },
        }
    }

    return json_api_response


async def get_total_count(q: str = None):
    total_count_query = select(func.count()).select_from(geoblacklight_development)
    if q:
        total_count_query = total_count_query.where(
            geoblacklight_development.c.dct_title_s.ilike(f"%{q}%")
        )
    return await database.fetch_val(total_count_query)

async def get_paginated_documents(skip: int, limit: int, q: str = None):
    query = geoblacklight_development.select().offset(skip).limit(limit)
    if q:
        query = query.where(geoblacklight_development.c.dct_title_s.ilike(f"%{q}%"))
    return await database.fetch_all(query)

def calculate_pagination_details(skip: int, limit: int, total_count: int):
    current_page = (skip // limit) + 1
    total_pages = (total_count // limit) + (1 if total_count % limit > 0 else 0)
    next_page = current_page + 1 if current_page < total_pages else None
    prev_page = current_page - 1 if current_page > 1 else None
    return current_page, total_pages, next_page, prev_page

@router.get("/documents/")
async def read_documents(skip: int = 0, limit: int = 20, q: str = None):
    # Check if the requested limit exceeds the maximum allowed
    if limit > 100:
        raise HTTPException(
            status_code=400,
            detail="The maximum number of rows that can be requested is 100."
        )

    # Fetch the total count of documents
    total_count = await get_total_count(q)

    # Fetch the paginated documents
    documents = await get_paginated_documents(skip, limit, q)

    # Calculate pagination details
    current_page, total_pages, next_page, prev_page = calculate_pagination_details(skip, limit, total_count)

    # Base URL for constructing links
    base_url = "http://localhost:8000/api/v1/documents"

    # Construct links for pagination
    links = {
        "self": f"{base_url}?skip={skip}&limit={limit}",
        "next": f"{base_url}?skip={skip + limit}&limit={limit}" if next_page else None,
        "last": (
            f"{base_url}?skip={(total_pages - 1) * limit}&limit={limit}"
            if total_pages > 1
            else None
        ),
    }

    # Transform the documents into JSON:API format with viewer attributes
    json_api_response = {
        "data": [
            {
                "type": "document",
                "id": str(document["id"]),
                "attributes": {
                    **{
                        key: (json.loads(value) if key == "dct_references_s" else value)
                        for key, value in dict(document).items()
                        if key != "id"
                    },
                    "ui_viewer_protocol": viewer.viewer_protocol(),
                    "ui_viewer_endpoint": viewer.viewer_endpoint(),
                    "ui_viewer_geometry": viewer.viewer_geometry(),
                },
            }
            for document in documents
            for viewer in [ItemViewer({
                **(json.loads(document["dct_references_s"]) if document["dct_references_s"] else {}),
                "locn_geometry": document["locn_geometry"] if document["locn_geometry"] else None
            })]
        ],
        "meta": {
            "pages": {
                "current_page": current_page,
                "next_page": next_page,
                "prev_page": prev_page,
                "total_pages": total_pages,
                "limit_value": limit,
                "offset_value": skip,
                "total_count": total_count,
                "first_page?": current_page == 1,
                "last_page?": current_page == total_pages,
            }
        },
        "links": links,
    }

    return json_api_response

@router.post("/index")
async def index_to_elasticsearch():
    """Index all documents from PostgreSQL to Elasticsearch."""
    result = await index_documents()
    return result

@router.get("/search")
async def search(
    request: Request,
    q: Optional[str] = Query(None, description="Search query string"),
    page: int = Query(1, ge=1, description="Page number for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Number of records to return")
):
    """
    Search documents using Elasticsearch.
    
    Parameters:
    - q: Search query string
    - fq[field][]: Filter queries as arrays, e.g. fq[resource_class_agg][]=Map
    - page: Page number for pagination
    - limit: Number of records to return (page size)
    
    Example:
    /search?q=water&fq[resource_class_agg][]=Map&fq[provider_agg][]=Minnesota&page=2
    """
    try:
        # Calculate skip based on page and limit
        skip = (page - 1) * limit
        
        # Get all query parameters
        params = dict(request.query_params)
        
        # Extract filter queries from params
        filter_query = {}
        
        # Map aggregation names to actual field names
        agg_to_field = {
            'spatial_agg': 'dct_spatial_sm',
            'resource_class_agg': 'gbl_resourceclass_sm',
            'resource_type_agg': 'gbl_resourcetype_sm',
            'index_year_agg': 'gbl_indexyear_im',
            'language_agg': 'dct_language_sm',
            'creator_agg': 'dct_creator_sm',
            'provider_agg': 'schema_provider_s',
            'access_rights_agg': 'dct_accessrights_sm',
            'georeferenced_agg': 'gbl_georeferenced_b'
        }
        
        # Process filter parameters
        for key in params:
            if key.startswith('fq[') and key.endswith('][]'):
                # Extract the field name from fq[field][]
                field = key[3:-3]  # Remove 'fq[' and '[]'
                if field in agg_to_field:
                    # Get the actual field name and values
                    es_field = agg_to_field[field]
                    values = request.query_params.getlist(key)
                    if values:
                        filter_query[es_field] = values

        results = await search_documents(
            query=q,
            fq=filter_query,
            skip=skip,
            limit=limit
        )
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
