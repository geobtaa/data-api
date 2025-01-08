from fastapi import APIRouter, HTTPException, Query, Request
from db.database import database
from db.models import geoblacklight_development
from sqlalchemy import select, func
from ...viewers import ItemViewer
import json
from ...elasticsearch import index_documents, search_documents
from typing import Optional, Dict, List

router = APIRouter()

def parse_references(document: Dict) -> Dict:
    """Parse references from the document."""
    try:
        references = json.loads(document["dct_references_s"]) if document["dct_references_s"] else {}
        if document["locn_geometry"]:
            references["locn_geometry"] = document["locn_geometry"]
        return references
    except json.JSONDecodeError:
        return {}

def create_viewer_attributes(document: Dict) -> Dict:
    """Create viewer attributes from the document."""
    references = parse_references(document)
    viewer = ItemViewer(references)
    return {
        "ui_viewer_protocol": viewer.viewer_protocol(),
        "ui_viewer_endpoint": viewer.viewer_endpoint(),
        "ui_viewer_geometry": viewer.viewer_geometry()
    }

@router.get("/documents/{document_id}")
async def read_item(document_id: str):
    query = geoblacklight_development.select().where(
        geoblacklight_development.c.id == document_id
    )
    document = await database.fetch_one(query)

    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    viewer_attributes = create_viewer_attributes(document)

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
                **viewer_attributes
            },
        }
    }

    return json_api_response

async def get_total_count(q: Optional[str] = None) -> int:
    total_count_query = select(func.count()).select_from(geoblacklight_development)
    if q:
        total_count_query = total_count_query.where(
            geoblacklight_development.c.dct_title_s.ilike(f"%{q}%")
        )
    return await database.fetch_val(total_count_query)

async def get_paginated_documents(skip: int, limit: int, q: Optional[str] = None) -> List[Dict]:
    query = geoblacklight_development.select().offset(skip).limit(limit)
    if q:
        query = query.where(geoblacklight_development.c.dct_title_s.ilike(f"%{q}%"))
    return await database.fetch_all(query)

def calculate_pagination_details(skip: int, limit: int, total_count: int) -> Dict:
    current_page = (skip // limit) + 1
    total_pages = (total_count // limit) + (1 if total_count % limit > 0 else 0)
    return {
        "current_page": current_page,
        "total_pages": total_pages,
        "next_page": current_page + 1 if current_page < total_pages else None,
        "prev_page": current_page - 1 if current_page > 1 else None
    }

@router.get("/documents/")
async def read_documents(skip: int = 0, limit: int = 20, q: Optional[str] = None):
    if limit > 100:
        raise HTTPException(
            status_code=400,
            detail="The maximum number of rows that can be requested is 100."
        )

    total_count = await get_total_count(q)
    documents = await get_paginated_documents(skip, limit, q)
    pagination_details = calculate_pagination_details(skip, limit, total_count)

    base_url = "http://localhost:8000/api/v1/documents"
    links = {
        "self": f"{base_url}?skip={skip}&limit={limit}",
        "next": f"{base_url}?skip={skip + limit}&limit={limit}" if pagination_details["next_page"] else None,
        "last": (
            f"{base_url}?skip={(pagination_details['total_pages'] - 1) * limit}&limit={limit}"
            if pagination_details["total_pages"] > 1
            else None
        ),
    }

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
                    **create_viewer_attributes(document)
                },
            }
            for document in documents
        ],
        "meta": {
            "pages": {
                **pagination_details,
                "limit_value": limit,
                "offset_value": skip,
                "total_count": total_count,
                "first_page?": pagination_details["current_page"] == 1,
                "last_page?": pagination_details["current_page"] == pagination_details["total_pages"],
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
    try:
        skip = (page - 1) * limit
        params = dict(request.query_params)
        filter_query = extract_filter_queries(params)

        results = await search_documents(
            query=q,
            fq=filter_query,
            skip=skip,
            limit=limit
        )
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def extract_filter_queries(params: Dict) -> Dict:
    """Extract filter queries from request parameters."""
    filter_query = {}
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
    for key in params:
        if key.startswith('fq[') and key.endswith('][]'):
            field = key[3:-3]
            if field in agg_to_field:
                es_field = agg_to_field[field]
                values = params.getlist(key)
                if values:
                    filter_query[es_field] = values
    return filter_query
