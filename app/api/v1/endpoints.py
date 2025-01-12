from fastapi import APIRouter, HTTPException, Query, Request
from db.database import database
from db.models import geoblacklight_development
from sqlalchemy import select, func
import json
from ...elasticsearch import index_documents, search_documents
from ...services.viewer_service import create_viewer_attributes
from typing import Optional, Dict, List
from urllib.parse import parse_qs
from .shared import SortOption, SORT_MAPPINGS
import os
from ...elasticsearch.client import es

router = APIRouter()

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
    limit: int = Query(10, ge=1, le=100, description="Number of records to return"),
    sort: SortOption = Query(SortOption.RELEVANCE, description="Sort option")
):
    try:
        skip = (page - 1) * limit
        query_string = str(request.query_params)
        params = parse_qs(query_string)
        filter_query = extract_filter_queries(query_string)

        results = await search_documents(
            query=q,
            fq=filter_query,
            skip=skip,
            limit=limit,
            sort=SORT_MAPPINGS[sort]
        )
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def extract_filter_queries(params: Dict) -> Dict:
    """Extract filter queries from request parameters."""
    filter_query = {}
    # Parse the raw query string to handle multiple values
    raw_params = parse_qs(str(params))
    
    agg_to_field = {
        'id_agg': 'id',
        'spatial_agg': 'dct_spatial_sm',
        'resource_type_agg': 'gbl_resourcetype_sm',
        'resource_class_agg': 'gbl_resourceclass_sm',
        'index_year_agg': 'gbl_indexyear_im',
        'language_agg': 'dct_language_sm',
        'creator_agg': 'dct_creator_sm',
        'provider_agg': 'schema_provider_s',
        'access_rights_agg': 'dct_accessrights_sm',
        'georeferenced_agg': 'gbl_georeferenced_b'
    }

    for key, values in raw_params.items():
        if key.startswith('fq[') and key.endswith('][]'):
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
    size: int = Query(5, ge=1, le=20, description="Number of suggestions to return")
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
                "dct_spatial_sm"
            ],
            "suggest": {
                "my-suggestion": {  # Changed name to be more explicit
                    "prefix": q,
                    "completion": {
                        "field": "suggest",
                        "size": size,
                        "skip_duplicates": True,
                        "fuzzy": {
                            "fuzziness": "AUTO"
                        }
                    }
                }
            }
        }

        index_name = os.getenv("ELASTICSEARCH_INDEX", "geoblacklight")
        
        # Print the query for debugging
        print("Suggest Query:", json.dumps(suggest_query, indent=2))
        
        response = await es.search(
            index=index_name,
            body=suggest_query
        )
        
        # Convert response to dict for serialization
        response_dict = response.body
        
        # Print the full response for debugging
        print("ES Response:", json.dumps(response_dict, indent=2))

        suggestions = []
        seen_ids = set()  # Track seen suggestion IDs
        if response_dict.get("suggest", {}).get("my-suggestion"):
            for suggestion in response_dict["suggest"]["my-suggestion"]:
                print(f"Processing suggestion: {suggestion}")  # Debug print
                if options := suggestion.get("options", []):
                    for option in options:
                        suggestion_id = option["_id"]
                        if suggestion_id not in seen_ids:  # Check for duplicates
                            seen_ids.add(suggestion_id)
                            suggestions.append({
                                "type": "suggestion",
                                "id": suggestion_id,
                                "attributes": {
                                    "text": option.get("text", ""),
                                    "title": option.get("_source", {}).get("dct_title_s", ""),
                                    "score": option.get("_score", 0)
                                }
                            })

        return {
            "data": suggestions,
            "meta": {
                "query": q,
                "resource_class": resource_class,
                "es_query": suggest_query,
                "es_response": response_dict
            }
        }

    except Exception as e:
        print(f"Suggestion error: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Suggestion error: {str(e)}\nQuery: {suggest_query}"
        )
