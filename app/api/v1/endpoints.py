from fastapi import APIRouter, HTTPException
from db.database import database
from db.models import geoblacklight_development
from sqlalchemy import select, func
from ...viewers import ItemViewer
import json

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
        viewer = ItemViewer(references)
        
        # Get viewer attributes
        ui_viewer_protocol = viewer.viewer_protocol()
        ui_viewer_endpoint = viewer.viewer_endpoint()
    except json.JSONDecodeError:
        ui_viewer_protocol = None
        ui_viewer_endpoint = ""

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
                "ui_viewer_endpoint": ui_viewer_endpoint
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
                    "ui_viewer_protocol": ItemViewer(
                        json.loads(document["dct_references_s"]) if document["dct_references_s"] else {}
                    ).viewer_protocol(),
                    "ui_viewer_endpoint": ItemViewer(
                        json.loads(document["dct_references_s"]) if document["dct_references_s"] else {}
                    ).viewer_endpoint(),
                },
            }
            for document in documents
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
