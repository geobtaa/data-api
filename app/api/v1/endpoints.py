from fastapi import APIRouter, HTTPException
from db.database import database
from db.models import geoportal_development
from sqlalchemy import select, func
import json

router = APIRouter()


@router.get("/documents/{document_id}")
async def read_item(document_id: str):
    query = geoportal_development.select().where(
        geoportal_development.c.id == document_id
    )
    return await database.fetch_one(query)


# @TODO: Implement search across all text fields; Is there a Solr equivalent to copyfields?
@router.get("/documents/")
async def read_documents(skip: int = 0, limit: int = 20, q: str = None):
    # Check if the requested limit exceeds the maximum allowed
    if limit > 100:
        raise HTTPException(
            status_code=400,
            detail="The maximum number of rows that can be requested is 100."
        )

    # Fetch the total count of documents
    total_count_query = select(func.count()).select_from(geoportal_development)
    if q:
        total_count_query = total_count_query.where(
            geoportal_development.c.dct_title_s.ilike(f"%{q}%")
        )
    total_count = await database.fetch_val(total_count_query)

    # Fetch the paginated documents
    query = geoportal_development.select().offset(skip).limit(limit)
    if q:
        query = query.where(geoportal_development.c.dct_title_s.ilike(f"%{q}%"))
    documents = await database.fetch_all(query)

    # Calculate pagination details
    current_page = (skip // limit) + 1
    total_pages = (total_count // limit) + (1 if total_count % limit > 0 else 0)
    next_page = current_page + 1 if current_page < total_pages else None
    prev_page = current_page - 1 if current_page > 1 else None

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

    # Transform the documents into JSON:API format
    json_api_response = {
        "data": [
            {
                "type": "document",
                "id": str(document["id"]),
                "attributes": {
                    key: (json.loads(value) if key == "dct_references_s" else value)
                    for key, value in dict(document).items()
                    if key != "id"
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
