from fastapi import HTTPException
from .client import es
from db.database import database
from db.models import geoblacklight_development
from app.services.viewer_service import create_viewer_attributes  # Updated import
import os
import time
from sqlalchemy.sql import text
from urllib.parse import urlencode
from app.services.image_service import ImageService
import json
import logging

logger = logging.getLogger(__name__)


def get_search_criteria(query: str, fq: dict, skip: int, limit: int, sort: list = None):
    """Return the currently applied search criteria."""
    return {
        "query": query,
        "filters": fq,
        "pagination": {"skip": skip, "limit": limit},
        "sort": sort or [{"_score": "desc"}],
    }


async def search_documents(
    query: str = None, fq: dict = None, skip: int = 0, limit: int = 20, sort: list = None
):
    """Search documents in Elasticsearch with optional filters and sorting."""
    index_name = os.getenv("ELASTICSEARCH_INDEX", "geoblacklight")
    
    try:
        # Get the current search criteria
        search_criteria = get_search_criteria(query, fq, skip, limit, sort)
        logger.debug(f"Search criteria: {search_criteria}")

        # Construct the filter query
        filter_clauses = []
        if fq:
            for field, values in fq.items():
                logger.debug(f"Processing filter - Field: {field}, Values: {values}")
                if isinstance(values, list):
                    filter_clauses.append({"terms": {field: values}})
                else:
                    filter_clauses.append({"term": {field: values}})

        search_query = {
            "query": {
                "bool": {
                    "must": [{"match_all": {}}] if not query else [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": [
                                    "dct_title_s^3",
                                    "dct_description_sm^2",
                                    "dct_creator_sm",
                                    "dct_publisher_sm",
                                    "dct_subject_sm",
                                    "dcat_theme_sm",
                                    "dcat_keyword_sm",
                                    "dct_spatial_sm"
                                ],
                            }
                        }
                    ],
                    "filter": filter_clauses
                }
            },
            "from": skip,
            "size": limit,
            "sort": sort or [{"_score": "desc"}],
            "aggs": {
                "id_agg": {"terms": {"field": "id"}},
                "spatial_agg": {"terms": {"field": "dct_spatial_sm"}},
                "resource_class_agg": {"terms": {"field": "gbl_resourceclass_sm"}},
                "resource_type_agg": {"terms": {"field": "gbl_resourcetype_sm"}},
                "index_year_agg": {"terms": {"field": "gbl_indexyear_im"}},
                "language_agg": {"terms": {"field": "dct_language_sm"}},
                "creator_agg": {"terms": {"field": "dct_creator_sm"}},
                "provider_agg": {"terms": {"field": "schema_provider_s"}},
                "access_rights_agg": {"terms": {"field": "dct_accessrights_sm"}},
                "georeferenced_agg": {"terms": {"field": "gbl_georeferenced_b"}}
            }
        }

        logger.debug(f"ES Query: {json.dumps(search_query, indent=2)}")
        
        try:
            response = await es.search(
                index=index_name,
                body=search_query,
                track_total_hits=True
            )
        except Exception as es_error:
            logger.error(f"Elasticsearch error: {str(es_error)}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail={
                    "message": "Elasticsearch query failed",
                    "error": str(es_error),
                    "query": search_query,
                    "index": index_name
                }
            )
        
        logger.info(f"ES Response status: {response.meta.status}")
        
        return await process_search_response(response, limit, skip, search_criteria)

    except Exception as e:
        logger.error(f"Search documents error: {str(e)}", exc_info=True)
        raise


def get_sort_options(search_criteria):
    """Generate sort options for the response."""
    base_url = os.getenv("APPLICATION_URL") + "/api/v1/search"
    current_params = {"q": search_criteria["query"] or "", "search_field": "all_fields"}

    # Add any existing filters to the params
    if search_criteria["filters"]:
        for field, values in search_criteria["filters"].items():
            if isinstance(values, list):
                for value in values:
                    current_params[f"fq[{field}][]"] = value
            else:
                current_params[f"fq[{field}][]"] = values

    sort_options = [
        {
            "type": "sort",
            "id": "relevance",
            "attributes": {"label": "Relevance"},
            "links": {
                "self": f"{base_url}?{urlencode({**current_params, 'sort': 'relevance'}, doseq=True)}"
            },
        },
        {
            "type": "sort",
            "id": "year_desc",
            "attributes": {"label": "Year (Newest first)"},
            "links": {
                "self": f"{base_url}?{urlencode({**current_params, 'sort': 'year_desc'}, doseq=True)}"
            },
        },
        {
            "type": "sort",
            "id": "year_asc",
            "attributes": {"label": "Year (Oldest first)"},
            "links": {
                "self": f"{base_url}?{urlencode({**current_params, 'sort': 'year_asc'}, doseq=True)}"
            },
        },
        {
            "type": "sort",
            "id": "title_asc",
            "attributes": {"label": "Title (A-Z)"},
            "links": {
                "self": f"{base_url}?{urlencode({**current_params, 'sort': 'title_asc'}, doseq=True)}"
            },
        },
        {
            "type": "sort",
            "id": "title_desc",
            "attributes": {"label": "Title (Z-A)"},
            "links": {
                "self": f"{base_url}?{urlencode({**current_params, 'sort': 'title_desc'}, doseq=True)}"
            },
        },
    ]
    return sort_options


async def process_search_response(response, limit, skip, search_criteria):
    """Process Elasticsearch response and fetch documents from PostgreSQL."""
    try:
        total_hits = response.body["hits"]["total"]["value"]
        logger.debug(f"Total hits: {total_hits}")
        
        document_ids = [hit["_source"]["id"] for hit in response.body["hits"]["hits"]]
        logger.debug(f"Found document IDs: {document_ids}")

        if not document_ids:
            logger.debug("No documents found")
            return {
                "status": "success",
                "query_time": {"elasticsearch": response.body["took"].__str__() + "ms", "postgresql": "0ms"},
                "meta": {
                    "pages": {
                        "current_page": (skip // limit) + 1,
                        "next_page": None,
                        "prev_page": ((skip // limit)) if skip > 0 else None,
                        "total_pages": 0,
                        "limit_value": limit,
                        "offset_value": skip,
                        "total_count": total_hits,
                        "first_page?": True,
                        "last_page?": True,
                    }
                },
                "data": [],
                "included": [],
            }

        start_time = time.time()
        # Create a CASE statement to preserve the order of document_ids
        order_case = (
            "CASE "
            + " ".join(
                f"WHEN id = '{doc_id}' THEN {index}" for index, doc_id in enumerate(document_ids)
            )
            + " END"
        )

        query = (
            geoblacklight_development.select()
            .where(geoblacklight_development.c.id.in_(document_ids))
            .order_by(text(order_case))
        )

        documents = await database.fetch_all(query)
        processed_documents = []

        for doc in documents:
            processed_documents.append(
                {
                    "type": "document",
                    "id": doc["id"],
                    "score": next(
                        hit["_score"]
                        for hit in response.body["hits"]["hits"]
                        if hit["_source"]["id"] == doc["id"]
                    ),
                    "attributes": {**doc, **create_viewer_attributes(doc)},
                }
            )

        pg_query_time = (time.time() - start_time) * 1000

        included = [
            *process_aggregations(response.body.get("aggregations", {}), search_criteria),
            *get_sort_options(search_criteria),
        ]

        return {
            "status": "success",
            "query_time": {
                "elasticsearch": response.body["took"].__str__() + "ms",
                "postgresql": f"{round(pg_query_time)}ms",
            },
            "meta": {
                "pages": {
                    "current_page": (skip // limit) + 1,
                    "next_page": ((skip // limit) + 2) if (skip + limit) < total_hits else None,
                    "prev_page": ((skip // limit)) if skip > 0 else None,
                    "total_pages": (total_hits // limit) + (1 if total_hits % limit > 0 else 0),
                    "limit_value": limit,
                    "offset_value": skip,
                    "total_count": total_hits,
                    "first_page?": (skip == 0),
                    "last_page?": (skip + limit) >= total_hits,
                }
            },
            "data": processed_documents,
            "included": included,
        }

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f"Process response error: {str(e)}", exc_info=True)
        logger.error(f"Full traceback:\n{error_trace}")
        logger.error(f"Response body: {response.body}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "traceback": error_trace,
                "response": response.body
            }
        )


def process_aggregations(aggregations, search_criteria):
    """Transform Elasticsearch aggregations into JSON:API includes."""
    return [
        {
            "type": "facet",
            "id": agg_name,
            "attributes": {
                "label": agg_name.replace("_sm", "").replace("_", " ").title(),
                "items": [
                    {
                        "attributes": {
                            "label": bucket["key"],
                            "value": bucket["key"],
                            "hits": bucket["doc_count"],
                        },
                        "links": {
                            "self": generate_facet_link(agg_name, bucket["key"], search_criteria)
                        },
                    }
                    for bucket in agg_data["buckets"]
                ],
            },
        }
        for agg_name, agg_data in aggregations.items()
    ]


def generate_facet_link(agg_name, facet_value, search_criteria):
    """Generate a link for a facet with current search parameters."""
    base_url = os.getenv("APPLICATION_URL") + "/api/v1/search"
    query_params = {
        "q": search_criteria["query"] or "",
        "search_field": "all_fields",
        **{
            f"fq[{key}][]": value
            for key, values in search_criteria["filters"].items()
            for value in (values if isinstance(values, list) else [values])
        },
        f"fq[{agg_name}][]": facet_value,
    }
    query_string = "&".join(f"{key}={value}" for key, value in query_params.items())
    return f"{base_url}?{query_string}"
