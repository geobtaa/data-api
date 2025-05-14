import json
import logging
import os
import time
from urllib.parse import urlencode
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import HTTPException
from sqlalchemy.sql import text

from app.services.viewer_service import create_viewer_attributes  # Updated import
from db.database import database
from db.models import items

from .client import es

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


def get_search_criteria(query: str, fq: dict, skip: int, limit: int, sort: list = None):
    """Return the currently applied search criteria."""
    return {
        "query": query,
        "filters": fq,
        "pagination": {"skip": skip, "limit": limit},
        "sort": sort or [{"_score": "desc"}],
    }


async def search_items(
    query: str = None, fq: dict = None, skip: int = 0, limit: int = 20, sort: list = None
):
    """Search items in Elasticsearch with optional filters, sorting, and spelling
    suggestions."""
    # Ensure limit is not zero to avoid division by zero errors
    if limit <= 0:
        limit = 20  # Default to 20 if limit is zero or negative

    index_name = os.getenv("ELASTICSEARCH_INDEX", "btaa_geometadata_api")

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

        # Build the search query
        if search_criteria.get("query"):
            # Create a multi-match query that searches across multiple fields
            search_query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "multi_match": {
                                    "query": search_criteria["query"],
                                    "fields": [
                                        "dct_title_s^3",  # Boost title matches
                                        "dct_description_sm^2",  # Boost description matches
                                        "summary^2",  # Add summary field with boost
                                        "dct_creator_sm^2",  # Boost creator name matches
                                        "dct_subject_sm^1.5",  # Boost subject matches
                                        "dcat_keyword_sm^1.5",  # Boost keyword matches
                                        "dct_publisher_sm",  # Include publisher name
                                        "schema_provider_s",  # Include provider name
                                        "dct_spatial_sm",  # Include spatial name
                                        "gbl_displaynote_sm",  # Include display notes
                                    ],
                                    "type": "best_fields",
                                    "operator": "and",
                                }
                            }
                        ],
                        "filter": filter_clauses,
                    }
                },
                "from": skip,
                "size": limit,
                "sort": sort or [{"_score": "desc"}],
                "track_total_hits": True,
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
                    "georeferenced_agg": {"terms": {"field": "gbl_georeferenced_b"}},
                },
            }

            # Only add suggest if query is not empty
            if search_criteria["query"].strip():
                search_query["suggest"] = {
                    "text": search_criteria["query"],
                    "simple_phrase": {
                        "phrase": {
                            "field": "dct_title_s",
                            "size": 1,
                            "gram_size": 3,
                            "direct_generator": [
                                {"field": "dct_title_s", "suggest_mode": "always"},
                                {"field": "dct_description_sm", "suggest_mode": "always"},
                            ],
                            "highlight": {"pre_tag": "<em>", "post_tag": "</em>"},
                        }
                    },
                }
        else:
            search_query = {
                "query": {"bool": {"must": [{"match_all": {}}], "filter": filter_clauses}},
                "from": skip,
                "size": limit,
                "sort": sort or [{"_score": "desc"}],
                "track_total_hits": True,
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
                    "georeferenced_agg": {"terms": {"field": "gbl_georeferenced_b"}},
                },
            }

        logger.debug(f"ES Query: {json.dumps(search_query, indent=2)}")

        try:
            response = await es.search(
                index=index_name,
                query=search_query["query"],
                from_=skip,
                size=limit,
                sort=sort or [{"_score": "desc"}],
                track_total_hits=True,
                aggs=search_query["aggs"],
                suggest=search_query.get("suggest"),  # Only include suggest if it exists
            )
        except Exception as es_error:
            logger.error(f"Elasticsearch error: {str(es_error)}", exc_info=True)
            error_detail = {
                "message": "Elasticsearch query failed",
                "error": str(es_error),
                "query": search_query,
                "index": index_name,
            }
            if hasattr(es_error, "info"):
                error_detail["info"] = es_error.info
            if hasattr(es_error, "status_code"):
                error_detail["status_code"] = es_error.status_code
            raise HTTPException(status_code=500, detail=error_detail) from es_error

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
                "self": (
                    f"{base_url}?{urlencode({**current_params, 'sort': 'relevance'}, doseq=True)}"
                )
            },
        },
        {
            "type": "sort",
            "id": "year_desc",
            "attributes": {"label": "Year (Newest first)"},
            "links": {
                "self": (
                    f"{base_url}?{urlencode({**current_params, 'sort': 'year_desc'}, doseq=True)}"
                )
            },
        },
        {
            "type": "sort",
            "id": "year_asc",
            "attributes": {"label": "Year (Oldest first)"},
            "links": {
                "self": (
                    f"{base_url}?{urlencode({**current_params, 'sort': 'year_asc'}, doseq=True)}"
                )
            },
        },
        {
            "type": "sort",
            "id": "title_asc",
            "attributes": {"label": "Title (A-Z)"},
            "links": {
                "self": (
                    f"{base_url}?{urlencode({**current_params, 'sort': 'title_asc'}, doseq=True)}"
                )
            },
        },
        {
            "type": "sort",
            "id": "title_desc",
            "attributes": {"label": "Title (Z-A)"},
            "links": {
                "self": (
                    f"{base_url}?{urlencode({**current_params, 'sort': 'title_desc'}, doseq=True)}"
                )
            },
        },
    ]
    return sort_options


async def process_search_response(response, limit, skip, search_criteria):
    """Process Elasticsearch response and format for API output."""
    try:
        total_hits = response["hits"]["total"]["value"]
        logger.debug(f"Total hits: {total_hits}")

        document_ids = [hit["_source"]["id"] for hit in response["hits"]["hits"]]
        logger.debug(f"Found document IDs: {document_ids}")

        # Process spelling suggestions
        suggestions = []
        if "suggest" in response:
            simple_phrase = response["suggest"].get("simple_phrase", [])
            for suggestion in simple_phrase:
                if suggestion.get("options"):
                    for option in suggestion["options"]:
                        suggestions.append(
                            {
                                "text": option.get("text"),
                                "highlighted": option.get("highlighted"),
                                "score": option.get("score"),
                            }
                        )

        if not document_ids:
            logger.debug("No documents found")
            return {
                "status": "success",
                "query_time": {
                    "elasticsearch": response["took"].__str__() + "ms",
                    "postgresql": "0ms",
                },
                "meta": {
                    "pages": {
                        "current_page": (skip // limit) + 1,
                        "next_page": None,
                        "prev_page": (skip // limit) if skip > 0 else None,
                        "total_pages": 0,
                        "limit_value": limit,
                        "offset_value": skip,
                        "total_count": total_hits,
                        "first_page?": True,
                        "last_page?": True,
                    },
                    "suggestions": suggestions,  # Add suggestions to meta
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
            items.select()
            .where(items.c.id.in_(document_ids))
            .order_by(text(order_case))
        )

        item_rows = await database.fetch_all(query)
        processed_items = []

        for item in item_rows:
            processed_items.append(
                {
                    "type": "document",
                    "id": item["id"],
                    "score": next(
                        hit["_score"]
                        for hit in response["hits"]["hits"]
                        if hit["_source"]["id"] == item["id"]
                    ),
                    "attributes": {**item, **create_viewer_attributes(item)},
                }
            )

        pg_query_time = (time.time() - start_time) * 1000

        included = [
            *process_aggregations(response.get("aggregations", {}), search_criteria),
            *get_sort_options(search_criteria),
        ]

        return {
            "status": "success",
            "query_time": {
                "elasticsearch": response["took"].__str__() + "ms",
                "postgresql": f"{round(pg_query_time)}ms",
            },
            "meta": {
                "pages": {
                    "current_page": (skip // limit) + 1,
                    "next_page": ((skip // limit) + 2) if (skip + limit) < total_hits else None,
                    "prev_page": (skip // limit) if skip > 0 else None,
                    "total_pages": (
                        (total_hits // limit) + (1 if total_hits % limit > 0 else 0)
                        if limit > 0
                        else 0
                    ),
                    "limit_value": limit,
                    "offset_value": skip,
                    "total_count": total_hits,
                    "first_page?": (skip == 0),
                    "last_page?": (skip + limit) >= total_hits,
                },
                "suggestions": suggestions,  # Add suggestions to meta
            },
            "data": processed_items,
            "included": included,
        }

    except Exception as e:
        import traceback

        error_trace = traceback.format_exc()
        logger.error(f"Process response error: {str(e)}", exc_info=True)
        logger.error(f"Full traceback:\n{error_trace}")
        logger.error(f"Response body: {response}")
        raise HTTPException(
            status_code=500,
            detail={"error": str(e), "traceback": error_trace, "response": response},
        ) from e


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
    base_url = os.getenv("APPLICATION_URL", "http://localhost:8000") + "/api/v1/search"
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
