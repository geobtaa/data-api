from fastapi import HTTPException
from .client import es
from db.database import database
from db.models import geoblacklight_development
import os
import time

def get_search_criteria(query: str, fq: dict, skip: int, limit: int):
    """Return the currently applied search criteria."""
    return {
        "query": query,
        "filters": fq,
        "pagination": {
            "skip": skip,
            "limit": limit
        },
        "sort": [{"_score": "desc"}]
    }

async def search_documents(query: str = None, fq: dict = None, skip: int = 0, limit: int = 20):
    """Search documents in Elasticsearch with optional filters."""
    index_name = os.getenv("ELASTICSEARCH_INDEX", "geoblacklight")
    
    # Get the current search criteria
    search_criteria = get_search_criteria(query, fq, skip, limit)
    print("Current Search Criteria:", search_criteria)
    
    # Construct the filter query
    filter_clauses = []
    if fq:
        for field, values in fq.items():
            if isinstance(values, list):
                # Handle multiple values for a field
                filter_clauses.append({
                    "terms": {field: values}
                })
            else:
                # Handle single value
                filter_clauses.append({
                    "term": {field: values}
                })
    
    search_query = {
        "query": {
            "bool": {
                "must": [{"match_all": {}}] if not query else [
                    {
                        "multi_match": {
                            "query": query,
                            "fields": [
                                "dct_title_s^3",            # Boost title matches
                                "dct_description_sm^2",    # Boost description matches
                                "dct_creator_sm",
                                "dct_publisher_sm",
                                "dct_subject_sm",
                                "dcat_theme_sm",
                                "dcat_keyword_sm",
                                "dct_spatial_sm"
                            ]
                        }
                    }
                ],
                "filter": filter_clauses  # Add filter clauses here
            }
        },
        "from": skip,
        "size": limit,
        "sort": [{"_score": "desc"}],
        "aggs": {
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
    
    try:
        response = await es.search(
            index=index_name,
            body=search_query,
            track_total_hits=True
        )
        
        return await process_search_response(response, limit, skip, search_criteria)
        
    except Exception as e:
        print(f"Search error: {e}")
        raise HTTPException(status_code=500, detail="Search operation failed")

async def process_search_response(response, limit, skip, search_criteria):
    """Process Elasticsearch response and fetch documents from PostgreSQL."""
    total_hits = response["hits"]["total"]["value"]
    document_ids = [hit["_source"]["id"] for hit in response["hits"]["hits"]]
    
    start_time = time.time()
    query = geoblacklight_development.select().where(
        geoblacklight_development.c.id.in_(document_ids)
    )
    documents = await database.fetch_all(query)
    pg_query_time = (time.time() - start_time) * 1000

    included = process_aggregations(response.get("aggregations", {}), search_criteria)

    return {
        "status": "success",
        "query_time": {
            "elasticsearch": response["took"].__str__() + "ms",
            "postgresql": f"{round(pg_query_time)}ms"
        },
        "pagination": {
            "total": total_hits,
            "page_size": limit,
            "current_page": (skip // limit) + 1,
            "total_pages": (total_hits // limit) + (1 if total_hits % limit > 0 else 0)
        },
        "data": [
            {
                "type": "document",
                "id": doc["id"],
                "score": next(hit["_score"] for hit in response["hits"]["hits"] 
                            if hit["_source"]["id"] == doc["id"]),
                "attributes": doc
            }
            for doc in documents
        ],
        "included": included
    }

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
                            "hits": bucket["doc_count"]
                        },
                        "links": {
                            "self": generate_facet_link(agg_name, bucket["key"], search_criteria)
                        }
                    }
                    for bucket in agg_data["buckets"]
                ]
            }
        }
        for agg_name, agg_data in aggregations.items()
    ]

def generate_facet_link(agg_name, facet_value, search_criteria):
    """Generate a link for a facet with current search parameters."""
    base_url = os.getenv('APPLICATION_URL') + "/api/v1/search"
    query_params = {
        "q": search_criteria["query"] or "",
        "search_field": "all_fields",
        **{f"fq[{key}][]": value for key, values in search_criteria["filters"].items() for value in (values if isinstance(values, list) else [values])},
        f"fq[{agg_name}][]": facet_value
    }
    query_string = "&".join(f"{key}={value}" for key, value in query_params.items())
    return f"{base_url}?{query_string}"
  