import json
import logging
import os
import time
from typing import Dict, Optional
from urllib.parse import parse_qs

from elasticsearch.exceptions import NotFoundError
from fastapi import HTTPException

from app.api.v1.shared import SORT_MAPPINGS
from app.api.v1.utils import sanitize_for_json
from app.elasticsearch import search_items
from app.elasticsearch.client import es
from app.services.citation_service import CitationService
from app.services.download_service import DownloadService
from app.services.image_service import ImageService
from app.services.relationship_service import RelationshipService
from app.services.viewer_service import ViewerService, create_viewer_attributes
from db.database import database

logger = logging.getLogger(__name__)


class SearchService:
    def __init__(self):
        self.index_name = os.getenv("ELASTICSEARCH_INDEX", "btaa_ogm_api")
        self.es = es

    async def search(
        self,
        q: Optional[str],
        bbox: Optional[str] = None,
        page: int = 1,
        limit: int = 10,
        sort: Optional[str] = None,
        request_query_params: Optional[str] = None,
        callback: Optional[str] = None,
    ) -> Dict:
        """Search endpoint with caching support."""
        try:
            timings = {}
            start_time = time.time()

            # Calculate skip from page/limit
            skip = (page - 1) * limit

            # Get filter queries from request
            filter_query = (
                self.extract_filter_queries(request_query_params) if request_query_params else {}
            )

            # Add bbox filter if provided
            if bbox:
                try:
                    # Parse bbox string into coordinates
                    min_lon, min_lat, max_lon, max_lat = map(float, bbox.split())
                    logger.info(f"Parsed bbox coordinates: min_lon={min_lon}, min_lat={min_lat}, max_lon={max_lon}, max_lat={max_lat}")

                    # Build a geo_shape Polygon filter for dcat_bbox
                    polygon = [
                        [min_lon, min_lat],
                        [max_lon, min_lat],
                        [max_lon, max_lat],
                        [min_lon, max_lat],
                        [min_lon, min_lat],  # close the polygon
                    ]
                    
                    # Create the bbox filter
                    bbox_filter = {
                        "geo_shape": {
                            "dcat_bbox": {
                                "shape": {
                                    "type": "Polygon",
                                    "coordinates": [polygon],
                                },
                                "relation": "intersects"
                            }
                        }
                    }
                    
                    # Convert regular filters to bool query if needed
                    if filter_query and not isinstance(filter_query, dict):
                        filter_query = {
                            "bool": {
                                "must": [
                                    {"terms": {k: v}} if isinstance(v, list) else {"term": {k: v}}
                                    for k, v in filter_query.items()
                                ]
                            }
                        }
                    elif filter_query and not isinstance(filter_query.get("bool"), dict):
                        filter_query = {
                            "bool": {
                                "must": [
                                    {"terms": {k: v}} if isinstance(v, list) else {"term": {k: v}}
                                    for k, v in filter_query.items()
                                ]
                            }
                        }
                    
                    # If we already have a bool query, add to its must clauses
                    if isinstance(filter_query, dict) and "bool" in filter_query:
                        if "must" in filter_query["bool"]:
                            filter_query["bool"]["must"].append(bbox_filter)
                        else:
                            filter_query["bool"]["must"] = [bbox_filter]
                    else:
                        # Create a new bool query with the bbox filter
                        filter_query = {
                            "bool": {
                                "must": [bbox_filter]
                            }
                        }
                    
                    logger.info(f"Created combined filter query: {json.dumps(filter_query, indent=2)}")
                except (ValueError, IndexError) as e:
                    logger.error(f"Invalid bbox format: {bbox}")
                    raise HTTPException(
                        status_code=400,
                        detail="Invalid bbox format. Expected format: min_lon min_lat max_lon max_lat"
                    ) from e

            # Get sort mapping
            sort_mapping = SORT_MAPPINGS.get(sort, None)

            # Log the filter query for debugging
            logger.info(f"Filter query before search: {json.dumps(filter_query, indent=2)}")

            # Elasticsearch query
            es_start = time.time()
            results = await search_items(
                query=q,
                fq=filter_query,
                skip=skip,
                limit=limit,
                sort=sort_mapping,
            )
            es_time = (time.time() - es_start) * 1000
            timings["elasticsearch"] = f"{es_time:.0f}ms"

            # Process each item
            process_start = time.time()
            docs_processed = 0
            citation_time = 0
            thumbnail_time = 0
            viewer_time = 0

            for item in results.get("data", []):
                doc_start = time.time()

                # Add thumbnail URL
                thumb_start = time.time()
                image_service = ImageService(item["attributes"])
                item["attributes"]["ui_thumbnail_url"] = image_service.get_thumbnail_url()
                thumbnail_time += time.time() - thumb_start

                # Add citation
                cite_start = time.time()
                citation_service = CitationService(item["attributes"])
                item["attributes"]["ui_citation"] = citation_service.get_citation()
                citation_time += time.time() - cite_start

                # Add viewer attributes
                viewer_start = time.time()
                viewer_attrs = create_viewer_attributes(item["attributes"])
                item["attributes"].update(viewer_attrs)
                viewer_time += time.time() - viewer_start

                docs_processed += 1

            process_time = time.time() - process_start
            timings["item_processing"] = {
                "total": f"{(process_time * 1000):.0f}ms",
                "per_item": (
                    f"{((process_time / docs_processed) * 1000):.0f}ms"
                    if docs_processed > 0
                    else "0ms"
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

            return sanitized_results

        except Exception as e:
            logger.error("Search service error", exc_info=True)
            error_response = {
                "message": "Search operation failed",
                "error": str(e),
                "query": q,
                "bbox": bbox,
                "filters": filter_query if "filter_query" in locals() else None,
                "sort": sort,
            }
            return error_response

    async def get_item(
        self,
        id: str,
        callback: Optional[str] = None,
        include_relationships: bool = True,
        include_summaries: bool = True,
    ) -> Dict:
        """Get a single item by ID."""
        try:
            # Get the item from Elasticsearch
            try:
                result = await self.es.get(index=self.index_name, id=id)
            except NotFoundError:
                raise HTTPException(status_code=404, detail="Item not found") from None
            except Exception as e:
                logger.error(f"Elasticsearch error getting item {id}: {str(e)}", exc_info=True)
                raise HTTPException(status_code=500, detail=str(e)) from e

            source_data = result["_source"]

            # Create services
            download_service = DownloadService(source_data)
            viewer_service = ViewerService(source_data)
            citation_service = CitationService(source_data)

            # Parse dct_references_s if it's a string
            if isinstance(source_data.get("dct_references_s"), str):
                try:
                    source_data["dct_references_s"] = json.loads(source_data["dct_references_s"])
                except json.JSONDecodeError:
                    logger.warning(f"Could not parse dct_references_s for item {id}")

            # Add UI attributes in the same order as the original code
            source_data["ui_thumbnail_url"] = source_data.get("thumbnail_url")
            source_data["ui_citation"] = citation_service.get_citation()
            source_data["ui_downloads"] = download_service.get_download_options()

            # Add viewer attributes
            viewer_attributes = viewer_service.get_viewer_attributes()
            source_data.update(viewer_attributes)

            # Add relationships if requested
            if include_relationships:
                try:
                    relationship_service = RelationshipService()
                    relationships = await relationship_service.get_item_relationships(id)
                    source_data["ui_relationships"] = relationships
                except Exception as e:
                    logger.error(f"Error getting relationships: {e}", exc_info=True)
                    source_data["ui_relationships"] = {}

            # Add summaries if requested
            if include_summaries:
                try:
                    summaries_query = """
                        SELECT * FROM item_ai_enrichments 
                        WHERE item_id = :item_id 
                        ORDER BY created_at DESC
                    """
                    summaries = await database.fetch_all(summaries_query, {"item_id": id})
                    source_data["ui_summaries"] = [
                        sanitize_for_json(dict(summary)) for summary in summaries
                    ]
                except Exception as e:
                    logger.error(f"Error getting summaries: {e}", exc_info=True)
                    source_data["ui_summaries"] = []

            # Create the response structure
            response = {
                "data": {
                    "type": "item",
                    "id": id,
                    "attributes": source_data,
                }
            }

            return response

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting item {id}: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e)) from e

    async def suggest(self, q: str, resource_class: Optional[str] = None, size: int = 5) -> Dict:
        """Get search suggestions."""
        try:
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
                    "my-suggestion": {
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
            response = await es.search(index=self.index_name, body=suggest_query)
            response_dict = response.body
            suggestions = []
            seen_ids = set()

            if response_dict.get("suggest", {}).get("my-suggestion"):
                for suggestion in response_dict["suggest"]["my-suggestion"]:
                    if options := suggestion.get("options", []):
                        for option in options:
                            suggestion_id = option["_id"]
                            if suggestion_id not in seen_ids:
                                seen_ids.add(suggestion_id)
                                suggestions.append(
                                    {
                                        "type": "suggestion",
                                        "id": suggestion_id,
                                        "attributes": {
                                            "text": option.get("text", ""),
                                            "title": option.get("_source", {}).get(
                                                "dct_title_s", ""
                                            ),
                                            "score": option.get("_score", 0),
                                        },
                                    }
                                )
            return {
                "data": suggestions,
                "meta": {
                    "query": q,
                    "resource_class": resource_class,
                    "es_query": suggest_query,
                    "es_response": response_dict,
                },
            }
        except Exception as e:
            logger.error(f"Error getting suggestions: {str(e)}", exc_info=True)
            return {"data": [], "meta": {"error": str(e)}}

    def extract_filter_queries(self, params: str) -> Dict:
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
