from fastapi import APIRouter, HTTPException, Query, Request, BackgroundTasks
from typing import Optional, Dict, List, Any
from sqlalchemy import text, select, or_, and_, func
import os
import json
import logging
from datetime import datetime
from ...services.cache_service import cached_endpoint

from db.database import database
from db.models import (
    gazetteer_geonames,
    gazetteer_wof_spr,
    gazetteer_wof_ancestors,
    gazetteer_wof_concordances,
    gazetteer_wof_geojson,
    gazetteer_wof_names,
    gazetteer_btaa
)

router = APIRouter()
logger = logging.getLogger(__name__)

# Cache TTL for gazetteer endpoints (1 hour)
GAZETTEER_CACHE_TTL = int(os.getenv("GAZETTEER_CACHE_TTL", 3600))


@router.get("/gazetteers")
@cached_endpoint(ttl=GAZETTEER_CACHE_TTL)
async def list_gazetteers():
    """List all available gazetteers with record counts."""
    try:
        # Get record counts for each gazetteer
        geonames_count = await database.fetch_val(
            select([func.count()]).select_from(gazetteer_geonames)
        )
        
        wof_spr_count = await database.fetch_val(
            select([func.count()]).select_from(gazetteer_wof_spr)
        )
        
        btaa_count = await database.fetch_val(
            select([func.count()]).select_from(gazetteer_btaa)
        )
        
        # Additional WOF table counts
        wof_ancestors_count = await database.fetch_val(
            select([func.count()]).select_from(gazetteer_wof_ancestors)
        )
        
        wof_concordances_count = await database.fetch_val(
            select([func.count()]).select_from(gazetteer_wof_concordances)
        )
        
        wof_geojson_count = await database.fetch_val(
            select([func.count()]).select_from(gazetteer_wof_geojson)
        )
        
        wof_names_count = await database.fetch_val(
            select([func.count()]).select_from(gazetteer_wof_names)
        )
        
        return {
            "data": [
                {
                    "id": "geonames",
                    "type": "gazetteer",
                    "attributes": {
                        "name": "GeoNames",
                        "description": "GeoNames geographical database",
                        "record_count": geonames_count,
                        "website": "https://www.geonames.org/"
                    }
                },
                {
                    "id": "wof",
                    "type": "gazetteer",
                    "attributes": {
                        "name": "Who's on First",
                        "description": "Who's on First gazetteer from Mapzen",
                        "record_count": wof_spr_count,
                        "website": "https://whosonfirst.org/",
                        "additional_tables": {
                            "ancestors": wof_ancestors_count,
                            "concordances": wof_concordances_count,
                            "geojson": wof_geojson_count,
                            "names": wof_names_count
                        }
                    }
                },
                {
                    "id": "btaa",
                    "type": "gazetteer",
                    "attributes": {
                        "name": "BTAA",
                        "description": "Big Ten Academic Alliance Geoportal gazetteer",
                        "record_count": btaa_count,
                        "website": "https://geo.btaa.org/"
                    }
                }
            ],
            "meta": {
                "total_gazetteers": 3,
                "total_records": geonames_count + wof_spr_count + btaa_count
            }
        }
    except Exception as e:
        logger.error(f"Error listing gazetteers: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error listing gazetteers: {str(e)}")


@router.get("/gazetteers/geonames")
@cached_endpoint(ttl=GAZETTEER_CACHE_TTL)
async def search_geonames(
    q: Optional[str] = None,
    name: Optional[str] = None,
    country_code: Optional[str] = None,
    feature_class: Optional[str] = None,
    feature_code: Optional[str] = None,
    admin1_code: Optional[str] = None,
    admin2_code: Optional[str] = None,
    population_min: Optional[int] = None,
    population_max: Optional[int] = None,
    offset: int = 0,
    limit: int = 20
):
    """
    Search GeoNames gazetteer.
    
    Parameters:
    - q: General search query (searches name, asciiname, and alternatenames)
    - name: Exact name match
    - country_code: Two-letter country code
    - feature_class: Feature class (A, P, H, etc.)
    - feature_code: Feature code (e.g., PPL, ADM1)
    - admin1_code: First-level administrative division code
    - admin2_code: Second-level administrative division code
    - population_min: Minimum population
    - population_max: Maximum population
    - offset: Result offset for pagination
    - limit: Maximum number of results to return
    """
    try:
        # Build query
        query = select([gazetteer_geonames])
        
        # Apply filters
        conditions = []
        
        if q:
            # Search in name, asciiname, and alternatenames
            search_term = f"%{q}%"
            conditions.append(
                or_(
                    gazetteer_geonames.c.name.ilike(search_term),
                    gazetteer_geonames.c.asciiname.ilike(search_term),
                    gazetteer_geonames.c.alternatenames.ilike(search_term)
                )
            )
        
        if name:
            conditions.append(gazetteer_geonames.c.name == name)
        
        if country_code:
            conditions.append(gazetteer_geonames.c.country_code == country_code.upper())
        
        if feature_class:
            conditions.append(gazetteer_geonames.c.feature_class == feature_class)
        
        if feature_code:
            conditions.append(gazetteer_geonames.c.feature_code == feature_code)
        
        if admin1_code:
            conditions.append(gazetteer_geonames.c.admin1_code == admin1_code)
        
        if admin2_code:
            conditions.append(gazetteer_geonames.c.admin2_code == admin2_code)
        
        if population_min is not None:
            conditions.append(gazetteer_geonames.c.population >= population_min)
        
        if population_max is not None:
            conditions.append(gazetteer_geonames.c.population <= population_max)
        
        # Apply conditions to query
        if conditions:
            query = query.where(and_(*conditions))
        
        # Apply pagination and ordering
        query = query.order_by(
            gazetteer_geonames.c.population.desc(),
            gazetteer_geonames.c.name
        ).offset(offset).limit(limit)
        
        # Execute query
        results = await database.fetch_all(query)
        
        # Get total count for pagination
        count_query = select([func.count()]).select_from(gazetteer_geonames)
        if conditions:
            count_query = count_query.where(and_(*conditions))
        
        total_count = await database.fetch_val(count_query)
        
        # Format results
        formatted_results = []
        for result in results:
            record = dict(result)
            formatted_results.append({
                "id": str(record["geonameid"]),
                "type": "geoname",
                "attributes": {
                    "name": record["name"],
                    "asciiname": record["asciiname"],
                    "latitude": float(record["latitude"]) if record["latitude"] else None,
                    "longitude": float(record["longitude"]) if record["longitude"] else None,
                    "feature_class": record["feature_class"],
                    "feature_code": record["feature_code"],
                    "country_code": record["country_code"],
                    "admin1_code": record["admin1_code"],
                    "admin2_code": record["admin2_code"],
                    "admin3_code": record["admin3_code"],
                    "admin4_code": record["admin4_code"],
                    "population": record["population"],
                    "timezone": record["timezone"],
                    "modification_date": record["modification_date"].isoformat() if record["modification_date"] else None,
                    "elevation": record["elevation"],
                    "dem": record["dem"],
                    "cc2": record["cc2"],
                    "alternatenames": record["alternatenames"]
                }
            })
        
        return {
            "data": formatted_results,
            "meta": {
                "total_count": total_count,
                "offset": offset,
                "limit": limit,
                "query": {
                    "q": q,
                    "name": name,
                    "country_code": country_code,
                    "feature_class": feature_class,
                    "feature_code": feature_code,
                    "admin1_code": admin1_code,
                    "admin2_code": admin2_code,
                    "population_min": population_min,
                    "population_max": population_max
                }
            }
        }
        
    except Exception as e:
        logger.error(f"Error searching GeoNames: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error searching GeoNames: {str(e)}")


@router.get("/gazetteers/wof")
@cached_endpoint(ttl=GAZETTEER_CACHE_TTL)
async def search_wof(
    q: Optional[str] = None,
    name: Optional[str] = None,
    placetype: Optional[str] = None,
    country: Optional[str] = None,
    is_current: Optional[int] = None,
    parent_id: Optional[int] = None,
    offset: int = 0,
    limit: int = 20
):
    """
    Search Who's on First gazetteer.
    
    Parameters:
    - q: General search query (searches name)
    - name: Exact name match
    - placetype: Place type (e.g., country, region, locality)
    - country: Two-letter country code
    - is_current: Whether the place is current (1) or not (0)
    - parent_id: ID of the parent place
    - offset: Result offset for pagination
    - limit: Maximum number of results to return
    """
    try:
        # Build query
        query = select([gazetteer_wof_spr])
        
        # Apply filters
        conditions = []
        
        if q:
            # Search in name
            search_term = f"%{q}%"
            conditions.append(gazetteer_wof_spr.c.name.ilike(search_term))
        
        if name:
            conditions.append(gazetteer_wof_spr.c.name == name)
        
        if placetype:
            conditions.append(gazetteer_wof_spr.c.placetype == placetype)
        
        if country:
            conditions.append(gazetteer_wof_spr.c.country == country.upper())
        
        if is_current is not None:
            conditions.append(gazetteer_wof_spr.c.is_current == is_current)
        
        if parent_id is not None:
            conditions.append(gazetteer_wof_spr.c.parent_id == parent_id)
        
        # Apply conditions to query
        if conditions:
            query = query.where(and_(*conditions))
        
        # Apply pagination and ordering
        query = query.order_by(
            gazetteer_wof_spr.c.name
        ).offset(offset).limit(limit)
        
        # Execute query
        results = await database.fetch_all(query)
        
        # Get total count for pagination
        count_query = select([func.count()]).select_from(gazetteer_wof_spr)
        if conditions:
            count_query = count_query.where(and_(*conditions))
        
        total_count = await database.fetch_val(count_query)
        
        # Format results
        formatted_results = []
        for result in results:
            record = dict(result)
            
            # Convert decimal values to float for JSON serialization
            for key in ['latitude', 'longitude', 'min_latitude', 'min_longitude', 'max_latitude', 'max_longitude']:
                if record.get(key) is not None:
                    record[key] = float(record[key])
            
            formatted_results.append({
                "id": str(record["wok_id"]),
                "type": "wof",
                "attributes": {
                    "name": record["name"],
                    "placetype": record["placetype"],
                    "country": record["country"],
                    "parent_id": record["parent_id"],
                    "latitude": record["latitude"],
                    "longitude": record["longitude"],
                    "min_latitude": record["min_latitude"],
                    "min_longitude": record["min_longitude"],
                    "max_latitude": record["max_latitude"],
                    "max_longitude": record["max_longitude"],
                    "is_current": record["is_current"],
                    "is_deprecated": record["is_deprecated"],
                    "is_ceased": record["is_ceased"],
                    "is_superseded": record["is_superseded"],
                    "is_superseding": record["is_superseding"],
                    "repo": record["repo"],
                    "lastmodified": record["lastmodified"]
                }
            })
        
        return {
            "data": formatted_results,
            "meta": {
                "total_count": total_count,
                "offset": offset,
                "limit": limit,
                "query": {
                    "q": q,
                    "name": name,
                    "placetype": placetype,
                    "country": country,
                    "is_current": is_current,
                    "parent_id": parent_id
                }
            }
        }
        
    except Exception as e:
        logger.error(f"Error searching Who's on First: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error searching Who's on First: {str(e)}")


@router.get("/gazetteers/wof/{wok_id}")
@cached_endpoint(ttl=GAZETTEER_CACHE_TTL)
async def get_wof_details(wok_id: int):
    """
    Get detailed information about a Who's on First place.
    
    Parameters:
    - wok_id: Who's on First ID
    """
    try:
        # Get basic info
        spr_query = select([gazetteer_wof_spr]).where(gazetteer_wof_spr.c.wok_id == wok_id)
        spr = await database.fetch_one(spr_query)
        
        if not spr:
            raise HTTPException(status_code=404, detail=f"WOF place with ID {wok_id} not found")
        
        spr_record = dict(spr)
        
        # Convert decimal values to float for JSON serialization
        for key in ['latitude', 'longitude', 'min_latitude', 'min_longitude', 'max_latitude', 'max_longitude']:
            if spr_record.get(key) is not None:
                spr_record[key] = float(spr_record[key])
        
        # Get ancestors
        ancestors_query = select([gazetteer_wof_ancestors]).where(gazetteer_wof_ancestors.c.wok_id == wok_id)
        ancestors = await database.fetch_all(ancestors_query)
        
        # Get names
        names_query = select([gazetteer_wof_names]).where(gazetteer_wof_names.c.wok_id == wok_id)
        names = await database.fetch_all(names_query)
        
        # Get concordances
        concordances_query = select([gazetteer_wof_concordances]).where(gazetteer_wof_concordances.c.wok_id == wok_id)
        concordances = await database.fetch_all(concordances_query)
        
        # Get GeoJSON
        geojson_query = select([gazetteer_wof_geojson]).where(gazetteer_wof_geojson.c.wok_id == wok_id)
        geojson = await database.fetch_all(geojson_query)
        
        # Format result
        result = {
            "id": str(wok_id),
            "type": "wof_detail",
            "attributes": {
                "spr": {
                    "name": spr_record["name"],
                    "placetype": spr_record["placetype"],
                    "country": spr_record["country"],
                    "parent_id": spr_record["parent_id"],
                    "latitude": spr_record["latitude"],
                    "longitude": spr_record["longitude"],
                    "min_latitude": spr_record["min_latitude"],
                    "min_longitude": spr_record["min_longitude"],
                    "max_latitude": spr_record["max_latitude"],
                    "max_longitude": spr_record["max_longitude"],
                    "is_current": spr_record["is_current"],
                    "is_deprecated": spr_record["is_deprecated"],
                    "is_ceased": spr_record["is_ceased"],
                    "is_superseded": spr_record["is_superseded"],
                    "is_superseding": spr_record["is_superseding"],
                    "repo": spr_record["repo"],
                    "lastmodified": spr_record["lastmodified"]
                },
                "ancestors": [dict(a) for a in ancestors],
                "names": [dict(n) for n in names],
                "concordances": [dict(c) for c in concordances],
                "geojson": [dict(g) for g in geojson]
            }
        }
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting WOF details: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting WOF details: {str(e)}")


@router.get("/gazetteers/btaa")
@cached_endpoint(ttl=GAZETTEER_CACHE_TTL)
async def search_btaa(
    q: Optional[str] = None,
    fast_area: Optional[str] = None,
    state_abbv: Optional[str] = None,
    county_fips: Optional[str] = None,
    offset: int = 0,
    limit: int = 20
):
    """
    Search BTAA gazetteer.
    
    Parameters:
    - q: General search query (searches fast_area, state_name, and namelsad)
    - fast_area: Exact FAST area match
    - state_abbv: Two-letter state abbreviation
    - county_fips: County FIPS code
    - offset: Result offset for pagination
    - limit: Maximum number of results to return
    """
    try:
        # Build query
        query = select([gazetteer_btaa])
        
        # Apply filters
        conditions = []
        
        if q:
            # Search in fast_area, state_name, and namelsad
            search_term = f"%{q}%"
            conditions.append(
                or_(
                    gazetteer_btaa.c.fast_area.ilike(search_term),
                    gazetteer_btaa.c.state_name.ilike(search_term),
                    gazetteer_btaa.c.namelsad.ilike(search_term)
                )
            )
        
        if fast_area:
            conditions.append(gazetteer_btaa.c.fast_area == fast_area)
        
        if state_abbv:
            conditions.append(gazetteer_btaa.c.state_abbv == state_abbv.upper())
        
        if county_fips:
            conditions.append(gazetteer_btaa.c.county_fips == county_fips)
        
        # Apply conditions to query
        if conditions:
            query = query.where(and_(*conditions))
        
        # Apply pagination and ordering
        query = query.order_by(
            gazetteer_btaa.c.state_abbv,
            gazetteer_btaa.c.fast_area
        ).offset(offset).limit(limit)
        
        # Execute query
        results = await database.fetch_all(query)
        
        # Get total count for pagination
        count_query = select([func.count()]).select_from(gazetteer_btaa)
        if conditions:
            count_query = count_query.where(and_(*conditions))
        
        total_count = await database.fetch_val(count_query)
        
        # Format results
        formatted_results = []
        for result in results:
            record = dict(result)
            formatted_results.append({
                "id": str(record["id"]),
                "type": "btaa",
                "attributes": {
                    "fast_area": record["fast_area"],
                    "bounding_box": record["bounding_box"],
                    "geometry": record["geometry"],
                    "geonames_id": record["geonames_id"],
                    "state_abbv": record["state_abbv"],
                    "state_name": record["state_name"],
                    "county_fips": record["county_fips"],
                    "statefp": record["statefp"],
                    "namelsad": record["namelsad"]
                }
            })
        
        return {
            "data": formatted_results,
            "meta": {
                "total_count": total_count,
                "offset": offset,
                "limit": limit,
                "query": {
                    "q": q,
                    "fast_area": fast_area,
                    "state_abbv": state_abbv,
                    "county_fips": county_fips
                }
            }
        }
        
    except Exception as e:
        logger.error(f"Error searching BTAA: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error searching BTAA: {str(e)}")


@router.get("/gazetteers/search")
@cached_endpoint(ttl=GAZETTEER_CACHE_TTL)
async def search_all_gazetteers(
    q: str = Query(..., description="Search query"),
    gazetteer: Optional[str] = Query(None, description="Specific gazetteer to search (geonames, wof, btaa, or all)"),
    country_code: Optional[str] = Query(None, description="Two-letter country code"),
    state_abbv: Optional[str] = Query(None, description="Two-letter state abbreviation (for BTAA)"),
    offset: int = Query(0, description="Result offset for pagination"),
    limit: int = Query(20, description="Maximum number of results to return")
):
    """
    Search across all gazetteers.
    
    Parameters:
    - q: Search query (required)
    - gazetteer: Specific gazetteer to search (geonames, wof, btaa, or all)
    - country_code: Two-letter country code (for GeoNames and WOF)
    - state_abbv: Two-letter state abbreviation (for BTAA)
    - offset: Result offset for pagination
    - limit: Maximum number of results to return
    """
    try:
        results = []
        total_count = 0
        
        # Determine which gazetteers to search
        gazetteers_to_search = []
        if not gazetteer or gazetteer.lower() == 'all':
            gazetteers_to_search = ['geonames', 'wof', 'btaa']
        else:
            gazetteers_to_search = [gazetteer.lower()]
        
        # Search GeoNames
        if 'geonames' in gazetteers_to_search:
            geonames_results = await search_geonames(
                q=q,
                country_code=country_code,
                offset=offset,
                limit=limit
            )
            
            # Add source to each result
            for result in geonames_results["data"]:
                result["source"] = "geonames"
            
            results.extend(geonames_results["data"])
            total_count += geonames_results["meta"]["total_count"]
        
        # Search WOF
        if 'wof' in gazetteers_to_search:
            wof_results = await search_wof(
                q=q,
                country=country_code,
                offset=offset,
                limit=limit
            )
            
            # Add source to each result
            for result in wof_results["data"]:
                result["source"] = "wof"
            
            results.extend(wof_results["data"])
            total_count += wof_results["meta"]["total_count"]
        
        # Search BTAA
        if 'btaa' in gazetteers_to_search:
            btaa_results = await search_btaa(
                q=q,
                state_abbv=state_abbv,
                offset=offset,
                limit=limit
            )
            
            # Add source to each result
            for result in btaa_results["data"]:
                result["source"] = "btaa"
            
            results.extend(btaa_results["data"])
            total_count += btaa_results["meta"]["total_count"]
        
        return {
            "data": results[:limit],  # Limit results
            "meta": {
                "total_count": total_count,
                "offset": offset,
                "limit": limit,
                "query": {
                    "q": q,
                    "gazetteer": gazetteer,
                    "country_code": country_code,
                    "state_abbv": state_abbv,
                    "gazetteers_searched": gazetteers_to_search
                }
            }
        }
        
    except Exception as e:
        logger.error(f"Error searching all gazetteers: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error searching all gazetteers: {str(e)}") 