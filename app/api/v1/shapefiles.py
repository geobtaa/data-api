import logging
import os
import tempfile
import requests
import urllib3
import zipfile
from typing import Any, Optional

import duckdb
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.api.v1.utils import create_response, sanitize_for_json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Suppress SSL warnings when verification is disabled
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

router = APIRouter()
logger = logging.getLogger(__name__)

# DuckDB configuration
DUCKDB_DATABASE_PATH = os.getenv("DUCKDB_DATABASE_PATH", "data/duckdb/btaa_ogm_api.duckdb")

# Ensure the DuckDB directory exists
os.makedirs(os.path.dirname(DUCKDB_DATABASE_PATH), exist_ok=True)

# Initialize DuckDB connection
def get_duckdb_connection():
    """Get a DuckDB connection with spatial extension loaded."""
    con = duckdb.connect(DUCKDB_DATABASE_PATH)
    # Load the spatial extension for shapefile support
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")
    return con

class Page(BaseModel):
    total_rows: int
    columns: list[str]
    rows: list[dict[str, Any]]

def ensure_table(con: duckdb.DuckDBPyConnection, s3_uri: str) -> str:
    """
    Ensure a table exists for the given S3 URI.
    Creates a table name based on the URI and loads the shapefile if needed.
    """
    # Create a table name from the S3 URI
    table_name = f"shapefile_{hash(s3_uri) % 1000000}"
    
    # Check if table exists
    result = con.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'").fetchone()
    
    if result is None:
        logger.info(f"Creating table {table_name} for S3 URI: {s3_uri}")
        
        # Load the shapefile into DuckDB
        try:
            # For URLs, download the file first
            if s3_uri.startswith('http'):
                logger.info(f"Downloading file from URL: {s3_uri}")
                
                # Download the file with SSL verification disabled for problematic servers
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                
                try:
                    # First try with SSL verification
                    response = requests.get(s3_uri, stream=True, headers=headers, timeout=30)
                    response.raise_for_status()
                except requests.exceptions.SSLError:
                    logger.warning(f"SSL verification failed for {s3_uri}, retrying without verification")
                    # Retry without SSL verification
                    response = requests.get(s3_uri, stream=True, headers=headers, verify=False, timeout=30)
                    response.raise_for_status()
                
                # Create a temporary file
                with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
                    for chunk in response.iter_content(chunk_size=8192):
                        temp_file.write(chunk)
                    temp_file_path = temp_file.name
                
                try:
                    # Check if it's a ZIP file and extract if needed
                    if zipfile.is_zipfile(temp_file_path):
                        logger.info(f"File is a ZIP archive, extracting...")
                        
                        # Create a temporary directory for extraction
                        with tempfile.TemporaryDirectory() as extract_dir:
                            with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
                                zip_ref.extractall(extract_dir)
                            
                            # Look for .shp files in the extracted directory
                            shp_files = []
                            for root, dirs, files in os.walk(extract_dir):
                                for file in files:
                                    if file.lower().endswith('.shp'):
                                        shp_files.append(os.path.join(root, file))
                            
                            if not shp_files:
                                raise HTTPException(status_code=400, detail="No shapefile (.shp) found in ZIP archive")
                            
                            # Use the first shapefile found
                            shapefile_path = shp_files[0]
                            logger.info(f"Found shapefile: {shapefile_path}")
                            
                            # Read the extracted shapefile with DuckDB
                            con.execute(f"""
                                CREATE TABLE {table_name} AS 
                                SELECT * FROM st_read('{shapefile_path}')
                            """)
                    else:
                        # Try to read the file directly (might be a direct shapefile)
                        con.execute(f"""
                            CREATE TABLE {table_name} AS 
                            SELECT * FROM st_read('{temp_file_path}')
                        """)
                    
                    logger.info(f"Successfully loaded shapefile from {s3_uri} into table {table_name}")
                finally:
                    # Clean up the temporary file
                    os.unlink(temp_file_path)
                    
            # For S3 URIs, use st_read with S3 configuration
            elif s3_uri.startswith('s3://'):
                con.execute(f"""
                    CREATE TABLE {table_name} AS 
                    SELECT * FROM st_read('{s3_uri}')
                """)
            else:
                # For local files
                con.execute(f"""
                    CREATE TABLE {table_name} AS 
                    SELECT * FROM st_read('{s3_uri}')
                """)
            
        except Exception as e:
            logger.error(f"Error loading shapefile from {s3_uri}: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Error loading shapefile: {str(e)}")
    
    return table_name

@router.get("/shapefiles")
async def shapefile_table(
    s3_uri: str = Query(..., description="Full S3 URI to *.shp or *.zip"),
    page: int = Query(1, ge=1, description="Page number"),
    size: int = Query(50, le=1000, description="Items per page"),
    sort: Optional[str] = Query(None, description="Column to sort by"),
    dir: str = Query("asc", pattern="^(asc|desc)$", description="Sort direction"),
    q: Optional[str] = Query(None, description="Free-text search"),
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    """
    Get shapefile data from S3, paginated and searchable.
    
    Args:
        s3_uri: Full S3 URI to the shapefile (.shp or .zip)
        page: Page number (1-based)
        size: Number of items per page (max 1000)
        sort: Column name to sort by
        dir: Sort direction (asc or desc)
        q: Free-text search query
        callback: JSONP callback name (optional)
    
    Returns:
        Paginated shapefile data with metadata
    """
    try:
        con = get_duckdb_connection()
        
        # Ensure the table exists and is loaded
        table_name = ensure_table(con, s3_uri)
        
        # Discover columns
        meta = con.execute(f"PRAGMA table_info('{table_name}')").df()
        cols = meta['name'].tolist()
        
        # Identify geometry columns (spatial extension uses different types)
        # Look for geometry columns by name and type
        geom_cols = []
        text_cols = []
        
        for _, row in meta.iterrows():
            col_name = row['name']
            col_type = row['type']
            
            # Geometry columns are typically named 'geometry' or have spatial types
            if (col_name.lower() in ['geometry', 'geom', 'shape', 'wkb_geometry'] or 
                col_type.upper() in ['BLOB', 'STRUCT', 'VARCHAR'] and 'geometry' in col_name.lower()):
                geom_cols.append(col_name)
            else:
                text_cols.append(col_name)
        
        # Default sort: first non-geometry column
        if sort is None:
            sort = text_cols[0] if text_cols else cols[0]
        
        if sort not in cols:
            raise HTTPException(status_code=400, detail=f"Unknown sort column: {sort}")
        
        # Build WHERE clause for search
        where = "TRUE"
        if q:
            # Build ILIKE search for all textual columns using proper DuckDB syntax
            ors = [f"{c}::TEXT ILIKE '%{q}%'" for c in text_cols]
            where = " OR ".join(ors) if ors else "TRUE"
        
        # Pagination
        limit = size
        offset = (page - 1) * size
        
        # Get total count
        total = con.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {where}").fetchone()[0]
        
        # Build and execute the main query
        sql = f"""
            SELECT *
            FROM {table_name}
            WHERE {where}
            ORDER BY "{sort}" {dir.upper()}
            LIMIT {limit} OFFSET {offset}
        """
        
        logger.info(f"Executing SQL: {sql}")
        df = con.execute(sql).fetch_df()
        
        # Remove geometry columns and convert to records
        if geom_cols:
            df = df.drop(columns=geom_cols)
        
        # Convert to records and handle null values
        rows = df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")
        
        # Create response data
        response_data = {
            "total_rows": total,
            "columns": [c for c in cols if c not in geom_cols],
            "rows": rows,
            "page": page,
            "size": size,
            "total_pages": (total + size - 1) // size,
            "s3_uri": s3_uri,
            "table_name": table_name
        }
        
        # Sanitize for JSON serialization
        response_data = sanitize_for_json(response_data)
        
        return create_response(response_data, callback)
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error processing shapefile request: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    finally:
        if 'con' in locals():
            con.close()

@router.get("/shapefiles/{table_name}/info")
async def shapefile_info(
    table_name: str,
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    """
    Get metadata about a specific shapefile table.
    
    Args:
        table_name: The table name to get info for
        callback: JSONP callback name (optional)
    
    Returns:
        Table metadata including columns, row count, and sample data
    """
    try:
        con = get_duckdb_connection()
        
        # Check if table exists
        result = con.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'").fetchone()
        if result is None:
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found")
        
        # Get table info
        meta = con.execute(f"PRAGMA table_info('{table_name}')").df()
        cols = meta['name'].tolist()
        
        # Get row count
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        # Get sample data (first 5 rows)
        sample = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetch_df()
        
        # Create response
        response_data = {
            "table_name": table_name,
            "columns": cols,
            "total_rows": count,
            "sample_data": sample.to_dict(orient="records")
        }
        
        # Sanitize for JSON serialization
        response_data = sanitize_for_json(response_data)
        
        return create_response(response_data, callback)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting shapefile info: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    finally:
        if 'con' in locals():
            con.close()

@router.get("/shapefiles/tables")
async def list_shapefile_tables(
    callback: Optional[str] = Query(None, description="JSONP callback name"),
):
    """
    List all available shapefile tables.
    
    Args:
        callback: JSONP callback name (optional)
    
    Returns:
        List of all shapefile tables with their metadata
    """
    try:
        con = get_duckdb_connection()
        
        # Get all tables that start with 'shapefile_'
        tables = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'shapefile_%'").fetchall()
        
        table_info = []
        for (table_name,) in tables:
            # Get row count for each table
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            table_info.append({
                "table_name": table_name,
                "row_count": count
            })
        
        response_data = {
            "tables": table_info,
            "total_tables": len(table_info)
        }
        
        # Sanitize for JSON serialization
        response_data = sanitize_for_json(response_data)
        
        return create_response(response_data, callback)
        
    except Exception as e:
        logger.error(f"Error listing shapefile tables: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    finally:
        if 'con' in locals():
            con.close() 