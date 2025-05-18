#!/usr/bin/env python3
import os
import sys
from pathlib import Path

# Add the project root directory to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

import argparse
import logging
import asyncio

import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm

from app.elasticsearch import es
from app.elasticsearch.index import index_items
from db.models import items, item_relationships
from db.database import database

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_fixture_files():
    """Get list of CSV files in the fixtures directory."""
    fixtures_dir = Path("data/fixtures")
    if not fixtures_dir.exists():
        logger.error("Fixtures directory not found: data/fixtures")
        sys.exit(1)
    
    csv_files = list(fixtures_dir.glob("*.csv"))
    if not csv_files:
        logger.error("No CSV files found in data/fixtures")
        sys.exit(1)
    
    return csv_files

def select_fixture_file():
    """Show interactive list of CSV files and let user select one."""
    csv_files = get_fixture_files()
    
    print("\nAvailable fixture files:")
    for i, file in enumerate(csv_files, 1):
        size_mb = file.stat().st_size / (1024 * 1024)
        print(f"{i}. {file.name} ({size_mb:.1f} MB)")
    
    while True:
        try:
            choice = int(input("\nSelect a file number: "))
            if 1 <= choice <= len(csv_files):
                return csv_files[choice - 1]
            print(f"Please enter a number between 1 and {len(csv_files)}")
        except ValueError:
            print("Please enter a valid number")

def truncate_items_table(engine):
    """Truncate the items table."""
    logger.info("Truncating items table...")
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE items CASCADE"))
        conn.commit()

def load_csv_data(csv_path, engine):
    """Load data from CSV file into items table."""
    logger.info(f"Loading data from {csv_path}...")
    
    # Read CSV in chunks to handle large files
    chunk_size = 1000
    total_rows = sum(1 for _ in open(csv_path)) - 1  # Subtract header row
    skipped_records = 0
    
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size), total=total_rows//chunk_size + 1):
        # Convert DataFrame to list of dictionaries
        records = chunk.to_dict('records')
        
        # Insert records into database
        with engine.connect() as conn:
            for record in records:
                try:
                    # Skip records with missing required fields
                    if not record.get('id'):
                        logger.warning(f"Skipping record with missing ID: {record.get('dct_title_s', 'Unknown Title')}")
                        skipped_records += 1
                        continue
                    
                    # Process each field
                    for key, value in record.items():
                        if pd.isna(value):
                            record[key] = None
                        elif key.endswith('_sm'):
                            # Split multivalued string fields on pipe
                            record[key] = [v.strip() for v in str(value).split('|')]
                        elif key == 'gbl_indexyear_im':
                            # Handle integer array field
                            if isinstance(value, str):
                                # Try to parse as integer if it's a single value
                                try:
                                    record[key] = [int(value)]
                                except ValueError:
                                    # If it contains pipes, split and convert each part
                                    record[key] = [int(v.strip()) for v in value.split('|') if v.strip().isdigit()]
                            elif isinstance(value, (list, tuple)):
                                # If it's already a list, convert each item to int
                                record[key] = [int(v) for v in value if str(v).strip().isdigit()]
                            else:
                                record[key] = None
                        elif key == 'gbl_daterange_drsim':
                            # Handle date range field
                            if isinstance(value, str):
                                record[key] = [value.strip()]
                            elif isinstance(value, (list, tuple)):
                                record[key] = [str(v).strip() for v in value]
                            else:
                                record[key] = None
                        else:
                            # Keep other fields as is
                            record[key] = value
                    
                    conn.execute(items.insert(), record)
                    conn.commit()
                    
                except Exception as e:
                    logger.warning(f"Error processing record: {str(e)}")
                    logger.warning(f"Problematic record: {record.get('dct_title_s', 'Unknown Title')}")
                    skipped_records += 1
                    continue
    
    if skipped_records > 0:
        logger.warning(f"Skipped {skipped_records} records due to errors")
    logger.info(f"Successfully loaded {total_rows - skipped_records} records")

def rebuild_relationships(engine):
    """Rebuild item relationships."""
    logger.info("Rebuilding item relationships...")
    
    # First, clear existing relationships
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE item_relationships"))
        conn.commit()
    
    # SQL to rebuild relationships based on dct_relation_sm field
    relationship_sql = """
    WITH RECURSIVE split_relations AS (
        SELECT DISTINCT  -- Add DISTINCT to avoid duplicates
            id,
            unnest(dct_relation_sm) as relation
        FROM items
        WHERE dct_relation_sm IS NOT NULL
    )
    INSERT INTO item_relationships (subject_id, predicate, object_id)
    SELECT DISTINCT  -- Add DISTINCT to avoid duplicates
        id as subject_id,
        'dct:relation' as predicate,
        relation as object_id
    FROM split_relations;
    """
    
    with engine.connect() as conn:
        conn.execute(text(relationship_sql))
        conn.commit()
    
    logger.info("Relationships rebuilt successfully")

async def async_reindex():
    """Async function to handle reindexing."""
    try:
        # Connect to database
        await database.connect()
        logger.info("Connected to database")
        
        # Run the indexing
        result = await index_items()
        logger.info(f"Indexing result: {result}")
        
    finally:
        # Always disconnect from database
        await database.disconnect()
        logger.info("Disconnected from database")

def reindex_elasticsearch():
    """Reindex Elasticsearch."""
    logger.info("Reindexing Elasticsearch...")
    asyncio.run(async_reindex())
    logger.info("Elasticsearch reindexing completed")

def main():
    # Create database engine
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL environment variable not set")
        sys.exit(1)
    
    # Convert async URL to sync URL
    if database_url.startswith("postgresql+asyncpg://"):
        database_url = database_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    
    engine = create_engine(database_url)

    try:
        # Let user select a CSV file
        csv_path = select_fixture_file()
        
        # Confirm with user
        print(f"\nSelected file: {csv_path.name}")
        confirm = input("Proceed with loading this file? (y/N): ").lower()
        if confirm != 'y':
            print("Operation cancelled")
            sys.exit(0)

        # Truncate items table
        truncate_items_table(engine)

        # Load CSV data
        load_csv_data(csv_path, engine)

        # Rebuild relationships
        rebuild_relationships(engine)

        # Reindex Elasticsearch
        reindex_elasticsearch()

        logger.info("Fixture loading completed successfully!")

    except Exception as e:
        logger.error(f"Error loading fixtures: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 