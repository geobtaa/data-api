#!/usr/bin/env python
"""
Script to populate document relationships in the database.

This script processes document relationships from the geoblacklight_development table
and populates the document_relationships table with both primary and inverse relationships.
It handles various types of relationships like isPartOf, hasMember, isVersionOf, etc.

Environment Variables:
    LOG_PATH: Optional path for log files (default: logs)

Usage:
    python scripts/populate_relationships.py
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

from databases import Database

# Add the project root directory to Python path to allow importing app modules
sys.path.append(str(Path(__file__).parent.parent))

from db.config import ASYNC_DATABASE_URL

# Setup logging with both console and file output
log_path = os.getenv("LOG_PATH", "logs")
os.makedirs(log_path, exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Print to console
        logging.FileHandler(f"{log_path}/relationships.log"),  # Write to file
    ],
)
logger = logging.getLogger(__name__)

# Create database instance
database = Database(ASYNC_DATABASE_URL)

# Define relationship mappings with their inverse relationships
RELATIONSHIP_MAPPINGS = {
    "dct_relation_sm": ("relation", "relation"),  # Bidirectional
    "pcdm_memberof_sm": ("memberOf", "hasMember"),
    "dct_ispartof_sm": ("isPartOf", "hasPart"),
    "dct_source_sm": ("source", "isSourceOf"),
    "dct_isversionof_sm": ("isVersionOf", "hasVersion"),
    "dct_replaces_sm": ("replaces", "isReplacedBy"),
    "dct_isreplacedby_sm": ("isReplacedBy", "replaces"),
}


async def populate_relationships():
    """
    Populate the document_relationships table with relationships from geoblacklight_development.
    
    This function:
    1. Connects to the database
    2. Clears existing relationships
    3. Fetches all documents with relationship fields
    4. Processes each document and creates both primary and inverse relationships
    5. Tracks and logs the total number of relationships created
    
    Raises:
        Exception: If there's an error during the population process
    """
    try:
        # Connect to database if not already connected
        if not database.is_connected:
            await database.connect()

        # Clear existing relationships to ensure clean state
        logger.info("Clearing existing relationships...")
        await database.execute("TRUNCATE TABLE document_relationships")

        # Fetch all documents with their relationship fields
        logger.info("Fetching documents...")
        query = """
            SELECT id, dct_relation_sm, pcdm_memberof_sm, dct_ispartof_sm, 
                   dct_source_sm, dct_isversionof_sm, dct_replaces_sm, 
                   dct_isreplacedby_sm 
            FROM geoblacklight_development
        """
        documents = await database.fetch_all(query)
        logger.info(f"Processing {len(documents)} documents...")

        # Process each document and create relationships
        relationship_count = 0
        for doc in documents:
            for field, (predicate, inverse_predicate) in RELATIONSHIP_MAPPINGS.items():
                values = getattr(doc, field, None)

                if not values:
                    continue

                # Handle both single values and lists
                if isinstance(values, str):
                    values = [values]

                # Create relationships for each value
                for target_id in values:
                    # Insert the primary relationship
                    await database.execute(
                        """
                        INSERT INTO document_relationships (subject_id, predicate, object_id) 
                        VALUES (:subject, :predicate, :object)
                        """,
                        {"subject": doc.id, "predicate": predicate, "object": target_id},
                    )

                    # Insert the inverse relationship
                    await database.execute(
                        """
                        INSERT INTO document_relationships (subject_id, predicate, object_id) 
                        VALUES (:subject, :predicate, :object)
                        """,
                        {"subject": target_id, "predicate": inverse_predicate, "object": doc.id},
                    )
                    relationship_count += 2

        logger.info(f"Created {relationship_count} relationships")

    except Exception as e:
        logger.error(f"Error populating relationships: {e}")
        raise
    finally:
        # Ensure database connection is closed
        if database.is_connected:
            await database.disconnect()


if __name__ == "__main__":
    # Set logging level and run the population process
    logger.setLevel(logging.DEBUG)
    asyncio.run(populate_relationships())
