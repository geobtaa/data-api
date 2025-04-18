import asyncio
import logging
import os
import sys
from pathlib import Path

from databases import Database

# Add the project root directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

from db.config import ASYNC_DATABASE_URL

# Setup logging
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

# Relationship mappings with their inverses
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
    """Populate the document_relationships table."""
    try:
        # Connect to database
        if not database.is_connected:
            await database.connect()

        # First, clear existing relationships
        logger.info("Clearing existing relationships...")
        await database.execute("TRUNCATE TABLE document_relationships")

        # Get all documents
        logger.info("Fetching documents...")
        query = """
            SELECT id, dct_relation_sm, pcdm_memberof_sm, dct_ispartof_sm, 
                   dct_source_sm, dct_isversionof_sm, dct_replaces_sm, 
                   dct_isreplacedby_sm 
            FROM geoblacklight_development
        """
        documents = await database.fetch_all(query)
        logger.info(f"Processing {len(documents)} documents...")

        # Process each document
        relationship_count = 0
        for doc in documents:
            for field, (predicate, inverse_predicate) in RELATIONSHIP_MAPPINGS.items():
                values = getattr(doc, field, None)

                if not values:
                    continue

                # Ensure values is a list
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
        if database.is_connected:
            await database.disconnect()


if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    asyncio.run(populate_relationships())
