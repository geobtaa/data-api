#!/usr/bin/env python
"""
Script to generate and store embeddings for FAST gazetteer data.

This script uses OpenAI's API to generate embeddings for FAST gazetteer entries
and stores them in the database. It processes records in batches to manage memory
and API usage efficiently.

Environment Variables:
    OPENAI_API_KEY: Required API key for OpenAI
    LOG_PATH: Optional path for log files

Usage:
    python scripts/generate_fast_embeddings.py
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List

from dotenv import load_dotenv
from openai import OpenAI
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.sql import text

# Add the project root directory to Python path to allow importing app modules
sys.path.append(str(Path(__file__).parent.parent))

from db.config import DATABASE_URL
from db.models import gazetteer_fast, metadata

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define batch size for processing records
BATCH_SIZE = 100


def get_embeddings(texts: List[str], model: str = "text-embedding-3-small") -> List[List[float]]:
    """
    Generate embeddings for a list of texts using OpenAI's API.

    Args:
        texts: List of text strings to generate embeddings for
        model: OpenAI model to use (default: text-embedding-3-small)

    Returns:
        List of embedding vectors for each input text

    Raises:
        Exception: If there's an error calling the OpenAI API
    """
    try:
        # Create OpenAI client with explicit configuration
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"), base_url="https://api.openai.com/v1")

        response = client.embeddings.create(model=model, input=texts, encoding_format="float")
        return [embedding.embedding for embedding in response.data]
    except Exception as e:
        logger.error(f"Error generating embeddings: {e}")
        raise


async def process_batch(
    conn: AsyncSession, fast_records: List[dict], embeddings: List[List[float]]
):
    """
    Process a batch of records and their embeddings, inserting them into the database.

    Args:
        conn: Database connection
        fast_records: List of FAST gazetteer records
        embeddings: List of embedding vectors corresponding to the records

    Raises:
        Exception: If there's an error inserting the records
    """
    try:
        # Convert embeddings to PostgreSQL array format
        embeddings_str = [f"[{','.join(map(str, emb))}]" for emb in embeddings]

        # Prepare parameters for database insertion
        params = [
            {
                "fast_id": record["fast_id"],
                "label": record["label"],
                "geoname_id": record["geoname_id"],
                "viaf_id": record["viaf_id"],
                "wikipedia_id": record["wikipedia_id"],
                "embeddings": emb_str,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            }
            for record, emb_str in zip(fast_records, embeddings_str)
        ]

        # Prepare SQL insert statement
        insert_stmt = text("""
            INSERT INTO gazetteer_fast_embeddings 
            (fast_id, label, geoname_id, viaf_id, wikipedia_id, embeddings, created_at, updated_at)
            VALUES (:fast_id, :label, :geoname_id, :viaf_id, :wikipedia_id, :embeddings, :created_at, :updated_at)
        """)

        # Insert records with their embeddings
        for param in params:
            await conn.execute(insert_stmt, param)

        await conn.commit()
        logger.info(f"Successfully inserted batch of {len(fast_records)} records")

    except Exception as e:
        logger.error(f"Error inserting batch: {e}")
        raise


async def generate_and_insert_embeddings():
    """
    Generate embeddings for all FAST gazetteer data and insert them into the database.

    This function:
    1. Creates database tables if they don't exist
    2. Fetches all FAST records in batches
    3. Generates embeddings for each batch
    4. Inserts the records with their embeddings
    5. Tracks and logs progress

    Raises:
        Exception: If there's an error during the process
    """
    try:
        # Create async database engine
        engine = create_async_engine(DATABASE_URL)

        # Create tables if they don't exist
        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)

        async with engine.connect() as conn:
            # Get total count of records
            count_stmt = select(func.count()).select_from(gazetteer_fast)
            result = await conn.execute(count_stmt)
            total_records = result.scalar()
            logger.info(f"Total records to process: {total_records}")

            # Process records in batches
            offset = 0
            while offset < total_records:
                # Fetch a batch of records
                stmt = select(gazetteer_fast).offset(offset).limit(BATCH_SIZE)
                result = await conn.execute(stmt)
                batch = result.fetchall()

                if not batch:
                    break

                # Convert batch to list of dictionaries
                batch_records = [row._asdict() for row in batch]

                # Generate embeddings for the labels
                texts = [record["label"] for record in batch_records]
                embeddings = get_embeddings(texts)

                # Insert the records with their embeddings
                await process_batch(conn, batch_records, embeddings)

                offset += BATCH_SIZE
                logger.info(f"Processed {offset} of {total_records} records")

    except Exception as e:
        logger.error(f"Error in generate_and_insert_embeddings: {e}")
        raise
    finally:
        await engine.dispose()


if __name__ == "__main__":
    # Verify OpenAI API key is set
    if not os.getenv("OPENAI_API_KEY"):
        raise ValueError("OPENAI_API_KEY not found in environment variables")

    # Run the embedding generation process
    import asyncio

    asyncio.run(generate_and_insert_embeddings())
