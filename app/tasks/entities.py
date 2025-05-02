import asyncio
import logging
from datetime import datetime
from typing import Any, Dict

from celery import shared_task
from dotenv import load_dotenv
from sqlalchemy import insert

from app.services.llm_service import LLMService
from db.database import database
from db.models import ai_enrichments

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


@shared_task(
    soft_time_limit=180,  # 3 minutes
    time_limit=240,  # 4 minutes
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 2, "countdown": 5},
    retry_backoff=True,
)
def generate_geo_entities(item_id: str, metadata: Dict[str, Any]):
    """
    Celery task to identify geographic entities in document metadata.

    Args:
        item_id: The document ID
        metadata: Document metadata dictionary
    """
    # Create an event loop for the async function
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # Run the async function
        return loop.run_until_complete(_identify_geo_entities(item_id, metadata))
    finally:
        # Clean up
        loop.close()


async def store_geo_entities_in_db(
    document_id: str,
    model: str,
    entities: Dict[str, Any],
    prompt: Dict[str, Any],
    output_parser: Dict[str, Any],
):
    """
    Store the identified geographic entities in the ai_enrichments table.

    Args:
        document_id: The ID of the document
        model: The model used for identification
        entities: The identified geographic entities
        prompt: The prompt used for generation
        output_parser: The output parser configuration
    """
    try:
        # Ensure database is connected
        if not database.is_connected:
            await database.connect()

        # Prepare the data for insertion
        now = datetime.utcnow()

        # Create a structured response object with ISO format timestamps
        response_data = {"entities": entities, "timestamp": now.isoformat()}

        # Create the enrichment record
        enrichment_data = {
            "document_id": document_id,
            "enrichment_type": "geo_entities",
            "ai_provider": "OpenAI",
            "model": model,
            "prompt": prompt,
            "output_parser": output_parser,
            "response": response_data,
            "created_at": now,
            "updated_at": now,
        }

        # Insert the record into the database
        async with database.transaction():
            query = insert(ai_enrichments).values(**enrichment_data)
            await database.execute(query)

        logger.info(f"Stored geographic entities for document {document_id} in the database")

    except Exception as e:
        logger.error(f"Error storing geographic entities in database: {str(e)}")
        raise
    finally:
        # Clean up database connection
        if database.is_connected:
            await database.disconnect()


async def _identify_geo_entities(item_id: str, metadata: Dict[str, Any]):
    """
    Async implementation of geographic entity identification.
    """
    try:
        logger.info(f"Starting geographic entity identification for document {item_id}")

        # Initialize LLM service
        llm_service = LLMService()

        # Combine all metadata fields into a single text for analysis
        # Skip any fields that are None or empty strings
        text_content = []
        for key, value in metadata.items():
            if value is not None and str(value).strip():
                text_content.append(f"{key}: {value}")

        if not text_content:
            logger.warning(f"No metadata content found for document {item_id}")
            return

        combined_text = "\n".join(text_content)

        # Use LLM service to identify geographic entities
        entities, prompt, output_parser = await llm_service.identify_geo_entities(combined_text)

        # Store results in database
        await store_geo_entities_in_db(item_id, llm_service.model, entities, prompt, output_parser)

        logger.info(f"Completed geographic entity identification for document {item_id}")
        return entities

    except Exception as e:
        logger.error(f"Error in geographic entity identification for document {item_id}: {str(e)}")
        raise
