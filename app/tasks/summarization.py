import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, Optional

from celery import shared_task
from dotenv import load_dotenv
from sqlalchemy import insert

# Load environment variables from .env file
load_dotenv()

from app.services.llm_service import LLMService
from db.database import database

logger = logging.getLogger(__name__)


# Custom JSON encoder to handle datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


@shared_task(
    soft_time_limit=180,  # 3 minutes
    time_limit=240,  # 4 minutes
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 2, "countdown": 5},
    retry_backoff=True,
)
def generate_item_summary(
    item_id: str,
    metadata: Dict[str, Any],
    asset_path: Optional[str] = None,
    asset_type: Optional[str] = None,
) -> str:
    """
    Celery task to generate a summary for an item.

    Args:
        item_id: The ID of the item
        metadata: Dictionary containing the item's metadata
        asset_path: Optional path to the asset file
        asset_type: Optional type of the asset (e.g., 'image', 'shapefile', 'pdf')

    Returns:
        str: The generated summary
    """
    # Create an event loop for the async function
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # Run the async function
        summary = loop.run_until_complete(
            _generate_summary(item_id, metadata, asset_path, asset_type)
        )
        return summary
    except Exception as e:
        logger.error(f"Error generating summary for item {item_id}: {str(e)}")
        raise
    finally:
        # Clean up the event loop
        loop.close()


async def _generate_summary(
    item_id: str,
    metadata: Dict[str, Any],
    asset_path: Optional[str] = None,
    asset_type: Optional[str] = None,
) -> str:
    """Async helper function to generate the summary."""
    try:
        # Ensure database is connected
        if not database.is_connected:
            await database.connect()

        logger.info(f"Starting summary generation for item {item_id}")

        # Check if OpenAI API key is available
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OpenAI API key is required. Set OPENAI_API_KEY environment variable.")

        llm_service = LLMService(api_key=api_key)
        logger.info(f"Created LLM service with model {llm_service.model}")

        # Process the asset if provided
        asset_content = None
        if asset_path and asset_type:
            logger.info(f"Processing asset {asset_path} of type {asset_type}")
            asset_content = await llm_service.process_asset(asset_path, asset_type)
            logger.info(f"Asset content processed: {bool(asset_content)}")

        # Convert metadata to JSON-serializable format using our custom encoder
        logger.info("Converting metadata to JSON-serializable format")
        try:
            serializable_metadata = json.loads(json.dumps(metadata, cls=DateTimeEncoder))
        except TypeError as e:
            logger.error(f"Error serializing metadata: {str(e)}")
            # If serialization fails, try to convert datetime objects manually
            serializable_metadata = {}
            for key, value in metadata.items():
                if isinstance(value, datetime):
                    serializable_metadata[key] = value.isoformat()
                else:
                    serializable_metadata[key] = value

        # Generate the summary
        logger.info("Generating summary using LLM service")
        start_time = time.time()
        summary, prompt, output_parser = await llm_service.generate_summary(
            serializable_metadata, asset_content
        )
        end_time = time.time()
        logger.info(
            f"Summary generated in {end_time - start_time:.2f} seconds, length: {len(summary)}"
        )

        # Store the summary in the database
        logger.info("Storing summary in database")
        await store_summary_in_db(
            item_id=item_id,
            model=llm_service.model,
            summary=summary,
            prompt=prompt,
            output_parser=output_parser,
        )
        logger.info("Summary stored in database")

        return summary

    except Exception as e:
        logger.error(f"Error in _generate_summary for item {item_id}: {str(e)}")
        logger.exception("Full traceback:")
        raise
    finally:
        # Clean up database connection
        if database.is_connected:
            await database.disconnect()


async def store_summary_in_db(
    item_id: str,
    model: str,
    summary: str,
    prompt: Dict[str, Any],
    output_parser: Dict[str, Any],
):
    """
    Store the generated summary in the ai_enrichments table.

    Args:
        item_id: The ID of the item
        model: The model used for generation
        summary: The generated summary
        prompt: The prompt used for generation
        output_parser: The output parser configuration
    """
    try:
        # Prepare the data for insertion
        now = datetime.utcnow()

        # Create a structured response object with ISO format timestamps
        response_data = {"summary": summary, "timestamp": now.isoformat()}

        # Ensure prompt and output_parser are JSON serializable
        try:
            serializable_prompt = json.loads(json.dumps(prompt, cls=DateTimeEncoder))
            serializable_parser = json.loads(json.dumps(output_parser, cls=DateTimeEncoder))
        except TypeError as e:
            logger.error(f"Error serializing prompt or parser: {str(e)}")
            serializable_prompt = prompt
            serializable_parser = output_parser

        # Create the enrichment record with ISO format timestamps
        enrichment_data = {
            "item_id": item_id,
            "ai_provider": "OpenAI",
            "model": model,
            "enrichment_type": "summarization",
            "prompt": serializable_prompt,
            "output_parser": serializable_parser,
            "response": response_data,
            "created_at": now,
            "updated_at": now,
        }

        # Insert the record into the database
        async with database.transaction():
            query = insert(ai_enrichments).values(**enrichment_data)
            await database.execute(query)

        logger.info(f"Stored summary for item {item_id} in the database")

    except Exception as e:
        logger.error(f"Error storing summary in database: {str(e)}")
        raise
