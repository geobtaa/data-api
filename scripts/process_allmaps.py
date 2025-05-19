import argparse
import asyncio
import hashlib
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiohttp
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from db.models import item_allmaps, items

# Add the project root directory to Python path
sys.path.append(str(Path(__file__).parent))

# Set the correct database URL for local scripts
DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:2345/btaa_ogm_api"

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Create async engine and session
engine = create_async_engine(DATABASE_URL)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def generate_allmaps_id(manifest: str) -> str:
    """Generate an Allmaps ID from a IIIF manifest.

    Args:
        manifest: The IIIF manifest JSON as a string

    Returns:
        str: The generated Allmaps ID (first 16 chars of SHA-1 hash)
    """
    try:
        # Parse the manifest JSON
        manifest_json = json.loads(manifest)

        # Get the manifest ID from either @id or id field
        manifest_id = manifest_json.get("@id") or manifest_json.get("id")

        if not manifest_id:
            logger.error("No manifest ID found in IIIF manifest")
            return None

        # Use SHA-1 and take first 16 characters
        hash_object = hashlib.sha1(manifest_id.encode("utf-8"))
        return hash_object.hexdigest()[:16]

    except json.JSONDecodeError:
        logger.error("Invalid JSON in IIIF manifest")
        return None
    except Exception as e:
        logger.error(f"Error generating Allmaps ID: {e}")
        return None


async def fetch_manifest(session: aiohttp.ClientSession, manifest_url: str) -> Optional[str]:
    """Fetch and validate a IIIF manifest."""
    try:
        async with session.get(manifest_url) as response:
            if response.status == 200:
                manifest = await response.text()
                # Basic validation that it's a JSON object
                try:
                    manifest_json = json.loads(manifest)
                    # Check for required IIIF manifest fields
                    if not isinstance(manifest_json, dict):
                        raise ValueError("Manifest must be a JSON object")
                    if "@context" not in manifest_json:
                        raise ValueError("Manifest missing @context")
                    if "@type" not in manifest_json:
                        raise ValueError("Manifest missing @type")
                    if manifest_json["@type"] != "sc:Manifest":
                        raise ValueError("Not a valid IIIF manifest")
                    return manifest
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON response for manifest {manifest_url}")
                    return None
                except ValueError as e:
                    logger.error(f"Invalid IIIF manifest {manifest_url}: {e}")
                    return None
            else:
                logger.warning(
                    f"Failed to fetch manifest {manifest_url} (status: {response.status})"
                )
                return None
    except Exception as e:
        logger.error(f"Error fetching manifest {manifest_url}: {e}")
        return None


async def check_allmaps_annotation(
    session: aiohttp.ClientSession, manifest_url: str
) -> Optional[str]:
    """Check if Allmaps has an annotation for the given manifest URL."""
    annotation_url = f"https://annotations.allmaps.org/?url={manifest_url}"

    try:
        async with session.get(annotation_url, allow_redirects=True) as response:
            if response.status == 200:
                annotation = await response.text()
                # Basic validation that it's a JSON object
                try:
                    json.loads(annotation)
                    return annotation
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON response from Allmaps for {manifest_url}")
                    return None
            else:
                logger.warning(
                    f"No Allmaps annotation found for {manifest_url} (status: {response.status})"
                )
                return None
    except Exception as e:
        logger.error(f"Error checking Allmaps annotation for {manifest_url}: {e}")
        return None


async def process_item(
    item: any, session: AsyncSession, http_session: aiohttp.ClientSession
) -> bool:
    """Process a single item and store its Allmaps data.

    Args:
        item: The item to process
        session: SQLAlchemy async database session
        http_session: aiohttp client session

    Returns:
        bool: True if the item was processed successfully, False otherwise
    """
    try:
        # Parse references JSON
        references = json.loads(item.dct_references_s)
        manifest_url = references.get("http://iiif.io/api/presentation#manifest")

        if not manifest_url:
            logger.error(f"Item {item.id} has no IIIF manifest URL")
            return False

        # Fetch manifest and check for Allmaps annotation concurrently
        manifest_task = fetch_manifest(http_session, manifest_url)
        annotation_task = check_allmaps_annotation(http_session, manifest_url)

        manifest, annotation = await asyncio.gather(manifest_task, annotation_task)

        if not manifest:
            logger.warning(f"Could not fetch manifest for {item.id}. Skipping.")
            return False

        # Generate Allmaps ID from the manifest
        allmaps_id = generate_allmaps_id(manifest)
        if not allmaps_id:
            logger.error(f"Could not generate Allmaps ID for {item.id}. Skipping.")
            return False

        # Delete any existing record for this item
        await session.execute(item_allmaps.delete().where(item_allmaps.c.item_id == item.id))
        await session.commit()

        # Create new item_allmaps record
        now = datetime.now()
        new_record = {
            "item_id": item.id,
            "allmaps_id": allmaps_id,
            "iiif_manifest_uri": manifest_url,
            "annotated": bool(annotation),
            "iiif_manifest": manifest,
            "allmaps_annotation": annotation,
            "created_at": now,
            "updated_at": now,
        }

        await session.execute(item_allmaps.insert(), new_record)
        await session.commit()

        logger.info(f"Processed item {item.id} - Annotated: {bool(annotation)}")
        return True

    except Exception as e:
        logger.error(f"Error processing item {item.id}: {e}")
        await session.rollback()
        return False


async def process_single_item(item_id: str) -> None:
    """Process a single item by its ID."""
    async with async_session() as session:
        # Query the specific item
        query = select(items).where(items.c.id == item_id)
        result = await session.execute(query)
        item = result.first()

        if not item:
            logger.error(f"Item {item_id} not found")
            return

        async with aiohttp.ClientSession() as http_session:
            await process_item(item, session, http_session)


async def process_all_items() -> None:
    """Process all items with IIIF manifests."""
    async with async_session() as session:
        # Query items that have IIIF manifest references
        query = select(items).where(
            items.c.dct_references_s.like('%"http://iiif.io/api/presentation#manifest"%')
        )
        results = await session.execute(query)

        async with aiohttp.ClientSession() as http_session:
            for item in results:
                await process_item(item, session, http_session)


async def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Process IIIF manifests and Allmaps annotations")
    parser.add_argument("--item-id", help="Process a single item by ID")
    args = parser.parse_args()

    if args.item_id:
        await process_single_item(args.item_id)
    else:
        await process_all_items()


if __name__ == "__main__":
    asyncio.run(main())
