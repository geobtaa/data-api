import logging
from typing import Dict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import item_allmaps

logger = logging.getLogger(__name__)


class AllmapsService:
    """Service for handling Allmaps data and annotations."""

    def __init__(self, item: Dict):
        """Initialize the service with an item dictionary."""
        self.item = item
        # Handle both direct id and nested id in attributes
        self.item_id = str(item.get("id") or item.get("attributes", {}).get("id"))
        if not self.item_id:
            logger.warning(f"No item ID found in item data: {item}")
        else:
            logger.info(f"Initialized AllmapsService for item {self.item_id}")

    async def get_allmaps_attributes(self, session: AsyncSession) -> Dict:
        """Get Allmaps attributes for the item.

        Args:
            session: SQLAlchemy async database session

        Returns:
            Dict containing Allmaps attributes if found, empty dict otherwise
        """
        if not self.item_id:
            logger.warning("Cannot get Allmaps attributes: No item ID available")
            return {}

        try:
            # Query the item_allmaps table
            query = select(item_allmaps).where(item_allmaps.c.item_id == self.item_id)
            logger.info(f"Executing query for item {self.item_id}: {query}")

            result = await session.execute(query)
            row = result.fetchone()

            if not row:
                logger.info(f"No Allmaps data found for item {self.item_id}")
                return {}

            # Convert to dict and extract relevant fields
            allmaps_dict = dict(row._mapping)
            logger.info(f"Found Allmaps data for item {self.item_id}: {allmaps_dict}")

            attributes = {
                "ui_allmaps_id": allmaps_dict.get("allmaps_id"),
                "ui_allmaps_annotated": allmaps_dict.get("annotated"),
                "ui_allmaps_manifest_uri": allmaps_dict.get("iiif_manifest_uri"),
            }
            logger.info(f"Returning Allmaps attributes: {attributes}")
            return attributes

        except Exception as e:
            logger.error(
                f"Error getting Allmaps attributes for item {self.item_id}: {e}", exc_info=True
            )
            return {}
