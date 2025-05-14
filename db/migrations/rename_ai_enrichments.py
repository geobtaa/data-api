import logging
import sys
from pathlib import Path

from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import text

# Add the project root directory to Python path
sys.path.append(str(Path(__file__).parent.parent.parent))

from db.config import DATABASE_URL

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def rename_ai_enrichments_table():
    """Rename the ai_enrichments table to item_ai_enrichments."""
    try:
        # Create engine and inspector
        engine = create_engine(DATABASE_URL)
        inspector = inspect(engine)

        # Check if old table exists and new table doesn't
        if inspector.has_table("ai_enrichments") and not inspector.has_table("item_ai_enrichments"):
            with engine.connect() as conn:
                # Rename the table
                conn.execute(text("ALTER TABLE ai_enrichments RENAME TO item_ai_enrichments"))
                conn.commit()
                logger.info("Renamed ai_enrichments table to item_ai_enrichments")
        else:
            if not inspector.has_table("ai_enrichments"):
                logger.info("ai_enrichments table does not exist")
            if inspector.has_table("item_ai_enrichments"):
                logger.info("item_ai_enrichments table already exists")

    except Exception as e:
        logger.error(f"Error renaming ai_enrichments table: {e}")
        raise


if __name__ == "__main__":
    rename_ai_enrichments_table() 