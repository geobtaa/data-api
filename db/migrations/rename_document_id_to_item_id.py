import logging
import sys
from pathlib import Path

from sqlalchemy import create_engine, text

# Add the project root directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

from db.config import DATABASE_URL

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def rename_document_id_to_item_id():
    """Rename the 'document_id' column to 'item_id' in the item_ai_enrichments table."""
    try:
        # Create engine
        engine = create_engine(DATABASE_URL)

        with engine.connect() as conn:
            # Check if the column 'document_id' exists
            result = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'item_ai_enrichments' AND column_name = 'document_id';
            """))
            if result.fetchone():
                # Rename the column
                conn.execute(text("""
                    ALTER TABLE item_ai_enrichments RENAME COLUMN document_id TO item_id;
                """))
                conn.commit()
                logger.info("Successfully renamed 'document_id' to 'item_id' in item_ai_enrichments table.")
            else:
                logger.info("Column 'document_id' does not exist in item_ai_enrichments table. Skipping migration.")

    except Exception as e:
        logger.error(f"Error renaming column: {e}")
        raise


if __name__ == "__main__":
    rename_document_id_to_item_id() 