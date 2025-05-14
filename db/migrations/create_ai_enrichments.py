import logging
import sys
from pathlib import Path

from sqlalchemy import create_engine, inspect, text

# Add the project root directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

from db.config import DATABASE_URL
from db.models import item_ai_enrichments as ai_enrichments

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_ai_enrichments_table():
    """Create the ai_enrichments table."""
    try:
        # Create engine
        engine = create_engine(DATABASE_URL)
        inspector = inspect(engine)

        # Check if the table already exists
        if inspector.has_table("item_ai_enrichments"):
            logger.info("Table item_ai_enrichments already exists. Skipping creation.")
            return

        with engine.connect() as conn:
            # Drop the index if it exists
            conn.execute(text("DROP INDEX IF EXISTS ix_ai_enrichments_document_id;"))
            conn.commit()

            # Create the table
            ai_enrichments.create(engine)
            logger.info("Successfully created ai_enrichments table.")

    except Exception as e:
        logger.error(f"Error creating ai_enrichments table: {e}")
        raise


if __name__ == "__main__":
    create_ai_enrichments_table()
