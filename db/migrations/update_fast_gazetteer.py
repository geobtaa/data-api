import logging
import sys
from pathlib import Path

from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    TIMESTAMP,
    create_engine,
    inspect,
    text,
)

# Add the project root directory to Python path
sys.path.append(str(Path(__file__).parent.parent.parent))

from db.config import DATABASE_URL

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def update_fast_gazetteer():
    """Update the gazetteer_fast table with new columns."""
    try:
        # Create engine and inspector
        engine = create_engine(DATABASE_URL)
        inspector = inspect(engine)

        # Check if the table exists
        if not inspector.has_table("gazetteer_fast"):
            logger.error("gazetteer_fast table does not exist")
            return False

        # Get existing columns
        columns = [col["name"] for col in inspector.get_columns("gazetteer_fast")]
        logger.info(f"Existing columns: {columns}")

        # Create a connection to execute SQL
        with engine.connect() as conn:
            # Rename geonames column to geoname_id if it exists
            if "geonames" in columns and "geoname_id" not in columns:
                conn.execute(text("ALTER TABLE gazetteer_fast RENAME COLUMN geonames TO geoname_id"))
                logger.info("Renamed geonames column to geoname_id")

            # Add viaf_id column if it doesn't exist
            if "viaf_id" not in columns:
                conn.execute(text("ALTER TABLE gazetteer_fast ADD COLUMN viaf_id VARCHAR"))
                logger.info("Added viaf_id column")
                
            # Add wikipedia_id column if it doesn't exist
            if "wikipedia_id" not in columns:
                conn.execute(text("ALTER TABLE gazetteer_fast ADD COLUMN wikipedia_id VARCHAR"))
                logger.info("Added wikipedia_id column")

            # Update indexes
            conn.execute(
                text(
                    """
                DROP INDEX IF EXISTS idx_fast_geonames;
                CREATE INDEX IF NOT EXISTS idx_fast_geoname_id ON gazetteer_fast(geoname_id);
                CREATE INDEX IF NOT EXISTS idx_fast_viaf_id ON gazetteer_fast(viaf_id);
                CREATE INDEX IF NOT EXISTS idx_fast_wikipedia_id ON gazetteer_fast(wikipedia_id);
            """
                )
            )
            logger.info("Updated indexes for gazetteer_fast table")

            conn.commit()

        logger.info("Successfully updated gazetteer_fast table")
        return True

    except Exception as e:
        logger.error(f"Error updating gazetteer_fast table: {e}")
        return False


if __name__ == "__main__":
    update_fast_gazetteer() 