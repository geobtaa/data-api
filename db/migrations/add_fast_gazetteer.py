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
)
from sqlalchemy.sql import text

# Add the project root directory to Python path
sys.path.append(str(Path(__file__).parent.parent.parent))

from db.config import DATABASE_URL

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def add_fast_gazetteer():
    """Add the gazetteer_fast table."""
    try:
        # Create engine and inspector
        engine = create_engine(DATABASE_URL)
        inspector = inspect(engine)

        # Create MetaData instance
        metadata = MetaData()

        # Define the gazetteer_fast table
        gazetteer_fast = Table(
            "gazetteer_fast",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("fast_id", String, nullable=False, unique=True, index=True),
            Column("uri", String, nullable=False),
            Column("type", String, nullable=False),
            Column("label", String, nullable=False),
            Column("geoname_id", String),
            Column("created_at", TIMESTAMP),
            Column("updated_at", TIMESTAMP),
        )

        # Create table if it doesn't exist
        if not inspector.has_table("gazetteer_fast"):
            gazetteer_fast.create(engine)
            logger.info("Created gazetteer_fast table")
        else:
            logger.info("gazetteer_fast table already exists")

        # Create additional indexes
        with engine.connect() as conn:
            # Add index for label for optimized querying
            conn.execute(
                text(
                    """
                CREATE INDEX IF NOT EXISTS idx_fast_label ON gazetteer_fast(label);
                CREATE INDEX IF NOT EXISTS idx_fast_type ON gazetteer_fast(type);
                CREATE INDEX IF NOT EXISTS idx_fast_geoname_id ON gazetteer_fast(geoname_id);
            """
                )
            )
            conn.commit()
            logger.info("Created additional indexes for gazetteer_fast table")

    except Exception as e:
        logger.error(f"Error adding gazetteer_fast table: {e}")
        raise


if __name__ == "__main__":
    add_fast_gazetteer() 