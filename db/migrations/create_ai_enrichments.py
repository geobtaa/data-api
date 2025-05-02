import logging
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import (
    JSON,
    TIMESTAMP,
    Column,
    Integer,
    MetaData,
    String,
    Table,
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


def create_ai_enrichments_table():
    """Create the ai_enrichments table."""
    try:
        # Create engine and inspector
        engine = create_engine(DATABASE_URL)
        inspector = inspect(engine)

        # Create MetaData instance
        metadata = MetaData()

        # Define the ai_enrichments table
        ai_enrichments = Table(
            "ai_enrichments",
            metadata,
            Column("enrichment_id", Integer, primary_key=True, autoincrement=True),
            Column("document_id", String, nullable=False, index=True),
            Column("ai_provider", String, nullable=False),
            Column("model", String, nullable=False),
            Column("prompt", JSON, nullable=True),
            Column("output_parser", JSON, nullable=True),
            Column("response", JSON, nullable=True),
            Column("created_at", TIMESTAMP, nullable=False, default=datetime.utcnow),
            Column(
                "updated_at",
                TIMESTAMP,
                nullable=False,
                default=datetime.utcnow,
                onupdate=datetime.utcnow,
            ),
        )

        # Check if table exists using inspector
        if not inspector.has_table("ai_enrichments"):
            ai_enrichments.create(engine)
            logger.info("Created ai_enrichments table")

            # Create indexes
            with engine.connect() as conn:
                conn.execute(
                    text(
                        """
                    CREATE INDEX IF NOT EXISTS idx_document_id ON ai_enrichments(document_id);
                    CREATE INDEX IF NOT EXISTS idx_ai_provider ON ai_enrichments(ai_provider);
                    CREATE INDEX IF NOT EXISTS idx_model ON ai_enrichments(model);
                """
                    )
                )
                conn.commit()
                logger.info("Created indexes on ai_enrichments table")
        else:
            logger.info("ai_enrichments table already exists")

    except Exception as e:
        logger.error(f"Error creating ai_enrichments table: {e}")
        raise


if __name__ == "__main__":
    create_ai_enrichments_table()
