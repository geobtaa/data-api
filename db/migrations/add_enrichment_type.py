import sys
import os
from pathlib import Path

# Add debug information
print("Python path:", sys.path)
print("Current working directory:", os.getcwd())
print("Virtual environment:", os.environ.get("VIRTUAL_ENV"))

# Add the project root directory to Python path
sys.path.append(str(Path(__file__).parent.parent.parent))

from sqlalchemy import Table, Column, String, MetaData, create_engine, inspect, text
import logging

from db.config import DATABASE_URL

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def add_enrichment_type_column():
    """Add the enrichment_type column to the ai_enrichments table."""
    try:
        # Create engine and inspector
        engine = create_engine(DATABASE_URL)
        inspector = inspect(engine)

        # Check if table exists
        if inspector.has_table("ai_enrichments"):
            # Check if column already exists
            columns = [col["name"] for col in inspector.get_columns("ai_enrichments")]
            if "enrichment_type" not in columns:
                # Add the enrichment_type column
                with engine.connect() as conn:
                    conn.execute(
                        text(
                            """
                        ALTER TABLE ai_enrichments 
                        ADD COLUMN enrichment_type VARCHAR(50) NOT NULL DEFAULT 'summarization';
                        
                        -- Create an index on the enrichment_type column
                        CREATE INDEX IF NOT EXISTS idx_enrichment_type ON ai_enrichments(enrichment_type);
                    """
                        )
                    )
                    conn.commit()
                    logger.info("Added enrichment_type column to ai_enrichments table")
            else:
                logger.info("enrichment_type column already exists in ai_enrichments table")
        else:
            logger.error("ai_enrichments table does not exist")

    except Exception as e:
        logger.error(f"Error adding enrichment_type column: {e}")
        raise


if __name__ == "__main__":
    add_enrichment_type_column()
