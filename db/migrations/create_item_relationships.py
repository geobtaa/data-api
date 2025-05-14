import logging
import sys
from pathlib import Path

from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, inspect
from sqlalchemy.sql import text

# Add the project root directory to Python path
sys.path.append(str(Path(__file__).parent.parent.parent))

from db.config import DATABASE_URL

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_relationships_table():
    """Create the item_relationships table."""
    try:
        # Create engine and inspector
        engine = create_engine(DATABASE_URL)
        inspector = inspect(engine)

        # Create MetaData instance
        metadata = MetaData()

        # Define the relationships table
        item_relationships = Table(
            "item_relationships",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("subject_id", String, nullable=False),
            Column("predicate", String, nullable=False),
            Column("object_id", String, nullable=False),
        )

        # Check if table exists using inspector
        if not inspector.has_table("item_relationships"):
            item_relationships.create(engine)
            logger.info("Created item_relationships table")

            # Create indexes
            with engine.connect() as conn:
                conn.execute(
                    text(
                        """
                    CREATE INDEX IF NOT EXISTS idx_subject_id ON item_relationships(subject_id);
                    CREATE INDEX IF NOT EXISTS idx_object_id ON item_relationships(object_id);
                    CREATE INDEX IF NOT EXISTS idx_predicate ON item_relationships(predicate);
                """
                    )
                )
                conn.commit()
                logger.info("Created indexes on item_relationships table")
        else:
            logger.info("item_relationships table already exists")

    except Exception as e:
        logger.error(f"Error creating relationships table: {e}")
        raise


if __name__ == "__main__":
    create_relationships_table()
