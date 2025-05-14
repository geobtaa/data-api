import logging
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import (
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
sys.path.append(str(Path(__file__).parent.parent))

from db.config import DATABASE_URL

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_fast_embeddings_table():
    """Create the gazetteer_fast_embeddings table with vector support."""
    try:
        # Create engine
        engine = create_engine(DATABASE_URL)
        
        # Check if table exists
        inspector = inspect(engine)
        if inspector.has_table("gazetteer_fast_embeddings"):
            logger.info("Table gazetteer_fast_embeddings already exists")
            return

        with engine.connect() as conn:
            # First, ensure the vector extension is installed
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
            
            # Create the table with proper vector type
            conn.execute(text("""
                CREATE TABLE gazetteer_fast_embeddings (
                    id SERIAL PRIMARY KEY,
                    fast_id VARCHAR NOT NULL UNIQUE,
                    label VARCHAR NOT NULL,
                    geoname_id VARCHAR,
                    viaf_id VARCHAR,
                    wikipedia_id VARCHAR,
                    embeddings vector(1536) NOT NULL,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                );
            """))
            
            # Create index on the embeddings column for vector similarity search
            conn.execute(text("""
                CREATE INDEX ON gazetteer_fast_embeddings 
                USING ivfflat (embeddings vector_cosine_ops)
                WITH (lists = 100);
            """))
            
            conn.commit()
            logger.info("Successfully created gazetteer_fast_embeddings table")

    except Exception as e:
        logger.error(f"Error creating gazetteer_fast_embeddings table: {e}")
        raise


if __name__ == "__main__":
    create_fast_embeddings_table() 