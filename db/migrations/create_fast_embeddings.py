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
    inspect,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.sql import text

# Add the project root directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

from db.config import DATABASE_URL

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def create_fast_embeddings_table():
    """Create the gazetteer_fast_embeddings table with vector support."""
    try:
        # Create async engine
        engine = create_async_engine(DATABASE_URL)
        
        # Check if table exists
        async with engine.connect() as conn:
            result = await conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'gazetteer_fast_embeddings'
                );
            """))
            table_exists = result.scalar()
            
            if table_exists:
                logger.info("Table gazetteer_fast_embeddings already exists")
                return

            # First, ensure the vector extension is installed
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
            
            # Create the table with proper vector type
            await conn.execute(text("""
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
            await conn.execute(text("""
                CREATE INDEX ON gazetteer_fast_embeddings 
                USING ivfflat (embeddings vector_cosine_ops)
                WITH (lists = 100);
            """))
            
            await conn.commit()
            logger.info("Successfully created gazetteer_fast_embeddings table")

    except Exception as e:
        logger.error(f"Error creating gazetteer_fast_embeddings table: {e}")
        raise
    finally:
        await engine.dispose()


if __name__ == "__main__":
    import asyncio
    asyncio.run(create_fast_embeddings_table()) 