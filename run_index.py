from app.elasticsearch.index import index_items
from db.database import database
from app.elasticsearch.client import es
import asyncio
import logging
import os
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

async def verify_index():
    """Verify that the index exists and has documents."""
    index_name = os.getenv("ELASTICSEARCH_INDEX", "btaa_ogm_api")
    
    try:
        # Check if index exists
        index_exists = await es.indices.exists(index=index_name)
        if not index_exists:
            logger.error(f"Index '{index_name}' was not created!")
            return False
            
        # Get document count
        count = await es.count(index=index_name)
        doc_count = count["count"]
        logger.info(f"Index '{index_name}' contains {doc_count} documents")
        
        # Get a sample document to verify content
        sample = await es.search(
            index=index_name,
            query={"match_all": {}},
            size=1
        )
        
        if sample["hits"]["total"]["value"] > 0:
            logger.info("Successfully verified document structure")
            return True
        else:
            logger.error("Index exists but contains no documents!")
            return False
            
    except Exception as e:
        logger.error(f"Error verifying index: {str(e)}")
        return False

async def main():
    try:
        logger.info("Connecting to database...")
        await database.connect()
        
        logger.info("Starting indexing process...")
        await index_items()
        
        logger.info("Verifying index...")
        if await verify_index():
            logger.info("Indexing completed successfully!")
        else:
            logger.error("Indexing completed but verification failed!")
            
    except Exception as e:
        logger.error(f"Error during indexing: {str(e)}")
        raise
    finally:
        logger.info("Cleaning up connections...")
        await database.disconnect()
        await es.close()

if __name__ == "__main__":
    asyncio.run(main()) 