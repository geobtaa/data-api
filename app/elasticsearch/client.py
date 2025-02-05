from elasticsearch import AsyncElasticsearch
from dotenv import load_dotenv
import os
import logging

load_dotenv()

# Create the AsyncElasticsearch client
es = AsyncElasticsearch(os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200"))

logger = logging.getLogger(__name__)

async def init_elasticsearch():
    """Initialize Elasticsearch index and mappings."""
    from .mappings import INDEX_MAPPING

    index_name = os.getenv("ELASTICSEARCH_INDEX", "geoblacklight")
    
    try:
        # Test the connection
        info = await es.info()
        logger.info(f"Connected to Elasticsearch cluster: {info.body['cluster_name']}")
        
        # Check if index exists
        exists = await es.indices.exists(index=index_name)
        if not exists:
            logger.info(f"Creating index {index_name}")
            await es.indices.create(
                index=index_name,
                body={"mappings": INDEX_MAPPING["mappings"], "settings": INDEX_MAPPING["settings"]},
            )
        else:
            logger.info(f"Index {index_name} already exists")
            
    except Exception as e:
        logger.error(f"Elasticsearch initialization error: {str(e)}", exc_info=True)
        raise


async def close_elasticsearch():
    """Close the Elasticsearch connection."""
    await es.close()
