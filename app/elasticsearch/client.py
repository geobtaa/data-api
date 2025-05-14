import logging
import os

from dotenv import load_dotenv
from elasticsearch import AsyncElasticsearch

load_dotenv()

# Create the AsyncElasticsearch client with minimal settings
es = AsyncElasticsearch(
    os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200"),
    verify_certs=False,  # For development only
    ssl_show_warn=False,  # For development only
    request_timeout=60,  # Increase timeout to 60 seconds
    retry_on_timeout=True,  # Retry on timeout
    max_retries=3,  # Maximum number of retries
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


async def init_elasticsearch():
    """Initialize Elasticsearch index and mappings."""
    from .mappings import INDEX_MAPPING

    index_name = os.getenv("ELASTICSEARCH_INDEX", "btaa_geometadata_api")

    try:
        # Test the connection
        info = await es.info()
        logger.info(f"Connected to Elasticsearch cluster: {info['cluster_name']}")

        # Check if index exists
        exists = await es.indices.exists(index=index_name)
        if not exists:
            logger.info(f"Creating index {index_name}")
            await es.indices.create(
                index=index_name,
                mappings=INDEX_MAPPING["mappings"],
                settings=INDEX_MAPPING["settings"],
            )
        else:
            logger.info(f"Index {index_name} already exists")

    except Exception as e:
        logger.error(f"Elasticsearch initialization error: {str(e)}", exc_info=True)
        raise


async def close_elasticsearch():
    """Close the Elasticsearch connection."""
    await es.close()
