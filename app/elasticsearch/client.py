from elasticsearch import AsyncElasticsearch
from dotenv import load_dotenv
import os

load_dotenv()

# Create the AsyncElasticsearch client
es = AsyncElasticsearch(os.getenv("ELASTICSEARCH_URL", "http://localhost:9200"))


async def init_elasticsearch():
    """Initialize Elasticsearch index and mappings."""
    from .mappings import INDEX_MAPPING

    index_name = os.getenv("ELASTICSEARCH_INDEX", "geoblacklight")

    # Create the index if it doesn't exist
    if not await es.indices.exists(index=index_name):
        await es.indices.create(
            index=index_name,
            body={"mappings": INDEX_MAPPING["mappings"], "settings": INDEX_MAPPING["settings"]},
        )


async def close_elasticsearch():
    """Close the Elasticsearch connection."""
    await es.close()
