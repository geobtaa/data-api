import asyncio
import os

from dotenv import load_dotenv
from elasticsearch import AsyncElasticsearch

# Load environment variables from .env file
load_dotenv()

# Use the ELASTICSEARCH_URL from .env file or default to localhost
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")

async def list_indices():
    # Create a client using the same settings as the application
    client = AsyncElasticsearch(
        hosts=[ELASTICSEARCH_URL],
        verify_certs=False,
        ssl_show_warn=False,
        request_timeout=60,
        retry_on_timeout=True,
        max_retries=3
    )
    
    try:
        # Check connection
        info = await client.info()
        print(f"Connected to Elasticsearch cluster: {info['cluster_name']}")
        print(f"Elasticsearch version: {info['version']['number']}")
        
        # List all indices
        indices = await client.indices.get(index="*")
        
        print("\nIndices:")
        for index_name, index_info in indices.items():
            doc_count = await get_doc_count(client, index_name)
            print(f"- {index_name} ({doc_count} documents)")
            
        # Get cluster health
        health = await client.cluster.health()
        print(f"\nCluster health: {health['status']}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        await client.close()

async def get_doc_count(client, index_name):
    try:
        stats = await client.indices.stats(index=index_name)
        return stats['indices'][index_name]['total']['docs']['count']
    except:
        return "unknown"

if __name__ == "__main__":
    asyncio.run(list_indices()) 