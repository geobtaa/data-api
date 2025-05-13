import os

import pytest
import pytest_asyncio
from dotenv import load_dotenv
from elasticsearch import AsyncElasticsearch

from app.elasticsearch.client import close_elasticsearch, init_elasticsearch
from app.elasticsearch.mappings import INDEX_MAPPING

# Load environment variables from .env.test file
load_dotenv(".env.test")

# Get the test index name from environment variables
TEST_INDEX_NAME = os.getenv("ELASTICSEARCH_INDEX", "data_api_test")

# Use the ELASTICSEARCH_URL from .env file or default to localhost
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")


@pytest_asyncio.fixture
async def es_client():
    """Create and return an Elasticsearch client for testing."""
    # Create a client using the same URL and settings as the application
    client = AsyncElasticsearch(
        hosts=[ELASTICSEARCH_URL],
        verify_certs=False,  # For development only
        ssl_show_warn=False,  # For development only
        request_timeout=60,  # Increase timeout to 60 seconds
        retry_on_timeout=True,  # Retry on timeout
        max_retries=3,  # Maximum number of retries
    )

    try:
        # Verify connection
        info = await client.info()
        print(f"Connected to Elasticsearch cluster: {info['cluster_name']}")

        # Delete the test index if it exists
        if await client.indices.exists(index=TEST_INDEX_NAME):
            await client.indices.delete(index=TEST_INDEX_NAME)
            print(f"Deleted existing test index {TEST_INDEX_NAME}")

        yield client

    finally:
        # Clean up - delete the test index
        try:
            if await client.indices.exists(index=TEST_INDEX_NAME):
                await client.indices.delete(index=TEST_INDEX_NAME)
                print(f"Cleaned up test index {TEST_INDEX_NAME}")
        except Exception as e:
            print(f"Error cleaning up test index: {e}")
        
        # Always close the client
        await client.close()


@pytest.mark.asyncio
async def test_init_elasticsearch_success(es_client, monkeypatch):
    """Test successful initialization of Elasticsearch."""
    # Monkeypatch the global ES client to use our test client
    import app.elasticsearch.client

    monkeypatch.setattr(app.elasticsearch.client, "es", es_client)

    # Call the function
    await init_elasticsearch()

    # Verify the index was created
    assert await es_client.indices.exists(index=TEST_INDEX_NAME)


@pytest.mark.asyncio
async def test_init_elasticsearch_index_exists(es_client, monkeypatch):
    """Test initialization when index already exists."""
    # Monkeypatch the global ES client to use our test client
    import app.elasticsearch.client
    monkeypatch.setattr(app.elasticsearch.client, "es", es_client)

    # Delete the index if it exists
    if await es_client.indices.exists(index=TEST_INDEX_NAME):
        await es_client.indices.delete(index=TEST_INDEX_NAME)

    # Create the index first
    await es_client.indices.create(
        index=TEST_INDEX_NAME,
        mappings=INDEX_MAPPING["mappings"],
        settings=INDEX_MAPPING["settings"],
    )

    # Call the function
    await init_elasticsearch()

    # Verify the index still exists
    assert await es_client.indices.exists(index=TEST_INDEX_NAME)

    # Clean up - delete the index
    await es_client.indices.delete(index=TEST_INDEX_NAME)


@pytest.mark.asyncio
async def test_close_elasticsearch(es_client, monkeypatch):
    """Test closing the Elasticsearch connection."""
    # Monkeypatch the global ES client to use our test client
    import app.elasticsearch.client

    monkeypatch.setattr(app.elasticsearch.client, "es", es_client)

    # Call the function
    await close_elasticsearch()

    # This test is successful if no exception is raised
