import json
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from jsonschema import validate

from app.main import app

client = TestClient(app)


@pytest.fixture
def mock_search_response():
    """Return a mock search response for testing."""
    return {
        "status": "success",
        "query_time": {
            "cache": "0ms",
            "elasticsearch": "10ms",
            "item_processing": {
                "total": "20ms",
                "per_item": "10ms",
                "thumbnail_service": "5ms",
                "citation_service": "5ms",
                "viewer_service": "10ms",
            },
            "total_response_time": "30ms",
        },
        "meta": {
            "pages": {
                "current_page": 1,
                "next_page": 2,
                "prev_page": None,
                "total_pages": 5,
                "limit_value": 10,
                "offset_value": 0,
                "total_count": 45,
                "first_page?": True,
                "last_page?": False,
            },
            "spelling_suggestions": [],
        },
        "data": [
            {
                "type": "document",
                "id": "test-doc-1",
                "score": 9.5,
                "attributes": {
                    "dct_title_s": "Test Document 1",
                    "dct_description_sm": ["Test description 1"],
                    "dct_creator_sm": ["Test Creator 1"],
                },
            },
            {
                "type": "document",
                "id": "test-doc-2",
                "score": 8.2,
                "attributes": {
                    "dct_title_s": "Test Document 2",
                    "dct_description_sm": ["Test description 2"],
                    "dct_creator_sm": ["Test Creator 2"],
                },
            },
        ],
        "included": [],
    }


@pytest.fixture
def mock_suggest_response():
    """Return a mock suggest response for testing."""
    return {
        "suggest": {
            "my-suggestion": [
                {
                    "text": "min",
                    "offset": 0,
                    "length": 3,
                    "options": [
                        {
                            "text": "minnesota",
                            "_id": "test-doc-1",
                            "_score": 0.95,
                            "_source": {"dct_title_s": "Minnesota Map"},
                        },
                        {
                            "text": "mining",
                            "_id": "test-doc-2",
                            "_score": 0.85,
                            "_source": {"dct_title_s": "Mining Data"},
                        },
                    ],
                }
            ]
        }
    }


@pytest.mark.asyncio
@patch("app.services.search_service.SearchService.search")
async def test_search_endpoint(mock_search, mock_search_response):
    """Test the search endpoint."""
    # Setup mock
    mock_search.return_value = mock_search_response

    # Call endpoint with basic query
    response = client.get("/api/v1/search?q=test&page=1&limit=10")

    # Verify the response
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "query_time" in data
    assert "meta" in data
    assert "data" in data
    assert len(data["data"]) == 2
    assert data["data"][0]["id"] == "test-doc-1"
    assert data["data"][0]["attributes"]["dct_title_s"] == "Test Document 1"

    # Verify that search was called with correct parameters
    mock_search.assert_called_once()
    # Check the arguments
    args, kwargs = mock_search.call_args
    assert kwargs["q"] == "test"
    assert kwargs["page"] == 1
    assert kwargs["limit"] == 10

    # Test the response is valid against the search schema
    # Load the schema
    with open("data/schemas/search.schema.json") as f:
        schema = json.load(f)

    # Validate response against schema
    validate(instance=data, schema=schema)


@pytest.mark.asyncio
@patch("app.services.search_service.SearchService.search")
async def test_search_with_sort(mock_search, mock_search_response):
    """Test the search endpoint with sorting."""
    # Setup mock
    mock_search.return_value = mock_search_response

    # Call endpoint with sort parameter
    response = client.get("/api/v1/search?q=test&sort=year_desc")

    # Verify the response
    assert response.status_code == 200

    # Verify that search was called with correct parameters
    mock_search.assert_called_once()
    # Check the sort argument
    args, kwargs = mock_search.call_args
    assert kwargs["sort"] is not None


@pytest.mark.asyncio
@patch("app.services.search_service.SearchService.search")
async def test_search_with_filters(mock_search, mock_search_response):
    """Test the search endpoint with filters."""
    # Setup mock
    mock_search.return_value = mock_search_response

    # Call endpoint with filter parameters
    response = client.get(
        "/api/v1/search?q=test&fq[dct_spatial_sm][]=Minnesota&fq[schema_provider_s][]=Test%20Provider"
    )

    # Verify the response
    assert response.status_code == 200

    # Verify that search was called with correct filter parameters
    mock_search.assert_called_once()
    args, kwargs = mock_search.call_args
    assert "request_query_params" in kwargs
    query_params = kwargs["request_query_params"]
    assert "fq%5Bdct_spatial_sm%5D%5B%5D=Minnesota" in query_params
    assert "fq%5Bschema_provider_s%5D%5B%5D=Test+Provider" in query_params


@pytest.mark.asyncio
@patch("app.elasticsearch.client.es.search")
async def test_suggest_endpoint(mock_es_search, mock_suggest_response):
    """Test the suggest endpoint."""
    # Setup mock
    mock_search_instance = MagicMock()
    mock_search_instance.body = mock_suggest_response

    # Create an async mock that returns the mock_search_instance
    async def async_mock_search(*args, **kwargs):
        return mock_search_instance

    mock_es_search.side_effect = async_mock_search

    # Call endpoint
    response = client.get("/api/v1/suggest?q=min")

    # Verify the response
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert len(data["data"]) > 0
    assert data["data"][0]["type"] == "suggestion"
    assert data["data"][0]["attributes"]["text"] == "minnesota"

    # Verify ES was called with correct parameters
    mock_es_search.assert_called_once()
    args, kwargs = mock_es_search.call_args
    assert "index" in kwargs
    assert "body" in kwargs
    assert kwargs["body"]["suggest"]["my-suggestion"]["prefix"] == "min"


@pytest.mark.asyncio
@patch("app.elasticsearch.client.es.search")
async def test_suggest_with_resource_class(mock_es_search, mock_suggest_response):
    """Test the suggest endpoint with resource class filter."""
    # Setup mock
    mock_search_instance = MagicMock()
    mock_search_instance.body = mock_suggest_response
    mock_es_search.return_value = mock_search_instance

    # Call endpoint with resource class
    response = client.get("/api/v1/suggest?q=min&resource_class=Maps")

    # Verify the response
    assert response.status_code == 200

    # Verify ES was called with correct parameters
    mock_es_search.assert_called_once()
    args, kwargs = mock_es_search.call_args
    assert "body" in kwargs
    assert kwargs["body"]["suggest"]["my-suggestion"]["prefix"] == "min"
