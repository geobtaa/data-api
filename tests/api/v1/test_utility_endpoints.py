import json
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


@pytest.fixture
def mock_document():
    """Return a mock document for testing."""
    return {
        "id": "test-document-id",
        "dct_title_s": "Test Document Title",
        "dct_description_sm": ["This is a test document description"],
        "dct_references_s": json.dumps(
            {
                "http://schema.org/downloadUrl": "https://example.com/download",
                "http://iiif.io/api/image": "https://example.com/iiif/image",
            }
        ),
        "dc_format_s": "PDF",
    }


@pytest.fixture
def mock_summary():
    """Return a mock AI summary for testing."""
    return {
        "id": 1,
        "document_id": "test-document-id",
        "type": "summary",
        "content": "This is a test AI-generated summary.",
        "created_at": "2023-01-01T00:00:00",
        "updated_at": "2023-01-01T00:00:00",
    }


@pytest.mark.asyncio
@patch("app.api.v1.endpoints.ENDPOINT_CACHE", True)
@patch("app.api.v1.endpoints.invalidate_cache_with_prefix")
async def test_clear_cache(mock_invalidate_cache):
    """Test the clear_cache endpoint."""
    # Setup mock
    mock_invalidate_cache.return_value = None

    # Test clearing specific cache type
    response = client.get("/api/v1/cache/clear?cache_type=search")
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert "search" in data["message"]

    # Verify invalidate was called with correct prefix
    mock_invalidate_cache.assert_called_with("app.api.v1.endpoints:search")

    # Test clearing all cache
    mock_invalidate_cache.reset_mock()
    response = client.get("/api/v1/cache/clear?cache_type=all")
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert "all" in data["message"]

    # Test with no cache type (should clear all)
    mock_invalidate_cache.reset_mock()
    response = client.get("/api/v1/cache/clear")
    assert response.status_code == 200
    data = response.json()
    assert "message" in data


@pytest.mark.asyncio
@patch("app.api.v1.endpoints.ImageService.get_cached_image")
async def test_get_thumbnail(mock_get_cached_image):
    """Test the get_thumbnail endpoint."""
    # Setup mock
    mock_image_data = b"test_image_data"
    mock_get_cached_image.return_value = mock_image_data

    # Test successful image retrieval
    response = client.get("/api/v1/thumbnails/test_hash")
    assert response.status_code == 200
    assert response.content == mock_image_data
    assert response.headers["Content-Type"] == "image/jpeg"
    assert "Cache-Control" in response.headers

    # Test image not found
    mock_get_cached_image.return_value = None
    response = client.get("/api/v1/thumbnails/nonexistent_hash")
    assert response.status_code == 404
    assert response.json()["detail"] == "Image not found"


@pytest.mark.asyncio
@patch("app.api.v1.endpoints.database.fetch_one")
@patch("app.api.v1.endpoints.generate_item_summary.delay")
@patch("app.api.v1.endpoints.invalidate_cache_with_prefix")
async def test_summarize_document(
    mock_invalidate_cache, mock_summary_task, mock_fetch_one, mock_document
):
    """Test the summarize_document endpoint."""
    # Setup mocks
    mock_fetch_one.return_value = mock_document

    # Mock the celery task
    mock_task = MagicMock()
    mock_task.id = "test-task-id"
    mock_summary_task.return_value = mock_task

    # Test successful summarization request
    response = client.post(f"/api/v1/documents/{mock_document['id']}/summarize")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "tasks" in data
    assert data["tasks"]["summary"] == "test-task-id"

    # Verify the task was called with correct parameters
    mock_summary_task.assert_called_once()
    args, kwargs = mock_summary_task.call_args
    assert kwargs["item_id"] == mock_document["id"]
    assert kwargs["metadata"] == mock_document

    # Verify cache invalidation
    mock_invalidate_cache.assert_called_once()

    # Test document not found
    mock_fetch_one.return_value = None
    response = client.post("/api/v1/documents/nonexistent-id/summarize")
    assert response.status_code == 404
    assert response.json()["detail"] == "Document not found"


@pytest.mark.asyncio
@patch("app.api.v1.endpoints.database.fetch_all")
async def test_get_document_summaries(mock_fetch_all, mock_summary):
    """Test the get_document_summaries endpoint."""
    # Setup mock
    mock_fetch_all.return_value = [mock_summary]

    # Test successful summaries retrieval
    response = client.get(f"/api/v1/documents/{mock_summary['document_id']}/summaries")
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["type"] == "summaries"
    assert data["data"]["id"] == mock_summary["document_id"]
    assert "summaries" in data["data"]["attributes"]
    assert len(data["data"]["attributes"]["summaries"]) == 1
    assert data["data"]["attributes"]["summaries"][0]["id"] == mock_summary["id"]
    assert data["data"]["attributes"]["summaries"][0]["content"] == mock_summary["content"]

    # Test no summaries
    mock_fetch_all.return_value = []
    response = client.get("/api/v1/documents/document-without-summaries/summaries")
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["type"] == "summaries"
    assert len(data["data"]["attributes"]["summaries"]) == 0


@pytest.mark.asyncio
@patch("app.api.v1.endpoints.reindex_documents")
@patch("app.api.v1.endpoints.ENDPOINT_CACHE", True)
@patch("app.api.v1.endpoints.invalidate_cache_with_prefix")
async def test_reindex(mock_invalidate_cache, mock_reindex_documents):
    """Test the reindex endpoint."""
    # Setup mock
    mock_reindex_documents.return_value = {"indexed": 100, "elapsed": 5.2}

    # Test successful reindexing
    response = client.post("/api/v1/reindex")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "details" in data
    assert data["details"]["indexed"] == 100

    # Verify caches were invalidated
    assert mock_invalidate_cache.call_count == 2  # Called for search and suggest
    mock_invalidate_cache.assert_any_call("app.api.v1.endpoints:search")
    mock_invalidate_cache.assert_any_call("app.api.v1.endpoints:suggest")

    # Verify reindex was called
    mock_reindex_documents.assert_called_once()
