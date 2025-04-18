import json
from unittest.mock import patch

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
        "dct_creator_sm": ["Test Creator"],
        "dct_publisher_sm": ["Test Publisher"],
        "dct_references_s": json.dumps(
            {
                "http://schema.org/downloadUrl": "https://example.com/download",
                "http://iiif.io/api/image": "https://example.com/iiif/image",
            }
        ),
        "dc_format_s": "PDF",
        "gbl_resourcetype_sm": ["Maps"],
        "gbl_resourceclass_sm": ["Datasets"],
        "dct_spatial_sm": ["Minnesota"],
        "dct_rights_sm": ["Public"],
        "schema_provider_s": "Test Provider",
    }


@pytest.fixture
def mock_relationships():
    """Return mock relationships for testing."""
    return {
        "isPartOf": [
            {
                "doc_id": "related-doc-1",
                "doc_title": "Related Document 1",
                "link": "http://localhost:8000/api/v1/documents/related-doc-1",
            }
        ],
        "hasPart": [
            {
                "doc_id": "related-doc-2",
                "doc_title": "Related Document 2",
                "link": "http://localhost:8000/api/v1/documents/related-doc-2",
            }
        ],
    }


@pytest.fixture
def mock_summaries():
    """Return mock AI summaries for testing."""
    return [
        {
            "id": 1,
            "document_id": "test-document-id",
            "type": "summary",
            "content": "This is a test AI-generated summary.",
            "created_at": "2023-01-01T00:00:00",
            "updated_at": "2023-01-01T00:00:00",
        }
    ]


@pytest.mark.asyncio
@patch("app.api.v1.endpoints.database.fetch_one")
@patch("app.api.v1.endpoints.get_document_relationships")
@patch("app.api.v1.endpoints.database.fetch_all")
async def test_get_document(
    mock_fetch_all,
    mock_get_relationships,
    mock_fetch_one,
    mock_document,
    mock_relationships,
    mock_summaries,
):
    """Test the get_document endpoint."""
    # Setup mocks
    mock_fetch_one.return_value = mock_document
    mock_get_relationships.return_value = mock_relationships
    mock_fetch_all.return_value = mock_summaries

    # Call endpoint
    response = client.get(f"/api/v1/documents/{mock_document['id']}")

    # Verify response
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["id"] == mock_document["id"]
    assert data["data"]["attributes"]["dct_title_s"] == mock_document["dct_title_s"]
    assert "ui_thumbnail_url" in data["data"]["attributes"]
    assert "ui_citation" in data["data"]["attributes"]
    assert "ui_downloads" in data["data"]["attributes"]
    assert "ui_relationships" in data["data"]["attributes"]
    assert data["data"]["attributes"]["ui_relationships"] == mock_relationships
    assert "ui_summaries" in data["data"]["attributes"]


@pytest.mark.asyncio
@patch("app.api.v1.endpoints.database.fetch_one")
async def test_get_document_not_found(mock_fetch_one):
    """Test the get_document endpoint with non-existent ID."""
    # Setup mock to return None (document not found)
    mock_fetch_one.return_value = None

    # Call endpoint
    response = client.get("/api/v1/documents/non-existent-id")

    # Verify response
    assert response.status_code == 404
    assert response.json()["detail"] == "Document not found"


@pytest.mark.asyncio
@patch("app.api.v1.endpoints.database.fetch_all")
async def test_list_documents(mock_fetch_all, mock_document):
    """Test the list_documents endpoint."""
    # Setup mock to return a list of documents
    mock_fetch_all.return_value = [mock_document, mock_document]

    # Call endpoint
    response = client.get("/api/v1/documents/")

    # Verify response
    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]) == 2
    assert data["data"][0]["id"] == mock_document["id"]
    assert data["data"][0]["attributes"]["dct_title_s"] == mock_document["dct_title_s"]
    assert "ui_thumbnail_url" in data["data"][0]["attributes"]
    assert "ui_citation" in data["data"][0]["attributes"]
    assert "ui_downloads" in data["data"][0]["attributes"]


@pytest.mark.asyncio
@patch("app.api.v1.endpoints.database.fetch_all")
async def test_get_document_relationships(mock_fetch_all):
    """Test the get_document_relationships function."""
    # Setup mock to return relationship data
    mock_fetch_all.return_value = [
        {
            "predicate": "isPartOf",
            "object_id": "related-doc-1",
            "dct_title_s": "Related Document 1",
        },
        {"predicate": "hasPart", "object_id": "related-doc-2", "dct_title_s": "Related Document 2"},
    ]

    # Call function directly
    from app.api.v1.endpoints import get_document_relationships

    relationships = await get_document_relationships("test-document-id")

    # Verify result
    assert "isPartOf" in relationships
    assert "hasPart" in relationships
    assert len(relationships["isPartOf"]) == 1
    assert len(relationships["hasPart"]) == 1
    assert relationships["isPartOf"][0]["doc_id"] == "related-doc-1"
    assert relationships["hasPart"][0]["doc_id"] == "related-doc-2"
