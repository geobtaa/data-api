import json
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.main import app
from app.services.relationship_service import RelationshipService

client = TestClient(app)


@pytest.fixture
def mock_item():
    """Return a mock item for testing."""
    return {
        "id": "test-item-id",
        "dct_title_s": "Test Item Title",
        "dct_description_sm": ["This is a test item description"],
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
                "item_id": "related-item-1",
                "item_title": "Related Item 1",
                "link": "http://localhost:8000/api/v1/items/related-item-1",
            }
        ],
        "hasPart": [
            {
                "item_id": "related-item-2",
                "item_title": "Related Item 2",
                "link": "http://localhost:8000/api/v1/items/related-item-2",
            }
        ],
    }


@pytest.fixture
def mock_summaries():
    """Return mock AI summaries for testing."""
    return [
        {
            "id": 1,
            "item_id": "test-item-id",
            "type": "summary",
            "content": "This is a test AI-generated summary.",
            "created_at": "2023-01-01T00:00:00",
            "updated_at": "2023-01-01T00:00:00",
        }
    ]


@pytest.mark.asyncio
@patch("app.services.search_service.SearchService.get_item")
async def test_get_item(
    mock_get_item,
    mock_item,
    mock_relationships,
    mock_summaries,
):
    """Test the get_item endpoint."""
    # Setup mock response from SearchService
    mock_get_item.return_value = {
        "data": {
            "type": "item",
            "id": mock_item["id"],
            "attributes": {
                **mock_item,
                "ui_thumbnail_url": "https://example.com/thumbnail.jpg",
                "ui_citation": "Test Citation",
                "ui_downloads": {"pdf": "https://example.com/download.pdf"},
                "ui_relationships": mock_relationships,
                "ui_summaries": mock_summaries,
                "ui_viewer_endpoint": "https://example.com/viewer",
                "ui_viewer_geometry": "POINT(0 0)",
                "ui_viewer_protocol": "iiif",
            },
        }
    }

    # Call endpoint
    response = client.get(f"/api/v1/items/{mock_item['id']}")

    # Verify response
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["id"] == mock_item["id"]
    assert data["data"]["attributes"]["dct_title_s"] == mock_item["dct_title_s"]
    assert "ui_thumbnail_url" in data["data"]["attributes"]
    assert "ui_citation" in data["data"]["attributes"]
    assert "ui_downloads" in data["data"]["attributes"]
    assert "ui_relationships" in data["data"]["attributes"]
    assert data["data"]["attributes"]["ui_relationships"] == mock_relationships
    assert "ui_summaries" in data["data"]["attributes"]


@pytest.mark.asyncio
@patch("app.services.search_service.SearchService.get_item")
async def test_get_item_not_found(mock_get_item):
    """Test the get_item endpoint with non-existent ID."""

    # Setup mock to raise NotFoundError
    async def raise_not_found(*args, **kwargs):
        raise HTTPException(status_code=404, detail="Item not found")

    mock_get_item.side_effect = raise_not_found

    # Call endpoint
    response = client.get("/api/v1/items/non-existent-id")

    # Verify response
    assert response.status_code == 404
    assert response.json()["detail"] == "Item not found"


@pytest.mark.asyncio
@patch("app.api.v1.endpoints.database.fetch_all")
async def test_list_items(mock_fetch_all, mock_item):
    """Test the list_items endpoint."""
    # Setup mock to return a list of items
    mock_fetch_all.return_value = [mock_item, mock_item]

    # Call endpoint
    response = client.get("/api/v1/items/")

    # Verify response
    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]) == 2
    assert data["data"][0]["id"] == mock_item["id"]
    assert data["data"][0]["attributes"]["dct_title_s"] == mock_item["dct_title_s"]
    assert "ui_thumbnail_url" in data["data"][0]["attributes"]
    assert "ui_citation" in data["data"][0]["attributes"]
    assert "ui_downloads" in data["data"][0]["attributes"]


@pytest.mark.asyncio
@patch("app.api.v1.endpoints.database.fetch_all")
async def test_get_item_relationships(mock_fetch_all):
    """Test the get_item_relationships function."""
    # Setup mock to return relationship data
    mock_fetch_all.return_value = [
        {
            "predicate": "isPartOf",
            "object_id": "related-item-1",
            "dct_title_s": "Related Item 1",
        },
        {"predicate": "hasPart", "object_id": "related-item-2", "dct_title_s": "Related Item 2"},
    ]

    # Create service instance and call method
    relationship_service = RelationshipService()
    relationships = await relationship_service.get_item_relationships("test-item-id")

    # Verify result
    assert "isPartOf" in relationships
    assert "hasPart" in relationships
    assert len(relationships["isPartOf"]) == 1
    assert len(relationships["hasPart"]) == 1
    assert relationships["isPartOf"][0]["item_id"] == "related-item-1"
    assert relationships["hasPart"][0]["item_id"] == "related-item-2"
