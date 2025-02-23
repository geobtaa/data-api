import pytest
from fastapi.testclient import TestClient
import json
from app.api.v1.endpoints import router, extract_filter_queries
from fastapi import FastAPI

# Create a test app
app = FastAPI()
app.include_router(router, prefix="/api/v1")

# Create a test client
client = TestClient(app)

@pytest.mark.asyncio
async def test_list_documents():
    """Test document listing."""
    response = client.get("/api/v1/documents/")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert len(data["data"]) > 0
    assert data["data"][0]["type"] == "document"

@pytest.mark.asyncio
async def test_read_item():
    """Test document retrieval."""
    response = client.get("/api/v1/documents/p16022coll230:2910")
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["type"] == "document"
    assert data["data"]["id"] == "p16022coll230:2910"
    assert data["data"]["attributes"]["dct_title_s"] == "A new description of Kent ..."

@pytest.mark.asyncio
async def test_read_item_not_found():
    """Test document not found."""
    response = client.get("/api/v1/documents/nonexistent")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_search_endpoint():
    """Test search functionality."""
    # Search for a term we know exists in our test document
    response = client.get("/api/v1/search?q=Kent")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert len(data["data"]) > 0
    assert data["data"][0]["attributes"]["dct_title_s"] == "A new description of Kent ..."

@pytest.mark.asyncio
async def test_search_no_results():
    """Test search with no results."""
    response = client.get("/api/v1/search?q=nonexistent")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert len(data["data"]) == 0

@pytest.mark.asyncio
async def test_jsonp_callback():
    """Test JSONP response format."""
    response = client.get("/api/v1/documents/p16022coll230:2910?callback=myCallback")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/javascript"
    assert response.text.startswith("myCallback(")
    assert response.text.endswith(")")

def test_extract_filter_queries():
    """Test filter query extraction."""
    query_string = "fq[resource_class_agg][]=Maps&fq[provider_agg][]=Big+Ten"
    
    result = extract_filter_queries(query_string)
    
    assert "gbl_resourceclass_sm" in result
    assert result["gbl_resourceclass_sm"] == ["Maps"]
    assert "schema_provider_s" in result
    assert result["schema_provider_s"] == ["Big Ten"] 